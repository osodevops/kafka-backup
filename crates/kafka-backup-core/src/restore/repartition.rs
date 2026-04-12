//! Fan-out repartitioning for restore to a different partition count.
//!
//! When restoring a backup to a cluster with fewer (or more) partitions,
//! this module re-partitions records on the fly using Kafka's default
//! murmur2 partitioner so that records land on the correct target partition
//! based on their key — just like a native Kafka producer.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, Mutex, Semaphore};
use tracing::{debug, info};

use crate::circuit_breaker::CircuitBreaker;
use crate::config::{RepartitioningStrategy, RestoreOptions, TopicRepartitioning};
use crate::health::HealthCheck;
use crate::kafka::PartitionLeaderRouter;
use crate::manifest::{
    BackupRecord, PartitionRestoreReport, RestoreCheckpoint, SegmentMetadata, TopicBackup,
    TopicRestoreReport,
};
use crate::metrics::PerformanceMetrics;
use crate::storage::StorageBackend;
use crate::{Error, Result};

use super::helpers;

/// Kafka's default seed for murmur2 hashing (0x9747b28c).
const KAFKA_MURMUR2_SEED: u32 = murmur2::KAFKA_SEED;

/// Channel capacity for backpressure between reader and writer tasks.
const CHANNEL_CAPACITY: usize = 1_000;

/// Partitioner that routes records to target partitions.
pub struct Partitioner {
    strategy: RepartitioningStrategy,
    target_partitions: i32,
    round_robin_counter: AtomicI32,
}

impl Partitioner {
    pub fn new(strategy: RepartitioningStrategy, target_partitions: i32) -> Self {
        Self {
            strategy,
            target_partitions,
            round_robin_counter: AtomicI32::new(0),
        }
    }

    /// Compute the target partition for a record.
    ///
    /// - **Murmur2 + key present** (including empty `[]`): `(murmur2(key) as i32 & 0x7fffffff) % N`
    /// - **Murmur2 + key is None**: atomic round-robin
    /// - **Automatic**: round-robin for all records
    pub fn partition(&self, key: Option<&[u8]>) -> i32 {
        match self.strategy {
            RepartitioningStrategy::Murmur2 => match key {
                Some(k) => {
                    let hash = murmur2::murmur2(k, KAFKA_MURMUR2_SEED);
                    ((hash as i32) & 0x7fffffff) % self.target_partitions
                }
                None => self.next_round_robin(),
            },
            RepartitioningStrategy::Automatic => self.next_round_robin(),
        }
    }

    fn next_round_robin(&self) -> i32 {
        let val = self.round_robin_counter.fetch_add(1, Ordering::Relaxed);
        // Wrap-safe: use unsigned modulo via rem_euclid
        val.rem_euclid(self.target_partitions)
    }
}

/// Restore a topic using fan-out repartitioning.
///
/// N source-partition reader tasks feed M target-partition writer tasks
/// through bounded channels, with the `Partitioner` routing each record
/// to the correct channel based on its key.
#[allow(clippy::too_many_arguments)]
pub async fn restore_topic_repartitioned(
    topic_backup: &TopicBackup,
    target_topic: &str,
    repart_config: &TopicRepartitioning,
    options: &RestoreOptions,
    router: Arc<PartitionLeaderRouter>,
    storage: Arc<dyn StorageBackend>,
    metrics: Arc<PerformanceMetrics>,
    health: Arc<HealthCheck>,
    kafka_cb: Arc<CircuitBreaker>,
    storage_cb: Arc<CircuitBreaker>,
    checkpoint: Arc<Mutex<Option<RestoreCheckpoint>>>,
) -> Result<TopicRestoreReport> {
    let num_target_partitions = repart_config.target_partitions;
    info!(
        "Repartitioning topic {} -> {} ({} source partitions -> {} target partitions, strategy: {:?})",
        topic_backup.name,
        target_topic,
        topic_backup.partitions.len(),
        num_target_partitions,
        repart_config.strategy,
    );

    // Filter source partitions
    let partitions_to_restore: Vec<_> = topic_backup
        .partitions
        .iter()
        .filter(|p| {
            options
                .source_partitions
                .as_ref()
                .map(|filter| filter.contains(&p.partition_id))
                .unwrap_or(true)
        })
        .collect();

    if partitions_to_restore.is_empty() {
        debug!(
            "No partitions to restore for topic {} after filtering",
            topic_backup.name
        );
        return Ok(TopicRestoreReport {
            source_topic: topic_backup.name.clone(),
            target_topic: target_topic.to_string(),
            partitions: Vec::new(),
            records: 0,
            bytes: 0,
        });
    }

    // Create M bounded channels (one per target partition)
    let mut senders: Vec<mpsc::Sender<BackupRecord>> =
        Vec::with_capacity(num_target_partitions as usize);
    let mut receivers: Vec<Option<mpsc::Receiver<BackupRecord>>> =
        Vec::with_capacity(num_target_partitions as usize);

    for _ in 0..num_target_partitions {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        senders.push(tx);
        receivers.push(Some(rx));
    }

    let partitioner = Arc::new(Partitioner::new(
        repart_config.strategy,
        num_target_partitions,
    ));
    let cancel = Arc::new(AtomicBool::new(false));

    // Spawn N reader tasks
    let semaphore = Arc::new(Semaphore::new(options.max_concurrent_partitions));
    let mut reader_handles = Vec::new();

    for partition_backup in &partitions_to_restore {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let segments = partition_backup.segments.clone();
        let source_partition = partition_backup.partition_id;
        let source_topic = topic_backup.name.clone();
        let senders = senders.clone();
        let partitioner = Arc::clone(&partitioner);
        let cancel = Arc::clone(&cancel);
        let storage = Arc::clone(&storage);
        let storage_cb = Arc::clone(&storage_cb);
        let health = Arc::clone(&health);
        let checkpoint = Arc::clone(&checkpoint);
        let options = options.clone();

        reader_handles.push(tokio::spawn(async move {
            let result = run_reader(
                &source_topic,
                source_partition,
                &segments,
                &senders,
                &partitioner,
                &cancel,
                storage.as_ref(),
                &storage_cb,
                &health,
                &checkpoint,
                &options,
            )
            .await;
            drop(permit);
            result
        }));
    }

    // Drop our copies of the senders so writers see EOF when all readers finish
    drop(senders);

    // Spawn M writer tasks (one per target partition)
    let mut writer_handles = Vec::new();

    for target_partition in 0..num_target_partitions {
        let rx = receivers[target_partition as usize]
            .take()
            .expect("receiver already taken");
        let router = Arc::clone(&router);
        let target_topic = target_topic.to_string();
        let cancel = Arc::clone(&cancel);
        let kafka_cb = Arc::clone(&kafka_cb);
        let health = Arc::clone(&health);
        let metrics = Arc::clone(&metrics);
        let options = options.clone();

        writer_handles.push(tokio::spawn(async move {
            run_writer(
                rx,
                &router,
                &target_topic,
                target_partition,
                &cancel,
                &kafka_cb,
                &health,
                &metrics,
                &options,
            )
            .await
        }));
    }

    // Join all reader tasks
    let mut reader_errors = Vec::new();
    for handle in reader_handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => reader_errors.push(e),
            Err(e) => reader_errors.push(Error::Io(std::io::Error::other(format!(
                "Reader task join error: {}",
                e
            )))),
        }
    }

    // Join all writer tasks
    let mut partition_reports = Vec::new();
    let mut total_records = 0u64;
    let mut total_bytes = 0u64;

    for handle in writer_handles {
        match handle.await {
            Ok(Ok(report)) => {
                total_records += report.records;
                total_bytes += report.bytes;
                partition_reports.push(report);
            }
            Ok(Err(e)) => reader_errors.push(e),
            Err(e) => reader_errors.push(Error::Io(std::io::Error::other(format!(
                "Writer task join error: {}",
                e
            )))),
        }
    }

    if let Some(first_err) = reader_errors.into_iter().next() {
        return Err(first_err);
    }

    Ok(TopicRestoreReport {
        source_topic: topic_backup.name.clone(),
        target_topic: target_topic.to_string(),
        partitions: partition_reports,
        records: total_records,
        bytes: total_bytes,
    })
}

/// Reader task: reads segments from storage, routes records through partitioner to channels.
#[allow(clippy::too_many_arguments)]
async fn run_reader(
    source_topic: &str,
    source_partition: i32,
    segments: &[SegmentMetadata],
    senders: &[mpsc::Sender<BackupRecord>],
    partitioner: &Partitioner,
    cancel: &Arc<AtomicBool>,
    storage: &dyn StorageBackend,
    storage_cb: &CircuitBreaker,
    health: &HealthCheck,
    checkpoint: &Mutex<Option<RestoreCheckpoint>>,
    options: &RestoreOptions,
) -> Result<()> {
    // Filter segments by time window
    let filtered_segments: Vec<&SegmentMetadata> = segments
        .iter()
        .filter(|s| s.overlaps_time_window(options.time_window_start, options.time_window_end))
        .collect();

    // Check which segments are already completed (for resume)
    let completed_segments: HashSet<String> = {
        let cp = checkpoint.lock().await;
        cp.as_ref()
            .map(|c| c.segments_completed.iter().cloned().collect())
            .unwrap_or_default()
    };

    for segment in filtered_segments {
        // Abort if a writer has failed
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }

        if completed_segments.contains(&segment.key) {
            debug!("Skipping completed segment: {}", segment.key);
            continue;
        }

        let records = match helpers::read_segment(storage, segment).await {
            Ok(r) => {
                storage_cb.record_success();
                health.mark_healthy("storage");
                r
            }
            Err(e) => {
                storage_cb.record_failure();
                health.mark_degraded("storage", &format!("Read error: {}", e));
                return Err(e);
            }
        };

        let filtered = helpers::filter_records_by_time(records, options);

        if filtered.is_empty() {
            helpers::mark_segment_completed(checkpoint, &segment.key).await;
            continue;
        }

        let records_to_send = helpers::inject_offset_headers(filtered, source_partition, options);

        // Route each record through the partitioner
        for record in records_to_send {
            if cancel.load(Ordering::Relaxed) {
                return Ok(());
            }

            let target_partition = partitioner.partition(record.key.as_deref());
            if senders[target_partition as usize]
                .send(record)
                .await
                .is_err()
            {
                // Channel closed — writer died, cancellation will propagate
                return Ok(());
            }
        }

        helpers::mark_segment_completed(checkpoint, &segment.key).await;
    }

    debug!("Reader for {}:{} finished", source_topic, source_partition);
    Ok(())
}

/// Writer task: receives records from a channel and produces them in batches.
#[allow(clippy::too_many_arguments)]
async fn run_writer(
    mut rx: mpsc::Receiver<BackupRecord>,
    router: &PartitionLeaderRouter,
    target_topic: &str,
    target_partition: i32,
    cancel: &Arc<AtomicBool>,
    kafka_cb: &CircuitBreaker,
    health: &HealthCheck,
    metrics: &PerformanceMetrics,
    options: &RestoreOptions,
) -> Result<PartitionRestoreReport> {
    let batch_size = options.produce_batch_size;
    let mut total_records = 0u64;
    let mut total_bytes = 0u64;

    let mut batch: Vec<BackupRecord> = Vec::with_capacity(batch_size);

    loop {
        // Drain up to batch_size records, blocking on the first one
        if batch.is_empty() {
            match rx.recv().await {
                Some(record) => batch.push(record),
                None => break, // channel closed, all readers done
            }
        }

        // Non-blocking drain to fill the batch
        while batch.len() < batch_size {
            match rx.try_recv() {
                Ok(record) => batch.push(record),
                Err(_) => break,
            }
        }

        // Apply rate limiting
        if let Some(rate) = options.rate_limit_records_per_sec {
            let delay = Duration::from_secs_f64(batch.len() as f64 / rate as f64);
            tokio::time::sleep(delay).await;
        }

        let batch_bytes: u64 = batch
            .iter()
            .map(|r| {
                r.key.as_ref().map(|k| k.len()).unwrap_or(0) as u64
                    + r.value.as_ref().map(|v| v.len()).unwrap_or(0) as u64
            })
            .sum();

        match router
            .produce(
                target_topic,
                target_partition,
                batch.clone(),
                options.produce_acks,
                options.produce_timeout_ms,
            )
            .await
        {
            Ok(_) => {
                kafka_cb.record_success();
                health.mark_healthy("kafka");

                total_records += batch.len() as u64;
                total_bytes += batch_bytes;
                metrics.record_records(batch.len() as u64);
                health.record_records(batch.len() as u64);
            }
            Err(e) => {
                kafka_cb.record_failure();
                health.mark_degraded("kafka", &format!("Produce error: {}", e));
                cancel.store(true, Ordering::Relaxed);
                return Err(e);
            }
        }

        batch.clear();
    }

    debug!(
        "Writer for {}:{} finished ({} records)",
        target_topic, target_partition, total_records
    );

    Ok(PartitionRestoreReport {
        source_partition: -1, // N:M mapping, no single source
        target_partition,
        segments_processed: 0, // tracked by readers
        records: total_records,
        bytes: total_bytes,
        first_offset: 0,
        last_offset: 0,
        first_timestamp: 0,
        last_timestamp: 0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Murmur2 cross-language verification ────────────────────────────
    // The murmur2 crate implements Kafka's murmur2 algorithm with KAFKA_SEED.
    // We verify determinism and the toPositive + modulo partition assignment.

    #[test]
    fn test_murmur2_known_keys() {
        // Verify the hash is deterministic and the seed constant matches
        assert_eq!(KAFKA_MURMUR2_SEED, 0x9747b28c);

        let keys: Vec<&[u8]> = vec![b"hello", b"test-key", b"", b"order-12345"];
        for key in &keys {
            let h1 = murmur2::murmur2(key, KAFKA_MURMUR2_SEED);
            let h2 = murmur2::murmur2(key, KAFKA_MURMUR2_SEED);
            assert_eq!(h1, h2, "murmur2 must be deterministic for key {:?}", key);
        }

        // Different keys should (almost certainly) produce different hashes
        let h_hello = murmur2::murmur2(b"hello", KAFKA_MURMUR2_SEED);
        let h_world = murmur2::murmur2(b"world", KAFKA_MURMUR2_SEED);
        assert_ne!(h_hello, h_world);
    }

    #[test]
    fn test_murmur2_partition_assignment() {
        // Verify toPositive: (hash as i32 & 0x7fffffff) is always >= 0
        let keys: Vec<&[u8]> = vec![b"hello", b"test-key", b"", b"order-12345", b"\xff\xff"];
        for key in &keys {
            let hash = murmur2::murmur2(key, KAFKA_MURMUR2_SEED);
            let positive = (hash as i32) & 0x7fffffff;
            assert!(
                positive >= 0,
                "toPositive must produce non-negative for {:?}",
                key
            );
            let partition = positive % 6;
            assert!((0..6).contains(&partition));
        }
    }

    // ── Partitioner behavior ───────────────────────────────────────────

    #[test]
    fn test_empty_key_is_hashed() {
        // Empty key (Some(vec![])) should be hashed, NOT round-robin
        let p = Partitioner::new(RepartitioningStrategy::Murmur2, 10);
        let key: Vec<u8> = vec![];
        let p1 = p.partition(Some(&key));
        let p2 = p.partition(Some(&key));
        assert_eq!(p1, p2, "Empty key must hash deterministically");
    }

    #[test]
    fn test_null_key_round_robin() {
        let p = Partitioner::new(RepartitioningStrategy::Murmur2, 3);
        let mut seen = std::collections::HashSet::new();
        for _ in 0..9 {
            seen.insert(p.partition(None));
        }
        // After 9 calls with 3 partitions, should have hit all 3
        assert_eq!(
            seen.len(),
            3,
            "Null key must round-robin across all partitions"
        );
    }

    #[test]
    fn test_automatic_round_robin() {
        let p = Partitioner::new(RepartitioningStrategy::Automatic, 4);
        let key = b"some-key";
        let results: Vec<i32> = (0..8).map(|_| p.partition(Some(key))).collect();
        // Should cycle: 0,1,2,3,0,1,2,3
        assert_eq!(results, vec![0, 1, 2, 3, 0, 1, 2, 3]);
    }

    #[test]
    fn test_determinism() {
        let p = Partitioner::new(RepartitioningStrategy::Murmur2, 12);
        let key = b"deterministic-key";
        let first = p.partition(Some(key));
        for _ in 0..1000 {
            assert_eq!(p.partition(Some(key)), first);
        }
    }
}
