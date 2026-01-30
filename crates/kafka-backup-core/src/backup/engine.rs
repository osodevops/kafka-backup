//! Backup engine orchestration.

use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex, Semaphore};
use tracing::{debug, error, info, warn};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::compression::extension;
use crate::config::{BackupOptions, CompressionType, Config, Mode, StartOffset};
use crate::health::HealthCheck;
use crate::kafka::PartitionLeaderRouter;
use crate::manifest::{BackupManifest, BackupRecord, SegmentMetadata};
use crate::metrics::{ErrorType, PerformanceMetrics, PrometheusMetrics};
use crate::offset_store::{OffsetStore, OffsetStoreConfig, SqliteOffsetStore};
use crate::segment::format::BinaryRecord;
use crate::segment::writer::{SegmentWriter, SegmentWriterConfig};
use crate::storage::{create_backend, StorageBackend};
use crate::{Error, Result};

/// Backup engine for backing up Kafka topics
pub struct BackupEngine {
    config: Config,
    router: Arc<PartitionLeaderRouter>,
    storage: Arc<dyn StorageBackend>,
    manifest: Arc<Mutex<BackupManifest>>,
    metrics: Arc<PerformanceMetrics>,
    prometheus_metrics: Option<Arc<PrometheusMetrics>>,
    health: Arc<HealthCheck>,
    offset_store: Option<Arc<SqliteOffsetStore>>,
    kafka_circuit_breaker: Arc<CircuitBreaker>,
    storage_circuit_breaker: Arc<CircuitBreaker>,
    shutdown_tx: broadcast::Sender<()>,
}

impl BackupEngine {
    /// Create a new backup engine
    pub async fn new(config: Config) -> Result<Self> {
        Self::new_with_metrics(config, None).await
    }

    /// Create a new backup engine with Prometheus metrics
    pub async fn new_with_metrics(
        config: Config,
        prometheus_metrics: Option<Arc<PrometheusMetrics>>,
    ) -> Result<Self> {
        config.validate()?;

        if config.mode != Mode::Backup {
            return Err(Error::Config(
                "Configuration mode must be 'backup'".to_string(),
            ));
        }

        let source = config
            .source
            .as_ref()
            .ok_or_else(|| Error::Config("Source configuration required".to_string()))?;

        // Create partition leader router for multi-broker support
        let router = Arc::new(PartitionLeaderRouter::new(source.clone()).await?);
        let storage = create_backend(&config.storage)?;
        let manifest = BackupManifest::new(config.backup_id.clone());

        // Set compression in manifest
        let compression = config
            .backup
            .as_ref()
            .map(|b| b.compression)
            .unwrap_or(CompressionType::Zstd);
        let mut manifest = manifest;
        manifest.compression = format!("{:?}", compression).to_lowercase();

        // Initialize metrics and health
        let metrics = Arc::new(PerformanceMetrics::new());
        let health = Arc::new(HealthCheck::new());

        // Register health components
        health.register_component("kafka");
        health.register_component("storage");
        health.register_component("checkpointing");

        // Initialize circuit breakers
        let kafka_circuit_breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(30),
            success_threshold: 2,
            name: "kafka".to_string(),
        }));

        let storage_circuit_breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            reset_timeout: Duration::from_secs(60),
            success_threshold: 1,
            name: "storage".to_string(),
        }));

        // Initialize offset store for continuous backups
        let backup_opts = config.backup.clone().unwrap_or_default();
        let offset_store = if backup_opts.continuous {
            let offset_config = OffsetStoreConfig {
                db_path: std::path::PathBuf::from(format!("./{}-offsets.db", config.backup_id)),
                s3_key: Some(format!("{}/offsets.db", config.backup_id)),
                checkpoint_interval_secs: backup_opts.checkpoint_interval_secs,
                sync_interval_secs: backup_opts.sync_interval_secs,
            };
            Some(Arc::new(SqliteOffsetStore::new(offset_config).await?))
        } else {
            None
        };

        let (shutdown_tx, _) = broadcast::channel(1);

        Ok(Self {
            config,
            router,
            storage,
            manifest: Arc::new(Mutex::new(manifest)),
            metrics,
            prometheus_metrics,
            health,
            offset_store,
            kafka_circuit_breaker,
            storage_circuit_breaker,
            shutdown_tx,
        })
    }

    /// Create a new backup engine (sync version for backward compatibility)
    pub fn new_sync(config: Config) -> Result<Self> {
        tokio::runtime::Handle::current().block_on(Self::new(config))
    }

    /// Get a shutdown signal receiver
    pub fn shutdown_receiver(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Get metrics
    pub fn metrics(&self) -> &PerformanceMetrics {
        &self.metrics
    }

    /// Get Prometheus metrics (if configured)
    pub fn prometheus_metrics(&self) -> Option<Arc<PrometheusMetrics>> {
        self.prometheus_metrics.clone()
    }

    /// Get health check
    pub fn health(&self) -> &HealthCheck {
        &self.health
    }

    /// Run the backup process
    pub async fn run(&self) -> Result<()> {
        self.health.job_started();

        let result = self.run_internal().await;

        self.health.job_completed();

        if result.is_err() {
            self.metrics.record_error();
        }

        result
    }

    async fn run_internal(&self) -> Result<()> {
        // Router is already connected during construction
        info!("Connected to Kafka cluster via partition leader router");
        self.health.mark_healthy("kafka");

        // Try to load offset store from remote if continuous
        if let Some(ref offset_store) = self.offset_store {
            if let Err(e) = offset_store
                .try_load_from_storage(
                    self.storage.as_ref(),
                    &format!("{}/offsets.db", self.config.backup_id),
                )
                .await
            {
                warn!("Failed to load offset store from remote: {}", e);
            }

            // Register job
            offset_store
                .get_or_create_job(&self.config.backup_id, None)
                .await?;
        }

        // Fetch metadata and determine topics to back up
        let source = self.config.source.as_ref().unwrap();
        let backup_opts = self.config.backup.clone().unwrap_or_default();
        let topics = self.resolve_topics(&source.topics, &backup_opts).await?;

        if topics.is_empty() {
            warn!("No topics matched the configured patterns");
            return Ok(());
        }

        info!("Backing up {} topics", topics.len());

        // Capture snapshot offsets if stop_at_current_offsets is enabled
        // This provides a consistent "point-in-time" snapshot for DR backups
        let snapshot_offsets: Option<HashMap<(String, i32), i64>> =
            if backup_opts.stop_at_current_offsets {
                Some(self.capture_snapshot_offsets(&topics).await?)
            } else {
                None
            };

        let mut shutdown_rx = self.shutdown_receiver();

        // Create semaphore to limit concurrent partition backups
        let semaphore = Arc::new(Semaphore::new(backup_opts.max_concurrent_partitions));

        info!(
            "Backup engine starting with max_concurrent_partitions={}, poll_interval_ms={}",
            backup_opts.max_concurrent_partitions, backup_opts.poll_interval_ms
        );

        // Run backup loop
        loop {
            // Process topics with controlled parallelism
            let mut all_handles = Vec::new();

            for topic in &topics {
                // Check for shutdown before spawning
                if shutdown_rx.try_recv().is_ok() {
                    info!("Shutdown signal received, stopping backup");
                    self.finalize().await?;
                    return Ok(());
                }

                // Get partitions for this topic
                let metadata = match self.router.get_topic_metadata(topic).await {
                    Ok(m) => m,
                    Err(e) => {
                        error!("Failed to get metadata for topic {}: {}", topic, e);
                        continue;
                    }
                };

                let partitions: Vec<i32> =
                    metadata.partitions.iter().map(|p| p.partition_id).collect();

                // Spawn a task for each partition (limited by semaphore)
                for partition in partitions {
                    // Get target offset for snapshot mode (if enabled)
                    let target_offset = snapshot_offsets
                        .as_ref()
                        .and_then(|m| m.get(&(topic.clone(), partition)))
                        .copied();

                    // Acquire semaphore permit to limit concurrency
                    let permit = semaphore.clone().acquire_owned().await.unwrap();

                    let ctx = BackupPartitionContext {
                        topic: topic.to_string(),
                        partition,
                        router: Arc::clone(&self.router),
                        storage: self.storage.clone(),
                        manifest: self.manifest.clone(),
                        backup_id: self.config.backup_id.clone(),
                        options: self.config.backup.clone().unwrap_or_default(),
                        metrics: Arc::clone(&self.metrics),
                        prometheus_metrics: self.prometheus_metrics.clone(),
                        health: Arc::clone(&self.health),
                        offset_store: self.offset_store.clone(),
                        kafka_cb: Arc::clone(&self.kafka_circuit_breaker),
                        storage_cb: Arc::clone(&self.storage_circuit_breaker),
                        target_offset,
                    };

                    all_handles.push(tokio::spawn(async move {
                        let result = (
                            ctx.topic.clone(),
                            ctx.partition,
                            ctx.backup_partition().await,
                        );
                        drop(permit); // Release semaphore permit when done
                        result
                    }));
                }
            }

            info!(
                "Spawned {} backup tasks (max {} concurrent)",
                all_handles.len(),
                backup_opts.max_concurrent_partitions
            );

            // Wait for ALL partitions across ALL topics to complete
            let results = futures::future::join_all(all_handles).await;

            let mut error_count = 0;
            for result in results {
                match result {
                    Ok((topic, partition, Ok(_))) => {
                        debug!("Completed backup of {}:{}", topic, partition);
                    }
                    Ok((topic, partition, Err(e))) => {
                        error!("Error backing up {}:{}: {}", topic, partition, e);
                        error_count += 1;
                        self.metrics.record_error();
                        // Record error to Prometheus metrics
                        if let Some(ref prom) = self.prometheus_metrics {
                            let error_type = ErrorType::from_error(&e);
                            prom.record_error(&self.config.backup_id, error_type);
                        }
                    }
                    Err(e) => {
                        error!("Task join error: {}", e);
                        error_count += 1;
                        // Record as unknown error
                        if let Some(ref prom) = self.prometheus_metrics {
                            prom.record_error(&self.config.backup_id, ErrorType::Unknown);
                        }
                    }
                }
            }

            if error_count > 0 && !self.health.is_operational() {
                return Err(Error::Io(std::io::Error::other(format!(
                    "{} partitions failed to backup",
                    error_count
                ))));
            }

            // Checkpoint offsets
            if let Some(ref offset_store) = self.offset_store {
                let start = Instant::now();
                offset_store.checkpoint().await?;
                self.metrics.record_checkpoint_latency(start.elapsed());
                self.health.mark_healthy("checkpointing");
            }

            // Save manifest periodically
            self.save_manifest().await?;

            // If not continuous, exit after one pass
            if !backup_opts.continuous {
                break;
            }

            // Wait before next iteration (configurable poll interval)
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(backup_opts.poll_interval_ms)) => {}
                _ = shutdown_rx.recv() => {
                    info!("Shutdown signal received, stopping backup");
                    break;
                }
            }
        }

        self.finalize().await?;

        if backup_opts.stop_at_current_offsets {
            info!(
                "Snapshot backup completed successfully - all partitions reached their target offsets"
            );
        } else {
            info!("Backup completed successfully");
        }

        Ok(())
    }

    async fn finalize(&self) -> Result<()> {
        // Final checkpoint
        if let Some(ref offset_store) = self.offset_store {
            offset_store.checkpoint().await?;
            offset_store
                .sync_to_storage(
                    self.storage.as_ref(),
                    &format!("{}/offsets.db", self.config.backup_id),
                )
                .await?;
            offset_store
                .update_job_status(&self.config.backup_id, "completed")
                .await?;
        }

        // Save final manifest
        self.save_manifest().await?;

        // Log metrics
        let report = self.metrics.report();
        info!("{}", report);

        Ok(())
    }

    /// Resolve topic patterns to actual topic names
    async fn resolve_topics(
        &self,
        selection: &crate::config::TopicSelection,
        backup_opts: &BackupOptions,
    ) -> Result<Vec<String>> {
        let all_topics = self.router.fetch_metadata(None).await?;

        let mut selected = Vec::new();
        for topic in all_topics {
            let name = &topic.name;

            // Handle internal topics
            if topic.is_internal {
                if !backup_opts.include_internal_topics {
                    continue;
                }
                // Check if this internal topic is in the explicit list
                if !backup_opts.internal_topics.is_empty()
                    && !backup_opts.internal_topics.contains(name)
                {
                    continue;
                }
            }

            // Check include patterns
            let included = if selection.include.is_empty() {
                true // Include all if no patterns specified
            } else {
                selection
                    .include
                    .iter()
                    .any(|pattern| glob_match(pattern, name))
            };

            // Check exclude patterns
            let excluded = selection
                .exclude
                .iter()
                .any(|pattern| glob_match(pattern, name));

            if included && !excluded {
                selected.push(name.clone());
            }
        }

        selected.sort();
        Ok(selected)
    }

    /// Save the manifest to storage
    async fn save_manifest(&self) -> Result<()> {
        let manifest = self.manifest.lock().await;
        let manifest_json = serde_json::to_string_pretty(&*manifest)?;
        let key = format!("{}/manifest.json", self.config.backup_id);

        self.storage.put(&key, Bytes::from(manifest_json)).await?;
        debug!("Saved manifest to {}", key);

        Ok(())
    }

    /// Capture current high watermarks for all partitions (snapshot mode)
    ///
    /// This provides a consistent snapshot point - all partitions will backup
    /// to the same logical point in time (the moment this method is called).
    async fn capture_snapshot_offsets(
        &self,
        topics: &[String],
    ) -> Result<HashMap<(String, i32), i64>> {
        let mut offsets = HashMap::new();
        let mut total_records = 0i64;

        info!(
            "Snapshot mode: capturing high watermarks for {} topics",
            topics.len()
        );

        for topic in topics {
            let metadata = match self.router.get_topic_metadata(topic).await {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to get metadata for topic {}: {}", topic, e);
                    return Err(e);
                }
            };

            for partition_meta in &metadata.partitions {
                let partition = partition_meta.partition_id;
                let (earliest, latest) = self.router.get_offsets(topic, partition).await?;
                offsets.insert((topic.clone(), partition), latest);

                let partition_records = latest - earliest;
                total_records += partition_records;

                debug!(
                    "Snapshot target: {}:{} -> offset {} ({} records available)",
                    topic, partition, latest, partition_records
                );
            }
        }

        info!(
            "Captured snapshot offsets for {} partitions ({} total records to backup)",
            offsets.len(),
            total_records
        );

        Ok(offsets)
    }
}

/// Context for backing up a single partition
struct BackupPartitionContext {
    topic: String,
    partition: i32,
    router: Arc<PartitionLeaderRouter>,
    storage: Arc<dyn StorageBackend>,
    manifest: Arc<Mutex<BackupManifest>>,
    backup_id: String,
    options: BackupOptions,
    metrics: Arc<PerformanceMetrics>,
    prometheus_metrics: Option<Arc<PrometheusMetrics>>,
    health: Arc<HealthCheck>,
    offset_store: Option<Arc<SqliteOffsetStore>>,
    kafka_cb: Arc<CircuitBreaker>,
    #[allow(dead_code)] // Reserved for future storage circuit breaker integration
    storage_cb: Arc<CircuitBreaker>,
    /// Target offset for snapshot mode (stop_at_current_offsets)
    /// When set, backup stops when this offset is reached instead of latest
    target_offset: Option<i64>,
}

impl BackupPartitionContext {
    async fn backup_partition(self) -> Result<()> {
        debug!("Starting backup of {}:{}", self.topic, self.partition);

        // Get partition offsets (routed to correct partition leader)
        let (earliest, latest) = self.router.get_offsets(&self.topic, self.partition).await?;

        // Determine end offset: use snapshot target if set, otherwise current latest
        // This enables "stop_at_current_offsets" mode for consistent DR snapshots
        let end_offset = self.target_offset.unwrap_or(latest);

        if self.target_offset.is_some() {
            debug!(
                "{}:{}: snapshot mode - target offset {} (current latest: {})",
                self.topic, self.partition, end_offset, latest
            );
        }

        // Determine starting offset
        let start_offset = if let Some(ref offset_store) = self.offset_store {
            // Check for saved offset first
            if let Some(saved) = offset_store
                .get_offset(&self.backup_id, &self.topic, self.partition)
                .await?
            {
                saved + 1 // Start from next offset
            } else {
                self.get_configured_start_offset(earliest)
            }
        } else {
            self.get_configured_start_offset(earliest)
        };

        // Record consumer lag (how many records we need to catch up)
        let lag = end_offset - start_offset;
        if let Some(ref prom) = self.prometheus_metrics {
            prom.record_lag(&self.topic, self.partition, &self.backup_id, lag, None);
        }

        debug!(
            "{}:{}: offsets earliest={}, end={}, starting at {}, lag={}",
            self.topic, self.partition, earliest, end_offset, start_offset, lag
        );

        // If no data to back up, skip
        if start_offset >= end_offset {
            debug!("{}:{}: no new data to back up", self.topic, self.partition);
            // Record zero lag
            if let Some(ref prom) = self.prometheus_metrics {
                prom.record_lag(&self.topic, self.partition, &self.backup_id, 0, None);
            }
            return Ok(());
        }

        // Create segment writer
        let writer_config = SegmentWriterConfig {
            max_segment_bytes: self.options.segment_max_bytes,
            max_segment_interval_ms: self.options.segment_max_interval_ms,
            compression: self.options.compression,
            compression_level: self.options.compression_level,
        };
        let mut segment_writer = SegmentWriter::with_prometheus(
            writer_config,
            self.storage.clone(),
            self.metrics.clone(),
            self.prometheus_metrics.clone(),
            self.backup_id.clone(),
        );

        let mut current_offset = start_offset;
        let mut segment_sequence = 0u64;

        // Fetch and store records in segments
        while current_offset < end_offset {
            let fetch_result = self.fetch_records(current_offset, end_offset).await;

            let (records, next_offset) = match fetch_result {
                Ok(data) => data,
                Err(e) => {
                    self.health
                        .mark_degraded("kafka", &format!("Fetch error: {}", e));
                    self.kafka_cb.record_failure();
                    // Record error to Prometheus metrics
                    if let Some(ref prom) = self.prometheus_metrics {
                        let error_type = ErrorType::from_error(&e);
                        prom.record_error(&self.backup_id, error_type);
                    }
                    return Err(e);
                }
            };

            self.kafka_cb.record_success();
            self.health.mark_healthy("kafka");

            if records.is_empty() {
                break;
            }

            // Convert to binary records and add to writer
            for record in &records {
                // Start with existing headers
                let mut headers: Vec<(String, Option<Bytes>)> = record
                    .headers
                    .iter()
                    .map(|h| (h.key.clone(), Some(Bytes::from(h.value.clone()))))
                    .collect();

                // Phase 1: Add offset mapping headers if configured
                if self.options.include_offset_headers {
                    // Store original offset as binary i64 (8 bytes, little-endian)
                    headers.push((
                        "x-original-offset".to_string(),
                        Some(Bytes::from(record.offset.to_le_bytes().to_vec())),
                    ));
                    // Store original timestamp as binary i64 (8 bytes, little-endian)
                    headers.push((
                        "x-original-timestamp".to_string(),
                        Some(Bytes::from(record.timestamp.to_le_bytes().to_vec())),
                    ));
                    // Store source cluster ID if configured
                    if let Some(ref cluster_id) = self.options.source_cluster_id {
                        headers.push((
                            "x-source-cluster".to_string(),
                            Some(Bytes::from(cluster_id.as_bytes().to_vec())),
                        ));
                    }
                }

                let binary_record = BinaryRecord {
                    timestamp: record.timestamp,
                    offset: record.offset,
                    key: record.key.as_ref().map(|k| Bytes::from(k.clone())),
                    value: record.value.as_ref().map(|v| Bytes::from(v.clone())),
                    headers,
                };
                segment_writer.add_record(binary_record)?;

                // Check if we should rotate
                if segment_writer.should_rotate() {
                    let key = self.segment_key(segment_sequence);
                    if let Some(segment_metadata) = segment_writer.flush(&key).await? {
                        self.add_segment_to_manifest(segment_metadata).await;
                        segment_sequence += 1;
                    }
                }
            }

            // Update offset tracking
            if let Some(ref offset_store) = self.offset_store {
                let last_offset = records.last().map(|r| r.offset).unwrap_or(current_offset);
                offset_store
                    .set_offset(&self.backup_id, &self.topic, self.partition, last_offset)
                    .await?;
            }

            // Track progress
            let record_count = records.len() as u64;
            let bytes_processed: u64 = records
                .iter()
                .map(|r| {
                    r.key.as_ref().map(|k| k.len()).unwrap_or(0)
                        + r.value.as_ref().map(|v| v.len()).unwrap_or(0)
                })
                .sum::<usize>() as u64;

            self.health.record_records(record_count);

            // Update Prometheus metrics
            if let Some(ref prom) = self.prometheus_metrics {
                prom.inc_records(&self.backup_id, record_count);
                prom.inc_bytes(&self.backup_id, bytes_processed);

                // Update lag (records remaining to process towards our target)
                let remaining_lag = end_offset - next_offset;
                prom.record_lag(
                    &self.topic,
                    self.partition,
                    &self.backup_id,
                    remaining_lag.max(0),
                    None,
                );
            }

            current_offset = next_offset;
        }

        // Flush any remaining records
        if segment_writer.has_data() {
            let key = self.segment_key(segment_sequence);
            if let Some(segment_metadata) = segment_writer.flush(&key).await? {
                self.add_segment_to_manifest(segment_metadata).await;
                segment_sequence += 1;
            }
        }

        if self.target_offset.is_some() {
            info!(
                "Completed snapshot backup of {}:{} - {} segments (reached target offset {})",
                self.topic, self.partition, segment_sequence, end_offset
            );
        } else {
            info!(
                "Completed backup of {}:{} - {} segments",
                self.topic, self.partition, segment_sequence
            );
        }

        Ok(())
    }

    fn get_configured_start_offset(&self, earliest: i64) -> i64 {
        match &self.options.start_offset {
            StartOffset::Earliest => earliest,
            StartOffset::Latest => earliest, // For backup, latest at start means start fresh
            StartOffset::Specific(map) => map
                .get(&self.topic)
                .and_then(|partitions| partitions.get(&self.partition))
                .copied()
                .unwrap_or(earliest),
        }
    }

    fn segment_key(&self, sequence: u64) -> String {
        let ext = extension(self.options.compression);
        format!(
            "{}/topics/{}/partition={}/segment-{:06}.bin{}",
            self.backup_id, self.topic, self.partition, sequence, ext
        )
    }

    async fn add_segment_to_manifest(&self, segment_metadata: SegmentMetadata) {
        let mut manifest = self.manifest.lock().await;
        let topic_backup = manifest.get_or_create_topic(&self.topic);
        let partition_backup = topic_backup.get_or_create_partition(self.partition);
        partition_backup.add_segment(segment_metadata);
    }

    async fn fetch_records(
        &self,
        start_offset: i64,
        _end_offset: i64,
    ) -> Result<(Vec<BackupRecord>, i64)> {
        let max_bytes = self.options.segment_max_bytes.min(16 * 1024 * 1024) as i32;

        // Fetch records using router (automatically routes to partition leader)
        let fetch_response = self
            .router
            .fetch(&self.topic, self.partition, start_offset, max_bytes)
            .await?;

        Ok((fetch_response.records, fetch_response.next_offset))
    }
}

/// Simple glob pattern matching (supports * and ?)
fn glob_match(pattern: &str, text: &str) -> bool {
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let text_chars: Vec<char> = text.chars().collect();

    glob_match_impl(&pattern_chars, &text_chars)
}

fn glob_match_impl(pattern: &[char], text: &[char]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }

    match pattern[0] {
        '*' => {
            // Match zero or more characters
            // Try matching the rest of the pattern at each position in text
            for i in 0..=text.len() {
                if glob_match_impl(&pattern[1..], &text[i..]) {
                    return true;
                }
            }
            false
        }
        '?' => {
            // Match exactly one character
            if text.is_empty() {
                false
            } else {
                glob_match_impl(&pattern[1..], &text[1..])
            }
        }
        c => {
            // Match literal character
            if text.is_empty() || text[0] != c {
                false
            } else {
                glob_match_impl(&pattern[1..], &text[1..])
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("orders", "orders"));
        assert!(glob_match("orders*", "orders"));
        assert!(glob_match("orders*", "orders-v2"));
        assert!(glob_match("*orders", "my-orders"));
        assert!(glob_match("*orders*", "my-orders-v2"));
        assert!(glob_match("order?", "orders"));
        assert!(!glob_match("order?", "order"));
        assert!(!glob_match("orders", "payments"));
    }
}
