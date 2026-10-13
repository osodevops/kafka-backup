//! Restore engine orchestration.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex, Semaphore};
use tracing::{debug, error, info, warn};

use super::telemetry::{RestoreOutcome, RestorePlan, RestoreTelemetry};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::config::{
    Config, ExistingTopicConfigPolicy, IsolationLevel, Mode, OffsetStrategy, RestoreOptions,
    UnsupportedConfigPolicy,
};
use crate::health::HealthCheck;
use crate::kafka::{ConfigResourceType, PartitionLeaderRouter, TopicToCreate};
use crate::manifest::{
    BackupManifest, BackupRecord, DryRunPartitionReport, DryRunRepartitioningInfo, DryRunReport,
    DryRunTopicReport, OffsetMapping, PartitionRestoreReport, RestoreCheckpoint, RestoreReport,
    SegmentMetadata, TopicBackup, TopicRestoreReport,
};
use crate::metrics::{PerformanceMetrics, PrometheusMetrics};
use crate::storage::{create_backend, StorageBackend};
use crate::{Error, Result};

use super::checkpoint::CheckpointStore;

/// Progress update sent during restore (see
/// [`RestoreEngine::progress_receiver`]): once the restore's totals are
/// known, after the first produced batch, then at most every 5 seconds while
/// records flow, and once at the end of the run.
#[derive(Debug, Clone)]
pub struct RestoreProgress {
    /// Current topic being restored
    pub current_topic: String,
    /// Current partition being restored
    pub current_partition: i32,
    /// Total topics to restore
    pub total_topics: usize,
    /// Topics completed
    pub topics_completed: usize,
    /// Total segments to process
    pub total_segments: u64,
    /// Segments completed
    pub segments_completed: u64,
    /// Total records to restore (estimate)
    pub total_records: u64,
    /// Records restored so far
    pub records_restored: u64,
    /// Total bytes to restore (estimate)
    pub total_bytes: u64,
    /// Bytes restored so far
    pub bytes_restored: u64,
    /// Current throughput (records/sec)
    pub throughput_records_per_sec: f64,
    /// Current throughput (bytes/sec)
    pub throughput_bytes_per_sec: f64,
    /// Elapsed time in milliseconds
    pub elapsed_ms: u64,
    /// Estimated time remaining in milliseconds
    pub eta_ms: Option<u64>,
    /// Progress percentage (0.0 - 100.0)
    pub percentage: f64,
}

impl RestoreProgress {
    /// Format progress as a human-readable string
    pub fn format(&self) -> String {
        let eta_str = self
            .eta_ms
            .map(|eta| format!("ETA: {}s", eta / 1000))
            .unwrap_or_else(|| "ETA: calculating...".to_string());

        format!(
            "[{:.1}%] Topic {}/{} ({}) | Partition {} | Records: {} | Bytes: {} | {} | Throughput: {:.0} rec/s",
            self.percentage,
            self.topics_completed + 1,
            self.total_topics,
            self.current_topic,
            self.current_partition,
            format_count(self.records_restored),
            format_bytes(self.bytes_restored),
            eta_str,
            self.throughput_records_per_sec
        )
    }
}

fn format_count(count: u64) -> String {
    if count >= 1_000_000_000 {
        format!("{:.2}B", count as f64 / 1_000_000_000.0)
    } else if count >= 1_000_000 {
        format!("{:.2}M", count as f64 / 1_000_000.0)
    } else if count >= 1_000 {
        format!("{:.2}K", count as f64 / 1_000.0)
    } else {
        count.to_string()
    }
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Restore engine for restoring Kafka topics from backups
pub struct RestoreEngine {
    config: Config,
    router: Arc<tokio::sync::RwLock<Option<Arc<PartitionLeaderRouter>>>>,
    storage: Arc<dyn StorageBackend>,
    metrics: Arc<PerformanceMetrics>,
    health: Arc<HealthCheck>,
    kafka_circuit_breaker: Arc<CircuitBreaker>,
    storage_circuit_breaker: Arc<CircuitBreaker>,
    shutdown_tx: broadcast::Sender<()>,
    checkpoint: Arc<Mutex<Option<RestoreCheckpoint>>>,
    offset_mapping: Arc<Mutex<OffsetMapping>>,
    /// Restore metrics and the progress broadcast (issue #173).
    telemetry: Arc<RestoreTelemetry>,
    target_config: Option<crate::config::KafkaConfig>,
    /// Restore-wide record/byte rate limit, shared by every partition.
    rate_limiter: Option<Arc<super::rate_limit::RestoreRateLimiter>>,
    /// The restore's (idempotent) producer, started in `run()`.
    producer: Arc<tokio::sync::RwLock<Option<Arc<super::producer::RestoreProducer>>>>,
    /// Where the restore checkpoint is saved, when checkpointing is on.
    checkpoint_store: Arc<tokio::sync::RwLock<Option<Arc<CheckpointStore>>>>,
    /// Target topic -> `max.message.bytes` (`None` when it could not be read).
    max_message_bytes: Arc<Mutex<HashMap<String, Option<usize>>>>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct AutoConsumerGroupSnapshot {
    #[serde(default)]
    snapshot_time: Option<i64>,
    #[serde(default)]
    groups: Vec<AutoConsumerGroupSnapshotGroup>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub(crate) struct AutoConsumerGroupSnapshotGroup {
    group_id: String,
    #[serde(default)]
    offsets: HashMap<String, HashMap<String, i64>>,
}

impl AutoConsumerGroupSnapshot {
    fn group_ids(&self) -> Vec<String> {
        self.groups
            .iter()
            .map(|group| group.group_id.clone())
            .collect()
    }

    /// Number of consumer groups listed in the snapshot.
    pub(crate) fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Number of partition offsets recorded across all groups.
    pub(crate) fn offset_count(&self) -> usize {
        self.groups
            .iter()
            .map(|group| group.offsets.values().map(HashMap::len).sum::<usize>())
            .sum()
    }
}

pub(crate) fn parse_auto_consumer_group_snapshot(data: &[u8]) -> Result<AutoConsumerGroupSnapshot> {
    Ok(serde_json::from_slice(data)?)
}

fn import_auto_consumer_group_snapshot_offsets(
    offset_mapping: &mut OffsetMapping,
    snapshot: &AutoConsumerGroupSnapshot,
    restore_options: &RestoreOptions,
) -> Vec<String> {
    let group_ids = snapshot.group_ids();
    let timestamp = snapshot
        .snapshot_time
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
    let mut imported_offsets = 0usize;
    let mut skipped_offsets = 0usize;

    for group in &snapshot.groups {
        for (source_topic, topic_offsets) in &group.offsets {
            let target_topic = restore_options
                .topic_mapping
                .get(source_topic)
                .map(String::as_str)
                .unwrap_or(source_topic.as_str());

            for (source_partition, source_offset) in topic_offsets {
                if *source_offset < 0 {
                    skipped_offsets += 1;
                    warn!(
                        "auto_consumer_groups: skipping negative offset for group {} topic {} partition {}",
                        group.group_id, source_topic, source_partition
                    );
                    continue;
                }

                let source_partition = match source_partition.parse::<i32>() {
                    Ok(partition) => partition,
                    Err(e) => {
                        skipped_offsets += 1;
                        warn!(
                            "auto_consumer_groups: skipping invalid partition '{}' for group {} topic {}: {}",
                            source_partition, group.group_id, source_topic, e
                        );
                        continue;
                    }
                };

                let target_partition = restore_options
                    .partition_mapping
                    .get(&source_partition)
                    .copied()
                    .unwrap_or(source_partition);

                offset_mapping.add_consumer_group_offset(
                    &group.group_id,
                    target_topic,
                    target_partition,
                    *source_offset,
                    timestamp,
                    None,
                );
                imported_offsets += 1;
            }
        }
    }

    offset_mapping.recalculate_consumer_group_offsets();

    if imported_offsets == 0 && !group_ids.is_empty() {
        warn!(
            "auto_consumer_groups: snapshot listed {} groups but contained no usable offsets",
            group_ids.len()
        );
    } else {
        info!(
            "auto_consumer_groups: imported {} consumer group offsets from snapshot",
            imported_offsets
        );
    }

    if skipped_offsets > 0 {
        warn!(
            "auto_consumer_groups: skipped {} malformed snapshot offsets",
            skipped_offsets
        );
    }

    group_ids
}

impl RestoreEngine {
    /// Create a new restore engine
    pub fn new(config: Config) -> Result<Self> {
        Self::new_with_metrics(config, None)
    }

    /// Create a new restore engine that records the `kafka_restore_*`
    /// Prometheus metrics (records, bytes, partitions completed, errors,
    /// duration, progress) in `prometheus_metrics`, labelled with the backup
    /// id.
    pub fn new_with_metrics(
        config: Config,
        prometheus_metrics: Option<Arc<PrometheusMetrics>>,
    ) -> Result<Self> {
        config.validate()?;

        if config.mode != Mode::Restore {
            return Err(Error::Config(
                "Configuration mode must be 'restore'".to_string(),
            ));
        }

        let target_config = config
            .target
            .as_ref()
            .ok_or_else(|| Error::Config("Target configuration required".to_string()))?
            .clone();

        let storage = create_backend(&config.storage)?;

        // Initialize metrics and health
        let metrics = Arc::new(PerformanceMetrics::new());
        let health = Arc::new(HealthCheck::new());

        // Register health components
        health.register_component("kafka");
        health.register_component("storage");

        // Initialize circuit breakers from restore options (Kafka breaker is
        // advisory — see CircuitBreakerSettings docs / issue #197).
        let breaker_settings = config
            .restore
            .as_ref()
            .map(|r| r.circuit_breaker.clone())
            .unwrap_or_default();
        let kafka_circuit_breaker =
            Arc::new(CircuitBreaker::from_settings(&breaker_settings, "kafka"));

        let storage_circuit_breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            reset_timeout: Duration::from_secs(60),
            success_threshold: 1,
            name: "storage".to_string(),
            enabled: true,
        }));

        let (shutdown_tx, _) = broadcast::channel(1);
        let telemetry = Arc::new(RestoreTelemetry::new(&config.backup_id, prometheus_metrics));
        let rate_limiter = config
            .restore
            .as_ref()
            .and_then(super::rate_limit::RestoreRateLimiter::from_options)
            .map(Arc::new);

        Ok(Self {
            config,
            router: Arc::new(tokio::sync::RwLock::new(None)), // Router is created during run() since it requires async
            storage,
            metrics,
            health,
            kafka_circuit_breaker,
            storage_circuit_breaker,
            shutdown_tx,
            checkpoint: Arc::new(Mutex::new(None)),
            offset_mapping: Arc::new(Mutex::new(OffsetMapping::new())),
            telemetry,
            target_config: Some(target_config),
            rate_limiter,
            producer: Arc::new(tokio::sync::RwLock::new(None)),
            checkpoint_store: Arc::new(tokio::sync::RwLock::new(None)),
            max_message_bytes: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Current Kafka circuit-breaker state (for tests / observability).
    pub fn kafka_circuit_state(&self) -> crate::circuit_breaker::CircuitState {
        self.kafka_circuit_breaker.state()
    }

    /// Subscribe to progress updates
    pub fn progress_receiver(&self) -> broadcast::Receiver<RestoreProgress> {
        self.telemetry.subscribe()
    }

    /// Get Prometheus metrics (if configured)
    pub fn prometheus_metrics(&self) -> Option<Arc<PrometheusMetrics>> {
        self.telemetry.prometheus()
    }

    /// Get a shutdown signal receiver
    pub fn shutdown_receiver(&self) -> broadcast::Receiver<()> {
        self.shutdown_tx.subscribe()
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
    }

    /// Get a clone of the shutdown sender for external signal handling
    pub fn shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
    }

    /// Get metrics
    pub fn metrics(&self) -> &PerformanceMetrics {
        &self.metrics
    }

    /// Get health check
    pub fn health(&self) -> &HealthCheck {
        &self.health
    }

    /// Run the restore process
    pub async fn run(&self) -> Result<RestoreReport> {
        self.health.job_started();
        self.telemetry.start();

        let start_time = Instant::now();
        let start_timestamp = chrono::Utc::now().timestamp_millis();

        let result = self.run_internal().await;

        self.health.job_completed();

        let end_timestamp = chrono::Utc::now().timestamp_millis();
        let duration_ms = start_time.elapsed().as_millis() as u64;
        self.telemetry.finish(
            match &result {
                Ok(report) if report.dry_run => RestoreOutcome::DryRun,
                Ok(_) => RestoreOutcome::Success,
                Err(e) => RestoreOutcome::Failure(e),
            },
            start_time.elapsed(),
        );

        // Log final metrics
        let report = self.metrics.report();
        info!("{}", report);

        match result {
            Ok(mut restore_result) => {
                restore_result.start_time = start_timestamp;
                restore_result.end_time = end_timestamp;
                restore_result.duration_ms = duration_ms;

                if duration_ms > 0 {
                    restore_result.throughput_records_per_sec =
                        restore_result.records_restored as f64 / (duration_ms as f64 / 1000.0);
                    restore_result.throughput_bytes_per_sec =
                        restore_result.bytes_restored as f64 / (duration_ms as f64 / 1000.0);
                }

                // Write offset mapping report if configured
                if let Some(report_path) = self
                    .config
                    .restore
                    .as_ref()
                    .and_then(|r| r.offset_report.as_ref())
                {
                    if let Err(e) = self.write_offset_report(report_path).await {
                        warn!("Failed to write offset report: {}", e);
                    }
                }

                Ok(restore_result)
            }
            Err(e) => {
                self.metrics.record_error();
                Err(e)
            }
        }
    }

    /// Run a dry-run validation
    pub async fn dry_run(&self) -> Result<DryRunReport> {
        info!(
            "Running dry-run validation for backup: {}",
            self.config.backup_id
        );

        // Load manifest
        let manifest = self.load_manifest().await?;

        let restore_options = self.config.restore.clone().unwrap_or_default();
        let target = self.config.target.as_ref().unwrap();

        // Filter topics
        let topics_to_restore = self.filter_topics(&manifest, &target.topics)?;

        let mut report = DryRunReport {
            backup_id: self.config.backup_id.clone(),
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            segments_to_process: 0,
            records_to_restore: 0,
            bytes_to_restore: 0,
            time_range: None,
            topics_to_restore: Vec::new(),
            consumer_offset_actions: Vec::new(),
            header_preflight: None,
        };

        // Phase 1 header preflight (issue #137): validate-restore runs the
        // same scanner the real restore would, so a preflight that would
        // block the restore is reported here first.
        {
            let mode = restore_options.header_preflight;
            let recovery = super::preflight::offset_recovery_requested(&restore_options);
            let scan = super::preflight::scan_required(mode, &restore_options);
            if scan || recovery {
                let preflight = super::preflight::run_header_preflight(
                    self.storage.as_ref(),
                    &manifest,
                    &target.topics,
                    &restore_options,
                    mode,
                )
                .await;
                report.warnings.extend(preflight.warnings.clone());
                if recovery && !preflight.passed {
                    report.valid = false;
                    report.errors.extend(preflight.errors.clone());
                }
                report.header_preflight = Some(preflight);
            }
        }

        if topics_to_restore.is_empty() {
            report
                .warnings
                .push("No topics matched for restore".to_string());
            return Ok(report);
        }

        let mut min_timestamp: Option<i64> = None;
        let mut max_timestamp: Option<i64> = None;

        for topic_backup in &topics_to_restore {
            let target_topic = restore_options
                .topic_mapping
                .get(&topic_backup.name)
                .cloned()
                .unwrap_or_else(|| topic_backup.name.clone());

            let repartitioning_info =
                restore_options
                    .repartitioning
                    .get(&target_topic)
                    .map(|repart| DryRunRepartitioningInfo {
                        strategy: repart.strategy.to_string(),
                        source_partitions: topic_backup.partitions.len() as i32,
                        target_partitions: repart.target_partitions,
                    });

            let mut topic_report = DryRunTopicReport {
                source_topic: topic_backup.name.clone(),
                target_topic,
                repartitioning: repartitioning_info,
                partitions: Vec::new(),
            };

            for partition_backup in &topic_backup.partitions {
                // Apply partition filter
                if let Some(partitions) = &restore_options.source_partitions {
                    if !partitions.contains(&partition_backup.partition_id) {
                        continue;
                    }
                }

                // Get target partition (with remapping)
                let target_partition = restore_options
                    .partition_mapping
                    .get(&partition_backup.partition_id)
                    .copied()
                    .unwrap_or(partition_backup.partition_id);

                // Filter segments by time window
                let segments: Vec<_> = partition_backup
                    .segments
                    .iter()
                    .filter(|s| {
                        s.overlaps_time_window(
                            restore_options.time_window_start,
                            restore_options.time_window_end,
                        )
                    })
                    .collect();

                if segments.is_empty() {
                    if !partition_backup.pruned.is_empty() {
                        report.warnings.push(format!(
                            "{}:{}: all segments in the selected window were pruned by retention",
                            topic_backup.name, partition_backup.partition_id
                        ));
                    }
                    continue;
                }

                // Storage existence checks: the oldest overlapping segment is
                // always checked (bucket lifecycle rules expire oldest-first,
                // so this canary catches a wiped archive at one request per
                // partition); dry_run_check_segments sweeps every segment.
                let to_check: Vec<&SegmentMetadata> = if restore_options.dry_run_check_segments {
                    segments.iter().map(|s| &**s).collect()
                } else {
                    segments.first().map(|s| &**s).into_iter().collect()
                };
                for segment in to_check {
                    match self.storage.exists(&segment.key).await {
                        Ok(true) => {}
                        Ok(false) => {
                            report.valid = false;
                            report.errors.push(format!(
                                "segment missing from storage: {} (deleted by a bucket \
                                 lifecycle rule? use `kafka-backup prune` instead — see the \
                                 storage guide)",
                                segment.key
                            ));
                        }
                        Err(e) => {
                            report
                                .warnings
                                .push(format!("could not check segment {}: {}", segment.key, e));
                        }
                    }
                }

                for range in &partition_backup.pruned {
                    report.warnings.push(format!(
                        "{}:{}: offsets {}..{} were pruned by retention on {}",
                        topic_backup.name,
                        partition_backup.partition_id,
                        range.start_offset,
                        range.end_offset,
                        chrono::DateTime::from_timestamp_millis(range.pruned_at)
                            .map(|dt| dt.to_rfc3339())
                            .unwrap_or_else(|| range.pruned_at.to_string())
                    ));
                }

                let mut partition_records = 0i64;
                let mut partition_min_offset = i64::MAX;
                let mut partition_max_offset = i64::MIN;
                let mut partition_min_ts = i64::MAX;
                let mut partition_max_ts = i64::MIN;

                for segment in &segments {
                    report.segments_to_process += 1;
                    partition_records += segment.record_count;
                    report.bytes_to_restore += segment.uncompressed_size;

                    partition_min_offset = partition_min_offset.min(segment.start_offset);
                    partition_max_offset = partition_max_offset.max(segment.end_offset);
                    partition_min_ts = partition_min_ts.min(segment.start_timestamp);
                    partition_max_ts = partition_max_ts.max(segment.end_timestamp);

                    // Update global time range
                    min_timestamp = Some(
                        min_timestamp
                            .map_or(segment.start_timestamp, |m| m.min(segment.start_timestamp)),
                    );
                    max_timestamp = Some(
                        max_timestamp
                            .map_or(segment.end_timestamp, |m| m.max(segment.end_timestamp)),
                    );
                }

                report.records_to_restore += partition_records as u64;

                topic_report.partitions.push(DryRunPartitionReport {
                    source_partition: partition_backup.partition_id,
                    target_partition,
                    segments: segments.len() as u64,
                    records: partition_records as u64,
                    offset_range: (partition_min_offset, partition_max_offset),
                    timestamp_range: (partition_min_ts, partition_max_ts),
                });
            }

            if !topic_report.partitions.is_empty() {
                report.topics_to_restore.push(topic_report);
            }
        }

        report.time_range = min_timestamp.zip(max_timestamp);

        // Add consumer offset actions based on strategy
        match restore_options.consumer_group_strategy {
            OffsetStrategy::Skip => {
                report
                    .consumer_offset_actions
                    .push("Consumer offsets will not be modified (skip strategy)".to_string());
            }
            OffsetStrategy::HeaderBased => {
                report.consumer_offset_actions.push(
                    "Original offsets will be stored in message headers (x-original-offset, x-original-timestamp)".to_string(),
                );
            }
            OffsetStrategy::TimestampBased => {
                report
                    .consumer_offset_actions
                    .push("Consumer offsets will be calculated based on timestamps".to_string());
            }
            OffsetStrategy::ClusterScan => {
                report.consumer_offset_actions.push(
                    "Consumer offsets will be determined by scanning target cluster".to_string(),
                );
            }
            OffsetStrategy::Manual => {
                report
                    .consumer_offset_actions
                    .push("Offset mapping will be reported; manual reset required".to_string());
            }
        }

        if restore_options.reset_consumer_offsets {
            for group in &restore_options.consumer_groups {
                report
                    .consumer_offset_actions
                    .push(format!("Consumer group '{}' offsets will be reset", group));
            }
        }

        info!(
            "Dry-run complete: {} topics, {} segments, {} records, {} bytes",
            report.topics_to_restore.len(),
            report.segments_to_process,
            report.records_to_restore,
            report.bytes_to_restore
        );

        Ok(report)
    }

    async fn run_internal(&self) -> Result<RestoreReport> {
        let mut restore_options = self.config.restore.clone().unwrap_or_default();

        // Check for dry-run mode
        if restore_options.dry_run {
            let dry_run_report = self.dry_run().await?;
            return Ok(RestoreReport {
                backup_id: dry_run_report.backup_id,
                dry_run: true,
                start_time: 0,
                end_time: 0,
                duration_ms: 0,
                topics_restored: Vec::new(),
                segments_processed: dry_run_report.segments_to_process,
                records_restored: dry_run_report.records_to_restore,
                bytes_restored: dry_run_report.bytes_to_restore,
                throughput_records_per_sec: 0.0,
                throughput_bytes_per_sec: 0.0,
                errors: dry_run_report.errors,
                offset_mapping: OffsetMapping::new(),
                resolved_consumer_groups: Vec::new(),
                records_dropped_by_filter: 0,
                records_tombstoned_by_filter: 0,
                record_filter: None,
                topic_configs_skipped: Vec::new(),
                topic_configs_adjusted: Vec::new(),
                idempotent_producer: false,
                idempotent_producer_fallback: None,
            });
        }

        // Load manifest
        info!("Loading backup manifest for: {}", self.config.backup_id);
        let manifest = self.load_manifest().await?;

        // Load or seed the resume checkpoint. The config hash covers the
        // restore options (including any record-filter fingerprint), so a
        // resumed run with a changed configuration starts over instead of
        // silently skipping segments the new configuration would treat
        // differently. The seeded checkpoint is saved at once (#160), then
        // every checkpoint_interval_secs and after every topic.
        let resuming = self.open_checkpoint(&restore_options).await?;
        let _periodic_saver = self.spawn_periodic_checkpoint_saver(&restore_options).await;

        // Phase 1 header preflight (issue #137): prove tracking-metadata
        // coverage BEFORE connecting to the target and before any mutation
        // (topic creation, config changes, purge, produce). When consumer-
        // offset recovery is requested and required metadata is absent, the
        // restore fails here.
        let target = self.config.target.as_ref().unwrap();
        let _header_preflight = self
            .run_header_preflight(&manifest, &target.topics, &restore_options)
            .await?;

        // Create partition leader router for multi-broker support
        info!("Connecting to target Kafka cluster via partition leader router...");
        let target_config = self
            .target_config
            .clone()
            .ok_or_else(|| Error::Config("Target configuration required".to_string()))?;

        match PartitionLeaderRouter::new(target_config).await {
            Ok(router) => {
                let router = Arc::new(router);
                *self.router.write().await = Some(Arc::clone(&router));
                self.health.mark_healthy("kafka");
                info!("Connected to target Kafka cluster via partition leader router");
            }
            Err(e) => {
                self.health
                    .mark_unhealthy("kafka", &format!("Connection failed: {}", e));
                return Err(e);
            }
        }

        // Filter topics based on configuration
        let target = self.config.target.as_ref().unwrap();
        let topics_to_restore = self.filter_topics(&manifest, &target.topics)?;

        if topics_to_restore.is_empty() {
            warn!("No topics matched for restore");
            return Ok(RestoreReport {
                backup_id: self.config.backup_id.clone(),
                dry_run: false,
                start_time: 0,
                end_time: 0,
                duration_ms: 0,
                topics_restored: Vec::new(),
                segments_processed: 0,
                records_restored: 0,
                bytes_restored: 0,
                throughput_records_per_sec: 0.0,
                throughput_bytes_per_sec: 0.0,
                errors: Vec::new(),
                offset_mapping: OffsetMapping::new(),
                resolved_consumer_groups: Vec::new(),
                records_dropped_by_filter: 0,
                records_tombstoned_by_filter: 0,
                record_filter: None,
                topic_configs_skipped: Vec::new(),
                topic_configs_adjusted: Vec::new(),
                idempotent_producer: false,
                idempotent_producer_fallback: None,
            });
        }

        self.telemetry.plan(RestorePlan::new(
            &topics_to_restore,
            &restore_options,
            self.checkpoint.lock().await.as_ref(),
        ));

        // Start the producer before any change to the target: one
        // InitProducerId for the whole restore (or the fallback).
        let producer = self.start_producer(&restore_options).await?;

        // Auto-create topics if configured
        let newly_created_topics = if restore_options.create_topics {
            self.ensure_topics_exist(&topics_to_restore, &restore_options)
                .await?
        } else {
            HashSet::new()
        };

        let topic_configs = if restore_options.restore_topic_configs {
            self.restore_topic_configs(&topics_to_restore, &restore_options, &newly_created_topics)
                .await?
        } else {
            super::topic_configs::TopicConfigOutcome::default()
        };

        // Auto-load consumer groups from snapshot if requested (Issue #51 / #67 bug 5)
        let auto_consumer_group_snapshot = if restore_options.auto_consumer_groups
            && restore_options.consumer_groups.is_empty()
        {
            match self.load_auto_consumer_group_snapshot().await {
                Ok(snapshot) => {
                    let groups = snapshot.group_ids();
                    let offset_count = snapshot
                        .groups
                        .iter()
                        .map(|group| group.offsets.values().map(HashMap::len).sum::<usize>())
                        .sum::<usize>();

                    info!(
                        "auto_consumer_groups: loaded {} groups and {} offsets from snapshot",
                        groups.len(),
                        offset_count
                    );
                    restore_options.consumer_groups = groups;
                    restore_options.reset_consumer_offsets = true;
                    Some(snapshot)
                }
                Err(e) => {
                    warn!(
                        "auto_consumer_groups: failed to load snapshot ({e}); continuing without group reset"
                    );
                    None
                }
            }
        } else {
            None
        };

        // Purge target topics before restore if requested (Issue #67 bug 10).
        // Never when resuming: it would delete the records the checkpoint
        // says are restored.
        if restore_options.purge_topics && resuming {
            warn!(
                "purge_topics is set but the restore is resuming from a checkpoint with progress; \
                 the target topics are not purged again"
            );
        } else if restore_options.purge_topics {
            self.purge_topics_before_restore(&topics_to_restore, &restore_options)
                .await?;
        }

        info!("Restoring {} topics", topics_to_restore.len());
        if restore_options.strip_offset_headers {
            info!(
                "strip_offset_headers=true: removing {} headers from archived records before producing",
                crate::offset_headers::ALL.join(", ")
            );
        }
        if restore_options.include_original_offset_header
            || restore_options.consumer_group_strategy == OffsetStrategy::HeaderBased
        {
            info!(
                "Adding {}, {} and {} headers to every produced record (include_original_offset_header / header-based strategy)",
                crate::offset_headers::X_ORIGINAL_OFFSET,
                crate::offset_headers::X_ORIGINAL_TIMESTAMP,
                crate::offset_headers::X_SOURCE_PARTITION
            );
        }

        let mut shutdown_rx = self.shutdown_receiver();
        let mut topic_reports = Vec::new();
        let mut total_segments = 0u64;
        let mut total_records = 0u64;
        let mut total_bytes = 0u64;
        let mut errors = Vec::new();

        // Restore each topic
        for topic_backup in topics_to_restore {
            // Check for shutdown
            if shutdown_rx.try_recv().is_ok() {
                info!("Shutdown signal received, stopping restore");
                break;
            }

            match self.restore_topic(&topic_backup, &restore_options).await {
                Ok(report) => {
                    total_segments += report
                        .partitions
                        .iter()
                        .map(|p| p.segments_processed)
                        .sum::<u64>();
                    total_records += report.records;
                    total_bytes += report.bytes;
                    topic_reports.push(report);
                    self.telemetry.topic_completed();
                }
                Err(e) => {
                    error!("Error restoring topic {}: {}", topic_backup.name, e);
                    errors.push(format!("Topic {}: {}", topic_backup.name, e));
                    self.metrics.record_error();
                    self.telemetry.record_error(&e);

                    if !self.health.is_operational() {
                        // Every partition task of the topic has stopped:
                        // the checkpoint holds exactly the acknowledged work.
                        self.save_checkpoint(true).await?;
                        return Err(e);
                    }
                }
            }

            // Save the checkpoint after every topic
            self.save_checkpoint(false).await?;
        }
        self.save_checkpoint(true).await?;

        if let Some(snapshot) = &auto_consumer_group_snapshot {
            let mut mapping = self.offset_mapping.lock().await;
            import_auto_consumer_group_snapshot_offsets(&mut mapping, snapshot, &restore_options);
        }

        let offset_mapping = self.offset_mapping.lock().await.clone();

        // Phase 2 ends here. The engine deliberately never commits consumer
        // offsets — Phase 3 (`ThreePhaseRestore::run_offset_reset_phase`) does,
        // exactly once, from the mapping in the returned report. The `restore`
        // and `three-phase-restore` commands run it automatically when
        // `reset_consumer_offsets` / `auto_consumer_groups` is set (issue #148).
        if restore_options.reset_consumer_offsets && !restore_options.consumer_groups.is_empty() {
            info!(
                "Offset mapping ready for {} consumer group(s); offsets are applied in Phase 3 \
                 (not by the restore engine)",
                restore_options.consumer_groups.len()
            );
        }

        let total_dropped_by_filter: u64 = topic_reports
            .iter()
            .map(|t| t.records_dropped_by_filter)
            .sum();
        let total_tombstoned_by_filter: u64 = topic_reports
            .iter()
            .map(|t| t.records_tombstoned_by_filter)
            .sum();
        let report = RestoreReport {
            backup_id: self.config.backup_id.clone(),
            dry_run: false,
            start_time: 0,
            end_time: 0,
            duration_ms: 0,
            topics_restored: topic_reports,
            segments_processed: total_segments,
            records_restored: total_records,
            bytes_restored: total_bytes,
            throughput_records_per_sec: 0.0,
            throughput_bytes_per_sec: 0.0,
            errors,
            offset_mapping,
            resolved_consumer_groups: restore_options.consumer_groups.clone(),
            records_dropped_by_filter: total_dropped_by_filter,
            records_tombstoned_by_filter: total_tombstoned_by_filter,
            record_filter: restore_options
                .record_filter
                .as_ref()
                .map(|f| f.name().to_string()),
            topic_configs_skipped: topic_configs.skipped,
            topic_configs_adjusted: topic_configs.adjusted,
            idempotent_producer: producer.is_idempotent().await,
            idempotent_producer_fallback: producer.fallback_reason().map(str::to_string),
        };
        if producer.epoch_bumps() > 0 || producer.derived_batches() > 0 {
            info!(
                "Idempotent producer: {} epoch bump(s), {} batch(es) acknowledged with derived \
                 offsets",
                producer.epoch_bumps(),
                producer.derived_batches()
            );
        }

        finalize_restore_report(report)
    }

    /// Load the consumer group snapshot written by `snapshot_consumer_groups()`.
    ///
    /// Reads `{backup_id}/consumer-groups-snapshot.json`. Returns an error when the
    /// file is absent so the caller can treat it as a non-fatal warning.
    async fn load_auto_consumer_group_snapshot(&self) -> Result<AutoConsumerGroupSnapshot> {
        let key = format!("{}/consumer-groups-snapshot.json", self.config.backup_id);
        let data = self.storage.get(&key).await.map_err(|e| {
            Error::BackupNotFound(format!(
                "consumer-groups-snapshot.json not found at '{}': {}",
                key, e
            ))
        })?;

        parse_auto_consumer_group_snapshot(&data)
    }

    /// Purge target topics before restore by advancing log-start-offset to the current end.
    ///
    /// Uses the `DeleteRecords` API so the topic resource is preserved — required for
    /// Strimzi-managed topics (Issue #67 bug 10).
    async fn purge_topics_before_restore(
        &self,
        topics_to_restore: &[TopicBackup],
        restore_options: &RestoreOptions,
    ) -> Result<()> {
        let router = {
            let guard = self.router.read().await;
            guard
                .as_ref()
                .ok_or_else(|| Error::Config("Router not initialized".to_string()))?
                .clone()
        };

        info!(
            "Purging {} topics before restore (DeleteRecords to log-start-offset=end)",
            topics_to_restore.len()
        );

        for topic_backup in topics_to_restore {
            let target_name = restore_options
                .topic_mapping
                .get(&topic_backup.name)
                .cloned()
                .unwrap_or_else(|| topic_backup.name.clone());

            let partition_count = topic_backup.original_partition_count.unwrap_or_else(|| {
                topic_backup
                    .partitions
                    .iter()
                    .map(|p| p.partition_id)
                    .max()
                    .map(|m| m + 1)
                    .unwrap_or(0)
            });

            let mut to_purge: Vec<(i32, i64)> = Vec::new();
            for pid in 0..partition_count {
                // The log end (high watermark): DeleteRecords purges up to it.
                match router
                    .get_offsets(&target_name, pid, IsolationLevel::ReadUncommitted)
                    .await
                {
                    Ok((_earliest, latest)) => {
                        if latest > 0 {
                            to_purge.push((pid, latest));
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Could not get offsets for {}[{}], skipping purge: {}",
                            target_name, pid, e
                        );
                    }
                }
            }

            if to_purge.is_empty() {
                debug!("Topic {} is already empty, nothing to purge", target_name);
                continue;
            }

            router
                .delete_records(&target_name, &to_purge, 30_000)
                .await?;
            info!(
                "Purged topic {} ({} partitions)",
                target_name,
                to_purge.len()
            );
        }

        Ok(())
    }

    /// Load the backup manifest from storage
    async fn load_manifest(&self) -> Result<BackupManifest> {
        let manifest_key = format!("{}/manifest.json", self.config.backup_id);

        let data = self.storage.get(&manifest_key).await.map_err(|e| {
            self.health
                .mark_unhealthy("storage", &format!("Manifest load failed: {}", e));
            Error::BackupNotFound(format!(
                "Could not load manifest for backup '{}': {}",
                self.config.backup_id, e
            ))
        })?;

        self.health.mark_healthy("storage");

        let manifest: BackupManifest = serde_json::from_slice(&data)?;
        info!(
            "Loaded manifest: {} topics, {} total records",
            manifest.topics.len(),
            manifest.total_records()
        );

        Ok(manifest)
    }

    /// Run the Phase 1 header preflight (issue #137) unless an orchestrator
    /// already ran it (`header_preflight_external`) or nothing requires it.
    ///
    /// Returns the structured report when a preflight ran. Fails with
    /// [`Error::Preflight`] when consumer-offset recovery is requested and
    /// required tracking metadata is missing, partial, corrupt, or
    /// unprovable — before any target mutation.
    async fn run_header_preflight(
        &self,
        manifest: &BackupManifest,
        selection: &crate::config::TopicSelection,
        restore_options: &RestoreOptions,
    ) -> Result<Option<super::preflight::HeaderPreflightReport>> {
        if restore_options.header_preflight_external {
            debug!("Header preflight already performed by orchestrator; skipping engine scan");
            return Ok(None);
        }

        let mode = restore_options.header_preflight;
        let recovery = super::preflight::offset_recovery_requested(restore_options);
        let scan = super::preflight::scan_required(mode, restore_options);

        // Nothing to do: no scan wanted and no offset recovery to protect.
        if !scan && !recovery {
            return Ok(None);
        }

        info!(
            "Running Phase 1 header preflight (mode: {}, offset recovery requested: {})",
            mode, recovery
        );
        let report = super::preflight::run_header_preflight(
            self.storage.as_ref(),
            manifest,
            selection,
            restore_options,
            mode,
        )
        .await;

        for warning in &report.warnings {
            warn!("Header preflight: {}", warning);
        }

        if recovery && !report.passed {
            return Err(Error::Preflight(format!(
                "consumer-offset recovery was requested but the backup failed the tracking-\
                 metadata preflight; no topics were created and no records were produced. {}. \
                 Errors: {}",
                report.summary(),
                report.errors.join(" | ")
            )));
        }

        Ok(Some(report))
    }

    /// Filter topics based on target configuration.
    /// Supports both glob patterns (e.g., `orders-*`) and regex patterns (prefixed with `~`, e.g., `~orders-\d+`).
    fn filter_topics(
        &self,
        manifest: &BackupManifest,
        selection: &crate::config::TopicSelection,
    ) -> Result<Vec<TopicBackup>> {
        let mut result = Vec::new();

        for topic in &manifest.topics {
            // Check include patterns (glob or regex)
            let included = if selection.include.is_empty() {
                true
            } else {
                selection
                    .include
                    .iter()
                    .any(|pattern| pattern_match(pattern, &topic.name))
            };

            // Check exclude patterns (glob or regex)
            let excluded = selection
                .exclude
                .iter()
                .any(|pattern| pattern_match(pattern, &topic.name));

            if included && !excluded {
                result.push(topic.clone());
            }
        }

        Ok(result)
    }

    /// Restore a single topic
    async fn restore_topic(
        &self,
        topic_backup: &TopicBackup,
        options: &RestoreOptions,
    ) -> Result<TopicRestoreReport> {
        // Determine target topic name (may be remapped)
        let target_topic = options
            .topic_mapping
            .get(&topic_backup.name)
            .cloned()
            .unwrap_or_else(|| topic_backup.name.clone());

        let produce_path = self.produce_path(&target_topic, options).await?;

        // Repartitioning branch: delegate to fan-out module
        if let Some(repart_config) = options.repartitioning.get(&target_topic) {
            info!(
                "Repartitioning topic {} -> {} ({} source -> {} target partitions)",
                topic_backup.name,
                target_topic,
                topic_backup.partitions.len(),
                repart_config.target_partitions,
            );

            let router = self
                .router
                .read()
                .await
                .clone()
                .ok_or_else(|| Error::Config("Router not initialized".to_string()))?;

            return super::repartition::restore_topic_repartitioned(
                topic_backup,
                &target_topic,
                repart_config,
                options,
                router,
                produce_path,
                self.storage.clone(),
                Arc::clone(&self.metrics),
                Arc::clone(&self.health),
                Arc::clone(&self.kafka_circuit_breaker),
                Arc::clone(&self.storage_circuit_breaker),
                Arc::clone(&self.checkpoint),
                Arc::clone(&self.telemetry),
            )
            .await;
        }

        info!(
            "Restoring topic {} -> {} ({} partitions)",
            topic_backup.name,
            target_topic,
            topic_backup.partitions.len()
        );

        // Filter partitions based on configuration
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
                target_topic,
                partitions: Vec::new(),
                records: 0,
                bytes: 0,
                records_dropped_by_filter: 0,
                records_tombstoned_by_filter: 0,
                schema_ids_rewritten: 0,
                schema_ids_unmapped: 0,
            });
        }

        // Get the router from the RwLock
        let router = self
            .router
            .read()
            .await
            .clone()
            .ok_or_else(|| Error::Config("Router not initialized".to_string()))?;

        // Determine target partitions (may be remapped). A target partition
        // fed by several source partitions cannot attribute its high
        // watermark to one of them, so its unsaved tail is not reconciled.
        let targets: Vec<(&crate::manifest::PartitionBackup, i32)> = partitions_to_restore
            .iter()
            .map(|p| {
                let target = options
                    .partition_mapping
                    .get(&p.partition_id)
                    .copied()
                    .unwrap_or(p.partition_id);
                (*p, target)
            })
            .collect();
        let mut sources_per_target: HashMap<i32, usize> = HashMap::new();
        for (_, target) in &targets {
            *sources_per_target.entry(*target).or_default() += 1;
        }
        let checkpointing = self.checkpoint_store.read().await.is_some();
        if checkpointing {
            if sources_per_target.values().any(|n| *n > 1) {
                warn!(
                    "{}: several source partitions are mapped onto one target partition; a resume \
                     of those partitions skips by source offset only, so records acknowledged after \
                     the last checkpoint save may be produced again",
                    target_topic
                );
            }
            self.record_partition_baselines(&router, &topic_backup.name, &target_topic, &targets)
                .await?;
        }

        // Partition tasks stop (after their in-flight batch) once one fails,
        // and are all awaited before the topic returns, so the checkpoint
        // holds only acknowledged work when the error surfaces.
        let semaphore = Arc::new(Semaphore::new(options.max_concurrent_partitions));
        let cancel = tokio_util::sync::CancellationToken::new();
        let mut handles = Vec::new();
        // If this future is dropped (the run is cancelled), the partition
        // tasks stop too instead of producing on in the background.
        let mut abort_guard = AbortAllOnDrop(Vec::new());
        let schema_ids = Arc::new(super::helpers::SchemaIdCounts::default());

        for (partition_backup, target_partition) in targets.iter().copied() {
            let ctx = RestorePartitionContext {
                source_topic: topic_backup.name.clone(),
                target_topic: target_topic.clone(),
                source_partition: partition_backup.partition_id,
                target_partition,
                segments: partition_backup.segments.clone(),
                router: Arc::clone(&router),
                produce_path: produce_path.clone(),
                storage: self.storage.clone(),
                options: options.clone(),
                schema_ids: Arc::clone(&schema_ids),
                metrics: Arc::clone(&self.metrics),
                health: Arc::clone(&self.health),
                kafka_cb: Arc::clone(&self.kafka_circuit_breaker),
                storage_cb: Arc::clone(&self.storage_circuit_breaker),
                checkpoint: Arc::clone(&self.checkpoint),
                offset_mapping: Arc::clone(&self.offset_mapping),
                telemetry: Arc::clone(&self.telemetry),
                cancel: cancel.clone(),
                reconcile_tail: checkpointing && sources_per_target[&target_partition] == 1,
            };
            let semaphore = Arc::clone(&semaphore);
            let cancel = cancel.clone();

            let handle = tokio::spawn(async move {
                let _permit = semaphore.acquire_owned().await.expect("semaphore open");
                if cancel.is_cancelled() {
                    return Ok(None);
                }
                let result = ctx.restore_partition().await;
                if result.is_err() {
                    cancel.cancel();
                }
                result
            });
            abort_guard.0.push(handle.abort_handle());
            handles.push(handle);
        }

        // Wait for all partitions
        let mut partition_reports = Vec::new();
        let mut total_records = 0u64;
        let mut total_bytes = 0u64;
        let mut dropped_by_filter = 0u64;
        let mut tombstoned_by_filter = 0u64;
        let mut first_error = None;

        for handle in handles {
            let report = match handle.await {
                // Counted once, and only for a partition that restored all
                // its segments: one stopped by a failing sibling is not.
                Ok(Ok(Some(report))) => {
                    self.telemetry.partition_completed();
                    report
                }
                // Not started, or stopped after its in-flight batch, because
                // another partition failed (that one's error is returned).
                Ok(Ok(None)) => continue,
                Ok(Err(e)) => {
                    cancel.cancel();
                    first_error.get_or_insert(e);
                    continue;
                }
                Err(e) => {
                    cancel.cancel();
                    first_error.get_or_insert(Error::Io(std::io::Error::other(format!(
                        "Task join error: {}",
                        e
                    ))));
                    continue;
                }
            };

            total_records += report.records;
            total_bytes += report.bytes;
            dropped_by_filter += report.records_dropped_by_filter;
            tombstoned_by_filter += report.records_tombstoned_by_filter;
            partition_reports.push(report);
        }
        if let Some(e) = first_error {
            return Err(e);
        }

        let (schema_ids_rewritten, schema_ids_unmapped) = schema_ids.finish(&topic_backup.name);
        Ok(TopicRestoreReport {
            source_topic: topic_backup.name.clone(),
            target_topic,
            partitions: partition_reports,
            records: total_records,
            bytes: total_bytes,
            records_dropped_by_filter: dropped_by_filter,
            records_tombstoned_by_filter: tombstoned_by_filter,
            schema_ids_rewritten,
            schema_ids_unmapped,
        })
    }

    /// Record, before anything is produced, the target high watermark of
    /// every partition of the topic that has no checkpointed progress yet,
    /// and save the checkpoint: a run killed before its first periodic save
    /// can then still tell which records it already wrote.
    async fn record_partition_baselines(
        &self,
        router: &PartitionLeaderRouter,
        source_topic: &str,
        target_topic: &str,
        targets: &[(&crate::manifest::PartitionBackup, i32)],
    ) -> Result<()> {
        let missing: Vec<(i32, i32)> = {
            let guard = self.checkpoint.lock().await;
            let Some(cp) = guard.as_ref() else {
                return Ok(());
            };
            targets
                .iter()
                .filter(|(p, _)| {
                    !cp.partitions
                        .contains_key(&RestoreCheckpoint::partition_key(
                            source_topic,
                            p.partition_id,
                        ))
                })
                .map(|(p, target)| (p.partition_id, *target))
                .collect()
        };
        if missing.is_empty() {
            return Ok(());
        }
        let requests: Vec<(String, i32)> = missing
            .iter()
            .map(|(_, target)| (target_topic.to_string(), *target))
            .collect();
        let high_watermarks = router
            .batch_get_latest_offsets(&requests, IsolationLevel::ReadUncommitted)
            .await?;
        {
            let mut guard = self.checkpoint.lock().await;
            if let Some(cp) = guard.as_mut() {
                for (source_partition, target_partition) in &missing {
                    let hw = high_watermarks
                        .get(&(target_topic.to_string(), *target_partition))
                        .copied()
                        .unwrap_or(0);
                    cp.partitions.insert(
                        RestoreCheckpoint::partition_key(source_topic, *source_partition),
                        crate::manifest::PartitionRestoreProgress {
                            source_topic: source_topic.to_string(),
                            source_partition: *source_partition,
                            target_topic: target_topic.to_string(),
                            target_partition: *target_partition,
                            last_source_offset_acked: -1,
                            target_hw_after_ack: hw,
                            records_acked: 0,
                            bytes_acked: 0,
                        },
                    );
                }
            }
        }
        self.save_checkpoint(false).await
    }

    /// The produce plumbing for one target topic.
    async fn produce_path(
        &self,
        target_topic: &str,
        options: &RestoreOptions,
    ) -> Result<super::produce_path::ProducePath> {
        let router = self
            .router
            .read()
            .await
            .clone()
            .ok_or_else(|| Error::Config("Router not initialized".to_string()))?;
        let max_message_bytes = self.target_max_message_bytes(&router, target_topic).await;
        let producer = self
            .producer
            .read()
            .await
            .clone()
            .ok_or_else(|| Error::Config("Producer not started".to_string()))?;
        Ok(super::produce_path::ProducePath::new(
            producer,
            self.rate_limiter.clone(),
            max_message_bytes,
            options,
        ))
    }

    /// Start the restore's producer on the target cluster's router.
    async fn start_producer(
        &self,
        options: &RestoreOptions,
    ) -> Result<Arc<super::producer::RestoreProducer>> {
        let router = self
            .router
            .read()
            .await
            .clone()
            .ok_or_else(|| Error::Config("Router not initialized".to_string()))?;
        let producer = Arc::new(
            super::producer::RestoreProducer::start(
                router,
                super::producer::ProducerSettings::from_options(options),
                options.idempotent_producer,
            )
            .await?,
        );
        *self.producer.write().await = Some(Arc::clone(&producer));
        Ok(producer)
    }

    /// The target topic's `max.message.bytes` (DescribeConfigs, cached per
    /// topic). `None` when it cannot be read, e.g. without DescribeConfigs
    /// permission: the pre-check is then skipped and an oversized record
    /// fails at produce time with the broker's RECORD_TOO_LARGE.
    async fn target_max_message_bytes(
        &self,
        router: &PartitionLeaderRouter,
        target_topic: &str,
    ) -> Option<usize> {
        if let Some(cached) = self.max_message_bytes.lock().await.get(target_topic) {
            return *cached;
        }
        let value = match router
            .describe_configs(&[(ConfigResourceType::Topic, target_topic.to_string())])
            .await
        {
            Ok(configs) => configs
                .get(&(ConfigResourceType::Topic, target_topic.to_string()))
                .and_then(|entries| super::produce_path::max_message_bytes_from_configs(entries)),
            Err(e) => {
                warn!(
                    "Could not read max.message.bytes of target topic {}: {}; records are not \
                     size-checked before producing",
                    target_topic, e
                );
                None
            }
        };
        if let Some(max) = value {
            debug!("Target topic {} max.message.bytes = {}", target_topic, max);
        }
        self.max_message_bytes
            .lock()
            .await
            .insert(target_topic.to_string(), value);
        value
    }

    /// Set up checkpointing for this run: load the saved checkpoint (or
    /// seed a new one), wait out the in-doubt window of an interrupted
    /// previous run, and save it at once so the file exists from the start
    /// (#160). Returns whether the run resumes earlier progress.
    async fn open_checkpoint(&self, options: &RestoreOptions) -> Result<bool> {
        let expected_hash = restore_config_hash(options);
        let target_servers = self
            .config
            .target
            .as_ref()
            .map(|t| t.bootstrap_servers.clone())
            .unwrap_or_default();
        let restore_id = options.restore_id.clone().unwrap_or_else(|| {
            super::checkpoint::default_restore_id(
                &self.config.backup_id,
                &target_servers,
                &expected_hash,
            )
        });
        let Some(store) = CheckpointStore::from_options(
            options,
            Arc::clone(&self.storage),
            &self.config.backup_id,
            &restore_id,
        )
        .map(Arc::new) else {
            return Ok(false);
        };

        let mut checkpoint = match store.load().await? {
            Some(cp) if cp.config_hash == expected_hash => {
                info!(
                    "Loaded restore checkpoint from {}: {} segments completed, {} records \
                     restored",
                    store.describe(),
                    cp.segments_completed.len(),
                    cp.records_restored
                );
                let wait = super::checkpoint::in_doubt_wait(
                    &cp,
                    options,
                    chrono::Utc::now().timestamp_millis(),
                );
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
                cp
            }
            Some(_) => {
                warn!(
                    "Restore configuration changed since the checkpoint was written \
                     (config hash mismatch); restarting from the beginning"
                );
                RestoreCheckpoint::new(self.config.backup_id.clone(), expected_hash)
            }
            None => RestoreCheckpoint::new(self.config.backup_id.clone(), expected_hash),
        };
        let resuming =
            !checkpoint.segments_completed.is_empty() || !checkpoint.partitions.is_empty();
        checkpoint.restore_id = Some(restore_id);
        checkpoint.stopped_cleanly = false;
        *self.checkpoint.lock().await = Some(checkpoint);
        store.save(&self.checkpoint).await?;
        info!("Restore checkpoint: {}", store.describe());
        *self.checkpoint_store.write().await = Some(store);
        Ok(resuming)
    }

    /// Save the checkpoint every `checkpoint_interval_secs` until the
    /// returned guard is dropped.
    async fn spawn_periodic_checkpoint_saver(
        &self,
        options: &RestoreOptions,
    ) -> Option<AbortOnDrop> {
        let store = self.checkpoint_store.read().await.clone()?;
        let checkpoint = Arc::clone(&self.checkpoint);
        let interval = Duration::from_secs(options.checkpoint_interval_secs.max(1));
        Some(AbortOnDrop(tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
            loop {
                ticker.tick().await;
                if let Err(e) = store.save(&checkpoint).await {
                    warn!("Periodic restore checkpoint save failed: {}", e);
                }
            }
        })))
    }

    /// Save the checkpoint now (no-op without checkpointing). `stopped`
    /// marks that nothing is in flight any more.
    async fn save_checkpoint(&self, stopped: bool) -> Result<()> {
        let Some(store) = self.checkpoint_store.read().await.clone() else {
            return Ok(());
        };
        if stopped {
            if let Some(cp) = self.checkpoint.lock().await.as_mut() {
                cp.stopped_cleanly = true;
            }
        }
        store.save(&self.checkpoint).await
    }

    /// Write offset mapping report
    async fn write_offset_report(&self, path: &std::path::Path) -> Result<()> {
        let mapping = self.offset_mapping.lock().await;
        let json = serde_json::to_string_pretty(&*mapping)?;
        tokio::fs::write(path, json).await?;
        info!("Offset mapping report written to {:?}", path);
        Ok(())
    }

    /// Ensure all target topics exist, creating them if necessary
    async fn ensure_topics_exist(
        &self,
        topics_to_restore: &[TopicBackup],
        options: &RestoreOptions,
    ) -> Result<HashSet<String>> {
        let router = self
            .router
            .read()
            .await
            .clone()
            .ok_or_else(|| Error::Config("Router not initialized".to_string()))?;

        // Collect target topic names with their required partition counts
        let mut target_topics: std::collections::HashMap<String, i32> =
            std::collections::HashMap::new();

        for topic_backup in topics_to_restore {
            let target_name = options
                .topic_mapping
                .get(&topic_backup.name)
                .cloned()
                .unwrap_or_else(|| topic_backup.name.clone());

            // Use repartitioning target_partitions when configured,
            // otherwise use the original partition count stored at backup time,
            // falling back to max(partition_id)+1 for older backups that lack
            // the original_partition_count field (Issue #67 bug 4).
            let partition_count = if let Some(repart) = options.repartitioning.get(&target_name) {
                repart.target_partitions
            } else if let Some(count) = topic_backup.original_partition_count {
                count
            } else {
                topic_backup
                    .partitions
                    .iter()
                    .map(|p| p.partition_id)
                    .max()
                    .map(|max_id| max_id + 1)
                    .unwrap_or(1)
            };

            target_topics
                .entry(target_name)
                .and_modify(|count| *count = (*count).max(partition_count))
                .or_insert(partition_count);
        }

        // Check which topics already exist
        let existing_topics = router.fetch_metadata(None).await?;
        let existing_names: HashSet<String> =
            existing_topics.iter().map(|t| t.name.clone()).collect();

        // Find missing topics
        let missing_topics: Vec<TopicToCreate> = target_topics
            .iter()
            .filter(|(name, _)| !existing_names.contains(*name))
            .map(|(name, &partition_count)| {
                let replication_factor = super::topic_configs::default_replication_factor(
                    options.default_replication_factor,
                    options.target_profile,
                );
                TopicToCreate {
                    name: name.clone(),
                    num_partitions: partition_count,
                    replication_factor,
                }
            })
            .collect();

        if missing_topics.is_empty() {
            debug!("All target topics already exist");
            return Ok(HashSet::new());
        }

        let missing_names: HashSet<String> = missing_topics
            .iter()
            .map(|topic| topic.name.clone())
            .collect();

        info!(
            "Creating {} missing topics: {:?}",
            missing_topics.len(),
            missing_topics.iter().map(|t| &t.name).collect::<Vec<_>>()
        );

        // Create the missing topics
        let results = router.create_topics(missing_topics.clone(), 60000).await?;

        // Log results
        for result in &results {
            if result.is_success() {
                info!("Created topic '{}' successfully", result.name);
            } else if result.already_exists() {
                debug!("Topic '{}' already exists (race condition)", result.name);
            }
        }

        // Wait for topic metadata to propagate
        self.wait_for_topics_ready(&router, &missing_topics).await?;

        // Refresh router metadata to pick up new topics
        router.refresh_metadata().await?;
        info!("Refreshed metadata after topic creation");

        Ok(missing_names)
    }

    /// Restore the captured topic configuration (plus `topic_config_overrides`)
    /// to the topics that get it: newly created ones, and existing ones under
    /// `existing_topic_config_policy: apply`. What the target does not take
    /// is handled by `unsupported_config_policy` (see `restore::topic_configs`).
    async fn restore_topic_configs(
        &self,
        topics_to_restore: &[TopicBackup],
        options: &RestoreOptions,
        newly_created: &HashSet<String>,
    ) -> Result<super::topic_configs::TopicConfigOutcome> {
        let with_configs: Vec<_> = topics_to_restore
            .iter()
            .filter(|topic| !topic.configurations.is_empty())
            .collect();
        if with_configs.is_empty() {
            return Ok(super::topic_configs::TopicConfigOutcome::default());
        }

        let router = self
            .router
            .read()
            .await
            .clone()
            .ok_or_else(|| Error::Config("Router not initialized".to_string()))?;
        let target_name = |topic: &TopicBackup| {
            options
                .topic_mapping
                .get(&topic.name)
                .cloned()
                .unwrap_or_else(|| topic.name.clone())
        };
        let resources: Vec<_> = with_configs
            .iter()
            .map(|topic| (ConfigResourceType::Topic, target_name(topic)))
            .collect();
        let current = router.describe_configs(&resources).await?;
        let mut outcome = super::topic_configs::TopicConfigOutcome::default();
        let mut plans = Vec::new();
        let mut fail_drift = Vec::new();

        for topic in with_configs {
            let target = target_name(topic);
            let current_entries = current
                .get(&(ConfigResourceType::Topic, target.clone()))
                .ok_or_else(|| {
                    Error::Kafka(crate::error::KafkaError::Protocol(format!(
                        "DescribeConfigs returned no result for target topic {target}"
                    )))
                })?;
            let current_values: HashMap<_, _> = current_entries
                .iter()
                .filter_map(|entry| {
                    entry
                        .value
                        .as_ref()
                        .map(|value| (entry.name.as_str(), value.as_str()))
                })
                .collect();
            let mut desired = topic.configurations.clone();
            if let Some(overrides) = options.topic_config_overrides.get(&target) {
                desired.extend(overrides.clone());
            }
            // Keys the target does not list as writable cannot be restored
            // (nor drift); values outside the profile limits are compared and
            // applied as clamped.
            let (mut desired, unsupported) =
                super::topic_configs::prefilter(&target, desired, current_entries);
            let adjusted = super::topic_configs::apply_profile(
                &target,
                &mut desired,
                options.target_profile,
                options.target_cluster_type,
            );
            let drift: Vec<String> = desired
                .iter()
                .filter(|(name, value)| {
                    current_values.get(name.as_str()).copied() != Some(value.as_str())
                })
                .map(|(name, value)| {
                    format!(
                        "{name}: target={:?}, backup={value}",
                        current_values.get(name.as_str())
                    )
                })
                .collect();

            let should_apply = newly_created.contains(&target)
                || options.existing_topic_config_policy == ExistingTopicConfigPolicy::Apply;
            if should_apply {
                outcome.skipped.extend(unsupported);
                outcome.adjusted.extend(adjusted);
                if !drift.is_empty() {
                    plans.push((target, desired));
                }
            } else if drift.is_empty() {
                continue;
            } else if options.existing_topic_config_policy == ExistingTopicConfigPolicy::Fail {
                fail_drift.push(format!("{target}: {}", drift.join(", ")));
            } else {
                warn!(
                    topic = %target,
                    drift = %drift.join(", "),
                    "Existing target topic configuration differs from backup; validate_only policy left it unchanged"
                );
            }
        }

        if !fail_drift.is_empty() {
            return Err(Error::Config(format!(
                "Target topic configuration drift: {}",
                fail_drift.join("; ")
            )));
        }
        let policy = options.unsupported_config_policy;
        if policy == UnsupportedConfigPolicy::Fail {
            if !outcome.skipped.is_empty() {
                return Err(super::topic_configs::fail_error(&outcome.skipped));
            }
            if !outcome.adjusted.is_empty() {
                return Err(super::topic_configs::limits_error(&outcome.adjusted));
            }
        }
        let rejected =
            super::topic_configs::apply_topic_configs(router.as_ref(), plans, policy).await?;
        outcome.skipped.extend(rejected);
        outcome.log();
        Ok(outcome)
    }

    /// Wait for newly created topics to be ready (partitions available)
    async fn wait_for_topics_ready(
        &self,
        router: &Arc<PartitionLeaderRouter>,
        topics: &[TopicToCreate],
    ) -> Result<()> {
        let max_retries = 30; // 30 seconds max wait
        let retry_delay = Duration::from_secs(1);

        for topic in topics {
            let mut ready = false;
            for attempt in 1..=max_retries {
                match router.get_topic_metadata(&topic.name).await {
                    Ok(metadata) => {
                        if metadata.partitions.len() >= topic.num_partitions as usize {
                            debug!(
                                "Topic '{}' is ready with {} partitions",
                                topic.name,
                                metadata.partitions.len()
                            );
                            ready = true;
                            break;
                        }
                        debug!(
                            "Topic '{}' has {} partitions, waiting for {} (attempt {}/{})",
                            topic.name,
                            metadata.partitions.len(),
                            topic.num_partitions,
                            attempt,
                            max_retries
                        );
                    }
                    Err(e) => {
                        debug!(
                            "Topic '{}' not yet available: {} (attempt {}/{})",
                            topic.name, e, attempt, max_retries
                        );
                    }
                }
                tokio::time::sleep(retry_delay).await;
            }

            if !ready {
                return Err(Error::Kafka(crate::error::KafkaError::Timeout(format!(
                    "Timeout waiting for topic '{}' to be ready",
                    topic.name
                ))));
            }
        }

        Ok(())
    }
}

/// Context for restoring a single partition
struct RestorePartitionContext {
    source_topic: String,
    target_topic: String,
    source_partition: i32,
    target_partition: i32,
    segments: Vec<SegmentMetadata>,
    router: Arc<PartitionLeaderRouter>,
    produce_path: super::produce_path::ProducePath,
    storage: Arc<dyn StorageBackend>,
    options: RestoreOptions,
    /// The topic's schema ID rewrite counts, shared by its partitions.
    schema_ids: Arc<super::helpers::SchemaIdCounts>,
    metrics: Arc<PerformanceMetrics>,
    health: Arc<HealthCheck>,
    kafka_cb: Arc<CircuitBreaker>,
    storage_cb: Arc<CircuitBreaker>,
    checkpoint: Arc<Mutex<Option<RestoreCheckpoint>>>,
    offset_mapping: Arc<Mutex<OffsetMapping>>,
    telemetry: Arc<RestoreTelemetry>,
    /// Set when a sibling partition failed: stop after the batch in flight.
    cancel: tokio_util::sync::CancellationToken,
    /// Reconcile the unsaved tail against the target's high watermark on
    /// resume (off when checkpointing is off or the target partition is
    /// shared with another source partition).
    reconcile_tail: bool,
}

impl RestorePartitionContext {
    /// Restore one partition. `None` when it stopped early because another
    /// partition of the topic failed.
    async fn restore_partition(self) -> Result<Option<PartitionRestoreReport>> {
        debug!(
            "Restoring partition {}:{} -> {}:{}",
            self.source_topic, self.source_partition, self.target_topic, self.target_partition
        );

        // Filter segments by time window
        let segments = self.filter_segments_by_time();

        if segments.is_empty() {
            debug!(
                "{}:{}: no segments match time window",
                self.source_topic, self.source_partition
            );
            return Ok(Some(PartitionRestoreReport {
                source_partition: self.source_partition,
                target_partition: self.target_partition,
                segments_processed: 0,
                records: 0,
                bytes: 0,
                first_offset: 0,
                last_offset: 0,
                first_timestamp: 0,
                last_timestamp: 0,
                records_dropped_by_filter: 0,
                records_tombstoned_by_filter: 0,
            }));
        }

        // Check which segments are already completed, and the partition's
        // acknowledged progress (for resume)
        let progress_key =
            RestoreCheckpoint::partition_key(&self.source_topic, self.source_partition);
        let (completed_segments, progress): (HashSet<String>, _) = {
            let checkpoint = self.checkpoint.lock().await;
            checkpoint
                .as_ref()
                .map(|cp| {
                    (
                        cp.segments_completed.iter().cloned().collect(),
                        cp.partitions.get(&progress_key).cloned(),
                    )
                })
                .unwrap_or_default()
        };
        // Without recorded progress (checkpointing off) there is no high
        // watermark to reconcile against.
        let reconcile_tail = self.reconcile_tail && progress.is_some();
        let progress = progress.unwrap_or_else(|| crate::manifest::PartitionRestoreProgress {
            source_topic: self.source_topic.clone(),
            source_partition: self.source_partition,
            target_topic: self.target_topic.clone(),
            target_partition: self.target_partition,
            last_source_offset_acked: -1,
            target_hw_after_ack: 0,
            records_acked: 0,
            bytes_acked: 0,
        });
        // Records at or below this source offset were acknowledged by an
        // earlier run.
        let resume_after = progress.last_source_offset_acked;
        // Records acknowledged after the last checkpoint save: the target's
        // high watermark beyond the recorded one. Verified and skipped.
        let mut tail = if reconcile_tail {
            let (_, high_watermark) = self
                .router
                .get_offsets(
                    &self.target_topic,
                    self.target_partition,
                    IsolationLevel::ReadUncommitted,
                )
                .await?;
            match super::checkpoint::unsaved_tail(&progress, high_watermark)? {
                0 => None,
                n => Some(super::checkpoint::TailVerifier::new(
                    progress.clone(),
                    Arc::clone(&self.router),
                    n,
                )),
            }
        } else {
            None
        };

        let mut total_records = 0u64;
        let mut total_bytes = 0u64;
        let mut segments_processed = 0u64;
        let mut records_dropped_by_filter = 0u64;
        let mut records_tombstoned_by_filter = 0u64;
        let mut first_offset = i64::MAX;
        let mut last_offset = i64::MIN;
        let mut first_timestamp = i64::MAX;
        let mut last_timestamp = i64::MIN;

        // Set when the partition stops before its last segment because a
        // sibling partition failed.
        let mut stopped = false;

        // Process each segment, in source-offset order
        let mut overlap = super::helpers::OverlapGuard::default();
        for segment in super::helpers::segments_in_offset_order(segments) {
            // Skip if already completed
            if completed_segments.contains(&segment.key) {
                debug!("Skipping completed segment: {}", segment.key);
                continue;
            }

            if self.cancel.is_cancelled() {
                stopped = true;
                break;
            }

            let mut records = match self.read_segment(segment).await {
                Ok(r) => {
                    self.storage_cb.record_success();
                    self.health.mark_healthy("storage");
                    self.telemetry.segment_processed();
                    r
                }
                Err(e) => {
                    self.storage_cb.record_failure();
                    self.health
                        .mark_degraded("storage", &format!("Read error: {}", e));
                    return Err(e);
                }
            };

            // Resume inside a segment: skip what an earlier run had
            // acknowledged.
            if resume_after >= 0 {
                let before = records.len();
                records.retain(|r| r.offset > resume_after);
                if records.len() < before {
                    info!(
                        "{}:{}: resuming after source offset {} ({} record(s) of {} already \
                         restored)",
                        self.source_topic,
                        self.source_partition,
                        resume_after,
                        before - records.len(),
                        segment.key
                    );
                }
            }

            // A legacy manifest can list overlapping segments: never
            // produce a source offset twice.
            let overlapping = overlap.skip_restored(&mut records);
            if overlapping > 0 {
                warn!(
                    "{}:{}: skipped {} record(s) in {} already restored from an overlapping \
                     segment",
                    self.source_topic, self.source_partition, overlapping, segment.key
                );
            }

            // Filter records by time window
            let filtered_records = self.filter_records_by_time(records);

            // Programmatic record filter (Keep / Drop / Tombstone), if set.
            let (filtered_records, filter_outcome) = match &self.options.record_filter {
                Some(handle) => {
                    super::filter::apply_record_filter(&self.source_topic, filtered_records, handle)
                }
                None => (filtered_records, super::filter::FilterOutcome::default()),
            };
            records_dropped_by_filter += filter_outcome.dropped;
            records_tombstoned_by_filter += filter_outcome.tombstoned;

            if filtered_records.is_empty() {
                // Mark segment as completed even if no records match
                self.mark_segment_completed(&segment.key).await;
                continue;
            }

            // Track offset range locally, then update mapping once per segment
            // (avoids holding the shared mutex once per record — issue #197).
            // The segment extremes are found by offset value, not by position,
            // so this does not depend on the records being offset-ordered; the
            // timestamps passed are the segment's earliest / latest so the
            // range's timestamp bounds match what the per-record path produced.
            let mut segment_min_offset = i64::MAX;
            let mut segment_max_offset = i64::MIN;
            let mut segment_min_ts = i64::MAX;
            let mut segment_max_ts = i64::MIN;
            for record in &filtered_records {
                first_offset = first_offset.min(record.offset);
                last_offset = last_offset.max(record.offset);
                first_timestamp = first_timestamp.min(record.timestamp);
                last_timestamp = last_timestamp.max(record.timestamp);
                segment_min_offset = segment_min_offset.min(record.offset);
                segment_max_offset = segment_max_offset.max(record.offset);
                segment_min_ts = segment_min_ts.min(record.timestamp);
                segment_max_ts = segment_max_ts.max(record.timestamp);
            }
            {
                // filtered_records is non-empty here, so all extremes are set.
                let mut mapping = self.offset_mapping.lock().await;
                mapping.update_range(
                    &self.target_topic,
                    self.target_partition,
                    segment_min_offset,
                    None,
                    segment_min_ts,
                );
                if segment_max_offset != segment_min_offset || segment_max_ts != segment_min_ts {
                    mapping.update_range(
                        &self.target_topic,
                        self.target_partition,
                        segment_max_offset,
                        None,
                        segment_max_ts,
                    );
                }
            }

            // Drop the archive's kafka-backup headers if asked to, then add
            // fresh original-offset headers if configured (header-based strategy)
            let stripped_records =
                super::helpers::strip_offset_headers(filtered_records, &self.options);
            let mut records_to_produce = super::helpers::inject_offset_headers(
                stripped_records,
                self.source_partition,
                &self.options,
            );
            super::helpers::rewrite_schema_ids(
                &mut records_to_produce,
                &self.source_topic,
                self.source_partition,
                &self.options,
                &self.schema_ids,
            )?;

            // Records the interrupted run wrote after its last checkpoint
            // save: verified against the target, mapped and acknowledged
            // without producing them again.
            if let Some(verifier) = tail.as_mut().filter(|t| t.remaining() > 0) {
                let (written, base) = verifier.match_written(&records_to_produce).await?;
                if written > 0 {
                    let already: Vec<BackupRecord> = records_to_produce.drain(..written).collect();
                    self.acknowledge(&progress, &already, &[(base, written)])
                        .await;
                    total_records += written as u64;
                    total_bytes += record_bytes(&already);
                }
            }
            if records_to_produce.is_empty() {
                segments_processed += 1;
                self.mark_segment_completed(&segment.key).await;
                continue;
            }

            // Track bytes
            let batch_bytes: u64 = record_bytes(&records_to_produce);

            // A record larger than the target topic's max.message.bytes
            // fails the partition before anything of this segment is sent.
            self.produce_path.check_record_sizes(
                &records_to_produce,
                super::produce_path::RecordOrigin {
                    source_topic: &self.source_topic,
                    source_partition: self.source_partition,
                },
                &self.target_topic,
                self.target_partition,
            )?;

            // Produce records in batches capped by record count and encoded
            // bytes. Records the filter dropped are not
            // mapped: `OffsetMapping::resolve_target_offset` resolves a
            // committed offset that points at one to the next restored
            // record, as it does for any other gap.
            let mut cancelled = false;
            for range in self.produce_path.plan(&records_to_produce) {
                if self.cancel.is_cancelled() {
                    cancelled = true;
                    break;
                }
                let batch = &records_to_produce[range];

                // Produce (routed to the partition leader, after the
                // restore-wide rate limit).
                match self
                    .produce_path
                    .produce(&self.target_topic, self.target_partition, batch)
                    .await
                {
                    Ok(produced) => {
                        self.kafka_cb.record_success();
                        self.health.mark_healthy("kafka");

                        // Capture offset mapping (Phase 2: detailed offset mapping)
                        // Use per-sub-batch offsets for correct mapping when records
                        // were split across multiple produce requests.
                        // Collect pairs locally then take the mapping lock once
                        // per produce batch (issue #197).
                        let mut batch_pairs: Vec<crate::manifest::OffsetPair> =
                            Vec::with_capacity(batch.len());
                        let mut record_idx = 0;
                        // A batch acknowledged through DUPLICATE_SEQUENCE_NUMBER
                        // or reconciliation has derived offsets: the end of the
                        // partition's previous batch, exact while this restore
                        // is the partition's only writer.
                        for (sub_base_offset, sub_count) in &produced.sub_batches {
                            for j in 0..*sub_count {
                                let record = &batch[record_idx];
                                let target_offset = sub_base_offset + j as i64;

                                // Extract source offset from header (if available) or use record offset
                                let source_offset = self.extract_source_offset(record);

                                batch_pairs.push(crate::manifest::OffsetPair {
                                    source_offset,
                                    target_offset,
                                    timestamp: record.timestamp,
                                });
                                record_idx += 1;
                            }
                        }
                        debug_assert_eq!(
                            record_idx,
                            batch.len(),
                            "sub_batch_offsets total count ({}) != batch length ({})",
                            record_idx,
                            batch.len()
                        );
                        self.offset_mapping.lock().await.add_detailed_batch(
                            &self.target_topic,
                            self.target_partition,
                            batch_pairs,
                        );
                        self.record_ack(&progress, batch, &produced.sub_batches)
                            .await;
                    }
                    Err(e) => {
                        self.kafka_cb.record_failure();
                        self.health
                            .mark_degraded("kafka", &format!("Produce error: {}", e));
                        return Err(e);
                    }
                }

                total_records += batch.len() as u64;
                self.metrics.record_records(batch.len() as u64);
                self.health.record_records(batch.len() as u64);
                self.telemetry
                    .record_produced(&self.target_topic, self.target_partition, batch);
            }

            if cancelled {
                // The segment stays incomplete; the checkpoint has the
                // acknowledged part of it.
                stopped = true;
                break;
            }

            total_bytes += batch_bytes;
            segments_processed += 1;

            // Mark segment as completed: all its records are acknowledged
            self.mark_segment_completed(&segment.key).await;
        }

        if self.cancel.is_cancelled() {
            debug!(
                "{}:{}: stopped because another partition of the topic failed",
                self.source_topic, self.source_partition
            );
        } else if let Some(verifier) = &tail {
            verifier.finish()?;
        }
        if stopped {
            return Ok(None);
        }

        info!(
            "Restored {}:{} -> {}:{} ({} records, {} segments)",
            self.source_topic,
            self.source_partition,
            self.target_topic,
            self.target_partition,
            total_records,
            segments_processed
        );

        Ok(Some(PartitionRestoreReport {
            source_partition: self.source_partition,
            target_partition: self.target_partition,
            segments_processed,
            records: total_records,
            bytes: total_bytes,
            records_dropped_by_filter,
            records_tombstoned_by_filter,
            first_offset: if first_offset == i64::MAX {
                0
            } else {
                first_offset
            },
            last_offset: if last_offset == i64::MIN {
                0
            } else {
                last_offset
            },
            first_timestamp: if first_timestamp == i64::MAX {
                0
            } else {
                first_timestamp
            },
            last_timestamp: if last_timestamp == i64::MIN {
                0
            } else {
                last_timestamp
            },
        }))
    }

    async fn mark_segment_completed(&self, key: &str) {
        super::helpers::mark_segment_completed(&self.checkpoint, key).await;
    }

    /// Record an acknowledged batch in the checkpoint: its last source
    /// offset and the target partition's high watermark after it.
    async fn record_ack(
        &self,
        progress: &crate::manifest::PartitionRestoreProgress,
        batch: &[BackupRecord],
        sub_batches: &[(i64, usize)],
    ) {
        let (Some(last_source), Some((base, count))) =
            (batch.iter().map(|r| r.offset).max(), sub_batches.last())
        else {
            return;
        };
        if let Some(cp) = self.checkpoint.lock().await.as_mut() {
            cp.record_ack(
                progress,
                last_source,
                base + *count as i64,
                batch.len() as u64,
                record_bytes(batch),
            );
        }
    }

    /// Map and acknowledge records found already written to the target by
    /// an interrupted run.
    async fn acknowledge(
        &self,
        progress: &crate::manifest::PartitionRestoreProgress,
        records: &[BackupRecord],
        sub_batches: &[(i64, usize)],
    ) {
        let (base, _) = sub_batches[0];
        let pairs = records
            .iter()
            .enumerate()
            .map(|(i, r)| crate::manifest::OffsetPair {
                source_offset: self.extract_source_offset(r),
                target_offset: base + i as i64,
                timestamp: r.timestamp,
            })
            .collect();
        self.offset_mapping.lock().await.add_detailed_batch(
            &self.target_topic,
            self.target_partition,
            pairs,
        );
        self.record_ack(progress, records, sub_batches).await;
        self.metrics.record_records(records.len() as u64);
        // Counted like produced records, as the restore report counts them
        // (they are never produced again, so never counted twice).
        self.telemetry
            .record_produced(&self.target_topic, self.target_partition, records);
    }

    fn filter_segments_by_time(&self) -> Vec<&SegmentMetadata> {
        self.segments
            .iter()
            .filter(|s| {
                s.overlaps_time_window(self.options.time_window_start, self.options.time_window_end)
            })
            .collect()
    }

    async fn read_segment(&self, segment: &SegmentMetadata) -> Result<Vec<BackupRecord>> {
        super::helpers::read_segment(self.storage.as_ref(), segment).await
    }

    fn filter_records_by_time(&self, records: Vec<BackupRecord>) -> Vec<BackupRecord> {
        super::helpers::filter_records_by_time(records, &self.options)
    }

    /// Extract source offset from record headers or fall back to record.offset.
    fn extract_source_offset(&self, record: &BackupRecord) -> i64 {
        record
            .headers
            .iter()
            .filter(|h| h.key == crate::offset_headers::X_ORIGINAL_OFFSET)
            .find_map(|h| decode_i64_header(h.value.as_deref()))
            .unwrap_or(record.offset)
    }

    /// Extract source timestamp from record headers or fall back to record.timestamp.
    #[allow(dead_code)]
    fn extract_source_timestamp(&self, record: &BackupRecord) -> i64 {
        record
            .headers
            .iter()
            .filter(|h| h.key == crate::offset_headers::X_ORIGINAL_TIMESTAMP)
            .find_map(|h| decode_i64_header(h.value.as_deref()))
            .unwrap_or(record.timestamp)
    }
}

/// Key plus value bytes of `records`, as the restore report counts them.
fn record_bytes(records: &[BackupRecord]) -> u64 {
    records
        .iter()
        .map(|r| {
            r.key.as_ref().map(|k| k.len()).unwrap_or(0) as u64
                + r.value.as_ref().map(|v| v.len()).unwrap_or(0) as u64
        })
        .sum()
}

/// Aborts tasks still running when dropped.
struct AbortAllOnDrop(Vec<tokio::task::AbortHandle>);

impl Drop for AbortAllOnDrop {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

/// Aborts a background task when dropped.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Decode an `x-original-*` header value.
///
/// The binary little-endian i64 the backup engine writes (8 bytes) is tried
/// first; a decimal string is accepted for backwards compatibility. A null
/// header value (`None`) decodes to `None` so callers fall back to the
/// record's own offset/timestamp instead of misreading it as 0.
fn decode_i64_header(value: Option<&[u8]>) -> Option<i64> {
    let value = value?;
    if let Ok(bytes) = <[u8; 8]>::try_from(value) {
        return Some(i64::from_le_bytes(bytes));
    }
    std::str::from_utf8(value).ok()?.parse().ok()
}

/// Pattern matching for topic names.
/// Supports:
/// - Glob patterns: `orders-*`, `?-topic`, `*-test-*`
/// - Regex patterns (prefixed with `~`): `~orders-\d+`, `~^test-.*$`
pub(crate) fn pattern_match(pattern: &str, text: &str) -> bool {
    if let Some(regex_pattern) = pattern.strip_prefix('~') {
        // Regex pattern
        match regex::Regex::new(regex_pattern) {
            Ok(re) => re.is_match(text),
            Err(_) => false,
        }
    } else {
        // Glob pattern
        glob_match(pattern, text)
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

/// Hash the restore options (and any record-filter fingerprint) for the
/// resume checkpoint. The serde representation skips the programmatic filter
/// handle itself, so the fingerprint is folded in explicitly.
fn restore_config_hash(options: &RestoreOptions) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(options).unwrap_or_default());
    if let Some(fingerprint) = &options.record_filter_fingerprint {
        hasher.update(fingerprint.as_bytes());
    }
    let digest = hasher.finalize();
    digest.iter().map(|b| format!("{:02x}", b)).collect()
}

fn finalize_restore_report(report: RestoreReport) -> Result<RestoreReport> {
    if !report.errors.is_empty() {
        let error_count = report.errors.len();
        let joined = report.errors.join("; ");
        return Err(Error::Config(format!(
            "Restore completed with {error_count} error(s): {joined}"
        )));
    }

    info!("Restore completed successfully");
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::restore::offset_reset::{OffsetResetExecutor, OffsetResetStrategy};
    use std::collections::HashMap;

    #[test]
    fn decode_i64_header_handles_binary_string_and_null() {
        assert_eq!(
            decode_i64_header(Some(&12345i64.to_le_bytes())),
            Some(12345)
        );
        assert_eq!(decode_i64_header(Some(b"12345")), Some(12345));
        assert_eq!(decode_i64_header(Some(b"-7")), Some(-7));
        assert_eq!(decode_i64_header(Some(b"")), None);
        assert_eq!(decode_i64_header(Some(b"not-a-number")), None);
        // Issue #155: a null header value is not 0 and must not panic.
        assert_eq!(decode_i64_header(None), None);
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("test*", "testing"));
        assert!(glob_match("*test", "mytest"));
        assert!(glob_match("*test*", "mytesting"));
        assert!(glob_match("test?", "test1"));
        assert!(glob_match("te?t", "test"));
        assert!(!glob_match("test", "testing"));
        assert!(!glob_match("test?", "test"));
        assert!(glob_match("orders-*", "orders-v1"));
        assert!(glob_match("orders-*", "orders-"));
        assert!(!glob_match("orders-*", "orders"));
    }

    #[test]
    fn test_pattern_match_glob() {
        // Glob patterns (no prefix)
        assert!(pattern_match("*", "anything"));
        assert!(pattern_match("orders-*", "orders-v1"));
        assert!(pattern_match("*-topic", "my-topic"));
        assert!(!pattern_match("orders-*", "payments-v1"));
    }

    #[test]
    fn test_pattern_match_regex() {
        // Regex patterns (prefixed with ~)
        assert!(pattern_match("~orders-\\d+", "orders-123"));
        assert!(pattern_match("~^test-.*$", "test-anything"));
        assert!(pattern_match("~(orders|payments)-.*", "orders-v1"));
        assert!(pattern_match("~(orders|payments)-.*", "payments-v2"));
        assert!(!pattern_match("~orders-\\d+", "orders-abc"));
        assert!(!pattern_match("~^test-.*$", "prefix-test-anything"));
    }

    #[test]
    fn test_pattern_match_invalid_regex() {
        // Invalid regex should return false
        assert!(!pattern_match("~[invalid", "test"));
        assert!(!pattern_match("~(unclosed", "test"));
    }

    #[test]
    fn test_pattern_match_exact() {
        // Exact match (no wildcards)
        assert!(pattern_match("orders", "orders"));
        assert!(!pattern_match("orders", "orders-v1"));
        assert!(pattern_match("~^orders$", "orders"));
        assert!(!pattern_match("~^orders$", "orders-v1"));
    }

    #[test]
    fn restore_report_with_errors_is_failure() {
        let report = RestoreReport {
            backup_id: "restore-error-regression".to_string(),
            dry_run: false,
            start_time: 0,
            end_time: 0,
            duration_ms: 0,
            topics_restored: Vec::new(),
            segments_processed: 0,
            records_restored: 0,
            bytes_restored: 0,
            throughput_records_per_sec: 0.0,
            throughput_bytes_per_sec: 0.0,
            errors: vec!["Topic orders: Produce error for orders:5: code 6".to_string()],
            offset_mapping: OffsetMapping::new(),
            resolved_consumer_groups: Vec::new(),
            records_dropped_by_filter: 0,
            records_tombstoned_by_filter: 0,
            record_filter: None,
            topic_configs_skipped: Vec::new(),
            topic_configs_adjusted: Vec::new(),
            idempotent_producer: false,
            idempotent_producer_fallback: None,
        };

        let err = finalize_restore_report(report).expect_err(
            "restore reports containing partition/topic errors must fail the restore run",
        );
        assert!(
            err.to_string().contains("Restore completed with 1 error"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn auto_consumer_group_snapshot_offsets_feed_reset_plan() {
        let snapshot = parse_auto_consumer_group_snapshot(
            br#"{
                "snapshot_time": 1778044734905,
                "groups": [
                    {
                        "group_id": "issue51-group",
                        "offsets": {
                            "issue51-topic": {
                                "0": 10,
                                "2": 10,
                                "1": 10
                            }
                        }
                    }
                ]
            }"#,
        )
        .expect("snapshot should parse");

        let mut restore_options = RestoreOptions::default();
        restore_options.topic_mapping.insert(
            "issue51-topic".to_string(),
            "issue51-restored-topic".to_string(),
        );
        restore_options.partition_mapping.insert(2, 5);

        let mut mapping = OffsetMapping::new();
        for source_offset in 0..10 {
            mapping.add_detailed(
                "issue51-restored-topic",
                0,
                source_offset,
                100 + source_offset,
                1778044700000 + source_offset,
            );
            mapping.add_detailed(
                "issue51-restored-topic",
                1,
                source_offset,
                200 + source_offset,
                1778044700000 + source_offset,
            );
            mapping.add_detailed(
                "issue51-restored-topic",
                5,
                source_offset,
                500 + source_offset,
                1778044700000 + source_offset,
            );
        }

        let group_ids =
            import_auto_consumer_group_snapshot_offsets(&mut mapping, &snapshot, &restore_options);

        assert_eq!(group_ids, vec!["issue51-group".to_string()]);

        let executor = OffsetResetExecutor::new_offline(vec!["target:9092".to_string()]);
        let plan = executor
            .generate_plan(&mapping, &group_ids, OffsetResetStrategy::Auto)
            .await
            .expect("reset plan should be generated from snapshot offsets");

        assert_eq!(plan.groups.len(), 1);
        let group_plan = &plan.groups[0];
        assert!(group_plan.complete);
        assert_eq!(group_plan.partition_count, 3);

        let planned_offsets: HashMap<i32, i64> = group_plan
            .partitions
            .iter()
            .map(|partition| (partition.partition, partition.target_offset))
            .collect();

        assert_eq!(planned_offsets.get(&0), Some(&110));
        assert_eq!(planned_offsets.get(&1), Some(&210));
        assert_eq!(planned_offsets.get(&5), Some(&510));
    }
}
