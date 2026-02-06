//! Restore engine orchestration.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex, Semaphore};
use tracing::{debug, error, info, warn};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::compression::{decompress, detect_from_extension};
use crate::config::{Config, Mode, OffsetStrategy, RestoreOptions};
use crate::health::HealthCheck;
use crate::kafka::{PartitionLeaderRouter, TopicToCreate};
use crate::manifest::{
    BackupManifest, BackupRecord, DryRunPartitionReport, DryRunReport, DryRunTopicReport,
    OffsetMapping, PartitionRestoreReport, RecordHeader, RestoreCheckpoint, RestoreReport,
    SegmentMetadata, TopicBackup, TopicRestoreReport,
};
use crate::metrics::PerformanceMetrics;
use crate::segment::format::MAGIC_BYTES;
use crate::segment::SegmentReader;
use crate::storage::{create_backend, StorageBackend};
use crate::{Error, Result};

/// Progress update sent during restore
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
    progress_tx: broadcast::Sender<RestoreProgress>,
    target_config: Option<crate::config::KafkaConfig>,
}

impl RestoreEngine {
    /// Create a new restore engine
    pub fn new(config: Config) -> Result<Self> {
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

        let (shutdown_tx, _) = broadcast::channel(1);
        let (progress_tx, _) = broadcast::channel(100);

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
            progress_tx,
            target_config: Some(target_config),
        })
    }

    /// Subscribe to progress updates
    pub fn progress_receiver(&self) -> broadcast::Receiver<RestoreProgress> {
        self.progress_tx.subscribe()
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

        let start_time = Instant::now();
        let start_timestamp = chrono::Utc::now().timestamp_millis();

        let result = self.run_internal().await;

        self.health.job_completed();

        let end_timestamp = chrono::Utc::now().timestamp_millis();
        let duration_ms = start_time.elapsed().as_millis() as u64;

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
        };

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

            let mut topic_report = DryRunTopicReport {
                source_topic: topic_backup.name.clone(),
                target_topic,
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
                    continue;
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
        let restore_options = self.config.restore.clone().unwrap_or_default();

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
            });
        }

        // Load manifest
        info!("Loading backup manifest for: {}", self.config.backup_id);
        let manifest = self.load_manifest().await?;

        // Load checkpoint if resuming
        if let Some(checkpoint_path) = &restore_options.checkpoint_state {
            if checkpoint_path.exists() {
                self.load_checkpoint(checkpoint_path).await?;
            }
        }

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
            });
        }

        // Auto-create topics if configured
        if restore_options.create_topics {
            self.ensure_topics_exist(&topics_to_restore, &restore_options)
                .await?;
        }

        info!("Restoring {} topics", topics_to_restore.len());

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
                }
                Err(e) => {
                    error!("Error restoring topic {}: {}", topic_backup.name, e);
                    errors.push(format!("Topic {}: {}", topic_backup.name, e));
                    self.metrics.record_error();

                    if !self.health.is_operational() {
                        return Err(e);
                    }
                }
            }

            // Save checkpoint periodically
            if let Some(checkpoint_path) = &restore_options.checkpoint_state {
                self.save_checkpoint(checkpoint_path).await?;
            }
        }

        let offset_mapping = self.offset_mapping.lock().await.clone();

        info!("Restore completed successfully");

        Ok(RestoreReport {
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
        })
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
            });
        }

        // Create semaphore to limit concurrent partition restores
        let semaphore = Arc::new(Semaphore::new(options.max_concurrent_partitions));

        // Restore each partition concurrently
        let mut handles = Vec::new();

        for partition_backup in partitions_to_restore {
            let permit = semaphore.clone().acquire_owned().await.unwrap();

            // Determine target partition (may be remapped)
            let target_partition = options
                .partition_mapping
                .get(&partition_backup.partition_id)
                .copied()
                .unwrap_or(partition_backup.partition_id);

            // Get the router from the RwLock
            let router = self
                .router
                .read()
                .await
                .clone()
                .ok_or_else(|| Error::Config("Router not initialized".to_string()))?;

            let ctx = RestorePartitionContext {
                source_topic: topic_backup.name.clone(),
                target_topic: target_topic.clone(),
                source_partition: partition_backup.partition_id,
                target_partition,
                segments: partition_backup.segments.clone(),
                router,
                storage: self.storage.clone(),
                options: options.clone(),
                metrics: Arc::clone(&self.metrics),
                health: Arc::clone(&self.health),
                kafka_cb: Arc::clone(&self.kafka_circuit_breaker),
                storage_cb: Arc::clone(&self.storage_circuit_breaker),
                checkpoint: Arc::clone(&self.checkpoint),
                offset_mapping: Arc::clone(&self.offset_mapping),
            };

            handles.push(tokio::spawn(async move {
                let result = ctx.restore_partition().await;
                drop(permit);
                result
            }));
        }

        // Wait for all partitions
        let mut partition_reports = Vec::new();
        let mut total_records = 0u64;
        let mut total_bytes = 0u64;

        for handle in handles {
            let report = handle.await.map_err(|e| {
                Error::Io(std::io::Error::other(format!("Task join error: {}", e)))
            })??;

            total_records += report.records;
            total_bytes += report.bytes;
            partition_reports.push(report);
        }

        Ok(TopicRestoreReport {
            source_topic: topic_backup.name.clone(),
            target_topic,
            partitions: partition_reports,
            records: total_records,
            bytes: total_bytes,
        })
    }

    /// Load checkpoint from file
    async fn load_checkpoint(&self, path: &std::path::Path) -> Result<()> {
        let data = tokio::fs::read_to_string(path).await?;
        let checkpoint: RestoreCheckpoint = serde_json::from_str(&data)?;

        info!(
            "Loaded checkpoint: {} segments completed, {} records restored",
            checkpoint.segments_completed.len(),
            checkpoint.records_restored
        );

        *self.checkpoint.lock().await = Some(checkpoint);
        Ok(())
    }

    /// Save checkpoint to file
    async fn save_checkpoint(&self, path: &std::path::Path) -> Result<()> {
        let mut checkpoint = self.checkpoint.lock().await;

        if let Some(cp) = checkpoint.as_mut() {
            cp.touch();
            let json = serde_json::to_string_pretty(cp)?;
            tokio::fs::write(path, json).await?;
            debug!("Checkpoint saved to {:?}", path);
        }

        Ok(())
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
    ) -> Result<()> {
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

            // Calculate partition count (max partition_id + 1)
            let partition_count = topic_backup
                .partitions
                .iter()
                .map(|p| p.partition_id)
                .max()
                .map(|max_id| max_id + 1)
                .unwrap_or(1);

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
                let replication_factor = options.default_replication_factor.unwrap_or(-1);
                TopicToCreate {
                    name: name.clone(),
                    num_partitions: partition_count,
                    replication_factor,
                }
            })
            .collect();

        if missing_topics.is_empty() {
            debug!("All target topics already exist");
            return Ok(());
        }

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

        Ok(())
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
    storage: Arc<dyn StorageBackend>,
    options: RestoreOptions,
    metrics: Arc<PerformanceMetrics>,
    health: Arc<HealthCheck>,
    kafka_cb: Arc<CircuitBreaker>,
    storage_cb: Arc<CircuitBreaker>,
    checkpoint: Arc<Mutex<Option<RestoreCheckpoint>>>,
    offset_mapping: Arc<Mutex<OffsetMapping>>,
}

impl RestorePartitionContext {
    async fn restore_partition(self) -> Result<PartitionRestoreReport> {
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
            return Ok(PartitionRestoreReport {
                source_partition: self.source_partition,
                target_partition: self.target_partition,
                segments_processed: 0,
                records: 0,
                bytes: 0,
                first_offset: 0,
                last_offset: 0,
                first_timestamp: 0,
                last_timestamp: 0,
            });
        }

        // Check which segments are already completed (for resume)
        let completed_segments: HashSet<String> = {
            let checkpoint = self.checkpoint.lock().await;
            checkpoint
                .as_ref()
                .map(|cp| cp.segments_completed.iter().cloned().collect())
                .unwrap_or_default()
        };

        let mut total_records = 0u64;
        let mut total_bytes = 0u64;
        let mut segments_processed = 0u64;
        let mut first_offset = i64::MAX;
        let mut last_offset = i64::MIN;
        let mut first_timestamp = i64::MAX;
        let mut last_timestamp = i64::MIN;

        // Process each segment
        for segment in segments {
            // Skip if already completed
            if completed_segments.contains(&segment.key) {
                debug!("Skipping completed segment: {}", segment.key);
                continue;
            }

            let records = match self.read_segment(segment).await {
                Ok(r) => {
                    self.storage_cb.record_success();
                    self.health.mark_healthy("storage");
                    r
                }
                Err(e) => {
                    self.storage_cb.record_failure();
                    self.health
                        .mark_degraded("storage", &format!("Read error: {}", e));
                    return Err(e);
                }
            };

            // Filter records by time window
            let filtered_records = self.filter_records_by_time(records);

            if filtered_records.is_empty() {
                // Mark segment as completed even if no records match
                self.mark_segment_completed(&segment.key).await;
                continue;
            }

            // Track offset range
            for record in &filtered_records {
                first_offset = first_offset.min(record.offset);
                last_offset = last_offset.max(record.offset);
                first_timestamp = first_timestamp.min(record.timestamp);
                last_timestamp = last_timestamp.max(record.timestamp);

                // Update offset mapping
                self.offset_mapping.lock().await.update_range(
                    &self.target_topic,
                    self.target_partition,
                    record.offset,
                    None,
                    record.timestamp,
                );
            }

            // Add original offset headers if configured (header-based strategy)
            // Use binary encoding (i64 little-endian) for proper deserialization
            let records_to_produce = if self.options.include_original_offset_header
                || self.options.consumer_group_strategy == OffsetStrategy::HeaderBased
            {
                filtered_records
                    .into_iter()
                    .map(|mut r| {
                        // Store original offset as binary i64 (8 bytes, little-endian)
                        r.headers.push(RecordHeader {
                            key: "x-original-offset".to_string(),
                            value: r.offset.to_le_bytes().to_vec(),
                        });
                        // Store original timestamp as binary i64 (8 bytes, little-endian)
                        r.headers.push(RecordHeader {
                            key: "x-original-timestamp".to_string(),
                            value: r.timestamp.to_le_bytes().to_vec(),
                        });
                        // Store source cluster info if available
                        r.headers.push(RecordHeader {
                            key: "x-source-partition".to_string(),
                            value: self.source_partition.to_le_bytes().to_vec(),
                        });
                        r
                    })
                    .collect()
            } else {
                filtered_records
            };

            // Track bytes
            let batch_bytes: u64 = records_to_produce
                .iter()
                .map(|r| {
                    r.key.as_ref().map(|k| k.len()).unwrap_or(0) as u64
                        + r.value.as_ref().map(|v| v.len()).unwrap_or(0) as u64
                })
                .sum();

            // Produce records in batches
            let batch_size = self.options.produce_batch_size;
            for batch in records_to_produce.chunks(batch_size) {
                // Apply rate limiting if configured
                if let Some(rate) = self.options.rate_limit_records_per_sec {
                    let delay = Duration::from_secs_f64(batch.len() as f64 / rate as f64);
                    tokio::time::sleep(delay).await;
                }

                // Produce using router (automatically routes to partition leader)
                match self
                    .router
                    .produce(&self.target_topic, self.target_partition, batch.to_vec())
                    .await
                {
                    Ok(produce_response) => {
                        self.kafka_cb.record_success();
                        self.health.mark_healthy("kafka");

                        // Capture offset mapping (Phase 2: detailed offset mapping)
                        // The base_offset is the offset assigned to the first record in the batch
                        let base_offset = produce_response.base_offset;
                        for (i, record) in batch.iter().enumerate() {
                            let target_offset = base_offset + i as i64;

                            // Extract source offset from header (if available) or use record offset
                            let source_offset = self.extract_source_offset(record);

                            // Add detailed mapping for exact offset lookup during consumer group reset
                            self.offset_mapping.lock().await.add_detailed(
                                &self.target_topic,
                                self.target_partition,
                                source_offset,
                                target_offset,
                                record.timestamp,
                            );
                        }
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
            }

            total_bytes += batch_bytes;
            segments_processed += 1;

            // Mark segment as completed
            self.mark_segment_completed(&segment.key).await;
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

        Ok(PartitionRestoreReport {
            source_partition: self.source_partition,
            target_partition: self.target_partition,
            segments_processed,
            records: total_records,
            bytes: total_bytes,
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
        })
    }

    async fn mark_segment_completed(&self, key: &str) {
        let mut checkpoint = self.checkpoint.lock().await;
        if let Some(cp) = checkpoint.as_mut() {
            cp.mark_segment_completed(key);
        }
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
        // Read from storage
        let data = self.storage.get(&segment.key).await?;

        // Check if this is binary format (starts with MAGIC_BYTES)
        if data.len() >= 4 && data[0..4] == MAGIC_BYTES {
            // Binary format
            self.read_binary_segment(data).await
        } else {
            // Legacy JSON format
            self.read_json_segment(&segment.key, data)
        }
    }

    async fn read_binary_segment(&self, data: bytes::Bytes) -> Result<Vec<BackupRecord>> {
        let mut reader = SegmentReader::open(data)?;
        let binary_records = reader.read_all()?;

        // Convert binary records to BackupRecord
        let records = binary_records
            .into_iter()
            .map(|br| BackupRecord {
                key: br.key.map(|b| b.to_vec()),
                value: br.value.map(|b| b.to_vec()),
                headers: br
                    .headers
                    .into_iter()
                    .map(|(k, v)| RecordHeader {
                        key: k,
                        value: v.map(|b| b.to_vec()).unwrap_or_default(),
                    })
                    .collect(),
                timestamp: br.timestamp,
                offset: br.offset,
            })
            .collect();

        Ok(records)
    }

    fn read_json_segment(&self, key: &str, data: bytes::Bytes) -> Result<Vec<BackupRecord>> {
        // Detect and apply decompression
        let compression = detect_from_extension(key);
        let decompressed = decompress(&data, compression)?;

        // Deserialize records
        let records: Vec<BackupRecord> = serde_json::from_slice(&decompressed)?;

        Ok(records)
    }

    fn filter_records_by_time(&self, records: Vec<BackupRecord>) -> Vec<BackupRecord> {
        records
            .into_iter()
            .filter(|r| {
                let after_start = self
                    .options
                    .time_window_start
                    .map(|s| r.timestamp >= s)
                    .unwrap_or(true);

                let before_end = self
                    .options
                    .time_window_end
                    .map(|e| r.timestamp <= e)
                    .unwrap_or(true);

                after_start && before_end
            })
            .collect()
    }

    /// Extract source offset from record headers or fall back to record.offset
    /// Headers can be either binary (preferred) or string encoded
    fn extract_source_offset(&self, record: &BackupRecord) -> i64 {
        for header in &record.headers {
            if header.key == "x-original-offset" {
                // Try binary format first (8 bytes, little-endian i64)
                if header.value.len() == 8 {
                    if let Ok(bytes) = header.value[..8].try_into() {
                        return i64::from_le_bytes(bytes);
                    }
                }
                // Fall back to string format for backwards compatibility
                if let Ok(s) = std::str::from_utf8(&header.value) {
                    if let Ok(offset) = s.parse::<i64>() {
                        return offset;
                    }
                }
            }
        }
        // No header found, use record offset
        record.offset
    }

    /// Extract source timestamp from record headers or fall back to record.timestamp
    /// Headers can be either binary (preferred) or string encoded
    #[allow(dead_code)]
    fn extract_source_timestamp(&self, record: &BackupRecord) -> i64 {
        for header in &record.headers {
            if header.key == "x-original-timestamp" {
                // Try binary format first (8 bytes, little-endian i64)
                if header.value.len() == 8 {
                    if let Ok(bytes) = header.value[..8].try_into() {
                        return i64::from_le_bytes(bytes);
                    }
                }
                // Fall back to string format for backwards compatibility
                if let Ok(s) = std::str::from_utf8(&header.value) {
                    if let Ok(ts) = s.parse::<i64>() {
                        return ts;
                    }
                }
            }
        }
        // No header found, use record timestamp
        record.timestamp
    }
}

/// Pattern matching for topic names.
/// Supports:
/// - Glob patterns: `orders-*`, `?-topic`, `*-test-*`
/// - Regex patterns (prefixed with `~`): `~orders-\d+`, `~^test-.*$`
fn pattern_match(pattern: &str, text: &str) -> bool {
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
