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
use crate::kafka::{consumer_groups::fetch_offsets, PartitionLeaderRouter, TopicMetadata};
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
            // Use config.offset_storage.db_path if provided, otherwise default to
            // temp directory. Previously this was hardcoded to "./{backup_id}-offsets.db"
            // which fails on read-only filesystems (Issue #62).
            let db_path = config
                .offset_storage
                .as_ref()
                .map(|os| os.db_path.clone())
                .unwrap_or_else(|| {
                    std::env::temp_dir().join(format!("{}-offsets.db", config.backup_id))
                });

            let offset_config = OffsetStoreConfig {
                db_path,
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

    /// Get a clone of the shutdown sender for external signal handling
    pub fn shutdown_handle(&self) -> broadcast::Sender<()> {
        self.shutdown_tx.clone()
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

        let source = self.config.source.as_ref().unwrap();
        let backup_opts = self.config.backup.clone().unwrap_or_default();

        let mut shutdown_rx = self.shutdown_receiver();

        // Create semaphore to limit concurrent partition backups
        let semaphore = Arc::new(Semaphore::new(backup_opts.max_concurrent_partitions));

        info!(
            "Backup engine starting with max_concurrent_partitions={}, poll_interval_ms={}",
            backup_opts.max_concurrent_partitions, backup_opts.poll_interval_ms
        );

        // Run backup loop
        loop {
            // Re-discover topics at the start of every cycle so that topics created
            // after the backup process started are picked up automatically (Issue #67 bug 1).
            // The Metadata request is a single bulk call and costs only a few milliseconds
            // — negligible compared to poll_interval_ms (Issue #29 optimisation preserved).
            let topics_metadata = self.resolve_topics(&source.topics, &backup_opts).await?;

            if topics_metadata.is_empty() {
                warn!("No topics matched the configured patterns — skipping cycle");
                if !backup_opts.continuous {
                    break;
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(backup_opts.poll_interval_ms)) => {}
                    _ = shutdown_rx.recv() => {
                        info!("Shutdown signal received, stopping backup");
                        return Ok(());
                    }
                }
                continue;
            }

            info!("Backing up {} topics", topics_metadata.len());

            // Capture snapshot offsets if stop_at_current_offsets is enabled
            // This provides a consistent "point-in-time" snapshot for DR backups
            // Returns (earliest, latest) pairs to avoid redundant offset fetches later
            let snapshot_offsets: Option<HashMap<(String, i32), (i64, i64)>> =
                if backup_opts.stop_at_current_offsets {
                    Some(self.capture_snapshot_offsets(&topics_metadata).await?)
                } else {
                    None
                };

            let mut all_handles = Vec::new();

            for topic_meta in &topics_metadata {
                let topic = &topic_meta.name;

                // Check for shutdown before spawning
                if shutdown_rx.try_recv().is_ok() {
                    info!("Shutdown signal received, stopping backup");
                    self.finalize().await?;
                    return Ok(());
                }

                // Use cached partition info from the metadata we already fetched
                let partitions: Vec<i32> = topic_meta
                    .partitions
                    .iter()
                    .map(|p| p.partition_id)
                    .collect();

                // Record the real partition count from Kafka metadata so restore can
                // recreate the topic correctly even when some partitions are empty
                // and therefore have no segments in the manifest (Issue #67 bug 4).
                {
                    let mut manifest = self.manifest.lock().await;
                    let topic_entry = manifest.get_or_create_topic(topic);
                    topic_entry.original_partition_count = Some(partitions.len() as i32);
                }

                // Spawn a task for each partition (limited by semaphore)
                // NOTE: The semaphore is acquired INSIDE the spawned task, not before
                // spawning. This allows all tasks to be spawned immediately and queued,
                // with the semaphore controlling how many execute concurrently. Previously,
                // acquiring the semaphore before spawning serialized the loop and caused
                // severe slowdowns on high-latency connections (Issue #29).
                for partition in partitions {
                    // Get cached offsets for snapshot mode (if enabled)
                    let (earliest_offset, target_offset) = snapshot_offsets
                        .as_ref()
                        .and_then(|m| m.get(&(topic.clone(), partition)))
                        .map(|(earliest, latest)| (Some(*earliest), Some(*latest)))
                        .unwrap_or((None, None));

                    let sem = semaphore.clone();

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
                        earliest_offset,
                        target_offset,
                    };

                    all_handles.push(tokio::spawn(async move {
                        // Acquire permit inside the task - limits concurrency
                        // without blocking the spawning loop
                        let _permit = sem.acquire_owned().await.unwrap();
                        let topic_name = ctx.topic.clone();
                        let partition_id = ctx.partition;
                        let backup_result = ctx.backup_partition().await;
                        (topic_name, partition_id, backup_result)
                    }));
                }
            }

            info!(
                "Spawned {} backup tasks (max {} concurrent)",
                all_handles.len(),
                backup_opts.max_concurrent_partitions
            );

            // Wait for ALL partitions across ALL topics to complete,
            // but allow interruption by shutdown signal for graceful exit
            let mut shutdown_join_rx = self.shutdown_receiver();

            tokio::select! {
                results = futures::future::join_all(all_handles) => {
                    let total_tasks = results.len();

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

                    if error_count > 0 {
                        error!(
                            "{} of {} partition backup tasks failed",
                            error_count, total_tasks
                        );
                        return Err(Error::Io(std::io::Error::other(format!(
                            "{} of {} partitions failed to backup",
                            error_count, total_tasks
                        ))));
                    }
                }
                _ = shutdown_join_rx.recv() => {
                    info!("Shutdown signal received during backup cycle, finalizing...");
                    self.finalize().await?;
                    return Ok(());
                }
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

            // Optionally snapshot consumer group offsets (Issue #67 bug 5/6)
            if backup_opts.consumer_group_snapshot {
                if let Err(e) = self.snapshot_consumer_groups().await {
                    warn!("Consumer group snapshot failed (non-fatal): {}", e);
                }
            }

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

    /// Resolve topic patterns to actual topic metadata.
    ///
    /// Returns full `TopicMetadata` (including partition info) to avoid
    /// redundant per-topic metadata calls later. This is critical for
    /// high-latency connections where per-topic calls would cause severe
    /// performance issues (Issue #29).
    async fn resolve_topics(
        &self,
        selection: &crate::config::TopicSelection,
        backup_opts: &BackupOptions,
    ) -> Result<Vec<TopicMetadata>> {
        // Fetch ALL topic metadata in a single bulk call
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
                // Keep full TopicMetadata instead of just the name
                selected.push(topic);
            }
        }

        // Sort by topic name for consistent ordering
        selected.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(selected)
    }

    /// Save the manifest to storage, merging with any existing manifest.
    ///
    /// Continuous backups and restarts both produce a fresh in-memory manifest that
    /// only contains topics active in the current session. Without merging, topics
    /// with no new data in this session would be silently dropped from the stored
    /// manifest on the next write (Issue #67 bug 2).
    ///
    /// The merge is a union: topics/partitions/segments from the stored manifest are
    /// preserved and new entries from the current session are appended. Duplicate
    /// segments (same key or same start_offset) are deduplicated — the stored entry
    /// wins on conflict.
    async fn save_manifest(&self) -> Result<()> {
        let current = self.manifest.lock().await.clone();
        let key = format!("{}/manifest.json", self.config.backup_id);

        // Load existing manifest and merge; fall back to current-only on any error
        let merged = match self.storage.get(&key).await {
            Ok(data) => match serde_json::from_slice::<BackupManifest>(&data) {
                Ok(existing) => merge_manifests(existing, current),
                Err(e) => {
                    warn!("Existing manifest is unparseable, overwriting: {}", e);
                    current
                }
            },
            Err(_) => current, // First run — no manifest yet
        };

        let manifest_json = serde_json::to_string_pretty(&merged)?;
        self.storage.put(&key, Bytes::from(manifest_json)).await?;
        debug!("Saved manifest to {} ({} topics)", key, merged.topics.len());

        Ok(())
    }

    /// Snapshot consumer group committed offsets to storage.
    ///
    /// Queries every broker individually (KRaft-safe), fetches committed offsets for
    /// each group, filters to groups that have offsets on backed-up topics, and writes
    /// `{backup_id}/consumer-groups-snapshot.json` (Issue #67 bugs 5 & 6).
    ///
    /// Uses the existing router connections — does NOT open new TCP connections.
    async fn snapshot_consumer_groups(&self) -> Result<()> {
        // Collect backed-up topic names from in-memory manifest
        let backed_topics: std::collections::HashSet<String> = {
            let manifest = self.manifest.lock().await;
            manifest.topics.iter().map(|t| t.name.clone()).collect()
        };

        if backed_topics.is_empty() {
            debug!("Skipping consumer group snapshot: no topics in manifest yet");
            return Ok(());
        }

        // List groups from every broker (KRaft: each broker is coordinator for a subset)
        let all_groups = self.router.list_groups_all_brokers().await?;
        debug!(
            "Consumer group snapshot: {} groups across all brokers",
            all_groups.len()
        );

        #[derive(serde::Serialize)]
        struct GroupEntry {
            group_id: String,
            /// topic -> partition_id (string) -> committed offset
            offsets: std::collections::HashMap<String, std::collections::HashMap<String, i64>>,
        }

        #[derive(serde::Serialize)]
        struct Snapshot {
            snapshot_time: i64,
            groups: Vec<GroupEntry>,
        }

        // Fetch offsets via bootstrap_client through the router
        let bootstrap_client = self.router.bootstrap_client();
        let mut snapshot_groups: Vec<GroupEntry> = Vec::new();

        for group in &all_groups {
            let committed = match fetch_offsets(bootstrap_client, &group.group_id, None).await {
                Ok(c) => c,
                Err(e) => {
                    debug!(
                        "Could not fetch offsets for group {}: {}",
                        group.group_id, e
                    );
                    continue;
                }
            };

            if committed.is_empty() {
                continue;
            }

            let mut offsets_by_topic: std::collections::HashMap<
                String,
                std::collections::HashMap<String, i64>,
            > = std::collections::HashMap::new();
            for co in &committed {
                if backed_topics.contains(&co.topic) && co.offset >= 0 {
                    offsets_by_topic
                        .entry(co.topic.clone())
                        .or_default()
                        .insert(co.partition.to_string(), co.offset);
                }
            }

            if !offsets_by_topic.is_empty() {
                snapshot_groups.push(GroupEntry {
                    group_id: group.group_id.clone(),
                    offsets: offsets_by_topic,
                });
            }
        }

        let snapshot = Snapshot {
            snapshot_time: chrono::Utc::now().timestamp_millis(),
            groups: snapshot_groups,
        };

        let key = format!("{}/consumer-groups-snapshot.json", self.config.backup_id);
        let json = serde_json::to_string_pretty(&snapshot)?;
        self.storage.put(&key, Bytes::from(json)).await?;

        info!(
            "Consumer groups snapshot saved ({} groups) to {}",
            snapshot.groups.len(),
            key
        );
        Ok(())
    }

    /// Capture current offsets for all partitions (snapshot mode).
    ///
    /// Returns both earliest and latest offsets for each partition.
    /// The latest offsets provide a consistent snapshot point - all partitions
    /// will backup to the same logical point in time.
    ///
    /// Uses batched ListOffsets requests (one per broker per timestamp) instead
    /// of per-partition requests. For 8,660 partitions across 3 brokers, this
    /// sends ~6 requests instead of ~17,320 (Issue #29).
    async fn capture_snapshot_offsets(
        &self,
        topics_metadata: &[TopicMetadata],
    ) -> Result<HashMap<(String, i32), (i64, i64)>> {
        info!(
            "Snapshot mode: capturing offsets for {} topics",
            topics_metadata.len()
        );

        let snapshot_start = Instant::now();

        // Build the full list of (topic, partition) pairs
        let all_partitions: Vec<(String, i32)> = topics_metadata
            .iter()
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .map(move |p| (t.name.clone(), p.partition_id))
            })
            .collect();

        // Batch fetch all offsets (grouped by leader broker)
        let offsets = self.router.batch_get_all_offsets(&all_partitions).await?;

        let total_records: i64 = offsets
            .values()
            .map(|(earliest, latest)| latest - earliest)
            .sum();

        let snapshot_elapsed_ms = snapshot_start.elapsed().as_millis();
        info!(
            "snapshot_capture_complete: {} partitions in {}ms ({} total records to backup)",
            offsets.len(),
            snapshot_elapsed_ms,
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
    /// Cached earliest offset from snapshot capture.
    /// When set, avoids a redundant get_offsets() call in backup_partition().
    earliest_offset: Option<i64>,
    /// Target offset for snapshot mode (stop_at_current_offsets)
    /// When set, backup stops when this offset is reached instead of latest
    target_offset: Option<i64>,
}

impl BackupPartitionContext {
    async fn backup_partition(self) -> Result<()> {
        debug!("Starting backup of {}:{}", self.topic, self.partition);

        // Use cached offsets from snapshot capture if available (avoids redundant
        // network calls that serialize through the broker mutex - Issue #29).
        // In snapshot mode, capture_snapshot_offsets() already fetched both earliest
        // and latest offsets for all partitions in batched requests.
        let (earliest, latest) =
            if let (Some(e), Some(l)) = (self.earliest_offset, self.target_offset) {
                (e, l)
            } else {
                self.router.get_offsets(&self.topic, self.partition).await?
            };

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
        let mut segments_written = 0u64;

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
                    let seg_start = segment_writer.start_offset().unwrap();
                    let key = self.segment_key(seg_start);
                    if let Some(segment_metadata) = segment_writer.flush(&key).await? {
                        self.add_segment_to_manifest(segment_metadata).await;
                        segments_written += 1;
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
            let seg_start = segment_writer.start_offset().unwrap();
            let key = self.segment_key(seg_start);
            if let Some(segment_metadata) = segment_writer.flush(&key).await? {
                self.add_segment_to_manifest(segment_metadata).await;
                segments_written += 1;
            }
        }

        if self.target_offset.is_some() {
            info!(
                "Completed snapshot backup of {}:{} - {} segments (reached target offset {})",
                self.topic, self.partition, segments_written, end_offset
            );
        } else {
            info!(
                "Completed backup of {}:{} - {} segments",
                self.topic, self.partition, segments_written
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

    fn segment_key(&self, start_offset: i64) -> String {
        let ext = extension(self.options.compression);
        format!(
            "{}/topics/{}/partition={}/segment-{:020}.bin{}",
            self.backup_id, self.topic, self.partition, start_offset, ext
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

/// Merge two backup manifests, performing a union of topics/partitions/segments.
///
/// Rules:
/// - Topics only in `existing` — preserved as-is (covers inactive topics from prior sessions)
/// - Topics only in `current`  — appended
/// - Topics in both            — partitions are merged recursively:
///   - `original_partition_count` updated from `current` when present
///   - Partitions only in existing → preserved
///   - Partitions only in current  → appended
///   - Partitions in both: segments deduplicated by (key, start_offset); existing wins on
///     conflict. Output sorted by start_offset.
fn merge_manifests(mut existing: BackupManifest, current: BackupManifest) -> BackupManifest {
    use std::collections::HashMap as HM;

    for cur_topic in current.topics {
        if let Some(ex_topic) = existing
            .topics
            .iter_mut()
            .find(|t| t.name == cur_topic.name)
        {
            // Update partition count when the current session has fresh metadata
            if cur_topic.original_partition_count.is_some() {
                ex_topic.original_partition_count = cur_topic.original_partition_count;
            }
            for cur_part in cur_topic.partitions {
                if let Some(ex_part) = ex_topic
                    .partitions
                    .iter_mut()
                    .find(|p| p.partition_id == cur_part.partition_id)
                {
                    // Merge segments: deduplicate by key and start_offset; existing wins
                    let mut seen_keys: HM<String, ()> = ex_part
                        .segments
                        .iter()
                        .map(|s| (s.key.clone(), ()))
                        .collect();
                    let mut seen_offsets: HM<i64, ()> = ex_part
                        .segments
                        .iter()
                        .map(|s| (s.start_offset, ()))
                        .collect();
                    for seg in cur_part.segments {
                        if !seen_keys.contains_key(&seg.key)
                            && !seen_offsets.contains_key(&seg.start_offset)
                        {
                            seen_keys.insert(seg.key.clone(), ());
                            seen_offsets.insert(seg.start_offset, ());
                            ex_part.segments.push(seg);
                        }
                    }
                    ex_part.segments.sort_by_key(|s| s.start_offset);
                } else {
                    ex_topic.partitions.push(cur_part);
                }
            }
        } else {
            existing.topics.push(cur_topic);
        }
    }

    existing
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
