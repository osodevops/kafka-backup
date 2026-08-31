//! Backup engine orchestration.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex, Semaphore};
use tracing::{debug, error, info, warn};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::compression::extension;
use crate::config::{BackupOptions, CompressionType, Config, Mode, StartOffset, TopicSelection};
use crate::error::KafkaError;
use crate::health::HealthCheck;
use crate::kafka::{ConfigResourceType, PartitionLeaderRouter, TopicMetadata};
use crate::manifest::{
    BackupManifest, BackupRecord, OffsetGap, OffsetGapReason, PartitionBackup, SegmentMetadata,
};
use crate::metrics::{ErrorType, PerformanceMetrics, PrometheusMetrics};
use crate::offset_headers;
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
    manifest_persistence: Arc<ManifestPersistence>,
    offset_persistence: Option<Arc<OffsetPersistence>>,
    kafka_circuit_breaker: Arc<CircuitBreaker>,
    storage_circuit_breaker: Arc<CircuitBreaker>,
    shutdown_tx: broadcast::Sender<()>,
}

struct ManifestPersistence {
    backup_id: String,
    storage: Arc<dyn StorageBackend>,
    manifest: Arc<Mutex<BackupManifest>>,
    save_lock: Mutex<()>,
    progress_lock: Mutex<Option<Instant>>,
    progress_interval: Duration,
}

impl ManifestPersistence {
    fn new(
        backup_id: String,
        storage: Arc<dyn StorageBackend>,
        manifest: Arc<Mutex<BackupManifest>>,
        progress_interval: Duration,
    ) -> Self {
        Self {
            backup_id,
            storage,
            manifest,
            save_lock: Mutex::new(()),
            progress_lock: Mutex::new(None),
            progress_interval,
        }
    }

    async fn save_now(&self) -> Result<()> {
        let _guard = self.save_lock.lock().await;
        let current = self.manifest.lock().await.clone();
        save_manifest_snapshot(self.storage.as_ref(), &self.backup_id, current).await
    }

    async fn save_progress(&self) -> Result<()> {
        let mut last_save = self.progress_lock.lock().await;
        if last_save.is_some_and(|last| last.elapsed() < self.progress_interval) {
            return Ok(());
        }

        self.save_now().await?;
        *last_save = Some(Instant::now());
        Ok(())
    }
}

struct OffsetPersistence {
    backup_id: String,
    storage: Arc<dyn StorageBackend>,
    offset_store: Arc<SqliteOffsetStore>,
    sync_lock: Mutex<Option<Instant>>,
    sync_interval: Duration,
}

impl OffsetPersistence {
    fn new(
        backup_id: String,
        storage: Arc<dyn StorageBackend>,
        offset_store: Arc<SqliteOffsetStore>,
        sync_interval: Duration,
    ) -> Self {
        Self {
            backup_id,
            storage,
            offset_store,
            sync_lock: Mutex::new(None),
            sync_interval,
        }
    }

    async fn sync_now(&self) -> Result<()> {
        let mut last_sync = self.sync_lock.lock().await;
        self.offset_store
            .sync_to_storage(
                self.storage.as_ref(),
                &format!("{}/offsets.db", self.backup_id),
            )
            .await?;

        *last_sync = Some(Instant::now());
        Ok(())
    }

    async fn sync_if_due(&self) -> Result<()> {
        let mut last_sync = self.sync_lock.lock().await;
        if last_sync.is_some_and(|last| last.elapsed() < self.sync_interval) {
            return Ok(());
        }

        self.offset_store
            .sync_to_storage(
                self.storage.as_ref(),
                &format!("{}/offsets.db", self.backup_id),
            )
            .await?;
        *last_sync = Some(Instant::now());
        Ok(())
    }
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

        // Initialize offset store for continuous backups or when explicitly configured
        let backup_opts = config.backup.clone().unwrap_or_default();
        let offset_store = if should_create_offset_store(
            backup_opts.continuous,
            config.offset_storage.is_some(),
        ) {
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

        let manifest = Arc::new(Mutex::new(manifest));
        let manifest_persistence = Arc::new(ManifestPersistence::new(
            config.backup_id.clone(),
            storage.clone(),
            manifest.clone(),
            Duration::from_secs(backup_opts.sync_interval_secs),
        ));
        let offset_persistence = offset_store.as_ref().map(|offset_store| {
            Arc::new(OffsetPersistence::new(
                config.backup_id.clone(),
                storage.clone(),
                offset_store.clone(),
                Duration::from_secs(backup_opts.sync_interval_secs),
            ))
        });

        Ok(Self {
            config,
            router,
            storage,
            manifest,
            metrics,
            prometheus_metrics,
            health,
            offset_store,
            manifest_persistence,
            offset_persistence,
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
            if let Some(ref offset_persistence) = self.offset_persistence {
                offset_persistence.sync_now().await?;
            }
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
        info!("{}", describe_offset_headers(&backup_opts));

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

            if backup_opts.capture_topic_configs {
                if let Err(error) = self.capture_topic_configs(&topics_metadata).await {
                    if backup_opts.require_topic_configs {
                        return Err(error);
                    }
                    warn!(
                        %error,
                        "Unable to capture topic configuration; continuing because require_topic_configs=false"
                    );
                }
            }

            // Capture snapshot offsets if stop_at_current_offsets is enabled
            // This provides a consistent "point-in-time" snapshot for DR backups
            // Returns (earliest, latest) pairs to avoid redundant offset fetches later
            let snapshot_ranges: Option<HashMap<(String, i32), SnapshotRange>> =
                if backup_opts.stop_at_current_offsets {
                    Some(
                        self.capture_snapshot_offsets(&topics_metadata, &backup_opts.start_offset)
                            .await?,
                    )
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
                    topic_entry.source_replication_factor = topic_meta
                        .partitions
                        .first()
                        .map(|partition| partition.replica_nodes.len() as i16);
                }

                // Publish metadata before any partition task for this topic can
                // upload segments. A hard kill after a segment upload cannot run
                // cleanup, so the manifest must already be discoverable.
                self.save_manifest().await?;

                // Spawn a task for each partition (limited by semaphore)
                // NOTE: The semaphore is acquired INSIDE the spawned task, not before
                // spawning. This allows all tasks to be spawned immediately and queued,
                // with the semaphore controlling how many execute concurrently. Previously,
                // acquiring the semaphore before spawning serialized the loop and caused
                // severe slowdowns on high-latency connections (Issue #29).
                for partition in partitions {
                    // Planned offset range for snapshot mode (if enabled)
                    let snapshot = snapshot_ranges
                        .as_ref()
                        .and_then(|m| m.get(&(topic.clone(), partition)).copied());

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
                        manifest_persistence: Arc::clone(&self.manifest_persistence),
                        offset_persistence: self.offset_persistence.clone(),
                        kafka_cb: Arc::clone(&self.kafka_circuit_breaker),
                        storage_cb: Arc::clone(&self.storage_circuit_breaker),
                        snapshot,
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

            // Publish a manifest before long-running partition tasks complete so
            // interrupted continuous backups still have discoverable metadata.
            self.save_manifest().await?;

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
                        self.persist_progress_best_effort("partition task failure").await;
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

            // A literal topic selected at backup start must still exist after
            // the cycle. This catches topics deleted while a backup is running
            // before a zero-record/empty-artifact result can be reported as
            // successful.
            self.ensure_literal_topic_includes_exist(&source.topics)
                .await?;

            // Checkpoint offsets
            if let Some(ref offset_store) = self.offset_store {
                let start = Instant::now();
                offset_store.checkpoint().await?;
                if let Some(ref offset_persistence) = self.offset_persistence {
                    offset_persistence.sync_now().await?;
                }
                self.metrics.record_checkpoint_latency(start.elapsed());
                self.health.mark_healthy("checkpointing");
            }

            // Save manifest periodically
            self.save_manifest().await?;

            // Apply backup.retention (issue #169): prune aged/oversized
            // segments from this backup set. Runs each cycle in continuous
            // mode, once for one-shot/snapshot runs. Non-fatal: a failed
            // prune must not fail the backup that just succeeded.
            if let Some(retention) = &backup_opts.retention {
                if let Err(e) = self.apply_retention(retention).await {
                    warn!("Retention prune failed (non-fatal): {}", e);
                }
            }

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

    /// Apply `backup.retention` to this backup set: plan against the
    /// in-memory manifest, remove the planned segments from it, persist the
    /// manifest (manifest first, so a crash leaves orphan objects rather
    /// than a manifest referencing deleted segments), then delete the
    /// objects.
    async fn apply_retention(&self, retention: &crate::config::RetentionOptions) -> Result<()> {
        let criteria = retention.to_criteria()?;
        let resume = match &self.offset_store {
            Some(store) => {
                super::prune::resume_positions(store.as_ref(), &self.config.backup_id).await?
            }
            None => std::collections::HashMap::new(),
        };

        let plan = {
            let manifest = self.manifest.lock().await;
            super::prune::plan_prune(
                &manifest,
                &resume,
                &criteria,
                chrono::Utc::now().timestamp_millis(),
                crate::manifest::PruneReason::Retention,
            )
        };
        if plan.segments == 0 {
            return Ok(());
        }

        info!(
            "Retention: pruning {} segment(s), {} bytes across {} partition(s)",
            plan.segments,
            plan.bytes,
            plan.partitions.len()
        );

        {
            let mut manifest = self.manifest.lock().await;
            super::prune::apply_plan_to_manifest(&mut manifest, &plan);
        }
        self.manifest_persistence.save_now().await?;
        super::prune::delete_planned_segments(self.storage.as_ref(), &plan).await;

        if let Some(ref prom) = self.prometheus_metrics {
            prom.record_segments_pruned(&self.config.backup_id, plan.segments, plan.bytes);
        }
        Ok(())
    }

    async fn capture_topic_configs(&self, topics: &[TopicMetadata]) -> Result<()> {
        let resources: Vec<_> = topics
            .iter()
            .map(|topic| (ConfigResourceType::Topic, topic.name.clone()))
            .collect();
        let described = self.router.describe_configs(&resources).await?;
        let mut manifest = self.manifest.lock().await;

        for topic in topics {
            let entries = described
                .get(&(ConfigResourceType::Topic, topic.name.clone()))
                .ok_or_else(|| {
                    Error::Kafka(crate::error::KafkaError::Protocol(format!(
                        "DescribeConfigs returned no result for topic {}",
                        topic.name
                    )))
                })?;
            let captured = entries
                .iter()
                .filter(|entry| {
                    entry.is_topic_override()
                        && !entry.read_only
                        && !entry.is_sensitive
                        && is_recovery_topic_config(&entry.name)
                })
                .filter_map(|entry| {
                    entry
                        .value
                        .as_ref()
                        .map(|value| (entry.name.clone(), value.clone()))
                })
                .collect();
            manifest.get_or_create_topic(&topic.name).configurations = captured;
        }
        Ok(())
    }

    async fn finalize(&self) -> Result<()> {
        // Final checkpoint
        if let Some(ref offset_store) = self.offset_store {
            offset_store.checkpoint().await?;
            if let Some(ref offset_persistence) = self.offset_persistence {
                offset_persistence.sync_now().await?;
            }
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

        fail_if_literal_topic_includes_missing(selection, &all_topics)?;

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

    async fn ensure_literal_topic_includes_exist(&self, selection: &TopicSelection) -> Result<()> {
        let all_topics = self.router.fetch_metadata(None).await?;
        fail_if_literal_topic_includes_missing(selection, &all_topics)
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
        self.manifest_persistence.save_now().await
    }

    async fn persist_progress_best_effort(&self, reason: &str) {
        if let Err(e) = self.manifest_persistence.save_now().await {
            warn!("Best-effort manifest save failed after {}: {}", reason, e);
        }
        if let Some(ref offset_persistence) = self.offset_persistence {
            if let Err(e) = offset_persistence.sync_now().await {
                warn!("Best-effort offset sync failed after {}: {}", reason, e);
            }
        }
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

        // Fetch offsets per coordinator broker: each broker handles OffsetFetch only
        // for groups it coordinates. Using the bootstrap client for all groups causes
        // NOT_COORDINATOR (error 16) responses for groups on other brokers, silently
        // producing empty offset lists.
        let group_offsets = self.router.fetch_group_offsets_all_coordinators().await?;
        debug!(
            "Consumer group snapshot: {} groups with committed offsets across all coordinators",
            group_offsets.len()
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

        let mut snapshot_groups: Vec<GroupEntry> = Vec::new();

        for (group_id, committed) in group_offsets {
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
                    group_id,
                    offsets: offsets_by_topic,
                });
            }
        }

        // Do not overwrite an existing snapshot with an empty one.
        // This protects against losing group offsets when kafka-backup restarts
        // before consuming applications have reconnected and committed offsets.
        // If no previous snapshot exists, write the empty one (first-time setup).
        if snapshot_groups.is_empty() {
            let key = format!("{}/consumer-groups-snapshot.json", self.config.backup_id);
            if self.storage.exists(&key).await.unwrap_or(false) {
                debug!("Consumer group snapshot: no groups with committed offsets, preserving existing snapshot");
                return Ok(());
            }
            debug!("Consumer group snapshot: no groups found, writing initial empty snapshot");
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
    /// Returns the planned offset range for each partition: the log start and
    /// high watermark at capture time plus the offset this run resumes from.
    /// The high watermarks provide a consistent snapshot point - all partitions
    /// will backup to the same logical point in time.
    ///
    /// Uses batched ListOffsets requests (one per broker per timestamp) instead
    /// of per-partition requests. For 8,660 partitions across 3 brokers, this
    /// sends ~6 requests instead of ~17,320 (Issue #29).
    ///
    /// The progress gauges are sized from the records this run will actually
    /// fetch, not the whole captured range: an incremental run that resumes
    /// from checkpoints reports only its new records
    /// (strimzi-backup-operator#57).
    async fn capture_snapshot_offsets(
        &self,
        topics_metadata: &[TopicMetadata],
        start_offset: &StartOffset,
    ) -> Result<HashMap<(String, i32), SnapshotRange>> {
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

        let checkpoints = self.load_checkpoints().await?;
        let ranges = plan_snapshot_ranges(offsets, &checkpoints, start_offset);

        let planned_records: i64 = ranges.values().map(SnapshotRange::planned_records).sum();
        let captured_records: i64 = ranges.values().map(SnapshotRange::captured_records).sum();
        let resumed_partitions = ranges
            .keys()
            .filter(|key| checkpoints.contains_key(*key))
            .count();

        let snapshot_elapsed_ms = snapshot_start.elapsed().as_millis();
        info!(
            "snapshot_capture_complete: {} partitions in {}ms ({} records to back up this run, \
             {} in the captured offset range, {} partitions resuming from a checkpoint)",
            ranges.len(),
            snapshot_elapsed_ms,
            planned_records,
            captured_records,
            resumed_partitions
        );

        if let Some(ref prom) = self.prometheus_metrics {
            prom.initialize_snapshot_progress(&self.config.backup_id, planned_records);
        }

        Ok(ranges)
    }

    /// Last checkpointed offset per partition for this backup id (empty without
    /// an offset store). Read once, in bulk, at snapshot capture time: the store
    /// has just been loaded and nothing has been written yet, so one query is
    /// the complete picture — no per-partition lookups for large clusters.
    async fn load_checkpoints(&self) -> Result<HashMap<(String, i32), i64>> {
        let Some(ref offset_store) = self.offset_store else {
            return Ok(HashMap::new());
        };
        let checkpoints = offset_store
            .get_all_offsets(&self.config.backup_id)
            .await?
            .into_iter()
            .map(|info| ((info.topic, info.partition), info.last_offset))
            .collect();
        Ok(checkpoints)
    }
}

/// Offsets captured for one partition of a snapshot (`stop_at_current_offsets`) run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SnapshotRange {
    /// Log start offset at capture time.
    earliest: i64,
    /// First offset this run fetches: the successor of the checkpointed offset
    /// when one exists, otherwise the configured start position.
    start: i64,
    /// High watermark at capture time; the run stops here.
    latest: i64,
}

impl SnapshotRange {
    /// Offsets this run has to fetch. Zero when the checkpoint already covers
    /// the captured range (nothing new since the previous run) and never
    /// negative, even if a checkpoint outlives a recreated topic.
    fn planned_records(&self) -> i64 {
        (self.latest - self.start).max(0)
    }

    /// The whole captured range, whether or not earlier runs archived part of it.
    fn captured_records(&self) -> i64 {
        (self.latest - self.earliest).max(0)
    }
}

/// Start offset for a partition from the configured `start_offset`, when no
/// checkpoint exists. For backups `latest` has always meant "start fresh".
fn configured_start_offset(
    start_offset: &StartOffset,
    topic: &str,
    partition: i32,
    earliest: i64,
) -> i64 {
    match start_offset {
        StartOffset::Earliest => earliest,
        StartOffset::Latest => earliest,
        StartOffset::Specific(map) => map
            .get(topic)
            .and_then(|partitions| partitions.get(&partition))
            .copied()
            .unwrap_or(earliest),
    }
}

/// Turn captured `(earliest, latest)` offsets into the range each partition
/// will actually fetch this run. A checkpoint takes precedence over the
/// configured start offset, exactly as `backup_partition` resumes.
///
/// A checkpoint below `earliest` (retention moved the log start since the
/// previous run) is kept as the start: the fetch loop resumes there, hits
/// OFFSET_OUT_OF_RANGE, records the gap and advances the progress gauge by the
/// gap span, so counting from the checkpoint keeps target and remaining
/// consistent.
fn plan_snapshot_ranges(
    offsets: HashMap<(String, i32), (i64, i64)>,
    checkpoints: &HashMap<(String, i32), i64>,
    start_offset: &StartOffset,
) -> HashMap<(String, i32), SnapshotRange> {
    offsets
        .into_iter()
        .map(|((topic, partition), (earliest, latest))| {
            let start = match checkpoints.get(&(topic.clone(), partition)) {
                Some(last_offset) => last_offset + 1,
                None => configured_start_offset(start_offset, &topic, partition, earliest),
            };
            (
                (topic, partition),
                SnapshotRange {
                    earliest,
                    start,
                    latest,
                },
            )
        })
        .collect()
}

fn is_recovery_topic_config(name: &str) -> bool {
    matches!(
        name,
        "cleanup.policy"
            | "compression.type"
            | "delete.retention.ms"
            | "file.delete.delay.ms"
            | "flush.messages"
            | "flush.ms"
            | "index.interval.bytes"
            | "max.compaction.lag.ms"
            | "max.message.bytes"
            | "message.downconversion.enable"
            | "message.format.version"
            | "message.timestamp.difference.max.ms"
            | "message.timestamp.type"
            | "min.cleanable.dirty.ratio"
            | "min.compaction.lag.ms"
            | "min.insync.replicas"
            | "preallocate"
            | "retention.bytes"
            | "retention.ms"
            | "segment.bytes"
            | "segment.index.bytes"
            | "segment.jitter.ms"
            | "segment.ms"
            | "unclean.leader.election.enable"
    )
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
    manifest_persistence: Arc<ManifestPersistence>,
    offset_persistence: Option<Arc<OffsetPersistence>>,
    kafka_cb: Arc<CircuitBreaker>,
    #[allow(dead_code)] // Reserved for future storage circuit breaker integration
    storage_cb: Arc<CircuitBreaker>,
    /// Planned offset range from snapshot capture (stop_at_current_offsets).
    /// When set, backup resumes at `start`, stops at `latest` instead of the
    /// live high watermark, and skips the redundant get_offsets() call.
    snapshot: Option<SnapshotRange>,
}

impl BackupPartitionContext {
    async fn backup_partition(self) -> Result<()> {
        debug!("Starting backup of {}:{}", self.topic, self.partition);

        // Snapshot mode: capture_snapshot_offsets() already resolved this
        // partition's earliest/start/latest in batched requests (avoids
        // redundant network calls that serialize through the broker mutex -
        // Issue #29) and sized the progress gauges from the planned start, so
        // the end offset is the captured high watermark ("stop_at_current_offsets"
        // for consistent DR snapshots). Otherwise resolve the offsets and the
        // resume position here.
        let (earliest, start_offset, end_offset) = match self.snapshot {
            Some(range) => {
                debug!(
                    "{}:{}: snapshot mode - target offset {} (earliest {}, resuming at {})",
                    self.topic, self.partition, range.latest, range.earliest, range.start
                );
                (range.earliest, range.start, range.latest)
            }
            None => {
                let (earliest, latest) =
                    self.router.get_offsets(&self.topic, self.partition).await?;
                let start_offset = self.resolve_start_offset(earliest).await?;
                (earliest, start_offset, latest)
            }
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
            max_segment_records: self.options.segment_max_records,
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

        // At most one segment is compressed/uploaded in the background while
        // we keep fetching; this overlaps compression with fetch wait time
        // without unbounded memory growth.
        let segment_flusher = segment_writer.flusher();
        let mut pending_flush: Option<tokio::task::JoinHandle<Result<SegmentMetadata>>> = None;

        // Fetch and store records in segments
        while current_offset < end_offset {
            let fetch_result = self.fetch_records(current_offset, end_offset).await;

            let (records, next_offset) = match fetch_result {
                Ok(data) => data,
                Err(e) => {
                    // OFFSET_OUT_OF_RANGE (issue #144): the offset we want was
                    // captured earlier (snapshot or checkpoint) and retention has
                    // since deleted it. Re-read the broker's log start offset and
                    // resume from there rather than failing the partition — but
                    // record the skipped range durably first: those records are
                    // gone from the source, and a backup that silently contains
                    // a hole is worse than one that fails loudly.
                    let e = if is_offset_out_of_range(&e) {
                        warn!(
                            "{}:{}: offset {} out of range (likely deleted by retention), \
                             refetching earliest and resuming",
                            self.topic, self.partition, current_offset
                        );
                        match self.router.get_offsets(&self.topic, self.partition).await {
                            Ok((new_earliest, _)) if new_earliest > current_offset => {
                                // Only the part of the gap inside this run's target
                                // range counts as lost *for this backup*; anything
                                // past `end_offset` was never going to be captured.
                                let gap_end = new_earliest.min(end_offset);
                                self.record_offset_gap(current_offset, gap_end).await;
                                current_offset = new_earliest;
                                continue;
                            }
                            Ok((new_earliest, new_latest)) => {
                                // The log start offset has NOT moved past us, so this
                                // is not a retention gap we can jump over: the offset
                                // is beyond the log end (log truncated or topic
                                // recreated underneath us). Not recoverable here —
                                // fail with a message that says which case it is.
                                out_of_range_error(
                                    &self.topic,
                                    self.partition,
                                    current_offset,
                                    new_earliest,
                                    new_latest,
                                )
                            }
                            Err(offset_err) => {
                                warn!(
                                    "{}:{}: failed to refetch earliest offset after out-of-range error: {}",
                                    self.topic, self.partition, offset_err
                                );
                                e
                            }
                        }
                    } else {
                        e
                    };

                    self.health
                        .mark_degraded("kafka", &format!("Fetch error: {}", e));
                    self.kafka_cb.record_failure();
                    if let Some(ref prom) = self.prometheus_metrics {
                        let error_type = ErrorType::from_error(&e);
                        prom.record_error(&self.backup_id, error_type);
                    }
                    self.persist_progress_best_effort("fetch error").await;
                    return Err(e);
                }
            };

            self.kafka_cb.record_success();
            self.health.mark_healthy("kafka");

            // A fetch can return zero records yet still advance the offset:
            // on compacted topics the log cleaner may have removed every
            // record in the fetched batches. Only "no records and no
            // progress" means we have reached the end of available data.
            if records.is_empty() && next_offset <= current_offset {
                break;
            }

            // Convert to binary records and add to writer
            for record in &records {
                let binary_record = to_binary_record(record, &self.options);
                segment_writer.add_record(binary_record)?;

                // Check if we should rotate
                if segment_writer.should_rotate() {
                    // Wait for the previous in-flight segment first, so
                    // manifest entries stay ordered and at most one sealed
                    // segment is held in memory per partition.
                    if let Some(handle) = pending_flush.take() {
                        let segment_metadata = join_segment_flush(handle).await?;
                        self.add_segment_to_manifest(segment_metadata).await;
                        self.manifest_persistence.save_progress().await?;
                        segments_written += 1;
                    }

                    let seg_start = segment_writer.start_offset().unwrap();
                    let key = self.segment_key(seg_start);
                    if let Some(sealed) = segment_writer.seal(&key) {
                        let flusher = segment_flusher.clone();
                        pending_flush =
                            Some(tokio::spawn(async move { flusher.write(sealed).await }));
                    }
                }
            }

            // Update offset tracking. Checkpoint the end of the fetched
            // batches rather than the last record: on compacted topics the
            // trailing records of a batch may no longer exist, and resuming
            // there would re-read the whole batch.
            if let Some(ref offset_store) = self.offset_store {
                let last_offset = next_offset - 1;
                offset_store
                    .set_offset(&self.backup_id, &self.topic, self.partition, last_offset)
                    .await?;
                if let Some(ref offset_persistence) = self.offset_persistence {
                    offset_persistence.sync_if_due().await?;
                }
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

                if self.snapshot.is_some() {
                    prom.advance_snapshot_progress(
                        &self.backup_id,
                        (next_offset - current_offset).max(0),
                    );
                }

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

        // Wait for any in-flight segment flush
        if let Some(handle) = pending_flush.take() {
            let segment_metadata = join_segment_flush(handle).await?;
            self.add_segment_to_manifest(segment_metadata).await;
            self.manifest_persistence.save_progress().await?;
            segments_written += 1;
        }

        // Flush any remaining records
        if segment_writer.has_data() {
            let seg_start = segment_writer.start_offset().unwrap();
            let key = self.segment_key(seg_start);
            if let Some(segment_metadata) = segment_writer.flush(&key).await? {
                self.add_segment_to_manifest(segment_metadata).await;
                self.manifest_persistence.save_progress().await?;
                segments_written += 1;
            }
        }

        if self.snapshot.is_some() {
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

    /// Resume position outside snapshot mode: the successor of the checkpointed
    /// offset when one exists, otherwise the configured start position.
    async fn resolve_start_offset(&self, earliest: i64) -> Result<i64> {
        if let Some(ref offset_store) = self.offset_store {
            if let Some(saved) = offset_store
                .get_offset(&self.backup_id, &self.topic, self.partition)
                .await?
            {
                return Ok(saved + 1);
            }
        }
        Ok(configured_start_offset(
            &self.options.start_offset,
            &self.topic,
            self.partition,
            earliest,
        ))
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

    /// Record that offsets `[start_offset, end_offset)` are permanently missing
    /// from this backup (issue #144): manifest entry, Prometheus counters,
    /// snapshot progress, and a best-effort manifest save so the gap survives
    /// a crash before the next segment flush.
    async fn record_offset_gap(&self, start_offset: i64, end_offset: i64) {
        let gap = OffsetGap {
            start_offset,
            end_offset,
            reason: OffsetGapReason::OffsetOutOfRange,
            detected_at: chrono::Utc::now().timestamp_millis(),
        };
        let span = gap.offset_span();
        if span == 0 {
            return;
        }

        warn!(
            "{}:{}: recording data gap [{}, {}) — {} offsets permanently missing from backup {} \
             (deleted from the source before they could be fetched)",
            self.topic, self.partition, start_offset, end_offset, span, self.backup_id
        );

        {
            let mut manifest = self.manifest.lock().await;
            manifest
                .get_or_create_topic(&self.topic)
                .get_or_create_partition(self.partition)
                .add_gap(gap);
        }

        if let Some(ref prom) = self.prometheus_metrics {
            prom.record_offset_gap(&self.backup_id, span);
            // The skipped span still counts towards the captured snapshot's
            // remaining offsets, otherwise the progress gauge never reaches
            // zero for a partition that recovered from a gap.
            if self.snapshot.is_some() {
                prom.advance_snapshot_progress(&self.backup_id, span);
            }
        }

        if let Err(e) = self.manifest_persistence.save_now().await {
            warn!(
                "Best-effort manifest save failed for {}:{} after recording data gap: {}",
                self.topic, self.partition, e
            );
        }
    }

    async fn persist_progress_best_effort(&self, reason: &str) {
        if let Err(e) = self.manifest_persistence.save_now().await {
            warn!(
                "Best-effort manifest save failed for {}:{} after {}: {}",
                self.topic, self.partition, reason, e
            );
        }
        if let Some(ref offset_persistence) = self.offset_persistence {
            if let Err(e) = offset_persistence.sync_now().await {
                warn!(
                    "Best-effort offset sync failed for {}:{} after {}: {}",
                    self.topic, self.partition, reason, e
                );
            }
        }
    }

    async fn fetch_records(
        &self,
        start_offset: i64,
        _end_offset: i64,
    ) -> Result<(Vec<BackupRecord>, i64)> {
        let max_bytes = effective_fetch_max_bytes(&self.options);

        // Fetch records using router (automatically routes to partition leader)
        let fetch_response = self
            .router
            .fetch(&self.topic, self.partition, start_offset, max_bytes)
            .await?;

        Ok((fetch_response.records, fetch_response.next_offset))
    }
}

/// Kafka protocol error code for OFFSET_OUT_OF_RANGE.
const OFFSET_OUT_OF_RANGE: i16 = 1;

/// True if `e` is the broker rejecting a Fetch with OFFSET_OUT_OF_RANGE.
fn is_offset_out_of_range(e: &Error) -> bool {
    matches!(
        e,
        Error::Kafka(KafkaError::BrokerError {
            code: OFFSET_OUT_OF_RANGE,
            ..
        })
    )
}

/// Build the error for an OFFSET_OUT_OF_RANGE that cannot be recovered by
/// skipping forward: the broker's log start offset (`earliest`) has not moved
/// past `offset`, so the fetch position must be beyond the log end instead.
/// Keeps error code 1 so metrics still classify it as `OffsetInvalid`.
fn out_of_range_error(
    topic: &str,
    partition: i32,
    offset: i64,
    earliest: i64,
    latest: i64,
) -> Error {
    let cause = if offset >= latest {
        "beyond the log end offset (log truncated or topic recreated?)"
    } else {
        "inside the broker's reported range (leader change or replica inconsistency?)"
    };
    Error::Kafka(KafkaError::BrokerError {
        code: OFFSET_OUT_OF_RANGE,
        message: format!(
            "Fetch offset {} for {}:{} is out of range and cannot be recovered by skipping \
             forward: broker log range is [{}, {}), offset is {}",
            offset, topic, partition, earliest, latest, cause
        ),
    })
}

/// Maximum bytes to request per Fetch call: an explicit `fetch_max_bytes`
/// wins; otherwise fall back to the segment size capped at 16MB. Clamped to
/// the Kafka protocol's i32 range.
fn effective_fetch_max_bytes(options: &BackupOptions) -> i32 {
    const DEFAULT_FETCH_CAP: u64 = 16 * 1024 * 1024;
    options
        .fetch_max_bytes
        .unwrap_or_else(|| options.segment_max_bytes.min(DEFAULT_FETCH_CAP))
        .clamp(1, i32::MAX as u64) as i32
}

/// Await a background segment flush task, surfacing panics as errors.
async fn join_segment_flush(
    handle: tokio::task::JoinHandle<Result<SegmentMetadata>>,
) -> Result<SegmentMetadata> {
    handle
        .await
        .map_err(|e| Error::Compression(format!("segment flush task failed: {e}")))?
}

async fn save_manifest_snapshot(
    storage: &dyn StorageBackend,
    backup_id: &str,
    current: BackupManifest,
) -> Result<()> {
    let key = format!("{}/manifest.json", backup_id);

    // Load existing manifest and merge; fall back to current-only on any error.
    let merged = match storage.get(&key).await {
        Ok(data) => match serde_json::from_slice::<BackupManifest>(&data) {
            Ok(existing) => merge_manifests(existing, current),
            Err(e) => {
                warn!("Existing manifest is unparseable, overwriting: {}", e);
                current
            }
        },
        Err(_) => current, // First write — no manifest yet.
    };

    let manifest_json = serde_json::to_string_pretty(&merged)?;
    storage.put(&key, Bytes::from(manifest_json)).await?;
    debug!("Saved manifest to {} ({} topics)", key, merged.topics.len());

    Ok(())
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
///     conflict. Output sorted by start_offset. Offset gaps are unioned, deduplicated by
///     start_offset.
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
                    // Pruned ranges are unioned FIRST: a segment either side
                    // recorded as deliberately deleted must never come back,
                    // even when the other side's in-memory manifest still
                    // lists it (e.g. a run that pruned mid-flight, or a prune
                    // that raced this run's save).
                    for range in cur_part.pruned.clone() {
                        ex_part.add_pruned(range);
                    }
                    let inside_pruned = |seg: &SegmentMetadata, part: &PartitionBackup| {
                        part.pruned.iter().any(|r| {
                            seg.start_offset >= r.start_offset && seg.end_offset <= r.end_offset
                        })
                    };
                    ex_part.segments = {
                        let pruned_view = ex_part.clone();
                        ex_part
                            .segments
                            .drain(..)
                            .filter(|seg| !inside_pruned(seg, &pruned_view))
                            .collect()
                    };
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
                        if inside_pruned(&seg, ex_part) {
                            continue;
                        }
                        if !seen_keys.contains_key(&seg.key)
                            && !seen_offsets.contains_key(&seg.start_offset)
                        {
                            seen_keys.insert(seg.key.clone(), ());
                            seen_offsets.insert(seg.start_offset, ());
                            ex_part.segments.push(seg);
                        }
                    }
                    ex_part.segments.sort_by_key(|s| s.start_offset);
                    // Gaps recorded by the current session are unioned in;
                    // add_gap dedups by start_offset and keeps them sorted.
                    for gap in cur_part.gaps {
                        ex_part.add_gap(gap);
                    }
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

fn fail_if_literal_topic_includes_missing(
    selection: &TopicSelection,
    existing_topics: &[TopicMetadata],
) -> Result<()> {
    let missing = missing_literal_topic_includes(selection, existing_topics);
    if missing.is_empty() {
        return Ok(());
    }

    Err(Error::TopicNotFound(format!(
        "configured backup topic(s) not found in Kafka cluster: {}",
        missing.join(", ")
    )))
}

fn missing_literal_topic_includes(
    selection: &TopicSelection,
    existing_topics: &[TopicMetadata],
) -> Vec<String> {
    let existing_names: HashSet<&str> = existing_topics
        .iter()
        .map(|topic| topic.name.as_str())
        .collect();

    let mut missing: Vec<String> = selection
        .include
        .iter()
        .filter(|topic| is_literal_topic_pattern(topic))
        .filter(|topic| !existing_names.contains(topic.as_str()))
        .cloned()
        .collect();

    missing.sort();
    missing.dedup();
    missing
}

fn is_literal_topic_pattern(pattern: &str) -> bool {
    !pattern.contains('*') && !pattern.contains('?')
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

/// Determine whether the offset store should be created.
///
/// The offset store is created when continuous mode is enabled (backward compat)
/// OR when the user explicitly configures `offset_storage` in their YAML config,
/// enabling incremental one-shot and snapshot backups.
fn should_create_offset_store(continuous: bool, offset_storage_configured: bool) -> bool {
    continuous || offset_storage_configured
}

/// One-line startup summary of the headers this backup will add to every
/// archived record, so the (default-on) behaviour is visible in the logs
/// (issue #154).
fn describe_offset_headers(options: &BackupOptions) -> String {
    if !options.include_offset_headers {
        return "include_offset_headers=false: records are archived with their original headers only"
            .to_string();
    }
    let cluster = match &options.source_cluster_id {
        Some(id) => format!(" and {}={id}", offset_headers::X_SOURCE_CLUSTER),
        None => String::new(),
    };
    format!(
        "include_offset_headers=true (default): every archived record gets {} and {}{cluster} \
         headers for header-based consumer offset recovery; set backup.include_offset_headers: \
         false for a header-for-header identical archive, or restore.strip_offset_headers: true \
         to drop them at restore time",
        offset_headers::X_ORIGINAL_OFFSET,
        offset_headers::X_ORIGINAL_TIMESTAMP,
    )
}

/// Convert a fetched record into the on-disk segment representation.
///
/// Header values are copied as-is: a null header value stays `None` and is
/// written with a `-1` length by the segment format (issue #155). When
/// `include_offset_headers` is set, the Phase 1 offset-mapping headers
/// (`x-original-offset`, `x-original-timestamp`, and `x-source-cluster` if a
/// `source_cluster_id` is configured) are appended after the record's own
/// headers, as binary little-endian i64 values.
fn to_binary_record(record: &BackupRecord, options: &BackupOptions) -> BinaryRecord {
    let mut headers: Vec<(String, Option<Bytes>)> = record
        .headers
        .iter()
        .map(|h| (h.key.clone(), h.value.clone().map(Bytes::from)))
        .collect();

    if options.include_offset_headers {
        headers.push((
            offset_headers::X_ORIGINAL_OFFSET.to_string(),
            Some(Bytes::from(record.offset.to_le_bytes().to_vec())),
        ));
        headers.push((
            offset_headers::X_ORIGINAL_TIMESTAMP.to_string(),
            Some(Bytes::from(record.timestamp.to_le_bytes().to_vec())),
        ));
        if let Some(cluster_id) = &options.source_cluster_id {
            headers.push((
                offset_headers::X_SOURCE_CLUSTER.to_string(),
                Some(Bytes::from(cluster_id.as_bytes().to_vec())),
            ));
        }
    }

    BinaryRecord {
        timestamp: record.timestamp,
        offset: record.offset,
        key: record.key.as_ref().map(|k| Bytes::from(k.clone())),
        value: record.value.as_ref().map(|v| Bytes::from(v.clone())),
        headers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{PartitionBackup, RecordHeader, TopicBackup};

    // ------------------------------------------------------------------
    // Snapshot planning (strimzi-backup-operator#57)
    //
    // `kafka_backup_snapshot_records_target` / `_remaining` must describe the
    // work of *this* run. An incremental run that resumes from checkpoints
    // must not start its gauges at the size of the whole archive.
    // ------------------------------------------------------------------

    fn tp(topic: &str, partition: i32) -> (String, i32) {
        (topic.to_string(), partition)
    }

    fn offsets(entries: &[(&str, i32, i64, i64)]) -> HashMap<(String, i32), (i64, i64)> {
        entries
            .iter()
            .map(|(t, p, earliest, latest)| (tp(t, *p), (*earliest, *latest)))
            .collect()
    }

    fn checkpoints(entries: &[(&str, i32, i64)]) -> HashMap<(String, i32), i64> {
        entries
            .iter()
            .map(|(t, p, last)| (tp(t, *p), *last))
            .collect()
    }

    fn planned_total(ranges: &HashMap<(String, i32), SnapshotRange>) -> i64 {
        ranges.values().map(SnapshotRange::planned_records).sum()
    }

    #[test]
    fn first_run_without_checkpoints_plans_the_full_captured_range() {
        let ranges = plan_snapshot_ranges(
            offsets(&[("orders", 0, 0, 1_000), ("orders", 1, 250, 1_250)]),
            &checkpoints(&[]),
            &StartOffset::Earliest,
        );

        assert_eq!(
            ranges[&tp("orders", 0)],
            SnapshotRange {
                earliest: 0,
                start: 0,
                latest: 1_000
            }
        );
        assert_eq!(
            ranges[&tp("orders", 1)],
            SnapshotRange {
                earliest: 250,
                start: 250,
                latest: 1_250
            }
        );
        assert_eq!(planned_total(&ranges), 2_000);
    }

    #[test]
    fn incremental_run_plans_only_offsets_after_the_checkpoint() {
        // Previous run checkpointed offset 899 of 1_000 -> 100 new records.
        let ranges = plan_snapshot_ranges(
            offsets(&[("orders", 0, 0, 1_000)]),
            &checkpoints(&[("orders", 0, 899)]),
            &StartOffset::Earliest,
        );

        let range = ranges[&tp("orders", 0)];
        assert_eq!(range.start, 900);
        assert_eq!(range.planned_records(), 100);
        assert_eq!(range.captured_records(), 1_000);
    }

    #[test]
    fn caught_up_partition_plans_zero_records() {
        // Checkpoint is the last offset in the log: nothing new since last run.
        let ranges = plan_snapshot_ranges(
            offsets(&[("orders", 0, 0, 1_000)]),
            &checkpoints(&[("orders", 0, 999)]),
            &StartOffset::Earliest,
        );

        assert_eq!(ranges[&tp("orders", 0)].planned_records(), 0);
    }

    #[test]
    fn checkpoint_beyond_the_captured_watermark_never_goes_negative() {
        // A recreated/truncated topic can leave a checkpoint past the new
        // high watermark; the plan must clamp instead of counting backwards.
        let ranges = plan_snapshot_ranges(
            offsets(&[("orders", 0, 0, 100)]),
            &checkpoints(&[("orders", 0, 5_000)]),
            &StartOffset::Earliest,
        );

        assert_eq!(ranges[&tp("orders", 0)].planned_records(), 0);
        assert_eq!(planned_total(&ranges), 0);
    }

    #[test]
    fn retention_gap_since_last_run_counts_from_the_checkpoint() {
        // Log start moved past the checkpoint: the fetch loop resumes at
        // checkpoint+1, hits OFFSET_OUT_OF_RANGE, records the gap and advances
        // the progress gauge by the gap span — so the plan counts from the
        // checkpoint too, keeping target and remaining consistent.
        let ranges = plan_snapshot_ranges(
            offsets(&[("orders", 0, 500, 1_000)]),
            &checkpoints(&[("orders", 0, 99)]),
            &StartOffset::Earliest,
        );

        let range = ranges[&tp("orders", 0)];
        assert_eq!(range.start, 100);
        assert_eq!(range.planned_records(), 900);
        assert_eq!(range.captured_records(), 500);
    }

    #[test]
    fn checkpoint_wins_over_configured_start_offset() {
        let mut specific = HashMap::new();
        specific.insert("orders".to_string(), HashMap::from([(0, 300_i64)]));

        let ranges = plan_snapshot_ranges(
            offsets(&[("orders", 0, 0, 1_000)]),
            &checkpoints(&[("orders", 0, 599)]),
            &StartOffset::Specific(specific),
        );

        assert_eq!(ranges[&tp("orders", 0)].start, 600);
    }

    #[test]
    fn configured_specific_offset_applies_without_a_checkpoint() {
        let mut specific = HashMap::new();
        specific.insert("orders".to_string(), HashMap::from([(0, 300_i64)]));

        let ranges = plan_snapshot_ranges(
            offsets(&[("orders", 0, 0, 1_000), ("orders", 1, 0, 1_000)]),
            &checkpoints(&[]),
            &StartOffset::Specific(specific),
        );

        // Partition 0 has an explicit start; partition 1 falls back to earliest.
        assert_eq!(ranges[&tp("orders", 0)].planned_records(), 700);
        assert_eq!(ranges[&tp("orders", 1)].planned_records(), 1_000);
        assert_eq!(planned_total(&ranges), 1_700);
    }

    #[test]
    fn configured_latest_start_keeps_backup_semantics_of_earliest() {
        // For backups `latest` has always meant "start fresh from earliest";
        // planning must match what backup_partition() actually fetches.
        assert_eq!(
            configured_start_offset(&StartOffset::Latest, "orders", 0, 42),
            42
        );
        assert_eq!(
            configured_start_offset(&StartOffset::Earliest, "orders", 0, 42),
            42
        );
    }

    #[test]
    fn checkpoints_for_other_partitions_do_not_leak() {
        let ranges = plan_snapshot_ranges(
            offsets(&[("orders", 0, 0, 100), ("orders", 1, 0, 100)]),
            &checkpoints(&[("orders", 1, 49), ("payments", 0, 99)]),
            &StartOffset::Earliest,
        );

        assert_eq!(ranges[&tp("orders", 0)].planned_records(), 100);
        assert_eq!(ranges[&tp("orders", 1)].planned_records(), 50);
        assert_eq!(ranges.len(), 2, "plan covers captured partitions only");
    }

    #[test]
    fn mixed_partitions_sum_to_this_runs_work_not_the_archive_size() {
        // The reporter's shape: a huge archive, a few new records per run.
        let ranges = plan_snapshot_ranges(
            offsets(&[
                ("big", 0, 0, 8_000_000_000),
                ("big", 1, 0, 8_000_000_000),
                ("busy", 0, 0, 3_000_000),
            ]),
            &checkpoints(&[
                ("big", 0, 7_999_999_999),
                ("big", 1, 7_999_999_999),
                ("busy", 0, 1_999_999),
            ]),
            &StartOffset::Earliest,
        );

        assert_eq!(planned_total(&ranges), 1_000_000);
        let captured: i64 = ranges.values().map(SnapshotRange::captured_records).sum();
        assert_eq!(captured, 16_003_000_000);
    }

    fn record_with_headers(headers: Vec<RecordHeader>) -> BackupRecord {
        BackupRecord {
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers,
            timestamp: 1_700_000_000_000,
            offset: 42,
        }
    }

    fn header(key: &str, value: Option<&[u8]>) -> RecordHeader {
        RecordHeader {
            key: key.to_string(),
            value: value.map(|v| v.to_vec()),
        }
    }

    /// Issue #155: a null header value must reach the segment writer as
    /// `None` (encoded as length -1), not be flattened into an empty value.
    #[test]
    fn to_binary_record_preserves_null_and_empty_header_values() {
        let record = record_with_headers(vec![
            header("trace-id", None),
            header("empty", Some(b"")),
            header("tenant", Some(b"42")),
        ]);
        let options = BackupOptions {
            include_offset_headers: false,
            ..BackupOptions::default()
        };

        let binary = to_binary_record(&record, &options);

        assert_eq!(
            binary.headers,
            vec![
                ("trace-id".to_string(), None),
                ("empty".to_string(), Some(Bytes::new())),
                ("tenant".to_string(), Some(Bytes::from_static(b"42"))),
            ]
        );
        assert_eq!(binary.offset, 42);
        assert_eq!(binary.timestamp, 1_700_000_000_000);
        assert_eq!(binary.key.as_deref(), Some(&b"k"[..]));
        assert_eq!(binary.value.as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn to_binary_record_appends_offset_headers_after_user_headers() {
        let record = record_with_headers(vec![header("trace-id", None)]);
        let options = BackupOptions {
            include_offset_headers: true,
            source_cluster_id: Some("src-eu".to_string()),
            ..BackupOptions::default()
        };

        let binary = to_binary_record(&record, &options);

        let keys: Vec<&str> = binary.headers.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(
            keys,
            [
                "trace-id",
                "x-original-offset",
                "x-original-timestamp",
                "x-source-cluster"
            ]
        );
        assert_eq!(binary.headers[0].1, None);
        assert_eq!(
            binary.headers[1].1.as_deref(),
            Some(&42i64.to_le_bytes()[..])
        );
        assert_eq!(
            binary.headers[2].1.as_deref(),
            Some(&1_700_000_000_000i64.to_le_bytes()[..])
        );
        assert_eq!(binary.headers[3].1.as_deref(), Some(&b"src-eu"[..]));
    }

    #[test]
    fn describe_offset_headers_names_the_headers_and_the_opt_outs() {
        let on = describe_offset_headers(&BackupOptions::default());
        assert!(
            on.starts_with("include_offset_headers=true (default)"),
            "{on}"
        );
        for needle in [
            "x-original-offset",
            "x-original-timestamp",
            "backup.include_offset_headers: false",
            "restore.strip_offset_headers: true",
        ] {
            assert!(on.contains(needle), "missing {needle:?} in {on}");
        }
        assert!(!on.contains("x-source-cluster"));

        let with_cluster = describe_offset_headers(&BackupOptions {
            source_cluster_id: Some("eu-prod".to_string()),
            ..BackupOptions::default()
        });
        assert!(
            with_cluster.contains("x-source-cluster=eu-prod"),
            "{with_cluster}"
        );

        let off = describe_offset_headers(&BackupOptions {
            include_offset_headers: false,
            ..BackupOptions::default()
        });
        assert!(off.starts_with("include_offset_headers=false"), "{off}");
        assert!(!off.contains("x-original-offset"));
    }

    #[test]
    fn to_binary_record_without_offset_headers_adds_nothing() {
        let record = record_with_headers(vec![]);
        let options = BackupOptions {
            include_offset_headers: false,
            source_cluster_id: Some("ignored".to_string()),
            ..BackupOptions::default()
        };
        assert!(to_binary_record(&record, &options).headers.is_empty());
    }

    #[test]
    fn fetch_max_bytes_defaults_to_capped_segment_size() {
        let mut opts = BackupOptions {
            segment_max_bytes: 512 * 1024,
            ..BackupOptions::default()
        };
        assert_eq!(effective_fetch_max_bytes(&opts), 512 * 1024);

        opts.segment_max_bytes = 256 * 1024 * 1024;
        assert_eq!(effective_fetch_max_bytes(&opts), 16 * 1024 * 1024);
    }

    #[test]
    fn explicit_fetch_max_bytes_wins_over_segment_size() {
        let opts = BackupOptions {
            segment_max_bytes: 256 * 1024 * 1024,
            fetch_max_bytes: Some(64 * 1024 * 1024),
            ..BackupOptions::default()
        };
        assert_eq!(effective_fetch_max_bytes(&opts), 64 * 1024 * 1024);
    }

    #[test]
    fn fetch_max_bytes_is_clamped_to_protocol_range() {
        let opts = BackupOptions {
            fetch_max_bytes: Some(u64::MAX),
            ..BackupOptions::default()
        };
        assert_eq!(effective_fetch_max_bytes(&opts), i32::MAX);

        let opts = BackupOptions {
            fetch_max_bytes: Some(0),
            ..BackupOptions::default()
        };
        assert_eq!(effective_fetch_max_bytes(&opts), 1);
    }

    #[test]
    fn topic_config_capture_allowlist_excludes_unknown_entries() {
        assert!(super::is_recovery_topic_config("cleanup.policy"));
        assert!(super::is_recovery_topic_config("min.insync.replicas"));
        assert!(!super::is_recovery_topic_config("vendor.secret.option"));
    }

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

    fn topic_metadata(name: &str) -> TopicMetadata {
        TopicMetadata {
            name: name.to_string(),
            is_internal: false,
            partitions: Vec::new(),
        }
    }

    #[test]
    fn missing_literal_topic_includes_reports_absent_exact_topics() {
        let selection = TopicSelection {
            include: vec![
                "orders".to_string(),
                "payments".to_string(),
                "orders".to_string(),
            ],
            exclude: vec![],
        };
        let existing = vec![topic_metadata("orders")];

        assert_eq!(
            missing_literal_topic_includes(&selection, &existing),
            vec!["payments".to_string()]
        );
    }

    #[test]
    fn missing_literal_topic_includes_ignores_globs_and_empty_include() {
        let existing = vec![topic_metadata("orders")];

        let glob_selection = TopicSelection {
            include: vec!["missing-*".to_string(), "order?".to_string()],
            exclude: vec![],
        };
        assert!(missing_literal_topic_includes(&glob_selection, &existing).is_empty());

        let all_topics_selection = TopicSelection {
            include: vec![],
            exclude: vec![],
        };
        assert!(missing_literal_topic_includes(&all_topics_selection, &existing).is_empty());
    }

    // -- should_create_offset_store tests --

    #[test]
    fn test_offset_store_created_for_continuous() {
        assert!(should_create_offset_store(true, false));
    }

    #[test]
    fn test_offset_store_created_for_explicit_config() {
        assert!(should_create_offset_store(false, true));
    }

    #[test]
    fn test_offset_store_not_created_default_oneshot() {
        assert!(!should_create_offset_store(false, false));
    }

    #[test]
    fn test_offset_store_created_for_both() {
        assert!(should_create_offset_store(true, true));
    }

    // -- merge_manifests tests --

    fn make_segment(key: &str, start_offset: i64, end_offset: i64) -> SegmentMetadata {
        SegmentMetadata {
            key: key.to_string(),
            start_offset,
            end_offset,
            start_timestamp: start_offset * 1000,
            end_timestamp: end_offset * 1000,
            record_count: end_offset - start_offset + 1,
            uncompressed_size: 0,
            compressed_size: 0,
            sha256: String::new(),
            uploaded_at: 0,
        }
    }

    fn make_partition(id: i32, segments: Vec<SegmentMetadata>) -> PartitionBackup {
        PartitionBackup {
            partition_id: id,
            segments,
            gaps: Vec::new(),
            pruned: Vec::new(),
        }
    }

    fn make_topic(
        name: &str,
        partition_count: Option<i32>,
        partitions: Vec<PartitionBackup>,
    ) -> TopicBackup {
        TopicBackup {
            name: name.to_string(),
            original_partition_count: partition_count,
            source_replication_factor: None,
            configurations: Default::default(),
            partitions,
        }
    }

    fn count_segments(manifest: &BackupManifest) -> usize {
        manifest
            .topics
            .iter()
            .flat_map(|topic| &topic.partitions)
            .map(|partition| partition.segments.len())
            .sum()
    }

    #[tokio::test]
    async fn test_manifest_persistence_writes_initial_manifest() {
        let storage: Arc<dyn StorageBackend> = Arc::new(crate::storage::MemoryBackend::new());
        let manifest = Arc::new(Mutex::new(BackupManifest::new("backup-1".to_string())));
        {
            let mut manifest_guard = manifest.lock().await;
            manifest_guard
                .topics
                .push(make_topic("orders", Some(3), Vec::new()));
        }

        let persistence = ManifestPersistence::new(
            "backup-1".to_string(),
            storage.clone(),
            manifest,
            Duration::from_secs(30),
        );

        persistence.save_now().await.unwrap();

        let data = storage.get("backup-1/manifest.json").await.unwrap();
        let stored: BackupManifest = serde_json::from_slice(&data).unwrap();
        assert_eq!(stored.backup_id, "backup-1");
        assert_eq!(stored.topics[0].name, "orders");
        assert_eq!(stored.topics[0].original_partition_count, Some(3));
    }

    #[tokio::test]
    async fn test_manifest_progress_save_persists_first_segment_and_throttles() {
        let storage: Arc<dyn StorageBackend> = Arc::new(crate::storage::MemoryBackend::new());
        let manifest = Arc::new(Mutex::new(BackupManifest::new("backup-1".to_string())));
        let persistence = ManifestPersistence::new(
            "backup-1".to_string(),
            storage.clone(),
            manifest.clone(),
            Duration::from_secs(3600),
        );

        {
            let mut manifest_guard = manifest.lock().await;
            manifest_guard.topics.push(make_topic(
                "orders",
                Some(1),
                vec![make_partition(0, vec![make_segment("seg-a", 0, 9)])],
            ));
        }
        persistence.save_progress().await.unwrap();

        let data = storage.get("backup-1/manifest.json").await.unwrap();
        let stored: BackupManifest = serde_json::from_slice(&data).unwrap();
        assert_eq!(count_segments(&stored), 1);

        {
            let mut manifest_guard = manifest.lock().await;
            manifest_guard.topics[0].partitions[0]
                .segments
                .push(make_segment("seg-b", 10, 19));
        }
        persistence.save_progress().await.unwrap();

        let data = storage.get("backup-1/manifest.json").await.unwrap();
        let stored: BackupManifest = serde_json::from_slice(&data).unwrap();
        assert_eq!(count_segments(&stored), 1);

        persistence.save_now().await.unwrap();

        let data = storage.get("backup-1/manifest.json").await.unwrap();
        let stored: BackupManifest = serde_json::from_slice(&data).unwrap();
        assert_eq!(count_segments(&stored), 2);
    }

    #[tokio::test]
    async fn test_offset_persistence_syncs_remote_offset_database() {
        use crate::offset_store::{OffsetStore, OffsetStoreConfig};

        let temp_dir = tempfile::TempDir::new().unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(crate::storage::MemoryBackend::new());
        let offset_store = Arc::new(
            SqliteOffsetStore::new(OffsetStoreConfig {
                db_path: temp_dir.path().join("offsets.db"),
                ..Default::default()
            })
            .await
            .unwrap(),
        );

        offset_store
            .get_or_create_job("backup-1", None)
            .await
            .unwrap();
        offset_store
            .set_offset("backup-1", "orders", 0, 42)
            .await
            .unwrap();

        let persistence = OffsetPersistence::new(
            "backup-1".to_string(),
            storage.clone(),
            offset_store,
            Duration::from_secs(30),
        );

        persistence.sync_now().await.unwrap();
        assert!(storage.exists("backup-1/offsets.db").await.unwrap());
    }

    #[test]
    fn test_merge_manifests_disjoint_topics() {
        let mut existing = BackupManifest::new("test".to_string());
        existing.topics.push(make_topic(
            "orders",
            Some(1),
            vec![make_partition(0, vec![make_segment("seg-a", 0, 99)])],
        ));

        let mut current = BackupManifest::new("test".to_string());
        current.topics.push(make_topic(
            "payments",
            Some(1),
            vec![make_partition(0, vec![make_segment("seg-b", 0, 49)])],
        ));

        let merged = merge_manifests(existing, current);
        assert_eq!(merged.topics.len(), 2);
        assert!(merged.topics.iter().any(|t| t.name == "orders"));
        assert!(merged.topics.iter().any(|t| t.name == "payments"));
    }

    #[test]
    fn test_merge_manifests_same_topic_disjoint_partitions() {
        let mut existing = BackupManifest::new("test".to_string());
        existing.topics.push(make_topic(
            "orders",
            Some(2),
            vec![make_partition(0, vec![make_segment("seg-a", 0, 99)])],
        ));

        let mut current = BackupManifest::new("test".to_string());
        current.topics.push(make_topic(
            "orders",
            Some(2),
            vec![make_partition(1, vec![make_segment("seg-b", 0, 49)])],
        ));

        let merged = merge_manifests(existing, current);
        assert_eq!(merged.topics.len(), 1);
        assert_eq!(merged.topics[0].partitions.len(), 2);
    }

    #[test]
    fn test_merge_manifests_same_partition_dedup_by_offset() {
        let mut existing = BackupManifest::new("test".to_string());
        existing.topics.push(make_topic(
            "orders",
            Some(1),
            vec![make_partition(
                0,
                vec![
                    make_segment("seg-a", 0, 99),
                    make_segment("seg-b", 100, 199),
                ],
            )],
        ));

        let mut current = BackupManifest::new("test".to_string());
        current.topics.push(make_topic(
            "orders",
            Some(1),
            vec![make_partition(
                0,
                vec![
                    make_segment("seg-c", 100, 199), // overlaps by start_offset
                    make_segment("seg-d", 200, 299),
                ],
            )],
        ));

        let merged = merge_manifests(existing, current);
        let segs = &merged.topics[0].partitions[0].segments;
        assert_eq!(segs.len(), 3); // dedup removes duplicate offset 100
        assert_eq!(segs[0].start_offset, 0);
        assert_eq!(segs[1].start_offset, 100);
        assert_eq!(segs[1].key, "seg-b"); // existing wins
        assert_eq!(segs[2].start_offset, 200);
    }

    #[test]
    fn test_merge_manifests_same_key_dedup() {
        let mut existing = BackupManifest::new("test".to_string());
        existing.topics.push(make_topic(
            "orders",
            Some(1),
            vec![make_partition(0, vec![make_segment("seg-001", 0, 99)])],
        ));

        let mut current = BackupManifest::new("test".to_string());
        current.topics.push(make_topic(
            "orders",
            Some(1),
            vec![make_partition(
                0,
                vec![make_segment("seg-001", 50, 149)], // same key, different offset
            )],
        ));

        let merged = merge_manifests(existing, current);
        let segs = &merged.topics[0].partitions[0].segments;
        assert_eq!(segs.len(), 1); // dedup by key
        assert_eq!(segs[0].start_offset, 0); // existing wins
    }

    #[test]
    fn test_merge_manifests_updates_partition_count() {
        let mut existing = BackupManifest::new("test".to_string());
        existing.topics.push(make_topic("orders", Some(3), vec![]));

        let mut current = BackupManifest::new("test".to_string());
        current.topics.push(make_topic("orders", Some(6), vec![]));

        let merged = merge_manifests(existing, current);
        assert_eq!(merged.topics[0].original_partition_count, Some(6));
    }

    #[test]
    fn test_merge_manifests_preserves_partition_count_when_none() {
        let mut existing = BackupManifest::new("test".to_string());
        existing.topics.push(make_topic("orders", Some(3), vec![]));

        let mut current = BackupManifest::new("test".to_string());
        current.topics.push(make_topic("orders", None, vec![]));

        let merged = merge_manifests(existing, current);
        assert_eq!(merged.topics[0].original_partition_count, Some(3));
    }

    #[test]
    fn test_merge_manifests_empty_current() {
        let mut existing = BackupManifest::new("test".to_string());
        existing.topics.push(make_topic(
            "orders",
            Some(1),
            vec![make_partition(0, vec![make_segment("seg-a", 0, 99)])],
        ));

        let current = BackupManifest::new("test".to_string());

        let merged = merge_manifests(existing, current);
        assert_eq!(merged.topics.len(), 1);
        assert_eq!(merged.topics[0].partitions[0].segments.len(), 1);
    }

    #[test]
    fn test_merge_manifests_empty_existing() {
        let existing = BackupManifest::new("test".to_string());

        let mut current = BackupManifest::new("test".to_string());
        current.topics.push(make_topic(
            "orders",
            Some(1),
            vec![make_partition(0, vec![make_segment("seg-a", 0, 99)])],
        ));

        let merged = merge_manifests(existing, current);
        assert_eq!(merged.topics.len(), 1);
        assert_eq!(merged.topics[0].name, "orders");
    }

    // ------------------------------------------------------------------
    // Issue #144: OFFSET_OUT_OF_RANGE recovery
    // ------------------------------------------------------------------

    fn broker_error(code: i16) -> Error {
        Error::Kafka(KafkaError::BrokerError {
            code,
            message: format!("Fetch error for t:0: code {code}"),
        })
    }

    #[test]
    fn test_is_offset_out_of_range_matches_only_code_1() {
        assert!(is_offset_out_of_range(&broker_error(1)));
        // NOT_LEADER_FOR_PARTITION and other broker codes are not retention gaps.
        assert!(!is_offset_out_of_range(&broker_error(6)));
        assert!(!is_offset_out_of_range(&broker_error(0)));
        // Non-broker errors never match, even if they mention "code 1".
        assert!(!is_offset_out_of_range(&Error::Kafka(
            KafkaError::Protocol("code 1".to_string())
        )));
        assert!(!is_offset_out_of_range(&Error::Compression(
            "code 1".to_string()
        )));
    }

    #[test]
    fn test_out_of_range_error_keeps_code_1_and_explains_cause() {
        // Beyond the log end: truncation / topic recreated.
        let e = out_of_range_error("orders", 3, 5_000, 100, 4_000);
        assert!(is_offset_out_of_range(&e), "must still classify as code 1");
        let msg = e.to_string();
        assert!(msg.contains("orders:3"), "{msg}");
        assert!(msg.contains("5000"), "{msg}");
        assert!(msg.contains("[100, 4000)"), "{msg}");
        assert!(msg.contains("beyond the log end"), "{msg}");

        // Inside the reported range: leader change / replica inconsistency.
        let e = out_of_range_error("orders", 3, 2_000, 100, 4_000);
        assert!(is_offset_out_of_range(&e));
        assert!(e.to_string().contains("inside the broker's reported range"));
    }

    fn make_gap(start: i64, end: i64) -> OffsetGap {
        OffsetGap {
            start_offset: start,
            end_offset: end,
            reason: OffsetGapReason::OffsetOutOfRange,
            detected_at: 1_700_000_000_000,
        }
    }

    #[test]
    fn merge_keeps_pruned_ranges_and_does_not_resurrect_pruned_segments() {
        use crate::manifest::{PruneReason, PrunedRange};
        let range = PrunedRange {
            start_offset: 0,
            end_offset: 9,
            segments: 1,
            bytes: 100,
            pruned_at: 5,
            cutoff_timestamp: 4,
            reason: PruneReason::Retention,
        };

        // Existing (stored) manifest: segment pruned away, range recorded.
        let mut existing_part = make_partition(0, vec![make_segment("k2", 10, 19)]);
        existing_part.add_pruned(range.clone());
        let existing = {
            let mut m = BackupManifest::new("b".to_string());
            m.topics.push(make_topic("t", Some(1), vec![existing_part]));
            m
        };

        // Current (in-memory) manifest of a still-running process: it wrote
        // the pruned segment this run and still lists it.
        let current = {
            let mut m = BackupManifest::new("b".to_string());
            m.topics.push(make_topic(
                "t",
                Some(1),
                vec![make_partition(
                    0,
                    vec![make_segment("k1", 0, 9), make_segment("k2", 10, 19)],
                )],
            ));
            m
        };

        let merged = merge_manifests(existing, current);
        let partition = &merged.topics[0].partitions[0];
        assert_eq!(partition.pruned, vec![range]);
        assert_eq!(
            partition
                .segments
                .iter()
                .map(|s| s.key.as_str())
                .collect::<Vec<_>>(),
            vec!["k2"],
            "the pruned segment must not be resurrected by the merge"
        );
    }

    #[test]
    fn merge_drops_existing_segments_inside_a_newly_pruned_range() {
        use crate::manifest::{PruneReason, PrunedRange};
        let range = PrunedRange {
            start_offset: 0,
            end_offset: 9,
            segments: 1,
            bytes: 100,
            pruned_at: 5,
            cutoff_timestamp: 4,
            reason: PruneReason::Manual,
        };

        // Existing (stored) manifest still lists the segment; the current
        // side (an engine that pruned in memory) carries the range.
        let existing = {
            let mut m = BackupManifest::new("b".to_string());
            m.topics.push(make_topic(
                "t",
                Some(1),
                vec![make_partition(
                    0,
                    vec![make_segment("k1", 0, 9), make_segment("k2", 10, 19)],
                )],
            ));
            m
        };
        let current = {
            let mut part = make_partition(0, vec![make_segment("k2", 10, 19)]);
            part.add_pruned(range);
            let mut m = BackupManifest::new("b".to_string());
            m.topics.push(make_topic("t", Some(1), vec![part]));
            m
        };

        let merged = merge_manifests(existing, current);
        let partition = &merged.topics[0].partitions[0];
        assert_eq!(
            partition
                .segments
                .iter()
                .map(|s| s.key.as_str())
                .collect::<Vec<_>>(),
            vec!["k2"]
        );
        assert_eq!(partition.pruned.len(), 1);
    }

    #[test]
    fn test_merge_manifests_unions_and_dedups_gaps() {
        // Existing manifest (from an earlier save in the same run, or an
        // earlier run) already carries a gap; the current session re-detected
        // the same gap and found a new one.
        let mut ex_part = make_partition(0, vec![make_segment("seg-a", 200, 299)]);
        ex_part.add_gap(make_gap(100, 200));
        let mut existing = BackupManifest::new("test".to_string());
        existing
            .topics
            .push(make_topic("orders", Some(1), vec![ex_part]));

        let mut cur_part = make_partition(0, vec![make_segment("seg-b", 500, 599)]);
        cur_part.add_gap(make_gap(100, 200)); // duplicate — must not double count
        cur_part.add_gap(make_gap(300, 500)); // new
        let mut current = BackupManifest::new("test".to_string());
        current
            .topics
            .push(make_topic("orders", Some(1), vec![cur_part]));

        let merged = merge_manifests(existing, current);
        let part = &merged.topics[0].partitions[0];
        assert_eq!(part.segments.len(), 2);
        assert_eq!(
            part.gaps,
            vec![make_gap(100, 200), make_gap(300, 500)],
            "gaps are unioned, deduplicated by start_offset, and sorted"
        );
        assert_eq!(merged.total_gaps(), 2);
    }

    #[test]
    fn test_merge_manifests_keeps_gaps_on_new_partition() {
        // Partition only in `current` is appended wholesale, gaps included.
        let mut cur_part = make_partition(1, vec![make_segment("seg-b", 50, 99)]);
        cur_part.add_gap(make_gap(0, 50));
        let mut current = BackupManifest::new("test".to_string());
        current
            .topics
            .push(make_topic("orders", Some(2), vec![cur_part]));

        let mut existing = BackupManifest::new("test".to_string());
        existing.topics.push(make_topic(
            "orders",
            Some(2),
            vec![make_partition(0, vec![make_segment("seg-a", 0, 99)])],
        ));

        let merged = merge_manifests(existing, current);
        assert_eq!(merged.total_gaps(), 1);
        let (topic, partition, gap) = merged.gaps().next().unwrap();
        assert_eq!((topic, partition), ("orders", 1));
        assert_eq!(gap, &make_gap(0, 50));
    }
}
