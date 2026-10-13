//! Backup engine orchestration.

use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, Mutex, Semaphore};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use crate::compression::extension;
use crate::config::{
    BackupOptions, CompressionType, Config, IsolationLevel, Mode, StartOffset, TopicSelection,
};
use crate::error::KafkaError;
use crate::health::HealthCheck;
use crate::kafka::{
    ConfigResourceType, FetchResponse, PartitionFetcher, PartitionLeaderRouter, TopicMetadata,
};
use crate::manifest::{
    BackupManifest, BackupRecord, OffsetGap, OffsetGapReason, PartitionBackup, SegmentMetadata,
};
use crate::metrics::{ErrorType, PerformanceMetrics, PrometheusMetrics};
use crate::offset_headers;
use crate::offset_store::{OffsetStore, OffsetStoreConfig, SqliteOffsetStore};
use crate::segment::format::BinaryRecord;
use crate::segment::writer::{SegmentFlusher, SegmentWriter, SegmentWriterConfig};
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

/// `(topic, partition)`.
type PartitionKey = (String, i32);

/// Transaction markers and aborted records a `read_committed` backup passed
/// over in a range of offsets: `(control records, aborted records)`.
type TxnSkips = (u64, u64);

/// What a saved manifest accounts for in one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DurableReach {
    /// [`PartitionBackup::reach`]: the run resumes at `reach + 1`.
    reach: i64,
    /// [`PartitionBackup::explicit_reach`]: the next segment's covered range
    /// starts at `explicit + 1`, absorbing a trailing coverage-only advance.
    explicit: Option<i64>,
    /// Transaction records skipped in that trailing coverage-only range
    /// ([`PartitionBackup::tail_control_records_skipped`] /
    /// [`PartitionBackup::tail_aborted_records_dropped`]); the next segment
    /// takes them over.
    tail: TxnSkips,
}

impl DurableReach {
    /// Combine with a newer staged value: the furthest reach wins, and the
    /// tail counts belong to it (the newer value on a tie).
    fn merge(self, other: DurableReach) -> DurableReach {
        DurableReach {
            reach: self.reach.max(other.reach),
            explicit: match (self.explicit, other.explicit) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (a, b) => a.or(b),
            },
            tail: if self.reach > other.reach {
                self.tail
            } else {
                other.tail
            },
        }
    }
}

/// Persists the manifest and, strictly after it, the offset checkpoints the
/// saved manifest justifies.
///
/// Partition tasks *stage* progress once it is durable in storage — an
/// uploaded segment, a recorded gap, or a coverage-only advance (offsets
/// with no record) when nothing is buffered or in flight. Staging adds it to
/// the in-memory manifest and to `pending`. A save snapshots the manifest and
/// drains `pending` atomically, writes the merged manifest, and only then
/// sets those checkpoints in the offset store (I1/I2: a checkpoint never
/// exceeds what a saved manifest lists, and offsets.db is never written ahead
/// of the manifest). A failed save restages the drained entries.
struct ManifestPersistence {
    backup_id: String,
    storage: Arc<dyn StorageBackend>,
    manifest: Arc<Mutex<BackupManifest>>,
    save_lock: Mutex<()>,
    progress_lock: Mutex<Option<Instant>>,
    progress_interval: Duration,
    /// Offset checkpointing (absent when the run has no offset store).
    offsets: Option<Arc<OffsetPersistence>>,
    /// Progress staged since the last successful save. Always locked while
    /// holding the manifest lock, so a save drains exactly what its
    /// snapshot contains.
    pending: parking_lot::Mutex<HashMap<PartitionKey, DurableReach>>,
    /// What saved manifests account for: seeded from the stored manifest at
    /// startup, advanced after each successful save.
    durable: parking_lot::Mutex<HashMap<PartitionKey, DurableReach>>,
    /// Segment keys listed in the stored manifest at startup or staged
    /// since: writing one again would overwrite listed data (I3).
    listed_keys: parking_lot::Mutex<HashSet<String>>,
    /// Partitions whose legacy checkpoint was ahead of the manifest at
    /// startup; their first task records a `ResumeBeyondDurable` gap when the
    /// source has already deleted the offsets it goes back for.
    healed: parking_lot::Mutex<HashSet<PartitionKey>>,
}

impl ManifestPersistence {
    fn new(
        backup_id: String,
        storage: Arc<dyn StorageBackend>,
        manifest: Arc<Mutex<BackupManifest>>,
        progress_interval: Duration,
        offsets: Option<Arc<OffsetPersistence>>,
    ) -> Self {
        Self {
            backup_id,
            storage,
            manifest,
            save_lock: Mutex::new(()),
            progress_lock: Mutex::new(None),
            progress_interval,
            offsets,
            pending: parking_lot::Mutex::new(HashMap::new()),
            durable: parking_lot::Mutex::new(HashMap::new()),
            listed_keys: parking_lot::Mutex::new(HashSet::new()),
            healed: parking_lot::Mutex::new(HashSet::new()),
        }
    }

    /// Take the durable reach and listed segment keys from the manifest
    /// stored by earlier runs.
    fn seed(&self, stored: &BackupManifest) {
        let mut durable = self.durable.lock();
        let mut keys = self.listed_keys.lock();
        for topic in &stored.topics {
            for partition in &topic.partitions {
                keys.extend(partition.segments.iter().map(|s| s.key.clone()));
                if let Some(reach) = partition.reach() {
                    let entry = DurableReach {
                        reach,
                        explicit: partition.explicit_reach(),
                        tail: (
                            partition.tail_control_records_skipped,
                            partition.tail_aborted_records_dropped,
                        ),
                    };
                    durable
                        .entry((topic.name.clone(), partition.partition_id))
                        .and_modify(|d| *d = d.merge(entry))
                        .or_insert(entry);
                }
            }
        }
    }

    fn durable_reach(&self, key: &PartitionKey) -> Option<DurableReach> {
        self.durable.lock().get(key).copied()
    }

    fn durable_reaches(&self) -> HashMap<PartitionKey, DurableReach> {
        self.durable.lock().clone()
    }

    fn mark_healed(&self, key: PartitionKey) {
        self.healed.lock().insert(key);
    }

    fn take_healed(&self, key: &PartitionKey) -> bool {
        self.healed.lock().remove(key)
    }

    /// Fail loudly if `key` is already listed: a run must start beyond every
    /// listed segment (I3), so this is a bug, never an expected condition.
    fn ensure_unlisted(&self, key: &str) -> Result<()> {
        if self.listed_keys.lock().contains(key) {
            return Err(Error::Manifest(format!(
                "refusing to write segment {key}: the key is already listed in the manifest, \
                 so writing it would overwrite backed-up data (a backup must resume beyond \
                 every listed segment). This is a bug; please report it"
            )));
        }
        Ok(())
    }

    fn stage_locked(&self, key: PartitionKey, staged: DurableReach) {
        self.pending
            .lock()
            .entry(key)
            .and_modify(|d| *d = d.merge(staged))
            .or_insert(staged);
    }

    /// Stage an uploaded segment: list it and advance the partition's
    /// coverage to its covered end.
    async fn stage_segment(&self, topic: &str, partition: i32, segment: SegmentMetadata) {
        let covered_end = segment.covered_end();
        let key = segment.key.clone();
        let mut manifest = self.manifest.lock().await;
        let part = manifest
            .get_or_create_topic(topic)
            .get_or_create_partition(partition);
        part.add_segment(segment);
        part.covered_through = Some(
            part.covered_through
                .map_or(covered_end, |c| c.max(covered_end)),
        );
        // The segment absorbed any trailing range (and took its counts).
        part.tail_control_records_skipped = 0;
        part.tail_aborted_records_dropped = 0;
        self.listed_keys.lock().insert(key);
        self.stage_locked(
            (topic.to_string(), partition),
            DurableReach {
                reach: covered_end,
                explicit: Some(covered_end),
                tail: (0, 0),
            },
        );
    }

    /// Stage a coverage-only advance: every offset through `covered_end`
    /// is in a staged segment or held no record. Only valid with nothing
    /// buffered and no upload in flight for the partition. `tail` counts the
    /// transaction records skipped in the trailing range since the last
    /// segment.
    async fn stage_coverage(&self, topic: &str, partition: i32, covered_end: i64, tail: TxnSkips) {
        let mut manifest = self.manifest.lock().await;
        let part = manifest
            .get_or_create_topic(topic)
            .get_or_create_partition(partition);
        part.covered_through = Some(
            part.covered_through
                .map_or(covered_end, |c| c.max(covered_end)),
        );
        (
            part.tail_control_records_skipped,
            part.tail_aborted_records_dropped,
        ) = tail;
        self.stage_locked(
            (topic.to_string(), partition),
            DurableReach {
                reach: covered_end,
                explicit: None,
                tail,
            },
        );
    }

    /// Stage a recorded offset gap. Its end (exclusive) is accounted for, so
    /// it must only be staged after everything before it. `tail` counts the
    /// transaction records skipped since the last segment (just before the
    /// gap); they stay with the partition until the next segment takes them
    /// over.
    async fn stage_gap(&self, topic: &str, partition: i32, gap: OffsetGap, tail: TxnSkips) {
        let last = gap.end_offset - 1;
        let mut manifest = self.manifest.lock().await;
        let part = manifest
            .get_or_create_topic(topic)
            .get_or_create_partition(partition);
        part.add_gap(gap);
        part.covered_through = Some(part.covered_through.map_or(last, |c| c.max(last)));
        (
            part.tail_control_records_skipped,
            part.tail_aborted_records_dropped,
        ) = tail;
        self.stage_locked(
            (topic.to_string(), partition),
            DurableReach {
                reach: last,
                explicit: Some(last),
                tail,
            },
        );
    }

    fn restage(&self, staged: HashMap<PartitionKey, DurableReach>) {
        let mut pending = self.pending.lock();
        for (key, value) in staged {
            pending
                .entry(key)
                .and_modify(|d| *d = d.merge(value))
                .or_insert(value);
        }
    }

    /// Save the manifest now, then checkpoint what it justifies.
    async fn save_now(&self) -> Result<()> {
        let _guard = self.save_lock.lock().await;
        let (snapshot, staged) = {
            let manifest = self.manifest.lock().await;
            let staged = std::mem::take(&mut *self.pending.lock());
            (manifest.clone(), staged)
        };

        let saved = async {
            save_manifest_snapshot(self.storage.as_ref(), &self.backup_id, snapshot).await?;
            failpoint!("backup::manifest_put::after");
            Ok(())
        }
        .await;
        if let Err(e) = saved {
            self.restage(staged);
            return Err(e);
        }

        {
            let mut durable = self.durable.lock();
            for (key, value) in &staged {
                durable
                    .entry(key.clone())
                    .and_modify(|d| *d = d.merge(*value))
                    .or_insert(*value);
            }
        }

        // Only now — the manifest that lists these is in storage — may the
        // checkpoints move.
        if let Some(offsets) = &self.offsets {
            for ((topic, partition), reach) in &staged {
                offsets
                    .offset_store
                    .set_offset(&self.backup_id, topic, *partition, reach.reach)
                    .await?;
            }
            offsets.sync_if_due().await?;
        }
        Ok(())
    }

    /// Throttled [`save_now`](Self::save_now): at most once per progress
    /// interval, so the checkpoint may lag by up to one interval.
    async fn save_progress(&self) -> Result<()> {
        let mut last_save = self.progress_lock.lock().await;
        if last_save.is_some_and(|last| last.elapsed() < self.progress_interval) {
            return Ok(());
        }

        self.save_now().await?;
        *last_save = Some(Instant::now());
        Ok(())
    }

    /// End of a backup cycle: save the manifest, then write and upload the
    /// checkpoints.
    async fn end_cycle(&self) -> Result<()> {
        self.save_now().await?;
        if let Some(offsets) = &self.offsets {
            offsets.offset_store.checkpoint().await?;
            offsets.sync_now().await?;
        }
        Ok(())
    }

    /// Final persistence: manifest first, then checkpoints and the job
    /// status, then the offsets.db upload — so the stored copy records that
    /// the run finished (`prune` reads it to decide whether a run is live).
    async fn finalize(&self, job_status: Option<&str>) -> Result<()> {
        self.save_now().await?;
        if let Some(offsets) = &self.offsets {
            offsets.offset_store.checkpoint().await?;
            if let Some(status) = job_status {
                offsets
                    .offset_store
                    .update_job_status(&self.backup_id, status)
                    .await?;
            }
            offsets.sync_now().await?;
        }
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

/// Remote key of the offset database for a backup set. Fixed by design —
/// `prune`, `status` and the operators all assume it (issue #161).
pub fn offsets_key(backup_id: &str) -> String {
    format!("{}/offsets.db", backup_id)
}

/// `offset_storage.sync_interval_secs` overrides `backup.sync_interval_secs`
/// for the offset database sync; the backup value applies when unset.
fn effective_offset_sync_secs(
    offset_storage: Option<&crate::config::OffsetStorageConfig>,
    backup: &crate::config::BackupOptions,
) -> u64 {
    offset_storage
        .and_then(|os| os.sync_interval_secs)
        .unwrap_or(backup.sync_interval_secs)
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
        self.upload().await?;
        *last_sync = Some(Instant::now());
        Ok(())
    }

    async fn sync_if_due(&self) -> Result<()> {
        let mut last_sync = self.sync_lock.lock().await;
        if last_sync.is_some_and(|last| last.elapsed() < self.sync_interval) {
            return Ok(());
        }
        self.upload().await?;
        *last_sync = Some(Instant::now());
        Ok(())
    }

    async fn upload(&self) -> Result<()> {
        failpoint!("backup::offsets_sync::before");
        self.offset_store
            .sync_to_storage(self.storage.as_ref(), &offsets_key(&self.backup_id))
            .await
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

        // Initialize offset store for continuous backups or when explicitly configured
        let backup_opts = config.backup.clone().unwrap_or_default();
        manifest.isolation_level = Some(backup_opts.isolation_level);

        // Initialize circuit breakers (Kafka breaker from backup options —
        // advisory health signal; see CircuitBreakerSettings / issue #197).
        let kafka_circuit_breaker = Arc::new(CircuitBreaker::from_settings(
            &backup_opts.circuit_breaker,
            "kafka",
        ));

        let storage_circuit_breaker = Arc::new(CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            reset_timeout: Duration::from_secs(60),
            success_threshold: 1,
            name: "storage".to_string(),
            enabled: true,
        }));

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
                s3_key: Some(offsets_key(&config.backup_id)),
                checkpoint_interval_secs: backup_opts.checkpoint_interval_secs,
                sync_interval_secs: effective_offset_sync_secs(
                    config.offset_storage.as_ref(),
                    &backup_opts,
                ),
            };
            Some(Arc::new(SqliteOffsetStore::new(offset_config).await?))
        } else {
            None
        };

        let (shutdown_tx, _) = broadcast::channel(1);

        let manifest = Arc::new(Mutex::new(manifest));
        let offset_persistence = offset_store.as_ref().map(|offset_store| {
            Arc::new(OffsetPersistence::new(
                config.backup_id.clone(),
                storage.clone(),
                offset_store.clone(),
                Duration::from_secs(effective_offset_sync_secs(
                    config.offset_storage.as_ref(),
                    &backup_opts,
                )),
            ))
        });
        let manifest_persistence = Arc::new(ManifestPersistence::new(
            config.backup_id.clone(),
            storage.clone(),
            manifest.clone(),
            Duration::from_secs(backup_opts.sync_interval_secs),
            offset_persistence.clone(),
        ));

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

        // Load the remote offset database (preferred over a local copy).
        if let Some(ref offset_store) = self.offset_store {
            if let Err(e) = offset_store
                .try_load_from_storage(self.storage.as_ref(), &offsets_key(&self.config.backup_id))
                .await
            {
                warn!("Failed to load offset store from remote: {}", e);
            }
        }

        // The stored manifest is the source of truth for where every
        // partition resumes (I3). A failed read fails the run (#218).
        let isolation = self
            .config
            .backup
            .as_ref()
            .map(|b| b.isolation_level)
            .unwrap_or_default();
        if let Some(stored) = self.load_stored_manifest().await? {
            warn_on_isolation_change(&self.config.backup_id, stored.isolation_level, isolation);
            self.manifest_persistence.seed(&stored);
        }
        self.reconcile_checkpoints().await?;

        if let Some(ref offset_store) = self.offset_store {
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
        // Partition tasks stop fetching, flush and stage when this fires.
        let cancel = CancellationToken::new();
        let shutdown_grace = Duration::from_secs(backup_opts.shutdown_grace_secs);

        // Create semaphore to limit concurrent partition backups
        let semaphore = Arc::new(Semaphore::new(backup_opts.max_concurrent_partitions));

        info!(
            "Backup engine starting with max_concurrent_partitions={}, poll_interval_ms={}",
            backup_opts.max_concurrent_partitions, backup_opts.poll_interval_ms
        );
        info!("{}", describe_offset_headers(&backup_opts));
        info!("{}", describe_isolation_level(backup_opts.isolation_level));

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
                        break;
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
                        self.capture_snapshot_offsets(
                            &topics_metadata,
                            &backup_opts.start_offset,
                            backup_opts.isolation_level,
                        )
                        .await?,
                    )
                } else {
                    if backup_opts.isolation_level == IsolationLevel::ReadCommitted {
                        // Partitions caught up to their LSO do not fetch, so
                        // refresh the LSO lag of every partition here (two
                        // batched requests per broker) to keep a hanging
                        // transaction visible.
                        self.observe_lso_lag(&topics_metadata).await;
                    }
                    None
                };

            let mut tasks: JoinSet<(String, i32, Result<()>)> = JoinSet::new();
            let mut shutdown_requested = false;

            'spawn: for topic_meta in &topics_metadata {
                let topic = &topic_meta.name;

                // Check for shutdown before spawning
                if shutdown_rx.try_recv().is_ok() {
                    shutdown_requested = true;
                    break 'spawn;
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
                        router: Arc::clone(&self.router) as Arc<dyn PartitionFetcher>,
                        storage: self.storage.clone(),
                        backup_id: self.config.backup_id.clone(),
                        options: self.config.backup.clone().unwrap_or_default(),
                        metrics: Arc::clone(&self.metrics),
                        prometheus_metrics: self.prometheus_metrics.clone(),
                        health: Arc::clone(&self.health),
                        offset_store: self.offset_store.clone(),
                        manifest_persistence: Arc::clone(&self.manifest_persistence),
                        kafka_cb: Arc::clone(&self.kafka_circuit_breaker),
                        storage_cb: Arc::clone(&self.storage_circuit_breaker),
                        snapshot,
                        cancel: cancel.clone(),
                        resume_beyond_durable: self
                            .manifest_persistence
                            .take_healed(&(topic.clone(), partition)),
                    };

                    tasks.spawn(async move {
                        let topic_name = ctx.topic.clone();
                        let partition_id = ctx.partition;
                        // Acquire permit inside the task - limits concurrency
                        // without blocking the spawning loop. A queued task
                        // exits at once on shutdown.
                        let _permit = tokio::select! {
                            permit = sem.acquire_owned() => permit.unwrap(),
                            _ = ctx.cancel.cancelled() => return (topic_name, partition_id, Ok(())),
                        };
                        let backup_result = ctx.backup_partition().await;
                        (topic_name, partition_id, backup_result)
                    });
                }
            }

            info!(
                "Spawned {} backup tasks (max {} concurrent)",
                tasks.len(),
                backup_opts.max_concurrent_partitions
            );

            // Wait for ALL partitions across ALL topics to complete, but
            // allow interruption by the shutdown signal for a graceful exit.
            let results = if shutdown_requested {
                None
            } else {
                // Publish a manifest before long-running partition tasks complete so
                // interrupted continuous backups still have discoverable metadata.
                self.save_manifest().await?;

                let mut results = Vec::with_capacity(tasks.len());
                loop {
                    tokio::select! {
                        next = tasks.join_next() => match next {
                            Some(result) => results.push(result),
                            None => break Some(results),
                        },
                        _ = shutdown_rx.recv() => break None,
                    }
                }
            };

            let Some(results) = results else {
                info!(
                    "Shutdown signal received during backup cycle; flushing partitions \
                     (up to {}s) before finalizing",
                    shutdown_grace.as_secs()
                );
                cancel.cancel();
                let (_, aborted) = drain_partition_tasks(&mut tasks, shutdown_grace).await;
                if aborted > 0 {
                    warn!(
                        "{} partition task(s) did not finish within backup.shutdown_grace_secs={}; \
                         aborted (their unsaved progress is re-read on the next run)",
                        aborted,
                        shutdown_grace.as_secs()
                    );
                }
                self.finalize().await?;
                return Ok(());
            };

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
                self.persist_progress_best_effort("partition task failure")
                    .await;
                return Err(Error::Io(std::io::Error::other(format!(
                    "{} of {} partitions failed to backup",
                    error_count, total_tasks
                ))));
            }

            // A literal topic selected at backup start must still exist after
            // the cycle. This catches topics deleted while a backup is running
            // before a zero-record/empty-artifact result can be reported as
            // successful.
            self.ensure_literal_topic_includes_exist(&source.topics, &backup_opts)
                .await?;

            // Save the manifest, then checkpoint what it lists (never the
            // other way round).
            let start = Instant::now();
            self.manifest_persistence.end_cycle().await?;
            if self.offset_store.is_some() {
                self.metrics.record_checkpoint_latency(start.elapsed());
                self.health.mark_healthy("checkpointing");
            }

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

    /// The manifest stored by earlier runs, or `None` for a new backup set.
    async fn load_stored_manifest(&self) -> Result<Option<BackupManifest>> {
        load_stored_manifest(self.storage.as_ref(), &self.config.backup_id).await
    }

    /// Bring offset-store checkpoints in line with the stored manifest. A
    /// checkpoint ahead of the manifest reach (written by an older version
    /// before its segment was listed) is lowered to the reach, and the
    /// partition re-reads from there: the offsets it skipped were never
    /// durably backed up.
    async fn reconcile_checkpoints(&self) -> Result<()> {
        let Some(ref offset_store) = self.offset_store else {
            return Ok(());
        };
        let backup_id = &self.config.backup_id;
        for info in offset_store.get_all_offsets(backup_id).await? {
            let key = (info.topic.clone(), info.partition);
            let reach = self
                .manifest_persistence
                .durable_reach(&key)
                .map(|d| d.reach);
            if let Some(ResumeFrom::Manifest {
                reach,
                checkpoint_ahead: Some(checkpoint),
            }) = plan_resume(reach, Some(info.last_offset))
            {
                warn!(
                    "{}:{}: offset checkpoint {} is ahead of the manifest, which accounts for \
                     offsets up to {}; offsets {}..={} were never recorded in a saved manifest \
                     (checkpoint written by an older version). Re-reading from offset {}",
                    info.topic,
                    info.partition,
                    checkpoint,
                    reach,
                    reach + 1,
                    checkpoint,
                    reach + 1
                );
                offset_store
                    .set_offset(backup_id, &info.topic, info.partition, reach)
                    .await?;
                self.manifest_persistence.mark_healed(key);
            }
        }
        Ok(())
    }

    /// Apply `backup.retention` to this backup set.
    ///
    /// Plans against the STORED (merged) manifest — the in-memory manifest of
    /// a fresh process only holds segments this run wrote, while the aged
    /// segments retention exists for live in the merged manifest that
    /// `save_manifest()` just persisted. The stored manifest is rewritten
    /// first (a crash leaves orphan objects, never a manifest referencing
    /// deleted segments), the pruned ranges are mirrored into the in-memory
    /// manifest so later saves cannot resurrect the segments, and only then
    /// are the objects deleted.
    async fn apply_retention(&self, retention: &crate::config::RetentionOptions) -> Result<()> {
        let criteria = retention.to_criteria()?;
        let resume = match &self.offset_store {
            Some(store) => {
                super::prune::resume_positions(store.as_ref(), &self.config.backup_id).await?
            }
            None => std::collections::HashMap::new(),
        };

        // Load the merged manifest that save_manifest() just wrote.
        let key = format!("{}/manifest.json", self.config.backup_id);
        let stored: BackupManifest =
            match crate::storage::get_if_exists(self.storage.as_ref(), &key).await? {
                Some(data) => serde_json::from_slice(&data)
                    .map_err(|e| Error::Config(format!("retention: manifest unparseable: {e}")))?,
                None => return Ok(()), // nothing persisted yet
            };

        let plan = super::prune::plan_prune(
            &stored,
            &resume,
            &criteria,
            chrono::Utc::now().timestamp_millis(),
            crate::manifest::PruneReason::Retention,
        );
        if plan.segments == 0 {
            return Ok(());
        }

        info!(
            "Retention: pruning {} segment(s), {} bytes across {} partition(s)",
            plan.segments,
            plan.bytes,
            plan.partitions.len()
        );

        // Mirror the pruned ranges (and drop any affected segments) into the
        // in-memory manifest FIRST, so a concurrent/subsequent save merges
        // consistently instead of resurrecting.
        {
            let mut manifest = self.manifest.lock().await;
            super::prune::apply_plan_to_manifest(&mut manifest, &plan);
            for part_plan in &plan.partitions {
                let topic = manifest.get_or_create_topic(&part_plan.topic);
                let partition = topic.get_or_create_partition(part_plan.partition);
                partition.add_pruned(part_plan.range.clone());
            }
        }

        // Rewrite the stored manifest directly, then delete the objects.
        super::prune::execute_prune(self.storage.as_ref(), stored, &plan).await?;

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
        // Manifest first, then the checkpoints it justifies and the job
        // status, then the offsets.db upload. The status flips to
        // "completed" BEFORE the remote sync, so the copy of offsets.db in
        // storage records that the run finished — tools like
        // `kafka-backup prune` read it to decide whether a run is still live.
        self.manifest_persistence
            .finalize(Some("completed"))
            .await?;

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

        let missing =
            check_literal_topic_includes(backup_opts.on_missing_topic, selection, &all_topics)?;

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
        missing_topics_outcome(&missing, selected.len())?;
        self.record_missing_topics(missing, false).await;
        Ok(selected)
    }

    /// Record the literal include topics skipped under `on_missing_topic: warn`
    /// in the manifest and the `kafka_backup_missing_topics` gauge. With
    /// `union = false` the list replaces the previous cycle's (so a topic
    /// created later clears the record); with `union = true` (mid-run check)
    /// it is added to it.
    async fn record_missing_topics(&self, missing: Vec<String>, union: bool) {
        if !missing.is_empty() {
            warn!(
                topics = ?missing,
                "configured backup topic(s) not found in Kafka cluster; continuing (backup.on_missing_topic: warn)"
            );
        }
        let recorded = {
            let mut manifest = self.manifest.lock().await;
            if union {
                let mut all: Vec<String> =
                    manifest.missing_topics.drain(..).chain(missing).collect();
                all.sort();
                all.dedup();
                manifest.missing_topics = all;
            } else {
                manifest.missing_topics = missing;
            }
            manifest.missing_topics.len()
        };
        if let Some(prom) = &self.prometheus_metrics {
            prom.set_missing_topics(&self.config.backup_id, recorded);
        }
    }

    async fn ensure_literal_topic_includes_exist(
        &self,
        selection: &TopicSelection,
        backup_opts: &BackupOptions,
    ) -> Result<()> {
        let all_topics = self.router.fetch_metadata(None).await?;
        let missing =
            check_literal_topic_includes(backup_opts.on_missing_topic, selection, &all_topics)?;
        if !missing.is_empty() {
            self.record_missing_topics(missing, true).await;
        }
        Ok(())
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

    /// Save what partitions staged, then sync the checkpoints. Safe on any
    /// error path: the offset store only ever holds values a saved manifest
    /// justifies.
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
            // An error here fails the (non-fatal) snapshot step rather than
            // reading as "no snapshot yet" and overwriting it (#218).
            if self.storage.exists(&key).await? {
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
    /// end offset at capture time plus the offset this run resumes from. The
    /// end offsets provide a consistent snapshot point - all partitions will
    /// backup to the same logical point in time. Under read_committed the end
    /// is the last stable offset (records of open transactions are left for a
    /// later run); the manifest records it next to the high watermark.
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
        isolation: IsolationLevel,
    ) -> Result<HashMap<(String, i32), SnapshotRange>> {
        info!(
            "Snapshot mode: capturing offsets for {} topics (isolation={})",
            topics_metadata.len(),
            isolation
        );

        let snapshot_start = Instant::now();

        let all_partitions = all_partitions(topics_metadata);

        // Batch fetch all offsets (grouped by leader broker). Under
        // read_committed the end is the last stable offset: records of
        // transactions still open are left for a later run.
        let offsets = self
            .router
            .batch_get_all_offsets(&all_partitions, isolation)
            .await?;
        // The high watermark at capture time, recorded next to the end. Read
        // after the LSO, so it is never below it.
        let high_watermarks: HashMap<(String, i32), i64> = match isolation {
            IsolationLevel::ReadUncommitted => offsets
                .iter()
                .map(|(key, (_, latest))| (key.clone(), *latest))
                .collect(),
            IsolationLevel::ReadCommitted => {
                self.router
                    .batch_get_latest_offsets(&all_partitions, IsolationLevel::ReadUncommitted)
                    .await?
            }
        };
        self.record_snapshot_capture(&offsets, &high_watermarks)
            .await;

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

    /// Record the snapshot's end and the high watermark at capture time in
    /// the manifest, set the LSO-lag gauges, and log partitions held back by
    /// open transactions.
    async fn record_snapshot_capture(
        &self,
        offsets: &HashMap<(String, i32), (i64, i64)>,
        high_watermarks: &HashMap<(String, i32), i64>,
    ) {
        let mut held_back = Vec::new();
        {
            let mut manifest = self.manifest.lock().await;
            for ((topic, partition), (earliest, end)) in offsets {
                let high_watermark = high_watermarks
                    .get(&(topic.clone(), *partition))
                    .copied()
                    .unwrap_or(*end)
                    .max(*end);
                if high_watermark > *end {
                    held_back.push((topic.clone(), *partition, *end, high_watermark));
                }
                // An empty partition has nothing to record (and large
                // clusters have many): keep it out of the manifest.
                if *end <= *earliest && high_watermark <= *end {
                    continue;
                }
                let part = manifest
                    .get_or_create_topic(topic)
                    .get_or_create_partition(*partition);
                part.snapshot_end_offset = Some(*end);
                part.high_watermark_at_capture = Some(high_watermark);
            }
        }
        if let Some(ref prom) = self.prometheus_metrics {
            for ((topic, partition), (_, end)) in offsets {
                let high_watermark = high_watermarks
                    .get(&(topic.clone(), *partition))
                    .copied()
                    .unwrap_or(*end);
                prom.record_lso_lag(
                    topic,
                    *partition,
                    &self.config.backup_id,
                    high_watermark - end,
                );
            }
        }
        log_open_transactions(&held_back);
    }

    /// Refresh `kafka_backup_lso_lag` for every partition (non-snapshot
    /// read_committed runs). Best effort: a failure only logs.
    async fn observe_lso_lag(&self, topics_metadata: &[TopicMetadata]) {
        let Some(ref prom) = self.prometheus_metrics else {
            return;
        };
        let partitions = all_partitions(topics_metadata);
        let observed = async {
            let lso = self
                .router
                .batch_get_latest_offsets(&partitions, IsolationLevel::ReadCommitted)
                .await?;
            let hw = self
                .router
                .batch_get_latest_offsets(&partitions, IsolationLevel::ReadUncommitted)
                .await?;
            Ok::<_, Error>((lso, hw))
        }
        .await;
        match observed {
            Ok((lso, hw)) => {
                for ((topic, partition), lso) in &lso {
                    if let Some(hw) = hw.get(&(topic.clone(), *partition)) {
                        prom.record_lso_lag(topic, *partition, &self.config.backup_id, hw - lso);
                    }
                }
            }
            Err(e) => debug!("Could not refresh the LSO lag (non-fatal): {}", e),
        }
    }

    /// Last durable offset per partition: the manifest reach, or — only for
    /// partitions the manifest does not know (legacy) — the offset-store
    /// checkpoint. Read in bulk at snapshot capture time, so large clusters
    /// need no per-partition lookups.
    async fn load_checkpoints(&self) -> Result<HashMap<(String, i32), i64>> {
        let legacy: Vec<(PartitionKey, i64)> = match self.offset_store {
            Some(ref offset_store) => offset_store
                .get_all_offsets(&self.config.backup_id)
                .await?
                .into_iter()
                .map(|info| ((info.topic, info.partition), info.last_offset))
                .collect(),
            None => Vec::new(),
        };
        Ok(merge_resume_points(
            self.manifest_persistence.durable_reaches(),
            legacy,
        ))
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
    /// End offset at capture time — the last stable offset under
    /// read_committed, the high watermark under read_uncommitted; the run
    /// stops here.
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

/// Topic configuration a backup captures (explicit, writable,
/// non-sensitive overrides only). A restore applies a key only when the
/// target lists it as writable, so keys some targets lack
/// (`message.format.version` on Kafka 4.x, `confluent.*` outside Confluent)
/// are captured anyway and skipped there.
fn is_recovery_topic_config(name: &str) -> bool {
    // Confluent Platform / Cloud topic settings (schema validation, subject
    // name strategies, tiering), except replica placement, which names the
    // source cluster's racks.
    if let Some(confluent) = name.strip_prefix("confluent.") {
        return confluent != "placement.constraints";
    }
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

/// Where a partition resumes, from the stored manifest and the offset store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResumeFrom {
    /// The manifest accounts for offsets through `reach` (the normal case).
    /// `checkpoint_ahead` is a legacy offset-store checkpoint beyond it.
    Manifest {
        reach: i64,
        checkpoint_ahead: Option<i64>,
    },
    /// The manifest does not know the partition: an offset-store checkpoint
    /// from an older version is all there is.
    LegacyCheckpoint(i64),
}

impl ResumeFrom {
    /// First offset to fetch.
    fn start(&self) -> i64 {
        match self {
            ResumeFrom::Manifest { reach, .. } => reach + 1,
            ResumeFrom::LegacyCheckpoint(checkpoint) => checkpoint + 1,
        }
    }
}

/// The manifest wins: resume at its reach + 1. The offset store is used only
/// for partitions the manifest does not know. `None` means neither knows the
/// partition (the configured start offset applies).
fn plan_resume(manifest_reach: Option<i64>, checkpoint: Option<i64>) -> Option<ResumeFrom> {
    match (manifest_reach, checkpoint) {
        (Some(reach), checkpoint) => Some(ResumeFrom::Manifest {
            reach,
            checkpoint_ahead: checkpoint.filter(|c| *c > reach),
        }),
        (None, Some(checkpoint)) => Some(ResumeFrom::LegacyCheckpoint(checkpoint)),
        (None, None) => None,
    }
}

/// Last durable offset per partition (`start - 1`), per [`plan_resume`].
fn merge_resume_points(
    durable: HashMap<PartitionKey, DurableReach>,
    legacy: Vec<(PartitionKey, i64)>,
) -> HashMap<PartitionKey, i64> {
    let mut points: HashMap<PartitionKey, i64> = legacy.into_iter().collect();
    for (key, reach) in durable {
        points.insert(key, reach.reach);
    }
    points
}

/// Wait up to `grace` for the remaining tasks to finish (they have been
/// told to stop), then abort the stragglers. Returns the results collected
/// and how many tasks were aborted.
async fn drain_partition_tasks<T: Send + 'static>(
    tasks: &mut JoinSet<T>,
    grace: Duration,
) -> (Vec<std::result::Result<T, tokio::task::JoinError>>, usize) {
    let deadline = tokio::time::Instant::now() + grace;
    let mut results = Vec::new();
    loop {
        match tokio::time::timeout_at(deadline, tasks.join_next()).await {
            Ok(Some(result)) => results.push(result),
            Ok(None) => return (results, 0),
            Err(_) => break,
        }
    }
    let aborted = tasks.len();
    tasks.abort_all();
    // Aborted tasks stop at their next await point; don't wait forever on
    // one stuck in blocking code.
    let reap = async {
        while let Some(result) = tasks.join_next().await {
            if !result.as_ref().is_err_and(|e| e.is_cancelled()) {
                results.push(result);
            }
        }
    };
    if tokio::time::timeout(Duration::from_secs(5), reap)
        .await
        .is_err()
    {
        tasks.detach_all();
    }
    (results, aborted)
}

/// Context for backing up a single partition
struct BackupPartitionContext {
    topic: String,
    partition: i32,
    router: Arc<dyn PartitionFetcher>,
    storage: Arc<dyn StorageBackend>,
    backup_id: String,
    options: BackupOptions,
    metrics: Arc<PerformanceMetrics>,
    prometheus_metrics: Option<Arc<PrometheusMetrics>>,
    health: Arc<HealthCheck>,
    /// Legacy resume source for partitions the manifest does not know.
    offset_store: Option<Arc<SqliteOffsetStore>>,
    manifest_persistence: Arc<ManifestPersistence>,
    kafka_cb: Arc<CircuitBreaker>,
    #[allow(dead_code)] // Reserved for future storage circuit breaker integration
    storage_cb: Arc<CircuitBreaker>,
    /// Planned offset range from snapshot capture (stop_at_current_offsets).
    /// When set, backup resumes at `start`, stops at `latest` instead of the
    /// live high watermark, and skips the redundant get_offsets() call.
    snapshot: Option<SnapshotRange>,
    /// Graceful shutdown: stop fetching, flush the buffer, stage, return.
    cancel: CancellationToken,
    /// The legacy checkpoint was ahead of the manifest; when the source has
    /// already deleted the offsets this run goes back for, record them as a
    /// `ResumeBeyondDurable` gap.
    resume_beyond_durable: bool,
}

/// The partition's in-progress segment state.
struct PartitionWriter {
    writer: SegmentWriter,
    flusher: SegmentFlusher,
    /// At most one sealed segment is compressed/uploaded in the background
    /// while fetching continues (bounded memory, ordered manifest entries).
    in_flight: Option<tokio::task::JoinHandle<Result<SegmentMetadata>>>,
    segments_written: u64,
    /// Last coverage-only advance staged, to avoid restaging it every fetch.
    staged_tail: Option<i64>,
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
        let (earliest, mut start_offset, end_offset) = match self.snapshot {
            Some(range) => {
                debug!(
                    "{}:{}: snapshot mode - target offset {} (earliest {}, resuming at {})",
                    self.topic, self.partition, range.latest, range.earliest, range.start
                );
                (range.earliest, range.start, range.latest)
            }
            None => {
                // Under read_committed "latest" is the last stable offset:
                // the run stops where an open transaction begins.
                let (earliest, latest) = self
                    .router
                    .partition_offsets(&self.topic, self.partition, self.isolation())
                    .await?;
                let start_offset = self.resolve_start_offset(earliest).await?;
                (earliest, start_offset, latest)
            }
        };

        let mut pw = self.partition_writer(start_offset);

        // Self-heal after an older version's checkpoint ran ahead of the
        // manifest: the offsets we go back for may already be gone.
        if self.resume_beyond_durable && earliest > start_offset && start_offset < end_offset {
            let gap_start = pw.writer.coverage_start().unwrap_or(start_offset);
            self.record_offset_gap(
                gap_start,
                earliest,
                OffsetGapReason::ResumeBeyondDurable,
                pw.writer.pending_skipped(),
            )
            .await;
            pw.writer.skip_coverage_to(earliest);
            start_offset = earliest;
        }

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

        let mut current_offset = start_offset;
        // LSO and high watermark from the latest fetch response.
        let mut last_stable = None;

        // Fetch and store records in segments. Under read_committed the
        // end is a last stable offset, which the fetch never reads past, and
        // a fetch whose batches were all filtered out (markers, aborted
        // records) still advances `next_offset`, so this terminates exactly
        // as it does at a high watermark.
        while current_offset < end_offset {
            if self.cancel.is_cancelled() {
                info!(
                    "{}:{}: shutdown requested, stopping at offset {} and flushing",
                    self.topic, self.partition, current_offset
                );
                break;
            }

            let fetch_result = self.fetch_records(current_offset, end_offset).await;

            let fetched = match fetch_result {
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
                        match self
                            .router
                            .partition_offsets(&self.topic, self.partition, self.isolation())
                            .await
                        {
                            Ok((new_earliest, _)) if new_earliest > current_offset => {
                                // The gap accounts for everything up to its end,
                                // so it may only become durable after the records
                                // fetched before it: flush and stage those first.
                                self.drain(&mut pw).await?;
                                // Only the part of the gap inside this run's target
                                // range counts as lost *for this backup*; anything
                                // past `end_offset` was never going to be captured.
                                let gap_start = pw
                                    .writer
                                    .coverage_start()
                                    .unwrap_or(current_offset)
                                    .min(current_offset);
                                let gap_end = new_earliest.min(end_offset);
                                self.record_offset_gap(
                                    gap_start,
                                    gap_end,
                                    OffsetGapReason::OffsetOutOfRange,
                                    pw.writer.pending_skipped(),
                                )
                                .await;
                                pw.writer.skip_coverage_to(new_earliest);
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
                    // Keep what was already fetched: write the buffered records
                    // as a final segment, so the next run does not re-read them.
                    if let Err(flush_err) = self.drain(&mut pw).await {
                        warn!(
                            "{}:{}: could not flush buffered records after fetch error: {}",
                            self.topic, self.partition, flush_err
                        );
                    }
                    self.persist_progress_best_effort("fetch error").await;
                    return Err(e);
                }
            };

            self.kafka_cb.record_success();
            self.health.mark_healthy("kafka");

            let records = &fetched.records;
            let next_offset = fetched.next_offset;
            if fetched.last_stable_offset >= 0 {
                last_stable = Some((fetched.last_stable_offset, fetched.high_watermark));
            }
            if let Some(ref prom) = self.prometheus_metrics {
                if fetched.last_stable_offset >= 0 {
                    prom.record_lso_lag(
                        &self.topic,
                        self.partition,
                        &self.backup_id,
                        fetched.high_watermark - fetched.last_stable_offset,
                    );
                }
                prom.record_transaction_skips(
                    &self.backup_id,
                    fetched.control_records_skipped,
                    fetched.aborted_records_dropped,
                );
            }

            // A fetch can return zero records yet still advance the offset:
            // on compacted topics the log cleaner may have removed every
            // record in the fetched batches, and under read_committed every
            // batch may have been a marker or aborted. Only "no records and
            // no progress" means we have reached the end of available data.
            if records.is_empty() && next_offset <= current_offset {
                break;
            }

            // Convert to binary records and add to writer. Batches the
            // read_committed filter passed over are counted against the
            // segment whose covered range holds them: those before a record
            // are noted before it is added (and so before any rotation it
            // triggers).
            let mut skipped = fetched.skipped.iter().peekable();
            for record in records {
                while let Some(batch) = skipped.next_if(|b| b.last_offset < record.offset) {
                    pw.writer
                        .note_skipped(batch.control_records, batch.aborted_records);
                }
                let binary_record = to_binary_record(record, &self.options);
                pw.writer.add_record(binary_record)?;

                if pw.writer.should_rotate() {
                    // Wait for the previous in-flight segment first, so
                    // manifest entries stay ordered and at most one sealed
                    // segment is held in memory per partition.
                    self.stage_in_flight(&mut pw).await?;
                    self.seal_in_background(&mut pw)?;
                }
            }

            for batch in skipped {
                pw.writer
                    .note_skipped(batch.control_records, batch.aborted_records);
            }

            // Everything below next_offset has been added or held no record
            // (compacted away, markers, aborted records). Extend coverage
            // after the records, so a rotation mid-batch never claims offsets
            // of the next segment.
            pw.writer.extend_coverage(next_offset - 1);

            // Stage a finished upload promptly so the checkpoint can advance.
            if pw.in_flight.as_ref().is_some_and(|h| h.is_finished()) {
                self.stage_in_flight(&mut pw).await?;
            }
            // Nothing buffered and nothing in flight: the offsets just passed
            // held no record; account for them now.
            self.stage_uncovered_tail(&mut pw).await?;

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

        // Caught up, reached the target, or shutting down: write what is
        // buffered and stage it.
        self.drain(&mut pw).await?;
        self.manifest_persistence.save_progress().await?;

        if let Some((lso, high_watermark)) = last_stable {
            if self.isolation() == IsolationLevel::ReadCommitted && lso < high_watermark {
                // Debug only: transactions in flight are normal; a hanging
                // one shows as a kafka_backup_lso_lag that stays up.
                debug!(
                    "{}:{}: stopped at the last stable offset {}; {} offsets below the high                      watermark {} belong to open transactions and are backed up once they                      complete",
                    self.topic,
                    self.partition,
                    lso,
                    high_watermark - lso,
                    high_watermark
                );
            }
        }

        if self.snapshot.is_some() {
            info!(
                "Completed snapshot backup of {}:{} - {} segments (reached target offset {})",
                self.topic, self.partition, pw.segments_written, end_offset
            );
        } else {
            info!(
                "Completed backup of {}:{} - {} segments",
                self.topic, self.partition, pw.segments_written
            );
        }

        Ok(())
    }

    /// A segment writer whose coverage continues from what the stored
    /// manifest accounts for, or starts at `start_offset`.
    fn partition_writer(&self, start_offset: i64) -> PartitionWriter {
        let writer_config = SegmentWriterConfig {
            max_segment_bytes: self.options.segment_max_bytes,
            max_segment_interval_ms: self.options.segment_max_interval_ms,
            compression: self.options.compression,
            compression_level: self.options.compression_level,
            max_segment_records: self.options.segment_max_records,
        };
        let mut writer = SegmentWriter::with_prometheus(
            writer_config,
            self.storage.clone(),
            self.metrics.clone(),
            self.prometheus_metrics.clone(),
            self.backup_id.clone(),
        );
        let durable = self
            .manifest_persistence
            .durable_reach(&(self.topic.clone(), self.partition));
        match durable {
            // Resuming at the manifest reach: the first new segment covers
            // from the last explicit interval, absorbing a trailing
            // coverage-only advance, so the intervals stay contiguous. It
            // also takes over the transaction records counted in that range.
            Some(d) if d.reach + 1 == start_offset => {
                let first_uncovered = d.explicit.map_or(start_offset, |e| e + 1);
                writer.begin_coverage(first_uncovered.min(start_offset), d.reach);
                writer.note_skipped(d.tail.0, d.tail.1);
            }
            _ => writer.begin_coverage(start_offset, start_offset - 1),
        }
        let flusher = writer.flusher();
        PartitionWriter {
            writer,
            flusher,
            in_flight: None,
            segments_written: 0,
            staged_tail: None,
        }
    }

    /// Await the in-flight upload (if any) and stage it.
    async fn stage_in_flight(&self, pw: &mut PartitionWriter) -> Result<()> {
        if let Some(handle) = pw.in_flight.take() {
            let segment = join_segment_flush(handle).await?;
            self.manifest_persistence
                .stage_segment(&self.topic, self.partition, segment)
                .await;
            pw.segments_written += 1;
            self.manifest_persistence.save_progress().await?;
        }
        Ok(())
    }

    /// Seal the buffered records and upload them in the background.
    fn seal_in_background(&self, pw: &mut PartitionWriter) -> Result<()> {
        let Some(seg_start) = pw.writer.start_offset() else {
            return Ok(());
        };
        let key = self.segment_key(seg_start);
        self.manifest_persistence.ensure_unlisted(&key)?;
        if let Some(sealed) = pw.writer.seal(&key) {
            let flusher = pw.flusher.clone();
            pw.in_flight = Some(tokio::spawn(async move { flusher.write(sealed).await }));
        }
        Ok(())
    }

    /// Stage a coverage-only advance when nothing is buffered or in flight.
    async fn stage_uncovered_tail(&self, pw: &mut PartitionWriter) -> Result<()> {
        if pw.in_flight.is_some() {
            return Ok(());
        }
        if let Some((_, end)) = pw.writer.uncovered_tail() {
            if pw.staged_tail.is_none_or(|staged| end > staged) {
                self.manifest_persistence
                    .stage_coverage(
                        &self.topic,
                        self.partition,
                        end,
                        pw.writer.pending_skipped(),
                    )
                    .await;
                pw.staged_tail = Some(end);
                self.manifest_persistence.save_progress().await?;
            }
        }
        Ok(())
    }

    /// Finish the partition's segment state in order: the in-flight upload,
    /// then the buffered records, then any trailing coverage. Stops at the
    /// first failure, so nothing after a failed upload is ever staged.
    async fn drain(&self, pw: &mut PartitionWriter) -> Result<()> {
        self.stage_in_flight(pw).await?;
        if pw.writer.has_data() {
            self.seal_in_background(pw)?;
            self.stage_in_flight(pw).await?;
        }
        self.stage_uncovered_tail(pw).await
    }

    /// Resume position outside snapshot mode: the manifest reach + 1, else
    /// (legacy) the offset-store checkpoint + 1, else the configured start.
    async fn resolve_start_offset(&self, earliest: i64) -> Result<i64> {
        let key = (self.topic.clone(), self.partition);
        let reach = self
            .manifest_persistence
            .durable_reach(&key)
            .map(|d| d.reach);
        let checkpoint = match (&self.offset_store, reach) {
            // The manifest wins; the store is consulted only when it has
            // nothing for this partition.
            (Some(offset_store), None) => {
                offset_store
                    .get_offset(&self.backup_id, &self.topic, self.partition)
                    .await?
            }
            _ => None,
        };
        Ok(match plan_resume(reach, checkpoint) {
            Some(resume) => resume.start(),
            None => configured_start_offset(
                &self.options.start_offset,
                &self.topic,
                self.partition,
                earliest,
            ),
        })
    }

    fn segment_key(&self, start_offset: i64) -> String {
        let ext = extension(self.options.compression);
        format!(
            "{}/topics/{}/partition={}/segment-{:020}.bin{}",
            self.backup_id, self.topic, self.partition, start_offset, ext
        )
    }

    /// Record that offsets `[start_offset, end_offset)` are permanently missing
    /// from this backup: staged into the manifest (the caller has already
    /// staged everything before it), Prometheus counters, snapshot progress,
    /// and a best-effort manifest save so the gap survives a crash.
    async fn record_offset_gap(
        &self,
        start_offset: i64,
        end_offset: i64,
        reason: OffsetGapReason,
        tail: TxnSkips,
    ) {
        let gap = OffsetGap {
            start_offset,
            end_offset,
            reason,
            detected_at: chrono::Utc::now().timestamp_millis(),
        };
        let span = gap.offset_span();
        if span == 0 {
            return;
        }

        warn!(
            "{}:{}: recording data gap [{}, {}) — {} offsets permanently missing from backup {} \
             ({})",
            self.topic,
            self.partition,
            start_offset,
            end_offset,
            span,
            self.backup_id,
            match reason {
                OffsetGapReason::ResumeBeyondDurable =>
                    "never durably backed up by an earlier run, and deleted from the source \
                     before they could be re-read",
                _ => "deleted from the source before they could be fetched",
            }
        );

        self.manifest_persistence
            .stage_gap(&self.topic, self.partition, gap, tail)
            .await;

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
        if let Some(offsets) = &self.manifest_persistence.offsets {
            if let Err(e) = offsets.sync_now().await {
                warn!(
                    "Best-effort offset sync failed for {}:{} after {}: {}",
                    self.topic, self.partition, reason, e
                );
            }
        }
    }

    /// The configured transaction isolation for this partition's reads.
    fn isolation(&self) -> IsolationLevel {
        self.options.isolation_level
    }

    async fn fetch_records(&self, start_offset: i64, _end_offset: i64) -> Result<FetchResponse> {
        let max_bytes = effective_fetch_max_bytes(&self.options);

        // Fetch records using router (automatically routes to partition leader)
        let fetched = self
            .router
            .fetch_partition(
                &self.topic,
                self.partition,
                start_offset,
                max_bytes,
                self.isolation(),
            )
            .await?;
        failpoint!("backup::fetch::after");
        Ok(fetched)
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

/// Load `{backup_id}/manifest.json` at startup. Missing means a new backup
/// set. A read error fails (#218), and so does an unparseable manifest: it
/// is the only record of which segments the set holds, and saving over it
/// would orphan every one of them.
async fn load_stored_manifest(
    storage: &dyn StorageBackend,
    backup_id: &str,
) -> Result<Option<BackupManifest>> {
    let key = format!("{}/manifest.json", backup_id);
    let Some(data) = crate::storage::get_if_exists(storage, &key).await? else {
        return Ok(None);
    };
    serde_json::from_slice(&data).map(Some).map_err(|e| {
        Error::Manifest(format!(
            "stored manifest {key} is unparseable ({e}); refusing to back up, because saving \
             would replace it and orphan every segment it lists. Restore it (e.g. from bucket \
             versioning) or use a new backup_id"
        ))
    })
}

async fn save_manifest_snapshot(
    storage: &dyn StorageBackend,
    backup_id: &str,
    current: BackupManifest,
) -> Result<()> {
    let key = format!("{}/manifest.json", backup_id);

    // Merge with the stored manifest. Only a missing one means "first
    // write": a failed read (403, network, 5xx after retries) must fail the
    // save, or this run's manifest would replace every earlier run's (#218).
    let merged = match crate::storage::get_if_exists(storage, &key).await? {
        Some(data) => match serde_json::from_slice::<BackupManifest>(&data) {
            Ok(existing) => merge_manifests(existing, current),
            Err(e) => {
                warn!("Existing manifest is unparseable, overwriting: {}", e);
                current
            }
        },
        None => current,
    };

    let manifest_json = serde_json::to_string_pretty(&merged)?;
    failpoint!("backup::manifest_put::before");
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
///     start_offset. `covered_through` is the maximum of the two sides, and the trailing
///     range's transaction counts come from the side that reaches furthest. The snapshot
///     end and high watermark at capture come from `current` when it captured them.
/// - `isolation_level` is the current run's.
fn merge_manifests(mut existing: BackupManifest, current: BackupManifest) -> BackupManifest {
    use std::collections::HashMap as HM;

    // Latest discovery pass wins: the field must be copied here or every
    // save_manifest (which starts from the stored manifest) would drop it.
    existing.missing_topics = current.missing_topics.clone();
    // The most recent run's isolation level.
    if current.isolation_level.is_some() {
        existing.isolation_level = current.isolation_level;
    }

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
                    // The trailing range's transaction counts belong to the
                    // side that reaches furthest: this run's when it staged
                    // anything at or past the stored reach (a segment that
                    // absorbed the stored tail has reset them to 0).
                    let current_reaches_further =
                        match (ex_part.covered_through, cur_part.covered_through) {
                            (_, None) => false,
                            (None, Some(_)) => true,
                            (Some(a), Some(b)) => b >= a,
                        };
                    if current_reaches_further {
                        ex_part.tail_control_records_skipped =
                            cur_part.tail_control_records_skipped;
                        ex_part.tail_aborted_records_dropped =
                            cur_part.tail_aborted_records_dropped;
                    }
                    // Coverage only ever moves forward: a stale in-memory
                    // value must not pull the stored reach back.
                    ex_part.covered_through =
                        match (ex_part.covered_through, cur_part.covered_through) {
                            (Some(a), Some(b)) => Some(a.max(b)),
                            (a, b) => a.or(b),
                        };
                    // The most recent snapshot capture wins.
                    if cur_part.snapshot_end_offset.is_some() {
                        ex_part.snapshot_end_offset = cur_part.snapshot_end_offset;
                        ex_part.high_watermark_at_capture = cur_part.high_watermark_at_capture;
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

/// Check literal `topics.include` entries against the cluster. In `fail` mode
/// any missing entry is an error; in `warn` mode the missing names are
/// returned for the caller to record (issue #167).
fn check_literal_topic_includes(
    mode: crate::config::OnMissingTopic,
    selection: &TopicSelection,
    existing_topics: &[TopicMetadata],
) -> Result<Vec<String>> {
    let missing = missing_literal_topic_includes(selection, existing_topics);
    if missing.is_empty() {
        return Ok(missing);
    }
    match mode {
        crate::config::OnMissingTopic::Fail => Err(Error::TopicNotFound(format!(
            "configured backup topic(s) not found in Kafka cluster: {}",
            missing.join(", ")
        ))),
        crate::config::OnMissingTopic::Warn => Ok(missing),
    }
}

/// `warn` mode still refuses a run that has nothing left to back up. A
/// selection that is empty for other reasons (globs matching nothing) keeps
/// the existing warn-and-skip behaviour.
fn missing_topics_outcome(missing: &[String], selected_count: usize) -> Result<()> {
    if !missing.is_empty() && selected_count == 0 {
        return Err(Error::TopicNotFound(format!(
            "all configured backup topics are missing from the Kafka cluster: {}",
            missing.join(", ")
        )));
    }
    Ok(())
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

/// One-line startup summary of the transaction isolation the backup reads
/// with.
fn describe_isolation_level(isolation: IsolationLevel) -> String {
    match isolation {
        IsolationLevel::ReadCommitted => {
            "isolation_level=read_committed (default): records of aborted transactions and \
             transaction markers are not archived, and each partition is read up to its last \
             stable offset (records of open transactions are archived once they commit); set \
             backup.isolation_level: read_uncommitted to archive everything up to the high \
             watermark as versions before 0.25 did"
                .to_string()
        }
        IsolationLevel::ReadUncommitted => {
            "isolation_level=read_uncommitted: every record up to the high watermark is archived, \
             including records of aborted and open transactions and the transaction markers, \
             which a restore writes back as ordinary records"
                .to_string()
        }
    }
}

/// Warn when a backup set continues with a different isolation level than
/// its earlier runs: the archive then mixes both.
fn warn_on_isolation_change(
    backup_id: &str,
    stored: Option<IsolationLevel>,
    configured: IsolationLevel,
) -> bool {
    // Manifests written before 0.25 have no field: they read uncommitted.
    let previous = stored.unwrap_or(IsolationLevel::ReadUncommitted);
    if previous == configured {
        return false;
    }
    warn!(
        "Backup set {} was written with isolation_level={}{} and continues with {}: segments \
         written so far keep what they captured{}. Set backup.isolation_level: {} to keep the \
         set uniform, or use a new backup_id",
        backup_id,
        previous,
        if stored.is_none() {
            " (versions before 0.25)"
        } else {
            ""
        },
        configured,
        if previous == IsolationLevel::ReadUncommitted {
            " (including any aborted records and transaction markers)"
        } else {
            ""
        },
        previous
    );
    true
}

/// Every `(topic, partition)` of the given topics.
fn all_partitions(topics_metadata: &[TopicMetadata]) -> Vec<(String, i32)> {
    topics_metadata
        .iter()
        .flat_map(|t| {
            t.partitions
                .iter()
                .map(move |p| (t.name.clone(), p.partition_id))
        })
        .collect()
}

/// Log the partitions whose last stable offset was below the high watermark
/// at snapshot capture: open transactions held the snapshot back there.
fn log_open_transactions(held_back: &[(String, i32, i64, i64)]) {
    if held_back.is_empty() {
        return;
    }
    let held_offsets: i64 = held_back.iter().map(|(_, _, lso, hw)| hw - lso).sum();
    info!(
        "{} partition(s) have open transactions: this snapshot ends at their last stable offset, \
         {} offsets below the high watermark are left for a later run (first: {}:{} LSO {} HW \
         {}). A gap that never closes is a hanging transaction; alert on kafka_backup_lso_lag",
        held_back.len(),
        held_offsets,
        held_back[0].0,
        held_back[0].1,
        held_back[0].2,
        held_back[0].3
    );
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
        assert!(super::is_recovery_topic_config("message.format.version"));
        assert!(!super::is_recovery_topic_config("vendor.secret.option"));
        // Confluent topic settings, but not the source's rack placement.
        assert!(super::is_recovery_topic_config(
            "confluent.value.schema.validation"
        ));
        assert!(super::is_recovery_topic_config(
            "confluent.key.subject.name.strategy"
        ));
        assert!(super::is_recovery_topic_config("confluent.tier.enable"));
        assert!(!super::is_recovery_topic_config(
            "confluent.placement.constraints"
        ));
        assert!(!super::is_recovery_topic_config("confluent"));
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
            covered_start_offset: None,
            covered_end_offset: None,
            control_records_skipped: 0,
            aborted_records_dropped: 0,
        }
    }

    fn make_partition(id: i32, segments: Vec<SegmentMetadata>) -> PartitionBackup {
        PartitionBackup {
            partition_id: id,
            segments,
            gaps: Vec::new(),
            pruned: Vec::new(),
            covered_through: None,
            tail_control_records_skipped: 0,
            tail_aborted_records_dropped: 0,
            snapshot_end_offset: None,
            high_watermark_at_capture: None,
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
            None,
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
            None,
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

    #[test]
    fn merge_keeps_the_highest_covered_through() {
        let with_coverage = |covered: Option<i64>| {
            let mut part = make_partition(0, vec![make_segment("seg-a", 0, 9)]);
            part.covered_through = covered;
            let mut m = BackupManifest::new("test".to_string());
            m.topics.push(make_topic("orders", Some(1), vec![part]));
            m
        };

        let merged = merge_manifests(with_coverage(Some(20)), with_coverage(Some(30)));
        assert_eq!(merged.topics[0].partitions[0].covered_through, Some(30));

        // A stale in-memory value never moves the stored one backwards.
        let merged = merge_manifests(with_coverage(Some(30)), with_coverage(Some(20)));
        assert_eq!(merged.topics[0].partitions[0].covered_through, Some(30));

        // A legacy side without the field keeps the other side's value.
        let merged = merge_manifests(with_coverage(None), with_coverage(Some(25)));
        assert_eq!(merged.topics[0].partitions[0].covered_through, Some(25));
        let merged = merge_manifests(with_coverage(Some(25)), with_coverage(None));
        assert_eq!(merged.topics[0].partitions[0].covered_through, Some(25));
    }

    #[test]
    fn merge_takes_the_current_isolation_level_and_snapshot_capture() {
        let manifest = |isolation: Option<IsolationLevel>, capture: Option<(i64, i64)>| {
            let mut part = make_partition(0, vec![make_segment("seg-a", 0, 9)]);
            if let Some((end, hw)) = capture {
                part.snapshot_end_offset = Some(end);
                part.high_watermark_at_capture = Some(hw);
            }
            let mut m = BackupManifest::new("test".to_string());
            m.isolation_level = isolation;
            m.topics.push(make_topic("orders", Some(1), vec![part]));
            m
        };

        // A legacy stored manifest continued by a read_committed run.
        let merged = merge_manifests(
            manifest(None, None),
            manifest(Some(IsolationLevel::ReadCommitted), Some((10, 14))),
        );
        assert_eq!(merged.isolation_level, Some(IsolationLevel::ReadCommitted));
        let part = &merged.topics[0].partitions[0];
        assert_eq!(
            (part.snapshot_end_offset, part.high_watermark_at_capture),
            (Some(10), Some(14))
        );

        // A current side without a capture keeps the stored one.
        let merged = merge_manifests(
            manifest(Some(IsolationLevel::ReadCommitted), Some((10, 14))),
            manifest(Some(IsolationLevel::ReadUncommitted), None),
        );
        assert_eq!(
            merged.isolation_level,
            Some(IsolationLevel::ReadUncommitted)
        );
        assert_eq!(merged.topics[0].partitions[0].snapshot_end_offset, Some(10));
    }

    #[test]
    fn merge_keeps_the_tail_counts_of_the_side_that_reaches_furthest() {
        let with_tail = |covered: Option<i64>, tail: (u64, u64)| {
            let mut part = make_partition(0, vec![make_segment("seg-a", 0, 9)]);
            part.covered_through = covered;
            part.tail_control_records_skipped = tail.0;
            part.tail_aborted_records_dropped = tail.1;
            let mut m = BackupManifest::new("test".to_string());
            m.topics.push(make_topic("orders", Some(1), vec![part]));
            m
        };
        let tail_of = |m: &BackupManifest| {
            let p = &m.topics[0].partitions[0];
            (
                p.tail_control_records_skipped,
                p.tail_aborted_records_dropped,
            )
        };

        // This run absorbed the stored tail into a segment: reset.
        let merged = merge_manifests(with_tail(Some(15), (1, 3)), with_tail(Some(30), (0, 0)));
        assert_eq!(tail_of(&merged), (0, 0));
        // This run staged nothing for the partition: the stored tail stays.
        let merged = merge_manifests(with_tail(Some(15), (1, 3)), with_tail(None, (0, 0)));
        assert_eq!(tail_of(&merged), (1, 3));
        // A stale in-memory value does not overwrite a further stored one.
        let merged = merge_manifests(with_tail(Some(30), (2, 0)), with_tail(Some(15), (1, 3)));
        assert_eq!(tail_of(&merged), (2, 0));
    }

    #[test]
    fn isolation_change_on_a_continued_backup_set_is_reported() {
        use IsolationLevel::{ReadCommitted, ReadUncommitted};
        // Legacy manifests read uncommitted.
        assert!(warn_on_isolation_change("b", None, ReadCommitted));
        assert!(!warn_on_isolation_change("b", None, ReadUncommitted));
        assert!(!warn_on_isolation_change(
            "b",
            Some(ReadCommitted),
            ReadCommitted
        ));
        assert!(warn_on_isolation_change(
            "b",
            Some(ReadCommitted),
            ReadUncommitted
        ));
        assert!(describe_isolation_level(ReadCommitted).contains("read_uncommitted"));
        assert!(describe_isolation_level(ReadUncommitted).contains("transaction markers"));
    }

    #[test]
    fn effective_offset_sync_secs_precedence() {
        use crate::config::{BackupOptions, OffsetStorageConfig};
        let backup = BackupOptions {
            sync_interval_secs: 5,
            ..Default::default()
        };
        // No offset_storage section → backup value.
        assert_eq!(effective_offset_sync_secs(None, &backup), 5);
        // Section present but interval unset → backup value (no silent reset to 30).
        let unset = OffsetStorageConfig::default();
        assert_eq!(effective_offset_sync_secs(Some(&unset), &backup), 5);
        // Explicit override wins.
        let set = OffsetStorageConfig {
            sync_interval_secs: Some(60),
            ..Default::default()
        };
        assert_eq!(effective_offset_sync_secs(Some(&set), &backup), 60);
        assert_eq!(offsets_key("daily"), "daily/offsets.db");
    }

    #[test]
    fn check_literal_topic_includes_mode_matrix() {
        use crate::config::OnMissingTopic;
        let selection = TopicSelection {
            include: vec![
                "orders".to_string(),
                "ghost".to_string(),
                "logs-*".to_string(),
            ],
            exclude: vec![],
        };
        let existing = vec![topic_metadata("orders")];

        let err =
            check_literal_topic_includes(OnMissingTopic::Fail, &selection, &existing).unwrap_err();
        assert!(err.to_string().contains("ghost"), "{err}");

        let missing =
            check_literal_topic_includes(OnMissingTopic::Warn, &selection, &existing).unwrap();
        assert_eq!(
            missing,
            vec!["ghost".to_string()],
            "globs are never reported"
        );

        let all_present = vec![topic_metadata("orders"), topic_metadata("ghost")];
        assert!(
            check_literal_topic_includes(OnMissingTopic::Fail, &selection, &all_present)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn missing_topics_outcome_rules() {
        let missing = vec!["ghost".to_string()];
        assert!(
            missing_topics_outcome(&missing, 2).is_ok(),
            "something left to back up"
        );
        assert!(
            missing_topics_outcome(&missing, 0).is_err(),
            "nothing left to back up"
        );
        assert!(
            missing_topics_outcome(&[], 0).is_ok(),
            "empty glob selection keeps today's behaviour"
        );
    }

    #[test]
    fn merge_manifests_carries_missing_topics() {
        let mut existing = BackupManifest::new("m".to_string());
        existing.missing_topics = vec!["stale".to_string()];
        let mut current = BackupManifest::new("m".to_string());
        current.missing_topics = vec!["ghost".to_string()];
        let merged = merge_manifests(existing, current);
        assert_eq!(merged.missing_topics, vec!["ghost".to_string()]);

        let cleared = merge_manifests(merged, BackupManifest::new("m".to_string()));
        assert!(
            cleared.missing_topics.is_empty(),
            "a clean pass clears the record"
        );
    }

    // ------------------------------------------------------------------
    // #218: only a missing manifest is a first write. Any other read error
    // (S3 403, network, 5xx after retries) used to fall through to "write
    // this run's manifest", replacing every earlier run's segments.
    // ------------------------------------------------------------------

    fn manifest_with(topic: &str) -> BackupManifest {
        let mut manifest = BackupManifest::new("b".to_string());
        manifest.topics.push(make_topic(topic, Some(1), Vec::new()));
        manifest
    }

    async fn stored_topics(storage: &crate::storage::testing::FaultyStorage) -> Vec<String> {
        storage.heal();
        let data = storage.get("b/manifest.json").await.unwrap();
        let stored: BackupManifest = serde_json::from_slice(&data).unwrap();
        stored.topics.into_iter().map(|t| t.name).collect()
    }

    #[tokio::test]
    async fn manifest_read_error_does_not_overwrite_the_stored_manifest() {
        let storage = crate::storage::testing::FaultyStorage::new();
        save_manifest_snapshot(&storage, "b", manifest_with("alpha"))
            .await
            .unwrap();

        storage.deny_reads("b/manifest.json");
        let puts = storage.puts().len();
        let result = save_manifest_snapshot(&storage, "b", manifest_with("beta")).await;

        assert!(result.is_err(), "a failed manifest read must fail the save");
        assert_eq!(storage.puts().len(), puts, "no PUT after a failed read");
        assert_eq!(stored_topics(&storage).await, vec!["alpha"]);
    }

    #[tokio::test]
    async fn stored_manifest_loading_distinguishes_missing_unreadable_and_corrupt() {
        let storage = crate::storage::testing::FaultyStorage::new();
        assert!(load_stored_manifest(&storage, "b").await.unwrap().is_none());

        save_manifest_snapshot(&storage, "b", manifest_with("alpha"))
            .await
            .unwrap();
        let loaded = load_stored_manifest(&storage, "b").await.unwrap().unwrap();
        assert_eq!(loaded.topics[0].name, "alpha");

        storage.deny_reads("b/manifest.json");
        assert!(load_stored_manifest(&storage, "b").await.is_err());
        storage.heal();

        // A torn or corrupt manifest must stop the run, not be overwritten.
        storage
            .put("b/manifest.json", Bytes::from_static(b""))
            .await
            .unwrap();
        let err = load_stored_manifest(&storage, "b").await.unwrap_err();
        assert!(err.to_string().contains("unparseable"), "{err}");
    }

    #[tokio::test]
    async fn missing_manifest_is_a_first_write_and_existing_one_merges() {
        let storage = crate::storage::testing::FaultyStorage::new();
        save_manifest_snapshot(&storage, "b", manifest_with("alpha"))
            .await
            .unwrap();
        save_manifest_snapshot(&storage, "b", manifest_with("beta"))
            .await
            .unwrap();
        assert_eq!(stored_topics(&storage).await, vec!["alpha", "beta"]);
    }

    // ------------------------------------------------------------------
    // Checkpoint durability (I1: checkpoint <= durable point; I2: offsets.db
    // only after the manifest save that justifies it; I3: no listed key is
    // rewritten).
    // ------------------------------------------------------------------

    mod durability {
        use super::*;
        use crate::error::StorageError;
        use crate::kafka::{PartitionFetcher, SkippedBatch};
        use crate::offset_store::OffsetStoreConfig;
        use crate::storage::testing::FaultyStorage;
        use std::collections::VecDeque;
        use std::sync::atomic::{AtomicUsize, Ordering};

        const BID: &str = "b";

        fn tpk(partition: i32) -> PartitionKey {
            ("t".to_string(), partition)
        }

        fn seg_covering(start: i64, end: i64, cs: i64, ce: i64) -> SegmentMetadata {
            SegmentMetadata {
                key: format!("b/topics/t/partition=0/segment-{start:020}.bin.zst"),
                covered_start_offset: Some(cs),
                covered_end_offset: Some(ce),
                control_records_skipped: 0,
                aborted_records_dropped: 0,
                ..make_segment("unused", start, end)
            }
        }

        struct Harness {
            storage: Arc<FaultyStorage>,
            store: Arc<SqliteOffsetStore>,
            persistence: Arc<ManifestPersistence>,
            _dir: tempfile::TempDir,
        }

        async fn harness(progress_interval: Duration) -> Harness {
            let dir = tempfile::TempDir::new().unwrap();
            let storage = Arc::new(FaultyStorage::new());
            let manifest = Arc::new(Mutex::new(BackupManifest::new(BID.to_string())));
            let store = Arc::new(
                SqliteOffsetStore::new(OffsetStoreConfig {
                    db_path: dir.path().join("offsets.db"),
                    ..Default::default()
                })
                .await
                .unwrap(),
            );
            store.get_or_create_job(BID, None).await.unwrap();
            let offsets = Arc::new(OffsetPersistence::new(
                BID.to_string(),
                storage.clone(),
                store.clone(),
                Duration::from_secs(0),
            ));
            let persistence = Arc::new(ManifestPersistence::new(
                BID.to_string(),
                storage.clone(),
                manifest.clone(),
                progress_interval,
                Some(offsets),
            ));
            Harness {
                storage,
                store,
                persistence,
                _dir: dir,
            }
        }

        async fn stored_manifest(storage: &FaultyStorage) -> BackupManifest {
            serde_json::from_slice(&storage.get("b/manifest.json").await.unwrap()).unwrap()
        }

        fn reach_of(manifest: &BackupManifest, partition: i32) -> Option<i64> {
            manifest
                .topics
                .iter()
                .find(|t| t.name == "t")
                .and_then(|t| t.partitions.iter().find(|p| p.partition_id == partition))
                .and_then(PartitionBackup::reach)
        }

        /// Decode the checkpoints in an uploaded offsets.db (on a separate
        /// thread with its own runtime, so it can run inside a put hook).
        fn decode_offsets_db(bytes: Bytes) -> HashMap<PartitionKey, i64> {
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let dir = tempfile::TempDir::new().unwrap();
                    let path = dir.path().join("uploaded.db");
                    std::fs::write(&path, &bytes).unwrap();
                    let store = SqliteOffsetStore::new(OffsetStoreConfig {
                        db_path: path,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                    store
                        .get_all_offsets(BID)
                        .await
                        .unwrap()
                        .into_iter()
                        .map(|o| ((o.topic, o.partition), o.last_offset))
                        .collect()
                })
            })
            .join()
            .unwrap()
        }

        /// Install hooks that check I1/I2 on every offsets.db upload: each
        /// checkpoint must be within the reach of the last manifest stored
        /// before it.
        fn assert_offsets_never_ahead_of_manifest(
            storage: &Arc<FaultyStorage>,
        ) -> Arc<AtomicUsize> {
            let checked = Arc::new(AtomicUsize::new(0));
            let latest: Arc<parking_lot::Mutex<Option<BackupManifest>>> = Default::default();
            let seen = latest.clone();
            storage.on_every_put("manifest.json", move |_, bytes| {
                *seen.lock() = Some(serde_json::from_slice(bytes).unwrap());
            });
            let count = checked.clone();
            storage.on_every_put("offsets.db", move |_, bytes| {
                let checkpoints = decode_offsets_db(bytes.clone());
                let manifest = latest.lock().clone();
                for ((topic, partition), checkpoint) in checkpoints {
                    let reach = manifest
                        .as_ref()
                        .and_then(|m| m.topics.iter().find(|t| t.name == topic))
                        .and_then(|t| t.partitions.iter().find(|p| p.partition_id == partition))
                        .and_then(PartitionBackup::reach);
                    assert!(
                        reach.is_some_and(|r| checkpoint <= r),
                        "offsets.db uploaded {topic}:{partition}={checkpoint} ahead of the \
                         stored manifest reach {reach:?}"
                    );
                }
                count.fetch_add(1, Ordering::SeqCst);
            });
            checked
        }

        // -- staging and save-then-checkpoint ordering ---------------------

        #[tokio::test]
        async fn staged_checkpoint_is_set_only_after_the_manifest_save() {
            let h = harness(Duration::from_secs(3600)).await;
            h.persistence
                .stage_segment("t", 0, seg_covering(0, 9, 0, 12))
                .await;
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), None);

            h.storage.fail_puts(
                "manifest.json",
                StorageError::Backend("503 SlowDown".to_string()),
                0,
            );
            assert!(h.persistence.save_now().await.is_err());
            assert_eq!(
                h.store.get_offset(BID, "t", 0).await.unwrap(),
                None,
                "a failed manifest save must not advance the checkpoint"
            );

            h.storage.heal_puts();
            h.persistence.save_now().await.unwrap();
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(12));
            assert_eq!(reach_of(&stored_manifest(&h.storage).await, 0), Some(12));
        }

        #[tokio::test]
        async fn failed_save_restages_the_highest_checkpoint() {
            let h = harness(Duration::from_secs(3600)).await;
            h.persistence
                .stage_segment("t", 0, seg_covering(0, 9, 0, 9))
                .await;
            h.storage.fail_puts(
                "manifest.json",
                StorageError::Backend("down".to_string()),
                0,
            );
            assert!(h.persistence.save_now().await.is_err());
            // More progress while storage is down.
            h.persistence
                .stage_segment("t", 0, seg_covering(10, 19, 10, 24))
                .await;
            h.persistence.stage_coverage("t", 1, 5, (0, 0)).await;
            assert!(h.persistence.save_now().await.is_err());

            h.storage.heal_puts();
            h.persistence.save_now().await.unwrap();
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(24));
            assert_eq!(h.store.get_offset(BID, "t", 1).await.unwrap(), Some(5));
            let stored = stored_manifest(&h.storage).await;
            assert_eq!(stored.topics[0].partitions[0].segments.len(), 2);
            assert_eq!(h.persistence.durable_reach(&tpk(0)).unwrap().reach, 24);
        }

        #[tokio::test]
        async fn staging_does_not_advance_the_durable_reach_until_saved() {
            let h = harness(Duration::from_secs(3600)).await;
            h.persistence
                .stage_segment("t", 0, seg_covering(0, 9, 0, 9))
                .await;
            assert_eq!(h.persistence.durable_reach(&tpk(0)), None);
            h.persistence.save_now().await.unwrap();
            assert_eq!(
                h.persistence.durable_reach(&tpk(0)),
                Some(DurableReach {
                    reach: 9,
                    explicit: Some(9),
                    tail: (0, 0)
                })
            );
            // A coverage-only advance moves the reach but is not explicit.
            h.persistence.stage_coverage("t", 0, 15, (0, 0)).await;
            h.persistence.save_now().await.unwrap();
            assert_eq!(
                h.persistence.durable_reach(&tpk(0)),
                Some(DurableReach {
                    reach: 15,
                    explicit: Some(9),
                    tail: (0, 0)
                })
            );
        }

        #[tokio::test]
        async fn staged_gap_advances_coverage_through_its_end() {
            let h = harness(Duration::from_secs(3600)).await;
            h.persistence
                .stage_gap("t", 0, make_gap(10, 30), (0, 0))
                .await;
            h.persistence.save_now().await.unwrap();
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(29));
            let stored = stored_manifest(&h.storage).await;
            assert_eq!(stored.topics[0].partitions[0].gaps.len(), 1);
            assert_eq!(stored.topics[0].partitions[0].covered_through, Some(29));
        }

        #[tokio::test]
        async fn end_of_cycle_and_finalize_upload_offsets_only_after_the_manifest() {
            let h = harness(Duration::from_secs(3600)).await;
            let checked = assert_offsets_never_ahead_of_manifest(&h.storage);

            for (i, (start, end)) in [(0, 9), (10, 19), (20, 29)].into_iter().enumerate() {
                h.persistence
                    .stage_segment("t", 0, seg_covering(start, end, start, end + 2))
                    .await;
                h.persistence
                    .stage_coverage("t", 1, (i as i64 + 1) * 3, (0, 0))
                    .await;
                h.persistence.end_cycle().await.unwrap();
            }
            h.persistence.stage_coverage("t", 0, 40, (0, 0)).await;
            h.persistence.finalize(Some("completed")).await.unwrap();

            assert!(
                checked.load(Ordering::SeqCst) >= 4,
                "offsets.db was uploaded"
            );
            let puts = h.storage.puts();
            let last_manifest = puts.iter().rposition(|k| k.ends_with("manifest.json"));
            let last_offsets = puts.iter().rposition(|k| k.ends_with("offsets.db"));
            assert!(
                last_manifest < last_offsets,
                "finalize saves the manifest before offsets.db: {puts:?}"
            );
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(40));
            assert_eq!(
                h.store.job_status(BID).await.unwrap().as_deref(),
                Some("completed")
            );
        }

        // -- startup seeding, resume planning, key guard -------------------

        fn stored_with_segment(key: &str, start: i64, end: i64) -> BackupManifest {
            let mut m = BackupManifest::new(BID.to_string());
            let part = m.get_or_create_topic("t").get_or_create_partition(0);
            part.add_segment(SegmentMetadata {
                key: key.to_string(),
                ..make_segment(key, start, end)
            });
            m
        }

        #[tokio::test]
        async fn seeding_from_the_stored_manifest_sets_reach_and_listed_keys() {
            let h = harness(Duration::from_secs(3600)).await;
            let mut stored = stored_with_segment("b/topics/t/partition=0/segment-0.bin", 0, 99);
            {
                let part = stored.get_or_create_topic("t").get_or_create_partition(0);
                part.covered_through = Some(120);
            }
            h.persistence.seed(&stored);
            assert_eq!(
                h.persistence.durable_reach(&tpk(0)),
                Some(DurableReach {
                    reach: 120,
                    explicit: Some(99),
                    tail: (0, 0)
                })
            );
            assert_eq!(h.persistence.durable_reach(&tpk(1)), None);
        }

        #[tokio::test]
        async fn writing_a_listed_segment_key_is_refused() {
            let h = harness(Duration::from_secs(3600)).await;
            let listed = "b/topics/t/partition=0/segment-00000000000000000000.bin.zst";
            h.persistence.seed(&stored_with_segment(listed, 0, 99));

            let err = h.persistence.ensure_unlisted(listed).unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains(listed) && msg.contains("already listed"),
                "{msg}"
            );

            // Keys staged during this run are listed too.
            let staged = seg_covering(100, 109, 100, 109);
            let staged_key = staged.key.clone();
            assert!(h.persistence.ensure_unlisted(&staged_key).is_ok());
            h.persistence.stage_segment("t", 0, staged).await;
            assert!(h.persistence.ensure_unlisted(&staged_key).is_err());
        }

        #[test]
        fn resume_from_the_manifest_when_it_is_ahead_of_the_checkpoint() {
            // The checkpoint lags by up to one progress interval.
            let plan = plan_resume(Some(99), Some(49)).unwrap();
            assert_eq!(
                plan,
                ResumeFrom::Manifest {
                    reach: 99,
                    checkpoint_ahead: None
                }
            );
            assert_eq!(plan.start(), 100);
        }

        #[test]
        fn resume_from_the_manifest_when_a_legacy_checkpoint_is_ahead() {
            // An older version checkpointed offsets no listed segment holds:
            // go back and re-read them (self-heal).
            let plan = plan_resume(Some(49), Some(99)).unwrap();
            assert_eq!(
                plan,
                ResumeFrom::Manifest {
                    reach: 49,
                    checkpoint_ahead: Some(99)
                }
            );
            assert_eq!(plan.start(), 50);
        }

        #[test]
        fn resume_from_a_legacy_checkpoint_only_without_a_manifest_entry() {
            assert_eq!(
                plan_resume(None, Some(99)),
                Some(ResumeFrom::LegacyCheckpoint(99))
            );
            assert_eq!(plan_resume(None, Some(99)).unwrap().start(), 100);
            assert_eq!(plan_resume(Some(99), None).unwrap().start(), 100);
            assert_eq!(plan_resume(Some(99), Some(99)).unwrap().start(), 100);
            assert_eq!(plan_resume(None, None), None);
        }

        // -- graceful shutdown drain ---------------------------------------

        #[tokio::test]
        async fn draining_waits_for_tasks_within_the_grace_then_aborts() {
            let mut tasks: JoinSet<u32> = JoinSet::new();
            tasks.spawn(async {
                tokio::time::sleep(Duration::from_millis(20)).await;
                1
            });
            tasks.spawn(async {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                2
            });
            let started = Instant::now();
            let (results, aborted) =
                drain_partition_tasks(&mut tasks, Duration::from_millis(200)).await;
            assert!(started.elapsed() < Duration::from_secs(10));
            assert_eq!(aborted, 1);
            let finished: Vec<u32> = results.into_iter().filter_map(|r| r.ok()).collect();
            assert_eq!(finished, vec![1]);
            assert!(tasks.is_empty());
        }

        // -- the partition loop, against a scripted fetcher -----------------

        type Step = std::result::Result<FetchResponse, Error>;

        fn record_at(o: i64) -> BackupRecord {
            BackupRecord {
                key: Some(format!("k{o}").into_bytes()),
                value: Some(format!("v{o}").into_bytes()),
                headers: vec![],
                timestamp: 1_000 + o,
                offset: o,
            }
        }

        /// A fetch returning records at `offsets`, positioned at `next`.
        fn response(offsets: Vec<i64>, next: i64) -> FetchResponse {
            FetchResponse {
                records: offsets.into_iter().map(record_at).collect(),
                next_offset: next,
                last_stable_offset: -1,
                ..FetchResponse::default()
            }
        }

        /// Returns scripted fetch results in order, then "nothing new".
        struct ScriptedFetcher {
            steps: parking_lot::Mutex<VecDeque<Step>>,
            offsets: (i64, i64),
            fetches: AtomicUsize,
            on_fetch: Option<Box<dyn Fn(usize) + Send + Sync>>,
            isolation_seen: parking_lot::Mutex<Vec<IsolationLevel>>,
        }

        impl ScriptedFetcher {
            fn new(
                steps: Vec<std::result::Result<(Vec<i64>, i64), Error>>,
                offsets: (i64, i64),
            ) -> Self {
                Self::with_responses(
                    steps
                        .into_iter()
                        .map(|step| step.map(|(o, next)| response(o, next)))
                        .collect(),
                    offsets,
                )
            }

            fn with_responses(steps: Vec<Step>, offsets: (i64, i64)) -> Self {
                Self {
                    steps: parking_lot::Mutex::new(steps.into()),
                    offsets,
                    fetches: AtomicUsize::new(0),
                    on_fetch: None,
                    isolation_seen: parking_lot::Mutex::new(Vec::new()),
                }
            }
        }

        #[async_trait::async_trait]
        impl PartitionFetcher for ScriptedFetcher {
            async fn fetch_partition(
                &self,
                _topic: &str,
                _partition: i32,
                offset: i64,
                _max_bytes: i32,
                isolation: IsolationLevel,
            ) -> Result<FetchResponse> {
                let n = self.fetches.fetch_add(1, Ordering::SeqCst) + 1;
                self.isolation_seen.lock().push(isolation);
                if let Some(hook) = &self.on_fetch {
                    hook(n);
                }
                match self.steps.lock().pop_front() {
                    Some(step) => step,
                    None => Ok(response(vec![], offset)),
                }
            }

            async fn partition_offsets(
                &self,
                _topic: &str,
                _partition: i32,
                isolation: IsolationLevel,
            ) -> Result<(i64, i64)> {
                self.isolation_seen.lock().push(isolation);
                Ok(self.offsets)
            }
        }

        fn records(range: std::ops::Range<i64>) -> Vec<i64> {
            range.collect()
        }

        fn context(
            h: &Harness,
            fetcher: Arc<dyn PartitionFetcher>,
            snapshot: SnapshotRange,
            options: BackupOptions,
            cancel: CancellationToken,
        ) -> BackupPartitionContext {
            BackupPartitionContext {
                topic: "t".to_string(),
                partition: 0,
                router: fetcher,
                storage: h.storage.clone(),
                backup_id: BID.to_string(),
                options,
                metrics: Arc::new(PerformanceMetrics::new()),
                prometheus_metrics: None,
                health: Arc::new(HealthCheck::new()),
                offset_store: Some(h.store.clone()),
                manifest_persistence: h.persistence.clone(),
                kafka_cb: Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default())),
                storage_cb: Arc::new(CircuitBreaker::new(CircuitBreakerConfig::default())),
                snapshot: Some(snapshot),
                cancel,
                resume_beyond_durable: false,
            }
        }

        fn small_segments(records: u64) -> BackupOptions {
            BackupOptions {
                segment_max_records: Some(records),
                segment_max_interval_ms: u64::MAX,
                sync_interval_secs: 0,
                ..BackupOptions::default()
            }
        }

        fn range(earliest: i64, start: i64, latest: i64) -> SnapshotRange {
            SnapshotRange {
                earliest,
                start,
                latest,
            }
        }

        /// Covered ranges of the stored segments, plus gaps, sorted.
        fn intervals(m: &BackupManifest) -> Vec<(i64, i64)> {
            let part = &m.topics[0].partitions[0];
            let mut v: Vec<(i64, i64)> = part
                .segments
                .iter()
                .map(|s| (s.covered_start(), s.covered_end()))
                .chain(part.gaps.iter().map(|g| (g.start_offset, g.end_offset - 1)))
                .collect();
            v.sort();
            v
        }

        #[tokio::test]
        async fn holes_and_trailing_markers_are_covered_contiguously() {
            let h = harness(Duration::from_secs(0)).await;
            // 0..=2 present, 3..=4 compacted; 5..=9 present then markers
            // 10..=11; an all-compacted fetch 12..=19; then 20..=21.
            let fetcher = Arc::new(ScriptedFetcher::new(
                vec![
                    Ok((vec![0, 1, 2, 5, 6, 7, 8, 9], 12)),
                    Ok((vec![], 20)),
                    Ok((vec![20, 21], 22)),
                ],
                (0, 22),
            ));
            context(
                &h,
                fetcher,
                range(0, 0, 22),
                small_segments(4),
                CancellationToken::new(),
            )
            .backup_partition()
            .await
            .unwrap();
            h.persistence.end_cycle().await.unwrap();

            let stored = stored_manifest(&h.storage).await;
            let iv = intervals(&stored);
            assert_eq!(iv.first().unwrap().0, 0);
            for pair in iv.windows(2) {
                assert_eq!(
                    pair[1].0,
                    pair[0].1 + 1,
                    "contiguous covered ranges: {iv:?}"
                );
            }
            assert_eq!(iv.last().unwrap().1, 21);
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(21));
            assert_eq!(stored.total_records(), 10);
        }

        #[tokio::test]
        async fn trailing_coverage_without_records_advances_the_checkpoint() {
            let h = harness(Duration::from_secs(0)).await;
            let fetcher = Arc::new(ScriptedFetcher::new(
                vec![Ok((vec![0, 1], 2)), Ok((vec![], 10))],
                (0, 10),
            ));
            context(
                &h,
                fetcher,
                range(0, 0, 10),
                small_segments(1000),
                CancellationToken::new(),
            )
            .backup_partition()
            .await
            .unwrap();
            h.persistence.end_cycle().await.unwrap();
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(9));
        }

        #[tokio::test]
        async fn failed_segment_upload_keeps_the_checkpoint_behind_it() {
            let h = harness(Duration::from_secs(0)).await;
            let checked = assert_offsets_never_ahead_of_manifest(&h.storage);
            // The second segment upload fails.
            h.storage.fail_puts(
                ".bin.zst",
                StorageError::Backend("upload failed".to_string()),
                1,
            );
            let fetcher = Arc::new(ScriptedFetcher::new(
                vec![
                    Ok((records(0..10), 10)),
                    Ok((records(10..20), 20)),
                    Ok((records(20..30), 30)),
                ],
                (0, 30),
            ));
            let result = context(
                &h,
                fetcher,
                range(0, 0, 30),
                small_segments(10),
                CancellationToken::new(),
            )
            .backup_partition()
            .await;
            assert!(result.is_err());
            h.storage.heal_puts();
            h.persistence.end_cycle().await.unwrap();

            let stored = stored_manifest(&h.storage).await;
            assert_eq!(
                reach_of(&stored, 0),
                Some(9),
                "nothing past the failed segment"
            );
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(9));
            assert!(checked.load(Ordering::SeqCst) > 0);
        }

        #[tokio::test]
        async fn fetch_error_flushes_buffered_records_before_failing() {
            let h = harness(Duration::from_secs(3600)).await;
            let fetcher = Arc::new(ScriptedFetcher::new(
                vec![
                    Ok((records(0..5), 5)),
                    Ok((records(5..8), 9)),
                    Err(Error::Kafka(KafkaError::BrokerError {
                        code: 6,
                        message: "NOT_LEADER".to_string(),
                    })),
                ],
                (0, 100),
            ));
            let result = context(
                &h,
                fetcher,
                range(0, 0, 100),
                small_segments(1000),
                CancellationToken::new(),
            )
            .backup_partition()
            .await;
            assert!(result.is_err());

            // The buffered records were written and the manifest saved
            // before the error was returned.
            let stored = stored_manifest(&h.storage).await;
            assert_eq!(stored.total_records(), 8);
            assert_eq!(reach_of(&stored, 0), Some(8));
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(8));
        }

        #[tokio::test]
        async fn retention_gap_is_recorded_after_the_records_before_it() {
            let h = harness(Duration::from_secs(3600)).await;
            let manifests: Arc<parking_lot::Mutex<Vec<BackupManifest>>> = Default::default();
            let seen = manifests.clone();
            h.storage.on_every_put("manifest.json", move |_, bytes| {
                seen.lock().push(serde_json::from_slice(bytes).unwrap());
            });
            let fetcher = Arc::new(ScriptedFetcher::new(
                vec![
                    Ok((records(0..10), 10)),
                    Err(Error::Kafka(KafkaError::BrokerError {
                        code: OFFSET_OUT_OF_RANGE,
                        message: "out of range".to_string(),
                    })),
                    Ok((records(50..60), 60)),
                ],
                (50, 60),
            ));
            context(
                &h,
                fetcher,
                range(0, 0, 60),
                small_segments(1000),
                CancellationToken::new(),
            )
            .backup_partition()
            .await
            .unwrap();
            h.persistence.end_cycle().await.unwrap();

            // No saved manifest ever lists the gap without the records 0..=9.
            for m in manifests.lock().iter() {
                if let Some(part) = m.topics.first().and_then(|t| t.partitions.first()) {
                    if !part.gaps.is_empty() {
                        assert!(
                            part.segments.iter().any(|s| s.start_offset == 0),
                            "gap saved before the segment holding 0..=9"
                        );
                    }
                }
            }
            let stored = stored_manifest(&h.storage).await;
            let part = &stored.topics[0].partitions[0];
            assert_eq!(
                (part.gaps[0].start_offset, part.gaps[0].end_offset),
                (10, 50)
            );
            assert_eq!(part.gaps[0].reason, OffsetGapReason::OffsetOutOfRange);
            assert_eq!(intervals(&stored), vec![(0, 9), (10, 49), (50, 59)]);
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(59));
        }

        #[tokio::test]
        async fn cancellation_stops_fetching_and_flushes_the_buffer() {
            let h = harness(Duration::from_secs(3600)).await;
            let cancel = CancellationToken::new();
            let trigger = cancel.clone();
            let mut fetcher = ScriptedFetcher::new(
                vec![
                    Ok((records(0..10), 10)),
                    Ok((records(10..20), 20)),
                    Ok((records(20..30), 30)),
                ],
                (0, 30),
            );
            // Shutdown arrives while the second fetch is in flight.
            fetcher.on_fetch = Some(Box::new(move |n| {
                if n == 2 {
                    trigger.cancel();
                }
            }));
            let fetcher = Arc::new(fetcher);
            context(
                &h,
                fetcher.clone(),
                range(0, 0, 30),
                small_segments(1000),
                cancel,
            )
            .backup_partition()
            .await
            .unwrap();
            assert_eq!(
                fetcher.fetches.load(Ordering::SeqCst),
                2,
                "no fetch after cancel"
            );

            h.persistence.finalize(Some("completed")).await.unwrap();
            let stored = stored_manifest(&h.storage).await;
            assert_eq!(stored.total_records(), 20);
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(19));
        }

        #[tokio::test]
        async fn resume_beyond_durable_records_a_gap_when_the_source_moved_on() {
            let h = harness(Duration::from_secs(3600)).await;
            let fetcher = Arc::new(ScriptedFetcher::new(
                vec![Ok((records(30..40), 40))],
                (30, 40),
            ));
            let mut ctx = context(
                &h,
                fetcher,
                range(30, 10, 40),
                small_segments(1000),
                CancellationToken::new(),
            );
            ctx.resume_beyond_durable = true;
            ctx.backup_partition().await.unwrap();
            h.persistence.end_cycle().await.unwrap();

            let stored = stored_manifest(&h.storage).await;
            let gap = &stored.topics[0].partitions[0].gaps[0];
            assert_eq!((gap.start_offset, gap.end_offset), (10, 30));
            assert_eq!(gap.reason, OffsetGapReason::ResumeBeyondDurable);
            assert_eq!(h.store.get_offset(BID, "t", 0).await.unwrap(), Some(39));
        }

        #[tokio::test]
        async fn resuming_continues_after_the_listed_segments_without_rewriting_them() {
            let h = harness(Duration::from_secs(3600)).await;
            // Previous run: one segment covering 0..=12 (records 0..=9) and
            // a coverage-only advance through 19.
            let mut stored = BackupManifest::new(BID.to_string());
            {
                let part = stored.get_or_create_topic("t").get_or_create_partition(0);
                part.add_segment(seg_covering(0, 9, 0, 12));
                part.covered_through = Some(19);
            }
            h.storage
                .put(
                    "b/manifest.json",
                    Bytes::from(serde_json::to_vec(&stored).unwrap()),
                )
                .await
                .unwrap();
            h.persistence.seed(&stored);
            let start = h.persistence.durable_reach(&tpk(0)).unwrap().reach + 1;
            assert_eq!(start, 20);

            let fetcher = Arc::new(ScriptedFetcher::new(
                vec![Ok((records(20..25), 25))],
                (0, 25),
            ));
            context(
                &h,
                fetcher,
                range(0, start, 25),
                small_segments(1000),
                CancellationToken::new(),
            )
            .backup_partition()
            .await
            .unwrap();
            h.persistence.end_cycle().await.unwrap();

            let stored = stored_manifest(&h.storage).await;
            // The new segment's covered range starts right after the last
            // explicit interval, absorbing the coverage-only tail 13..=19.
            assert_eq!(intervals(&stored), vec![(0, 12), (13, 24)]);
        }

        #[tokio::test]
        async fn a_run_that_would_rewrite_a_listed_key_fails_loudly() {
            let h = harness(Duration::from_secs(3600)).await;
            let listed = seg_covering(0, 9, 0, 9);
            let mut stored = BackupManifest::new(BID.to_string());
            stored
                .get_or_create_topic("t")
                .get_or_create_partition(0)
                .add_segment(listed);
            h.persistence.seed(&stored);

            // A (buggy) start at 0 instead of reach + 1.
            let fetcher = Arc::new(ScriptedFetcher::new(
                vec![Ok((records(0..10), 10))],
                (0, 10),
            ));
            let err = context(
                &h,
                fetcher,
                range(0, 0, 10),
                small_segments(1000),
                CancellationToken::new(),
            )
            .backup_partition()
            .await
            .unwrap_err();
            assert!(err.to_string().contains("already listed"), "{err}");
            assert!(
                !h.storage.puts().iter().any(|k| k.ends_with(".bin.zst")),
                "the listed segment must not be overwritten"
            );
        }

        // -- read_committed: markers and aborted records --------------------

        fn skip(last_offset: i64, control_records: u64, aborted_records: u64) -> SkippedBatch {
            SkippedBatch {
                last_offset,
                control_records,
                aborted_records,
            }
        }

        /// A read_committed fetch returning `offsets` and passing over
        /// `skipped`, with the given LSO and high watermark.
        fn txn_response(
            offsets: Vec<i64>,
            next: i64,
            skipped: Vec<SkippedBatch>,
            lso_hw: (i64, i64),
        ) -> FetchResponse {
            FetchResponse {
                control_records_skipped: skipped.iter().map(|s| s.control_records).sum(),
                aborted_records_dropped: skipped.iter().map(|s| s.aborted_records).sum(),
                skipped,
                last_stable_offset: lso_hw.0,
                high_watermark: lso_hw.1,
                ..response(offsets, next)
            }
        }

        fn segment_counts(m: &BackupManifest) -> Vec<((i64, i64), (u64, u64))> {
            m.topics[0].partitions[0]
                .segments
                .iter()
                .map(|s| {
                    (
                        (s.covered_start(), s.covered_end()),
                        (s.control_records_skipped, s.aborted_records_dropped),
                    )
                })
                .collect()
        }

        #[tokio::test]
        async fn transaction_skips_are_counted_against_the_segment_covering_them() {
            let h = harness(Duration::from_secs(0)).await;
            // 0..=2 aborted, 3 their ABORT marker, 4 5 committed, 6 COMMIT,
            // 7 8 9 records, 10 a trailing COMMIT. Segments rotate at 3
            // records: the first ends at 7.
            let fetcher = Arc::new(ScriptedFetcher::with_responses(
                vec![Ok(txn_response(
                    vec![4, 5, 7, 8, 9],
                    11,
                    vec![skip(2, 0, 3), skip(3, 1, 0), skip(6, 1, 0), skip(10, 1, 0)],
                    (11, 14),
                ))],
                (0, 11),
            ));
            let prom = Arc::new(PrometheusMetrics::new());
            let mut ctx = context(
                &h,
                fetcher.clone(),
                range(0, 0, 11),
                small_segments(3),
                CancellationToken::new(),
            );
            ctx.prometheus_metrics = Some(prom.clone());
            ctx.backup_partition().await.unwrap();
            h.persistence.end_cycle().await.unwrap();

            let stored = stored_manifest(&h.storage).await;
            assert_eq!(
                segment_counts(&stored),
                vec![((0, 7), (2, 3)), ((8, 10), (1, 0))]
            );
            assert_eq!(stored.total_records(), 5);
            assert_eq!(stored.total_control_records_skipped(), 3);
            assert_eq!(stored.total_aborted_records_dropped(), 3);
            assert_eq!(intervals(&stored), vec![(0, 7), (8, 10)]);
            assert!(stored.topics[0].partitions[0].gaps.is_empty());
            // The default is read_committed, for the fetch and the offsets.
            assert!(fetcher
                .isolation_seen
                .lock()
                .iter()
                .all(|i| *i == IsolationLevel::ReadCommitted));

            let encoded = prom.encode();
            assert!(
                encoded.contains(
                    "kafka_backup_lso_lag{topic=\"t\",partition=\"0\",backup_id=\"b\"} 3"
                ),
                "{encoded}"
            );
            assert!(
                encoded.contains("kafka_backup_control_records_skipped_total{backup_id=\"b\"} 3"),
                "{encoded}"
            );
            assert!(
                encoded.contains("kafka_backup_aborted_records_dropped_total{backup_id=\"b\"} 3"),
                "{encoded}"
            );
        }

        #[tokio::test]
        async fn read_uncommitted_option_reaches_the_fetch() {
            let h = harness(Duration::from_secs(0)).await;
            let fetcher = Arc::new(ScriptedFetcher::new(vec![Ok((records(0..3), 3))], (0, 3)));
            let options = BackupOptions {
                isolation_level: IsolationLevel::ReadUncommitted,
                ..small_segments(1000)
            };
            let mut ctx = context(
                &h,
                fetcher.clone(),
                range(0, 0, 3),
                options,
                CancellationToken::new(),
            );
            // Not a snapshot: the end comes from partition_offsets.
            ctx.snapshot = None;
            ctx.backup_partition().await.unwrap();
            let seen = fetcher.isolation_seen.lock().clone();
            assert!(seen.len() >= 2, "offsets and fetch: {seen:?}");
            assert!(seen.iter().all(|i| *i == IsolationLevel::ReadUncommitted));
        }

        /// A run whose last offsets are an aborted transaction and its
        /// marker ends on a coverage-only advance: the counts are kept on the
        /// partition, and the segment that later absorbs that range takes
        /// them over — in the same process and after a restart.
        #[tokio::test]
        async fn marker_only_tail_counts_are_kept_until_a_segment_takes_them() {
            let h = harness(Duration::from_secs(0)).await;
            let fetcher = Arc::new(ScriptedFetcher::with_responses(
                vec![
                    Ok(txn_response(vec![0, 1], 2, vec![], (6, 6))),
                    Ok(txn_response(
                        vec![],
                        6,
                        vec![skip(4, 0, 3), skip(5, 1, 0)],
                        (6, 6),
                    )),
                ],
                (0, 6),
            ));
            context(
                &h,
                fetcher,
                range(0, 0, 6),
                small_segments(2),
                CancellationToken::new(),
            )
            .backup_partition()
            .await
            .unwrap();
            h.persistence.end_cycle().await.unwrap();

            let first = stored_manifest(&h.storage).await;
            let part = &first.topics[0].partitions[0];
            assert_eq!(segment_counts(&first), vec![((0, 1), (0, 0))]);
            assert_eq!(part.covered_through, Some(5));
            assert_eq!(
                (
                    part.tail_control_records_skipped,
                    part.tail_aborted_records_dropped
                ),
                (1, 3)
            );
            assert_eq!(first.total_control_records_skipped(), 1);
            assert_eq!(first.total_aborted_records_dropped(), 3);

            // After a restart, seeding carries the tail counts.
            let restarted = harness(Duration::from_secs(0)).await;
            restarted.persistence.seed(&first);
            assert_eq!(
                restarted.persistence.durable_reach(&tpk(0)).unwrap().tail,
                (1, 3)
            );

            // The next cycle in the same process: the new segment absorbs
            // the tail and its counts; the partition's tail is reset.
            let start = h.persistence.durable_reach(&tpk(0)).unwrap().reach + 1;
            let fetcher = Arc::new(ScriptedFetcher::with_responses(
                vec![Ok(txn_response(vec![6, 7], 8, vec![], (8, 8)))],
                (0, 8),
            ));
            context(
                &h,
                fetcher,
                range(0, start, 8),
                small_segments(1000),
                CancellationToken::new(),
            )
            .backup_partition()
            .await
            .unwrap();
            h.persistence.end_cycle().await.unwrap();

            let second = stored_manifest(&h.storage).await;
            let part = &second.topics[0].partitions[0];
            assert_eq!(
                segment_counts(&second),
                vec![((0, 1), (0, 0)), ((2, 7), (1, 3))]
            );
            assert_eq!(
                (
                    part.tail_control_records_skipped,
                    part.tail_aborted_records_dropped
                ),
                (0, 0)
            );
            assert_eq!(
                second.total_control_records_skipped(),
                1,
                "not counted twice"
            );
            assert_eq!(second.total_aborted_records_dropped(), 3);
        }

        /// Counts noted just before a retention gap are neither lost nor
        /// counted twice: the gap carries them as the partition's tail
        /// counts, and the next segment takes them over.
        #[tokio::test]
        async fn transaction_counts_before_a_retention_gap_are_kept() {
            let h = harness(Duration::from_secs(0)).await;
            let fetcher = Arc::new(ScriptedFetcher::with_responses(
                vec![
                    // 0..=4 then a marker at 5; the segment rotates at 5
                    // records, leaving the marker in the uncovered tail.
                    Ok(txn_response(records(0..5), 6, vec![skip(5, 1, 0)], (6, 6))),
                    Err(Error::Kafka(KafkaError::BrokerError {
                        code: OFFSET_OUT_OF_RANGE,
                        message: "out of range".to_string(),
                    })),
                    Ok(txn_response(vec![20, 21], 22, vec![], (22, 22))),
                ],
                (20, 22),
            ));
            context(
                &h,
                fetcher,
                range(0, 0, 22),
                small_segments(5),
                CancellationToken::new(),
            )
            .backup_partition()
            .await
            .unwrap();
            h.persistence.end_cycle().await.unwrap();

            let stored = stored_manifest(&h.storage).await;
            let part = &stored.topics[0].partitions[0];
            // The gap starts at the first offset no segment covers, so it
            // absorbs the marker-only tail at 5 (an upper bound, as for
            // compacted offsets); its count moves on to the next segment.
            assert_eq!(
                (part.gaps[0].start_offset, part.gaps[0].end_offset),
                (5, 20)
            );
            assert_eq!(
                segment_counts(&stored),
                vec![((0, 4), (0, 0)), ((20, 21), (1, 0))]
            );
            assert_eq!(part.tail_control_records_skipped, 0);
            assert_eq!(stored.total_control_records_skipped(), 1);
        }
    }
}
