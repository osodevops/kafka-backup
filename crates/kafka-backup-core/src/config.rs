//! Configuration structures for Kafka backup and restore operations.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::warn;

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Operation mode
    pub mode: Mode,

    /// Unique identifier for this backup
    pub backup_id: String,

    /// Source Kafka cluster configuration (for backup mode)
    #[serde(default)]
    pub source: Option<KafkaConfig>,

    /// Target Kafka cluster configuration (for restore mode)
    #[serde(default)]
    pub target: Option<KafkaConfig>,

    /// Storage configuration (supports S3, Azure, GCS, Filesystem, Memory)
    pub storage: crate::storage::StorageBackendConfig,

    /// Backup-specific options
    #[serde(default)]
    pub backup: Option<BackupOptions>,

    /// Restore-specific options
    #[serde(default)]
    pub restore: Option<RestoreOptions>,

    /// Offset storage configuration
    #[serde(default)]
    pub offset_storage: Option<OffsetStorageConfig>,

    /// Metrics server configuration
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
}

/// Metrics server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MetricsConfig {
    /// Whether metrics server is enabled (default: true)
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,

    /// Port for the metrics HTTP server (default: 8080)
    #[serde(default = "default_metrics_port")]
    pub port: u16,

    /// Bind address for the metrics server (default: "0.0.0.0")
    #[serde(default = "default_metrics_bind_address")]
    pub bind_address: String,

    /// Path for the metrics endpoint (default: "/metrics")
    #[serde(default = "default_metrics_path")]
    pub path: String,

    /// How often metrics are recalculated in milliseconds (default: 500)
    #[serde(default = "default_metrics_update_interval_ms")]
    pub update_interval_ms: u64,

    /// Keep serving metrics for this many seconds after a one-shot operation
    /// completes (default: 0).
    #[serde(default = "default_metrics_keep_alive_seconds")]
    pub keep_alive_seconds: u64,

    /// Maximum unique topic/partition label sets to expose (default: 100).
    /// Set to 0 to disable the limit; this can create high-cardinality metrics.
    #[serde(default = "default_max_partition_labels")]
    pub max_partition_labels: usize,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            port: default_metrics_port(),
            bind_address: default_metrics_bind_address(),
            path: default_metrics_path(),
            update_interval_ms: default_metrics_update_interval_ms(),
            keep_alive_seconds: default_metrics_keep_alive_seconds(),
            max_partition_labels: default_max_partition_labels(),
        }
    }
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_port() -> u16 {
    8080
}

fn default_metrics_bind_address() -> String {
    "0.0.0.0".to_string()
}

fn default_metrics_path() -> String {
    "/metrics".to_string()
}

fn default_metrics_update_interval_ms() -> u64 {
    500
}

fn default_metrics_keep_alive_seconds() -> u64 {
    0
}

fn default_max_partition_labels() -> usize {
    100
}

/// Offset storage configuration for tracking backup progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetStorageConfig {
    /// Storage backend type. **Deprecated since 0.22.0** — only `sqlite` is
    /// implemented; `memory` is accepted but ignored (a warning is logged).
    #[serde(default)]
    pub backend: OffsetStorageBackend,

    /// Path to local SQLite database file
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,

    /// **Deprecated since 0.22.0 — ignored.** The offset database is always
    /// stored at `{backup_id}/offsets.db` under the storage prefix (`prune`,
    /// `status` and the operator rely on that location). A warning is logged
    /// when this is set.
    #[serde(default)]
    pub s3_key: Option<String>,

    /// Override `backup.sync_interval_secs` for syncing the offset database to
    /// remote storage. When unset the backup value (default 30) applies.
    #[serde(default)]
    pub sync_interval_secs: Option<u64>,
}

impl Default for OffsetStorageConfig {
    fn default() -> Self {
        Self {
            backend: OffsetStorageBackend::default(),
            db_path: default_db_path(),
            s3_key: None,
            sync_interval_secs: None,
        }
    }
}

fn default_db_path() -> PathBuf {
    PathBuf::from("./offsets.db")
}

/// Offset storage backend type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OffsetStorageBackend {
    #[default]
    Sqlite,
    Memory,
}

/// Operation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Backup,
    Restore,
}

/// Kafka cluster configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaConfig {
    /// Bootstrap servers
    pub bootstrap_servers: Vec<String>,

    /// Security configuration
    #[serde(default)]
    pub security: SecurityConfig,

    /// Topic selection
    #[serde(default)]
    pub topics: TopicSelection,

    /// TCP connection settings (optional - uses defaults if not specified)
    #[serde(default)]
    pub connection: ConnectionConfig,
}

/// Security configuration for Kafka connections
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecurityConfig {
    /// Security protocol
    #[serde(default)]
    pub security_protocol: SecurityProtocol,

    /// SASL mechanism (if using SASL)
    #[serde(default)]
    pub sasl_mechanism: Option<SaslMechanism>,

    /// SASL username
    #[serde(default)]
    pub sasl_username: Option<String>,

    /// SASL password (consider using environment variable interpolation)
    #[serde(default)]
    pub sasl_password: Option<String>,

    /// Path to CA certificate file (for TLS)
    #[serde(default)]
    pub ssl_ca_location: Option<PathBuf>,

    /// Path to client certificate file (for mTLS)
    #[serde(default)]
    pub ssl_certificate_location: Option<PathBuf>,

    /// Path to client key file (for mTLS)
    #[serde(default)]
    pub ssl_key_location: Option<PathBuf>,

    /// Kerberos service name. Must match the broker's
    /// `sasl.kerberos.service.name` (typically `kafka`). Only meaningful
    /// when `sasl_mechanism: GSSAPI`. Defaults to `kafka` at the CLI
    /// wiring layer if unset.
    #[serde(default)]
    pub sasl_kerberos_service_name: Option<String>,

    /// Path to a Kerberos keytab file. If unset, the OS credential cache
    /// (populated via `kinit`) is used. Only meaningful when
    /// `sasl_mechanism: GSSAPI`.
    #[serde(default)]
    pub sasl_keytab_path: Option<PathBuf>,

    /// Path to a Kerberos `krb5.conf`. If unset, the system default is
    /// used. Only meaningful when `sasl_mechanism: GSSAPI`.
    #[serde(default)]
    pub sasl_krb5_config_path: Option<PathBuf>,

    /// Optional pluggable SASL mechanism factory.
    ///
    /// When set, overrides [`sasl_mechanism`] and drives the handshake
    /// through a plugin built per `KafkaClient` (e.g. `OAUTHBEARER` for
    /// MSK IAM, or `GSSAPI` for Kerberos). Not YAML-configurable —
    /// downstream crates inject this programmatically after parsing the
    /// config.
    ///
    /// The factory is invoked once per `KafkaClient` in
    /// `KafkaClient::authenticate`, with the broker endpoint that was
    /// successfully dialed. The `PartitionLeaderRouter` rewrites
    /// `bootstrap_servers` to the advertised per-broker `host:port` before
    /// spawning pooled clients, so stateful mechanisms (GSSAPI) receive the
    /// correct SPN hostname and stateless mechanisms (PLAIN, OAUTHBEARER)
    /// can ignore the argument via [`SharedPluginFactory`].
    ///
    /// [`SharedPluginFactory`]: crate::kafka::SharedPluginFactory
    #[serde(skip)]
    pub sasl_mechanism_plugin_factory: Option<crate::kafka::SaslMechanismPluginFactoryHandle>,
}

/// Security protocol
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SecurityProtocol {
    #[default]
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

/// TCP connection configuration for Kafka broker connections.
///
/// These settings help maintain stable connections to cloud-hosted Kafka services
/// (like Confluent Cloud) that may terminate idle connections.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ConnectionConfig {
    /// Enable TCP keepalive to prevent idle connection termination (default: true)
    pub tcp_keepalive: bool,

    /// Time in seconds before first keepalive probe is sent (default: 60)
    pub keepalive_time_secs: u64,

    /// Interval in seconds between keepalive probes (default: 20)
    pub keepalive_interval_secs: u64,

    /// Enable TCP_NODELAY to disable Nagle's algorithm (default: true)
    pub tcp_nodelay: bool,

    /// Number of TCP connections to maintain per broker (default: 4).
    ///
    /// Each KafkaClient uses a single TCP connection protected by a mutex.
    /// With a single connection per broker, all concurrent tasks serialize
    /// their requests, and network latency amplifies into a bottleneck.
    /// Multiple connections allow parallel in-flight requests to the same
    /// broker, dramatically improving throughput on high-latency connections.
    #[serde(default = "default_connections_per_broker")]
    pub connections_per_broker: usize,
}

fn default_connections_per_broker() -> usize {
    4
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            tcp_keepalive: true,
            keepalive_time_secs: 60,
            keepalive_interval_secs: 20,
            tcp_nodelay: true,
            connections_per_broker: default_connections_per_broker(),
        }
    }
}

/// SASL mechanism
///
/// `#[non_exhaustive]`: match on it with a wildcard arm, as more
/// mechanisms may be added.
///
/// SCRAM accepts Kafka's own names (`SCRAM-SHA-256`, `SCRAM-SHA-512`, as in
/// `sasl.mechanism`) and the `SCRAM-SHA256` / `SCRAM-SHA512` spellings
/// earlier releases required. Serialisation keeps the latter, so configs
/// written by this version still load in older ones.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
#[non_exhaustive]
pub enum SaslMechanism {
    Plain,
    #[serde(alias = "SCRAM-SHA-256")]
    ScramSha256,
    #[serde(alias = "SCRAM-SHA-512")]
    ScramSha512,
    /// SASL/GSSAPI (Kerberos). The enum variant is always present, but
    /// constructing a working client requires the `gssapi` cargo feature
    /// on `kafka-backup-cli` (which propagates to `kafka-backup-core`).
    /// CLI builds without the feature surface a clear runtime error when
    /// a config requests `GSSAPI`.
    Gssapi,
    /// SASL/OAUTHBEARER (RFC 7628), e.g. Confluent Cloud OAuth. YAML
    /// spelling `OAUTHBEARER`. This crate ships no token provider: a
    /// [`SaslMechanismPlugin`](crate::kafka::SaslMechanismPlugin) must be
    /// installed through `SecurityConfig::sasl_mechanism_plugin_factory`
    /// (Kafka Backup Enterprise does this when its `oauth` licence feature
    /// is enabled). Without one, authentication fails with an error that
    /// says so.
    #[serde(rename = "OAUTHBEARER")]
    OAuthBearer,
}

/// Error raised when `sasl_mechanism: OAUTHBEARER` reaches authentication
/// without a plugin factory.
pub const OAUTHBEARER_REQUIRES_PLUGIN: &str = "SASL OAUTHBEARER requires Kafka Backup Enterprise \
     (licence feature `oauth`) or a custom SaslMechanismPlugin";

/// Hint for OAuth-looking keys found under `source.security` /
/// `target.security`.
pub const OAUTH_SETTINGS_HINT: &str =
    "OAuth settings belong under enterprise.oauth (Kafka Backup Enterprise)";

/// Topic selection configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopicSelection {
    /// Topics to include (supports glob patterns)
    #[serde(default)]
    pub include: Vec<String>,

    /// Topics to exclude (supports glob patterns)
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// Legacy storage backend configuration
///
/// **Deprecated**: Use `StorageBackendConfig` from the `storage` module instead.
/// This type is maintained for backward compatibility only.
#[deprecated(
    since = "0.2.0",
    note = "Use crate::storage::StorageBackendConfig instead, which supports Azure, GCS, and more backends"
)]
#[allow(deprecated)] // Allow using deprecated StorageBackendType within this deprecated struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Storage backend type
    pub backend: StorageBackendType,

    /// Path for filesystem backend
    #[serde(default)]
    pub path: Option<PathBuf>,

    /// S3-compatible storage endpoint
    #[serde(default)]
    pub endpoint: Option<String>,

    /// Bucket name for S3-compatible storage
    #[serde(default)]
    pub bucket: Option<String>,

    /// Access key for S3-compatible storage
    #[serde(default)]
    pub access_key: Option<String>,

    /// Secret key for S3-compatible storage
    #[serde(default)]
    pub secret_key: Option<String>,

    /// Prefix for all storage keys
    #[serde(default)]
    pub prefix: Option<String>,

    /// Region for S3-compatible storage
    #[serde(default)]
    pub region: Option<String>,
}

/// Legacy storage backend type enum
///
/// **Deprecated**: Use `StorageBackendConfig` from the `storage` module instead.
#[deprecated(
    since = "0.2.0",
    note = "Use crate::storage::StorageBackendConfig instead, which supports Azure, GCS, and more backends"
)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StorageBackendType {
    Filesystem,
    S3,
}

/// What to do when a literal (non-glob) `topics.include` entry does not exist
/// in the cluster (issue #167). Glob patterns that match nothing are always
/// skipped silently.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OnMissingTopic {
    /// Fail the run (default) — a requested topic that is absent must not
    /// produce a "successful" backup with zero records for it.
    #[default]
    Fail,
    /// Log a warning, record the topic in the manifest's `missing_topics`,
    /// expose it via `kafka_backup_missing_topics`, and continue. The run
    /// still fails if nothing is left to back up.
    Warn,
}

/// Kafka circuit-breaker settings for backup and restore.
///
/// The breaker is **advisory**: the engines record successes and failures and
/// log state transitions, but do not currently block requests when Open.
/// Transient connection and leadership errors are retried by
/// `PartitionLeaderRouter` before a partition fails. Exposing these knobs
/// lets operators tune when the breaker opens, or keep it permanently Closed
/// with `enabled: false`. The `HealthCheck` component status
/// (`Component kafka became Degraded` / `recovered`) is independent of the
/// breaker and is not affected by these settings — see issue #197.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CircuitBreakerSettings {
    /// When `false`, the Kafka breaker is a no-op (always Closed). Default: `true`.
    pub enabled: bool,

    /// Consecutive failures before the breaker opens. Default: `5`.
    pub failure_threshold: u32,

    /// Milliseconds to wait in Open before transitioning to HalfOpen. Default: `30000`.
    pub reset_timeout_ms: u64,

    /// Consecutive successes in HalfOpen required to close. Default: `2`.
    pub success_threshold: u32,
}

impl Default for CircuitBreakerSettings {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: 5,
            reset_timeout_ms: 30_000,
            success_threshold: 2,
        }
    }
}

impl CircuitBreakerSettings {
    /// Validate circuit-breaker settings.
    pub fn validate(&self) -> crate::Result<()> {
        if !self.enabled {
            return Ok(());
        }
        if self.failure_threshold < 1 {
            return Err(crate::Error::Config(
                "circuit_breaker.failure_threshold must be >= 1".to_string(),
            ));
        }
        if self.success_threshold < 1 {
            return Err(crate::Error::Config(
                "circuit_breaker.success_threshold must be >= 1".to_string(),
            ));
        }
        if self.reset_timeout_ms < 1 {
            return Err(crate::Error::Config(
                "circuit_breaker.reset_timeout_ms must be >= 1".to_string(),
            ));
        }
        Ok(())
    }
}

/// Transaction isolation for the reads a backup makes (Fetch and
/// ListOffsets), the same choice as the Java consumer's `isolation.level`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationLevel {
    /// Archive only what a `read_committed` consumer sees (default since
    /// 0.24): records of committed transactions and non-transactional
    /// records. Records of aborted transactions and the transaction markers
    /// (commit/abort control records) are not archived, and a partition is
    /// read up to its last stable offset (LSO), not its high watermark — the
    /// records of a transaction still open are archived by a later run, once
    /// it has committed.
    #[default]
    #[serde(alias = "READ_COMMITTED")]
    ReadCommitted,
    /// Archive every record up to the high watermark, as versions before
    /// 0.24 did: records of aborted and open transactions are archived, and
    /// so are the transaction markers, which a restore writes back as
    /// ordinary records. Only for topics without transactional producers, or
    /// to keep an existing backup set uniform.
    #[serde(alias = "READ_UNCOMMITTED")]
    ReadUncommitted,
}

impl IsolationLevel {
    /// The Kafka protocol value (`isolation_level` in Fetch and ListOffsets).
    pub fn as_protocol(self) -> i8 {
        match self {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }

    /// The configuration spelling (`read_committed` / `read_uncommitted`).
    pub fn as_str(self) -> &'static str {
        match self {
            IsolationLevel::ReadCommitted => "read_committed",
            IsolationLevel::ReadUncommitted => "read_uncommitted",
        }
    }
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Backup-specific options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupOptions {
    /// Maximum segment size in bytes (default: 128MB)
    #[serde(default = "default_segment_max_bytes")]
    pub segment_max_bytes: u64,

    /// Maximum segment time interval in milliseconds (default: 60000)
    #[serde(default = "default_segment_max_interval_ms")]
    pub segment_max_interval_ms: u64,

    /// Compression algorithm
    #[serde(default)]
    pub compression: CompressionType,

    /// Compression level (zstd: 1-22, default: 3)
    #[serde(default = "default_compression_level")]
    pub compression_level: i32,

    /// Starting offset for backup
    #[serde(default)]
    pub start_offset: StartOffset,

    /// Run continuously or one-shot (default: one-shot)
    #[serde(default)]
    pub continuous: bool,

    /// Transaction isolation for the backup's reads: `read_committed`
    /// (default) archives what a `read_committed` consumer sees and stops at
    /// the last stable offset; `read_uncommitted` archives everything up to
    /// the high watermark, including aborted records and transaction
    /// markers, as versions before 0.24 did. See [`IsolationLevel`].
    #[serde(default)]
    pub isolation_level: IsolationLevel,

    /// Include internal topics (default: false)
    #[serde(default)]
    pub include_internal_topics: bool,

    /// Internal topics to backup (e.g., __consumer_offsets, __transaction_state)
    #[serde(default)]
    pub internal_topics: Vec<String>,

    /// Behaviour when a literal `topics.include` entry is absent from the
    /// cluster: `fail` (default) or `warn` (skip it, record it, continue).
    #[serde(default)]
    pub on_missing_topic: OnMissingTopic,

    /// **Deprecated since 0.22.0 — no effect.** Offsets are checkpointed at
    /// the end of every backup cycle; a warning is logged when this is set to
    /// a non-default value.
    #[serde(default = "default_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,

    /// Sync to remote storage interval in seconds (default: 30)
    #[serde(default = "default_sync_interval_secs")]
    pub sync_interval_secs: u64,

    /// Add offset-tracking headers to every record as it is archived
    /// (Phase 1 of the three-phase restore).
    ///
    /// Adds `x-original-offset` and `x-original-timestamp` (little-endian
    /// `i64`), plus `x-source-cluster` when `source_cluster_id` is set. They
    /// are what makes `consumer_group_strategy: header-based` recovery
    /// possible after a restore, but they also mean an archived record is
    /// not header-for-header identical to the source record.
    ///
    /// **Default: `true`.** Set to `false` for a verbatim archive, or leave
    /// it on and use `restore.strip_offset_headers: true` to drop the
    /// headers at restore time.
    #[serde(default = "default_include_offset_headers")]
    pub include_offset_headers: bool,

    /// Source cluster identifier for x-source-cluster header
    /// Useful for tracking which cluster the backup originated from
    #[serde(default)]
    pub source_cluster_id: Option<String>,

    /// Snapshot mode: capture each partition's end offset at backup start and
    /// stop when all partitions have reached their target offsets. The end is
    /// the last stable offset under `isolation_level: read_committed` (the
    /// default) and the high watermark under `read_uncommitted`.
    ///
    /// This provides consistent "point-in-time" snapshots for DR backups.
    /// When enabled:
    /// 1. At startup, captures the end offsets of ALL partitions
    /// 2. Each partition backs up until it reaches its snapshot target
    /// 3. Backup exits cleanly when all partitions are caught up
    ///
    /// Incompatible with `continuous: true` mode.
    /// Default: false
    #[serde(default)]
    pub stop_at_current_offsets: bool,

    /// Maximum concurrent partitions to backup in parallel.
    ///
    /// Limits resource usage (CPU, memory, network, storage I/O) by controlling
    /// how many partitions are processed simultaneously. Higher values increase
    /// throughput but may cause resource contention.
    ///
    /// Default: 8
    #[serde(default = "default_backup_max_concurrent_partitions")]
    pub max_concurrent_partitions: usize,

    /// Poll interval in milliseconds for continuous backup mode.
    ///
    /// In continuous mode, after each complete pass through all partitions,
    /// the backup waits this long before starting the next pass.
    /// Lower values reduce lag but increase CPU usage.
    ///
    /// Default: 100ms (was hardcoded to 1000ms)
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// Snapshot consumer group offsets to storage after each backup cycle.
    ///
    /// When `true`, the backup engine queries every broker for consumer groups,
    /// fetches their committed offsets, filters to groups with offsets on
    /// backed-up topics, and writes `{backup_id}/consumer-groups-snapshot.json`.
    ///
    /// This file is consumed by `auto_consumer_groups: true` at restore time.
    /// Default: `false` — opt in explicitly to avoid unexpected overhead.
    #[serde(default)]
    pub consumer_group_snapshot: bool,

    /// Maximum bytes requested per Kafka Fetch request.
    ///
    /// When unset, fetches request `min(segment_max_bytes, 16MB)`.
    /// Values are clamped to the Kafka protocol's i32 range.
    #[serde(default)]
    pub fetch_max_bytes: Option<u64>,

    /// Maximum records per segment before rotation.
    ///
    /// When unset, segments rotate on `segment_max_bytes` /
    /// `segment_max_interval_ms` only.
    #[serde(default)]
    pub segment_max_records: Option<u64>,

    /// Capture explicit, mutable topic-level configuration overrides in the
    /// backup manifest. Sensitive, read-only, and broker-default values are
    /// never persisted.
    #[serde(default = "default_capture_topic_configs")]
    pub capture_topic_configs: bool,

    /// Fail the backup when topic configuration cannot be captured. This is
    /// recommended for compliance and disaster-recovery jobs.
    #[serde(default)]
    pub require_topic_configs: bool,

    /// Retention for this backup set: prune aged/oversized segments at the
    /// end of each backup cycle (see `kafka-backup prune` for the on-demand
    /// form). Off by default. Bucket lifecycle rules must NOT be used on an
    /// incremental set — see the storage guide.
    #[serde(default)]
    pub retention: Option<RetentionOptions>,

    /// Kafka circuit-breaker settings (advisory — never blocks requests; see
    /// [`CircuitBreakerSettings`]). Default matches the previous hardcoded
    /// thresholds (failure=5, reset=30s, success=2).
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerSettings,

    /// On a graceful shutdown (SIGTERM / Ctrl-C), how long partition tasks
    /// get to stop fetching, write their buffered records as a final segment
    /// and record it in the manifest before they are aborted. An aborted
    /// partition loses nothing durable: its unsaved progress is re-read on
    /// the next run. Default: 30 seconds.
    #[serde(default = "default_shutdown_grace_secs")]
    pub shutdown_grace_secs: u64,
}

fn default_shutdown_grace_secs() -> u64 {
    30
}

/// Retention policy for a backup set (issue #169).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RetentionOptions {
    /// Prune segments older than this human-readable duration (`30d`, `12h`,
    /// `1d12h`, …). Deviates from the `_secs` convention deliberately:
    /// `2592000` is not a number a reviewer can read, and the operator's
    /// `retention.maxAge` already uses this grammar.
    #[serde(default)]
    pub max_age: Option<String>,

    /// After the age pass, keep pruning oldest-first until the set's total
    /// compressed size fits under this many bytes (compressed sizes).
    #[serde(default)]
    pub max_total_bytes: Option<u64>,

    /// Never prune a partition below this many newest segments.
    #[serde(default = "default_keep_segments")]
    pub keep_segments: usize,
}

fn default_keep_segments() -> usize {
    1
}

impl RetentionOptions {
    /// Convert to prune criteria, resolving `max_age` against "now".
    pub fn to_criteria(&self) -> crate::Result<crate::backup::prune::PruneCriteria> {
        let older_than = match &self.max_age {
            Some(raw) => {
                let d = crate::util::parse_duration(raw)?;
                Some(chrono::Utc::now().timestamp_millis() - d.as_millis() as i64)
            }
            None => None,
        };
        Ok(crate::backup::prune::PruneCriteria {
            older_than,
            max_total_bytes: self.max_total_bytes,
            keep_segments: self.keep_segments.max(1),
        })
    }

    pub fn validate(&self) -> crate::Result<()> {
        if let Some(raw) = &self.max_age {
            crate::util::parse_duration(raw)?;
        }
        if self.max_age.is_none() && self.max_total_bytes.is_none() {
            return Err(crate::Error::Config(
                "backup.retention requires max_age and/or max_total_bytes".to_string(),
            ));
        }
        if self.keep_segments == 0 {
            return Err(crate::Error::Config(
                "backup.retention.keep_segments must be >= 1".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_capture_topic_configs() -> bool {
    true
}

fn default_include_offset_headers() -> bool {
    true
}

fn default_backup_max_concurrent_partitions() -> usize {
    8 // Balance between parallelism and resource usage
}

fn default_poll_interval_ms() -> u64 {
    100 // 100ms - much lower than old hardcoded 1000ms for better lag reduction
}

impl Default for BackupOptions {
    fn default() -> Self {
        Self {
            segment_max_bytes: default_segment_max_bytes(),
            segment_max_interval_ms: default_segment_max_interval_ms(),
            compression: CompressionType::default(),
            compression_level: default_compression_level(),
            start_offset: StartOffset::default(),
            continuous: false,
            isolation_level: IsolationLevel::default(),
            include_internal_topics: false,
            internal_topics: Vec::new(),
            on_missing_topic: OnMissingTopic::default(),
            checkpoint_interval_secs: default_checkpoint_interval_secs(),
            sync_interval_secs: default_sync_interval_secs(),
            include_offset_headers: default_include_offset_headers(),
            source_cluster_id: None,
            stop_at_current_offsets: false,
            max_concurrent_partitions: default_backup_max_concurrent_partitions(),
            poll_interval_ms: default_poll_interval_ms(),
            consumer_group_snapshot: false,
            fetch_max_bytes: None,
            segment_max_records: None,
            capture_topic_configs: default_capture_topic_configs(),
            require_topic_configs: false,
            retention: None,
            circuit_breaker: CircuitBreakerSettings::default(),
            shutdown_grace_secs: default_shutdown_grace_secs(),
        }
    }
}

fn default_segment_max_bytes() -> u64 {
    128 * 1024 * 1024 // 128MB
}

fn default_segment_max_interval_ms() -> u64 {
    60_000 // 60 seconds
}

fn default_compression_level() -> i32 {
    3 // zstd default
}

fn default_checkpoint_interval_secs() -> u64 {
    5 // 5 seconds
}

fn default_sync_interval_secs() -> u64 {
    30 // 30 seconds
}

/// Compression type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    #[default]
    Zstd,
    Lz4,
}

/// Starting offset for backup
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StartOffset {
    #[default]
    Earliest,
    Latest,
    /// Specific offset per partition (topic -> partition -> offset)
    #[serde(rename = "specific")]
    Specific(std::collections::HashMap<String, std::collections::HashMap<i32, i64>>),
}

/// Repartitioning strategy for restoring to a different partition count
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RepartitioningStrategy {
    /// Murmur2 hash partitioning (Kafka default): murmur2(key) % N.
    /// Records with a null key use round-robin. Empty keys (`[]`) ARE hashed.
    #[default]
    Murmur2,
    /// Round-robin for all records regardless of key.
    Automatic,
}

impl std::fmt::Display for RepartitioningStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Murmur2 => write!(f, "murmur2"),
            Self::Automatic => write!(f, "automatic"),
        }
    }
}

/// Serialize a `HashMap` with its entries sorted by key.
///
/// `HashMap` iteration order differs from run to run, and the restore
/// checkpoint is keyed by a hash of the serialized `RestoreOptions`. Sorting
/// makes that hash stable, so a restore whose options contain a map with two
/// or more entries can resume its checkpoint. Maps with zero or one entry
/// serialize exactly as before, which keeps existing checkpoint hashes.
fn serialize_sorted_map<K, V, S>(
    map: &std::collections::HashMap<K, V>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    K: Serialize + Ord,
    V: Serialize,
    S: serde::Serializer,
{
    let sorted: std::collections::BTreeMap<&K, &V> = map.iter().collect();
    sorted.serialize(serializer)
}

/// Per-topic repartitioning configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicRepartitioning {
    /// Partitioning strategy to use
    #[serde(default)]
    pub strategy: RepartitioningStrategy,
    /// Number of partitions in the target topic (must be > 0)
    pub target_partitions: i32,
}

/// Consumer offset handling strategy during restore
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OffsetStrategy {
    /// Skip offset handling entirely (just restore data)
    #[default]
    Skip,
    /// Store original offset in message header (x-original-offset, x-original-timestamp)
    HeaderBased,
    /// Use timestamp-based seeking in target cluster
    TimestampBased,
    /// Scan target cluster and auto-map offsets
    ClusterScan,
    /// Report mapping only, require manual reset
    Manual,
}

/// Policy for restoring topic configuration to topics that already exist.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExistingTopicConfigPolicy {
    /// Report configuration drift but do not mutate an existing topic.
    #[default]
    ValidateOnly,
    /// Apply source topic overrides to an existing topic.
    Apply,
    /// Fail restore when an existing topic differs from the backup.
    Fail,
}

/// What a restore does with captured topic configuration the target does not
/// take as is (`restore.unsupported_config_policy`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnsupportedConfigPolicy {
    /// Restore without the keys the target does not report, reports
    /// read-only or rejects, clamp values outside the `target_profile`
    /// limits, and list both in the restore report.
    #[default]
    Skip,
    /// Fail the restore, listing every such key, before any configuration
    /// is changed.
    Fail,
}

/// Known limits of a restore target platform (`restore.target_profile`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TargetProfile {
    /// Confluent Cloud: replication factor 3 for created topics and the
    /// documented topic-configuration ranges.
    ConfluentCloud,
}

/// Confluent Cloud cluster type (`restore.target_cluster_type`); some
/// `target_profile: confluent_cloud` limits depend on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetClusterType {
    Basic,
    Standard,
    Enterprise,
    Dedicated,
}

fn is_default<T: Default + PartialEq>(value: &T) -> bool {
    *value == T::default()
}

/// Where a restore keeps its checkpoint.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RestoreCheckpointStorage {
    /// The local `checkpoint_state` file.
    #[default]
    Local,
    /// Backup storage only.
    Remote,
    /// The local file and backup storage.
    Both,
}

/// Restore-specific options
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreOptions {
    /// Time window start (epoch milliseconds) for PITR
    #[serde(default)]
    pub time_window_start: Option<i64>,

    /// Time window end (epoch milliseconds) for PITR
    #[serde(default)]
    pub time_window_end: Option<i64>,

    /// Source partitions to restore (if filtering specific partitions)
    #[serde(default)]
    pub source_partitions: Option<Vec<i32>>,

    /// Partition mapping (source_partition -> target_partition)
    #[serde(default, serialize_with = "serialize_sorted_map")]
    pub partition_mapping: std::collections::HashMap<i32, i32>,

    /// Topic mapping (original -> target)
    #[serde(default, serialize_with = "serialize_sorted_map")]
    pub topic_mapping: std::collections::HashMap<String, String>,

    /// Consumer group offset handling strategy
    #[serde(default)]
    pub consumer_group_strategy: OffsetStrategy,

    /// Dry-run mode - validate without writing to target cluster
    #[serde(default)]
    pub dry_run: bool,

    /// Add `x-original-offset`, `x-original-timestamp` and
    /// `x-source-partition` headers to every record as it is produced to
    /// the target cluster. Also implied by `consumer_group_strategy:
    /// header-based`. Default: `false`.
    #[serde(default)]
    pub include_original_offset_header: bool,

    /// Remove the headers kafka-backup adds (`x-original-offset`,
    /// `x-original-timestamp`, `x-source-cluster`, `x-source-partition`)
    /// from archived records before producing them, so a restored record
    /// carries exactly the headers the source record had.
    ///
    /// Use this to restore an archive taken with the default
    /// `backup.include_offset_headers: true` header-for-header identical to
    /// the source. Offset mapping is unaffected: the source offset is stored
    /// natively in the segment, not only in the header. If
    /// `include_original_offset_header` / `header-based` is also on, the
    /// headers are stripped first and fresh ones injected.
    ///
    /// Default: `false`.
    #[serde(default)]
    pub strip_offset_headers: bool,

    /// Rate limit in records per second for the whole restore: one token
    /// bucket shared by every partition (before 0.24 each partition was
    /// limited to this rate on its own).
    #[serde(default)]
    pub rate_limit_records_per_sec: Option<u64>,

    /// Rate limit in bytes per second for the whole restore, shared by
    /// every partition. Bytes are the records' uncompressed encoded size
    /// (key, value, headers and record framing).
    #[serde(default)]
    pub rate_limit_bytes_per_sec: Option<u64>,

    /// Maximum concurrent partitions to restore in parallel
    #[serde(default = "default_max_concurrent_partitions")]
    pub max_concurrent_partitions: usize,

    /// Batch size for producing to target cluster (number of records)
    #[serde(default = "default_produce_batch_size")]
    pub produce_batch_size: usize,

    /// Maximum uncompressed size of one produce batch in bytes (default
    /// 1 MiB). A batch closes at `produce_batch_size` records or at this
    /// size, whichever comes first; it is further capped below the target
    /// topic's `max.message.bytes`. A single record larger than the batch
    /// cap is sent in a batch of its own.
    #[serde(
        default = "default_produce_max_batch_bytes",
        skip_serializing_if = "is_default_produce_max_batch_bytes"
    )]
    pub produce_max_batch_bytes: usize,

    /// Producer acknowledgement level sent to the broker.
    ///
    /// - `-1` (default): wait for all in-sync replicas — highest durability.
    /// - `1`: wait for the partition leader only — lower latency on clusters with
    ///   lagging replicas at the cost of reduced durability.
    /// - `0`: fire-and-forget — fastest but no delivery guarantee.
    ///
    /// Change from `-1` only when restore speed is critical and the cluster
    /// replication factor is known to be > 1 (Issue #67 bug 9).
    #[serde(default = "default_produce_acks")]
    pub produce_acks: i16,

    /// Produce with an idempotent producer (default `true`): one producer id
    /// per restore and a sequence number per partition, so a batch resent
    /// after a timeout or a lost connection is de-duplicated by the broker
    /// instead of written twice. Requires `produce_acks: -1`. When the
    /// cluster denies or does not support idempotence (Kafka before 2.5,
    /// no IdempotentWrite/Write permission), the restore logs a warning,
    /// records it in the report and produces without a producer id.
    #[serde(
        default = "default_idempotent_producer",
        skip_serializing_if = "is_default_idempotent_producer"
    )]
    pub idempotent_producer: bool,

    /// Broker-side produce timeout in milliseconds (default: 30 000).
    ///
    /// The broker waits up to this duration for the required acks before
    /// returning REQUEST_TIMED_OUT. The client-side socket timeout must stay
    /// above this value so slow replicated writes can return broker errors
    /// instead of being misclassified as transport failures.
    #[serde(default = "default_produce_timeout_ms")]
    pub produce_timeout_ms: i32,

    /// Checkpoint state file path for resumable restores (written
    /// atomically: temp file, fsync, rename).
    #[serde(default)]
    pub checkpoint_state: Option<std::path::PathBuf>,

    /// Seconds between checkpoint saves while a restore runs (the
    /// checkpoint is also saved after every topic).
    #[serde(default = "default_restore_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,

    /// Where the restore checkpoint is kept: `local` (default, the
    /// `checkpoint_state` file), `remote` (backup storage, at
    /// `{backup_id}/restore-checkpoints/{restore_id}.json`, so a restarted
    /// pod without the local file resumes) or `both`.
    #[serde(default, skip_serializing_if = "is_default")]
    pub checkpoint_storage: RestoreCheckpointStorage,

    /// Names the remote checkpoint. Default: derived from the backup id,
    /// the target bootstrap servers and the restore configuration, so a
    /// re-run of the same restore finds its checkpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restore_id: Option<String>,

    /// Consumer groups to reset offsets for (requires reset_consumer_offsets)
    #[serde(default)]
    pub consumer_groups: Vec<String>,

    /// Whether to reset consumer group offsets (dangerous, requires explicit opt-in)
    #[serde(default)]
    pub reset_consumer_offsets: bool,

    /// Output file for offset mapping report
    #[serde(default)]
    pub offset_report: Option<std::path::PathBuf>,

    /// Automatically create missing topics before restore
    /// Default: false (safe default - won't create topics unexpectedly)
    #[serde(default)]
    pub create_topics: bool,

    /// Replication factor for auto-created topics
    /// Default: None (use broker default, typically 1)
    /// Set to -1 to explicitly use broker default
    #[serde(default)]
    pub default_replication_factor: Option<i16>,

    /// Per-topic repartitioning configuration, keyed by TARGET topic name.
    /// When set, records are re-partitioned on the fly using the configured strategy
    /// instead of the default 1:1 partition mapping.
    /// Mutually exclusive with `partition_mapping`.
    #[serde(default, serialize_with = "serialize_sorted_map")]
    pub repartitioning: std::collections::HashMap<String, TopicRepartitioning>,

    /// Purge target topics before restoring by advancing each partition's
    /// log-start-offset to its current end-offset via the `DeleteRecords` API.
    ///
    /// This makes the topic appear empty without deleting it — required for
    /// Strimzi-managed topics which cannot be deleted and recreated via the
    /// standard Kafka admin API.
    ///
    /// **Irreversible.** Default: `false`.
    #[serde(default)]
    pub purge_topics: bool,

    /// Automatically load the consumer-groups snapshot written by the backup
    /// engine (or the `snapshot-groups` command) and use it to populate
    /// `consumer_groups` before starting the restore.
    ///
    /// The snapshot is read from `{backup_id}/consumer-groups-snapshot.json`
    /// in the configured storage backend. When the file is absent the option
    /// is silently ignored and the restore continues without offset reset.
    ///
    /// Setting this also enables `reset_consumer_offsets` automatically.
    /// Default: `false`.
    #[serde(default)]
    pub auto_consumer_groups: bool,

    /// Restore captured topic-level configuration overrides. Newly created
    /// topics always receive the captured overrides when this is enabled.
    #[serde(default = "default_restore_topic_configs")]
    pub restore_topic_configs: bool,

    /// How to handle captured configuration for target topics that existed
    /// before the restore started.
    #[serde(default)]
    pub existing_topic_config_policy: ExistingTopicConfigPolicy,

    /// Per-target-topic configuration overrides. These take precedence over
    /// values captured from the source manifest.
    #[serde(default, serialize_with = "serialize_sorted_map")]
    pub topic_config_overrides:
        std::collections::HashMap<String, std::collections::BTreeMap<String, String>>,

    /// Topic configuration (captured or overridden) the target does not take
    /// as is: keys its DescribeConfigs does not report or reports read-only,
    /// keys it rejects (each probed on its own with `validate_only`), and
    /// values outside the `target_profile` limits. `skip` (default) restores
    /// the rest and lists them in the restore report; `fail` stops the
    /// restore before any configuration is changed.
    #[serde(default, skip_serializing_if = "is_default")]
    pub unsupported_config_policy: UnsupportedConfigPolicy,

    /// Known limits of the target platform: `confluent_cloud` creates topics
    /// with replication factor 3 (unless `default_replication_factor` is set)
    /// and clamps (or, with `unsupported_config_policy: fail`, rejects)
    /// values outside Confluent Cloud's documented ranges. Which keys the
    /// target accepts is always decided by probing it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_profile: Option<TargetProfile>,

    /// Confluent Cloud cluster type, for the limits that depend on it
    /// (`max.message.bytes`, `max.compaction.lag.ms`). Unset: the most
    /// permissive limit of any cluster type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_cluster_type: Option<TargetClusterType>,

    /// Rewrite Confluent wire-format schema IDs in record keys and values.
    /// This is populated by the enterprise Schema Registry restore before the
    /// Kafka data phase; it is inert unless explicitly enabled.
    #[serde(default)]
    pub rewrite_schema_ids: bool,

    /// Source-to-target Schema Registry ID mapping used by
    /// `rewrite_schema_ids`. Map keys are the IDs encoded after the Confluent
    /// magic byte and values are the IDs registered in the target registry.
    #[serde(default, serialize_with = "serialize_sorted_map")]
    pub schema_id_mapping: std::collections::HashMap<i32, i32>,

    /// Source topics whose records `rewrite_schema_ids` applies to (glob,
    /// or regex with a `~` prefix, as in topic selection). Empty (the
    /// default): every restored topic.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub schema_id_rewrite_topics: Vec<String>,

    /// Fail the restore on a key or value that carries the Confluent magic
    /// byte and a schema ID absent from `schema_id_mapping`, instead of
    /// producing it with the source registry's ID. A raw (not Schema
    /// Registry framed) payload that starts with a zero byte looks the same,
    /// so enable this only for topics whose keys and values are all framed
    /// or never start with a zero byte.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub schema_id_rewrite_strict: bool,

    /// Phase 1 header preflight behaviour.
    ///
    /// - `auto` (default): scan the backup for tracking-header coverage when
    ///   consumer-offset recovery is requested, and fail before any target
    ///   mutation if required metadata is missing.
    /// - `full`: always scan, even when offset recovery is not requested
    ///   (findings are warnings in that case).
    /// - `skip`: never scan. Explicit escape hatch; an offset-recovery
    ///   request proceeds UNVERIFIED with a loud warning.
    #[serde(default)]
    pub header_preflight: HeaderPreflightMode,

    /// Set by orchestrators (e.g. three-phase restore) that already ran the
    /// header preflight, so the restore engine does not scan the backup a
    /// second time. Not configurable from YAML.
    #[serde(skip)]
    pub header_preflight_external: bool,

    /// During `validate-restore` / dry-run, HEAD every segment in the
    /// selected time window instead of only the oldest per partition (the
    /// default canary, which catches lifecycle-rule expiry at
    /// one-request-per-partition cost). Full sweeps can take a long time on
    /// large archives. Default: `false`.
    #[serde(default)]
    pub dry_run_check_segments: bool,

    /// Programmatic per-record filter (Keep / Drop / Tombstone), consulted on
    /// every restore path after time-window filtering. Set by code — e.g. an
    /// embedding application or a commercial distribution — never from YAML.
    /// See [`crate::restore::filter`].
    #[serde(skip)]
    pub record_filter: Option<crate::restore::filter::RecordFilterHandle>,

    /// Opaque fingerprint of the configured record filter (e.g. a digest of
    /// its rule set), folded into the restore checkpoint's `config_hash` so a
    /// resumed restore notices when the filter changed. Set by whoever sets
    /// `record_filter`; not configurable from YAML.
    #[serde(skip)]
    pub record_filter_fingerprint: Option<String>,

    /// Kafka circuit-breaker settings (advisory — never blocks requests; see
    /// [`CircuitBreakerSettings`]). Default matches the previous hardcoded
    /// thresholds (failure=5, reset=30s, success=2).
    #[serde(default)]
    pub circuit_breaker: CircuitBreakerSettings,
}

/// Controls the Phase 1 header preflight scan (see issue #137).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HeaderPreflightMode {
    /// Scan when consumer-offset recovery is requested; strict failure.
    #[default]
    Auto,
    /// Always scan; strict only when offset recovery is requested.
    Full,
    /// Never scan (explicit operator override; loud warning).
    Skip,
}

impl std::fmt::Display for HeaderPreflightMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Auto => "auto",
            Self::Full => "full",
            Self::Skip => "skip",
        })
    }
}

fn default_restore_topic_configs() -> bool {
    true
}

/// Hand-written `Default` so that `RestoreOptions::default()` in Rust code
/// matches the serde defaults used when deserialising from YAML — the derived
/// `Default` would give `0` for all numeric fields, which breaks callers that
/// do `RestoreOptions { field: value, ..RestoreOptions::default() }`.
impl Default for RestoreOptions {
    fn default() -> Self {
        Self {
            time_window_start: None,
            time_window_end: None,
            source_partitions: None,
            partition_mapping: Default::default(),
            topic_mapping: Default::default(),
            consumer_group_strategy: OffsetStrategy::default(),
            dry_run: false,
            include_original_offset_header: false,
            strip_offset_headers: false,
            rate_limit_records_per_sec: None,
            rate_limit_bytes_per_sec: None,
            max_concurrent_partitions: default_max_concurrent_partitions(),
            produce_batch_size: default_produce_batch_size(),
            produce_max_batch_bytes: default_produce_max_batch_bytes(),
            produce_acks: default_produce_acks(),
            idempotent_producer: default_idempotent_producer(),
            produce_timeout_ms: default_produce_timeout_ms(),
            checkpoint_state: None,
            checkpoint_interval_secs: default_restore_checkpoint_interval_secs(),
            checkpoint_storage: RestoreCheckpointStorage::default(),
            restore_id: None,
            consumer_groups: Vec::new(),
            reset_consumer_offsets: false,
            offset_report: None,
            create_topics: false,
            default_replication_factor: None,
            repartitioning: Default::default(),
            purge_topics: false,
            auto_consumer_groups: false,
            restore_topic_configs: default_restore_topic_configs(),
            existing_topic_config_policy: ExistingTopicConfigPolicy::default(),
            topic_config_overrides: Default::default(),
            unsupported_config_policy: UnsupportedConfigPolicy::default(),
            target_profile: None,
            target_cluster_type: None,
            rewrite_schema_ids: false,
            schema_id_mapping: Default::default(),
            schema_id_rewrite_topics: Vec::new(),
            schema_id_rewrite_strict: false,
            header_preflight: HeaderPreflightMode::default(),
            header_preflight_external: false,
            dry_run_check_segments: false,
            record_filter: None,
            record_filter_fingerprint: None,
            circuit_breaker: CircuitBreakerSettings::default(),
        }
    }
}

fn default_max_concurrent_partitions() -> usize {
    4
}

fn default_produce_batch_size() -> usize {
    1000
}

fn default_produce_max_batch_bytes() -> usize {
    1024 * 1024
}

fn default_idempotent_producer() -> bool {
    true
}

// Options added in 0.24 are left out of the serialized options at their
// defaults: the restore checkpoint's config hash is taken over them, and a
// checkpoint written by an earlier release must keep its hash.
fn is_default_produce_max_batch_bytes(value: &usize) -> bool {
    *value == default_produce_max_batch_bytes()
}

fn is_default_idempotent_producer(value: &bool) -> bool {
    *value == default_idempotent_producer()
}

fn default_produce_acks() -> i16 {
    -1 // acks=all — highest durability (safe default)
}

fn default_produce_timeout_ms() -> i32 {
    30_000 // 30 seconds, matching the previous hardcoded value
}

/// Public accessor for integration tests that assert the default acks value.
pub fn default_produce_acks_pub() -> i16 {
    default_produce_acks()
}

/// YAML keys `SecurityConfig` accepts, derived from its serialized form so
/// the list cannot drift from the struct (`sasl_mechanism_plugin_factory`
/// is `#[serde(skip)]` and so is not one of them).
fn security_config_keys() -> Vec<String> {
    serde_yaml::to_value(SecurityConfig::default())
        .ok()
        .and_then(|v| {
            v.as_mapping().map(|m| {
                m.keys()
                    .filter_map(|k| k.as_str().map(str::to_string))
                    .collect()
            })
        })
        .unwrap_or_default()
}

/// Public accessor for integration tests that assert the default timeout value.
pub fn default_produce_timeout_ms_pub() -> i32 {
    default_produce_timeout_ms()
}

fn default_restore_checkpoint_interval_secs() -> u64 {
    60
}

impl Config {
    /// Parse a YAML config, collecting the paths of any keys that were
    /// ignored during deserialization.
    ///
    /// The config structs do not reject unknown fields, so a typo like
    /// `fetch_max_bytez` would otherwise be silently dropped and the
    /// operator-provided or hand-written option would simply not apply.
    /// Callers should surface the returned paths as warnings.
    ///
    /// Unknown keys under `source.security` / `target.security` are an
    /// error rather than a warning (see [`Self::check_security_sections`]).
    pub fn from_yaml_with_warnings(yaml: &str) -> crate::Result<(Self, Vec<String>)> {
        let mut ignored = Vec::new();
        let deserializer = serde_yaml::Deserializer::from_str(yaml);
        let config: Config =
            serde_ignored::deserialize(deserializer, |path| ignored.push(path.to_string()))
                .map_err(|e| crate::Error::Config(format!("Failed to parse config: {e}")))?;

        let mut raw: serde_yaml::Value = serde_yaml::from_str(yaml)
            .map_err(|e| crate::Error::Config(format!("Failed to parse config: {e}")))?;
        // Resolve `<<: *anchor` merge keys so they are not read as keys.
        let _ = raw.apply_merge();
        Self::check_security_sections(&raw)?;

        let ignored = ignored
            .into_iter()
            // serde_ignored renders Option/enum layers as `?` segments
            // (e.g. `backup.?.key`); drop them for readable key paths.
            .map(|path| {
                path.split('.')
                    .filter(|segment| *segment != "?")
                    .collect::<Vec<_>>()
                    .join(".")
            })
            // The documented `logging` section is applied by the CLI layer
            // (RUST_LOG / -v), not deserialized into this struct — not a typo.
            .filter(|path| path != "logging" && !path.starts_with("logging."))
            .collect();

        Ok((config, ignored))
    }

    /// Reject unknown keys under `source.security` and `target.security` in
    /// a raw YAML config.
    ///
    /// Elsewhere an unknown key is only a warning, but a security block
    /// with a misplaced or misspelt setting connects with different
    /// credentials than the operator intended (or none), so it is an error.
    /// OAuth-looking keys (for example `security.oauth`) get a hint that
    /// those settings belong under `enterprise.oauth`. Every unknown key is
    /// reported at once. Values of the wrong type are left to
    /// deserialization to report.
    pub fn check_security_sections(root: &serde_yaml::Value) -> crate::Result<()> {
        let known = security_config_keys();
        let mut problems = Vec::new();
        for section in ["source", "target"] {
            let Some(map) = root
                .get(section)
                .and_then(|s| s.get("security"))
                .and_then(|s| s.as_mapping())
            else {
                continue;
            };
            for key in map.keys() {
                let name = match key.as_str() {
                    Some(name) => name.to_string(),
                    None => serde_yaml::to_string(key)
                        .unwrap_or_default()
                        .trim()
                        .to_string(),
                };
                if known.contains(&name) {
                    continue;
                }
                let hint = if name.to_ascii_lowercase().contains("oauth") {
                    OAUTH_SETTINGS_HINT.to_string()
                } else {
                    format!("valid keys are {}", known.join(", "))
                };
                problems.push(format!("unknown key `{section}.security.{name}`: {hint}"));
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(crate::Error::Config(problems.join("; ")))
        }
    }

    /// Keys that are accepted for backwards compatibility but have no effect
    /// (issue #161). Pure so it can be unit-tested; `validate()` logs each one.
    pub fn deprecation_warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if let Some(os) = &self.offset_storage {
            if os.s3_key.is_some() {
                warnings.push(
                    "offset_storage.s3_key is ignored (deprecated since 0.22.0): the offset \
                     database is always stored at {backup_id}/offsets.db under the storage prefix"
                        .to_string(),
                );
            }
            if os.backend == OffsetStorageBackend::Memory {
                warnings.push(
                    "offset_storage.backend: memory is not implemented (deprecated since 0.22.0); \
                     sqlite is used"
                        .to_string(),
                );
            }
        }
        if let Some(backup) = &self.backup {
            if backup.checkpoint_interval_secs != default_checkpoint_interval_secs() {
                warnings.push(
                    "backup.checkpoint_interval_secs has no effect (deprecated since 0.22.0): \
                     offsets are checkpointed at the end of every backup cycle"
                        .to_string(),
                );
            }
        }
        warnings
    }

    /// Validate the configuration
    pub fn validate(&self) -> crate::Result<()> {
        for warning in self.deprecation_warnings() {
            warn!("{}", warning);
        }

        match self.mode {
            Mode::Backup => {
                if self.source.is_none() {
                    return Err(crate::Error::Config(
                        "Source configuration is required for backup mode".to_string(),
                    ));
                }

                // Validate backup-specific options
                if let Some(backup) = &self.backup {
                    backup.validate()?;
                }
            }
            Mode::Restore => {
                if self.target.is_none() {
                    return Err(crate::Error::Config(
                        "Target configuration is required for restore mode".to_string(),
                    ));
                }

                // Validate restore-specific options
                if let Some(restore) = &self.restore {
                    restore.validate()?;
                }
            }
        }

        // Storage config validation is handled by StorageBackendConfig's typed enum
        // Required fields are non-Optional in each variant, so invalid configs
        // fail at deserialization time rather than runtime validation

        Ok(())
    }
}

impl BackupOptions {
    /// Validate backup options
    pub fn validate(&self) -> crate::Result<()> {
        // stop_at_current_offsets is incompatible with continuous mode
        if self.stop_at_current_offsets && self.continuous {
            return Err(crate::Error::Config(
                "stop_at_current_offsets cannot be used with continuous mode. \
                 Use stop_at_current_offsets for snapshot backups (catch up and exit), \
                 or continuous for streaming replication (run forever)."
                    .to_string(),
            ));
        }

        // Validate compression level
        if self.compression_level < 1 || self.compression_level > 22 {
            return Err(crate::Error::Config(format!(
                "compression_level must be between 1 and 22, got {}",
                self.compression_level
            )));
        }

        // Validate max_concurrent_partitions
        if self.max_concurrent_partitions == 0 {
            return Err(crate::Error::Config(
                "max_concurrent_partitions must be > 0".to_string(),
            ));
        }

        if let Some(retention) = &self.retention {
            retention.validate()?;
        }

        self.circuit_breaker.validate()?;

        Ok(())
    }
}

impl RestoreOptions {
    /// Validate restore options
    pub fn validate(&self) -> crate::Result<()> {
        // Check time window sanity
        if let (Some(from), Some(to)) = (self.time_window_start, self.time_window_end) {
            if from > to {
                return Err(crate::Error::Config(format!(
                    "time_window_start ({}) > time_window_end ({})",
                    from, to
                )));
            }
        }

        // Validate partition mapping
        for (src, dst) in &self.partition_mapping {
            if *src < 0 || *dst < 0 {
                return Err(crate::Error::Config(format!(
                    "Invalid partition number in mapping: {} -> {}",
                    src, dst
                )));
            }
        }

        // Validate source partitions
        if let Some(partitions) = &self.source_partitions {
            for p in partitions {
                if *p < 0 {
                    return Err(crate::Error::Config(format!(
                        "Invalid source partition number: {}",
                        p
                    )));
                }
            }
        }

        // Validate rate limits
        if let Some(rate) = self.rate_limit_records_per_sec {
            if rate == 0 {
                return Err(crate::Error::Config(
                    "rate_limit_records_per_sec must be > 0".to_string(),
                ));
            }
        }

        if let Some(rate) = self.rate_limit_bytes_per_sec {
            if rate == 0 {
                return Err(crate::Error::Config(
                    "rate_limit_bytes_per_sec must be > 0".to_string(),
                ));
            }
        }

        // Validate concurrent partitions
        if self.max_concurrent_partitions == 0 {
            return Err(crate::Error::Config(
                "max_concurrent_partitions must be > 0".to_string(),
            ));
        }

        // Validate batch size
        if self.produce_batch_size == 0 {
            return Err(crate::Error::Config(
                "produce_batch_size must be > 0".to_string(),
            ));
        }
        if self.idempotent_producer && self.produce_acks != -1 {
            return Err(crate::Error::Config(format!(
                "restore.idempotent_producer requires produce_acks: -1 (got {}). Set \
                 produce_acks: -1, or idempotent_producer: false to produce with acks {} \
                 without duplicate protection",
                self.produce_acks, self.produce_acks
            )));
        }
        if self.checkpoint_storage == RestoreCheckpointStorage::Both
            && self.checkpoint_state.is_none()
        {
            return Err(crate::Error::Config(
                "restore.checkpoint_storage: both requires restore.checkpoint_state (the local \
                 file)"
                    .to_string(),
            ));
        }
        if self.checkpoint_interval_secs == 0 {
            return Err(crate::Error::Config(
                "restore.checkpoint_interval_secs must be > 0".to_string(),
            ));
        }
        if let Some(id) = &self.restore_id {
            if id.is_empty()
                || !id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
            {
                return Err(crate::Error::Config(format!(
                    "restore.restore_id may only contain letters, digits, '-', '_' and '.', got \
                     {id:?}"
                )));
            }
        }
        if self.produce_max_batch_bytes < 1024 {
            return Err(crate::Error::Config(format!(
                "produce_max_batch_bytes must be at least 1024, got {}",
                self.produce_max_batch_bytes
            )));
        }

        if self.target_cluster_type.is_some()
            && self.target_profile != Some(TargetProfile::ConfluentCloud)
        {
            return Err(crate::Error::Config(
                "target_cluster_type needs target_profile: confluent_cloud".to_string(),
            ));
        }

        // Validate consumer group offset reset
        if self.reset_consumer_offsets
            && self.consumer_groups.is_empty()
            && !self.auto_consumer_groups
        {
            return Err(crate::Error::Config(
                "consumer_groups must be specified when reset_consumer_offsets is true".to_string(),
            ));
        }

        if !self.rewrite_schema_ids
            && (self.schema_id_rewrite_strict || !self.schema_id_rewrite_topics.is_empty())
        {
            return Err(crate::Error::Config(
                "schema_id_rewrite_topics and schema_id_rewrite_strict need \
                 rewrite_schema_ids: true"
                    .to_string(),
            ));
        }

        // Validate repartitioning
        for (topic, repart) in &self.repartitioning {
            if repart.target_partitions <= 0 {
                return Err(crate::Error::Config(format!(
                    "repartitioning target_partitions must be > 0 for topic '{}', got {}",
                    topic, repart.target_partitions
                )));
            }
        }

        // Note: partition_mapping and repartitioning can coexist — repartitioning
        // takes precedence for topics that have it configured, while partition_mapping
        // applies to the remaining topics. The engine handles this via early-return
        // branching in restore_topic().

        self.circuit_breaker.validate()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backup_options_parse_fetch_max_bytes_and_segment_max_records() {
        let yaml = "fetch_max_bytes: 16777216\nsegment_max_records: 2000000\n";
        let opts: BackupOptions = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(opts.fetch_max_bytes, Some(16777216));
        assert_eq!(opts.segment_max_records, Some(2_000_000));

        let defaults = BackupOptions::default();
        assert_eq!(defaults.fetch_max_bytes, None);
        assert_eq!(defaults.segment_max_records, None);
    }

    #[test]
    fn pre_v0_16_config_parses_unchanged_with_no_warnings() {
        // A config written for older versions (no v0.16.0 keys) must parse
        // with identical semantics and produce zero warnings.
        let yaml = r#"
mode: backup
backup_id: compat-test
source:
  bootstrap_servers:
    - localhost:9092
storage:
  backend: filesystem
  path: /tmp/compat-test
backup:
  compression: zstd
  segment_max_bytes: 134217728
  stop_at_current_offsets: true
"#;
        let (config, warnings) = Config::from_yaml_with_warnings(yaml).unwrap();
        assert!(
            warnings.is_empty(),
            "expected no warnings, got {warnings:?}"
        );
        let opts = config.backup.unwrap();
        assert_eq!(opts.fetch_max_bytes, None);
        assert_eq!(opts.segment_max_records, None);
        assert_eq!(opts.segment_max_bytes, 134217728);
        assert!(opts.stop_at_current_offsets);
    }

    #[test]
    fn config_from_yaml_with_warnings_reports_unknown_keys() {
        let yaml = r#"
mode: backup
backup_id: warn-test
source:
  bootstrap_servers:
    - localhost:9092
storage:
  backend: filesystem
  path: /tmp/warn-test
backup:
  segment_max_bytes: 1048576
  fetch_max_bytez: 123
not_a_real_section:
  foo: 1
"#;
        let (config, warnings) = Config::from_yaml_with_warnings(yaml).unwrap();
        assert_eq!(config.backup_id, "warn-test");
        assert!(
            warnings.iter().any(|w| w == "backup.fetch_max_bytez"),
            "expected a clean `backup.fetch_max_bytez` path (no `?` segments), got {warnings:?}"
        );
        assert!(
            warnings.iter().any(|w| w.contains("not_a_real_section")),
            "expected a warning for not_a_real_section, got {warnings:?}"
        );
    }

    #[test]
    fn config_from_yaml_with_warnings_is_quiet_for_known_keys() {
        let yaml = r#"
mode: backup
backup_id: quiet-test
source:
  bootstrap_servers:
    - localhost:9092
storage:
  backend: filesystem
  path: /tmp/quiet-test
backup:
  segment_max_bytes: 1048576
  fetch_max_bytes: 16777216
  segment_max_records: 2000000
logging:
  level: debug
  format: text
"#;
        let (_, warnings) = Config::from_yaml_with_warnings(yaml).unwrap();
        assert!(
            warnings.is_empty(),
            "expected no warnings, got {warnings:?}"
        );
    }

    /// Issue #154: the backup-side default is `true` and was undocumented.
    /// This pins the contract so a change to it is a deliberate one.
    #[test]
    fn backup_include_offset_headers_defaults_to_true() {
        assert!(BackupOptions::default().include_offset_headers);
        let yaml = r#"
mode: backup
backup_id: defaults
source:
  bootstrap_servers: [localhost:9092]
storage:
  backend: filesystem
  path: /tmp/defaults
backup:
  compression: zstd
"#;
        let (config, _) = Config::from_yaml_with_warnings(yaml).unwrap();
        assert!(config.backup.unwrap().include_offset_headers);
    }

    #[test]
    fn restore_strip_offset_headers_defaults_to_false_and_parses() {
        assert!(!RestoreOptions::default().strip_offset_headers);
        let yaml = r#"
mode: restore
backup_id: strip
target:
  bootstrap_servers: [localhost:9092]
storage:
  backend: filesystem
  path: /tmp/strip
restore:
  strip_offset_headers: true
  consumer_group_strategy: skip
"#;
        let (config, warnings) = Config::from_yaml_with_warnings(yaml).unwrap();
        assert!(warnings.is_empty(), "{warnings:?}");
        let restore = config.restore.unwrap();
        assert!(restore.strip_offset_headers);
        assert!(!restore.include_original_offset_header);
        assert_eq!(restore.consumer_group_strategy, OffsetStrategy::Skip);
    }

    #[test]
    fn test_connection_config_defaults() {
        let config = ConnectionConfig::default();
        assert!(config.tcp_keepalive);
        assert_eq!(config.keepalive_time_secs, 60);
        assert_eq!(config.keepalive_interval_secs, 20);
        assert!(config.tcp_nodelay);
    }

    #[test]
    fn test_kafka_config_connection_defaults() {
        // Test that KafkaConfig uses ConnectionConfig::default() when not specified
        let yaml = r#"
bootstrap_servers:
  - "localhost:9092"
"#;
        let config: KafkaConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.connection.tcp_keepalive);
        assert_eq!(config.connection.keepalive_time_secs, 60);
        assert_eq!(config.connection.keepalive_interval_secs, 20);
        assert!(config.connection.tcp_nodelay);
    }

    #[test]
    fn test_metrics_config_defaults() {
        let config = MetricsConfig::default();
        assert_eq!(config.keep_alive_seconds, 0);
    }

    #[test]
    fn test_metrics_config_keep_alive_yaml() {
        let yaml = r#"
enabled: true
port: 8080
bind_address: "0.0.0.0"
path: "/metrics"
keep_alive_seconds: 90
"#;
        let config: MetricsConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.keep_alive_seconds, 90);
    }

    #[test]
    fn test_security_config_serde_skips_plugin_factory_field() {
        // `sasl_mechanism_plugin_factory` is `#[serde(skip)]` —
        // programmatic only. Regressions would leak a non-serializable
        // trait object into YAML and break config round-tripping. This
        // test pins it.
        use std::sync::Arc;

        #[derive(Debug)]
        struct NoopPlugin;
        #[async_trait::async_trait]
        impl crate::kafka::SaslMechanismPlugin for NoopPlugin {
            fn mechanism_name(&self) -> &str {
                "NOOP"
            }
            async fn initial_payload(&self) -> Result<Vec<u8>, crate::kafka::SaslPluginError> {
                Ok(vec![])
            }
        }

        let plugin: crate::kafka::SaslMechanismPluginHandle = Arc::new(NoopPlugin);
        let factory = crate::kafka::SharedPluginFactory::new(plugin).into_handle();

        let cfg = SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism_plugin_factory: Some(factory),
            ..Default::default()
        };
        let yaml = serde_yaml::to_string(&cfg).expect("serialize security config");
        assert!(
            !yaml.contains("sasl_mechanism_plugin_factory"),
            "serde(skip) must keep the factory field out of YAML; got:\n{yaml}"
        );

        // Round-trip: deserialization into a config without the field
        // yields `None` — we never try to rehydrate a trait object.
        let back: SecurityConfig = serde_yaml::from_str(&yaml).expect("deserialize");
        assert!(back.sasl_mechanism_plugin_factory.is_none());
    }

    #[test]
    fn test_security_config_gssapi_serde_roundtrip() {
        // Pins the YAML spelling `GSSAPI` (SCREAMING-KEBAB-CASE) and that
        // all three Kerberos path fields deserialize into Options.
        let yaml = r#"
security_protocol: SASL_PLAINTEXT
sasl_mechanism: GSSAPI
sasl_kerberos_service_name: kafka
sasl_keytab_path: /etc/kafka/client.keytab
sasl_krb5_config_path: /etc/krb5.conf
"#;
        let cfg: SecurityConfig = serde_yaml::from_str(yaml).expect("deserialize GSSAPI config");
        assert_eq!(cfg.security_protocol, SecurityProtocol::SaslPlaintext);
        assert_eq!(cfg.sasl_mechanism, Some(SaslMechanism::Gssapi));
        assert_eq!(cfg.sasl_kerberos_service_name.as_deref(), Some("kafka"));
        assert_eq!(
            cfg.sasl_keytab_path.as_deref(),
            Some(std::path::Path::new("/etc/kafka/client.keytab"))
        );
        assert_eq!(
            cfg.sasl_krb5_config_path.as_deref(),
            Some(std::path::Path::new("/etc/krb5.conf"))
        );

        // Round-trip back to YAML — variant must spell as `GSSAPI`, not
        // `Gssapi` or `GSS-API`.
        let back = serde_yaml::to_string(&cfg).expect("serialize");
        assert!(
            back.contains("sasl_mechanism: GSSAPI"),
            "expected GSSAPI spelling in:\n{back}"
        );
    }

    #[test]
    fn test_security_config_oauthbearer_serde_roundtrip() {
        let yaml = r#"
security_protocol: SASL_SSL
sasl_mechanism: OAUTHBEARER
"#;
        let cfg: SecurityConfig = serde_yaml::from_str(yaml).expect("deserialize OAUTHBEARER");
        assert_eq!(cfg.sasl_mechanism, Some(SaslMechanism::OAuthBearer));
        let back = serde_yaml::to_string(&cfg).expect("serialize");
        assert!(
            back.contains("sasl_mechanism: OAUTHBEARER"),
            "expected OAUTHBEARER spelling in:\n{back}"
        );
        // The automatic SCREAMING-KEBAB-CASE spelling must not be accepted.
        assert!(serde_yaml::from_str::<SaslMechanism>("O-AUTH-BEARER").is_err());
    }

    #[test]
    fn test_sasl_mechanism_accepts_kafka_canonical_and_legacy_scram_spellings() {
        // Kafka's own names (`sasl.mechanism=SCRAM-SHA-512`, used throughout
        // the docs) and the spellings this crate has always accepted.
        for (spelling, expected) in [
            ("SCRAM-SHA-256", SaslMechanism::ScramSha256),
            ("SCRAM-SHA256", SaslMechanism::ScramSha256),
            ("SCRAM-SHA-512", SaslMechanism::ScramSha512),
            ("SCRAM-SHA512", SaslMechanism::ScramSha512),
            ("PLAIN", SaslMechanism::Plain),
            ("GSSAPI", SaslMechanism::Gssapi),
            ("OAUTHBEARER", SaslMechanism::OAuthBearer),
        ] {
            let parsed: SaslMechanism = serde_yaml::from_str(spelling)
                .unwrap_or_else(|e| panic!("{spelling} must parse: {e}"));
            assert_eq!(parsed, expected, "{spelling}");

            let yaml = format!(
                "security_protocol: SASL_SSL\nsasl_mechanism: {spelling}\n\
                 sasl_username: u\nsasl_password: p\n"
            );
            let cfg: SecurityConfig = serde_yaml::from_str(&yaml)
                .unwrap_or_else(|e| panic!("security block with {spelling}: {e}"));
            assert_eq!(cfg.sasl_mechanism, Some(expected), "{spelling}");
        }
        // Not a mechanism name in either spelling.
        assert!(serde_yaml::from_str::<SaslMechanism>("SCRAM-SHA-1").is_err());
        assert!(serde_yaml::from_str::<SaslMechanism>("SCRAM").is_err());
    }

    #[test]
    fn test_sasl_mechanism_serialized_spelling_is_unchanged() {
        // Serialisation keeps the spelling earlier releases wrote and read,
        // so a config written by this version still loads in older ones.
        for (mechanism, spelling) in [
            (SaslMechanism::ScramSha256, "SCRAM-SHA256"),
            (SaslMechanism::ScramSha512, "SCRAM-SHA512"),
        ] {
            let out = serde_yaml::to_string(&mechanism).expect("serialize");
            assert_eq!(out.trim(), spelling);
            let back: SaslMechanism = serde_yaml::from_str(&out).expect("roundtrip");
            assert_eq!(back, mechanism);
        }
    }

    #[test]
    fn test_full_config_with_canonical_scram_name_parses() {
        // The shape used in docs/configuration.md and the example configs.
        let yaml = r#"
mode: backup
backup_id: scram-canonical
source:
  bootstrap_servers: ["kafka:9092"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: user
    sasl_password: secret
storage:
  backend: filesystem
  path: /tmp/scram-canonical
"#;
        let (config, warnings) =
            Config::from_yaml_with_warnings(yaml).expect("canonical SCRAM name parses");
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(
            config.source.unwrap().security.sasl_mechanism,
            Some(SaslMechanism::ScramSha512)
        );
    }

    const SECURITY_LINT_BASE: &str = r#"
mode: backup
backup_id: lint
source:
  bootstrap_servers: ["localhost:9092"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
storage:
  backend: filesystem
  path: /tmp/lint
"#;

    #[test]
    fn security_lint_accepts_known_keys() {
        let yaml = r#"
mode: restore
backup_id: lint
source:
  bootstrap_servers: ["localhost:9092"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA512
    sasl_username: u
    sasl_password: p
    ssl_ca_location: /ca.pem
    ssl_certificate_location: /c.pem
    ssl_key_location: /k.pem
    sasl_kerberos_service_name: kafka
    sasl_keytab_path: /k.keytab
    sasl_krb5_config_path: /krb5.conf
target:
  bootstrap_servers: ["localhost:9092"]
storage:
  backend: filesystem
  path: /tmp/lint
enterprise:
  oauth:
    providers: {}
"#;
        let (config, warnings) = Config::from_yaml_with_warnings(yaml).expect("valid config");
        assert_eq!(
            config.source.unwrap().security.sasl_mechanism,
            Some(SaslMechanism::ScramSha512)
        );
        // `enterprise:` is not this crate's business: a warning, not an error.
        assert!(warnings.iter().any(|w| w.starts_with("enterprise")));
        Config::from_yaml_with_warnings(SECURITY_LINT_BASE).expect("OAUTHBEARER parses");
    }

    #[test]
    fn security_lint_rejects_oauth_settings_with_enterprise_hint() {
        let source_oauth = SECURITY_LINT_BASE.replace(
            "    sasl_mechanism: OAUTHBEARER\n",
            "    sasl_mechanism: OAUTHBEARER\n    oauth:\n      client_id: x\n",
        );
        let target_oauth = format!(
            "{SECURITY_LINT_BASE}target:\n  bootstrap_servers: [\"localhost:9092\"]\n  \
             security:\n    sasl_oauthbearer_token_endpoint_url: https://idp/token\n"
        );
        for (yaml, key) in [
            (source_oauth, "source.security.oauth"),
            (
                target_oauth,
                "target.security.sasl_oauthbearer_token_endpoint_url",
            ),
        ] {
            let err = Config::from_yaml_with_warnings(&yaml)
                .expect_err(&format!("{key} must be rejected"))
                .to_string();
            assert!(err.contains(key), "names the key: {err}");
            assert!(
                err.contains(
                    "OAuth settings belong under enterprise.oauth (Kafka Backup Enterprise)"
                ),
                "hint: {err}"
            );
        }
    }

    #[test]
    fn security_lint_rejects_typos_and_lists_valid_keys() {
        let yaml = SECURITY_LINT_BASE.replace(
            "    sasl_mechanism: OAUTHBEARER\n",
            "    sasl_mechanism: OAUTHBEARER\n    sasl_pasword: oops\n",
        );
        let err = Config::from_yaml_with_warnings(&yaml)
            .expect_err("typo under security must be rejected")
            .to_string();
        assert!(err.contains("source.security.sasl_pasword"), "{err}");
        assert!(err.contains("sasl_password"), "lists valid keys: {err}");
        assert!(!err.contains("enterprise.oauth"), "no OAuth hint: {err}");
    }

    #[test]
    fn security_lint_ignores_other_sections_and_missing_security() {
        // Unknown keys elsewhere stay warnings (existing behaviour).
        let yaml = r#"
mode: backup
backup_id: lint
source:
  bootstrap_servers: ["localhost:9092"]
  not_security: 1
storage:
  backend: filesystem
  path: /tmp/lint
"#;
        let (_, warnings) = Config::from_yaml_with_warnings(yaml).expect("no security block");
        assert!(warnings.iter().any(|w| w == "source.not_security"));
        assert!(Config::check_security_sections(&serde_yaml::Value::Null).is_ok());
    }

    #[test]
    fn test_security_config_gssapi_fields_default_to_none() {
        // A pre-GSSAPI config (no kerberos fields) must still parse, with
        // the new fields defaulting to None. Guards backwards-compat for
        // existing deployed configs.
        let yaml = r#"
security_protocol: SASL_SSL
sasl_mechanism: SCRAM-SHA512
sasl_username: alice
sasl_password: hunter2
"#;
        let cfg: SecurityConfig = serde_yaml::from_str(yaml).expect("deserialize legacy config");
        assert_eq!(cfg.sasl_mechanism, Some(SaslMechanism::ScramSha512));
        assert!(cfg.sasl_kerberos_service_name.is_none());
        assert!(cfg.sasl_keytab_path.is_none());
        assert!(cfg.sasl_krb5_config_path.is_none());
    }

    #[test]
    fn test_kafka_config_connection_custom() {
        // Test that custom connection settings are properly parsed
        let yaml = r#"
bootstrap_servers:
  - "localhost:9092"
connection:
  tcp_keepalive: false
  keepalive_time_secs: 30
  keepalive_interval_secs: 10
  tcp_nodelay: false
"#;
        let config: KafkaConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(!config.connection.tcp_keepalive);
        assert_eq!(config.connection.keepalive_time_secs, 30);
        assert_eq!(config.connection.keepalive_interval_secs, 10);
        assert!(!config.connection.tcp_nodelay);
    }

    /// Create a RestoreOptions with valid defaults suitable for testing.
    /// (Rust's derived Default gives 0 for numeric fields, which fails validation.)
    fn valid_restore_options() -> RestoreOptions {
        RestoreOptions {
            max_concurrent_partitions: default_max_concurrent_partitions(),
            produce_batch_size: default_produce_batch_size(),
            produce_max_batch_bytes: default_produce_max_batch_bytes(),
            produce_acks: default_produce_acks(),
            produce_timeout_ms: default_produce_timeout_ms(),
            checkpoint_interval_secs: default_restore_checkpoint_interval_secs(),
            ..RestoreOptions::default()
        }
    }

    #[test]
    fn schema_id_rewrite_options_parse_and_validate() {
        let yaml = r#"
rewrite_schema_ids: true
schema_id_mapping: {7: 42}
schema_id_rewrite_topics: ["orders-*", "~^payments-v\\d+$"]
schema_id_rewrite_strict: true
"#;
        let opts: RestoreOptions = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(opts.schema_id_rewrite_topics.len(), 2);
        assert!(opts.schema_id_rewrite_strict);
        opts.validate().unwrap();

        for tweak in [
            |o: &mut RestoreOptions| o.schema_id_rewrite_strict = true,
            |o: &mut RestoreOptions| o.schema_id_rewrite_topics = vec!["orders".into()],
        ] {
            let mut opts = RestoreOptions::default();
            tweak(&mut opts);
            let err = opts.validate().unwrap_err().to_string();
            assert!(err.contains("rewrite_schema_ids: true"), "{err}");
        }
    }

    #[test]
    fn schema_id_rewrite_defaults_do_not_change_the_serialized_options() {
        // The restore checkpoint's config hash is taken over the serialized
        // options: unset new fields must not change it for existing
        // checkpoints.
        let json = serde_json::to_string(&RestoreOptions::default()).unwrap();
        assert!(!json.contains("schema_id_rewrite_topics"), "{json}");
        assert!(!json.contains("schema_id_rewrite_strict"), "{json}");
    }

    /// The restore checkpoint's config hash is taken over the serialized
    /// options. Every option added in 0.24 is left out at its default, so the
    /// serialized defaults are exactly the 0.23.x keys, in the 0.23.x order,
    /// and a checkpoint written by 0.23.x is still resumed after an upgrade.
    /// A non-default value is serialized, so changing it starts over.
    #[test]
    fn options_added_in_0_24_are_not_serialized_at_their_defaults() {
        const KEYS_0_23: &[&str] = &[
            "time_window_start",
            "time_window_end",
            "source_partitions",
            "partition_mapping",
            "topic_mapping",
            "consumer_group_strategy",
            "dry_run",
            "include_original_offset_header",
            "strip_offset_headers",
            "rate_limit_records_per_sec",
            "rate_limit_bytes_per_sec",
            "max_concurrent_partitions",
            "produce_batch_size",
            "produce_acks",
            "produce_timeout_ms",
            "checkpoint_state",
            "checkpoint_interval_secs",
            "consumer_groups",
            "reset_consumer_offsets",
            "offset_report",
            "create_topics",
            "default_replication_factor",
            "repartitioning",
            "purge_topics",
            "auto_consumer_groups",
            "restore_topic_configs",
            "existing_topic_config_policy",
            "topic_config_overrides",
            "rewrite_schema_ids",
            "schema_id_mapping",
            "header_preflight",
            "dry_run_check_segments",
            "circuit_breaker",
        ];
        // Top-level keys in serialized order (a YAML mapping keeps it).
        let top_level_keys = |opts: &RestoreOptions| -> Vec<String> {
            serde_yaml::to_value(opts)
                .unwrap()
                .as_mapping()
                .unwrap()
                .keys()
                .map(|k| k.as_str().unwrap().to_string())
                .collect()
        };

        assert_eq!(top_level_keys(&RestoreOptions::default()), KEYS_0_23);
        // Deserialising the 0.23.x form yields the 0.24 defaults.
        let json = serde_json::to_string(&RestoreOptions::default()).unwrap();
        let back: RestoreOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), json);
        assert!(back.idempotent_producer);
        assert_eq!(
            back.produce_max_batch_bytes,
            default_produce_max_batch_bytes()
        );
        assert_eq!(back.checkpoint_storage, RestoreCheckpointStorage::Local);

        // Each 0.24 option, set away from its default, is serialized.
        type Tweak = fn(&mut RestoreOptions);
        let tweaks: Vec<(&str, Tweak)> = vec![
            ("produce_max_batch_bytes", |o| {
                o.produce_max_batch_bytes = 4096
            }),
            ("idempotent_producer", |o| o.idempotent_producer = false),
            ("checkpoint_storage", |o| {
                o.checkpoint_storage = RestoreCheckpointStorage::Remote
            }),
            ("restore_id", |o| o.restore_id = Some("orders-dr".into())),
            ("unsupported_config_policy", |o| {
                o.unsupported_config_policy = UnsupportedConfigPolicy::Fail
            }),
            ("target_profile", |o| {
                o.target_profile = Some(TargetProfile::ConfluentCloud)
            }),
            ("target_cluster_type", |o| {
                o.target_cluster_type = Some(TargetClusterType::Dedicated)
            }),
            ("schema_id_rewrite_topics", |o| {
                o.schema_id_rewrite_topics = vec!["orders".into()]
            }),
            ("schema_id_rewrite_strict", |o| {
                o.schema_id_rewrite_strict = true
            }),
        ];
        for (key, tweak) in tweaks {
            let mut opts = RestoreOptions::default();
            tweak(&mut opts);
            let keys = top_level_keys(&opts);
            assert!(
                keys.iter().any(|k| k == key),
                "{key} not serialized: {keys:?}"
            );
            assert_eq!(keys.len(), KEYS_0_23.len() + 1, "{key}: {keys:?}");
        }
    }

    #[test]
    fn idempotent_producer_is_on_by_default_and_requires_acks_all() {
        let opts = valid_restore_options();
        assert!(opts.idempotent_producer);
        assert_eq!(opts.produce_acks, -1);
        assert!(opts.validate().is_ok());

        for acks in [0, 1] {
            let mut opts = valid_restore_options();
            opts.produce_acks = acks;
            let err = opts.validate().unwrap_err().to_string();
            assert!(
                err.contains("idempotent_producer requires produce_acks: -1"),
                "{err}"
            );

            opts.idempotent_producer = false;
            assert!(opts.validate().is_ok(), "acks {acks} without idempotence");
        }

        let yaml = "produce_acks: 1\nidempotent_producer: false\n";
        let opts: RestoreOptions = serde_yaml::from_str(yaml).unwrap();
        assert!(!opts.idempotent_producer);
        assert!(opts.validate().is_ok());
        let opts: RestoreOptions = serde_yaml::from_str("{}").unwrap();
        assert!(opts.idempotent_producer);
        assert_eq!(opts.produce_max_batch_bytes, 1024 * 1024);
    }

    #[test]
    fn restore_checkpoint_storage_options_validate() {
        let opts: RestoreOptions =
            serde_yaml::from_str("checkpoint_storage: remote\nrestore_id: orders-dr.1\n").unwrap();
        assert_eq!(opts.checkpoint_storage, RestoreCheckpointStorage::Remote);
        assert!(opts.validate().is_ok(), "remote needs no local file");

        let opts: RestoreOptions = serde_yaml::from_str("{}").unwrap();
        assert_eq!(opts.checkpoint_storage, RestoreCheckpointStorage::Local);
        assert_eq!(opts.restore_id, None);

        let mut opts = valid_restore_options();
        opts.checkpoint_storage = RestoreCheckpointStorage::Both;
        let err = opts.validate().unwrap_err().to_string();
        assert!(err.contains("requires restore.checkpoint_state"), "{err}");
        opts.checkpoint_state = Some("/tmp/cp.json".into());
        assert!(opts.validate().is_ok());

        let mut opts = valid_restore_options();
        opts.restore_id = Some("../escape".to_string());
        assert!(opts.validate().is_err());
        opts.restore_id = Some(String::new());
        assert!(opts.validate().is_err());

        let mut opts = valid_restore_options();
        opts.checkpoint_interval_secs = 0;
        assert!(opts.validate().is_err());
    }

    #[test]
    fn produce_max_batch_bytes_must_hold_a_batch() {
        let mut opts = valid_restore_options();
        opts.produce_max_batch_bytes = 100;
        assert!(opts.validate().is_err());
        opts.produce_max_batch_bytes = 8 * 1024 * 1024;
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn test_repartitioning_and_partition_mapping_coexist() {
        // partition_mapping and repartitioning can coexist for different topics:
        // repartitioning takes precedence for its topics, partition_mapping for the rest.
        let mut opts = valid_restore_options();
        opts.partition_mapping.insert(0, 1);
        opts.repartitioning.insert(
            "my-topic".to_string(),
            TopicRepartitioning {
                strategy: RepartitioningStrategy::Murmur2,
                target_partitions: 3,
            },
        );
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn topic_config_compat_options_parse_and_validate() {
        let yaml = r#"
unsupported_config_policy: fail
target_profile: confluent_cloud
target_cluster_type: standard
"#;
        let opts: RestoreOptions = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            opts.unsupported_config_policy,
            UnsupportedConfigPolicy::Fail
        );
        assert_eq!(opts.target_profile, Some(TargetProfile::ConfluentCloud));
        assert_eq!(opts.target_cluster_type, Some(TargetClusterType::Standard));
        opts.validate().unwrap();

        let defaults = RestoreOptions::default();
        assert_eq!(
            defaults.unsupported_config_policy,
            UnsupportedConfigPolicy::Skip
        );
        assert_eq!(defaults.target_profile, None);

        let orphan = RestoreOptions {
            target_cluster_type: Some(TargetClusterType::Dedicated),
            ..Default::default()
        };
        let err = orphan.validate().unwrap_err().to_string();
        assert!(err.contains("target_profile: confluent_cloud"), "{err}");

        assert!(serde_yaml::from_str::<RestoreOptions>("target_profile: aws_msk").is_err());
        assert!(
            serde_yaml::from_str::<RestoreOptions>("unsupported_config_policy: ignore").is_err()
        );
    }

    #[test]
    fn topic_config_compat_defaults_do_not_change_the_serialized_options() {
        // The restore checkpoint's config hash is taken over the serialized
        // options: unset new fields must not change it.
        let json = serde_json::to_string(&RestoreOptions::default()).unwrap();
        for key in [
            "unsupported_config_policy",
            "target_profile",
            "target_cluster_type",
        ] {
            assert!(!json.contains(key), "{key} in {json}");
        }
    }

    #[test]
    fn test_repartitioning_invalid_target_partitions() {
        let mut opts = valid_restore_options();
        opts.repartitioning.insert(
            "my-topic".to_string(),
            TopicRepartitioning {
                strategy: RepartitioningStrategy::Murmur2,
                target_partitions: 0,
            },
        );
        assert!(opts.validate().is_err());

        let mut opts2 = valid_restore_options();
        opts2.repartitioning.insert(
            "my-topic".to_string(),
            TopicRepartitioning {
                strategy: RepartitioningStrategy::Murmur2,
                target_partitions: -1,
            },
        );
        assert!(opts2.validate().is_err());
    }

    #[test]
    fn test_repartitioning_yaml_round_trip() {
        let yaml = r#"
repartitioning:
  orders:
    strategy: murmur2
    target_partitions: 3
  events:
    strategy: automatic
    target_partitions: 6
"#;
        let opts: RestoreOptions = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(opts.repartitioning.len(), 2);
        assert_eq!(opts.repartitioning["orders"].target_partitions, 3);
        assert_eq!(
            opts.repartitioning["orders"].strategy,
            RepartitioningStrategy::Murmur2
        );
        assert_eq!(opts.repartitioning["events"].target_partitions, 6);
        assert_eq!(
            opts.repartitioning["events"].strategy,
            RepartitioningStrategy::Automatic
        );

        // Re-serialize and verify round-trip
        let re_serialized = serde_yaml::to_string(&opts).unwrap();
        let re_parsed: RestoreOptions = serde_yaml::from_str(&re_serialized).unwrap();
        assert_eq!(re_parsed.repartitioning.len(), 2);
        assert_eq!(re_parsed.repartitioning["orders"].target_partitions, 3);
    }

    #[test]
    fn test_repartitioning_valid_config() {
        let mut opts = valid_restore_options();
        opts.repartitioning.insert(
            "my-topic".to_string(),
            TopicRepartitioning {
                strategy: RepartitioningStrategy::Murmur2,
                target_partitions: 3,
            },
        );
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn deprecation_warnings_list_ignored_offset_keys() {
        let yaml = r#"
mode: backup
backup_id: dep
source:
  bootstrap_servers: ["localhost:9092"]
storage:
  backend: memory
backup:
  checkpoint_interval_secs: 30
offset_storage:
  backend: memory
  s3_key: custom/offsets.db
  sync_interval_secs: 60
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let warnings = config.deprecation_warnings();
        assert_eq!(warnings.len(), 3, "{warnings:?}");
        assert!(warnings.iter().any(|w| w.contains("offset_storage.s3_key")));
        assert!(warnings
            .iter()
            .any(|w| w.contains("offset_storage.backend")));
        assert!(warnings
            .iter()
            .any(|w| w.contains("backup.checkpoint_interval_secs")));
        assert_eq!(
            config.offset_storage.as_ref().unwrap().sync_interval_secs,
            Some(60)
        );
    }

    #[test]
    fn deprecation_warnings_empty_for_defaults() {
        let yaml = r#"
mode: backup
backup_id: clean
source:
  bootstrap_servers: ["localhost:9092"]
storage:
  backend: memory
offset_storage:
  db_path: /tmp/offsets.db
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(config.deprecation_warnings().is_empty());
        assert_eq!(
            config.offset_storage.as_ref().unwrap().sync_interval_secs,
            None,
            "unset means: use backup.sync_interval_secs"
        );
    }

    #[test]
    fn on_missing_topic_parses_and_defaults_to_fail() {
        let base = r#"
mode: backup
backup_id: omt
source:
  bootstrap_servers: ["localhost:9092"]
storage:
  backend: memory
"#;
        let config: Config = serde_yaml::from_str(base).unwrap();
        assert_eq!(
            config.backup.unwrap_or_default().on_missing_topic,
            OnMissingTopic::Fail
        );
        let warn = format!("{base}backup:\n  on_missing_topic: warn\n");
        let config: Config = serde_yaml::from_str(&warn).unwrap();
        assert_eq!(
            config.backup.unwrap().on_missing_topic,
            OnMissingTopic::Warn
        );
        let bad = format!("{base}backup:\n  on_missing_topic: ignore\n");
        assert!(serde_yaml::from_str::<Config>(&bad).is_err());
    }

    #[test]
    fn isolation_level_defaults_to_read_committed() {
        let base = r#"
mode: backup
backup_id: iso
source:
  bootstrap_servers: ["localhost:9092"]
storage:
  backend: memory
"#;
        // No backup section, and a backup section without the key.
        let config: Config = serde_yaml::from_str(base).unwrap();
        assert_eq!(
            config.backup.unwrap_or_default().isolation_level,
            IsolationLevel::ReadCommitted
        );
        let other = format!("{base}backup:\n  compression: lz4\n");
        let config: Config = serde_yaml::from_str(&other).unwrap();
        assert_eq!(
            config.backup.unwrap().isolation_level,
            IsolationLevel::ReadCommitted
        );
        assert_eq!(IsolationLevel::ReadCommitted.as_protocol(), 1);
        assert_eq!(IsolationLevel::ReadUncommitted.as_protocol(), 0);
    }

    #[test]
    fn isolation_level_read_uncommitted_is_the_opt_out() {
        let base = r#"
mode: backup
backup_id: iso
source:
  bootstrap_servers: ["localhost:9092"]
storage:
  backend: memory
"#;
        for (value, expected) in [
            ("read_uncommitted", IsolationLevel::ReadUncommitted),
            ("READ_UNCOMMITTED", IsolationLevel::ReadUncommitted),
            ("read_committed", IsolationLevel::ReadCommitted),
            ("READ_COMMITTED", IsolationLevel::ReadCommitted),
        ] {
            let yaml = format!("{base}backup:\n  isolation_level: {value}\n");
            let config: Config = serde_yaml::from_str(&yaml).unwrap();
            assert_eq!(config.backup.unwrap().isolation_level, expected, "{value}");
        }
        let bad = format!("{base}backup:\n  isolation_level: committed\n");
        assert!(serde_yaml::from_str::<Config>(&bad).is_err());
        assert_eq!(
            serde_json::to_string(&IsolationLevel::ReadUncommitted).unwrap(),
            "\"read_uncommitted\""
        );
    }

    #[test]
    fn circuit_breaker_defaults_match_previous_hardcoded_values() {
        let opts = RestoreOptions::default();
        assert!(opts.circuit_breaker.enabled);
        assert_eq!(opts.circuit_breaker.failure_threshold, 5);
        assert_eq!(opts.circuit_breaker.reset_timeout_ms, 30_000);
        assert_eq!(opts.circuit_breaker.success_threshold, 2);

        let backup = BackupOptions::default();
        assert!(backup.circuit_breaker.enabled);
        assert_eq!(backup.circuit_breaker.failure_threshold, 5);
    }

    #[test]
    fn circuit_breaker_yaml_round_trip() {
        let yaml = r#"
circuit_breaker:
  enabled: false
  failure_threshold: 15
  reset_timeout_ms: 2000
  success_threshold: 1
"#;
        let opts: RestoreOptions = serde_yaml::from_str(yaml).unwrap();
        assert!(!opts.circuit_breaker.enabled);
        assert_eq!(opts.circuit_breaker.failure_threshold, 15);
        assert_eq!(opts.circuit_breaker.reset_timeout_ms, 2000);
        assert_eq!(opts.circuit_breaker.success_threshold, 1);
        assert!(opts.validate().is_ok());
    }

    #[test]
    fn circuit_breaker_validation_rejects_zero_thresholds() {
        let mut opts = valid_restore_options();
        opts.circuit_breaker.failure_threshold = 0;
        assert!(opts.validate().is_err());

        opts.circuit_breaker.failure_threshold = 5;
        opts.circuit_breaker.success_threshold = 0;
        assert!(opts.validate().is_err());

        opts.circuit_breaker.success_threshold = 1;
        opts.circuit_breaker.reset_timeout_ms = 0;
        assert!(opts.validate().is_err());

        // Disabled breaker skips threshold validation
        opts.circuit_breaker.enabled = false;
        assert!(opts.validate().is_ok());
    }
}
