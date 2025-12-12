//! Configuration structures for Kafka backup and restore operations.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
}

/// Offset storage configuration for tracking backup progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetStorageConfig {
    /// Storage backend type (sqlite or memory)
    #[serde(default)]
    pub backend: OffsetStorageBackend,

    /// Path to local SQLite database file
    #[serde(default = "default_db_path")]
    pub db_path: PathBuf,

    /// S3 key for syncing offset database
    #[serde(default)]
    pub s3_key: Option<String>,

    /// Sync interval to remote storage in seconds (default: 30)
    #[serde(default = "default_sync_interval_secs")]
    pub sync_interval_secs: u64,
}

impl Default for OffsetStorageConfig {
    fn default() -> Self {
        Self {
            backend: OffsetStorageBackend::default(),
            db_path: default_db_path(),
            s3_key: None,
            sync_interval_secs: default_sync_interval_secs(),
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

/// SASL mechanism
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING-KEBAB-CASE")]
pub enum SaslMechanism {
    Plain,
    ScramSha256,
    ScramSha512,
}

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

    /// Include internal topics (default: false)
    #[serde(default)]
    pub include_internal_topics: bool,

    /// Internal topics to backup (e.g., __consumer_offsets, __transaction_state)
    #[serde(default)]
    pub internal_topics: Vec<String>,

    /// Checkpoint interval in seconds (default: 5)
    #[serde(default = "default_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,

    /// Sync to remote storage interval in seconds (default: 30)
    #[serde(default = "default_sync_interval_secs")]
    pub sync_interval_secs: u64,

    /// Include original offset headers in backup (Phase 1 of three-phase restore)
    /// Headers: x-original-offset, x-original-timestamp, x-source-cluster (if set)
    /// Default: true for DR scenarios
    #[serde(default = "default_include_offset_headers")]
    pub include_offset_headers: bool,

    /// Source cluster identifier for x-source-cluster header
    /// Useful for tracking which cluster the backup originated from
    #[serde(default)]
    pub source_cluster_id: Option<String>,
}

fn default_include_offset_headers() -> bool {
    true
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
            include_internal_topics: false,
            internal_topics: Vec::new(),
            checkpoint_interval_secs: default_checkpoint_interval_secs(),
            sync_interval_secs: default_sync_interval_secs(),
            include_offset_headers: default_include_offset_headers(),
            source_cluster_id: None,
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

/// Restore-specific options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    #[serde(default)]
    pub partition_mapping: std::collections::HashMap<i32, i32>,

    /// Topic mapping (original -> target)
    #[serde(default)]
    pub topic_mapping: std::collections::HashMap<String, String>,

    /// Consumer group offset handling strategy
    #[serde(default)]
    pub consumer_group_strategy: OffsetStrategy,

    /// Dry-run mode - validate without writing to target cluster
    #[serde(default)]
    pub dry_run: bool,

    /// Include original offset as a header
    #[serde(default)]
    pub include_original_offset_header: bool,

    /// Rate limit in records per second
    #[serde(default)]
    pub rate_limit_records_per_sec: Option<u64>,

    /// Rate limit in bytes per second
    #[serde(default)]
    pub rate_limit_bytes_per_sec: Option<u64>,

    /// Maximum concurrent partitions to restore in parallel
    #[serde(default = "default_max_concurrent_partitions")]
    pub max_concurrent_partitions: usize,

    /// Batch size for producing to target cluster (number of records)
    #[serde(default = "default_produce_batch_size")]
    pub produce_batch_size: usize,

    /// Checkpoint state file path for resumable restores
    #[serde(default)]
    pub checkpoint_state: Option<std::path::PathBuf>,

    /// Checkpoint interval in seconds
    #[serde(default = "default_restore_checkpoint_interval_secs")]
    pub checkpoint_interval_secs: u64,

    /// Consumer groups to reset offsets for (requires reset_consumer_offsets)
    #[serde(default)]
    pub consumer_groups: Vec<String>,

    /// Whether to reset consumer group offsets (dangerous, requires explicit opt-in)
    #[serde(default)]
    pub reset_consumer_offsets: bool,

    /// Output file for offset mapping report
    #[serde(default)]
    pub offset_report: Option<std::path::PathBuf>,
}

fn default_max_concurrent_partitions() -> usize {
    4
}

fn default_produce_batch_size() -> usize {
    1000
}

fn default_restore_checkpoint_interval_secs() -> u64 {
    60
}

impl Config {
    /// Validate the configuration
    pub fn validate(&self) -> crate::Result<()> {
        match self.mode {
            Mode::Backup => {
                if self.source.is_none() {
                    return Err(crate::Error::Config(
                        "Source configuration is required for backup mode".to_string(),
                    ));
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

        // Validate consumer group offset reset
        if self.reset_consumer_offsets && self.consumer_groups.is_empty() {
            return Err(crate::Error::Config(
                "consumer_groups must be specified when reset_consumer_offsets is true".to_string(),
            ));
        }

        Ok(())
    }
}
