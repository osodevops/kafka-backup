//! Validation context — shared state passed to every check.

use std::sync::Arc;

use crate::kafka::PartitionLeaderRouter;
use crate::manifest::BackupManifest;
use crate::storage::StorageBackend;

/// Shared context available to all validation checks during a run.
pub struct ValidationContext {
    /// Backup ID being validated.
    pub backup_id: String,

    /// The backup manifest loaded from object storage.
    pub backup_manifest: BackupManifest,

    /// Partition-leader-aware Kafka client connected to the restored (target)
    /// cluster. Uses `PartitionLeaderRouter` so that `ListOffsets` requests are
    /// routed to the correct broker for each partition, preventing
    /// `NOT_LEADER_FOR_PARTITION` errors on multi-broker clusters.
    pub target_client: Arc<PartitionLeaderRouter>,

    /// Storage backend for reading backup data / uploading evidence.
    pub storage: Arc<dyn StorageBackend>,

    /// PITR timestamp used during restore, if any (epoch millis).
    pub pitr_timestamp: Option<i64>,

    /// HTTP client for webhook checks.
    pub http_client: reqwest::Client,

    /// Bootstrap servers string for the restored cluster (for webhook payloads).
    pub target_bootstrap_servers: Vec<String>,
}
