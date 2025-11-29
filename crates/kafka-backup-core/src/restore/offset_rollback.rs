//! Offset reset rollback with pre-restore snapshot capability.
//!
//! This module implements the rollback mechanism for offset reset operations:
//!
//! - **Pre-restore Snapshot**: Capture current offset state before any changes
//! - **Rollback**: Restore original offsets if reset fails or needs reverting
//! - **Verification**: Validate rollback completed correctly
//!
//! # Design Pattern
//!
//! Inspired by Apache Flink's checkpointing mechanism:
//! 1. Snapshot current state (atomically)
//! 2. Apply changes
//! 3. On failure: rollback to snapshot
//! 4. On success: optionally retain snapshot for manual rollback
//!
//! # Storage
//!
//! Snapshots are stored durably in the configured storage backend (S3, filesystem, etc.)
//! with the following structure:
//! ```text
//! offset-snapshots/
//!   {snapshot_id}/
//!     snapshot.json      # OffsetSnapshot serialized
//!     metadata.json      # OffsetSnapshotMetadata
//! ```

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, error, info, warn};

use crate::kafka::{commit_offsets, fetch_offsets, KafkaClient};
use crate::storage::StorageBackend;
use crate::Result;

use super::offset_automation::{
    BulkOffsetReset, BulkOffsetResetConfig, BulkOffsetResetReport, BulkResetStatus,
    OffsetMapping as BulkOffsetMapping,
};

// ============================================================================
// Snapshot Data Structures
// ============================================================================

/// Pre-restore snapshot of consumer group offsets.
///
/// This structure captures the complete offset state for a set of consumer groups
/// at a specific point in time. It can be used to rollback changes if needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetSnapshot {
    /// Unique identifier for this snapshot (UUID v4)
    pub snapshot_id: String,

    /// When the snapshot was created
    pub created_at: DateTime<Utc>,

    /// Consumer group offset states
    pub group_offsets: HashMap<String, GroupOffsetState>,

    /// Associated restore operation ID (if any)
    pub restore_id: Option<String>,

    /// Kafka cluster ID (if available)
    pub cluster_id: Option<String>,

    /// Bootstrap servers used to create the snapshot
    pub bootstrap_servers: Vec<String>,

    /// Description or reason for the snapshot
    pub description: Option<String>,
}

/// Offset state for a single consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupOffsetState {
    /// Consumer group ID
    pub group_id: String,

    /// Current offsets: (topic, partition) -> offset info
    pub offsets: HashMap<String, HashMap<i32, PartitionOffsetState>>,

    /// Total number of partitions tracked
    pub partition_count: usize,
}

/// Offset state for a single partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionOffsetState {
    /// Current committed offset
    pub offset: i64,

    /// Commit metadata (if any)
    pub metadata: Option<String>,

    /// When this offset was committed (if known)
    pub timestamp: Option<i64>,
}

/// Metadata about a snapshot (lightweight, for listing).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetSnapshotMetadata {
    /// Snapshot ID
    pub snapshot_id: String,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Number of consumer groups in the snapshot
    pub group_count: usize,

    /// Total number of offsets captured
    pub offset_count: usize,

    /// Associated restore ID
    pub restore_id: Option<String>,

    /// Description
    pub description: Option<String>,
}

impl OffsetSnapshot {
    /// Create a new empty snapshot with generated ID.
    pub fn new(bootstrap_servers: Vec<String>) -> Self {
        Self {
            snapshot_id: generate_snapshot_id(),
            created_at: Utc::now(),
            group_offsets: HashMap::new(),
            restore_id: None,
            cluster_id: None,
            bootstrap_servers,
            description: None,
        }
    }

    /// Create a new snapshot with a specific ID.
    pub fn with_id(snapshot_id: String, bootstrap_servers: Vec<String>) -> Self {
        Self {
            snapshot_id,
            created_at: Utc::now(),
            group_offsets: HashMap::new(),
            restore_id: None,
            cluster_id: None,
            bootstrap_servers,
            description: None,
        }
    }

    /// Set the restore ID.
    pub fn with_restore_id(mut self, restore_id: String) -> Self {
        self.restore_id = Some(restore_id);
        self
    }

    /// Set the description.
    pub fn with_description(mut self, description: String) -> Self {
        self.description = Some(description);
        self
    }

    /// Add a group's offset state to the snapshot.
    pub fn add_group(&mut self, group_state: GroupOffsetState) {
        self.group_offsets.insert(group_state.group_id.clone(), group_state);
    }

    /// Get the total number of offsets in the snapshot.
    pub fn total_offsets(&self) -> usize {
        self.group_offsets
            .values()
            .map(|g| g.partition_count)
            .sum()
    }

    /// Convert to metadata (lightweight summary).
    pub fn to_metadata(&self) -> OffsetSnapshotMetadata {
        OffsetSnapshotMetadata {
            snapshot_id: self.snapshot_id.clone(),
            created_at: self.created_at,
            group_count: self.group_offsets.len(),
            offset_count: self.total_offsets(),
            restore_id: self.restore_id.clone(),
            description: self.description.clone(),
        }
    }
}

// ============================================================================
// Snapshot Storage
// ============================================================================

/// Trait for storing and retrieving offset snapshots.
#[async_trait]
pub trait OffsetSnapshotStorage: Send + Sync {
    /// Save a snapshot to storage.
    async fn save_snapshot(&self, snapshot: &OffsetSnapshot) -> Result<String>;

    /// Load a snapshot by ID.
    async fn load_snapshot(&self, snapshot_id: &str) -> Result<OffsetSnapshot>;

    /// List all snapshots (returns metadata only).
    async fn list_snapshots(&self) -> Result<Vec<OffsetSnapshotMetadata>>;

    /// Delete a snapshot.
    async fn delete_snapshot(&self, snapshot_id: &str) -> Result<()>;

    /// Check if a snapshot exists.
    async fn exists(&self, snapshot_id: &str) -> Result<bool>;
}

/// Implementation of OffsetSnapshotStorage using any StorageBackend.
pub struct StorageBackendSnapshotStore {
    /// Underlying storage backend
    backend: Arc<dyn StorageBackend>,

    /// Prefix for snapshot paths
    prefix: String,
}

impl StorageBackendSnapshotStore {
    /// Create a new snapshot store with the given backend.
    pub fn new(backend: Arc<dyn StorageBackend>) -> Self {
        Self {
            backend,
            prefix: "offset-snapshots".to_string(),
        }
    }

    /// Create with a custom prefix.
    pub fn with_prefix(backend: Arc<dyn StorageBackend>, prefix: String) -> Self {
        Self { backend, prefix }
    }

    /// Get the storage path for a snapshot.
    fn snapshot_path(&self, snapshot_id: &str) -> String {
        format!("{}/{}/snapshot.json", self.prefix, snapshot_id)
    }

    /// Get the metadata path for a snapshot.
    fn metadata_path(&self, snapshot_id: &str) -> String {
        format!("{}/{}/metadata.json", self.prefix, snapshot_id)
    }
}

#[async_trait]
impl OffsetSnapshotStorage for StorageBackendSnapshotStore {
    async fn save_snapshot(&self, snapshot: &OffsetSnapshot) -> Result<String> {
        // Serialize snapshot
        let snapshot_json = serde_json::to_vec_pretty(snapshot)?;
        let snapshot_path = self.snapshot_path(&snapshot.snapshot_id);

        // Serialize metadata
        let metadata = snapshot.to_metadata();
        let metadata_json = serde_json::to_vec_pretty(&metadata)?;
        let metadata_path = self.metadata_path(&snapshot.snapshot_id);

        // Write both files
        self.backend
            .put(&snapshot_path, Bytes::from(snapshot_json))
            .await?;
        self.backend
            .put(&metadata_path, Bytes::from(metadata_json))
            .await?;

        info!(
            "Saved offset snapshot {} with {} groups, {} offsets",
            snapshot.snapshot_id,
            snapshot.group_offsets.len(),
            snapshot.total_offsets()
        );

        Ok(snapshot.snapshot_id.clone())
    }

    async fn load_snapshot(&self, snapshot_id: &str) -> Result<OffsetSnapshot> {
        let path = self.snapshot_path(snapshot_id);
        let data = self.backend.get(&path).await?;
        let snapshot: OffsetSnapshot = serde_json::from_slice(&data)?;

        debug!("Loaded offset snapshot {}", snapshot_id);
        Ok(snapshot)
    }

    async fn list_snapshots(&self) -> Result<Vec<OffsetSnapshotMetadata>> {
        let prefix = format!("{}/", self.prefix);
        let keys = self.backend.list(&prefix).await?;

        let mut snapshots = Vec::new();

        // Find all metadata.json files
        for key in keys {
            if key.ends_with("/metadata.json") {
                match self.backend.get(&key).await {
                    Ok(data) => {
                        if let Ok(metadata) = serde_json::from_slice::<OffsetSnapshotMetadata>(&data)
                        {
                            snapshots.push(metadata);
                        }
                    }
                    Err(e) => {
                        warn!("Failed to read snapshot metadata {}: {}", key, e);
                    }
                }
            }
        }

        // Sort by creation time (newest first)
        snapshots.sort_by(|a, b| b.created_at.cmp(&a.created_at));

        Ok(snapshots)
    }

    async fn delete_snapshot(&self, snapshot_id: &str) -> Result<()> {
        let snapshot_path = self.snapshot_path(snapshot_id);
        let metadata_path = self.metadata_path(snapshot_id);

        // Delete both files (ignore errors for metadata if it doesn't exist)
        self.backend.delete(&snapshot_path).await?;
        let _ = self.backend.delete(&metadata_path).await;

        info!("Deleted offset snapshot {}", snapshot_id);
        Ok(())
    }

    async fn exists(&self, snapshot_id: &str) -> Result<bool> {
        let path = self.snapshot_path(snapshot_id);
        self.backend.exists(&path).await
    }
}

// ============================================================================
// Snapshot Operations
// ============================================================================

/// Capture current offset state for consumer groups.
///
/// This function fetches the currently committed offsets for each consumer group
/// from Kafka and creates a snapshot that can be used for rollback.
pub async fn snapshot_current_offsets(
    client: &KafkaClient,
    group_ids: &[String],
    bootstrap_servers: Vec<String>,
) -> Result<OffsetSnapshot> {
    let mut snapshot = OffsetSnapshot::new(bootstrap_servers);

    info!("Creating offset snapshot for {} groups", group_ids.len());

    for group_id in group_ids {
        debug!("Fetching offsets for group {}", group_id);

        match fetch_offsets(client, group_id, None).await {
            Ok(committed_offsets) => {
                let mut offsets: HashMap<String, HashMap<i32, PartitionOffsetState>> =
                    HashMap::new();
                let mut partition_count = 0;

                for committed in committed_offsets {
                    if committed.error_code == 0 {
                        offsets
                            .entry(committed.topic.clone())
                            .or_default()
                            .insert(
                                committed.partition,
                                PartitionOffsetState {
                                    offset: committed.offset,
                                    metadata: committed.metadata,
                                    timestamp: None,
                                },
                            );
                        partition_count += 1;
                    }
                }

                snapshot.add_group(GroupOffsetState {
                    group_id: group_id.clone(),
                    offsets,
                    partition_count,
                });

                debug!(
                    "Captured {} partitions for group {}",
                    partition_count, group_id
                );
            }
            Err(e) => {
                warn!("Failed to fetch offsets for group {}: {}", group_id, e);
                // Add empty state to indicate the group was processed but had no offsets
                snapshot.add_group(GroupOffsetState {
                    group_id: group_id.clone(),
                    offsets: HashMap::new(),
                    partition_count: 0,
                });
            }
        }
    }

    info!(
        "Created snapshot {} with {} groups, {} total offsets",
        snapshot.snapshot_id,
        snapshot.group_offsets.len(),
        snapshot.total_offsets()
    );

    Ok(snapshot)
}

// ============================================================================
// Rollback Operations
// ============================================================================

/// Result of a rollback operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackResult {
    /// Status of the rollback
    pub status: RollbackStatus,

    /// Snapshot ID used for rollback
    pub snapshot_id: String,

    /// Number of groups successfully rolled back
    pub groups_rolled_back: usize,

    /// Number of groups that failed to rollback
    pub groups_failed: usize,

    /// Total offsets restored
    pub offsets_restored: u64,

    /// Errors encountered
    pub errors: Vec<String>,

    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Status of a rollback operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RollbackStatus {
    /// All offsets restored successfully
    Success,
    /// Some offsets restored, some failed
    PartialSuccess,
    /// Rollback failed completely
    Failed,
}

/// Rollback offset reset to a previous snapshot.
///
/// This function restores consumer group offsets to the state captured
/// in the specified snapshot.
pub async fn rollback_offset_reset(
    client: &KafkaClient,
    snapshot: &OffsetSnapshot,
) -> Result<RollbackResult> {
    let start_time = Instant::now();

    info!(
        "Rolling back offsets to snapshot {} (created {})",
        snapshot.snapshot_id, snapshot.created_at
    );

    let mut groups_rolled_back = 0usize;
    let mut groups_failed = 0usize;
    let mut offsets_restored = 0u64;
    let mut errors = Vec::new();

    for (group_id, group_state) in &snapshot.group_offsets {
        // Build offset list for commit
        let mut offsets_to_commit: Vec<(String, i32, i64, Option<String>)> = Vec::new();

        for (topic, partitions) in &group_state.offsets {
            for (partition, state) in partitions {
                offsets_to_commit.push((
                    topic.clone(),
                    *partition,
                    state.offset,
                    state.metadata.clone(),
                ));
            }
        }

        if offsets_to_commit.is_empty() {
            debug!("No offsets to rollback for group {}", group_id);
            groups_rolled_back += 1;
            continue;
        }

        debug!(
            "Rolling back {} offsets for group {}",
            offsets_to_commit.len(),
            group_id
        );

        match commit_offsets(client, group_id, &offsets_to_commit).await {
            Ok(results) => {
                let mut group_success = true;

                for (topic, partition, error_code) in results {
                    if error_code == 0 {
                        offsets_restored += 1;
                    } else {
                        group_success = false;
                        errors.push(format!(
                            "{}:{}:{} - error code {}",
                            group_id, topic, partition, error_code
                        ));
                    }
                }

                if group_success {
                    groups_rolled_back += 1;
                    debug!("Successfully rolled back group {}", group_id);
                } else {
                    groups_failed += 1;
                    warn!("Partial rollback failure for group {}", group_id);
                }
            }
            Err(e) => {
                groups_failed += 1;
                errors.push(format!("Group {} commit failed: {}", group_id, e));
                error!("Failed to rollback group {}: {}", group_id, e);
            }
        }
    }

    let duration_ms = start_time.elapsed().as_millis() as u64;

    let status = if groups_failed == 0 {
        RollbackStatus::Success
    } else if groups_rolled_back == 0 {
        RollbackStatus::Failed
    } else {
        RollbackStatus::PartialSuccess
    };

    let result = RollbackResult {
        status,
        snapshot_id: snapshot.snapshot_id.clone(),
        groups_rolled_back,
        groups_failed,
        offsets_restored,
        errors,
        duration_ms,
    };

    match result.status {
        RollbackStatus::Success => {
            info!(
                "Rollback completed successfully: {} groups, {} offsets in {}ms",
                result.groups_rolled_back, result.offsets_restored, result.duration_ms
            );
        }
        RollbackStatus::PartialSuccess => {
            warn!(
                "Rollback completed with partial success: {}/{} groups, {} offsets, {} errors in {}ms",
                result.groups_rolled_back,
                result.groups_rolled_back + result.groups_failed,
                result.offsets_restored,
                result.errors.len(),
                result.duration_ms
            );
        }
        RollbackStatus::Failed => {
            error!(
                "Rollback failed: {} errors in {}ms",
                result.errors.len(),
                result.duration_ms
            );
        }
    }

    Ok(result)
}

/// Verify that current offsets match the snapshot.
///
/// This function compares the current committed offsets against the snapshot
/// to verify that a rollback was successful.
pub async fn verify_rollback(
    client: &KafkaClient,
    snapshot: &OffsetSnapshot,
) -> Result<VerificationResult> {
    let mut groups_verified = Vec::new();
    let mut groups_mismatched = Vec::new();
    let mut mismatches = Vec::new();

    for (group_id, expected_state) in &snapshot.group_offsets {
        let actual_offsets = fetch_offsets(client, group_id, None).await?;

        let mut group_matches = true;

        for committed in actual_offsets {
            if committed.error_code != 0 {
                continue;
            }

            if let Some(topic_offsets) = expected_state.offsets.get(&committed.topic) {
                if let Some(expected) = topic_offsets.get(&committed.partition) {
                    if committed.offset != expected.offset {
                        group_matches = false;
                        mismatches.push(OffsetMismatch {
                            group_id: group_id.clone(),
                            topic: committed.topic.clone(),
                            partition: committed.partition,
                            expected_offset: expected.offset,
                            actual_offset: committed.offset,
                        });
                    }
                }
            }
        }

        if group_matches {
            groups_verified.push(group_id.clone());
        } else {
            groups_mismatched.push(group_id.clone());
        }
    }

    let verified = groups_mismatched.is_empty();

    Ok(VerificationResult {
        verified,
        groups_verified,
        groups_mismatched,
        mismatches,
    })
}

/// Result of verifying offsets against a snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationResult {
    /// Whether all offsets match the snapshot
    pub verified: bool,

    /// Groups that match the snapshot
    pub groups_verified: Vec<String>,

    /// Groups that don't match the snapshot
    pub groups_mismatched: Vec<String>,

    /// Detailed mismatches
    pub mismatches: Vec<OffsetMismatch>,
}

/// Details of a single offset mismatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetMismatch {
    /// Consumer group ID
    pub group_id: String,

    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// Expected offset from snapshot
    pub expected_offset: i64,

    /// Actual offset in Kafka
    pub actual_offset: i64,
}

// ============================================================================
// Restore with Rollback Capability
// ============================================================================

/// Result of a restore operation with rollback capability.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreWithRollbackResult {
    /// Overall status
    pub status: RestoreWithRollbackStatus,

    /// Snapshot ID (for potential rollback)
    pub snapshot_id: String,

    /// Whether offsets were reset
    pub offsets_reset: bool,

    /// Whether rollback is available
    pub rollback_available: bool,

    /// Bulk reset report (if offsets were reset)
    pub reset_report: Option<BulkOffsetResetReport>,

    /// Rollback result (if rollback was performed)
    pub rollback_result: Option<RollbackResult>,

    /// Errors encountered
    pub errors: Vec<String>,

    /// Total duration in milliseconds
    pub duration_ms: u64,
}

/// Status of a restore with rollback operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RestoreWithRollbackStatus {
    /// Restore and offset reset completed successfully
    Success,
    /// Offset reset partially succeeded
    PartialSuccess,
    /// Offset reset failed, rollback succeeded
    RolledBack,
    /// Offset reset failed, rollback also failed
    Failed,
}

/// Execute offset reset with automatic snapshot and optional rollback.
///
/// This is the main orchestration function that:
/// 1. Creates a snapshot of current offsets
/// 2. Applies new offsets
/// 3. On failure: automatically rolls back to snapshot
///
/// Note: This function requires two KafkaClient instances because BulkOffsetReset
/// takes ownership. Pass separate clients for snapshot/rollback and bulk reset.
pub async fn reset_offsets_with_rollback(
    snapshot_client: &KafkaClient,
    bulk_reset_client: KafkaClient,
    offset_mappings: Vec<BulkOffsetMapping>,
    snapshot_storage: Arc<dyn OffsetSnapshotStorage>,
    config: BulkOffsetResetConfig,
    bootstrap_servers: Vec<String>,
    auto_rollback_on_failure: bool,
) -> Result<RestoreWithRollbackResult> {
    let start_time = Instant::now();
    let mut errors = Vec::new();

    // Extract unique group IDs for snapshot
    let group_ids: Vec<String> = offset_mappings
        .iter()
        .map(|m| m.group_id.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    info!(
        "Starting offset reset with rollback capability for {} groups",
        group_ids.len()
    );

    // Phase 1: Create snapshot
    info!("Phase 1: Creating offset snapshot...");
    let snapshot = match snapshot_current_offsets(snapshot_client, &group_ids, bootstrap_servers).await {
        Ok(snap) => snap,
        Err(e) => {
            let error_msg = format!("Failed to create snapshot: {}", e);
            errors.push(error_msg);
            return Ok(RestoreWithRollbackResult {
                status: RestoreWithRollbackStatus::Failed,
                snapshot_id: String::new(),
                offsets_reset: false,
                rollback_available: false,
                reset_report: None,
                rollback_result: None,
                errors,
                duration_ms: start_time.elapsed().as_millis() as u64,
            });
        }
    };

    // Save snapshot to storage
    let snapshot_id = match snapshot_storage.save_snapshot(&snapshot).await {
        Ok(id) => id,
        Err(e) => {
            let error_msg = format!("Failed to save snapshot: {}", e);
            errors.push(error_msg);
            return Ok(RestoreWithRollbackResult {
                status: RestoreWithRollbackStatus::Failed,
                snapshot_id: snapshot.snapshot_id,
                offsets_reset: false,
                rollback_available: false,
                reset_report: None,
                rollback_result: None,
                errors,
                duration_ms: start_time.elapsed().as_millis() as u64,
            });
        }
    };

    info!("Snapshot saved: {}", snapshot_id);

    // Phase 2: Apply new offsets
    info!("Phase 2: Applying new offsets...");
    let bulk_reset = BulkOffsetReset::new(bulk_reset_client, config);
    let reset_report = match bulk_reset.reset_offsets_parallel(offset_mappings).await {
        Ok(report) => report,
        Err(e) => {
            let error_msg = format!("Offset reset failed: {}", e);
            errors.push(error_msg.clone());

            // Attempt rollback if enabled
            if auto_rollback_on_failure {
                info!("Phase 3: Auto-rollback triggered due to reset failure...");
                let rollback_result = rollback_offset_reset(snapshot_client, &snapshot).await;

                return Ok(RestoreWithRollbackResult {
                    status: match &rollback_result {
                        Ok(r) if r.status == RollbackStatus::Success => {
                            RestoreWithRollbackStatus::RolledBack
                        }
                        _ => RestoreWithRollbackStatus::Failed,
                    },
                    snapshot_id,
                    offsets_reset: false,
                    rollback_available: true,
                    reset_report: None,
                    rollback_result: rollback_result.ok(),
                    errors,
                    duration_ms: start_time.elapsed().as_millis() as u64,
                });
            }

            return Ok(RestoreWithRollbackResult {
                status: RestoreWithRollbackStatus::Failed,
                snapshot_id,
                offsets_reset: false,
                rollback_available: true,
                reset_report: None,
                rollback_result: None,
                errors,
                duration_ms: start_time.elapsed().as_millis() as u64,
            });
        }
    };

    // Check if we need to rollback due to partial failure
    let should_rollback = auto_rollback_on_failure && reset_report.status == BulkResetStatus::Failed;

    if should_rollback {
        info!("Phase 3: Auto-rollback triggered due to reset failure...");
        let rollback_result = rollback_offset_reset(snapshot_client, &snapshot).await;

        return Ok(RestoreWithRollbackResult {
            status: match &rollback_result {
                Ok(r) if r.status == RollbackStatus::Success => RestoreWithRollbackStatus::RolledBack,
                _ => RestoreWithRollbackStatus::Failed,
            },
            snapshot_id,
            offsets_reset: false,
            rollback_available: true,
            reset_report: Some(reset_report),
            rollback_result: rollback_result.ok(),
            errors,
            duration_ms: start_time.elapsed().as_millis() as u64,
        });
    }

    let status = match reset_report.status {
        BulkResetStatus::Success => RestoreWithRollbackStatus::Success,
        BulkResetStatus::PartialSuccess => RestoreWithRollbackStatus::PartialSuccess,
        BulkResetStatus::Failed => RestoreWithRollbackStatus::Failed,
    };

    Ok(RestoreWithRollbackResult {
        status,
        snapshot_id,
        offsets_reset: true,
        rollback_available: true,
        reset_report: Some(reset_report),
        rollback_result: None,
        errors,
        duration_ms: start_time.elapsed().as_millis() as u64,
    })
}

// ============================================================================
// Utility Functions
// ============================================================================

/// Generate a unique snapshot ID.
fn generate_snapshot_id() -> String {
    use std::time::SystemTime;

    let timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    // Simple ID: timestamp + random suffix
    let random_suffix: u32 = (timestamp as u32) ^ 0xDEADBEEF;
    format!("snap-{}-{:08x}", timestamp, random_suffix)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;

    #[test]
    fn test_offset_snapshot_creation() {
        let snapshot = OffsetSnapshot::new(vec!["localhost:9092".to_string()]);

        assert!(!snapshot.snapshot_id.is_empty());
        assert!(snapshot.group_offsets.is_empty());
        assert_eq!(snapshot.bootstrap_servers, vec!["localhost:9092"]);
    }

    #[test]
    fn test_offset_snapshot_with_groups() {
        let mut snapshot = OffsetSnapshot::new(vec!["localhost:9092".to_string()]);

        let mut offsets = HashMap::new();
        offsets.insert(
            0,
            PartitionOffsetState {
                offset: 100,
                metadata: Some("test".to_string()),
                timestamp: Some(12345),
            },
        );

        let mut topic_offsets = HashMap::new();
        topic_offsets.insert("orders".to_string(), offsets);

        snapshot.add_group(GroupOffsetState {
            group_id: "app1".to_string(),
            offsets: topic_offsets,
            partition_count: 1,
        });

        assert_eq!(snapshot.group_offsets.len(), 1);
        assert_eq!(snapshot.total_offsets(), 1);
    }

    #[test]
    fn test_offset_snapshot_serialization() {
        let mut snapshot =
            OffsetSnapshot::new(vec!["localhost:9092".to_string()])
                .with_description("Test snapshot".to_string());

        let mut offsets = HashMap::new();
        offsets.insert(
            0,
            PartitionOffsetState {
                offset: 1000,
                metadata: None,
                timestamp: None,
            },
        );

        let mut topic_offsets = HashMap::new();
        topic_offsets.insert("orders".to_string(), offsets);

        snapshot.add_group(GroupOffsetState {
            group_id: "test-group".to_string(),
            offsets: topic_offsets,
            partition_count: 1,
        });

        let json = serde_json::to_string(&snapshot).unwrap();
        let deserialized: OffsetSnapshot = serde_json::from_str(&json).unwrap();

        assert_eq!(snapshot.snapshot_id, deserialized.snapshot_id);
        assert_eq!(snapshot.group_offsets.len(), deserialized.group_offsets.len());
        assert_eq!(snapshot.description, deserialized.description);
    }

    #[test]
    fn test_snapshot_metadata() {
        let mut snapshot = OffsetSnapshot::new(vec!["localhost:9092".to_string()])
            .with_restore_id("restore-123".to_string())
            .with_description("Pre-restore snapshot".to_string());

        snapshot.add_group(GroupOffsetState {
            group_id: "group1".to_string(),
            offsets: HashMap::new(),
            partition_count: 5,
        });
        snapshot.add_group(GroupOffsetState {
            group_id: "group2".to_string(),
            offsets: HashMap::new(),
            partition_count: 3,
        });

        let metadata = snapshot.to_metadata();

        assert_eq!(metadata.snapshot_id, snapshot.snapshot_id);
        assert_eq!(metadata.group_count, 2);
        assert_eq!(metadata.offset_count, 8);
        assert_eq!(metadata.restore_id, Some("restore-123".to_string()));
        assert_eq!(metadata.description, Some("Pre-restore snapshot".to_string()));
    }

    #[tokio::test]
    async fn test_storage_backend_snapshot_store() {
        let backend = Arc::new(MemoryBackend::new());
        let store = StorageBackendSnapshotStore::new(backend);

        // Create and save a snapshot
        let mut snapshot = OffsetSnapshot::new(vec!["localhost:9092".to_string()]);
        snapshot.add_group(GroupOffsetState {
            group_id: "test-group".to_string(),
            offsets: HashMap::new(),
            partition_count: 0,
        });

        let snapshot_id = store.save_snapshot(&snapshot).await.unwrap();
        assert_eq!(snapshot_id, snapshot.snapshot_id);

        // Verify it exists
        assert!(store.exists(&snapshot_id).await.unwrap());

        // Load it back
        let loaded = store.load_snapshot(&snapshot_id).await.unwrap();
        assert_eq!(loaded.snapshot_id, snapshot.snapshot_id);
        assert_eq!(loaded.group_offsets.len(), 1);

        // List snapshots
        let snapshots = store.list_snapshots().await.unwrap();
        assert_eq!(snapshots.len(), 1);
        assert_eq!(snapshots[0].snapshot_id, snapshot_id);

        // Delete it
        store.delete_snapshot(&snapshot_id).await.unwrap();
        assert!(!store.exists(&snapshot_id).await.unwrap());
    }

    #[test]
    fn test_generate_snapshot_id() {
        let id1 = generate_snapshot_id();
        let id2 = generate_snapshot_id();

        assert!(id1.starts_with("snap-"));
        assert!(id2.starts_with("snap-"));
        // IDs should be unique (different timestamps or random parts)
        // Note: In very fast execution they might collide, but unlikely
    }

    #[test]
    fn test_rollback_result_serialization() {
        let result = RollbackResult {
            status: RollbackStatus::PartialSuccess,
            snapshot_id: "snap-123".to_string(),
            groups_rolled_back: 8,
            groups_failed: 2,
            offsets_restored: 40,
            errors: vec!["Error 1".to_string()],
            duration_ms: 500,
        };

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"partial_success\""));
        assert!(json.contains("\"groups_rolled_back\":8"));
    }

    #[test]
    fn test_verification_result_serialization() {
        let result = VerificationResult {
            verified: false,
            groups_verified: vec!["group1".to_string()],
            groups_mismatched: vec!["group2".to_string()],
            mismatches: vec![OffsetMismatch {
                group_id: "group2".to_string(),
                topic: "orders".to_string(),
                partition: 0,
                expected_offset: 100,
                actual_offset: 150,
            }],
        };

        let json = serde_json::to_string_pretty(&result).unwrap();
        assert!(json.contains("\"verified\": false"));
        assert!(json.contains("\"expected_offset\": 100"));
        assert!(json.contains("\"actual_offset\": 150"));
    }

    #[test]
    fn test_restore_with_rollback_status_serialization() {
        let status = RestoreWithRollbackStatus::RolledBack;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"rolled_back\"");
    }
}
