//! Offset storage for tracking backup progress.
//!
//! This module provides persistent offset storage to enable backup resumption
//! after process crashes. Offsets are stored locally in SQLite and periodically
//! synced to remote storage (S3) for durability.

mod sqlite;

pub use sqlite::SqliteOffsetStore;

use async_trait::async_trait;

use crate::storage::StorageBackend;
use crate::Result;

/// Offset information for a topic-partition
#[derive(Debug, Clone)]
pub struct OffsetInfo {
    pub topic: String,
    pub partition: i32,
    pub last_offset: i64,
    pub checkpoint_ts: i64,
}

/// Trait for offset storage backends
#[async_trait]
pub trait OffsetStore: Send + Sync {
    /// Get the last committed offset for a topic-partition
    async fn get_offset(&self, backup_id: &str, topic: &str, partition: i32)
        -> Result<Option<i64>>;

    /// Set the offset for a topic-partition
    async fn set_offset(
        &self,
        backup_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()>;

    /// Get all offsets for a backup
    async fn get_all_offsets(&self, backup_id: &str) -> Result<Vec<OffsetInfo>>;

    /// Checkpoint all pending writes to durable storage
    async fn checkpoint(&self) -> Result<()>;

    /// Sync the local database to remote storage
    async fn sync_to_storage(&self, storage: &dyn StorageBackend, s3_key: &str) -> Result<()>;

    /// Load the database from remote storage
    async fn load_from_storage(&self, storage: &dyn StorageBackend, s3_key: &str) -> Result<bool>;

    /// Get or create a backup job record
    async fn get_or_create_job(&self, backup_id: &str, cluster_id: Option<&str>) -> Result<()>;

    /// Update job status
    async fn update_job_status(&self, backup_id: &str, status: &str) -> Result<()>;

    /// Update job heartbeat
    async fn heartbeat(&self, backup_id: &str) -> Result<()>;
}

/// Configuration for offset storage
#[derive(Debug, Clone)]
pub struct OffsetStoreConfig {
    /// Path to the local SQLite database
    pub db_path: std::path::PathBuf,
    /// S3 key for syncing the database (optional)
    pub s3_key: Option<String>,
    /// Checkpoint interval in seconds (default: 5)
    pub checkpoint_interval_secs: u64,
    /// Sync to S3 interval in seconds (default: 30)
    pub sync_interval_secs: u64,
}

impl Default for OffsetStoreConfig {
    fn default() -> Self {
        Self {
            db_path: std::path::PathBuf::from("./offsets.db"),
            s3_key: None,
            checkpoint_interval_secs: 5,
            sync_interval_secs: 30,
        }
    }
}
