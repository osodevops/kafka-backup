//! SQLite-based offset storage implementation.

use async_trait::async_trait;
use bytes::Bytes;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

use super::{OffsetInfo, OffsetStore, OffsetStoreConfig};
use crate::storage::StorageBackend;
use crate::Result;

/// Type alias for pending offset updates map (backup_id, topic, partition) -> offset
type PendingOffsets = std::collections::HashMap<(String, String, i32), i64>;

/// SQLite-based offset store
pub struct SqliteOffsetStore {
    pool: SqlitePool,
    config: OffsetStoreConfig,
    /// Pending offset updates (topic, partition) -> offset
    pending: Arc<RwLock<PendingOffsets>>,
}

impl SqliteOffsetStore {
    /// Create a new SQLite offset store
    pub async fn new(config: OffsetStoreConfig) -> Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = config.db_path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }

        let options = SqliteConnectOptions::from_str(&format!(
            "sqlite:{}?mode=rwc",
            config.db_path.display()
        ))?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;

        let store = Self {
            pool,
            config,
            pending: Arc::new(RwLock::new(std::collections::HashMap::new())),
        };

        store.initialize_schema().await?;

        Ok(store)
    }

    /// Initialize the database schema
    async fn initialize_schema(&self) -> Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS offsets (
                backup_id TEXT NOT NULL,
                topic TEXT NOT NULL,
                partition INTEGER NOT NULL,
                last_offset INTEGER NOT NULL,
                checkpoint_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000),
                PRIMARY KEY (backup_id, topic, partition)
            );

            CREATE TABLE IF NOT EXISTS backup_jobs (
                backup_id TEXT PRIMARY KEY,
                source_cluster_id TEXT,
                status TEXT NOT NULL DEFAULT 'running',
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000),
                last_heartbeat INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000),
                last_checkpoint INTEGER
            );

            CREATE INDEX IF NOT EXISTS idx_offsets_backup ON offsets(backup_id);
            CREATE INDEX IF NOT EXISTS idx_jobs_status ON backup_jobs(status);
            "#,
        )
        .execute(&self.pool)
        .await?;

        debug!("SQLite schema initialized");
        Ok(())
    }

    /// Try to load database from storage if local doesn't exist
    pub async fn try_load_from_storage(
        &self,
        storage: &dyn StorageBackend,
        s3_key: &str,
    ) -> Result<bool> {
        // Check if local database already has data
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM offsets")
            .fetch_one(&self.pool)
            .await?;

        if count.0 > 0 {
            debug!("Local database already has {} offset records", count.0);
            return Ok(false);
        }

        // Try to load from storage
        self.load_from_storage(storage, s3_key).await
    }
}

#[async_trait]
impl OffsetStore for SqliteOffsetStore {
    async fn get_offset(
        &self,
        backup_id: &str,
        topic: &str,
        partition: i32,
    ) -> Result<Option<i64>> {
        // Check pending updates first
        {
            let pending = self.pending.read().await;
            if let Some(&offset) =
                pending.get(&(backup_id.to_string(), topic.to_string(), partition))
            {
                return Ok(Some(offset));
            }
        }

        // Query database
        let result: Option<(i64,)> = sqlx::query_as(
            "SELECT last_offset FROM offsets WHERE backup_id = ? AND topic = ? AND partition = ?",
        )
        .bind(backup_id)
        .bind(topic)
        .bind(partition)
        .fetch_optional(&self.pool)
        .await?;

        Ok(result.map(|(offset,)| offset))
    }

    async fn set_offset(
        &self,
        backup_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        // Store in pending updates
        let mut pending = self.pending.write().await;
        pending.insert(
            (backup_id.to_string(), topic.to_string(), partition),
            offset,
        );
        Ok(())
    }

    async fn get_all_offsets(&self, backup_id: &str) -> Result<Vec<OffsetInfo>> {
        let rows: Vec<(String, i32, i64, i64)> = sqlx::query_as(
            "SELECT topic, partition, last_offset, checkpoint_ts FROM offsets WHERE backup_id = ?",
        )
        .bind(backup_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(
                |(topic, partition, last_offset, checkpoint_ts)| OffsetInfo {
                    topic,
                    partition,
                    last_offset,
                    checkpoint_ts,
                },
            )
            .collect())
    }

    async fn checkpoint(&self) -> Result<()> {
        let updates: Vec<_> = {
            let mut pending = self.pending.write().await;
            let updates: Vec<_> = pending.drain().collect();
            updates
        };

        if updates.is_empty() {
            return Ok(());
        }

        let now = chrono::Utc::now().timestamp_millis();

        // Batch insert/update
        for ((backup_id, topic, partition), offset) in updates {
            sqlx::query(
                r#"
                INSERT INTO offsets (backup_id, topic, partition, last_offset, checkpoint_ts)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(backup_id, topic, partition)
                DO UPDATE SET last_offset = excluded.last_offset, checkpoint_ts = excluded.checkpoint_ts
                "#,
            )
            .bind(&backup_id)
            .bind(&topic)
            .bind(partition)
            .bind(offset)
            .bind(now)
            .execute(&self.pool)
            .await?;
        }

        debug!("Checkpointed offsets to SQLite");
        Ok(())
    }

    async fn sync_to_storage(&self, storage: &dyn StorageBackend, s3_key: &str) -> Result<()> {
        // First checkpoint any pending updates
        self.checkpoint().await?;

        // Read the SQLite file
        let db_bytes = tokio::fs::read(&self.config.db_path).await?;

        // Upload to storage
        storage.put(s3_key, Bytes::from(db_bytes)).await?;

        info!("Synced offset database to storage: {}", s3_key);
        Ok(())
    }

    async fn load_from_storage(&self, storage: &dyn StorageBackend, s3_key: &str) -> Result<bool> {
        // Check if remote exists
        if !storage.exists(s3_key).await? {
            debug!("No remote offset database found at {}", s3_key);
            return Ok(false);
        }

        // Download from storage
        let db_bytes = storage.get(s3_key).await?;

        // Write to temporary file first
        let temp_path = self.config.db_path.with_extension("db.tmp");
        tokio::fs::write(&temp_path, &db_bytes).await?;

        // Close pool and rename
        self.pool.close().await;
        tokio::fs::rename(&temp_path, &self.config.db_path).await?;

        info!("Loaded offset database from storage: {}", s3_key);
        Ok(true)
    }

    async fn get_or_create_job(&self, backup_id: &str, cluster_id: Option<&str>) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO backup_jobs (backup_id, source_cluster_id, status)
            VALUES (?, ?, 'running')
            ON CONFLICT(backup_id) DO UPDATE SET
                status = 'running',
                last_heartbeat = strftime('%s', 'now') * 1000
            "#,
        )
        .bind(backup_id)
        .bind(cluster_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn update_job_status(&self, backup_id: &str, status: &str) -> Result<()> {
        sqlx::query("UPDATE backup_jobs SET status = ? WHERE backup_id = ?")
            .bind(status)
            .bind(backup_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn heartbeat(&self, backup_id: &str) -> Result<()> {
        sqlx::query(
            "UPDATE backup_jobs SET last_heartbeat = strftime('%s', 'now') * 1000 WHERE backup_id = ?",
        )
        .bind(backup_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_offset_store_basic() {
        let temp_dir = TempDir::new().unwrap();
        let config = OffsetStoreConfig {
            db_path: temp_dir.path().join("test.db"),
            ..Default::default()
        };

        let store = SqliteOffsetStore::new(config).await.unwrap();

        // Initially no offset
        let offset = store.get_offset("backup-1", "topic-1", 0).await.unwrap();
        assert!(offset.is_none());

        // Set offset
        store
            .set_offset("backup-1", "topic-1", 0, 100)
            .await
            .unwrap();

        // Should be in pending
        let offset = store.get_offset("backup-1", "topic-1", 0).await.unwrap();
        assert_eq!(offset, Some(100));

        // Checkpoint
        store.checkpoint().await.unwrap();

        // Should still be readable
        let offset = store.get_offset("backup-1", "topic-1", 0).await.unwrap();
        assert_eq!(offset, Some(100));
    }

    #[tokio::test]
    async fn test_offset_store_multiple_partitions() {
        let temp_dir = TempDir::new().unwrap();
        let config = OffsetStoreConfig {
            db_path: temp_dir.path().join("test.db"),
            ..Default::default()
        };

        let store = SqliteOffsetStore::new(config).await.unwrap();

        // Set offsets for multiple partitions
        for partition in 0..10 {
            store
                .set_offset("backup-1", "topic-1", partition, partition as i64 * 100)
                .await
                .unwrap();
        }

        store.checkpoint().await.unwrap();

        // Verify all offsets
        let offsets = store.get_all_offsets("backup-1").await.unwrap();
        assert_eq!(offsets.len(), 10);
    }
}
