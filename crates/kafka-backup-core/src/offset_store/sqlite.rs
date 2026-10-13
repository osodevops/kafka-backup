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
    /// Connection pool, wrapped in RwLock to allow replacement after
    /// loading a database snapshot from remote storage.
    pool: RwLock<SqlitePool>,
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

        let pool = Self::open_pool(&config).await?;

        let store = Self {
            pool: RwLock::new(pool),
            config,
            pending: Arc::new(RwLock::new(std::collections::HashMap::new())),
        };

        store.initialize_schema().await?;

        Ok(store)
    }

    /// Build a new SQLite connection pool for the configured db_path
    async fn open_pool(config: &OffsetStoreConfig) -> Result<SqlitePool> {
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

        Ok(pool)
    }

    /// Initialize the database schema
    async fn initialize_schema(&self) -> Result<()> {
        let pool = self.pool.read().await;
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
        .execute(&*pool)
        .await?;

        debug!("SQLite schema initialized");
        Ok(())
    }

    /// Status of the backup job row for `backup_id` (`running`/`completed`),
    /// or `None` when no run ever registered. Read by `kafka-backup prune`
    /// to decide whether a run is still live.
    pub async fn job_status(&self, backup_id: &str) -> Result<Option<String>> {
        let pool = self.pool.read().await;
        let row: Option<(String,)> =
            sqlx::query_as("SELECT status FROM backup_jobs WHERE backup_id = ?")
                .bind(backup_id)
                .fetch_optional(&*pool)
                .await?;
        Ok(row.map(|(status,)| status))
    }

    /// Load the remote copy of the database when one exists, replacing the
    /// local file; keep the local database only when there is no remote copy.
    ///
    /// The remote copy is authoritative: it is uploaded only after the
    /// manifest save that justifies its checkpoints, whereas a non-empty
    /// local file can be a stale leftover (a crashed run on the same host, a
    /// re-attached volume) that is ahead of what storage durably holds.
    /// Before 0.25 a non-empty local database always won.
    ///
    /// Returns `true` when the remote copy was loaded.
    pub async fn try_load_from_storage(
        &self,
        storage: &dyn StorageBackend,
        s3_key: &str,
    ) -> Result<bool> {
        let loaded = self.load_from_storage(storage, s3_key).await?;
        if !loaded {
            let pool = self.pool.read().await;
            let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM offsets")
                .fetch_one(&*pool)
                .await?;
            if count.0 > 0 {
                debug!(
                    "No remote offset database; keeping the local one ({} offset records)",
                    count.0
                );
            }
        }
        Ok(loaded)
    }

    /// Remove `{db}-wal` / `{db}-shm` left next to the database file. A WAL
    /// from the old file must never be replayed onto a replaced one.
    async fn remove_wal_files(&self) {
        for suffix in ["-wal", "-shm"] {
            let mut path = self.config.db_path.clone().into_os_string();
            path.push(suffix);
            match tokio::fs::remove_file(&path).await {
                Ok(()) => debug!("Removed stale {:?}", path),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!("Could not remove {:?}: {}", path, e),
            }
        }
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
        let pool = self.pool.read().await;
        let result: Option<(i64,)> = sqlx::query_as(
            "SELECT last_offset FROM offsets WHERE backup_id = ? AND topic = ? AND partition = ?",
        )
        .bind(backup_id)
        .bind(topic)
        .bind(partition)
        .fetch_optional(&*pool)
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
        let pool = self.pool.read().await;
        let rows: Vec<(String, i32, i64, i64)> = sqlx::query_as(
            "SELECT topic, partition, last_offset, checkpoint_ts FROM offsets WHERE backup_id = ?",
        )
        .bind(backup_id)
        .fetch_all(&*pool)
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
        let pool = self.pool.read().await;
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
            .execute(&*pool)
            .await?;
        }

        debug!("Checkpointed offsets to SQLite");
        Ok(())
    }

    async fn sync_to_storage(&self, storage: &dyn StorageBackend, s3_key: &str) -> Result<()> {
        // First checkpoint any pending updates
        self.checkpoint().await?;

        // Force WAL checkpoint so the main DB file has all data
        {
            let pool = self.pool.read().await;
            sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(&*pool)
                .await
                .ok();
        }

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

        // Ensure parent directory exists (may not if path was reconfigured)
        if let Some(parent) = self.config.db_path.parent() {
            tokio::fs::create_dir_all(parent).await.ok();
        }

        // Write to temporary file first
        let temp_path = self.config.db_path.with_extension("db.tmp");
        tokio::fs::write(&temp_path, &db_bytes).await?;

        // Close current pool, replace the file, then reconnect.
        // Previously the pool was closed but never reopened (Issue #62).
        {
            let pool = self.pool.read().await;
            pool.close().await;
        }
        self.remove_wal_files().await;
        tokio::fs::rename(&temp_path, &self.config.db_path).await?;
        // Unflushed writes belonged to the replaced database.
        self.pending.write().await.clear();

        // Reconnect to the replaced database
        let new_pool = Self::open_pool(&self.config).await?;
        {
            let mut pool = self.pool.write().await;
            *pool = new_pool;
        }

        // Run schema migrations on the loaded database. The stored DB may have
        // been created by an older version that is missing tables added later
        // (e.g. backup_jobs). All DDL uses IF NOT EXISTS so this is idempotent.
        self.initialize_schema().await?;

        info!("Loaded offset database from storage: {}", s3_key);
        Ok(true)
    }

    async fn get_or_create_job(&self, backup_id: &str, cluster_id: Option<&str>) -> Result<()> {
        let pool = self.pool.read().await;
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
        .execute(&*pool)
        .await?;

        Ok(())
    }

    async fn update_job_status(&self, backup_id: &str, status: &str) -> Result<()> {
        let pool = self.pool.read().await;
        sqlx::query("UPDATE backup_jobs SET status = ? WHERE backup_id = ?")
            .bind(status)
            .bind(backup_id)
            .execute(&*pool)
            .await?;

        Ok(())
    }

    async fn heartbeat(&self, backup_id: &str) -> Result<()> {
        let pool = self.pool.read().await;
        sqlx::query(
            "UPDATE backup_jobs SET last_heartbeat = strftime('%s', 'now') * 1000 WHERE backup_id = ?",
        )
        .bind(backup_id)
        .execute(&*pool)
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

    /// Regression test for Issue #62: SqliteOffsetStore::new() fails with
    /// SQLITE_CANTOPEN (code 14) when the db_path parent is read-only,
    /// e.g. K8s pods with readOnlyRootFilesystem: true.
    #[tokio::test]
    async fn test_issue_62_readonly_directory_fails() {
        let temp_dir = TempDir::new().unwrap();
        let readonly_dir = temp_dir.path().join("readonly");
        std::fs::create_dir(&readonly_dir).unwrap();

        // Make directory read-only
        let mut perms = std::fs::metadata(&readonly_dir).unwrap().permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&readonly_dir, perms).unwrap();

        let config = OffsetStoreConfig {
            db_path: readonly_dir.join("offsets.db"),
            ..Default::default()
        };

        let result = SqliteOffsetStore::new(config).await;
        assert!(result.is_err(), "Should fail on read-only directory");

        let err_msg = result.err().expect("should be an error").to_string();
        assert!(
            err_msg.contains("unable to open database file")
                || err_msg.contains("read-only")
                || err_msg.contains("permission denied"),
            "Expected SQLite CANTOPEN error, got: {}",
            err_msg
        );

        // Cleanup
        let mut perms = std::fs::metadata(&readonly_dir).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(&readonly_dir, perms).unwrap();
    }

    async fn store_at(path: std::path::PathBuf) -> SqliteOffsetStore {
        SqliteOffsetStore::new(OffsetStoreConfig {
            db_path: path,
            ..Default::default()
        })
        .await
        .unwrap()
    }

    /// The remote copy is written only after the manifest that justifies it
    /// (checkpoint durability), so a non-empty local database — left by a
    /// crashed run on the same host, or a restored volume — must not win
    /// over it.
    #[tokio::test]
    async fn try_load_prefers_the_remote_copy_over_a_non_empty_local_db() {
        use crate::storage::MemoryBackend;
        let temp_dir = TempDir::new().unwrap();
        let storage = MemoryBackend::new();

        // Remote: checkpoint 50.
        let remote = store_at(temp_dir.path().join("remote.db")).await;
        remote.set_offset("b", "t", 0, 50).await.unwrap();
        remote
            .sync_to_storage(&storage, "b/offsets.db")
            .await
            .unwrap();

        // Local (stale, different content): checkpoint 100.
        let local = store_at(temp_dir.path().join("local.db")).await;
        local.set_offset("b", "t", 0, 100).await.unwrap();
        local.checkpoint().await.unwrap();

        let loaded = local
            .try_load_from_storage(&storage, "b/offsets.db")
            .await
            .unwrap();
        assert!(loaded, "the remote copy must be loaded");
        assert_eq!(local.get_offset("b", "t", 0).await.unwrap(), Some(50));

        // The replaced database keeps working (schema, writes, re-sync).
        local.set_offset("b", "t", 1, 7).await.unwrap();
        local
            .sync_to_storage(&storage, "b/offsets.db")
            .await
            .unwrap();
        assert_eq!(local.get_offset("b", "t", 1).await.unwrap(), Some(7));
    }

    #[tokio::test]
    async fn try_load_keeps_the_local_db_when_there_is_no_remote_copy() {
        use crate::storage::MemoryBackend;
        let temp_dir = TempDir::new().unwrap();
        let storage = MemoryBackend::new();

        let local = store_at(temp_dir.path().join("local.db")).await;
        local.set_offset("b", "t", 0, 100).await.unwrap();
        local.checkpoint().await.unwrap();

        let loaded = local
            .try_load_from_storage(&storage, "b/offsets.db")
            .await
            .unwrap();
        assert!(!loaded);
        assert_eq!(local.get_offset("b", "t", 0).await.unwrap(), Some(100));
    }

    /// Regression test for Issue #62: after load_from_storage() replaces the
    /// database file, subsequent operations must still work (pool reconnection).
    #[tokio::test]
    async fn test_load_from_storage_reconnects_pool() {
        use crate::storage::MemoryBackend;

        // Create store with some data
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("offsets.db");
        let config = OffsetStoreConfig {
            db_path: db_path.clone(),
            ..Default::default()
        };

        let store = SqliteOffsetStore::new(config.clone()).await.unwrap();
        store
            .set_offset("backup-1", "topic-1", 0, 42)
            .await
            .unwrap();
        store.checkpoint().await.unwrap();

        // Sync to in-memory storage
        let storage = MemoryBackend::new();
        store
            .sync_to_storage(&storage, "state/offsets.db")
            .await
            .unwrap();

        // Create a fresh store (simulates process restart)
        let store2 = SqliteOffsetStore::new(config).await.unwrap();

        // Load from storage — this closes the pool and replaces the DB file
        let loaded = store2
            .load_from_storage(&storage, "state/offsets.db")
            .await
            .unwrap();
        assert!(loaded, "Should have loaded from storage");

        // Operations after load_from_storage must work (pool was reconnected)
        store2
            .get_or_create_job("backup-1", None)
            .await
            .expect("get_or_create_job should work after load_from_storage");

        let offset = store2
            .get_offset("backup-1", "topic-1", 0)
            .await
            .expect("get_offset should work after load_from_storage");
        assert_eq!(offset, Some(42), "Should recover offset from loaded DB");
    }
}
