//! Restore checkpoint persistence and resume reconciliation.
//!
//! The checkpoint is written atomically (temp file in the same directory,
//! fsync, rename, directory fsync) to the local `checkpoint_state` file
//! and/or to backup storage at `{backup_id}/restore-checkpoints/{restore_id}.json`
//! (`restore.checkpoint_storage`), every `checkpoint_interval_secs` and after
//! every topic. It only ever describes acknowledged work: a segment is
//! listed as completed once all its records were acknowledged, and each
//! source partition records the source offset of its last acknowledged
//! record and the target's high watermark after that acknowledgement.
//!
//! On resume, records at or below a partition's last acknowledged source
//! offset are skipped. Records acknowledged after the last save (the
//! "unsaved tail") are found by comparing the target partition's high
//! watermark with the recorded one: the difference is how many of the next
//! records were already written. They are verified record by record
//! against the target (key, value, headers) and skipped; a mismatch, or more
//! extra records than the backup has left, means another writer touched
//! the partition, and the restore fails instead of guessing.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::config::{IsolationLevel, RestoreCheckpointStorage, RestoreOptions};
use crate::error::KafkaError;
use crate::kafka::PartitionLeaderRouter;
use crate::manifest::{BackupRecord, PartitionRestoreProgress, RestoreCheckpoint};
use crate::storage::StorageBackend;
use crate::Result;

/// Distinguishes concurrent temp files within this process.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Write `data` to `path` atomically: a temp file in the same directory,
/// fsynced, renamed over `path`, then the directory fsynced (Unix). A
/// reader, or a process restarted after a crash, sees the old content or
/// the new content, never a partial file.
pub(crate) async fn write_file_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
        _ => PathBuf::from("."),
    };
    let file_name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other(format!("not a file path: {}", path.display())))?;
    tokio::fs::create_dir_all(&parent).await?;
    let tmp = parent.join(format!(
        ".{}.tmp-{}-{}",
        file_name.to_string_lossy(),
        std::process::id(),
        TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let result = async {
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(data).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&tmp, path).await
    }
    .await;
    if let Err(e) = result {
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(e);
    }
    #[cfg(unix)]
    if let Ok(dir) = tokio::fs::File::open(&parent).await {
        if let Err(e) = dir.sync_all().await {
            debug!("fsync of {} failed: {}", parent.display(), e);
        }
    }
    Ok(())
}

/// Default restore id: a digest of the backup id, the target bootstrap
/// servers and the restore configuration hash, so a re-run of the same
/// restore against the same cluster finds its remote checkpoint.
pub(crate) fn default_restore_id(
    backup_id: &str,
    target_bootstrap_servers: &[String],
    config_hash: &str,
) -> String {
    use sha2::{Digest, Sha256};
    let mut servers = target_bootstrap_servers.to_vec();
    servers.sort();
    let mut hasher = Sha256::new();
    hasher.update(backup_id.as_bytes());
    hasher.update([0]);
    hasher.update(servers.join(",").as_bytes());
    hasher.update([0]);
    hasher.update(config_hash.as_bytes());
    let digest = hasher.finalize();
    digest[..8].iter().map(|b| format!("{b:02x}")).collect()
}

/// Where the checkpoint lives and how it is saved.
pub(crate) struct CheckpointStore {
    local: Option<PathBuf>,
    remote: Option<(Arc<dyn StorageBackend>, String)>,
    /// Serialises saves, so an older snapshot never overwrites a newer one.
    save_lock: Mutex<()>,
}

impl CheckpointStore {
    /// The store for `options`; `None` when checkpointing is off (no
    /// `checkpoint_state` and `checkpoint_storage: local`).
    pub(crate) fn from_options(
        options: &RestoreOptions,
        storage: Arc<dyn StorageBackend>,
        backup_id: &str,
        restore_id: &str,
    ) -> Option<Self> {
        let local = match options.checkpoint_storage {
            RestoreCheckpointStorage::Local | RestoreCheckpointStorage::Both => {
                options.checkpoint_state.clone()
            }
            RestoreCheckpointStorage::Remote => None,
        };
        let remote = match options.checkpoint_storage {
            RestoreCheckpointStorage::Remote | RestoreCheckpointStorage::Both => {
                Some((storage, remote_checkpoint_key(backup_id, restore_id)))
            }
            RestoreCheckpointStorage::Local => None,
        };
        (local.is_some() || remote.is_some()).then(|| Self {
            local,
            remote,
            save_lock: Mutex::new(()),
        })
    }

    /// Where the checkpoint is kept, for logs.
    pub(crate) fn describe(&self) -> String {
        let mut places = Vec::new();
        if let Some(path) = &self.local {
            places.push(path.display().to_string());
        }
        if let Some((_, key)) = &self.remote {
            places.push(format!("backup storage key {key}"));
        }
        places.join(" and ")
    }

    /// The saved checkpoint, if any: the newer of the local and remote
    /// copies (by `last_checkpoint_time`).
    pub(crate) async fn load(&self) -> Result<Option<RestoreCheckpoint>> {
        let mut found: Vec<RestoreCheckpoint> = Vec::new();
        if let Some(path) = &self.local {
            match tokio::fs::read(path).await {
                Ok(data) => found.push(serde_json::from_slice(&data)?),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        if let Some((storage, key)) = &self.remote {
            if let Some(data) = crate::storage::get_if_exists(storage.as_ref(), key).await? {
                found.push(serde_json::from_slice(&data)?);
            }
        }
        Ok(found.into_iter().max_by_key(|cp| cp.last_checkpoint_time))
    }

    /// Save a snapshot of `checkpoint` (stamped with the save time) to
    /// every configured place.
    pub(crate) async fn save(&self, checkpoint: &Mutex<Option<RestoreCheckpoint>>) -> Result<()> {
        let _serialised = self.save_lock.lock().await;
        let json = {
            let mut guard = checkpoint.lock().await;
            let Some(cp) = guard.as_mut() else {
                return Ok(());
            };
            cp.touch();
            serde_json::to_vec_pretty(cp)?
        };
        if let Some(path) = &self.local {
            write_file_atomic(path, &json).await?;
        }
        if let Some((storage, key)) = &self.remote {
            storage.put(key, Bytes::from(json)).await?;
        }
        debug!("Restore checkpoint saved to {}", self.describe());
        Ok(())
    }
}

/// Storage key of a restore's remote checkpoint.
pub(crate) fn remote_checkpoint_key(backup_id: &str, restore_id: &str) -> String {
    format!("{backup_id}/restore-checkpoints/{restore_id}.json")
}

/// How many already-written records follow a partition's checkpointed
/// progress: the target's high watermark now minus the one recorded after
/// the last acknowledged batch. Fails when the target has fewer records
/// than were acknowledged (the topic was recreated or truncated).
pub(crate) fn unsaved_tail(
    progress: &PartitionRestoreProgress,
    high_watermark: i64,
) -> Result<u64> {
    let tail = high_watermark - progress.target_hw_after_ack;
    if tail < 0 {
        return Err(KafkaError::BrokerError {
            code: -1,
            message: format!(
                "cannot resume {}[{}] -> {}[{}]: the target's high watermark is {}, below the {} \
                 recorded in the checkpoint after the last acknowledged batch. Records the \
                 previous run wrote are gone (topic deleted, recreated or truncated?). Delete \
                 the checkpoint to restore from the beginning",
                progress.source_topic,
                progress.source_partition,
                progress.target_topic,
                progress.target_partition,
                high_watermark,
                progress.target_hw_after_ack
            ),
        }
        .into());
    }
    Ok(tail as u64)
}

fn another_writer(progress: &PartitionRestoreProgress, detail: String) -> crate::Error {
    KafkaError::BrokerError {
        code: -1,
        message: format!(
            "cannot resume {}[{}] -> {}[{}]: {detail}. Another writer has produced to the target \
             partition since the checkpoint, so the records this restore already wrote cannot be \
             told apart. Stop the other writer, or restore into an empty topic",
            progress.source_topic,
            progress.source_partition,
            progress.target_topic,
            progress.target_partition,
        ),
    }
    .into()
}

/// Whether a record read back from the target is the one the restore
/// would produce (key, value and headers; the timestamp can differ under
/// `LogAppendTime`).
fn same_record(planned: &BackupRecord, written: &BackupRecord) -> bool {
    planned.key == written.key
        && planned.value == written.value
        && planned.headers == written.headers
}

/// The unsaved tail of one partition being matched against the target.
pub(crate) struct TailVerifier {
    progress: PartitionRestoreProgress,
    router: Arc<PartitionLeaderRouter>,
    /// Records of the tail still to match.
    remaining: u64,
    /// Target offset of the next record to match.
    next_target_offset: i64,
    /// Records fetched from the target, not matched yet.
    fetched: VecDeque<BackupRecord>,
    /// Target offset the next fetch starts at.
    fetch_offset: i64,
}

impl TailVerifier {
    pub(crate) fn new(
        progress: PartitionRestoreProgress,
        router: Arc<PartitionLeaderRouter>,
        tail: u64,
    ) -> Self {
        info!(
            "Resuming {}[{}] -> {}[{}]: {} record(s) were acknowledged after the last checkpoint \
             save; verifying them against the target before continuing",
            progress.source_topic,
            progress.source_partition,
            progress.target_topic,
            progress.target_partition,
            tail
        );
        Self {
            next_target_offset: progress.target_hw_after_ack,
            fetch_offset: progress.target_hw_after_ack,
            progress,
            router,
            remaining: tail,
            fetched: VecDeque::new(),
        }
    }

    /// Records of the tail still to match.
    pub(crate) fn remaining(&self) -> u64 {
        self.remaining
    }

    /// Match the leading records of `planned` (the records this run would
    /// produce next) against the target. Returns how many are already
    /// written, with their target offsets starting at the returned base.
    pub(crate) async fn match_written(&mut self, planned: &[BackupRecord]) -> Result<(usize, i64)> {
        let count = (self.remaining as usize).min(planned.len());
        let base = self.next_target_offset;
        for (i, record) in planned[..count].iter().enumerate() {
            let written = self.next_written().await?;
            if written.offset != base + i as i64 || !same_record(record, &written) {
                return Err(another_writer(
                    &self.progress,
                    format!(
                        "the record at target offset {} is not the one the restore wrote there \
                         (source offset {})",
                        written.offset, record.offset
                    ),
                ));
            }
        }
        self.remaining -= count as u64;
        self.next_target_offset += count as i64;
        Ok((count, base))
    }

    /// Called when the backup has no more records for the partition.
    pub(crate) fn finish(&self) -> Result<()> {
        if self.remaining > 0 {
            return Err(another_writer(
                &self.progress,
                format!(
                    "the target has {} more record(s) than the backup has left to restore",
                    self.remaining
                ),
            ));
        }
        Ok(())
    }

    async fn next_written(&mut self) -> Result<BackupRecord> {
        loop {
            if let Some(record) = self.fetched.pop_front() {
                return Ok(record);
            }
            let offset = self.fetch_offset;
            let response = self
                .router
                .fetch(
                    &self.progress.target_topic,
                    self.progress.target_partition,
                    offset,
                    4 * 1024 * 1024,
                    IsolationLevel::ReadUncommitted,
                )
                .await?;
            // A fetch returns whole batches: drop records before `offset`.
            let records: Vec<BackupRecord> = response
                .records
                .into_iter()
                .filter(|r| r.offset >= offset)
                .collect();
            let Some(last) = records.last() else {
                return Err(another_writer(
                    &self.progress,
                    format!("no record could be read at target offset {offset}"),
                ));
            };
            self.fetch_offset = last.offset + 1;
            self.fetched.extend(records);
        }
    }
}

/// The in-doubt window of an interrupted run: requests it sent just before
/// it died may still be appended until about `produce_timeout_ms` after its
/// last activity, which was at most `checkpoint_interval_secs` after its
/// last save. Returns how long to wait before reading high watermarks.
pub(crate) fn in_doubt_wait(
    checkpoint: &RestoreCheckpoint,
    options: &RestoreOptions,
    now_ms: i64,
) -> std::time::Duration {
    if checkpoint.stopped_cleanly || checkpoint.partitions.is_empty() {
        return std::time::Duration::ZERO;
    }
    let settle_ms = options.checkpoint_interval_secs as i64 * 1000
        + i64::from(options.produce_timeout_ms.max(0))
        + 5_000;
    let until = checkpoint.last_checkpoint_time + settle_ms;
    if until > now_ms {
        let wait = std::time::Duration::from_millis((until - now_ms) as u64);
        warn!(
            "The previous restore run did not stop cleanly; waiting {:?} (checkpoint interval + \
             produce timeout + 5 s after its last checkpoint save) so requests it had in flight \
             cannot land after this run reads the target",
            wait
        );
        wait
    } else {
        std::time::Duration::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;

    fn progress(hw: i64) -> PartitionRestoreProgress {
        PartitionRestoreProgress {
            source_topic: "orders".to_string(),
            source_partition: 2,
            target_topic: "orders-restored".to_string(),
            target_partition: 2,
            last_source_offset_acked: 41,
            target_hw_after_ack: hw,
            records_acked: 42,
            bytes_acked: 4200,
        }
    }

    #[tokio::test]
    async fn atomic_write_replaces_the_file_and_leaves_no_temp_files() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("nested").join("restore-checkpoint.json");
        write_file_atomic(&path, b"first").await.unwrap();
        write_file_atomic(&path, b"second, longer content")
            .await
            .unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"second, longer content".to_vec()
        );
        let names: Vec<String> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(names, vec!["restore-checkpoint.json".to_string()]);
    }

    #[tokio::test]
    async fn atomic_write_keeps_the_old_file_when_the_write_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("cp.json");
        write_file_atomic(&path, b"good").await.unwrap();
        // Renaming a file over a directory fails: the old file must stay.
        let blocked = dir.path().join("dir-in-the-way");
        std::fs::create_dir(&blocked).unwrap();
        std::fs::write(blocked.join("x"), b"x").unwrap();
        assert!(write_file_atomic(&blocked, b"new").await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"good".to_vec());
        let leftovers = std::fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .contains(".tmp-")
            })
            .count();
        assert_eq!(leftovers, 0);
    }

    #[tokio::test]
    async fn store_saves_and_loads_local_remote_and_both() {
        let dir = tempfile::TempDir::new().unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let path = dir.path().join("cp.json");

        for mode in [
            RestoreCheckpointStorage::Local,
            RestoreCheckpointStorage::Remote,
            RestoreCheckpointStorage::Both,
        ] {
            let _ = std::fs::remove_file(&path);
            let options = RestoreOptions {
                checkpoint_state: Some(path.clone()),
                checkpoint_storage: mode,
                ..RestoreOptions::default()
            };
            let store = CheckpointStore::from_options(
                &options,
                storage.clone(),
                "bk",
                &format!("{mode:?}"),
            )
            .unwrap();
            assert!(
                store.load().await.unwrap().is_none() || mode == RestoreCheckpointStorage::Local
            );

            let mut cp = RestoreCheckpoint::new("bk".to_string(), "hash".to_string());
            cp.mark_segment_completed("bk/topics/orders/partition=0/segment-0.bin");
            cp.record_ack(&progress(0), 9, 10, 10, 100);
            let shared = Mutex::new(Some(cp));
            store.save(&shared).await.unwrap();

            assert_eq!(
                path.exists(),
                mode != RestoreCheckpointStorage::Remote,
                "{mode:?}"
            );
            let remote_key = remote_checkpoint_key("bk", &format!("{mode:?}"));
            assert_eq!(
                storage.exists(&remote_key).await.unwrap(),
                mode != RestoreCheckpointStorage::Local,
                "{mode:?}"
            );

            let loaded = store.load().await.unwrap().expect("saved checkpoint");
            assert_eq!(loaded.segments_completed.len(), 1);
            assert_eq!(loaded.records_restored, 10);
            let p = &loaded.partitions[&RestoreCheckpoint::partition_key("orders", 2)];
            assert_eq!(
                (p.last_source_offset_acked, p.target_hw_after_ack),
                (41, 10)
            );
        }

        // No local path and local storage: checkpointing is off.
        assert!(
            CheckpointStore::from_options(&RestoreOptions::default(), storage, "bk", "x").is_none()
        );
    }

    #[tokio::test]
    async fn the_newer_of_the_local_and_remote_copies_wins() {
        let dir = tempfile::TempDir::new().unwrap();
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        let options = RestoreOptions {
            checkpoint_state: Some(dir.path().join("cp.json")),
            checkpoint_storage: RestoreCheckpointStorage::Both,
            ..RestoreOptions::default()
        };
        let store = CheckpointStore::from_options(&options, storage.clone(), "bk", "r1").unwrap();
        let mut old = RestoreCheckpoint::new("bk".to_string(), "h".to_string());
        old.last_checkpoint_time = 1_000;
        old.records_restored = 1;
        let mut new = old.clone();
        new.last_checkpoint_time = 2_000;
        new.records_restored = 2;
        write_file_atomic(
            &dir.path().join("cp.json"),
            &serde_json::to_vec(&old).unwrap(),
        )
        .await
        .unwrap();
        storage
            .put(
                &remote_checkpoint_key("bk", "r1"),
                Bytes::from(serde_json::to_vec(&new).unwrap()),
            )
            .await
            .unwrap();
        assert_eq!(store.load().await.unwrap().unwrap().records_restored, 2);
    }

    #[test]
    fn checkpoint_without_the_new_fields_still_parses() {
        let json = r#"{"backup_id":"b","start_time":1,"last_checkpoint_time":2,
            "segments_completed":["s"],"segments_in_progress":[],"records_restored":0,
            "bytes_restored":0,"config_hash":"h"}"#;
        let cp: RestoreCheckpoint = serde_json::from_str(json).unwrap();
        assert!(cp.partitions.is_empty());
        assert!(!cp.stopped_cleanly);
        assert_eq!(cp.restore_id, None);
    }

    #[test]
    fn record_ack_tracks_partition_progress_and_totals() {
        let mut cp = RestoreCheckpoint::new("b".to_string(), "h".to_string());
        let start = PartitionRestoreProgress {
            last_source_offset_acked: -1,
            target_hw_after_ack: 500,
            records_acked: 0,
            bytes_acked: 0,
            ..progress(0)
        };
        cp.record_ack(&start, 99, 600, 100, 1_000);
        cp.record_ack(&start, 149, 650, 50, 500);
        let p = &cp.partitions[&RestoreCheckpoint::partition_key("orders", 2)];
        assert_eq!(p.last_source_offset_acked, 149);
        assert_eq!(p.target_hw_after_ack, 650);
        assert_eq!((p.records_acked, p.bytes_acked), (150, 1_500));
        assert_eq!((cp.records_restored, cp.bytes_restored), (150, 1_500));
    }

    #[test]
    fn unsaved_tail_is_the_high_watermark_advance() {
        assert_eq!(unsaved_tail(&progress(100), 100).unwrap(), 0);
        assert_eq!(unsaved_tail(&progress(100), 137).unwrap(), 37);
        let err = unsaved_tail(&progress(100), 90).unwrap_err().to_string();
        assert!(err.contains("below the 100"), "{err}");
    }

    #[test]
    fn in_doubt_wait_covers_an_interrupted_run_only() {
        let options = RestoreOptions {
            checkpoint_interval_secs: 10,
            produce_timeout_ms: 30_000,
            ..RestoreOptions::default()
        };
        let mut cp = RestoreCheckpoint::new("b".to_string(), "h".to_string());
        cp.last_checkpoint_time = 1_000_000;
        // No progress yet: nothing can be in flight.
        assert!(in_doubt_wait(&cp, &options, 1_000_000).is_zero());

        cp.record_ack(&progress(0), 1, 2, 2, 2);
        // Interval 10 s + timeout 30 s + 5 s margin after the last save.
        assert_eq!(
            in_doubt_wait(&cp, &options, 1_000_000),
            std::time::Duration::from_secs(45)
        );
        assert_eq!(
            in_doubt_wait(&cp, &options, 1_040_000),
            std::time::Duration::from_secs(5)
        );
        assert!(in_doubt_wait(&cp, &options, 1_045_000).is_zero());

        cp.stopped_cleanly = true;
        assert!(in_doubt_wait(&cp, &options, 1_000_000).is_zero());
    }

    #[test]
    fn default_restore_id_is_stable_and_distinguishes_targets() {
        let a = default_restore_id("bk", &["b:9092".into(), "a:9092".into()], "h");
        let b = default_restore_id("bk", &["a:9092".into(), "b:9092".into()], "h");
        assert_eq!(a, b, "server order does not matter");
        assert_eq!(a.len(), 16);
        assert_ne!(a, default_restore_id("bk", &["c:9092".into()], "h"));
        assert_ne!(
            a,
            default_restore_id("bk", &["a:9092".into(), "b:9092".into()], "h2")
        );
        assert_ne!(
            a,
            default_restore_id("bk2", &["a:9092".into(), "b:9092".into()], "h")
        );
    }

    #[test]
    fn same_record_compares_key_value_and_headers_not_timestamp() {
        let a = BackupRecord {
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers: vec![crate::manifest::RecordHeader {
                key: "h".to_string(),
                value: None,
            }],
            timestamp: 1,
            offset: 5,
        };
        let mut b = a.clone();
        b.timestamp = 99;
        b.offset = 1_000;
        assert!(same_record(&a, &b));
        b.headers[0].value = Some(vec![]);
        assert!(!same_record(&a, &b));
        let mut c = a.clone();
        c.value = None;
        assert!(!same_record(&a, &c));
    }
}
