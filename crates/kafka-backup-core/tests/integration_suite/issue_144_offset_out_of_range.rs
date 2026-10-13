//! Issue #144 — OFFSET_OUT_OF_RANGE recovery.
//!
//! When retention (or `DeleteRecords`) removes the offset a backup wants next
//! — a snapshot-captured `earliest` that aged out while the partition sat in
//! the concurrency queue, or a checkpoint that aged out between runs — the
//! broker rejects the Fetch with error code 1 and, before the fix, the whole
//! partition (and therefore the run) failed. The resume case was worse: it
//! never self-healed, because the checkpoint only advances on a successful
//! fetch.
//!
//! These tests drive the same fetch-loop path deterministically with the
//! `DeleteRecords` admin API instead of waiting on retention: back up, produce
//! more, delete past the checkpoint, back up again. The fix must (1) complete
//! the run, (2) resume from the broker's new log start offset, and (3) record
//! the skipped range in the manifest so the backup is not silently holed.
//!
//! These tests require Docker and use Testcontainers to run a real broker.

use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, OffsetStorageConfig, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::kafka::delete_records;
use kafka_backup_core::manifest::{BackupManifest, OffsetGapReason};
use kafka_backup_core::storage::StorageBackendConfig;
use kafka_backup_core::{OffsetStore, OffsetStoreConfig, SqliteOffsetStore};

use super::common::{generate_test_records, KafkaTestCluster};

const PARTITIONS: i32 = 3; // KafkaTestCluster::create_topic always creates 3

fn snapshot_backup_config(
    bootstrap_server: &str,
    storage_path: PathBuf,
    offset_db_path: PathBuf,
    backup_id: &str,
    topic: &str,
) -> Config {
    Config {
        mode: Mode::Backup,
        backup_id: backup_id.to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec![bootstrap_server.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: vec![topic.to_string()],
                exclude: vec![],
            },
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem { path: storage_path },
        backup: Some(BackupOptions {
            segment_max_bytes: 1024 * 1024,
            segment_max_interval_ms: 10000,
            compression: CompressionType::Zstd,
            // Snapshot mode is what issue #144 was reported against; it also
            // exercises the snapshot-progress accounting on the recovery path.
            stop_at_current_offsets: true,
            continuous: false,
            ..Default::default()
        }),
        restore: None,
        offset_storage: Some(OffsetStorageConfig {
            db_path: offset_db_path,
            ..Default::default()
        }),
        metrics: None,
    }
}

async fn run_backup(config: Config) -> kafka_backup_core::Result<()> {
    let engine = BackupEngine::new(config)
        .await
        .expect("Failed to create backup engine");
    tokio::time::timeout(Duration::from_secs(90), engine.run())
        .await
        .expect("Backup timed out")
}

fn read_manifest(storage_dir: &TempDir, backup_id: &str) -> BackupManifest {
    let path = storage_dir.path().join(backup_id).join("manifest.json");
    let content = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("Failed to read manifest {:?}: {}", path, e));
    serde_json::from_str(&content).expect("Failed to parse manifest")
}

async fn produce_round_robin(cluster: &KafkaTestCluster, topic: &str, count: usize) {
    let client = cluster.create_client();
    client.connect().await.expect("Failed to connect");
    for (i, record) in generate_test_records(count, topic).iter().enumerate() {
        client
            .produce(
                topic,
                (i as i32) % PARTITIONS,
                vec![record.clone()],
                -1,
                30_000,
            )
            .await
            .expect("Failed to produce");
    }
}

/// Resume path (the permanent-wedge case from the issue triage): the
/// checkpoint from run 1 has aged out of the log by the time run 2 starts.
///
/// Timeline per partition (3 partitions, records round-robined):
///   run 1 backs up offsets 0..=19        -> checkpoint 19
///   produce 20 more                      -> offsets 20..=39
///   DeleteRecords(30)                    -> log start offset 30
///   run 2 resumes at 20 -> OFFSET_OUT_OF_RANGE -> must skip to 30 and record
///   the gap [20, 30) instead of failing (and instead of wedging forever).
#[tokio::test]
#[ignore] // Requires Docker
async fn test_backup_recovers_from_offset_out_of_range_and_records_gap() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("Failed to start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let topic = "issue-144-retention-gap";
    let backup_id = "issue-144-backup";
    let per_partition_run1 = 20usize;

    cluster
        .create_topic(topic, per_partition_run1 * PARTITIONS as usize)
        .await
        .expect("Failed to create topic");
    sleep(Duration::from_secs(2)).await;

    let storage_dir = TempDir::new().expect("storage temp dir");
    let offset_dir = TempDir::new().expect("offset temp dir");
    let offset_db_path = offset_dir.path().join("offsets.db");

    let config = || {
        snapshot_backup_config(
            &cluster.bootstrap_servers,
            storage_dir.path().to_path_buf(),
            offset_db_path.clone(),
            backup_id,
            topic,
        )
    };

    // --- Run 1: baseline, everything present ---
    run_backup(config()).await.expect("Run 1 should succeed");
    let manifest1 = read_manifest(&storage_dir, backup_id);
    assert_eq!(
        manifest1.total_records(),
        (per_partition_run1 * PARTITIONS as usize) as i64,
        "run 1 should capture every record"
    );
    assert_eq!(manifest1.total_gaps(), 0, "run 1 must not record any gap");

    // --- Produce more, then delete past the checkpoint ---
    let per_partition_run2 = 20usize;
    produce_round_robin(&cluster, topic, per_partition_run2 * PARTITIONS as usize).await;
    sleep(Duration::from_secs(1)).await;

    let checkpoint = per_partition_run1 as i64 - 1; // 19
    let new_log_start = 30i64; // > checkpoint + 1, < high watermark (40)
    let client = cluster.create_client();
    client.connect().await.expect("Failed to connect");
    let targets: Vec<(i32, i64)> = (0..PARTITIONS).map(|p| (p, new_log_start)).collect();
    delete_records(&client, topic, &targets, 30_000)
        .await
        .expect("DeleteRecords should succeed");

    for p in 0..PARTITIONS {
        let (earliest, latest) = client
            .get_offsets(topic, p, kafka_backup_core::IsolationLevel::ReadUncommitted)
            .await
            .expect("get_offsets");
        assert_eq!(earliest, new_log_start, "partition {p}: log start offset");
        assert_eq!(
            latest,
            (per_partition_run1 + per_partition_run2) as i64,
            "partition {p}: high watermark"
        );
    }

    // --- Run 2: resumes from checkpoint 19 -> offset 20 is gone ---
    // Before the fix this failed every partition with
    // "Broker returned error code 1: Fetch error for <topic>:<p>: code 1"
    // and kept failing on every retry because the checkpoint never moved.
    run_backup(config())
        .await
        .expect("Run 2 must recover from OFFSET_OUT_OF_RANGE instead of failing");

    let manifest2 = read_manifest(&storage_dir, backup_id);
    let topic_backup = manifest2
        .topics
        .iter()
        .find(|t| t.name == topic)
        .expect("topic in manifest");
    assert_eq!(topic_backup.partitions.len(), PARTITIONS as usize);

    for partition in &topic_backup.partitions {
        let p = partition.partition_id;

        // Exactly one gap, with the exact bounds: [checkpoint + 1, new log start).
        assert_eq!(
            partition.gaps.len(),
            1,
            "partition {p}: expected exactly one recorded gap, got {:?}",
            partition.gaps
        );
        let gap = &partition.gaps[0];
        assert_eq!(gap.start_offset, checkpoint + 1, "partition {p}: gap start");
        assert_eq!(gap.end_offset, new_log_start, "partition {p}: gap end");
        assert_eq!(gap.reason, OffsetGapReason::OffsetOutOfRange);
        assert_eq!(gap.offset_span(), new_log_start - (checkpoint + 1));
        assert!(gap.detected_at > 0, "partition {p}: detected_at is set");

        // No segment claims to hold data from inside the gap.
        for seg in &partition.segments {
            assert!(
                seg.end_offset < gap.start_offset || seg.start_offset >= gap.end_offset,
                "partition {p}: segment {}..{} overlaps recorded gap {}..{}",
                seg.start_offset,
                seg.end_offset,
                gap.start_offset,
                gap.end_offset
            );
        }

        // Run 2 resumed exactly at the new log start offset and read to the end.
        assert!(
            partition
                .segments
                .iter()
                .any(|s| s.start_offset == new_log_start),
            "partition {p}: a segment should start at the new log start offset {new_log_start}; segments: {:?}",
            partition
                .segments
                .iter()
                .map(|s| (s.start_offset, s.end_offset))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            partition.last_offset(),
            Some((per_partition_run1 + per_partition_run2) as i64 - 1),
            "partition {p}: backed up to the high watermark"
        );
    }

    // Records: run 1 (0..=19) + run 2 (30..=39) per partition; 20..=29 lost.
    let expected_records = ((per_partition_run1 as i64) + (40 - new_log_start)) * PARTITIONS as i64;
    assert_eq!(manifest2.total_records(), expected_records);
    assert_eq!(manifest2.total_gaps(), PARTITIONS as usize);

    // The checkpoint advanced past the gap — the run is no longer wedged.
    let store = SqliteOffsetStore::new(OffsetStoreConfig {
        db_path: offset_db_path.clone(),
        ..Default::default()
    })
    .await
    .expect("open offset store");
    for p in 0..PARTITIONS {
        let saved = store
            .get_offset(backup_id, topic, p)
            .await
            .expect("get_offset")
            .expect("checkpoint present");
        assert_eq!(
            saved,
            (per_partition_run1 + per_partition_run2) as i64 - 1,
            "partition {p}: checkpoint should be at the high watermark after recovery"
        );
    }
    drop(store);

    // --- Run 3: nothing new. Must succeed and must not duplicate or lose
    // the recorded gaps when the manifest is merged again. ---
    run_backup(config()).await.expect("Run 3 should succeed");
    let manifest3 = read_manifest(&storage_dir, backup_id);
    assert_eq!(manifest3.total_records(), expected_records);
    assert_eq!(
        manifest3.total_gaps(),
        PARTITIONS as usize,
        "gaps must survive a further manifest merge without duplication"
    );
    for (t, p, gap) in manifest3.gaps() {
        assert_eq!(t, topic);
        assert!((0..PARTITIONS).contains(&p));
        assert_eq!(
            (gap.start_offset, gap.end_offset),
            (checkpoint + 1, new_log_start)
        );
    }
}

/// A backup whose data is fully intact must not grow a `gaps` field: the
/// manifest format for the common case is unchanged.
#[tokio::test]
#[ignore] // Requires Docker
async fn test_backup_without_data_loss_records_no_gaps() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("Failed to start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let topic = "issue-144-no-gap";
    cluster
        .create_topic(topic, 30)
        .await
        .expect("Failed to create topic");
    sleep(Duration::from_secs(2)).await;

    let storage_dir = TempDir::new().expect("storage temp dir");
    let offset_dir = TempDir::new().expect("offset temp dir");
    let config = snapshot_backup_config(
        &cluster.bootstrap_servers,
        storage_dir.path().to_path_buf(),
        offset_dir.path().join("offsets.db"),
        "issue-144-clean",
        topic,
    );
    run_backup(config).await.expect("backup should succeed");

    let raw = std::fs::read_to_string(
        storage_dir
            .path()
            .join("issue-144-clean")
            .join("manifest.json"),
    )
    .expect("read manifest");
    assert!(
        !raw.contains("\"gaps\""),
        "a clean backup must not serialise a gaps field: {raw}"
    );
    let manifest: BackupManifest = serde_json::from_str(&raw).expect("parse manifest");
    assert_eq!(manifest.total_gaps(), 0);
    assert_eq!(manifest.total_records(), 30);
}
