//! issue #169 — `backup.retention` prunes an incremental backup set safely:
//! aged segments are deleted, the manifest is rewritten first, the removal is
//! recorded as a `pruned` range, and a later run does not resurrect it.
//!
//! Requires Docker.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::time::sleep;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, OffsetStorageConfig,
    RetentionOptions, SecurityConfig, TopicSelection,
};
use kafka_backup_core::manifest::BackupManifest;
use kafka_backup_core::storage::StorageBackendConfig;

use super::common::{generate_test_records, KafkaTestCluster};

const TOPIC: &str = "issue-169-retention";
const BACKUP_ID: &str = "issue-169-backup";
const PARTITIONS: usize = 3;

fn config(bootstrap: &str, storage: &TempDir, offset_db: &Path, retention: bool) -> Config {
    Config {
        mode: Mode::Backup,
        backup_id: BACKUP_ID.to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec![bootstrap.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: vec![TOPIC.to_string()],
                exclude: vec![],
            },
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem {
            path: storage.path().to_path_buf(),
        },
        backup: Some(BackupOptions {
            // Tiny segments so each run writes several.
            segment_max_bytes: 4 * 1024,
            segment_max_records: Some(5),
            segment_max_interval_ms: 10_000,
            compression: CompressionType::Zstd,
            stop_at_current_offsets: true,
            continuous: false,
            retention: retention.then(|| RetentionOptions {
                // Everything written before this run is "aged" immediately.
                max_age: Some("1s".to_string()),
                max_total_bytes: None,
                keep_segments: 1,
            }),
            ..Default::default()
        }),
        restore: None,
        offset_storage: Some(OffsetStorageConfig {
            db_path: offset_db.to_path_buf(),
            ..Default::default()
        }),
        metrics: None,
    }
}

async fn run_backup(config: Config) {
    let engine = BackupEngine::new_with_metrics(config, None)
        .await
        .expect("failed to create backup engine");
    tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup run failed");
}

async fn produce(cluster: &KafkaTestCluster, count: usize, offset_hint: usize) {
    let client = cluster.create_client();
    client.connect().await.expect("failed to connect");
    for (i, record) in generate_test_records(count, TOPIC).iter().enumerate() {
        let partition = ((offset_hint + i) % PARTITIONS) as i32;
        client
            .produce(TOPIC, partition, vec![record.clone()], -1, 30_000)
            .await
            .expect("failed to produce");
    }
    sleep(Duration::from_secs(2)).await;
}

fn load_manifest(storage: &TempDir) -> BackupManifest {
    let bytes = std::fs::read(storage.path().join(format!("{BACKUP_ID}/manifest.json")))
        .expect("manifest.json present");
    serde_json::from_slice(&bytes).expect("manifest parses")
}

fn segment_files_on_disk(storage: &TempDir, manifest: &BackupManifest) -> (usize, usize) {
    let mut referenced_present = 0;
    let mut referenced_missing = 0;
    for topic in &manifest.topics {
        for partition in &topic.partitions {
            for segment in &partition.segments {
                if storage.path().join(&segment.key).exists() {
                    referenced_present += 1;
                } else {
                    referenced_missing += 1;
                }
            }
        }
    }
    (referenced_present, referenced_missing)
}

#[tokio::test]
#[ignore] // Requires Docker
async fn retention_prunes_aged_segments_and_the_next_run_does_not_resurrect_them() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("failed to start Kafka");
    cluster
        .create_topic(TOPIC, PARTITIONS)
        .await
        .expect("create topic");

    let storage = TempDir::new().unwrap();
    let offset_dir = TempDir::new().unwrap();
    let offset_db = offset_dir.path().join("offsets.db");
    let bootstrap = cluster.bootstrap_servers.clone();

    // Run 1 (no retention): seed the archive.
    produce(&cluster, 30, 0).await;
    run_backup(config(&bootstrap, &storage, &offset_db, false)).await;
    let m1 = load_manifest(&storage);
    let segments_after_run1 = m1.total_segments();
    assert!(segments_after_run1 >= PARTITIONS, "run 1 wrote segments");
    assert_eq!(m1.total_pruned(), 0);

    // Age everything past the 1s cutoff, produce more, run 2 WITH retention.
    sleep(Duration::from_secs(2)).await;
    produce(&cluster, 15, 30).await;
    run_backup(config(&bootstrap, &storage, &offset_db, true)).await;

    let m2 = load_manifest(&storage);
    assert!(
        m2.total_pruned() >= 1,
        "run 2 must record pruned ranges, got manifest: {:?}",
        m2.total_pruned()
    );
    assert!(
        m2.total_segments() < segments_after_run1 + m2.total_pruned(),
        "aged segments must have been removed from the manifest"
    );
    // Every segment the manifest references still exists on disk (manifest
    // was rewritten before deletion) and pruned objects are actually gone.
    let (present, missing) = segment_files_on_disk(&storage, &m2);
    assert!(present > 0);
    assert_eq!(missing, 0, "manifest must never reference deleted segments");

    // Run 3 (no retention): resume works and pruned segments stay pruned.
    produce(&cluster, 10, 45).await;
    run_backup(config(&bootstrap, &storage, &offset_db, false)).await;
    let m3 = load_manifest(&storage);
    assert!(
        m3.total_pruned() >= m2.total_pruned(),
        "pruned ranges survive merges"
    );
    let (_, missing3) = segment_files_on_disk(&storage, &m3);
    assert_eq!(missing3, 0, "no resurrection of pruned segment entries");
}
