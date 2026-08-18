//! Issue #148 — consumer group offsets must actually be applied after a restore
//! that asks for it (`reset_consumer_offsets` / `auto_consumer_groups`), and the
//! layering must hold: the restore *engine* (Phase 2) never commits offsets;
//! `ThreePhaseRestore::run_offset_reset_phase` (Phase 3) does — exactly once —
//! from the mapping the engine returns. `kafka-backup restore` and
//! `three-phase-restore` both go through that one method.
//!
//! Single-broker Testcontainers cluster: the "source" and "target" are the same
//! broker, so the restore maps `orders → orders-restored` and Phase 3 commits
//! the group's offsets against the restored topic. The source topic's offsets
//! must stay untouched.
//!
//! These tests require Docker.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::sleep;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, RestoreOptions, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::kafka::{fetch_offsets, KafkaClient, PartitionLeaderRouter};
use kafka_backup_core::restore::{RestoreEngine, ThreePhaseRestore};
use kafka_backup_core::storage::StorageBackendConfig;

use super::common::{create_temp_storage, KafkaTestCluster};

const SOURCE_TOPIC: &str = "issue-148-orders";
const TARGET_TOPIC: &str = "issue-148-orders-restored";
const GROUP: &str = "issue-148-app";
const BACKUP_ID: &str = "issue-148-backup";
/// Committed source offsets per partition (3 partitions, 20 records each).
const SOURCE_COMMITS: [(i32, i64); 3] = [(0, 5), (1, 12), (2, 20)];

fn backup_config(bs: &str, storage: PathBuf) -> Config {
    Config {
        mode: Mode::Backup,
        backup_id: BACKUP_ID.to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec![bs.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: vec![SOURCE_TOPIC.to_string()],
                exclude: vec![],
            },
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem { path: storage },
        backup: Some(BackupOptions {
            segment_max_bytes: 1024 * 1024,
            segment_max_interval_ms: 5000,
            compression: CompressionType::Zstd,
            stop_at_current_offsets: true,
            continuous: false,
            include_offset_headers: true,
            consumer_group_snapshot: true,
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
    }
}

fn restore_config(bs: &str, storage: PathBuf, auto_consumer_groups: bool) -> Config {
    let mut topic_mapping = HashMap::new();
    topic_mapping.insert(SOURCE_TOPIC.to_string(), TARGET_TOPIC.to_string());
    Config {
        mode: Mode::Restore,
        backup_id: BACKUP_ID.to_string(),
        source: None,
        target: Some(KafkaConfig {
            bootstrap_servers: vec![bs.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection::default(),
            connection: Default::default(),
        }),
        storage: StorageBackendConfig::Filesystem { path: storage },
        backup: None,
        restore: Some(RestoreOptions {
            topic_mapping,
            create_topics: true,
            auto_consumer_groups,
            ..RestoreOptions::default()
        }),
        offset_storage: None,
        metrics: None,
    }
}

/// Seed committed offsets for `GROUP` through the coordinator-aware router
/// path (retries the transient NOT_COORDINATOR / COORDINATOR_LOAD_IN_PROGRESS
/// a fresh broker returns while `__consumer_offsets` is being created).
async fn seed_group_offsets(bs: &str, offsets: &[(String, i32, i64, Option<String>)]) {
    let router = PartitionLeaderRouter::new(KafkaConfig {
        bootstrap_servers: vec![bs.to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection::default(),
        connection: Default::default(),
    })
    .await
    .expect("router");
    for attempt in 1..=10 {
        let results = router
            .commit_group_offsets(GROUP, offsets)
            .await
            .expect("commit_group_offsets");
        if results.iter().all(|(_, _, code)| *code == 0) {
            return;
        }
        if attempt == 10 {
            panic!("seeding group offsets failed: {results:?}");
        }
        sleep(Duration::from_secs(1)).await;
    }
}

/// Committed offsets of `GROUP` on `topic`, keyed by partition; partitions
/// without a commit are reported by the broker as -1 and omitted here.
async fn committed(client: &KafkaClient, topic: &str) -> HashMap<i32, i64> {
    // `None` = every topic the group has offsets for (an explicit topic list
    // with no partition indexes returns nothing from the broker).
    fetch_offsets(client, GROUP, None)
        .await
        .expect("fetch_offsets")
        .into_iter()
        .filter(|o| o.topic == topic && o.offset >= 0)
        .map(|o| (o.partition, o.offset))
        .collect()
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_restore_applies_consumer_group_offsets_in_phase_3_exactly_once() {
    let cluster = KafkaTestCluster::start().await.expect("start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka ready");
    let bs = cluster.bootstrap_servers.clone();

    // Source topic: 3 partitions × 20 records, and a group with committed offsets.
    cluster
        .create_topic(SOURCE_TOPIC, 60)
        .await
        .expect("create source topic");
    let client = cluster.create_client();
    client.connect().await.expect("connect");
    let commits: Vec<(String, i32, i64, Option<String>)> = SOURCE_COMMITS
        .iter()
        .map(|(p, o)| (SOURCE_TOPIC.to_string(), *p, *o, None))
        .collect();
    seed_group_offsets(&bs, &commits).await;
    sleep(Duration::from_secs(1)).await;
    let source_before = committed(&client, SOURCE_TOPIC).await;
    assert_eq!(source_before.len(), 3, "seeded offsets: {source_before:?}");

    // Backup with the consumer-groups snapshot.
    let storage = create_temp_storage();
    let engine = BackupEngine::new(backup_config(&bs, storage.path().to_path_buf()))
        .await
        .expect("backup engine");
    tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup");
    assert!(
        storage
            .path()
            .join(BACKUP_ID)
            .join("consumer-groups-snapshot.json")
            .exists(),
        "backup must write the consumer-groups snapshot"
    );

    // Phase 2 only: the engine restores data and builds the mapping — and must
    // NOT touch consumer offsets, even with auto_consumer_groups set.
    let config = restore_config(&bs, storage.path().to_path_buf(), true);
    let report = tokio::time::timeout(
        Duration::from_secs(60),
        RestoreEngine::new(config.clone())
            .expect("restore engine")
            .run(),
    )
    .await
    .expect("restore timed out")
    .expect("restore");
    assert_eq!(report.records_restored, 60);
    assert_eq!(
        report.resolved_consumer_groups,
        vec![GROUP.to_string()],
        "engine resolves the group from the snapshot"
    );
    sleep(Duration::from_secs(1)).await;
    assert!(
        committed(&client, TARGET_TOPIC).await.is_empty(),
        "Phase 2 must not commit consumer offsets on the target topic"
    );

    // Phase 3: apply. This is what `restore` and `three-phase-restore` run.
    let orchestrator = ThreePhaseRestore::new(config).expect("orchestrator");
    assert!(ThreePhaseRestore::wants_offset_reset(
        &orchestrator_restore_options(&bs, storage.path().to_path_buf())
    ));
    let outcome = orchestrator
        .run_offset_reset_phase(&report)
        .await
        .expect("phase 3");
    assert!(outcome.applied(), "offsets should have been applied");
    assert!(outcome.success(), "phase 3 errors: {:?}", outcome.report);
    let phase3 = outcome.report.as_ref().unwrap();
    assert_eq!(phase3.partitions_reset, 3);
    assert!(phase3.errors.is_empty(), "{:?}", phase3.errors);
    sleep(Duration::from_secs(1)).await;

    // The target topic was created empty, so translated offsets equal the
    // source offsets; the source topic's commits are untouched.
    let expected: HashMap<i32, i64> = SOURCE_COMMITS.iter().copied().collect();
    assert_eq!(committed(&client, TARGET_TOPIC).await, expected);
    assert_eq!(committed(&client, SOURCE_TOPIC).await, source_before);

    // Applying the same plan again is idempotent (same offsets, still success).
    let again = orchestrator
        .run_offset_reset_phase(&report)
        .await
        .expect("phase 3 again");
    assert!(again.success());
    assert_eq!(again.report.unwrap().partitions_reset, 3);
    sleep(Duration::from_secs(1)).await;
    assert_eq!(committed(&client, TARGET_TOPIC).await, expected);
}

/// Without either flag, a restore neither resolves groups nor touches offsets,
/// and Phase 3 is a no-op — the documented opt-in holds.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_restore_without_reset_flags_leaves_consumer_offsets_alone() {
    let cluster = KafkaTestCluster::start().await.expect("start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka ready");
    let bs = cluster.bootstrap_servers.clone();

    cluster
        .create_topic(SOURCE_TOPIC, 30)
        .await
        .expect("create source topic");
    let client = cluster.create_client();
    client.connect().await.expect("connect");
    seed_group_offsets(&bs, &[(SOURCE_TOPIC.to_string(), 0, 3, None)]).await;
    sleep(Duration::from_secs(1)).await;
    assert_eq!(committed(&client, SOURCE_TOPIC).await.get(&0), Some(&3));

    let storage = create_temp_storage();
    let engine = BackupEngine::new(backup_config(&bs, storage.path().to_path_buf()))
        .await
        .expect("backup engine");
    tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup");

    let config = restore_config(&bs, storage.path().to_path_buf(), false);
    let report = tokio::time::timeout(
        Duration::from_secs(60),
        RestoreEngine::new(config.clone())
            .expect("restore engine")
            .run(),
    )
    .await
    .expect("restore timed out")
    .expect("restore");
    assert!(report.resolved_consumer_groups.is_empty());

    let outcome = ThreePhaseRestore::new(config)
        .expect("orchestrator")
        .run_offset_reset_phase(&report)
        .await
        .expect("phase 3");
    assert!(!outcome.applied());
    assert!(outcome.plan.is_none());
    sleep(Duration::from_secs(1)).await;
    assert!(committed(&client, TARGET_TOPIC).await.is_empty());
}

fn orchestrator_restore_options(bs: &str, storage: PathBuf) -> RestoreOptions {
    restore_config(bs, storage, true).restore.unwrap()
}
