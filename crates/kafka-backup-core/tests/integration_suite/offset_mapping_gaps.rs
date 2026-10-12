//! Consumer groups resume on the right record after a restore whose source
//! offsets have gaps.
//!
//! A committed offset `S` means "the next record to read is `S`". After the
//! restore it must translate to the target offset of the first restored record
//! at or after `S`, or to the target's log end when the group had consumed
//! every restored record. Translating it as "nearest lower restored record +
//! (S - its source offset)" overshoots after any gap and the group skips
//! records it never consumed (or is committed past the log end).
//!
//! Single-broker Testcontainers cluster (random host port): a one-partition
//! topic of 20 records, a record filter that drops source offsets 4..=7 and the
//! tail 17..=19, and several groups committed around those gaps. Phase 3 runs
//! against the real broker and each group is then read back from the target.
//!
//! Requires Docker.

use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, RestoreOptions, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::kafka::{fetch_offsets, KafkaClient, PartitionLeaderRouter, TopicToCreate};
use kafka_backup_core::manifest::BackupRecord;
use kafka_backup_core::offset_headers::X_ORIGINAL_OFFSET;
use kafka_backup_core::restore::filter::{FilterAction, RecordFilter, RecordFilterHandle};
use kafka_backup_core::restore::{RestoreEngine, ThreePhaseRestore};
use kafka_backup_core::storage::StorageBackendConfig;

use super::common::{create_temp_storage, generate_test_records, KafkaTestCluster};

const SOURCE_TOPIC: &str = "o3-gaps";
const TARGET_TOPIC: &str = "o3-gaps-restored";
const BACKUP_ID: &str = "o3-gaps-backup";
const RECORDS: i64 = 20;

/// Source offsets the restore drops: a run in the middle and the tail.
fn dropped() -> BTreeSet<i64> {
    (4..=7).chain(17..=19).collect()
}

/// (group, committed source offset).
const GROUPS: &[(&str, i64)] = &[
    ("o3-before-gap", 2),
    ("o3-gap-start", 4),
    ("o3-inside-gap", 5),
    ("o3-right-after-gap", 8),
    ("o3-inside-tail-drop", 18),
    ("o3-at-high-watermark", 20),
];

struct DropOffsets(BTreeSet<i64>);

impl RecordFilter for DropOffsets {
    fn name(&self) -> &str {
        "o3-drop-offsets"
    }
    fn evaluate(&self, _topic: &str, record: &BackupRecord) -> FilterAction {
        if self.0.contains(&record.offset) {
            FilterAction::Drop
        } else {
            FilterAction::Keep
        }
    }
}

fn kafka(bs: &str) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![bs.to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection {
            include: vec![SOURCE_TOPIC.to_string()],
            exclude: vec![],
        },
        connection: Default::default(),
    }
}

fn backup_config(bs: &str, storage: PathBuf) -> Config {
    Config {
        mode: Mode::Backup,
        backup_id: BACKUP_ID.to_string(),
        source: Some(kafka(bs)),
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

fn restore_config(bs: &str, storage: PathBuf) -> Config {
    let mut topic_mapping = HashMap::new();
    topic_mapping.insert(SOURCE_TOPIC.to_string(), TARGET_TOPIC.to_string());
    let mut target = kafka(bs);
    target.topics = TopicSelection::default();
    Config {
        mode: Mode::Restore,
        backup_id: BACKUP_ID.to_string(),
        source: None,
        target: Some(target),
        storage: StorageBackendConfig::Filesystem { path: storage },
        backup: None,
        restore: Some(RestoreOptions {
            topic_mapping,
            create_topics: true,
            auto_consumer_groups: true,
            record_filter: Some(RecordFilterHandle::new(Arc::new(DropOffsets(dropped())))),
            ..RestoreOptions::default()
        }),
        offset_storage: None,
        metrics: None,
    }
}

async fn seed_group(router: &PartitionLeaderRouter, group: &str, offset: i64) {
    let offsets = vec![(SOURCE_TOPIC.to_string(), 0, offset, None)];
    for attempt in 1..=10 {
        let results = router
            .commit_group_offsets(group, &offsets)
            .await
            .expect("commit_group_offsets");
        if results.iter().all(|(_, _, code)| *code == 0) {
            return;
        }
        if attempt == 10 {
            panic!("seeding {group} failed: {results:?}");
        }
        sleep(Duration::from_secs(1)).await;
    }
}

async fn committed_on_target(client: &KafkaClient, group: &str) -> Option<i64> {
    fetch_offsets(client, group, None)
        .await
        .expect("fetch_offsets")
        .into_iter()
        .find(|o| o.topic == TARGET_TOPIC && o.partition == 0 && o.offset >= 0)
        .map(|o| o.offset)
}

fn original_offset(record: &BackupRecord) -> i64 {
    // x-original-offset is a little-endian i64 (offset_headers.rs).
    let header = record
        .headers
        .iter()
        .find(|h| h.key == X_ORIGINAL_OFFSET)
        .expect("restored record carries x-original-offset");
    let bytes: [u8; 8] = header.value.as_deref().unwrap().try_into().unwrap();
    let offset = i64::from_le_bytes(bytes);
    // Cross-check against the payload ("key-{source offset}").
    let key = String::from_utf8(record.key.clone().unwrap()).unwrap();
    assert_eq!(key, format!("key-{offset}"));
    offset
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn groups_resume_on_the_next_unconsumed_record_across_dropped_records() {
    let cluster = KafkaTestCluster::start().await.expect("start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka ready");
    let bs = cluster.bootstrap_servers.clone();
    let client = cluster.create_client();
    client.connect().await.expect("connect");

    // One partition, offsets 0..20.
    let created = client
        .create_topics(
            vec![TopicToCreate {
                name: SOURCE_TOPIC.to_string(),
                num_partitions: 1,
                replication_factor: 1,
            }],
            30_000,
        )
        .await
        .expect("create topic");
    assert!(created.iter().all(|r| r.is_success_or_exists()));
    sleep(Duration::from_secs(2)).await;
    client
        .produce(
            SOURCE_TOPIC,
            0,
            generate_test_records(RECORDS as usize, SOURCE_TOPIC),
            -1,
            30_000,
        )
        .await
        .expect("produce");
    assert_eq!(
        client.get_offsets(SOURCE_TOPIC, 0).await.expect("offsets"),
        (0, RECORDS)
    );

    let router = PartitionLeaderRouter::new(kafka(&bs))
        .await
        .expect("router");
    for (group, offset) in GROUPS {
        seed_group(&router, group, *offset).await;
    }
    sleep(Duration::from_secs(1)).await;

    // Backup with the consumer-group snapshot.
    let storage = create_temp_storage();
    let engine = BackupEngine::new(backup_config(&bs, storage.path().to_path_buf()))
        .await
        .expect("backup engine");
    tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup");

    // Phase 2: restore through the dropping filter.
    let config = restore_config(&bs, storage.path().to_path_buf());
    let report = tokio::time::timeout(
        Duration::from_secs(60),
        RestoreEngine::new(config.clone())
            .expect("restore engine")
            .run(),
    )
    .await
    .expect("restore timed out")
    .expect("restore");
    let survivors: Vec<i64> = (0..RECORDS).filter(|o| !dropped().contains(o)).collect();
    assert_eq!(report.records_restored, survivors.len() as u64);
    assert_eq!(report.records_dropped_by_filter, dropped().len() as u64);
    for (group, _) in GROUPS {
        assert!(
            report.resolved_consumer_groups.contains(&group.to_string()),
            "{group} resolved from the snapshot: {:?}",
            report.resolved_consumer_groups
        );
    }

    // Phase 3 against the broker.
    let outcome = ThreePhaseRestore::new(config)
        .expect("orchestrator")
        .run_offset_reset_phase(&report)
        .await
        .expect("phase 3");
    assert!(outcome.success(), "phase 3: {:?}", outcome.report);
    let plan = outcome.plan.as_ref().expect("plan");
    assert!(plan
        .groups
        .iter()
        .flat_map(|g| &g.partitions)
        .all(|p| !p.approximate));
    sleep(Duration::from_secs(1)).await;

    // The restored log, read back: target offset -> original source offset.
    let (_, target_end) = client.get_offsets(TARGET_TOPIC, 0).await.expect("offsets");
    assert_eq!(target_end, survivors.len() as i64);
    let restored = client
        .fetch(TARGET_TOPIC, 0, 0, 1024 * 1024)
        .await
        .expect("fetch target");
    let target_log: Vec<(i64, i64)> = restored
        .records
        .iter()
        .map(|r| (r.offset, original_offset(r)))
        .collect();
    assert_eq!(
        target_log.iter().map(|&(_, s)| s).collect::<Vec<_>>(),
        survivors
    );

    for (group, committed_source) in GROUPS {
        let committed = committed_on_target(&client, group)
            .await
            .unwrap_or_else(|| panic!("{group} has no commit on {TARGET_TOPIC}"));
        assert!(
            committed <= target_end,
            "{group}: committed {committed} past the target log end {target_end}"
        );
        let first_read = target_log
            .iter()
            .find(|(t, _)| *t >= committed)
            .map(|&(_, s)| s);
        let expected = survivors.iter().copied().find(|s| s >= committed_source);
        assert_eq!(
            first_read, expected,
            "{group} (committed at source {committed_source}, target {committed})"
        );
    }
}
