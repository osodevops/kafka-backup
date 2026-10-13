//! Read-committed backup of transactional topics (O5 / F3).
//!
//! Before 0.24 a backup read uncommitted: records of aborted transactions and
//! every transaction marker were archived, and a restore wrote the markers
//! back as ordinary records (key `00 00 00 0x`, value `00 00 00 00 00 00`).
//! Consumer groups committed at or after a marker resumed on an aborted
//! record. The default is now `read_committed`: the restored topic equals
//! what a `read_committed` consumer of the source sees, a backup stops at
//! the last stable offset, and `read_uncommitted` keeps the old behaviour.
//!
//! Single-broker Testcontainers cluster (random host port) with the
//! transaction state log at replication factor 1; transactions are written
//! by the test-support producer in `transactional_producer.rs`.
//!
//! Requires Docker.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, IsolationLevel, KafkaConfig, Mode, RestoreOptions,
    SecurityConfig, TopicSelection,
};
use kafka_backup_core::kafka::{fetch_offsets, KafkaClient, PartitionLeaderRouter, TopicToCreate};
use kafka_backup_core::manifest::{BackupManifest, BackupRecord};
use kafka_backup_core::metrics::PrometheusMetrics;
use kafka_backup_core::offset_headers::X_ORIGINAL_OFFSET;
use kafka_backup_core::restore::{RestoreEngine, ThreePhaseRestore};
use kafka_backup_core::storage::StorageBackendConfig;

use super::common::{create_temp_storage, KafkaTestCluster};
use super::transactional_producer::{wait_for_offsets, TransactionalProducer};

/// Long enough that no transaction times out while a test runs.
const TXN_TIMEOUT_MS: i32 = 120_000;

fn kafka(bs: &str, topic: &str) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![bs.to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection {
            include: vec![topic.to_string()],
            exclude: vec![],
        },
        connection: Default::default(),
    }
}

fn backup_config(
    bs: &str,
    storage: PathBuf,
    backup_id: &str,
    topic: &str,
    opts: BackupOptions,
) -> Config {
    Config {
        mode: Mode::Backup,
        backup_id: backup_id.to_string(),
        source: Some(kafka(bs, topic)),
        target: None,
        storage: StorageBackendConfig::Filesystem { path: storage },
        backup: Some(opts),
        restore: None,
        offset_storage: None,
        metrics: None,
    }
}

/// Defaults (so `isolation_level` is whatever the default is) with small
/// segments.
fn default_backup_options() -> BackupOptions {
    BackupOptions {
        segment_max_bytes: 1024 * 1024,
        segment_max_interval_ms: 5000,
        compression: CompressionType::Zstd,
        ..Default::default()
    }
}

fn restore_config(
    bs: &str,
    storage: PathBuf,
    backup_id: &str,
    topic: &str,
    target: &str,
) -> Config {
    let mut topic_mapping = HashMap::new();
    topic_mapping.insert(topic.to_string(), target.to_string());
    let mut target_cfg = kafka(bs, topic);
    target_cfg.topics = TopicSelection::default();
    Config {
        mode: Mode::Restore,
        backup_id: backup_id.to_string(),
        source: None,
        target: Some(target_cfg),
        storage: StorageBackendConfig::Filesystem { path: storage },
        backup: None,
        restore: Some(RestoreOptions {
            topic_mapping,
            create_topics: true,
            auto_consumer_groups: true,
            ..RestoreOptions::default()
        }),
        offset_storage: None,
        metrics: None,
    }
}

async fn create_topic(client: &KafkaClient, topic: &str) {
    let created = client
        .create_topics(
            vec![TopicToCreate {
                name: topic.to_string(),
                num_partitions: 1,
                replication_factor: 1,
            }],
            30_000,
        )
        .await
        .expect("create topic");
    assert!(
        created.iter().all(|r| r.is_success_or_exists()),
        "{created:?}"
    );
    sleep(Duration::from_secs(1)).await;
}

async fn produce_plain(client: &KafkaClient, topic: &str, key: &str, value: &str) {
    client
        .produce(
            topic,
            0,
            vec![BackupRecord {
                key: Some(key.as_bytes().to_vec()),
                value: Some(value.as_bytes().to_vec()),
                headers: vec![],
                timestamp: chrono::Utc::now().timestamp_millis(),
                offset: 0,
            }],
            -1,
            30_000,
        )
        .await
        .expect("plain produce");
}

/// Every record in `topic[0]` from offset 0, read as Kafka Backup read
/// before 0.24 (read_uncommitted: markers come back as records).
async fn read_raw(client: &KafkaClient, topic: &str) -> Vec<BackupRecord> {
    read_all(client, topic, IsolationLevel::ReadUncommitted).await
}

async fn read_all(
    client: &KafkaClient,
    topic: &str,
    isolation: IsolationLevel,
) -> Vec<BackupRecord> {
    let (_, end) = client
        .get_offsets(topic, 0, isolation)
        .await
        .expect("offsets");
    let mut out = Vec::new();
    let mut offset = 0;
    while offset < end {
        let resp = client
            .fetch(topic, 0, offset, 1024 * 1024, isolation)
            .await
            .expect("fetch");
        if resp.records.is_empty() && resp.next_offset <= offset {
            break;
        }
        out.extend(resp.records);
        offset = resp.next_offset;
    }
    out
}

fn kv(record: &BackupRecord) -> (Vec<u8>, Vec<u8>) {
    (
        record.key.clone().unwrap_or_default(),
        record.value.clone().unwrap_or_default(),
    )
}

fn text(pairs: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    pairs
        .iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect()
}

/// A transaction marker as read uncommitted: a 4-byte control key
/// (version 0, type 0 = ABORT / 1 = COMMIT).
fn is_marker(record: &BackupRecord) -> bool {
    matches!(record.key.as_deref(), Some([0, 0, 0, 0 | 1]))
}

/// The interleaved scenario: two transactional producers (one commits and
/// later aborts, the other aborts and later commits) and plain records in
/// between, on one partition. Returns what a read_committed consumer sees,
/// in order: the plain records and the committed transactions' records.
async fn write_interleaved_scenario(
    bs: &str,
    client: &KafkaClient,
    topic: &str,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut a = TransactionalProducer::new(bs, &format!("{topic}-a"), TXN_TIMEOUT_MS)
        .await
        .expect("producer A");
    let mut b = TransactionalProducer::new(bs, &format!("{topic}-b"), TXN_TIMEOUT_MS)
        .await
        .expect("producer B");

    produce_plain(client, topic, "n0", "plain-0").await;
    a.begin();
    a.send(topic, 0, &[("a0", "committed-a0"), ("a1", "committed-a1")])
        .await
        .unwrap();
    produce_plain(client, topic, "n1", "plain-1").await;
    b.begin();
    b.send(topic, 0, &[("b0", "aborted-b0")]).await.unwrap();
    a.send(topic, 0, &[("a2", "committed-a2")]).await.unwrap();
    a.commit().await.unwrap();
    b.send(topic, 0, &[("b1", "aborted-b1")]).await.unwrap();
    b.abort().await.unwrap();
    produce_plain(client, topic, "n2", "plain-2").await;
    a.begin();
    a.send(topic, 0, &[("a3", "aborted-a3")]).await.unwrap();
    a.abort().await.unwrap();
    b.begin();
    b.send(topic, 0, &[("b2", "committed-b2")]).await.unwrap();
    b.commit().await.unwrap();
    produce_plain(client, topic, "n3", "plain-3").await;

    // 8 data records kept, 3 aborted, 4 markers: the log ends at 15 once
    // the coordinator has written every marker.
    wait_for_offsets(client, topic, 0, |lso, hw| hw == 15 && lso == hw)
        .await
        .expect("all transactions complete");

    text(&[
        ("n0", "plain-0"),
        ("a0", "committed-a0"),
        ("a1", "committed-a1"),
        ("n1", "plain-1"),
        ("a2", "committed-a2"),
        ("n2", "plain-2"),
        ("b2", "committed-b2"),
        ("n3", "plain-3"),
    ])
}

async fn stored_manifest(storage: &std::path::Path, backup_id: &str) -> BackupManifest {
    let data = std::fs::read(storage.join(backup_id).join("manifest.json")).expect("manifest");
    serde_json::from_slice(&data).expect("parse manifest")
}

async fn run_backup(config: Config, metrics: Option<Arc<PrometheusMetrics>>) {
    let engine = BackupEngine::new_with_metrics(config, metrics)
        .await
        .expect("backup engine");
    tokio::time::timeout(Duration::from_secs(90), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup");
}

fn original_offset(record: &BackupRecord) -> i64 {
    let header = record
        .headers
        .iter()
        .find(|h| h.key == X_ORIGINAL_OFFSET)
        .expect("restored record carries x-original-offset");
    i64::from_le_bytes(header.value.as_deref().unwrap().try_into().unwrap())
}

async fn seed_group(router: &PartitionLeaderRouter, topic: &str, group: &str, offset: i64) {
    let offsets = vec![(topic.to_string(), 0, offset, None)];
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

fn gauge(encoded: &str, name: &str, labels: &str) -> Option<i64> {
    let prefix = format!("{name}{{{labels}}} ");
    encoded
        .lines()
        .find_map(|l| l.strip_prefix(&prefix))
        .and_then(|v| v.trim().parse().ok())
}

/// The bug report shape: the default backup must restore exactly what a
/// read_committed consumer sees — no aborted record, no marker — and groups
/// committed at a marker, just after it, inside an aborted transaction or at
/// the high watermark must resume on the right committed record.
#[tokio::test]
#[ignore = "requires Docker"]
async fn read_committed_backup_restores_what_a_read_committed_consumer_sees() {
    let cluster = KafkaTestCluster::start_with_transactions()
        .await
        .expect("start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(60))
        .await
        .expect("Kafka ready");
    let bs = cluster.bootstrap_servers.clone();
    let client = cluster.create_client();
    client.connect().await.expect("connect");
    let topic = "txn-events";
    let target = "txn-events-restored";
    create_topic(&client, topic).await;

    let expected = write_interleaved_scenario(&bs, &client, topic).await;

    // The raw log: where the markers and aborted records ended up (the
    // coordinator writes markers asynchronously, so positions vary).
    let raw = read_raw(&client, topic).await;
    assert_eq!(raw.len(), 15, "8 kept + 3 aborted + 4 markers");
    let markers: Vec<i64> = raw
        .iter()
        .filter(|r| is_marker(r))
        .map(|r| r.offset)
        .collect();
    let aborted: Vec<i64> = raw
        .iter()
        .filter(|r| {
            r.value
                .as_deref()
                .is_some_and(|v| v.starts_with(b"aborted"))
        })
        .map(|r| r.offset)
        .collect();
    assert_eq!((markers.len(), aborted.len()), (4, 3));
    let committed_source: Vec<i64> = raw
        .iter()
        .filter(|r| !markers.contains(&r.offset) && !aborted.contains(&r.offset))
        .map(|r| r.offset)
        .collect();

    // Groups committed around the transactions, snapshotted by the backup.
    let router = PartitionLeaderRouter::new(kafka(&bs, topic))
        .await
        .expect("router");
    let groups: Vec<(String, i64)> = vec![
        ("at-marker".to_string(), markers[0]),
        ("after-marker".to_string(), markers[0] + 1),
        ("in-aborted".to_string(), aborted[0]),
        ("at-last-marker".to_string(), *markers.last().unwrap()),
        ("at-high-watermark".to_string(), 15),
    ];
    for (group, offset) in &groups {
        seed_group(&router, topic, group, *offset).await;
    }
    sleep(Duration::from_secs(1)).await;

    // Backup with the defaults (read_committed) and Prometheus metrics.
    let storage = create_temp_storage();
    let prom = Arc::new(PrometheusMetrics::new());
    let opts = BackupOptions {
        consumer_group_snapshot: true,
        ..default_backup_options()
    };
    assert_eq!(
        opts.isolation_level,
        IsolationLevel::ReadCommitted,
        "the default"
    );
    run_backup(
        backup_config(&bs, storage.path().to_path_buf(), "rc", topic, opts.clone()),
        Some(prom.clone()),
    )
    .await;

    let manifest = stored_manifest(storage.path(), "rc").await;
    assert_eq!(
        manifest.isolation_level,
        Some(IsolationLevel::ReadCommitted)
    );
    assert_eq!(manifest.total_records(), 8);
    assert_eq!(manifest.total_control_records_skipped(), 4);
    assert_eq!(manifest.total_aborted_records_dropped(), 3);
    let part = &manifest.topics[0].partitions[0];
    assert_eq!(
        part.reach(),
        Some(14),
        "accounts for every offset to the HW"
    );
    assert!(
        part.gaps.is_empty(),
        "markers and aborted records are not gaps"
    );
    // ...and no holes: the segments' covered ranges tile 0..=14.
    let mut covered: Vec<(i64, i64)> = part
        .segments
        .iter()
        .map(|s| (s.covered_start(), s.covered_end()))
        .collect();
    covered.sort();
    assert_eq!(covered.first().map(|c| c.0), Some(0));
    assert_eq!(covered.last().map(|c| c.1), Some(14));
    for pair in covered.windows(2) {
        assert_eq!(pair[1].0, pair[0].1 + 1, "contiguous: {covered:?}");
    }
    let encoded = prom.encode();
    assert!(
        encoded.contains("kafka_backup_control_records_skipped_total{backup_id=\"rc\"} 4"),
        "{encoded}"
    );
    assert!(
        encoded.contains("kafka_backup_aborted_records_dropped_total{backup_id=\"rc\"} 3"),
        "{encoded}"
    );
    assert_eq!(
        gauge(
            &encoded,
            "kafka_backup_lso_lag",
            "topic=\"txn-events\",partition=\"0\",backup_id=\"rc\""
        ),
        Some(0)
    );

    // One batch per fetch: every resume lands inside or after transactions
    // (the aborted list overlaps the fetch start). Same result.
    run_backup(
        backup_config(
            &bs,
            storage.path().to_path_buf(),
            "rc-small-fetch",
            topic,
            BackupOptions {
                fetch_max_bytes: Some(1),
                ..default_backup_options()
            },
        ),
        None,
    )
    .await;
    let small = stored_manifest(storage.path(), "rc-small-fetch").await;
    assert_eq!(small.total_records(), 8);
    assert_eq!(small.total_control_records_skipped(), 4);
    assert_eq!(small.total_aborted_records_dropped(), 3);
    assert_eq!(small.topics[0].partitions[0].reach(), Some(14));

    // Restore, then Phase 3 for the groups.
    let config = restore_config(&bs, storage.path().to_path_buf(), "rc", topic, target);
    let report = tokio::time::timeout(
        Duration::from_secs(90),
        RestoreEngine::new(config.clone())
            .expect("restore engine")
            .run(),
    )
    .await
    .expect("restore timed out")
    .expect("restore");
    assert_eq!(report.records_restored, 8);

    // The restored topic equals the read_committed view, in order, and
    // holds no marker.
    let restored = read_raw(&client, target).await;
    assert_eq!(restored.iter().map(kv).collect::<Vec<_>>(), expected);
    assert!(!restored.iter().any(is_marker));
    // ...which is also what our own read_committed fetch of the source sees.
    let committed_view = read_all(&client, topic, IsolationLevel::ReadCommitted).await;
    assert_eq!(committed_view.iter().map(kv).collect::<Vec<_>>(), expected);
    assert_eq!(
        restored.iter().map(original_offset).collect::<Vec<_>>(),
        committed_source
    );

    let outcome = ThreePhaseRestore::new(config)
        .expect("orchestrator")
        .run_offset_reset_phase(&report)
        .await
        .expect("phase 3");
    assert!(outcome.success(), "phase 3: {:?}", outcome.report);
    sleep(Duration::from_secs(1)).await;

    let target_log: Vec<(i64, i64)> = restored
        .iter()
        .map(|r| (r.offset, original_offset(r)))
        .collect();
    for (group, committed_at_source) in &groups {
        let committed = fetch_offsets(&client, group, None)
            .await
            .expect("fetch_offsets")
            .into_iter()
            .find(|o| o.topic == target && o.partition == 0 && o.offset >= 0)
            .map(|o| o.offset)
            .unwrap_or_else(|| panic!("{group} has no commit on {target}"));
        assert!(
            committed <= target_log.len() as i64,
            "{group} past the log end"
        );
        let first_read = target_log
            .iter()
            .find(|(t, _)| *t >= committed)
            .map(|&(_, s)| s);
        let expected_next = committed_source
            .iter()
            .copied()
            .find(|s| s >= committed_at_source);
        assert_eq!(
            first_read, expected_next,
            "{group} (committed at source {committed_at_source}, target {committed})"
        );
    }
}

/// A transaction left open holds the last stable offset back: the backup
/// stops there (snapshot and one-shot), `kafka_backup_lso_lag` shows the
/// gap, and once the transaction is aborted the next run continues past it.
#[tokio::test]
#[ignore = "requires Docker"]
async fn hanging_transaction_holds_the_backup_at_the_last_stable_offset() {
    let cluster = KafkaTestCluster::start_with_transactions()
        .await
        .expect("start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(60))
        .await
        .expect("Kafka ready");
    let bs = cluster.bootstrap_servers.clone();
    let client = cluster.create_client();
    client.connect().await.expect("connect");
    let topic = "txn-hanging";
    create_topic(&client, topic).await;

    // x0 x1 | A: c0 COMMIT | B: h0 (left open) | y0 y1
    produce_plain(&client, topic, "x0", "plain-x0").await;
    produce_plain(&client, topic, "x1", "plain-x1").await;
    let mut a = TransactionalProducer::new(&bs, "hang-a", TXN_TIMEOUT_MS)
        .await
        .unwrap();
    a.begin();
    a.send(topic, 0, &[("c0", "committed-c0")]).await.unwrap();
    a.commit().await.unwrap();
    wait_for_offsets(&client, topic, 0, |lso, hw| hw == 4 && lso == 4)
        .await
        .unwrap();
    let mut b = TransactionalProducer::new(&bs, "hang-b", TXN_TIMEOUT_MS)
        .await
        .unwrap();
    b.begin();
    let open_at = b.send(topic, 0, &[("h0", "open-h0")]).await.unwrap();
    assert_eq!(open_at, 4);
    produce_plain(&client, topic, "y0", "plain-y0").await;
    produce_plain(&client, topic, "y1", "plain-y1").await;
    let (lso, hw) = wait_for_offsets(&client, topic, 0, |_, hw| hw == 7)
        .await
        .unwrap();
    assert_eq!((lso, hw), (4, 7), "the open transaction holds the LSO");

    let storage = create_temp_storage();

    // Snapshot: the end is the LSO; the HW at capture is recorded too.
    let prom = Arc::new(PrometheusMetrics::new());
    run_backup(
        backup_config(
            &bs,
            storage.path().to_path_buf(),
            "snap",
            topic,
            BackupOptions {
                stop_at_current_offsets: true,
                ..default_backup_options()
            },
        ),
        Some(prom.clone()),
    )
    .await;
    let snap = stored_manifest(storage.path(), "snap").await;
    let part = &snap.topics[0].partitions[0];
    assert_eq!(part.snapshot_end_offset, Some(4), "snapshot end = LSO");
    assert_eq!(part.high_watermark_at_capture, Some(7));
    assert_eq!(part.reach(), Some(3), "nothing at or past the LSO");
    assert_eq!(snap.total_records(), 3);
    assert_eq!(snap.total_control_records_skipped(), 1);
    assert!(part.segments.iter().all(|s| s.end_offset < 4));
    let encoded = prom.encode();
    assert_eq!(
        gauge(
            &encoded,
            "kafka_backup_lso_lag",
            "topic=\"txn-hanging\",partition=\"0\",backup_id=\"snap\""
        ),
        Some(3),
        "{encoded}"
    );

    // One-shot (not a snapshot): also ends at the LSO, and the LSO lag is
    // reported although the partition has nothing left to fetch.
    let prom = Arc::new(PrometheusMetrics::new());
    let one_shot = backup_config(
        &bs,
        storage.path().to_path_buf(),
        "oneshot",
        topic,
        default_backup_options(),
    );
    run_backup(one_shot.clone(), Some(prom.clone())).await;
    let first = stored_manifest(storage.path(), "oneshot").await;
    assert_eq!(first.topics[0].partitions[0].reach(), Some(3));
    assert_eq!(first.total_records(), 3);
    run_backup(one_shot.clone(), Some(prom.clone())).await;
    assert_eq!(
        stored_manifest(storage.path(), "oneshot").await.topics[0].partitions[0].reach(),
        Some(3),
        "a run while the transaction is open adds nothing"
    );
    assert_eq!(
        gauge(
            &prom.encode(),
            "kafka_backup_lso_lag",
            "topic=\"txn-hanging\",partition=\"0\",backup_id=\"oneshot\""
        ),
        Some(3)
    );

    // The transaction is aborted: the next run continues from the LSO,
    // drops h0, passes the marker and archives y0 y1.
    b.abort().await.unwrap();
    wait_for_offsets(&client, topic, 0, |lso, hw| hw == 8 && lso == 8)
        .await
        .unwrap();
    let prom = Arc::new(PrometheusMetrics::new());
    run_backup(one_shot, Some(prom.clone())).await;
    let resumed = stored_manifest(storage.path(), "oneshot").await;
    assert_eq!(resumed.topics[0].partitions[0].reach(), Some(7));
    assert_eq!(resumed.total_records(), 5);
    assert_eq!(resumed.total_aborted_records_dropped(), 1);
    assert_eq!(resumed.total_control_records_skipped(), 2);
    assert_eq!(
        gauge(
            &prom.encode(),
            "kafka_backup_lso_lag",
            "topic=\"txn-hanging\",partition=\"0\",backup_id=\"oneshot\""
        ),
        Some(0)
    );
}

/// `backup.isolation_level: read_uncommitted` is the opt-out: it archives
/// the raw log as before 0.24, markers and aborted records included, and a
/// restore writes them all back.
#[tokio::test]
#[ignore = "requires Docker"]
async fn read_uncommitted_opt_out_reproduces_the_previous_behaviour() {
    let cluster = KafkaTestCluster::start_with_transactions()
        .await
        .expect("start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(60))
        .await
        .expect("Kafka ready");
    let bs = cluster.bootstrap_servers.clone();
    let client = cluster.create_client();
    client.connect().await.expect("connect");
    let topic = "txn-legacy";
    let target = "txn-legacy-restored";
    create_topic(&client, topic).await;
    write_interleaved_scenario(&bs, &client, topic).await;
    let raw = read_raw(&client, topic).await;
    assert_eq!(raw.len(), 15);

    let storage = create_temp_storage();
    run_backup(
        backup_config(
            &bs,
            storage.path().to_path_buf(),
            "ru",
            topic,
            BackupOptions {
                isolation_level: IsolationLevel::ReadUncommitted,
                ..default_backup_options()
            },
        ),
        None,
    )
    .await;
    let manifest = stored_manifest(storage.path(), "ru").await;
    assert_eq!(
        manifest.isolation_level,
        Some(IsolationLevel::ReadUncommitted)
    );
    assert_eq!(manifest.total_records(), 15);
    assert_eq!(manifest.total_control_records_skipped(), 0);
    assert_eq!(manifest.total_aborted_records_dropped(), 0);

    let mut config = restore_config(&bs, storage.path().to_path_buf(), "ru", topic, target);
    config.restore.as_mut().unwrap().auto_consumer_groups = false;
    let report = tokio::time::timeout(
        Duration::from_secs(90),
        RestoreEngine::new(config).expect("restore engine").run(),
    )
    .await
    .expect("restore timed out")
    .expect("restore");
    assert_eq!(report.records_restored, 15);

    // The baseline bug, kept on purpose: markers come back as ordinary
    // records and aborted records are restored.
    let restored = read_raw(&client, target).await;
    assert_eq!(
        restored.iter().map(kv).collect::<Vec<_>>(),
        raw.iter().map(kv).collect::<Vec<_>>()
    );
    assert_eq!(restored.iter().filter(|r| is_marker(r)).count(), 4);
    assert_eq!(
        restored
            .iter()
            .filter(|r| r
                .value
                .as_deref()
                .is_some_and(|v| v.starts_with(b"aborted")))
            .count(),
        3
    );
}
