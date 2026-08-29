//! strimzi-backup-operator#57 — `kafka_backup_snapshot_records_target` and
//! `kafka_backup_snapshot_records_remaining` were sized from the whole
//! captured offset range on every run, so an incremental snapshot backup
//! (offset store + `stop_at_current_offsets`) with only a handful of new
//! records still started its gauges at the size of the entire archive. Both
//! gauges must describe the work of the current run.
//!
//! Requires Docker.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tempfile::TempDir;
use tokio::time::sleep;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, OffsetStorageConfig, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::metrics::PrometheusMetrics;
use kafka_backup_core::storage::StorageBackendConfig;

use super::common::{generate_test_records, KafkaTestCluster};

const TOPIC: &str = "issue-57-incremental";
const BACKUP_ID: &str = "issue-57-backup";
const PARTITIONS: usize = 3; // KafkaTestCluster::create_topic creates 3 partitions

fn snapshot_config(bootstrap: &str, storage: &TempDir, offset_db: &Path) -> Config {
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
            segment_max_bytes: 1024 * 1024,
            segment_max_interval_ms: 10_000,
            compression: CompressionType::Zstd,
            stop_at_current_offsets: true,
            continuous: false,
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

/// Value of the `backup_id`-labelled series for `name` in the rendered
/// exposition, or `None` when the series was never created.
fn metric(metrics: &PrometheusMetrics, name: &str) -> Option<i64> {
    let prefix = format!("{name}{{backup_id=\"{BACKUP_ID}\"}} ");
    metrics
        .encode()
        .lines()
        .find_map(|line| {
            line.strip_prefix(&prefix)
                .map(str::trim)
                .map(str::to_string)
        })
        .map(|value| value.parse().expect("integer metric value"))
}

fn gauge(metrics: &PrometheusMetrics, name: &str) -> i64 {
    metric(metrics, name)
        .unwrap_or_else(|| panic!("{name} was not exported:\n{}", metrics.encode()))
}

/// One snapshot run with a fresh registry, exactly like a new job pod.
async fn run_snapshot_backup(config: Config) -> Arc<PrometheusMetrics> {
    let metrics = Arc::new(PrometheusMetrics::new());
    let engine = BackupEngine::new_with_metrics(config, Some(Arc::clone(&metrics)))
        .await
        .expect("failed to create backup engine");
    tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup run failed");
    metrics
}

async fn produce(cluster: &KafkaTestCluster, count: usize) {
    let client = cluster.create_client();
    client.connect().await.expect("failed to connect");
    for (i, record) in generate_test_records(count, TOPIC).iter().enumerate() {
        let partition = (i % PARTITIONS) as i32;
        client
            .produce(TOPIC, partition, vec![record.clone()], -1, 30_000)
            .await
            .expect("failed to produce");
    }
    sleep(Duration::from_secs(2)).await;
}

/// Three runs against one archive: the first plans the whole topic, the
/// second only the records produced in between, the third nothing at all.
/// Before the fix run 2 reported a target of 75 and run 3 of 75 as well.
#[tokio::test]
#[ignore] // Requires Docker
async fn incremental_snapshot_runs_report_only_their_own_records() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("failed to start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let initial_records = 60;
    cluster
        .create_topic(TOPIC, initial_records)
        .await
        .expect("failed to create topic");
    sleep(Duration::from_secs(2)).await;

    let storage = TempDir::new().expect("storage dir");
    let offsets = TempDir::new().expect("offset dir");
    let offset_db = offsets.path().join("offsets.db");
    let config = || snapshot_config(&cluster.bootstrap_servers, &storage, &offset_db);

    // Run 1: nothing archived yet, so the whole topic is this run's work.
    let run1 = run_snapshot_backup(config()).await;
    assert_eq!(
        gauge(&run1, "kafka_backup_snapshot_records_target"),
        initial_records as i64
    );
    assert_eq!(gauge(&run1, "kafka_backup_snapshot_records_remaining"), 0);
    assert_eq!(
        metric(&run1, "kafka_backup_records_total"),
        Some(initial_records as i64)
    );

    // Run 2: only the records produced since run 1 are planned.
    let new_records = 15;
    produce(&cluster, new_records).await;

    let run2 = run_snapshot_backup(config()).await;
    assert_eq!(
        gauge(&run2, "kafka_backup_snapshot_records_target"),
        new_records as i64,
        "target must be this run's records, not the whole archive"
    );
    assert_eq!(gauge(&run2, "kafka_backup_snapshot_records_remaining"), 0);
    assert_eq!(
        metric(&run2, "kafka_backup_records_total"),
        Some(new_records as i64),
        "the run fetched exactly what it planned"
    );

    // Run 3: nothing new since run 2 — a zero-work run reports 0 / 0.
    let run3 = run_snapshot_backup(config()).await;
    assert_eq!(gauge(&run3, "kafka_backup_snapshot_records_target"), 0);
    assert_eq!(gauge(&run3, "kafka_backup_snapshot_records_remaining"), 0);
    assert_eq!(
        metric(&run3, "kafka_backup_records_total"),
        None,
        "no records were fetched, so the counter is never created"
    );
}
