//! Empty fetches below the end offset (tiered storage remote reads).
//!
//! A broker with tiered storage answers a Fetch for an offset that left local
//! disk with no records while it reads the remote tier. Before the fix the
//! partition ended on the first such answer and the run completed with zero
//! records and no error, so a backup chain stuck on a cold offset stayed
//! stuck silently. These tests drive that answer with the mock broker.

use std::time::Duration;
use tempfile::TempDir;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{BackupOptions, Config, KafkaConfig, Mode, TopicSelection};
use kafka_backup_core::manifest::BackupManifest;
use kafka_backup_core::storage::StorageBackendConfig;

use super::produce_mock_broker::MockBroker;

const TOPIC: &str = "cold";

fn config(broker: &MockBroker, storage: &TempDir, retry_secs: u64, max_wait_ms: i32) -> Config {
    let mut source: KafkaConfig = serde_json::from_value(serde_json::json!({
        "bootstrap_servers": [broker.bootstrap()],
    }))
    .unwrap();
    source.topics = TopicSelection {
        include: vec![TOPIC.to_string()],
        exclude: vec![],
    };
    source.connection.fetch_max_wait_ms = max_wait_ms;
    Config {
        mode: Mode::Backup,
        backup_id: "empty-fetch".to_string(),
        source: Some(source),
        target: None,
        storage: StorageBackendConfig::Filesystem {
            path: storage.path().to_path_buf(),
        },
        backup: Some(BackupOptions {
            continuous: false,
            stop_at_current_offsets: true,
            empty_fetch_retry_secs: retry_secs,
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
    }
}

async fn run(config: Config) -> kafka_backup_core::Result<()> {
    let engine = BackupEngine::new(config).await.expect("engine");
    tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("backup timed out")
}

fn backed_up_records(storage: &TempDir) -> i64 {
    let path = storage.path().join("empty-fetch").join("manifest.json");
    let manifest: BackupManifest =
        serde_json::from_str(&std::fs::read_to_string(path).expect("manifest")).unwrap();
    manifest
        .topics
        .iter()
        .flat_map(|t| t.partitions.iter())
        .flat_map(|p| p.segments.iter())
        .map(|s| s.record_count)
        .sum()
}

#[tokio::test]
async fn empty_fetches_below_end_are_retried_until_records_arrive() {
    let broker = MockBroker::start(TOPIC, 1).await;
    broker.append_foreign(0, &[b"one", b"two", b"three"]);
    broker.state.lock().unwrap().empty_fetches = 2;
    let storage = TempDir::new().unwrap();

    run(config(&broker, &storage, 30, 5000))
        .await
        .expect("backup");

    assert_eq!(backed_up_records(&storage), 3);
    let state = broker.state.lock().unwrap();
    assert_eq!(state.empty_fetches, 0, "both empty answers were consumed");
    assert!(
        state.fetch_max_waits.iter().all(|w| *w == 5000),
        "fetches carry the configured max_wait_ms: {:?}",
        state.fetch_max_waits
    );
}

#[tokio::test]
async fn empty_fetches_below_end_fail_the_run_when_the_budget_runs_out() {
    let broker = MockBroker::start(TOPIC, 1).await;
    broker.append_foreign(0, &[b"one", b"two", b"three"]);
    broker.state.lock().unwrap().empty_fetches = usize::MAX;
    let storage = TempDir::new().unwrap();

    let result = run(config(&broker, &storage, 2, 500)).await;

    let err = result.expect_err("a partition the broker never serves must fail the run");
    assert!(
        err.to_string().contains("partitions failed"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn zero_budget_fails_on_the_first_empty_fetch() {
    let broker = MockBroker::start(TOPIC, 1).await;
    broker.append_foreign(0, &[b"one", b"two", b"three"]);
    broker.state.lock().unwrap().empty_fetches = 1;
    let storage = TempDir::new().unwrap();

    run(config(&broker, &storage, 0, 500))
        .await
        .expect_err("unread data must never silently complete");

    assert_eq!(backed_up_records(&storage), 0);
    assert_eq!(broker.state.lock().unwrap().fetch_max_waits, vec![500]);
}
