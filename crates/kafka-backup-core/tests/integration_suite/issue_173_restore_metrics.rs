//! Issue #173 — `RestoreEngine` never received `PrometheusMetrics`, so every
//! `kafka_restore_*` series stayed empty during a restore, and the
//! `RestoreProgress` broadcast (`RestoreEngine::progress_receiver`) never
//! sent anything.
//!
//! These tests run in-process with no Docker, against the two-broker mock
//! from the issue #197 tests (Metadata + Produce). They restore a 2 x 20
//! record backup through the 1:1 partition path, the repartition (fan-out)
//! path and `ThreePhaseRestore::run_restore_phase`, and check the restore
//! metrics and progress events afterwards.

use std::collections::HashMap;
use std::sync::Arc;

use tempfile::TempDir;
use tokio::sync::Mutex;

use kafka_backup_core::config::{
    Config, Mode, RepartitioningStrategy, RestoreOptions, TopicRepartitioning,
};
use kafka_backup_core::restore::{RestoreEngine, RestoreProgress};
use kafka_backup_core::storage::StorageBackendConfig;
use kafka_backup_core::{PrometheusMetrics, ThreePhaseRestore};

use super::issue_197_circuit_breaker::{
    client_config, write_two_partition_backup, BrokerMock, ProduceMode, TOPIC,
};

/// Records in the fixture backup: 20 in each of 2 partitions.
const RECORDS: f64 = 40.0;

/// Value of the series `name{labels...}` in the Prometheus text output, if
/// present. Every `label="value"` pair in `labels` must match.
fn metric(encoded: &str, name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    encoded.lines().find_map(|line| {
        let rest = line.strip_prefix(name)?;
        let rest = rest.strip_prefix('{')?;
        let (label_part, value) = rest.split_once("} ")?;
        let all_match = labels
            .iter()
            .all(|(k, v)| label_part.contains(&format!("{k}=\"{v}\"")));
        all_match
            .then(|| value.trim().parse::<f64>().ok())
            .flatten()
    })
}

fn restore_config(bootstrap: &str, storage: &std::path::Path, backup_id: &str) -> Config {
    Config {
        mode: Mode::Restore,
        backup_id: backup_id.to_string(),
        source: None,
        target: Some(client_config(bootstrap)),
        storage: StorageBackendConfig::Filesystem {
            path: storage.to_path_buf(),
        },
        backup: None,
        restore: Some(RestoreOptions {
            restore_topic_configs: false,
            create_topics: false,
            max_concurrent_partitions: 2,
            produce_batch_size: 5,
            produce_acks: 1,
            // acks other than -1 need idempotence off (0.25)
            idempotent_producer: false,
            ..Default::default()
        }),
        offset_storage: None,
        metrics: None,
    }
}

async fn two_brokers() -> (BrokerMock, BrokerMock) {
    let peers = Arc::new(Mutex::new(Vec::new()));
    let b1 = BrokerMock::start(1, peers.clone()).await;
    let b2 = BrokerMock::start(2, peers.clone()).await;
    *peers.lock().await = vec![(1, b1.addr), (2, b2.addr)];
    (b1, b2)
}

fn drain(rx: &mut tokio::sync::broadcast::Receiver<RestoreProgress>) -> Vec<RestoreProgress> {
    let mut events = Vec::new();
    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }
    events
}

/// The counters and gauges every successful restore of the fixture must set.
fn assert_completed_restore_metrics(encoded: &str, backup_id: &str, bytes_restored: u64) {
    let id = [("restore_id", backup_id)];
    assert_eq!(
        metric(encoded, "kafka_restore_records_total", &id),
        Some(RECORDS),
        "records counter:\n{encoded}"
    );
    assert_eq!(
        metric(encoded, "kafka_restore_bytes_total", &id),
        Some(bytes_restored as f64),
        "bytes counter must match the report:\n{encoded}"
    );
    assert!(bytes_restored > 0);
    assert_eq!(
        metric(encoded, "kafka_restore_partitions_completed_total", &id),
        Some(2.0),
        "partitions completed:\n{encoded}"
    );
    assert_eq!(
        metric(
            encoded,
            "kafka_restore_duration_seconds_count",
            &[("backup_id", backup_id), ("status", "success")]
        ),
        Some(1.0),
        "one successful duration observation:\n{encoded}"
    );
    assert_eq!(
        metric(
            encoded,
            "kafka_restore_progress_percent",
            &[("restore_id", backup_id), ("backup_id", backup_id)]
        ),
        Some(100.0),
        "progress at completion:\n{encoded}"
    );
    assert_eq!(
        metric(encoded, "kafka_restore_eta_seconds", &id),
        Some(0.0),
        "no time left at completion:\n{encoded}"
    );
    assert!(
        metric(encoded, "kafka_restore_throughput_records_per_sec", &id).is_some_and(|v| v > 0.0),
        "throughput gauge:\n{encoded}"
    );
    assert_eq!(
        metric(encoded, "kafka_restore_errors_total", &id),
        None,
        "no errors on a clean restore:\n{encoded}"
    );
}

#[tokio::test]
async fn restore_records_prometheus_metrics_and_progress() {
    let (b1, b2) = two_brokers().await;
    let tmp = TempDir::new().unwrap();
    let backup_id = "issue-173-metrics";
    write_two_partition_backup(tmp.path(), backup_id).await;

    let prom = Arc::new(PrometheusMetrics::new());
    let engine = RestoreEngine::new_with_metrics(
        restore_config(&b1.addr.to_string(), tmp.path(), backup_id),
        Some(prom.clone()),
    )
    .expect("engine");
    assert!(engine.prometheus_metrics().is_some());
    let mut progress = engine.progress_receiver();

    let report = engine.run().await.expect("restore succeeds");
    assert_eq!(report.records_restored, RECORDS as u64);

    assert_completed_restore_metrics(&prom.encode(), backup_id, report.bytes_restored);

    let events = drain(&mut progress);
    let last = events.last().expect("at least the final progress event");
    assert_eq!(last.records_restored, RECORDS as u64);
    assert_eq!(last.total_records, RECORDS as u64);
    assert_eq!(last.bytes_restored, report.bytes_restored);
    assert_eq!(last.percentage, 100.0);
    assert_eq!(last.eta_ms, Some(0));
    assert_eq!((last.topics_completed, last.total_topics), (1, 1));
    assert_eq!((last.segments_completed, last.total_segments), (2, 2));
    assert_eq!(last.current_topic, TOPIC);
    // Percentages never go backwards.
    for pair in events.windows(2) {
        assert!(pair[0].percentage <= pair[1].percentage, "{events:?}");
    }

    b1.shutdown().await;
    b2.shutdown().await;
}

#[tokio::test]
async fn repartitioned_restore_records_prometheus_metrics() {
    let (b1, b2) = two_brokers().await;
    let tmp = TempDir::new().unwrap();
    let backup_id = "issue-173-repartition";
    write_two_partition_backup(tmp.path(), backup_id).await;

    let mut config = restore_config(&b1.addr.to_string(), tmp.path(), backup_id);
    config.restore.as_mut().unwrap().repartitioning = HashMap::from([(
        TOPIC.to_string(),
        TopicRepartitioning {
            strategy: RepartitioningStrategy::Murmur2,
            target_partitions: 2,
        },
    )]);

    let prom = Arc::new(PrometheusMetrics::new());
    let engine = RestoreEngine::new_with_metrics(config, Some(prom.clone())).expect("engine");
    let mut progress = engine.progress_receiver();
    let report = engine.run().await.expect("repartitioned restore succeeds");
    assert_eq!(report.records_restored, RECORDS as u64);

    assert_completed_restore_metrics(&prom.encode(), backup_id, report.bytes_restored);

    let last = drain(&mut progress).pop().expect("final progress event");
    assert_eq!(last.records_restored, RECORDS as u64);
    assert_eq!(last.percentage, 100.0);
    assert_eq!((last.segments_completed, last.total_segments), (2, 2));

    b1.shutdown().await;
    b2.shutdown().await;
}

#[tokio::test]
async fn failed_restore_counts_errors_and_a_failure_duration() {
    let (b1, b2) = two_brokers().await;
    // Partition 0 (broker 1) fails terminally once partition 1 has produced.
    b1.set_mode(ProduceMode::BrokerErrorAfterPeerProduced(
        b2.produce_count.clone(),
        1,
    ))
    .await;
    let tmp = TempDir::new().unwrap();
    let backup_id = "issue-173-failure";
    write_two_partition_backup(tmp.path(), backup_id).await;

    let prom = Arc::new(PrometheusMetrics::new());
    let engine = RestoreEngine::new_with_metrics(
        restore_config(&b1.addr.to_string(), tmp.path(), backup_id),
        Some(prom.clone()),
    )
    .expect("engine");
    let mut progress = engine.progress_receiver();
    let report = engine.run().await;
    let encoded = prom.encode();

    let id = [("restore_id", backup_id)];
    // The topic error fails the run; it must be counted once, not again for
    // the "Restore completed with 1 error(s)" summary.
    assert!(report.is_err(), "partition 0 failure fails the run");
    let errors: f64 = encoded
        .lines()
        .filter(|l| {
            l.starts_with("kafka_restore_errors_total{")
                && l.contains(&format!("restore_id=\"{backup_id}\""))
        })
        .filter_map(|l| l.rsplit_once(' ')?.1.parse::<f64>().ok())
        .sum();
    assert_eq!(errors, 1.0, "exactly one restore error:\n{encoded}");
    assert_eq!(
        metric(
            &encoded,
            "kafka_restore_duration_seconds_count",
            &[("backup_id", backup_id), ("status", "failure")]
        ),
        Some(1.0),
        "{encoded}"
    );
    assert!(
        metric(&encoded, "kafka_restore_records_total", &id).is_some_and(|v| v > 0.0),
        "the healthy partition's records are counted:\n{encoded}"
    );
    let progress_percent = metric(
        &encoded,
        "kafka_restore_progress_percent",
        &[("restore_id", backup_id)],
    )
    .expect("progress gauge");
    assert!(
        progress_percent < 100.0,
        "a failed restore is not 100% done: {progress_percent}"
    );
    let last = drain(&mut progress).pop().expect("final progress event");
    assert!(last.percentage < 100.0, "{last:?}");

    b1.shutdown().await;
    b2.shutdown().await;
}

#[tokio::test]
async fn three_phase_restore_phase_passes_metrics_to_the_engine() {
    let (b1, b2) = two_brokers().await;
    let tmp = TempDir::new().unwrap();
    let backup_id = "issue-173-three-phase";
    write_two_partition_backup(tmp.path(), backup_id).await;

    let prom = Arc::new(PrometheusMetrics::new());
    let orchestrator =
        ThreePhaseRestore::new(restore_config(&b1.addr.to_string(), tmp.path(), backup_id))
            .expect("orchestrator")
            .with_prometheus_metrics(prom.clone());
    let report = orchestrator
        .run_restore_phase()
        .await
        .expect("restore phase");

    assert_completed_restore_metrics(&prom.encode(), backup_id, report.bytes_restored);

    b1.shutdown().await;
    b2.shutdown().await;
}
