//! Restore produce path against an in-process broker mock (no Docker):
//! idempotent produce with byte-identical retries at both retry layers,
//! the fallback when idempotence is unavailable, byte-capped batches and
//! the `max.message.bytes` pre-check.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use kafka_backup_core::config::{
    CompressionType, Config, ConnectionConfig, KafkaConfig, Mode, RestoreOptions, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::manifest::{BackupManifest, BackupRecord, RestoreReport};
use kafka_backup_core::metrics::PerformanceMetrics;
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::segment::{BinaryRecord, SegmentWriter, SegmentWriterConfig};
use kafka_backup_core::storage::{create_backend, StorageBackendConfig};
use tempfile::TempDir;

use super::produce_mock_broker::{Fault, MockBroker, StoredRecord, CLUSTER_AUTHORIZATION_FAILED};

pub const TOPIC: &str = "produce-path";

/// A deterministic record; `value_len` bytes of pseudo-random (so
/// incompressible) value when `random` is set.
pub fn record(offset: i64, value_len: usize, random: bool) -> BackupRecord {
    let value = if random {
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15 ^ (offset as u64 + 1);
        (0..value_len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    } else {
        let mut v = format!("value-{offset}-").into_bytes();
        v.resize(value_len.max(v.len()), b'x');
        v
    };
    BackupRecord {
        key: Some(format!("key-{offset}").into_bytes()),
        value: Some(value),
        headers: vec![],
        timestamp: 1_700_000_000_000 + offset,
        offset,
    }
}

/// Write a backup of `TOPIC` with the given records per partition, split
/// into segments of `segment_records` records.
pub async fn write_backup(
    storage_path: &Path,
    backup_id: &str,
    partitions: &HashMap<i32, Vec<BackupRecord>>,
    segment_records: usize,
) {
    write_topic_backup(storage_path, backup_id, TOPIC, partitions, segment_records).await
}

/// Write a backup of `topic` with the given records per partition, split
/// into segments of `segment_records` records.
pub async fn write_topic_backup(
    storage_path: &Path,
    backup_id: &str,
    topic_name: &str,
    partitions: &HashMap<i32, Vec<BackupRecord>>,
    segment_records: usize,
) {
    let storage = create_backend(&StorageBackendConfig::Filesystem {
        path: storage_path.to_path_buf(),
    })
    .expect("filesystem backend");
    let metrics = Arc::new(PerformanceMetrics::new());
    let mut manifest = BackupManifest::new(backup_id.to_string());
    manifest.compression = "none".to_string();

    for (partition, records) in partitions {
        for chunk in records.chunks(segment_records) {
            let key = format!(
                "{backup_id}/topics/{topic_name}/partition={partition}/segment-{:020}.bin",
                chunk[0].offset
            );
            let mut writer = SegmentWriter::new(
                SegmentWriterConfig {
                    compression: CompressionType::None,
                    max_segment_bytes: 64 * 1024 * 1024,
                    ..Default::default()
                },
                storage.clone(),
                metrics.clone(),
            );
            for r in chunk {
                writer
                    .add_record(BinaryRecord {
                        timestamp: r.timestamp,
                        offset: r.offset,
                        key: r.key.clone().map(Bytes::from),
                        value: r.value.clone().map(Bytes::from),
                        headers: vec![],
                    })
                    .unwrap();
            }
            let meta = writer.flush(&key).await.unwrap().expect("segment written");
            let topic = manifest.get_or_create_topic(topic_name);
            topic.original_partition_count = Some(partitions.len() as i32);
            topic.get_or_create_partition(*partition).add_segment(meta);
        }
    }

    storage
        .put(
            &format!("{backup_id}/manifest.json"),
            Bytes::from(serde_json::to_vec_pretty(&manifest).unwrap()),
        )
        .await
        .unwrap();
}

pub fn restore_config(
    bootstrap: &str,
    storage_path: &Path,
    backup_id: &str,
    options: RestoreOptions,
) -> Config {
    topic_restore_config(bootstrap, storage_path, backup_id, TOPIC, options)
}

pub fn topic_restore_config(
    bootstrap: &str,
    storage_path: &Path,
    backup_id: &str,
    topic: &str,
    options: RestoreOptions,
) -> Config {
    Config {
        mode: Mode::Restore,
        backup_id: backup_id.to_string(),
        source: None,
        target: Some(KafkaConfig {
            bootstrap_servers: vec![bootstrap.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: vec![topic.to_string()],
                exclude: vec![],
            },
            connection: ConnectionConfig {
                connections_per_broker: 1,
                ..Default::default()
            },
        }),
        storage: StorageBackendConfig::Filesystem {
            path: storage_path.to_path_buf(),
        },
        backup: None,
        restore: Some(options),
        offset_storage: None,
        metrics: None,
    }
}

pub fn base_options() -> RestoreOptions {
    RestoreOptions {
        create_topics: false,
        restore_topic_configs: false,
        max_concurrent_partitions: 4,
        produce_batch_size: 10,
        ..Default::default()
    }
}

/// `(key, value)` of each record, in order.
pub type Digest = Vec<(Option<Vec<u8>>, Option<Vec<u8>>)>;

/// `(key, value)` of the source records, in order.
pub fn source_digest(records: &[BackupRecord]) -> Digest {
    records
        .iter()
        .map(|r| (r.key.clone(), r.value.clone()))
        .collect()
}

/// `(key, value)` of the records in the mock's log, in order.
pub fn log_digest(log: &[StoredRecord]) -> Digest {
    log.iter()
        .map(|r| {
            (
                r.key.as_ref().map(|k| k.to_vec()),
                r.value.as_ref().map(|v| v.to_vec()),
            )
        })
        .collect()
}

async fn run(config: Config) -> kafka_backup_core::Result<RestoreReport> {
    RestoreEngine::new(config).expect("engine").run().await
}

/// A batch whose response is lost twice — once caught by
/// `KafkaClient::send_request`'s reconnect-and-retry, once by the
/// partition router's retry loop — is resent with byte-identical content
/// each time, and the broker writes it once.
#[tokio::test]
async fn lost_responses_are_retried_with_identical_bytes_and_written_once() {
    let broker = MockBroker::start(TOPIC, 1).await;
    let dir = TempDir::new().unwrap();
    let source: Vec<BackupRecord> = (0..35).map(|i| record(i, 64, false)).collect();
    write_backup(
        dir.path(),
        "idem",
        &HashMap::from([(0, source.clone())]),
        20,
    )
    .await;

    // The second batch (attempt 1): appended, then the connection drops
    // before the response; the reconnect-and-retry in send_request (attempt
    // 2) meets the same fate; the router's retry (attempt 3) succeeds.
    broker.schedule(1, Fault::ApplyThenDrop);
    broker.schedule(2, Fault::ApplyThenDrop);
    let report = run(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "idem",
        base_options(),
    ))
    .await
    .expect("restore succeeds");

    assert!(report.idempotent_producer);
    assert_eq!(report.idempotent_producer_fallback, None);
    assert_eq!(report.records_restored, 35);

    let attempts = broker.attempts();
    // Batches of 10: 4 batches, the faulted one sent three times.
    let counts: Vec<i32> = attempts.iter().map(|a| a.record_count).collect();
    assert_eq!(attempts.len(), 6, "{counts:?}");
    let faulted = &attempts[1..4];
    assert!(
        faulted.iter().all(|a| a.batch == faulted[0].batch),
        "every resend of the batch is byte-identical"
    );
    assert!(attempts.iter().all(|a| a.producer_id > 0));
    let sequences: Vec<i32> = attempts.iter().map(|a| a.base_sequence).collect();
    assert_eq!(sequences, vec![0, 10, 10, 10, 20, 30]);

    let log = broker.log(0);
    assert_eq!(log.len(), 35, "no duplicates");
    assert_eq!(log_digest(&log), source_digest(&source));

    // The offset mapping is exact.
    let pairs = report
        .offset_mapping
        .detailed_mappings
        .get(&format!("{TOPIC}/0"))
        .expect("mapping for the partition");
    assert_eq!(pairs.len(), 35);
    for pair in pairs {
        assert_eq!(pair.source_offset, pair.target_offset);
    }
}

/// Control for the test above: without idempotence the same lost responses
/// write the batch three times.
#[tokio::test]
async fn without_idempotence_lost_responses_duplicate_the_batch() {
    let broker = MockBroker::start(TOPIC, 1).await;
    let dir = TempDir::new().unwrap();
    let source: Vec<BackupRecord> = (0..35).map(|i| record(i, 64, false)).collect();
    write_backup(dir.path(), "plain", &HashMap::from([(0, source)]), 20).await;

    broker.schedule(1, Fault::ApplyThenDrop);
    broker.schedule(2, Fault::ApplyThenDrop);
    let report = run(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "plain",
        RestoreOptions {
            idempotent_producer: false,
            ..base_options()
        },
    ))
    .await
    .expect("restore succeeds");

    assert!(!report.idempotent_producer);
    assert_eq!(report.idempotent_producer_fallback, None);
    assert_eq!(broker.state.lock().unwrap().init_requests, 0);
    assert_eq!(
        broker.log(0).len(),
        35 + 20,
        "the batch was written three times"
    );
}

/// InitProducerId denied: WARN, a report flag with the reason, and
/// produce without a producer id.
#[tokio::test]
async fn denied_idempotence_falls_back_and_is_reported() {
    let broker = MockBroker::start(TOPIC, 2).await;
    broker.state.lock().unwrap().init_producer_id_error = Some(CLUSTER_AUTHORIZATION_FAILED);
    let dir = TempDir::new().unwrap();
    let p0: Vec<BackupRecord> = (0..12).map(|i| record(i, 32, false)).collect();
    let p1: Vec<BackupRecord> = (0..7).map(|i| record(i, 32, false)).collect();
    write_backup(
        dir.path(),
        "fallback",
        &HashMap::from([(0, p0.clone()), (1, p1.clone())]),
        5,
    )
    .await;

    let report = run(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "fallback",
        base_options(),
    ))
    .await
    .expect("restore succeeds without idempotence");

    assert!(!report.idempotent_producer);
    let reason = report
        .idempotent_producer_fallback
        .expect("fallback reason");
    assert!(reason.contains("CLUSTER_AUTHORIZATION_FAILED"), "{reason}");
    assert!(broker.attempts().iter().all(|a| a.producer_id == -1));
    assert_eq!(log_digest(&broker.log(0)), source_digest(&p0));
    assert_eq!(log_digest(&broker.log(1)), source_digest(&p1));
}

/// A broker that does not support InitProducerId v3 (before Kafka 2.5) is
/// detected through ApiVersions and never sent InitProducerId.
#[tokio::test]
async fn old_broker_falls_back_without_init_producer_id() {
    let broker = MockBroker::start(TOPIC, 1).await;
    broker.state.lock().unwrap().init_producer_id_max_version = 2;
    let dir = TempDir::new().unwrap();
    let source: Vec<BackupRecord> = (0..5).map(|i| record(i, 32, false)).collect();
    write_backup(dir.path(), "old", &HashMap::from([(0, source)]), 5).await;

    let report = run(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "old",
        base_options(),
    ))
    .await
    .expect("restore succeeds");
    assert!(!report.idempotent_producer);
    assert!(report
        .idempotent_producer_fallback
        .unwrap()
        .contains("Kafka 2.5"));
    assert_eq!(broker.state.lock().unwrap().init_requests, 0);
    assert_eq!(broker.log(0).len(), 5);
}

/// Batches close at `produce_max_batch_bytes` before `produce_batch_size`.
#[tokio::test]
async fn batches_are_capped_by_encoded_bytes() {
    let broker = MockBroker::start(TOPIC, 1).await;
    let dir = TempDir::new().unwrap();
    let source: Vec<BackupRecord> = (0..100).map(|i| record(i, 200, true)).collect();
    write_backup(
        dir.path(),
        "bytes",
        &HashMap::from([(0, source.clone())]),
        100,
    )
    .await;

    let report = run(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "bytes",
        RestoreOptions {
            produce_batch_size: 1000,
            produce_max_batch_bytes: 4096,
            ..base_options()
        },
    ))
    .await
    .expect("restore succeeds");
    assert_eq!(report.records_restored, 100);

    let attempts = broker.attempts();
    assert!(attempts.len() >= 6, "{} batches", attempts.len());
    for a in &attempts {
        // ~228 encoded bytes per record (key, 200-byte value, framing at
        // its upper bound): at most 17 fit in 4 KiB with the batch header.
        assert!(a.record_count <= 17, "batch of {} records", a.record_count);
        assert!(
            a.batch.len() <= 4096 + 64,
            "batch of {} bytes",
            a.batch.len()
        );
    }
    assert_eq!(log_digest(&broker.log(0)), source_digest(&source));
}

/// The target's `max.message.bytes` lowers the batch cap, and a record
/// that cannot fit even alone fails the partition with its source offset
/// before anything of its segment is produced.
#[tokio::test]
async fn oversized_record_fails_before_its_segment_is_produced() {
    let broker = MockBroker::start(TOPIC, 1).await;
    broker.state.lock().unwrap().max_message_bytes = Some(2048);
    let dir = TempDir::new().unwrap();
    let mut source: Vec<BackupRecord> = (0..10).map(|i| record(i, 100, true)).collect();
    source[7] = record(7, 8_000, true);
    write_backup(dir.path(), "big", &HashMap::from([(0, source.clone())]), 5).await;

    let err = run(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "big",
        base_options(),
    ))
    .await
    .expect_err("oversized record fails the restore");
    let message = err.to_string();
    assert!(message.contains("source offset 7"), "{message}");
    assert!(message.contains("max.message.bytes (2048)"), "{message}");
    assert!(message.contains(&format!("{TOPIC}[0]")), "{message}");

    // The first segment (offsets 0-4) was produced; nothing of the second.
    assert_eq!(log_digest(&broker.log(0)), source_digest(&source[..5]));
    for a in broker.attempts() {
        assert!(a.batch.len() <= 2048, "batch of {} bytes", a.batch.len());
    }
}

/// Without DescribeConfigs permission the pre-check is skipped and the
/// restore still runs.
#[tokio::test]
async fn unreadable_max_message_bytes_skips_the_pre_check() {
    let broker = MockBroker::start(TOPIC, 1).await;
    broker.state.lock().unwrap().max_message_bytes = None;
    let dir = TempDir::new().unwrap();
    let source: Vec<BackupRecord> = (0..8).map(|i| record(i, 50, false)).collect();
    write_backup(
        dir.path(),
        "noconf",
        &HashMap::from([(0, source.clone())]),
        4,
    )
    .await;

    let report = run(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "noconf",
        base_options(),
    ))
    .await
    .expect("restore succeeds");
    assert_eq!(report.records_restored, 8);
    assert_eq!(log_digest(&broker.log(0)), source_digest(&source));
}

/// UNKNOWN_PRODUCER_ID mid-restore (producer state lost on the broker):
/// the producer bumps its epoch, reconciles and continues with no
/// duplicates and nothing missing.
#[tokio::test]
async fn lost_producer_state_is_recovered_by_an_epoch_bump() {
    let broker = MockBroker::start(TOPIC, 1).await;
    let dir = TempDir::new().unwrap();
    let source: Vec<BackupRecord> = (0..30).map(|i| record(i, 32, false)).collect();
    write_backup(
        dir.path(),
        "bump",
        &HashMap::from([(0, source.clone())]),
        30,
    )
    .await;

    broker.schedule(1, Fault::ForgetProducers);
    let report = run(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "bump",
        RestoreOptions {
            // Keep the reconciliation wait short.
            produce_timeout_ms: 200,
            ..base_options()
        },
    ))
    .await
    .expect("restore succeeds");

    assert!(report.idempotent_producer);
    assert_eq!(log_digest(&broker.log(0)), source_digest(&source));
    let attempts = broker.attempts();
    let producers: Vec<i64> = attempts.iter().map(|a| a.producer_id).collect();
    assert!(
        producers.windows(2).any(|w| w[0] != w[1]),
        "a new producer identity after the bump: {producers:?}"
    );
    assert_eq!(broker.state.lock().unwrap().init_requests, 2);
}
