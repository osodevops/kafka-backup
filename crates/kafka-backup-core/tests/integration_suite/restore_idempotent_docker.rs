//! Restore produce path against a real broker (Testcontainers Kafka).
//!
//! - Produce responses lost mid-restore (a Kafka-aware proxy drops the
//!   connection after the broker appended the batch): with the idempotent
//!   producer the target matches the source exactly; without it, the
//!   retried batches are duplicated (control).
//!
//! Requires Docker (`#[ignore]`). Containers use random ports.

use std::collections::HashMap;
use std::time::Duration;

use kafka_backup_core::config::{IsolationLevel, RestoreOptions};
use kafka_backup_core::kafka::{KafkaClient, TopicToCreate};
use kafka_backup_core::manifest::BackupRecord;
use kafka_backup_core::restore::RestoreEngine;
use tempfile::TempDir;

use super::common::KafkaTestCluster;
use super::kafka_fault_proxy::FaultProxy;
use super::restore_produce_path::{record, topic_restore_config, write_topic_backup, Digest};

pub const PARTITIONS: i32 = 3;

/// Source records: `per_partition` records in each of `PARTITIONS`.
pub fn source_records(per_partition: i64) -> HashMap<i32, Vec<BackupRecord>> {
    (0..PARTITIONS)
        .map(|p| {
            let records = (0..per_partition)
                .map(|i| {
                    let mut r = record(i, 120, true);
                    r.key = Some(format!("p{p}-key-{i}").into_bytes());
                    r
                })
                .collect();
            (p, records)
        })
        .collect()
}

pub async fn create_target_topic(cluster: &KafkaTestCluster, topic: &str) {
    let client = cluster.create_client();
    client.connect().await.expect("connect");
    let results = client
        .create_topics(
            vec![TopicToCreate {
                name: topic.to_string(),
                num_partitions: PARTITIONS,
                replication_factor: 1,
            }],
            30_000,
        )
        .await
        .expect("create topic");
    assert!(
        results.iter().all(|r| r.is_success_or_exists()),
        "{results:?}"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
}

/// Every record of `topic`/`partition`, read directly from the broker.
pub async fn read_partition(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
) -> Vec<BackupRecord> {
    let (_, high_watermark) = client
        .get_offsets(topic, partition, IsolationLevel::ReadUncommitted)
        .await
        .expect("offsets");
    let mut records = Vec::new();
    let mut offset = 0;
    while offset < high_watermark {
        let response = client
            .fetch(
                topic,
                partition,
                offset,
                4 * 1024 * 1024,
                IsolationLevel::ReadUncommitted,
            )
            .await
            .expect("fetch");
        if response.records.is_empty() && response.next_offset <= offset {
            break;
        }
        offset = response.next_offset.max(offset + 1);
        records.extend(response.records);
    }
    records
}

/// `(key, value)` pairs in order.
pub fn digest(records: &[BackupRecord]) -> Digest {
    records
        .iter()
        .map(|r| (r.key.clone(), r.value.clone()))
        .collect()
}

pub fn restore_options(idempotent: bool) -> RestoreOptions {
    RestoreOptions {
        create_topics: false,
        restore_topic_configs: false,
        max_concurrent_partitions: PARTITIONS as usize,
        produce_batch_size: 50,
        idempotent_producer: idempotent,
        ..Default::default()
    }
}

async fn restore_with_lost_responses(
    idempotent: bool,
) -> (
    HashMap<i32, Vec<BackupRecord>>,
    HashMap<i32, Vec<BackupRecord>>,
) {
    let cluster = KafkaTestCluster::start().await.expect("start kafka");
    cluster
        .wait_for_ready(Duration::from_secs(60))
        .await
        .expect("kafka ready");
    let topic = if idempotent {
        "restore-idem-lost-acks"
    } else {
        "restore-plain-lost-acks"
    };
    create_target_topic(&cluster, topic).await;

    let dir = TempDir::new().unwrap();
    let source = source_records(400);
    write_topic_backup(dir.path(), "lost-acks", topic, &source, 150).await;

    let proxy = FaultProxy::start(&cluster.bootstrap_servers).await;
    // 3 partitions x 8 batches of 50: lose the acknowledgement of five of
    // the 24 produce requests, spread over the restore.
    proxy.drop_produce_responses([1, 4, 9, 15, 20]);

    let report = RestoreEngine::new(topic_restore_config(
        &proxy.bootstrap(),
        dir.path(),
        "lost-acks",
        topic,
        restore_options(idempotent),
    ))
    .expect("engine")
    .run()
    .await
    .expect("restore succeeds despite lost responses");
    assert_eq!(proxy.dropped(), 5, "every planned fault fired");
    assert_eq!(report.idempotent_producer, idempotent);

    let client = cluster.create_client();
    client.connect().await.expect("connect");
    let mut target = HashMap::new();
    for p in 0..PARTITIONS {
        target.insert(p, read_partition(&client, topic, p).await);
    }
    (source, target)
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn lost_produce_responses_leave_zero_duplicates_with_the_idempotent_producer() {
    let (source, target) = restore_with_lost_responses(true).await;
    for p in 0..PARTITIONS {
        assert_eq!(
            target[&p].len(),
            source[&p].len(),
            "partition {p}: exact record count"
        );
        assert_eq!(
            digest(&target[&p]),
            digest(&source[&p]),
            "partition {p}: content and order"
        );
    }
}

/// Control: the same faults without idempotence duplicate the retried
/// batches, so the test above does exercise the retry path.
#[tokio::test]
#[ignore = "requires Docker"]
async fn lost_produce_responses_duplicate_batches_without_idempotence() {
    let (source, target) = restore_with_lost_responses(false).await;
    let source_total: usize = source.values().map(Vec::len).sum();
    let target_total: usize = target.values().map(Vec::len).sum();
    assert!(
        target_total > source_total,
        "expected duplicates without idempotence: {target_total} vs {source_total}"
    );
}
