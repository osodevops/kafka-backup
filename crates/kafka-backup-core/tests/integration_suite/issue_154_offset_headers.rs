//! Issue #154 — `backup.include_offset_headers` defaults to `true`, so every
//! archived record gains `x-original-offset` / `x-original-timestamp` and a
//! restore is never header-for-header identical to the source unless the
//! backup was taken with the option off. `restore.strip_offset_headers`
//! removes those headers from an archive that already carries them.
//!
//! Requires Docker.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, OffsetStrategy, RestoreOptions,
    SecurityConfig, TopicSelection,
};
use kafka_backup_core::kafka::{KafkaClient, TopicToCreate};
use kafka_backup_core::manifest::{BackupRecord, RecordHeader};
use kafka_backup_core::offset_headers;
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::storage::StorageBackendConfig;

use super::common::{create_temp_storage, KafkaTestCluster};

const SOURCE_TOPIC: &str = "issue-154-src";
const BACKUP_ID: &str = "issue-154-backup";
const RECORDS: usize = 30;

fn header(key: &str, value: Option<&[u8]>) -> RecordHeader {
    RecordHeader {
        key: key.to_string(),
        value: value.map(|v| v.to_vec()),
    }
}

/// The reporter's records: `event-type:created, tenant:42`, plus a null one.
fn source_records() -> Vec<BackupRecord> {
    (0..RECORDS)
        .map(|i| BackupRecord {
            key: Some(format!("k-{i}").into_bytes()),
            value: Some(format!("v-{i}").into_bytes()),
            headers: vec![
                header("event-type", Some(b"created")),
                header("tenant", Some(b"42")),
                header("trace-id", None),
            ],
            timestamp: 1_700_000_000_000 + i as i64,
            offset: i as i64,
        })
        .collect()
}

/// Backup config that leaves `include_offset_headers` at its default.
fn backup_config(bs: &str, storage: PathBuf) -> Config {
    let backup = BackupOptions {
        segment_max_bytes: 1024 * 1024,
        segment_max_interval_ms: 5000,
        compression: CompressionType::Zstd,
        stop_at_current_offsets: true,
        continuous: false,
        ..Default::default()
    };
    assert!(
        backup.include_offset_headers,
        "the default under test is `true`"
    );
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
        backup: Some(backup),
        restore: None,
        offset_storage: None,
        metrics: None,
    }
}

fn restore_config(bs: &str, storage: PathBuf, target: &str, restore: RestoreOptions) -> Config {
    let mut topic_mapping = HashMap::new();
    topic_mapping.insert(SOURCE_TOPIC.to_string(), target.to_string());
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
            ..restore
        }),
        offset_storage: None,
        metrics: None,
    }
}

async fn create_single_partition_topic(client: &KafkaClient, name: &str) {
    let results = client
        .create_topics(
            vec![TopicToCreate {
                name: name.to_string(),
                num_partitions: 1,
                replication_factor: 1,
            }],
            30_000,
        )
        .await
        .expect("create topic");
    assert!(
        results.iter().all(|r| r.is_success_or_exists()),
        "create {name}: {results:?}"
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
}

async fn fetch_all(client: &KafkaClient, topic: &str, expected: usize) -> Vec<BackupRecord> {
    let mut out = Vec::new();
    let mut offset = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while out.len() < expected && tokio::time::Instant::now() < deadline {
        let resp = client
            .fetch(topic, 0, offset, 8 * 1024 * 1024)
            .await
            .expect("fetch");
        if resp.records.is_empty() {
            tokio::time::sleep(Duration::from_millis(200)).await;
            continue;
        }
        offset = resp.next_offset;
        out.extend(resp.records);
    }
    out
}

fn header_keys(r: &BackupRecord) -> Vec<&str> {
    r.headers.iter().map(|h| h.key.as_str()).collect()
}

async fn restore_to(bs: &str, storage: PathBuf, target: &str, restore: RestoreOptions) {
    let report = tokio::time::timeout(
        Duration::from_secs(60),
        RestoreEngine::new(restore_config(bs, storage, target, restore))
            .expect("restore engine")
            .run(),
    )
    .await
    .expect("restore timed out")
    .expect("restore");
    assert_eq!(report.records_restored as usize, RECORDS);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_default_backup_adds_offset_headers_and_strip_restores_them_verbatim() {
    let cluster = KafkaTestCluster::start().await.expect("start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka ready");
    let bs = cluster.bootstrap_servers.clone();
    let client = cluster.create_client();
    client.connect().await.expect("connect");

    create_single_partition_topic(&client, SOURCE_TOPIC).await;
    let expected = source_records();
    client
        .produce(SOURCE_TOPIC, 0, expected.clone(), -1, 30_000)
        .await
        .expect("produce");

    // Backup with defaults.
    let storage = create_temp_storage();
    let engine = BackupEngine::new(backup_config(&bs, storage.path().to_path_buf()))
        .await
        .expect("backup engine");
    tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup");

    // 1. Plain restore: the reporter's observation — exactly two extra
    //    headers, appended after the record's own, and no x-source-partition
    //    (that one is restore-side only and off by default).
    restore_to(
        &bs,
        storage.path().to_path_buf(),
        "issue-154-plain",
        RestoreOptions::default(),
    )
    .await;
    let plain = fetch_all(&client, "issue-154-plain", RECORDS).await;
    assert_eq!(plain.len(), RECORDS);
    for (want, got) in expected.iter().zip(&plain) {
        assert_eq!(
            header_keys(got),
            [
                "event-type",
                "tenant",
                "trace-id",
                offset_headers::X_ORIGINAL_OFFSET,
                offset_headers::X_ORIGINAL_TIMESTAMP,
            ],
            "plain restore, offset {}",
            got.offset
        );
        assert_eq!(&got.headers[..3], &want.headers[..]);
        assert_eq!(
            got.headers[3].value.as_deref(),
            Some(&want.offset.to_le_bytes()[..]),
            "x-original-offset carries the source offset"
        );
    }

    // 2. strip_offset_headers: header-for-header identical to the source.
    restore_to(
        &bs,
        storage.path().to_path_buf(),
        "issue-154-stripped",
        RestoreOptions {
            strip_offset_headers: true,
            ..RestoreOptions::default()
        },
    )
    .await;
    let stripped = fetch_all(&client, "issue-154-stripped", RECORDS).await;
    assert_eq!(stripped.len(), RECORDS);
    for (want, got) in expected.iter().zip(&stripped) {
        assert_eq!(want.key, got.key);
        assert_eq!(want.value, got.value);
        assert_eq!(
            want.headers, got.headers,
            "stripped restore must match the source exactly (offset {})",
            got.offset
        );
    }

    // 3. strip + header-based strategy: the archived headers are replaced by
    //    exactly one fresh set of restore-side headers, and the offset
    //    mapping still points at the right source offsets.
    restore_to(
        &bs,
        storage.path().to_path_buf(),
        "issue-154-rebased",
        RestoreOptions {
            strip_offset_headers: true,
            consumer_group_strategy: OffsetStrategy::HeaderBased,
            ..RestoreOptions::default()
        },
    )
    .await;
    let rebased = fetch_all(&client, "issue-154-rebased", RECORDS).await;
    assert_eq!(rebased.len(), RECORDS);
    for (want, got) in expected.iter().zip(&rebased) {
        assert_eq!(
            header_keys(got),
            [
                "event-type",
                "tenant",
                "trace-id",
                offset_headers::X_ORIGINAL_OFFSET,
                offset_headers::X_ORIGINAL_TIMESTAMP,
                offset_headers::X_SOURCE_PARTITION,
            ],
            "strip + inject, offset {}",
            got.offset
        );
        assert_eq!(
            got.headers[3].value.as_deref(),
            Some(&want.offset.to_le_bytes()[..])
        );
        assert_eq!(
            got.headers[5].value.as_deref(),
            Some(&0i32.to_le_bytes()[..])
        );
    }
}
