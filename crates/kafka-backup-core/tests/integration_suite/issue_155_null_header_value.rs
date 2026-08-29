//! Issue #155 — a header with a **null** value must be backed up and restored
//! as null, not as an empty value.
//!
//! Kafka distinguishes the two (`-1` vs `0` length on the wire) and so does
//! the binary segment format; the loss happened in memory, in
//! `fetch.rs::convert_record`, before the record ever reached the segment
//! writer — so every archive written by 0.17.x carried `value_len == 0` for
//! every null header, and no restore-side change could recover that.
//!
//! This test drives the whole pipeline against a real broker: produce records
//! carrying null, empty and populated header values → `BackupEngine` →
//! `RestoreEngine` into a mapped topic → fetch from the restored topic and
//! compare the header lists byte-for-byte with the source.
//!
//! Requires Docker.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, RestoreOptions, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::kafka::{KafkaClient, TopicToCreate};
use kafka_backup_core::manifest::{BackupRecord, RecordHeader};
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::storage::StorageBackendConfig;

use super::common::{create_temp_storage, KafkaTestCluster};

const SOURCE_TOPIC: &str = "issue-155-src";
const TARGET_TOPIC: &str = "issue-155-restored";
const BACKUP_ID: &str = "issue-155-backup";
const RECORDS: usize = 40;

fn header(key: &str, value: Option<&[u8]>) -> RecordHeader {
    RecordHeader {
        key: key.to_string(),
        value: value.map(|v| v.to_vec()),
    }
}

/// Record `i` carries a null `trace-id` for even `i`, a populated one for odd
/// `i`, always an *empty* (not null) `empty` header, and a null key / value
/// every 7th / 11th record so the existing null handling is exercised too.
fn source_records() -> Vec<BackupRecord> {
    (0..RECORDS)
        .map(|i| {
            let trace = format!("tid-{i}");
            BackupRecord {
                key: (i % 7 != 0).then(|| format!("k-{i}").into_bytes()),
                value: (i % 11 != 0).then(|| format!("v-{i}").into_bytes()),
                headers: vec![
                    header(
                        "trace-id",
                        if i % 2 == 0 {
                            None
                        } else {
                            Some(trace.as_bytes())
                        },
                    ),
                    header("empty", Some(b"")),
                    header("tenant", Some(b"42")),
                ],
                timestamp: 1_700_000_000_000 + i as i64,
                offset: i as i64,
            }
        })
        .collect()
}

fn backup_config(bs: &str, storage: PathBuf, include_offset_headers: bool) -> Config {
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
            include_offset_headers,
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
            ..RestoreOptions::default()
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

/// Fetch every record of partition 0 in offset order.
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

fn header_view(r: &BackupRecord) -> Vec<(&str, Option<&[u8]>)> {
    r.headers
        .iter()
        .map(|h| (h.key.as_str(), h.value.as_deref()))
        .collect()
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_null_header_values_survive_backup_and_restore() {
    let cluster = KafkaTestCluster::start().await.expect("start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka ready");
    let bs = cluster.bootstrap_servers.clone();
    let client = cluster.create_client();
    client.connect().await.expect("connect");

    // Source topic with the header mix. Produce through our own client — the
    // produce path is one of the sites under test.
    create_single_partition_topic(&client, SOURCE_TOPIC).await;
    let expected = source_records();
    client
        .produce(SOURCE_TOPIC, 0, expected.clone(), -1, 30_000)
        .await
        .expect("produce");

    // Sanity: the broker hands the null back as null *before* any backup ran.
    let on_source = fetch_all(&client, SOURCE_TOPIC, RECORDS).await;
    assert_eq!(on_source.len(), RECORDS);
    for (want, got) in expected.iter().zip(&on_source) {
        assert_eq!(
            header_view(want),
            header_view(got),
            "source offset {}",
            got.offset
        );
    }
    assert_eq!(
        on_source
            .iter()
            .filter(|r| r.headers[0].value.is_none())
            .count(),
        RECORDS / 2,
        "half the records carry a null trace-id on the source"
    );

    // Backup without Phase 1 headers so the restored header list can be
    // compared with the source verbatim.
    let storage = create_temp_storage();
    let engine = BackupEngine::new(backup_config(&bs, storage.path().to_path_buf(), false))
        .await
        .expect("backup engine");
    tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup");

    // Restore into a fresh topic.
    let report = tokio::time::timeout(
        Duration::from_secs(60),
        RestoreEngine::new(restore_config(&bs, storage.path().to_path_buf()))
            .expect("restore engine")
            .run(),
    )
    .await
    .expect("restore timed out")
    .expect("restore");
    assert_eq!(report.records_restored as usize, RECORDS);

    // Every restored record must carry exactly the source header list —
    // null stays null, empty stays empty, order preserved.
    let restored = fetch_all(&client, TARGET_TOPIC, RECORDS).await;
    assert_eq!(restored.len(), RECORDS);
    for (want, got) in expected.iter().zip(&restored) {
        assert_eq!(want.key, got.key, "key at offset {}", got.offset);
        assert_eq!(want.value, got.value, "value at offset {}", got.offset);
        assert_eq!(
            header_view(want),
            header_view(got),
            "headers at restored offset {}",
            got.offset
        );
    }
    assert_eq!(
        restored
            .iter()
            .filter(|r| r.headers[0].value.is_none())
            .count(),
        RECORDS / 2,
        "null trace-id headers must not be restored as empty"
    );
    assert!(
        restored
            .iter()
            .all(|r| r.headers[1].value.as_deref() == Some(&[][..])),
        "empty header values must stay empty (not become null)"
    );
}

/// With Phase 1 headers on, the backup appends its own headers *after* the
/// record's headers; the user's null header must still be null.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_null_header_values_survive_with_offset_headers_enabled() {
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

    let storage = create_temp_storage();
    let engine = BackupEngine::new(backup_config(&bs, storage.path().to_path_buf(), true))
        .await
        .expect("backup engine");
    tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup");
    tokio::time::timeout(
        Duration::from_secs(60),
        RestoreEngine::new(restore_config(&bs, storage.path().to_path_buf()))
            .expect("restore engine")
            .run(),
    )
    .await
    .expect("restore timed out")
    .expect("restore");

    let restored = fetch_all(&client, TARGET_TOPIC, RECORDS).await;
    assert_eq!(restored.len(), RECORDS);
    for (want, got) in expected.iter().zip(&restored) {
        let got_headers = header_view(got);
        assert_eq!(
            &got_headers[..3],
            &header_view(want)[..],
            "user headers first, verbatim (offset {})",
            got.offset
        );
        let extra: Vec<&str> = got_headers[3..].iter().map(|(k, _)| *k).collect();
        assert_eq!(extra, ["x-original-offset", "x-original-timestamp"]);
        assert_eq!(
            got_headers[3].1,
            Some(&want.offset.to_le_bytes()[..]),
            "x-original-offset is the source offset (offset {})",
            got.offset
        );
    }
}
