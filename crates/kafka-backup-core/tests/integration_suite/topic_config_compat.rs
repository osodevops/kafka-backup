//! Restoring captured topic configuration into Kafka 4.x.
//!
//! A backup from an older (or Confluent) cluster carries topic overrides
//! that a Kafka 4.x target does not have (`message.format.version`,
//! `message.downconversion.enable`, `confluent.*`) or rejects. Before, one
//! such key failed IncrementalAlterConfigs for the whole topic and the
//! restore stopped after the topics were created. Now the restore applies
//! what the target takes, skips the rest (`unsupported_config_policy: skip`,
//! the default) and lists it in the report, or fails without changing any
//! configuration (`fail`).
//!
//! Requires Docker (`apache/kafka:4.1.1`; `KAFKA_BACKUP_TEST_APACHE_KAFKA_TAG`
//! selects another tag).

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::kafka::apache;

use kafka_backup_core::config::{
    CompressionType, Config, KafkaConfig, Mode, RestoreOptions, SecurityConfig, TopicSelection,
    UnsupportedConfigPolicy,
};
use kafka_backup_core::kafka::{describe_configs, ConfigResourceType, KafkaClient};
use kafka_backup_core::manifest::{BackupManifest, RestoreReport, TopicConfigSkipReason};
use kafka_backup_core::metrics::PerformanceMetrics;
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::segment::{BinaryRecord, SegmentWriter, SegmentWriterConfig};
use kafka_backup_core::storage::{create_backend, StorageBackendConfig};

const SOURCE_TOPIC: &str = "legacy-configs";
const BACKUP_ID: &str = "topic-config-compat";
const RECORDS: i64 = 10;

/// Overrides captured from a Kafka 3.x / Confluent Platform source.
const CAPTURED: [(&str, &str); 7] = [
    ("flush.ms", "1000"),
    ("preallocate", "true"),
    ("retention.ms", "3600000"),
    // Removed in Kafka 4.0: the target does not report them.
    ("message.format.version", "2.8-IV1"),
    ("message.downconversion.enable", "true"),
    // Confluent-only.
    ("confluent.value.schema.validation", "true"),
    // Reported, but the value is out of range (0..1): rejected.
    ("min.cleanable.dirty.ratio", "1.5"),
];

async fn start_kafka4() -> (ContainerAsync<apache::Kafka>, String) {
    let tag = std::env::var("KAFKA_BACKUP_TEST_APACHE_KAFKA_TAG")
        .ok()
        .filter(|t| !t.is_empty())
        .unwrap_or_else(|| "4.1.1".to_string());
    let container = apache::Kafka::default()
        .with_jvm_image()
        .with_tag(tag)
        .start()
        .await
        .expect("start apache/kafka");
    let port = container
        .get_host_port_ipv4(apache::KAFKA_PORT)
        .await
        .expect("mapped port");
    (container, format!("127.0.0.1:{port}"))
}

fn kafka_config(bootstrap: &str) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![bootstrap.to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection::default(),
        connection: Default::default(),
    }
}

async fn write_backup(root: &std::path::Path, captured: &[(&str, &str)]) {
    let storage = create_backend(&StorageBackendConfig::Filesystem {
        path: root.to_path_buf(),
    })
    .unwrap();
    let mut manifest = BackupManifest::new(BACKUP_ID.to_string());
    manifest.compression = "none".to_string();
    let config = SegmentWriterConfig {
        compression: CompressionType::None,
        max_segment_bytes: 1024 * 1024,
        ..Default::default()
    };
    let mut writer =
        SegmentWriter::new(config, storage.clone(), Arc::new(PerformanceMetrics::new()));
    for offset in 0..RECORDS {
        writer
            .add_record(BinaryRecord {
                timestamp: 1_700_000_000_000 + offset,
                offset,
                key: Some(Bytes::from(format!("k{offset}"))),
                value: Some(Bytes::from(format!("v{offset}"))),
                headers: vec![],
            })
            .unwrap();
    }
    let key = format!(
        "{BACKUP_ID}/topics/{SOURCE_TOPIC}/partition=0/segment-{:020}.bin",
        0
    );
    let meta = writer.flush(&key).await.unwrap().expect("segment");
    let topic = manifest.get_or_create_topic(SOURCE_TOPIC);
    topic.original_partition_count = Some(1);
    topic.configurations = captured
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect::<BTreeMap<_, _>>();
    topic.get_or_create_partition(0).add_segment(meta);
    storage
        .put(
            &format!("{BACKUP_ID}/manifest.json"),
            Bytes::from(serde_json::to_vec_pretty(&manifest).unwrap()),
        )
        .await
        .unwrap();
}

async fn restore(
    bootstrap: &str,
    root: &std::path::Path,
    target_topic: &str,
    policy: UnsupportedConfigPolicy,
) -> kafka_backup_core::Result<RestoreReport> {
    let config = Config {
        mode: Mode::Restore,
        backup_id: BACKUP_ID.to_string(),
        source: None,
        target: Some(kafka_config(bootstrap)),
        storage: StorageBackendConfig::Filesystem {
            path: root.to_path_buf(),
        },
        backup: None,
        restore: Some(RestoreOptions {
            topic_mapping: HashMap::from([(SOURCE_TOPIC.to_string(), target_topic.to_string())]),
            create_topics: true,
            restore_topic_configs: true,
            unsupported_config_policy: policy,
            ..Default::default()
        }),
        offset_storage: None,
        metrics: None,
    };
    tokio::time::timeout(
        Duration::from_secs(90),
        RestoreEngine::new(config).expect("engine").run(),
    )
    .await
    .expect("restore timed out")
}

/// The target topic's explicit (topic-level) overrides.
async fn topic_overrides(bootstrap: &str, topic: &str) -> BTreeMap<String, String> {
    let client = KafkaClient::new(kafka_config(bootstrap));
    client.connect().await.expect("connect");
    let described = describe_configs(&client, &[(ConfigResourceType::Topic, topic.to_string())])
        .await
        .expect("describe configs");
    described[&(ConfigResourceType::Topic, topic.to_string())]
        .iter()
        .filter(|e| e.is_topic_override())
        .filter_map(|e| e.value.clone().map(|v| (e.name.clone(), v)))
        .collect()
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka4_restore_applies_what_the_target_takes_and_reports_the_rest() {
    let (_kafka, bootstrap) = start_kafka4().await;
    let storage = tempfile::TempDir::new().unwrap();
    write_backup(storage.path(), &CAPTURED).await;

    // skip (default): the restore succeeds and lists the skipped keys.
    let target = "legacy-configs-skip";
    let report = restore(
        &bootstrap,
        storage.path(),
        target,
        UnsupportedConfigPolicy::Skip,
    )
    .await
    .expect("restore succeeds despite unsupported topic configs");
    assert_eq!(report.records_restored, RECORDS as u64);

    let mut skipped: Vec<_> = report
        .topic_configs_skipped
        .iter()
        .map(|s| (s.name.as_str(), s.reason))
        .collect();
    skipped.sort_by_key(|(name, _)| *name);
    assert_eq!(
        skipped,
        [
            (
                "confluent.value.schema.validation",
                TopicConfigSkipReason::NotReportedByTarget
            ),
            (
                "message.downconversion.enable",
                TopicConfigSkipReason::NotReportedByTarget
            ),
            (
                "message.format.version",
                TopicConfigSkipReason::NotReportedByTarget
            ),
            ("min.cleanable.dirty.ratio", TopicConfigSkipReason::Rejected),
        ],
        "{:#?}",
        report.topic_configs_skipped
    );
    let rejected = report
        .topic_configs_skipped
        .iter()
        .find(|s| s.reason == TopicConfigSkipReason::Rejected)
        .unwrap();
    assert_eq!(rejected.topic, target);
    let message = rejected.message.as_deref().unwrap();
    assert!(
        message.contains("InvalidConfig") && message.contains("min.cleanable.dirty.ratio"),
        "the broker's message is kept: {message}"
    );
    assert!(report.topic_configs_adjusted.is_empty());

    let overrides = topic_overrides(&bootstrap, target).await;
    for (name, value) in [
        ("flush.ms", "1000"),
        ("preallocate", "true"),
        ("retention.ms", "3600000"),
    ] {
        assert_eq!(
            overrides.get(name).map(String::as_str),
            Some(value),
            "{name} applied: {overrides:?}"
        );
    }
    assert!(!overrides.contains_key("min.cleanable.dirty.ratio"));

    // The report serialises the skipped keys for operators and tooling.
    let json = serde_json::to_value(&report).unwrap();
    assert_eq!(
        json["topic_configs_skipped"].as_array().map(Vec::len),
        Some(4)
    );
    assert_eq!(
        json["topic_configs_skipped"][0]["reason"],
        "not_reported_by_target"
    );

    // fail: the restore stops before changing any configuration.
    let target = "legacy-configs-fail";
    let err = restore(
        &bootstrap,
        storage.path(),
        target,
        UnsupportedConfigPolicy::Fail,
    )
    .await
    .expect_err("fail policy stops the restore")
    .to_string();
    assert!(err.contains("unsupported_config_policy: fail"), "{err}");
    assert!(err.contains("nothing was changed"), "{err}");
    assert!(
        err.contains("message.format.version=2.8-IV1 (not reported by the target)"),
        "{err}"
    );
    assert!(
        topic_overrides(&bootstrap, target).await.is_empty(),
        "no configuration applied under fail"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn kafka4_fail_policy_validates_before_applying() {
    let (_kafka, bootstrap) = start_kafka4().await;
    let storage = tempfile::TempDir::new().unwrap();
    // Every key is reported; one value is rejected by the broker.
    write_backup(
        storage.path(),
        &[("flush.ms", "1000"), ("min.cleanable.dirty.ratio", "1.5")],
    )
    .await;

    let target = "legacy-configs-validate";
    let err = restore(
        &bootstrap,
        storage.path(),
        target,
        UnsupportedConfigPolicy::Fail,
    )
    .await
    .expect_err("a rejected key fails the restore")
    .to_string();
    assert!(
        err.contains("min.cleanable.dirty.ratio=1.5 (rejected: error_code=40 (InvalidConfig)"),
        "{err}"
    );
    assert!(!err.contains("flush.ms"), "flush.ms validated: {err}");
    assert!(
        topic_overrides(&bootstrap, target).await.is_empty(),
        "validate-only: flush.ms was not applied either"
    );

    // The same backup under skip: flush.ms applied, the ratio skipped.
    let target = "legacy-configs-validate-skip";
    let report = restore(
        &bootstrap,
        storage.path(),
        target,
        UnsupportedConfigPolicy::Skip,
    )
    .await
    .expect("skip policy restores");
    assert_eq!(report.topic_configs_skipped.len(), 1);
    assert_eq!(
        topic_overrides(&bootstrap, target).await,
        BTreeMap::from([("flush.ms".to_string(), "1000".to_string())])
    );
}
