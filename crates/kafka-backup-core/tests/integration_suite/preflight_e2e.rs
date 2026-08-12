//! Issue #137 Docker E2E: a failed header preflight leaves the target
//! cluster completely unchanged.
//!
//! Scenario: a backup taken WITHOUT Phase 1 tracking headers
//! (`include_offset_headers: false`) is restored three-phase with
//! consumer-offset recovery requested. The preflight must fail before any
//! target mutation: the target topic is never created and no record is
//! produced. The control run proves the same pipeline mutates the target
//! once the backup carries full header coverage.

use std::time::Duration;

use tokio::time::sleep;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, RestoreOptions, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::storage::StorageBackendConfig;
use kafka_backup_core::{Error, ThreePhaseRestore};

use super::common::KafkaTestCluster;

fn backup_config(
    bootstrap: &str,
    storage_path: std::path::PathBuf,
    backup_id: &str,
    topic: &str,
    include_offset_headers: bool,
) -> Config {
    Config {
        mode: Mode::Backup,
        backup_id: backup_id.to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec![bootstrap.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: vec![topic.to_string()],
                exclude: vec![],
            },
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem { path: storage_path },
        backup: Some(BackupOptions {
            segment_max_bytes: 1024 * 1024,
            segment_max_interval_ms: 10_000,
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

fn three_phase_config(
    bootstrap: &str,
    storage_path: std::path::PathBuf,
    backup_id: &str,
    source_topic: &str,
    target_topic: &str,
) -> Config {
    let mut restore = RestoreOptions {
        create_topics: true,
        reset_consumer_offsets: true,
        consumer_groups: vec!["preflight-cg".to_string()],
        ..RestoreOptions::default()
    };
    restore
        .topic_mapping
        .insert(source_topic.to_string(), target_topic.to_string());

    Config {
        mode: Mode::Restore,
        backup_id: backup_id.to_string(),
        source: None,
        target: Some(KafkaConfig {
            bootstrap_servers: vec![bootstrap.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection::default(),
            connection: Default::default(),
        }),
        storage: StorageBackendConfig::Filesystem { path: storage_path },
        backup: None,
        restore: Some(restore),
        offset_storage: None,
        metrics: None,
    }
}

/// Fetch ALL topic names via a full-metadata request. Never queries a single
/// topic, because the test broker runs with auto.create.topics.enable=true
/// and a single-topic metadata request would create the topic it checks for.
async fn cluster_topics(cluster: &KafkaTestCluster) -> Vec<String> {
    let client = cluster.create_client();
    client.connect().await.expect("connect for metadata");
    client
        .fetch_metadata(None)
        .await
        .expect("fetch metadata")
        .iter()
        .map(|t| t.name.clone())
        .collect()
}

#[tokio::test]
#[ignore] // Requires Docker
async fn preflight_failure_leaves_target_unchanged() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("Failed to start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let source_topic = "preflight-source";
    let blocked_target = "preflight-blocked-restore";
    let backup_id = "preflight-e2e-no-headers";

    cluster
        .create_topic(source_topic, 60)
        .await
        .expect("create source topic");
    sleep(Duration::from_secs(2)).await;

    // Backup WITHOUT Phase 1 tracking headers: a legacy-style backup.
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let engine = BackupEngine::new(backup_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        backup_id,
        source_topic,
        false,
    ))
    .await
    .expect("backup engine");
    tokio::time::timeout(Duration::from_secs(120), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup failed");

    // Three-phase restore with consumer-offset recovery requested must fail
    // the preflight before touching the target.
    let orchestrator = ThreePhaseRestore::new(three_phase_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        backup_id,
        source_topic,
        blocked_target,
    ))
    .expect("orchestrator");

    let err = tokio::time::timeout(Duration::from_secs(120), orchestrator.run_all_phases())
        .await
        .expect("three-phase timed out")
        .expect_err("restore must fail the preflight");
    assert!(
        matches!(err, Error::Preflight(_)),
        "expected Error::Preflight, got: {err:?}"
    );
    assert!(err.to_string().contains("no topics were created"));

    // The target cluster is unchanged: the mapped target topic was never
    // created, so no record can have been produced to it either.
    let topics = cluster_topics(&cluster).await;
    assert!(
        !topics.iter().any(|t| t == blocked_target),
        "failed preflight must not create the target topic; cluster topics: {topics:?}"
    );
}

#[tokio::test]
#[ignore] // Requires Docker
async fn preflight_pass_restores_target_control() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("Failed to start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let source_topic = "preflight-source-ok";
    let restored_target = "preflight-restored-ok";
    let backup_id = "preflight-e2e-with-headers";

    cluster
        .create_topic(source_topic, 60)
        .await
        .expect("create source topic");
    sleep(Duration::from_secs(2)).await;

    // Backup WITH Phase 1 tracking headers (the DR default).
    let temp_dir = tempfile::TempDir::new().expect("temp dir");
    let engine = BackupEngine::new(backup_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        backup_id,
        source_topic,
        true,
    ))
    .await
    .expect("backup engine");
    tokio::time::timeout(Duration::from_secs(120), engine.run())
        .await
        .expect("backup timed out")
        .expect("backup failed");

    // The same three-phase pipeline now passes the preflight and mutates the
    // target — proving the failure assertions above detect real mutations.
    let orchestrator = ThreePhaseRestore::new(three_phase_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        backup_id,
        source_topic,
        restored_target,
    ))
    .expect("orchestrator");

    let report = tokio::time::timeout(Duration::from_secs(180), orchestrator.run_all_phases())
        .await
        .expect("three-phase timed out")
        .expect("three-phase restore failed");

    let preflight = report
        .header_preflight
        .as_ref()
        .expect("preflight report attached");
    assert!(preflight.passed, "errors: {:?}", preflight.errors);
    assert!(preflight.scan_performed);
    assert!(preflight.records_scanned_total >= 60);
    assert!(preflight
        .partitions
        .iter()
        .all(|p| p.records_with_required_headers == p.records_scanned));

    assert!(report.success, "warnings: {:?}", report.warnings);
    assert_eq!(report.restore_report.records_restored, 60);

    let topics = cluster_topics(&cluster).await;
    assert!(
        topics.iter().any(|t| t == restored_target),
        "control restore must create the target topic; cluster topics: {topics:?}"
    );
}
