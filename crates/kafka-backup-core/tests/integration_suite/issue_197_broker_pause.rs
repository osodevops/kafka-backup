//! Issue #197 — broker pause mid-restore must not stall for the circuit
//! breaker's `reset_timeout`.
//!
//! Uses `docker pause` / `docker unpause` against the Testcontainers Kafka
//! container. The restore continues after the broker resumes; wall-clock
//! stays well under the advisory breaker's default 30s open window.
//!
//! Requires Docker (`#[ignore]`).

use std::path::PathBuf;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::circuit_breaker::CircuitState;
use kafka_backup_core::config::{
    BackupOptions, CircuitBreakerSettings, CompressionType, Config, KafkaConfig, Mode,
    RestoreOptions, SecurityConfig, TopicSelection,
};
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::storage::StorageBackendConfig;

use super::common::{create_temp_storage, KafkaTestCluster};

const TOPIC: &str = "issue-197-pause";
const BACKUP_ID: &str = "issue-197-pause-backup";
const RECORDS: usize = 300;

fn backup_config(bs: &str, storage: PathBuf) -> Config {
    Config {
        mode: Mode::Backup,
        backup_id: BACKUP_ID.to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec![bs.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: vec![TOPIC.to_string()],
                exclude: vec![],
            },
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem { path: storage },
        backup: Some(BackupOptions {
            segment_max_bytes: 64 * 1024,
            segment_max_interval_ms: 5000,
            compression: CompressionType::None,
            stop_at_current_offsets: true,
            continuous: false,
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
    }
}

fn restore_config(bs: &str, storage: PathBuf) -> Config {
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
            create_topics: true,
            restore_topic_configs: false,
            max_concurrent_partitions: 3,
            produce_batch_size: 20,
            // Slow enough that a 3s pause lands mid-restore.
            rate_limit_records_per_sec: Some(80),
            circuit_breaker: CircuitBreakerSettings {
                enabled: true,
                failure_threshold: 5,
                reset_timeout_ms: 30_000,
                success_threshold: 2,
            },
            topic_mapping: std::collections::HashMap::from([(
                TOPIC.to_string(),
                format!("{TOPIC}-restored"),
            )]),
            ..Default::default()
        }),
        offset_storage: None,
        metrics: None,
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn broker_pause_mid_restore_recovers_without_reset_timeout_wait() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("start kafka container");
    cluster
        .wait_for_ready(Duration::from_secs(60))
        .await
        .expect("kafka ready");
    cluster
        .create_topic(TOPIC, RECORDS)
        .await
        .expect("seed topic");

    let storage = create_temp_storage();
    let backup = BackupEngine::new(backup_config(
        &cluster.bootstrap_servers,
        storage.path().to_path_buf(),
    ))
    .await
    .expect("backup engine");
    backup.run().await.expect("backup");

    let container_id = cluster.container.id().to_string();
    let engine = RestoreEngine::new(restore_config(
        &cluster.bootstrap_servers,
        storage.path().to_path_buf(),
    ))
    .expect("restore engine");

    let pause_handle = tokio::spawn(async move {
        sleep(Duration::from_secs(2)).await;
        let status = tokio::process::Command::new("docker")
            .args(["pause", &container_id])
            .status()
            .await
            .expect("docker pause");
        assert!(status.success(), "docker pause failed");
        sleep(Duration::from_secs(3)).await;
        let status = tokio::process::Command::new("docker")
            .args(["unpause", &container_id])
            .status()
            .await
            .expect("docker unpause");
        assert!(status.success(), "docker unpause failed");
    });

    let started = Instant::now();
    let report = engine.run().await.expect("restore after pause");
    let elapsed = started.elapsed();
    let _ = pause_handle.await;

    assert!(
        report.records_restored > 0,
        "restore should have produced records"
    );
    assert_eq!(
        engine.kafka_circuit_state(),
        CircuitState::Closed,
        "advisory breaker should not end Open after a recovered pause"
    );
    // Pause was 3s; router retries with short backoff. Must not wait the
    // full 30s reset_timeout of an opened breaker.
    assert!(
        elapsed < Duration::from_secs(45),
        "restore took {elapsed:?}; expected well under breaker reset_timeout+slack"
    );
}
