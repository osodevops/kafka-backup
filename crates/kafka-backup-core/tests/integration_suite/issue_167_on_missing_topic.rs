//! Issue #167 — `backup.on_missing_topic: warn` skips literal include topics
//! that do not exist, records them in the manifest, and keeps backing up the
//! rest. A run with nothing left to back up still fails, and the default
//! (`fail`) keeps the operator #56 guarantee (see `issue_56_missing_topic`).

use std::time::Duration;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::OnMissingTopic;
use kafka_backup_core::manifest::BackupManifest;

use super::common::{create_backup_config, create_temp_storage, KafkaTestCluster};

#[tokio::test]
#[ignore = "requires Docker"]
async fn warn_mode_skips_missing_topic_and_records_it() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("failed to start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let real_topic = "issue167-real";
    let ghost_topic = "issue167-ghost";
    cluster
        .create_topic(real_topic, 20)
        .await
        .expect("create topic");

    let temp_dir = create_temp_storage();
    let backup_id = "issue167-warn";
    let mut config = create_backup_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        backup_id,
        vec![real_topic.to_string(), ghost_topic.to_string()],
    );
    config
        .backup
        .as_mut()
        .expect("backup options")
        .on_missing_topic = OnMissingTopic::Warn;

    let engine = BackupEngine::new(config)
        .await
        .expect("backup engine should initialize");
    engine
        .run()
        .await
        .expect("warn mode must back up the remaining topics");

    let manifest_path = temp_dir.path().join(backup_id).join("manifest.json");
    let manifest: BackupManifest =
        serde_json::from_slice(&std::fs::read(&manifest_path).expect("manifest written"))
            .expect("manifest parses");

    assert_eq!(
        manifest.missing_topics,
        vec![ghost_topic.to_string()],
        "the absent literal topic is recorded"
    );
    let real = manifest
        .topics
        .iter()
        .find(|t| t.name == real_topic)
        .expect("existing topic backed up");
    let records: i64 = real
        .partitions
        .iter()
        .flat_map(|p| p.segments.iter())
        .map(|s| s.record_count)
        .sum();
    assert_eq!(records, 20, "all records of the existing topic archived");
    assert!(
        !manifest.topics.iter().any(|t| t.name == ghost_topic),
        "no manifest entry for the missing topic"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn warn_mode_still_fails_when_nothing_is_left() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("failed to start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let temp_dir = create_temp_storage();
    let ghost_topic = "issue167-only-ghost";
    let mut config = create_backup_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        "issue167-all-missing",
        vec![ghost_topic.to_string()],
    );
    config
        .backup
        .as_mut()
        .expect("backup options")
        .on_missing_topic = OnMissingTopic::Warn;

    let engine = BackupEngine::new(config)
        .await
        .expect("backup engine should initialize");
    let error = engine
        .run()
        .await
        .expect_err("a run with every topic missing must still fail");
    let message = error.to_string();
    assert!(
        message.contains(ghost_topic) && message.contains("all configured backup topics"),
        "unexpected error: {message}"
    );
}
