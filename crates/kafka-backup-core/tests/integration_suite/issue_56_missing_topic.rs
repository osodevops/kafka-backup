//! Regression coverage for kafka-backup-operator issue #56.
//!
//! A one-shot backup of an explicitly requested topic must fail when that
//! topic is absent. Returning success with zero records misleads the operator
//! into marking the KafkaBackup as Completed.

use std::time::Duration;

use kafka_backup_core::backup::BackupEngine;

use super::common::{create_backup_config, create_temp_storage, KafkaTestCluster};

#[tokio::test]
#[ignore = "requires Docker"]
async fn missing_literal_topic_fails_backup() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("failed to start Kafka");

    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let temp_dir = create_temp_storage();
    let missing_topic = "issue56-topic-does-not-exist";
    let config = create_backup_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        "issue56-missing-topic",
        vec![missing_topic.to_string()],
    );

    let engine = BackupEngine::new(config)
        .await
        .expect("backup engine should initialize");

    let error = engine
        .run()
        .await
        .expect_err("missing literal topic must fail backup");

    let message = error.to_string();
    assert!(
        message.contains(missing_topic),
        "error should name missing topic '{missing_topic}', got: {message}"
    );
}
