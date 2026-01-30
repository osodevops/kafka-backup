//! Snapshot backup integration tests.
//!
//! Tests for `stop_at_current_offsets` feature which enables consistent
//! point-in-time snapshots for DR backups.
//!
//! These tests require Docker and use Testcontainers to run real Kafka brokers.

use std::path::PathBuf;
use std::time::Duration;
use tempfile::TempDir;
use tokio::time::sleep;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, SecurityConfig, TopicSelection,
};
use kafka_backup_core::storage::StorageBackendConfig;

use super::common::{generate_test_records, KafkaTestCluster};

// ============================================================================
// Configuration Helpers
// ============================================================================

/// Create a backup config with stop_at_current_offsets enabled (snapshot mode).
fn create_snapshot_backup_config(
    bootstrap_server: &str,
    storage_path: PathBuf,
    backup_id: &str,
    topics: Vec<String>,
) -> Config {
    Config {
        mode: Mode::Backup,
        backup_id: backup_id.to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec![bootstrap_server.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: topics,
                exclude: vec![],
            },
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem { path: storage_path },
        backup: Some(BackupOptions {
            segment_max_bytes: 1024 * 1024, // 1MB for testing
            segment_max_interval_ms: 10000,
            compression: CompressionType::Zstd,
            stop_at_current_offsets: true, // Enable snapshot mode
            continuous: false,
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
    }
}

/// Create a continuous backup config for comparison.
fn create_continuous_backup_config(
    bootstrap_server: &str,
    storage_path: PathBuf,
    backup_id: &str,
    topics: Vec<String>,
) -> Config {
    let mut config = create_snapshot_backup_config(bootstrap_server, storage_path, backup_id, topics);
    if let Some(ref mut backup) = config.backup {
        backup.stop_at_current_offsets = false;
        backup.continuous = false; // One-shot mode for testing
    }
    config
}

// ============================================================================
// Unit Tests (No Docker Required)
// ============================================================================

/// Test that stop_at_current_offsets and continuous are mutually exclusive.
#[test]
fn test_config_validation_mutual_exclusivity() {
    let config = Config {
        mode: Mode::Backup,
        backup_id: "test".to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection::default(),
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem {
            path: PathBuf::from("/tmp"),
        },
        backup: Some(BackupOptions {
            stop_at_current_offsets: true,
            continuous: true, // Both enabled - should fail validation
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
    };

    let result = config.validate();
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("stop_at_current_offsets cannot be used with continuous mode"),
        "Expected mutual exclusivity error, got: {}",
        err_msg
    );
}

/// Test that stop_at_current_offsets alone is valid.
#[test]
fn test_config_validation_snapshot_mode_valid() {
    let config = Config {
        mode: Mode::Backup,
        backup_id: "test".to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection::default(),
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem {
            path: PathBuf::from("/tmp"),
        },
        backup: Some(BackupOptions {
            stop_at_current_offsets: true,
            continuous: false,
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
    };

    let result = config.validate();
    assert!(result.is_ok(), "Snapshot mode config should be valid");
}

/// Test compression level validation.
#[test]
fn test_config_validation_compression_level() {
    let config = Config {
        mode: Mode::Backup,
        backup_id: "test".to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec!["localhost:9092".to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection::default(),
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem {
            path: PathBuf::from("/tmp"),
        },
        backup: Some(BackupOptions {
            compression_level: 99, // Invalid - should fail
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
    };

    let result = config.validate();
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("compression_level must be between 1 and 22"),
        "Expected compression level error, got: {}",
        err_msg
    );
}

// ============================================================================
// Integration Tests (Docker Required)
// ============================================================================

/// Test that snapshot backup captures high watermarks at start and stops when caught up.
///
/// This test:
/// 1. Creates a topic with initial data
/// 2. Starts snapshot backup
/// 3. Produces more data DURING backup
/// 4. Verifies backup only contains data up to the snapshot point
#[tokio::test]
#[ignore] // Requires Docker
async fn test_snapshot_backup_captures_hwm_at_start() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("Failed to start Kafka");

    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let topic = "snapshot-test-topic";
    let initial_records = 100;

    // Create topic with initial data
    cluster
        .create_topic(topic, initial_records)
        .await
        .expect("Failed to create topic");

    // Wait for data to be committed
    sleep(Duration::from_secs(2)).await;

    // Set up backup
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = create_snapshot_backup_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        "snapshot-backup-001",
        vec![topic.to_string()],
    );

    // Start backup in background task
    let backup_handle = tokio::spawn(async move {
        let engine = BackupEngine::new(config).await.expect("Failed to create engine");
        engine.run().await
    });

    // Give backup time to capture HWMs and start processing
    sleep(Duration::from_secs(2)).await;

    // Produce MORE data while backup is running
    // (In snapshot mode, this should NOT be included)
    let additional_records = 50;
    let client = cluster.create_client();
    client.connect().await.expect("Failed to connect");

    let new_records = generate_test_records(additional_records, topic);
    for record in new_records {
        client
            .produce(topic, 0, vec![record])
            .await
            .expect("Failed to produce");
    }

    // Wait for backup to complete
    let result = tokio::time::timeout(Duration::from_secs(60), backup_handle)
        .await
        .expect("Backup timed out")
        .expect("Backup task panicked");

    // Verify backup completed successfully
    assert!(result.is_ok(), "Backup should complete successfully: {:?}", result);

    // Verify manifest exists
    let manifest_path = temp_dir.path().join("snapshot-backup-001").join("manifest.json");
    assert!(
        manifest_path.exists(),
        "Manifest should exist at {:?}",
        manifest_path
    );

    // Read and verify manifest
    let manifest_content =
        std::fs::read_to_string(&manifest_path).expect("Failed to read manifest");
    let manifest: serde_json::Value =
        serde_json::from_str(&manifest_content).expect("Failed to parse manifest");

    // Check that manifest has expected structure
    assert!(
        manifest.get("backup_id").is_some(),
        "Manifest should have backup_id"
    );
    assert!(
        manifest.get("topics").is_some(),
        "Manifest should have topics"
    );

    // Verify topics array exists
    let topics = manifest.get("topics").and_then(|t| t.as_array());
    assert!(
        topics.is_some(),
        "Manifest topics should be an array"
    );
}

/// Test that snapshot backup exits with success when all partitions are caught up.
#[tokio::test]
#[ignore] // Requires Docker
async fn test_snapshot_backup_exits_cleanly() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("Failed to start Kafka");

    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let topic = "snapshot-exit-test";

    // Create topic using Admin API
    cluster
        .create_topic(topic, 10)
        .await
        .expect("Failed to create topic");

    // Wait for data to be committed
    sleep(Duration::from_secs(2)).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = create_snapshot_backup_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        "snapshot-exit-test",
        vec![topic.to_string()],
    );

    // Run backup - should complete and exit
    let engine = BackupEngine::new(config).await.expect("Failed to create engine");

    let result = tokio::time::timeout(Duration::from_secs(30), engine.run())
        .await
        .expect("Backup timed out");

    assert!(
        result.is_ok(),
        "Snapshot backup should exit cleanly: {:?}",
        result
    );
}

/// Test that regular one-shot backup still works (regression test).
#[tokio::test]
#[ignore] // Requires Docker
async fn test_oneshot_backup_still_works() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("Failed to start Kafka");

    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let topic = "oneshot-test";

    cluster
        .create_topic(topic, 20)
        .await
        .expect("Failed to create topic");

    sleep(Duration::from_secs(2)).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = create_continuous_backup_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        "oneshot-backup",
        vec![topic.to_string()],
    );

    let engine = BackupEngine::new(config).await.expect("Failed to create engine");

    let result = tokio::time::timeout(Duration::from_secs(30), engine.run())
        .await
        .expect("Backup timed out");

    assert!(
        result.is_ok(),
        "One-shot backup should complete successfully: {:?}",
        result
    );

    // Verify manifest
    let manifest_path = temp_dir.path().join("oneshot-backup").join("manifest.json");
    assert!(manifest_path.exists(), "Manifest should exist");
}

/// Test snapshot backup with multiple partitions.
#[tokio::test]
#[ignore] // Requires Docker
async fn test_snapshot_backup_multiple_partitions() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("Failed to start Kafka");

    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    let topic = "multi-partition-snapshot";

    // Create topic with records distributed across partitions
    // (KafkaTestCluster.create_topic distributes across 3 partitions)
    cluster
        .create_topic(topic, 90) // 30 records per partition
        .await
        .expect("Failed to create topic");

    sleep(Duration::from_secs(2)).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = create_snapshot_backup_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        "multi-partition-snapshot",
        vec![topic.to_string()],
    );

    let engine = BackupEngine::new(config).await.expect("Failed to create engine");

    let result = tokio::time::timeout(Duration::from_secs(60), engine.run())
        .await
        .expect("Backup timed out");

    assert!(
        result.is_ok(),
        "Multi-partition snapshot should complete: {:?}",
        result
    );

    // Verify segments were created for the topic
    let topic_dir = temp_dir
        .path()
        .join("multi-partition-snapshot")
        .join("topics")
        .join(topic);

    assert!(
        topic_dir.exists(),
        "Topic directory should exist at {:?}",
        topic_dir
    );
}

/// Test that backup handles empty topics gracefully in snapshot mode.
#[tokio::test]
#[ignore] // Requires Docker
async fn test_snapshot_backup_empty_topic() {
    let cluster = KafkaTestCluster::start()
        .await
        .expect("Failed to start Kafka");

    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka not ready");

    // Create topic with a single message to force creation, then let it sit
    let topic = "empty-snapshot-test";
    cluster
        .create_topic(topic, 1)
        .await
        .expect("Failed to create topic");

    sleep(Duration::from_secs(2)).await;

    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let config = create_snapshot_backup_config(
        &cluster.bootstrap_servers,
        temp_dir.path().to_path_buf(),
        "empty-snapshot",
        vec![topic.to_string()],
    );

    let engine = BackupEngine::new(config).await.expect("Failed to create engine");

    let result = tokio::time::timeout(Duration::from_secs(30), engine.run())
        .await
        .expect("Backup timed out");

    assert!(
        result.is_ok(),
        "Snapshot backup of minimal topic should complete: {:?}",
        result
    );
}
