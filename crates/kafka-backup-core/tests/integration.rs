//! Integration tests for kafka-backup.

use std::path::PathBuf;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::kafka::Kafka;
use tempfile::TempDir;
use tokio::time::{sleep, Duration};

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, RestoreOptions, SecurityConfig,
    StorageBackendType, StorageConfig, TopicSelection,
};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::manifest::BackupRecord;
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::storage::{FilesystemBackend, StorageBackend};
use kafka_backup_core::BackupManifest;

// ============================================================================
// Test Helpers
// ============================================================================

/// Start a Kafka container for testing
async fn start_kafka() -> ContainerAsync<Kafka> {
    let kafka = Kafka::default()
        .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");

    kafka.start().await.expect("Failed to start Kafka container")
}

/// Get the bootstrap server address for a Kafka container
async fn get_bootstrap_server(container: &ContainerAsync<Kafka>) -> String {
    let host = container.get_host().await.expect("Failed to get host");
    let port = container
        .get_host_port_ipv4(9093)
        .await
        .expect("Failed to get port");
    format!("{}:{}", host, port)
}

/// Create a Kafka client for testing
fn create_test_client(bootstrap_server: &str) -> KafkaClient {
    let config = KafkaConfig {
        bootstrap_servers: vec![bootstrap_server.to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection::default(),
    };
    KafkaClient::new(config)
}

/// Wait for Kafka to be ready
async fn wait_for_kafka(client: &KafkaClient) {
    for _ in 0..30 {
        if client.connect().await.is_ok() {
            // Try fetching metadata to ensure Kafka is fully ready
            if client.fetch_metadata(None).await.is_ok() {
                return;
            }
        }
        sleep(Duration::from_secs(1)).await;
    }
    panic!("Kafka did not become ready in time");
}

/// Create a backup configuration for testing
fn create_backup_config(
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
        }),
        target: None,
        storage: StorageConfig {
            backend: StorageBackendType::Filesystem,
            path: Some(storage_path),
            endpoint: None,
            bucket: None,
            access_key: None,
            secret_key: None,
            prefix: None,
            region: None,
        },
        backup: Some(BackupOptions {
            segment_max_bytes: 1024 * 1024, // 1MB for testing
            segment_max_interval_ms: 10000,
            compression: CompressionType::Zstd,
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
    }
}

/// Create a restore configuration for testing
fn create_restore_config(
    bootstrap_server: &str,
    storage_path: PathBuf,
    backup_id: &str,
    topics: Vec<String>,
) -> Config {
    Config {
        mode: Mode::Restore,
        backup_id: backup_id.to_string(),
        source: None,
        target: Some(KafkaConfig {
            bootstrap_servers: vec![bootstrap_server.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: topics,
                exclude: vec![],
            },
        }),
        storage: StorageConfig {
            backend: StorageBackendType::Filesystem,
            path: Some(storage_path),
            endpoint: None,
            bucket: None,
            access_key: None,
            secret_key: None,
            prefix: None,
            region: None,
        },
        backup: None,
        restore: Some(RestoreOptions::default()),
        offset_storage: None,
    }
}

/// Generate test records
fn generate_test_records(count: usize, topic: &str) -> Vec<BackupRecord> {
    (0..count)
        .map(|i| BackupRecord {
            key: Some(format!("key-{}", i).into_bytes()),
            value: Some(format!("value-{} for topic {}", i, topic).into_bytes()),
            headers: vec![],
            timestamp: chrono::Utc::now().timestamp_millis() + i as i64,
            offset: i as i64,
        })
        .collect()
}

// ============================================================================
// Integration Tests
// ============================================================================

/// Test basic backup and restore flow
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_backup_and_restore() {
    // Start Kafka container
    let kafka = start_kafka().await;
    let bootstrap_server = get_bootstrap_server(&kafka).await;

    // Create test client and wait for Kafka to be ready
    let client = create_test_client(&bootstrap_server);
    wait_for_kafka(&client).await;

    // Create test topic by producing a message
    let test_topic = "test-backup-restore";
    let test_records = generate_test_records(100, test_topic);

    // Produce test records
    client.connect().await.expect("Failed to connect");
    for record in &test_records {
        client
            .produce(test_topic, 0, vec![record.clone()])
            .await
            .expect("Failed to produce record");
    }

    // Create temp directory for backup storage
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let storage_path = temp_dir.path().to_path_buf();

    // Run backup
    let backup_id = "test-backup-001";
    let backup_config = create_backup_config(
        &bootstrap_server,
        storage_path.clone(),
        backup_id,
        vec![test_topic.to_string()],
    );

    let backup_engine = BackupEngine::new(backup_config).await.expect("Failed to create backup engine");
    backup_engine.run().await.expect("Backup failed");

    // Verify backup was created
    let storage = FilesystemBackend::new(storage_path.clone());
    let manifest_key = format!("{}/manifest.json", backup_id);
    let manifest_data = storage
        .get(&manifest_key)
        .await
        .expect("Failed to read manifest");
    let manifest: BackupManifest =
        serde_json::from_slice(&manifest_data).expect("Failed to parse manifest");

    assert_eq!(manifest.backup_id, backup_id);
    assert!(!manifest.topics.is_empty());

    // Create a different topic for restore
    let restore_topic = "test-restore-target";

    // Run restore
    let mut restore_config = create_restore_config(
        &bootstrap_server,
        storage_path,
        backup_id,
        vec![test_topic.to_string()],
    );

    // Remap topic
    if let Some(ref mut restore_opts) = restore_config.restore {
        restore_opts
            .topic_mapping
            .insert(test_topic.to_string(), restore_topic.to_string());
    }

    let restore_engine =
        RestoreEngine::new(restore_config).expect("Failed to create restore engine");
    restore_engine.run().await.expect("Restore failed");

    // Verify restored data
    // Note: In a full test, we'd consume from restore_topic and verify records match
}

/// Test backup with compression options
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_backup_compression() {
    let kafka = start_kafka().await;
    let bootstrap_server = get_bootstrap_server(&kafka).await;

    let client = create_test_client(&bootstrap_server);
    wait_for_kafka(&client).await;

    let test_topic = "test-compression";
    let test_records = generate_test_records(50, test_topic);

    client.connect().await.expect("Failed to connect");
    for record in &test_records {
        client
            .produce(test_topic, 0, vec![record.clone()])
            .await
            .expect("Failed to produce record");
    }

    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    // Test with Zstd compression
    let backup_id = "test-backup-zstd";
    let mut config = create_backup_config(
        &bootstrap_server,
        temp_dir.path().to_path_buf(),
        backup_id,
        vec![test_topic.to_string()],
    );

    if let Some(ref mut backup_opts) = config.backup {
        backup_opts.compression = CompressionType::Zstd;
    }

    let engine = BackupEngine::new(config).await.expect("Failed to create backup engine");
    engine.run().await.expect("Backup failed");

    // Verify compressed segments exist
    let storage = FilesystemBackend::new(temp_dir.path().to_path_buf());
    let files = storage.list(backup_id).await.expect("Failed to list files");

    let has_zst_files = files.iter().any(|f| f.ends_with(".zst"));
    assert!(has_zst_files, "Expected .zst compressed segment files");
}

/// Test listing backups (no Docker required)
#[tokio::test]
async fn test_list_backups() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let storage = FilesystemBackend::new(temp_dir.path().to_path_buf());

    // Create some mock backup manifests
    let manifest1 = BackupManifest::new("backup-001".to_string());
    let manifest2 = BackupManifest::new("backup-002".to_string());

    storage
        .put(
            "backup-001/manifest.json",
            bytes::Bytes::from(serde_json::to_vec(&manifest1).unwrap()),
        )
        .await
        .expect("Failed to write manifest 1");

    storage
        .put(
            "backup-002/manifest.json",
            bytes::Bytes::from(serde_json::to_vec(&manifest2).unwrap()),
        )
        .await
        .expect("Failed to write manifest 2");

    // List backups
    let files = storage.list("").await.expect("Failed to list files");
    let backup_ids: Vec<_> = files
        .iter()
        .filter(|f| f.ends_with("/manifest.json"))
        .map(|f| f.trim_end_matches("/manifest.json"))
        .collect();

    assert_eq!(backup_ids.len(), 2);
    assert!(backup_ids.contains(&"backup-001"));
    assert!(backup_ids.contains(&"backup-002"));
}
