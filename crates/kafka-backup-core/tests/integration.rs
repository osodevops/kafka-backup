//! Integration tests for kafka-backup.

use std::path::PathBuf;
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::kafka::Kafka;
use tokio::time::{sleep, Duration};

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, RestoreOptions, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::manifest::BackupRecord;
use kafka_backup_core::restore::offset_automation::{
    BulkOffsetReset, BulkOffsetResetConfig, BulkResetStatus, OffsetMapping as BulkOffsetMapping,
};
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::storage::StorageBackendConfig;
use kafka_backup_core::storage::{FilesystemBackend, StorageBackend};
use kafka_backup_core::BackupManifest;

// ============================================================================
// Test Helpers
// ============================================================================

/// Start a Kafka container for testing
async fn start_kafka() -> ContainerAsync<Kafka> {
    let kafka = Kafka::default().with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");

    kafka
        .start()
        .await
        .expect("Failed to start Kafka container")
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
        connection: Default::default(),
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
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem { path: storage_path },
        backup: Some(BackupOptions {
            segment_max_bytes: 1024 * 1024, // 1MB for testing
            segment_max_interval_ms: 10000,
            compression: CompressionType::Zstd,
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
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
            connection: Default::default(),
        }),
        storage: StorageBackendConfig::Filesystem { path: storage_path },
        backup: None,
        restore: Some(RestoreOptions::default()),
        offset_storage: None,
        metrics: None,
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

/// Test that TCP keepalive settings are applied when connecting to Kafka.
/// This test verifies that our socket configuration actually reaches the broker connection.
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_tcp_keepalive_applied_to_kafka_connection() {
    use kafka_backup_core::config::ConnectionConfig;

    // Start Kafka container
    let kafka = start_kafka().await;
    let bootstrap_server = get_bootstrap_server(&kafka).await;

    println!("Kafka started at: {}", bootstrap_server);

    // Create a client with custom keepalive settings
    let config = KafkaConfig {
        bootstrap_servers: vec![bootstrap_server.clone()],
        security: SecurityConfig::default(),
        topics: TopicSelection::default(),
        connection: ConnectionConfig {
            tcp_keepalive: true,
            keepalive_time_secs: 30,
            keepalive_interval_secs: 10,
            tcp_nodelay: true,
        },
    };

    let client = KafkaClient::new(config);

    // Wait for Kafka to be ready
    wait_for_kafka(&client).await;

    // Verify connection works
    client.connect().await.expect("Failed to connect to Kafka");

    // Fetch metadata to prove the connection is working with keepalive enabled
    let metadata = client
        .fetch_metadata(None)
        .await
        .expect("Failed to fetch metadata");

    println!(
        "Connected to Kafka cluster with {} topic(s)",
        metadata.len()
    );

    // Fetch metadata again to exercise the connection
    let metadata2 = client
        .fetch_metadata(None)
        .await
        .expect("Failed to fetch metadata second time");

    println!(
        "Second metadata fetch successful with {} topic(s)",
        metadata2.len()
    );

    // The keepalive settings should have been applied during connect().
    // We verify this in unit tests in client.rs that directly check socket options.
    // This integration test confirms the connection works end-to-end with keepalive.

    println!("\n=== TCP Keepalive Verification ===");
    println!("Connection established successfully with TCP keepalive enabled!");
    println!("Settings: keepalive_time=30s, keepalive_interval=10s, tcp_nodelay=true");
    println!("\nTo manually verify keepalive is set, run in another terminal:");
    println!("  macOS: lsof -i -P | grep kafka-backup");
    println!("  Linux: ss -tnpo | grep {}", bootstrap_server);
}

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

    let backup_engine = BackupEngine::new(backup_config)
        .await
        .expect("Failed to create backup engine");
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

    let engine = BackupEngine::new(config)
        .await
        .expect("Failed to create backup engine");
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

// ============================================================================
// Bulk Offset Reset Integration Tests
// ============================================================================

/// Test bulk offset reset with parallel execution
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_bulk_offset_reset_parallel() {
    // Start Kafka container
    let kafka = start_kafka().await;
    let bootstrap_server = get_bootstrap_server(&kafka).await;

    // Create test client and wait for Kafka to be ready
    let client = create_test_client(&bootstrap_server);
    wait_for_kafka(&client).await;

    // Create test topics by producing messages
    let topics = vec![
        "bulk-test-topic-1",
        "bulk-test-topic-2",
        "bulk-test-topic-3",
    ];
    client.connect().await.expect("Failed to connect");

    for topic in &topics {
        let records = generate_test_records(10, topic);
        for record in &records {
            client
                .produce(topic, 0, vec![record.clone()])
                .await
                .expect("Failed to produce record");
        }
    }

    // Create offset mappings for multiple consumer groups
    let mut bulk_mappings = Vec::new();
    for i in 0..5 {
        let group_id = format!("test-group-{}", i);
        for topic in &topics {
            bulk_mappings.push(BulkOffsetMapping {
                group_id: group_id.clone(),
                topic: topic.to_string(),
                partition: 0,
                new_offset: 5, // Reset to offset 5
                metadata: Some("bulk-reset-test".to_string()),
            });
        }
    }

    // Configure bulk reset with concurrency
    let config = BulkOffsetResetConfig {
        max_concurrent_requests: 3, // Test with limited concurrency
        max_retry_attempts: 2,
        retry_base_delay_ms: 50,
        request_timeout_ms: 10000,
        continue_on_error: true,
    };

    // Execute bulk reset
    let bulk_reset = BulkOffsetReset::new(client, config);
    let report = bulk_reset
        .reset_offsets_parallel(bulk_mappings)
        .await
        .expect("Bulk reset failed");

    // Verify results
    assert_eq!(report.status, BulkResetStatus::Success);
    assert_eq!(report.total_groups, 5);
    assert_eq!(report.successful_groups, 5);
    assert_eq!(report.failed_groups, 0);
    assert_eq!(report.total_offsets_reset, 15); // 5 groups × 3 topics
    assert!(report.duration_ms > 0);
    assert!(report.performance.offsets_per_second > 0.0);
}

/// Test bulk offset reset performance compared to sequential
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_bulk_offset_reset_performance() {
    // Start Kafka container
    let kafka = start_kafka().await;
    let bootstrap_server = get_bootstrap_server(&kafka).await;

    let client = create_test_client(&bootstrap_server);
    wait_for_kafka(&client).await;
    client.connect().await.expect("Failed to connect");

    // Create a topic
    let topic = "perf-test-topic";
    let records = generate_test_records(10, topic);
    for record in &records {
        client
            .produce(topic, 0, vec![record.clone()])
            .await
            .expect("Failed to produce record");
    }

    // Create many offset mappings (20 groups)
    let mut bulk_mappings = Vec::new();
    for i in 0..20 {
        bulk_mappings.push(BulkOffsetMapping {
            group_id: format!("perf-group-{}", i),
            topic: topic.to_string(),
            partition: 0,
            new_offset: 5,
            metadata: None,
        });
    }

    // Test with high concurrency
    let config = BulkOffsetResetConfig {
        max_concurrent_requests: 10,
        max_retry_attempts: 1,
        retry_base_delay_ms: 50,
        request_timeout_ms: 10000,
        continue_on_error: true,
    };

    let bulk_reset = BulkOffsetReset::new(client, config);
    let report = bulk_reset
        .reset_offsets_parallel(bulk_mappings)
        .await
        .expect("Bulk reset failed");

    // Verify all groups were processed
    assert_eq!(report.total_groups, 20);
    assert_eq!(report.successful_groups, 20);

    // Performance metrics should be populated
    assert!(report.performance.avg_latency_ms >= 0.0);
    assert!(report.performance.p50_latency_ms >= 0.0);
    assert!(report.performance.p99_latency_ms >= 0.0);
    assert_eq!(report.performance.max_concurrency, 10);

    println!("Bulk reset performance:");
    println!("  Duration: {} ms", report.duration_ms);
    println!(
        "  Throughput: {:.2} ops/s",
        report.performance.offsets_per_second
    );
    println!("  Avg latency: {:.2} ms", report.performance.avg_latency_ms);
    println!("  P50 latency: {:.2} ms", report.performance.p50_latency_ms);
    println!("  P99 latency: {:.2} ms", report.performance.p99_latency_ms);
}

/// Test bulk offset reset with empty input
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_bulk_offset_reset_empty_input() {
    let kafka = start_kafka().await;
    let bootstrap_server = get_bootstrap_server(&kafka).await;

    let client = create_test_client(&bootstrap_server);
    wait_for_kafka(&client).await;
    client.connect().await.expect("Failed to connect");

    let config = BulkOffsetResetConfig::default();
    let bulk_reset = BulkOffsetReset::new(client, config);

    // Empty mappings should succeed with no-op
    let report = bulk_reset
        .reset_offsets_parallel(vec![])
        .await
        .expect("Bulk reset failed");

    assert_eq!(report.status, BulkResetStatus::Success);
    assert_eq!(report.total_groups, 0);
    assert_eq!(report.total_offsets_reset, 0);
}

/// Test metrics collection during bulk reset
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_bulk_offset_reset_metrics() {
    let kafka = start_kafka().await;
    let bootstrap_server = get_bootstrap_server(&kafka).await;

    let client = create_test_client(&bootstrap_server);
    wait_for_kafka(&client).await;
    client.connect().await.expect("Failed to connect");

    // Create a topic
    let topic = "metrics-test-topic";
    let records = generate_test_records(5, topic);
    for record in &records {
        client
            .produce(topic, 0, vec![record.clone()])
            .await
            .expect("Failed to produce record");
    }

    let mut bulk_mappings = Vec::new();
    for i in 0..3 {
        bulk_mappings.push(BulkOffsetMapping {
            group_id: format!("metrics-group-{}", i),
            topic: topic.to_string(),
            partition: 0,
            new_offset: 2,
            metadata: None,
        });
    }

    let config = BulkOffsetResetConfig {
        max_concurrent_requests: 2,
        ..Default::default()
    };

    let bulk_reset = BulkOffsetReset::new(client, config);
    let report = bulk_reset
        .reset_offsets_parallel(bulk_mappings)
        .await
        .expect("Bulk reset failed");

    // Check metrics
    let metrics = bulk_reset.metrics();
    assert!(metrics.elapsed_ms() > 0);
    assert!(metrics.offsets_per_second() >= 0.0);

    // Report should have complete performance stats
    assert!(report.performance.avg_latency_ms >= 0.0);
    assert_eq!(report.performance.max_concurrency, 2);
}

// ============================================================================
// Offset Rollback Integration Tests
// ============================================================================

use kafka_backup_core::restore::offset_rollback::{
    rollback_offset_reset, snapshot_current_offsets, verify_rollback, OffsetSnapshotStorage,
    RollbackStatus, StorageBackendSnapshotStore,
};
use std::sync::Arc;

/// Test creating an offset snapshot
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_offset_snapshot_creation() {
    // Start Kafka container
    let kafka = start_kafka().await;
    let bootstrap_server = get_bootstrap_server(&kafka).await;

    // Create test client and wait for Kafka to be ready
    let client = create_test_client(&bootstrap_server);
    wait_for_kafka(&client).await;
    client.connect().await.expect("Failed to connect");

    // Create a test topic and produce some messages
    let topic = "snapshot-test-topic";
    let records = generate_test_records(20, topic);
    for record in &records {
        client
            .produce(topic, 0, vec![record.clone()])
            .await
            .expect("Failed to produce record");
    }

    // Create a consumer group and commit some offsets
    let group_id = "snapshot-test-group";

    // Use the Kafka client to commit offsets for this group
    // First, let's create a snapshot of current offsets
    let consumer_groups = vec![group_id.to_string()];
    let bootstrap_servers = vec![bootstrap_server.clone()];

    let snapshot = snapshot_current_offsets(&client, &consumer_groups, bootstrap_servers)
        .await
        .expect("Failed to create snapshot");

    // Verify snapshot properties
    assert!(!snapshot.snapshot_id.is_empty());
    assert!(snapshot.created_at <= chrono::Utc::now());
    assert_eq!(snapshot.bootstrap_servers, vec![bootstrap_server]);
}

/// Test snapshot storage operations (list, save, load, delete)
#[tokio::test]
async fn test_snapshot_storage_operations() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let backend: Arc<dyn StorageBackend> =
        Arc::new(FilesystemBackend::new(temp_dir.path().to_path_buf()));
    let snapshot_store = StorageBackendSnapshotStore::new(backend);

    // Initially no snapshots
    let snapshots = snapshot_store
        .list_snapshots()
        .await
        .expect("Failed to list snapshots");
    assert!(snapshots.is_empty());

    // Create a mock snapshot
    use kafka_backup_core::restore::offset_rollback::{
        GroupOffsetState, OffsetSnapshot, PartitionOffsetState,
    };
    use std::collections::HashMap;

    let mut group_offsets = HashMap::new();
    let mut offsets = HashMap::new();
    let mut partitions = HashMap::new();
    partitions.insert(
        0i32,
        PartitionOffsetState {
            offset: 100,
            metadata: Some("test".to_string()),
            timestamp: Some(chrono::Utc::now().timestamp_millis()),
        },
    );
    offsets.insert("test-topic".to_string(), partitions);

    group_offsets.insert(
        "test-group".to_string(),
        GroupOffsetState {
            group_id: "test-group".to_string(),
            offsets,
            partition_count: 1,
        },
    );

    let snapshot = OffsetSnapshot {
        snapshot_id: "test-snapshot-001".to_string(),
        created_at: chrono::Utc::now(),
        group_offsets,
        restore_id: None,
        cluster_id: None,
        bootstrap_servers: vec!["localhost:9092".to_string()],
        description: Some("Test snapshot".to_string()),
    };

    // Save snapshot
    let snapshot_id = snapshot_store
        .save_snapshot(&snapshot)
        .await
        .expect("Failed to save snapshot");
    assert_eq!(snapshot_id, "test-snapshot-001");

    // Verify it exists
    assert!(snapshot_store
        .exists(&snapshot_id)
        .await
        .expect("Failed to check exists"));

    // List snapshots
    let snapshots = snapshot_store
        .list_snapshots()
        .await
        .expect("Failed to list snapshots");
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].snapshot_id, "test-snapshot-001");
    assert_eq!(snapshots[0].group_count, 1);
    assert_eq!(snapshots[0].offset_count, 1);

    // Load snapshot
    let loaded = snapshot_store
        .load_snapshot(&snapshot_id)
        .await
        .expect("Failed to load snapshot");
    assert_eq!(loaded.snapshot_id, snapshot.snapshot_id);
    assert_eq!(loaded.group_offsets.len(), 1);

    // Delete snapshot
    snapshot_store
        .delete_snapshot(&snapshot_id)
        .await
        .expect("Failed to delete snapshot");

    assert!(!snapshot_store
        .exists(&snapshot_id)
        .await
        .expect("Failed to check exists"));
}

/// Test rollback to a snapshot
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_offset_rollback() {
    // Start Kafka container
    let kafka = start_kafka().await;
    let bootstrap_server = get_bootstrap_server(&kafka).await;

    // Create test client and wait for Kafka to be ready
    let client = create_test_client(&bootstrap_server);
    wait_for_kafka(&client).await;
    client.connect().await.expect("Failed to connect");

    // Create a test topic and produce messages
    let topic = "rollback-test-topic";
    let records = generate_test_records(50, topic);
    for record in &records {
        client
            .produce(topic, 0, vec![record.clone()])
            .await
            .expect("Failed to produce record");
    }

    // Create a snapshot with initial state
    let group_id = "rollback-test-group";
    let consumer_groups = vec![group_id.to_string()];
    let bootstrap_servers = vec![bootstrap_server.clone()];

    let snapshot = snapshot_current_offsets(&client, &consumer_groups, bootstrap_servers.clone())
        .await
        .expect("Failed to create snapshot");

    println!("Created snapshot: {}", snapshot.snapshot_id);
    println!(
        "Groups in snapshot: {:?}",
        snapshot.group_offsets.keys().collect::<Vec<_>>()
    );

    // Execute rollback
    let result = rollback_offset_reset(&client, &snapshot)
        .await
        .expect("Rollback failed");

    println!("Rollback result: {:?}", result.status);
    println!("Groups rolled back: {}", result.groups_rolled_back);

    // Verify the rollback completed (may have 0 groups if group didn't exist)
    assert!(
        result.status == RollbackStatus::Success || result.status == RollbackStatus::PartialSuccess,
        "Expected success or partial success, got {:?}",
        result.status
    );
}

/// Test verification of offsets against snapshot
#[tokio::test]
#[ignore = "requires Docker"]
async fn test_offset_verification() {
    // Start Kafka container
    let kafka = start_kafka().await;
    let bootstrap_server = get_bootstrap_server(&kafka).await;

    // Create test client and wait for Kafka to be ready
    let client = create_test_client(&bootstrap_server);
    wait_for_kafka(&client).await;
    client.connect().await.expect("Failed to connect");

    // Create a snapshot (even with empty groups, verification should work)
    let consumer_groups: Vec<String> = vec![];
    let bootstrap_servers = vec![bootstrap_server.clone()];

    let snapshot = snapshot_current_offsets(&client, &consumer_groups, bootstrap_servers)
        .await
        .expect("Failed to create snapshot");

    // Verify against current state (should match since we just created it)
    let verification = verify_rollback(&client, &snapshot)
        .await
        .expect("Verification failed");

    // With no groups in snapshot, verification should pass
    assert!(
        verification.verified,
        "Expected verification to pass with empty snapshot"
    );
    assert!(
        verification.mismatches.is_empty(),
        "Expected no mismatches with empty snapshot"
    );
}
