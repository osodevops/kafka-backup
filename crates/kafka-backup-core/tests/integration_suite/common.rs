//! Common test infrastructure for integration tests.
//!
//! Provides Kafka container management, test client creation,
//! and helper functions used across integration tests.

#![allow(dead_code, unused_imports)]

use std::path::PathBuf;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};
use testcontainers_modules::kafka::Kafka;
use tokio::time::sleep;

use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, RestoreOptions, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::manifest::BackupRecord;
use kafka_backup_core::storage::StorageBackendConfig;

// ============================================================================
// Kafka Test Cluster
// ============================================================================

/// Wrapper around a Kafka testcontainer with helper methods.
pub struct KafkaTestCluster {
    pub container: ContainerAsync<Kafka>,
    pub bootstrap_servers: String,
}

impl KafkaTestCluster {
    /// Start a new Kafka container for testing.
    pub async fn start() -> anyhow::Result<Self> {
        let kafka = Kafka::default()
            .with_env_var("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .with_env_var("KAFKA_NUM_PARTITIONS", "3");

        let container = kafka.start().await?;

        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(9093).await?;
        let bootstrap_servers = format!("{}:{}", host, port);

        Ok(KafkaTestCluster {
            container,
            bootstrap_servers,
        })
    }

    /// Create a Kafka client connected to this cluster.
    pub fn create_client(&self) -> KafkaClient {
        let config = KafkaConfig {
            bootstrap_servers: vec![self.bootstrap_servers.clone()],
            security: SecurityConfig::default(),
            topics: TopicSelection::default(),
        };
        KafkaClient::new(config)
    }

    /// Wait for Kafka to be ready (up to 30 seconds).
    pub async fn wait_for_ready(&self, timeout: Duration) -> anyhow::Result<()> {
        let client = self.create_client();
        let start = Instant::now();

        loop {
            if client.connect().await.is_ok() && client.fetch_metadata(None).await.is_ok() {
                return Ok(());
            }

            if start.elapsed() > timeout {
                anyhow::bail!("Kafka did not become ready within {:?}", timeout);
            }

            sleep(Duration::from_secs(1)).await;
        }
    }

    /// Create a topic by producing a message to it (auto-create).
    pub async fn create_topic(&self, topic: &str, num_messages: usize) -> anyhow::Result<()> {
        let client = self.create_client();
        client.connect().await?;

        let records = generate_test_records(num_messages, topic);
        for (i, record) in records.iter().enumerate() {
            let partition = (i % 3) as i32; // Distribute across partitions
            client
                .produce(topic, partition, vec![record.clone()])
                .await?;
        }

        Ok(())
    }
}

// ============================================================================
// Configuration Builders
// ============================================================================

/// Create a backup configuration for testing.
pub fn create_backup_config(
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

/// Create a restore configuration for testing.
pub fn create_restore_config(
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
        storage: StorageBackendConfig::Filesystem { path: storage_path },
        backup: None,
        restore: Some(RestoreOptions::default()),
        offset_storage: None,
        metrics: None,
    }
}

/// Create a restore configuration with PITR time window.
pub fn create_pitr_restore_config(
    bootstrap_server: &str,
    storage_path: PathBuf,
    backup_id: &str,
    topics: Vec<String>,
    start_time: i64,
    end_time: i64,
) -> Config {
    let mut config = create_restore_config(bootstrap_server, storage_path, backup_id, topics);

    if let Some(ref mut restore_opts) = config.restore {
        restore_opts.time_window_start = Some(start_time);
        restore_opts.time_window_end = Some(end_time);
    }

    config
}

// ============================================================================
// Test Data Generation
// ============================================================================

/// Generate test records with sequential offsets.
pub fn generate_test_records(count: usize, topic: &str) -> Vec<BackupRecord> {
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

/// Generate test records with specific timestamps.
pub fn generate_records_with_timestamps(_topic: &str, timestamps: &[i64]) -> Vec<BackupRecord> {
    timestamps
        .iter()
        .enumerate()
        .map(|(i, &ts)| BackupRecord {
            key: Some(format!("key-{}", i).into_bytes()),
            value: Some(format!("value-{} at ts {}", i, ts).into_bytes()),
            headers: vec![],
            timestamp: ts,
            offset: i as i64,
        })
        .collect()
}

/// Get current timestamp in milliseconds.
pub fn unix_timestamp_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

// ============================================================================
// Test Storage
// ============================================================================

/// Create a temporary directory for test storage.
pub fn create_temp_storage() -> TempDir {
    TempDir::new().expect("Failed to create temp directory")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_test_records() {
        let records = generate_test_records(5, "test-topic");
        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.offset, i as i64);
        }
    }

    #[test]
    fn test_generate_records_with_timestamps() {
        let timestamps = vec![1000, 2000, 3000];
        let records = generate_records_with_timestamps("test", &timestamps);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].timestamp, 1000);
        assert_eq!(records[1].timestamp, 2000);
        assert_eq!(records[2].timestamp, 3000);
    }

    #[test]
    fn test_backup_config_creation() {
        let config = create_backup_config(
            "localhost:9092",
            PathBuf::from("/tmp/test"),
            "backup-001",
            vec!["orders".to_string()],
        );

        assert_eq!(config.mode, Mode::Backup);
        assert_eq!(config.backup_id, "backup-001");
        assert!(config.source.is_some());
        assert!(config.target.is_none());
    }

    #[test]
    fn test_restore_config_creation() {
        let config = create_restore_config(
            "localhost:9092",
            PathBuf::from("/tmp/test"),
            "backup-001",
            vec!["orders".to_string()],
        );

        assert_eq!(config.mode, Mode::Restore);
        assert!(config.source.is_none());
        assert!(config.target.is_some());
    }
}
