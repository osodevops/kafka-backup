//! Storage backend abstraction and implementations.
//!
//! This module provides a unified interface for storing backup data across
//! multiple storage backends including:
//!
//! - **S3**: AWS S3 and S3-compatible services (MinIO, Ceph RGW, etc.)
//! - **Azure**: Azure Blob Storage
//! - **GCS**: Google Cloud Storage
//! - **Filesystem**: Local filesystem storage
//! - **Memory**: In-memory storage (for testing)

mod azure;
mod backend;
mod config;
mod filesystem;
mod gcs;
mod memory;
mod s3;

pub use azure::{AzureBackend, AzureConfig};
pub use backend::{ObjectMetadata, StorageBackend};
pub use config::StorageBackendConfig;
pub use filesystem::FilesystemBackend;
pub use gcs::{GcsBackend, GcsConfig};
pub use memory::MemoryBackend;
pub use s3::{S3Backend, S3Config};

use crate::Result;
use std::sync::Arc;

/// Create a storage backend from the new configuration enum.
///
/// This is the recommended factory function for creating storage backends.
/// It supports all available backend types through a unified interface.
///
/// # Example
///
/// ```rust,ignore
/// use kafka_backup_core::storage::{create_backend_from_config, StorageBackendConfig};
///
/// let config = StorageBackendConfig::Memory;
/// let backend = create_backend_from_config(&config)?;
/// ```
pub fn create_backend_from_config(
    config: &StorageBackendConfig,
) -> Result<Arc<dyn StorageBackend>> {
    match config {
        StorageBackendConfig::S3 {
            bucket,
            region,
            endpoint,
            access_key,
            secret_key,
            prefix,
            path_style: _,
            allow_http,
        } => {
            let s3_config = S3Config {
                bucket: bucket.clone(),
                region: region.clone(),
                endpoint: endpoint.clone(),
                access_key_id: access_key.clone(),
                secret_access_key: secret_key.clone(),
                prefix: prefix.clone(),
                allow_http: *allow_http,
            };
            Ok(Arc::new(S3Backend::new(s3_config)?))
        }

        StorageBackendConfig::Azure {
            account_name,
            container_name,
            account_key,
            prefix,
            endpoint,
            use_workload_identity,
            client_id,
            tenant_id,
            client_secret,
            sas_token,
        } => {
            let azure_config = AzureConfig {
                account_name: account_name.clone(),
                container_name: container_name.clone(),
                account_key: account_key.clone(),
                prefix: prefix.clone(),
                endpoint: endpoint.clone(),
                use_workload_identity: *use_workload_identity,
                client_id: client_id.clone(),
                tenant_id: tenant_id.clone(),
                client_secret: client_secret.clone(),
                sas_token: sas_token.clone(),
            };
            Ok(Arc::new(AzureBackend::new(azure_config)?))
        }

        StorageBackendConfig::Gcs {
            bucket,
            service_account_path,
            prefix,
        } => {
            let gcs_config = GcsConfig {
                bucket: bucket.clone(),
                service_account_path: service_account_path.clone(),
                prefix: prefix.clone(),
            };
            Ok(Arc::new(GcsBackend::new(gcs_config)?))
        }

        StorageBackendConfig::Filesystem { path } => {
            Ok(Arc::new(FilesystemBackend::new(path.clone())))
        }

        StorageBackendConfig::Memory => Ok(Arc::new(MemoryBackend::new())),
    }
}

/// Create a storage backend from configuration.
///
/// This is an alias for `create_backend_from_config` for convenience.
/// Supports all backend types: S3, Azure, GCS, Filesystem, and Memory.
pub fn create_backend(config: &StorageBackendConfig) -> Result<Arc<dyn StorageBackend>> {
    create_backend_from_config(config)
}

/// Create a storage backend from legacy configuration.
///
/// **Deprecated**: Use `create_backend` or `create_backend_from_config` with
/// `StorageBackendConfig` instead.
#[deprecated(
    since = "0.2.0",
    note = "Use create_backend() with StorageBackendConfig instead"
)]
#[allow(deprecated)]
pub fn create_backend_legacy(
    config: &crate::config::StorageConfig,
) -> Result<Arc<dyn StorageBackend>> {
    match config.backend {
        crate::config::StorageBackendType::Filesystem => {
            let path = config
                .path
                .as_ref()
                .ok_or_else(|| crate::Error::Config("Filesystem path is required".to_string()))?;
            Ok(Arc::new(FilesystemBackend::new(path.clone())))
        }
        crate::config::StorageBackendType::S3 => {
            let bucket = config
                .bucket
                .as_ref()
                .ok_or_else(|| crate::Error::Config("S3 bucket is required".to_string()))?;

            let s3_config = S3Config {
                bucket: bucket.clone(),
                region: config.region.clone(),
                endpoint: config.endpoint.clone(),
                access_key_id: config.access_key.clone(),
                secret_access_key: config.secret_key.clone(),
                prefix: config.prefix.clone(),
                allow_http: config
                    .endpoint
                    .as_ref()
                    .is_some_and(|e| e.starts_with("http://")),
            };

            Ok(Arc::new(S3Backend::new(s3_config)?))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test]
    async fn test_create_memory_backend() {
        let config = StorageBackendConfig::Memory;
        let backend = create_backend_from_config(&config).unwrap();

        // Test basic operations
        let key = "test/data.txt";
        let data = Bytes::from("Hello, World!");

        backend.put(key, data.clone()).await.unwrap();
        let retrieved = backend.get(key).await.unwrap();
        assert_eq!(data, retrieved);

        assert!(backend.exists(key).await.unwrap());
        backend.delete(key).await.unwrap();
        assert!(!backend.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_filesystem_backend() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config = StorageBackendConfig::Filesystem {
            path: temp_dir.path().to_path_buf(),
        };
        let backend = create_backend_from_config(&config).unwrap();

        // Test basic operations
        let key = "test/data.txt";
        let data = Bytes::from("Hello, Filesystem!");

        backend.put(key, data.clone()).await.unwrap();
        let retrieved = backend.get(key).await.unwrap();
        assert_eq!(data, retrieved);
    }

    #[test]
    fn test_storage_config_from_url_s3() {
        let config = StorageBackendConfig::from_url("s3://my-bucket?region=us-west-2").unwrap();
        match config {
            StorageBackendConfig::S3 { bucket, region, .. } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(region, Some("us-west-2".to_string()));
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_storage_config_from_url_memory() {
        let config = StorageBackendConfig::from_url("memory://").unwrap();
        assert!(matches!(config, StorageBackendConfig::Memory));
    }

    #[test]
    fn test_storage_config_from_url_filesystem() {
        let config = StorageBackendConfig::from_url("file:///var/backups").unwrap();
        match config {
            StorageBackendConfig::Filesystem { path } => {
                assert_eq!(path, std::path::PathBuf::from("/var/backups"));
            }
            _ => panic!("Expected Filesystem config"),
        }
    }
}
