//! Storage configuration types.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Storage backend configuration using tagged enum for type-safe configuration.
///
/// Supports multiple storage backends:
/// - S3 and S3-compatible (MinIO, Ceph RGW, etc.)
/// - Azure Blob Storage
/// - Google Cloud Storage
/// - Local filesystem
/// - In-memory (for testing)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend")]
pub enum StorageBackendConfig {
    /// AWS S3 or S3-compatible storage (MinIO, Ceph RGW, DigitalOcean Spaces, etc.)
    #[serde(rename = "s3")]
    S3 {
        /// S3 bucket name
        bucket: String,
        /// AWS region (e.g., "us-east-1")
        #[serde(default)]
        region: Option<String>,
        /// Custom endpoint URL (for S3-compatible services like MinIO)
        #[serde(default)]
        endpoint: Option<String>,
        /// Access key ID (falls back to AWS_ACCESS_KEY_ID env var)
        #[serde(default)]
        access_key: Option<String>,
        /// Secret access key (falls back to AWS_SECRET_ACCESS_KEY env var)
        #[serde(default)]
        secret_key: Option<String>,
        /// Key prefix for all operations
        #[serde(default)]
        prefix: Option<String>,
        /// Use path-style requests (required for MinIO/Ceph RGW)
        #[serde(default)]
        path_style: bool,
        /// Allow HTTP (insecure) connections
        #[serde(default)]
        allow_http: bool,
    },

    /// Azure Blob Storage
    #[serde(rename = "azure")]
    Azure {
        /// Azure storage account name
        account_name: String,
        /// Azure blob container name
        container_name: String,
        /// Storage account key (if None, uses DefaultAzureCredential chain)
        #[serde(default)]
        account_key: Option<String>,
        /// Key prefix for all operations
        #[serde(default)]
        prefix: Option<String>,
    },

    /// Google Cloud Storage
    #[serde(rename = "gcs")]
    Gcs {
        /// GCS bucket name
        bucket: String,
        /// Path to service account JSON key file (if None, uses Application Default Credentials)
        #[serde(default)]
        service_account_path: Option<String>,
        /// Key prefix for all operations
        #[serde(default)]
        prefix: Option<String>,
    },

    /// Local filesystem storage
    #[serde(rename = "filesystem")]
    Filesystem {
        /// Base path for storage
        path: PathBuf,
    },

    /// In-memory storage (for testing)
    #[serde(rename = "memory")]
    Memory,
}

impl StorageBackendConfig {
    /// Parse configuration from a URL string
    ///
    /// Supported URL formats:
    /// - `s3://bucket-name?region=us-east-1`
    /// - `azure://container@account.blob.core.windows.net`
    /// - `gcs://bucket-name`
    /// - `file:///path/to/data`
    /// - `memory://`
    pub fn from_url(url: &str) -> crate::Result<Self> {
        let parsed = url::Url::parse(url)
            .map_err(|e| crate::Error::Config(format!("Invalid storage URL: {}", e)))?;

        match parsed.scheme() {
            "s3" | "s3a" => {
                let bucket = parsed.host_str().unwrap_or_default().to_string();
                let region = parsed
                    .query_pairs()
                    .find(|(k, _)| k == "region")
                    .map(|(_, v)| v.to_string());
                let endpoint = parsed
                    .query_pairs()
                    .find(|(k, _)| k == "endpoint")
                    .map(|(_, v)| v.to_string());
                let path_style = parsed
                    .query_pairs()
                    .find(|(k, _)| k == "path_style")
                    .map(|(_, v)| v == "true")
                    .unwrap_or(false);

                Ok(Self::S3 {
                    bucket,
                    region,
                    endpoint,
                    access_key: std::env::var("AWS_ACCESS_KEY_ID").ok(),
                    secret_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
                    prefix: None,
                    path_style,
                    allow_http: false,
                })
            }
            "azure" | "az" => {
                let host = parsed.host_str().unwrap_or_default();
                let account_name = host.split('.').next().unwrap_or(host).to_string();
                let container_name = parsed.path().trim_start_matches('/').to_string();

                Ok(Self::Azure {
                    account_name,
                    container_name,
                    account_key: std::env::var("AZURE_STORAGE_KEY").ok(),
                    prefix: None,
                })
            }
            "gcs" | "gs" => {
                let bucket = parsed.host_str().unwrap_or_default().to_string();

                Ok(Self::Gcs {
                    bucket,
                    service_account_path: std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
                    prefix: None,
                })
            }
            "file" => Ok(Self::Filesystem {
                path: PathBuf::from(parsed.path()),
            }),
            "memory" => Ok(Self::Memory),
            scheme => Err(crate::Error::Config(format!(
                "Unknown storage scheme: {}",
                scheme
            ))),
        }
    }

    /// Get the prefix for this storage configuration
    pub fn prefix(&self) -> Option<&str> {
        match self {
            Self::S3 { prefix, .. } => prefix.as_deref(),
            Self::Azure { prefix, .. } => prefix.as_deref(),
            Self::Gcs { prefix, .. } => prefix.as_deref(),
            Self::Filesystem { .. } => None,
            Self::Memory => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_url_parsing() {
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
    fn test_filesystem_url_parsing() {
        let config = StorageBackendConfig::from_url("file:///var/kafka-backups").unwrap();
        match config {
            StorageBackendConfig::Filesystem { path } => {
                assert_eq!(path, PathBuf::from("/var/kafka-backups"));
            }
            _ => panic!("Expected Filesystem config"),
        }
    }

    #[test]
    fn test_memory_url_parsing() {
        let config = StorageBackendConfig::from_url("memory://").unwrap();
        assert!(matches!(config, StorageBackendConfig::Memory));
    }

    #[test]
    fn test_yaml_deserialization_s3() {
        let yaml = r#"
backend: s3
bucket: kafka-backups
region: us-east-1
endpoint: http://localhost:9000
access_key: minioadmin
secret_key: minioadmin
path_style: true
allow_http: true
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageBackendConfig::S3 {
                bucket,
                region,
                endpoint,
                path_style,
                allow_http,
                ..
            } => {
                assert_eq!(bucket, "kafka-backups");
                assert_eq!(region, Some("us-east-1".to_string()));
                assert_eq!(endpoint, Some("http://localhost:9000".to_string()));
                assert!(path_style);
                assert!(allow_http);
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_yaml_deserialization_azure() {
        let yaml = r#"
backend: azure
account_name: mystorageaccount
container_name: kafka-backups
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageBackendConfig::Azure {
                account_name,
                container_name,
                ..
            } => {
                assert_eq!(account_name, "mystorageaccount");
                assert_eq!(container_name, "kafka-backups");
            }
            _ => panic!("Expected Azure config"),
        }
    }

    #[test]
    fn test_yaml_deserialization_gcs() {
        let yaml = r#"
backend: gcs
bucket: kafka-backups
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageBackendConfig::Gcs { bucket, .. } => {
                assert_eq!(bucket, "kafka-backups");
            }
            _ => panic!("Expected GCS config"),
        }
    }

    #[test]
    fn test_yaml_deserialization_filesystem() {
        let yaml = r#"
backend: filesystem
path: /var/kafka-backups
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageBackendConfig::Filesystem { path } => {
                assert_eq!(path, PathBuf::from("/var/kafka-backups"));
            }
            _ => panic!("Expected Filesystem config"),
        }
    }

    #[test]
    fn test_yaml_deserialization_memory() {
        let yaml = r#"
backend: memory
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(config, StorageBackendConfig::Memory));
    }
}
