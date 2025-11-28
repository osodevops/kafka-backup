//! Google Cloud Storage backend implementation.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use std::sync::Arc;
use tracing::{debug, info};

use super::{ObjectMetadata, StorageBackend};
use crate::error::StorageError;
use crate::{Error, Result};

/// Google Cloud Storage backend configuration
#[derive(Debug, Clone)]
pub struct GcsConfig {
    /// GCS bucket name
    pub bucket: String,
    /// Path to service account JSON key file (if None, uses Application Default Credentials)
    pub service_account_path: Option<String>,
    /// Key prefix for all operations
    pub prefix: Option<String>,
}

/// Google Cloud Storage backend
pub struct GcsBackend {
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
}

impl GcsBackend {
    /// Create a new Google Cloud Storage backend
    ///
    /// If `service_account_path` is not provided, the SDK will attempt to use
    /// Application Default Credentials (ADC) which tries:
    /// 1. GOOGLE_APPLICATION_CREDENTIALS environment variable
    /// 2. Google Cloud SDK default credentials
    /// 3. Compute Engine/GKE metadata service
    pub fn new(config: GcsConfig) -> Result<Self> {
        let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&config.bucket);

        if let Some(path) = &config.service_account_path {
            builder = builder.with_service_account_path(path);
        }

        let store = builder
            .build()
            .map_err(|e| Error::Storage(StorageError::Backend(format!("Failed to create GCS client: {}", e))))?;

        info!(
            "Created GCS backend for bucket: {}, prefix: {:?}",
            config.bucket, config.prefix
        );

        Ok(Self {
            store: Arc::new(store),
            prefix: config.prefix,
        })
    }

    /// Build the full path for a key
    fn full_path(&self, key: &str) -> Path {
        match &self.prefix {
            Some(prefix) => Path::from(format!("{}/{}", prefix.trim_end_matches('/'), key)),
            None => Path::from(key),
        }
    }

    /// Strip the prefix from a path to get the key
    fn strip_prefix(&self, path: &str) -> String {
        match &self.prefix {
            Some(p) => path
                .strip_prefix(&format!("{}/", p.trim_end_matches('/')))
                .unwrap_or(path)
                .to_string(),
            None => path.to_string(),
        }
    }
}

#[async_trait]
impl StorageBackend for GcsBackend {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let path = self.full_path(key);
        debug!("GCS PUT: {}", path);

        self.store
            .put(&path, PutPayload::from_bytes(data))
            .await
            .map_err(|e| Error::Storage(StorageError::Backend(format!("GCS PUT failed: {}", e))))?;

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.full_path(key);
        debug!("GCS GET: {}", path);

        let result = self
            .store
            .get(&path)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    Error::Storage(StorageError::NotFound(key.to_string()))
                }
                _ => Error::Storage(StorageError::Backend(format!("GCS GET failed: {}", e))),
            })?;

        result
            .bytes()
            .await
            .map_err(|e| Error::Storage(StorageError::Backend(format!("Failed to read GCS response: {}", e))))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_path(prefix);
        debug!("GCS LIST: {}", full_prefix);

        let mut keys = Vec::new();
        let mut stream = self.store.list(Some(&full_prefix));

        use futures::StreamExt;
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    let key = meta.location.to_string();
                    keys.push(self.strip_prefix(&key));
                }
                Err(e) => {
                    return Err(Error::Storage(StorageError::Backend(format!(
                        "GCS LIST failed: {}",
                        e
                    ))));
                }
            }
        }

        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let path = self.full_path(key);
        debug!("GCS HEAD: {}", path);

        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::Storage(StorageError::Backend(format!(
                "GCS HEAD failed: {}",
                e
            )))),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.full_path(key);
        debug!("GCS DELETE: {}", path);

        self.store
            .delete(&path)
            .await
            .map_err(|e| Error::Storage(StorageError::Backend(format!("GCS DELETE failed: {}", e))))?;

        Ok(())
    }

    async fn size(&self, key: &str) -> Result<u64> {
        let path = self.full_path(key);
        debug!("GCS HEAD (size): {}", path);

        let meta = self
            .store
            .head(&path)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    Error::Storage(StorageError::NotFound(key.to_string()))
                }
                _ => Error::Storage(StorageError::Backend(format!("GCS HEAD failed: {}", e))),
            })?;

        Ok(meta.size as u64)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let path = self.full_path(key);
        debug!("GCS HEAD: {}", path);

        let meta = self
            .store
            .head(&path)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    Error::Storage(StorageError::NotFound(key.to_string()))
                }
                _ => Error::Storage(StorageError::Backend(format!("GCS HEAD failed: {}", e))),
            })?;

        Ok(ObjectMetadata {
            size: meta.size as u64,
            last_modified: meta.last_modified.timestamp_millis(),
            e_tag: meta.e_tag.clone(),
        })
    }

    async fn copy(&self, src: &str, dest: &str) -> Result<()> {
        let src_path = self.full_path(src);
        let dest_path = self.full_path(dest);
        debug!("GCS COPY: {} -> {}", src_path, dest_path);

        self.store
            .copy(&src_path, &dest_path)
            .await
            .map_err(|e| Error::Storage(StorageError::Backend(format!("GCS COPY failed: {}", e))))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require actual GCS credentials to run
    // They are ignored by default

    #[tokio::test]
    #[ignore]
    async fn test_gcs_backend_basic() {
        let config = GcsConfig {
            bucket: std::env::var("GCS_BUCKET").unwrap_or_else(|_| "test-bucket".to_string()),
            service_account_path: std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
            prefix: Some("test".to_string()),
        };

        let backend = GcsBackend::new(config).unwrap();

        // Test put
        let data = Bytes::from("Hello, GCS!");
        backend.put("test-key", data.clone()).await.unwrap();

        // Test exists
        assert!(backend.exists("test-key").await.unwrap());

        // Test get
        let retrieved = backend.get("test-key").await.unwrap();
        assert_eq!(retrieved, data);

        // Test size
        let size = backend.size("test-key").await.unwrap();
        assert_eq!(size, data.len() as u64);

        // Test head
        let meta = backend.head("test-key").await.unwrap();
        assert_eq!(meta.size, data.len() as u64);

        // Test copy
        backend.copy("test-key", "test-key-copy").await.unwrap();
        assert!(backend.exists("test-key-copy").await.unwrap());

        // Test delete
        backend.delete("test-key").await.unwrap();
        backend.delete("test-key-copy").await.unwrap();
        assert!(!backend.exists("test-key").await.unwrap());
    }
}
