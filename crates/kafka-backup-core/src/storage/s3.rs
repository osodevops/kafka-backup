//! S3-compatible storage backend using object_store.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use std::sync::Arc;
use tracing::{debug, info};

use super::{ObjectMetadata, StorageBackend};
use crate::error::StorageError;
use crate::{Error, Result};

/// S3 storage backend configuration
#[derive(Debug, Clone)]
pub struct S3Config {
    /// S3 bucket name
    pub bucket: String,
    /// AWS region
    pub region: Option<String>,
    /// Custom endpoint (for S3-compatible services like MinIO)
    pub endpoint: Option<String>,
    /// Access key ID
    pub access_key_id: Option<String>,
    /// Secret access key
    pub secret_access_key: Option<String>,
    /// Key prefix for all operations
    pub prefix: Option<String>,
    /// Allow HTTP (insecure) connections
    pub allow_http: bool,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            region: Some("us-east-1".to_string()),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            prefix: None,
            allow_http: false,
        }
    }
}

/// S3 storage backend
pub struct S3Backend {
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
}

impl S3Backend {
    /// Create a new S3 backend
    pub fn new(config: S3Config) -> Result<Self> {
        let mut builder = AmazonS3Builder::new().with_bucket_name(&config.bucket);

        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }

        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
            // For custom endpoints, we typically need virtual hosted style disabled
            builder = builder.with_virtual_hosted_style_request(false);
        }

        if let Some(access_key) = &config.access_key_id {
            builder = builder.with_access_key_id(access_key);
        }

        if let Some(secret_key) = &config.secret_access_key {
            builder = builder.with_secret_access_key(secret_key);
        }

        if config.allow_http {
            builder = builder.with_allow_http(true);
        }

        let store = builder.build().map_err(|e| {
            Error::Storage(StorageError::Backend(format!(
                "Failed to create S3 client: {}",
                e
            )))
        })?;

        info!(
            "Created S3 backend for bucket: {}, prefix: {:?}",
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
}

#[async_trait]
impl StorageBackend for S3Backend {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let path = self.full_path(key);
        debug!("S3 PUT: {}", path);

        self.store
            .put(&path, PutPayload::from_bytes(data))
            .await
            .map_err(|e| Error::Storage(StorageError::Backend(format!("S3 PUT failed: {}", e))))?;

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.full_path(key);
        debug!("S3 GET: {}", path);

        let result =
            self.store.get(&path).await.map_err(|e| {
                Error::Storage(StorageError::Backend(format!("S3 GET failed: {}", e)))
            })?;

        let bytes = result.bytes().await.map_err(|e| {
            Error::Storage(StorageError::Backend(format!(
                "Failed to read S3 response: {}",
                e
            )))
        })?;

        Ok(bytes)
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_path(prefix);
        debug!("S3 LIST: {}", full_prefix);

        let mut keys = Vec::new();
        let mut stream = self.store.list(Some(&full_prefix));

        use futures::StreamExt;
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    // Remove the configured prefix from the path
                    let key = meta.location.to_string();
                    let stripped = match &self.prefix {
                        Some(p) => key
                            .strip_prefix(&format!("{}/", p.trim_end_matches('/')))
                            .unwrap_or(&key)
                            .to_string(),
                        None => key,
                    };
                    keys.push(stripped);
                }
                Err(e) => {
                    return Err(Error::Storage(StorageError::Backend(format!(
                        "S3 LIST failed: {}",
                        e
                    ))));
                }
            }
        }

        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let path = self.full_path(key);
        debug!("S3 HEAD: {}", path);

        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::Storage(StorageError::Backend(format!(
                "S3 HEAD failed: {}",
                e
            )))),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.full_path(key);
        debug!("S3 DELETE: {}", path);

        self.store.delete(&path).await.map_err(|e| {
            Error::Storage(StorageError::Backend(format!("S3 DELETE failed: {}", e)))
        })?;

        Ok(())
    }

    async fn size(&self, key: &str) -> Result<u64> {
        let path = self.full_path(key);
        debug!("S3 HEAD (size): {}", path);

        let meta =
            self.store.head(&path).await.map_err(|e| {
                Error::Storage(StorageError::Backend(format!("S3 HEAD failed: {}", e)))
            })?;

        Ok(meta.size as u64)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let path = self.full_path(key);
        debug!("S3 HEAD: {}", path);

        let meta =
            self.store.head(&path).await.map_err(|e| {
                Error::Storage(StorageError::Backend(format!("S3 HEAD failed: {}", e)))
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
        debug!("S3 COPY: {} -> {}", src_path, dest_path);

        self.store
            .copy(&src_path, &dest_path)
            .await
            .map_err(|e| Error::Storage(StorageError::Backend(format!("S3 COPY failed: {}", e))))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require actual S3 or MinIO to run
    // They are ignored by default

    #[tokio::test]
    #[ignore]
    async fn test_s3_backend_basic() {
        let config = S3Config {
            bucket: "test-bucket".to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            access_key_id: Some("minioadmin".to_string()),
            secret_access_key: Some("minioadmin".to_string()),
            allow_http: true,
            ..Default::default()
        };

        let backend = S3Backend::new(config).unwrap();

        // Test put
        let data = Bytes::from("Hello, S3!");
        backend.put("test-key", data.clone()).await.unwrap();

        // Test exists
        assert!(backend.exists("test-key").await.unwrap());

        // Test get
        let retrieved = backend.get("test-key").await.unwrap();
        assert_eq!(retrieved, data);

        // Test size
        let size = backend.size("test-key").await.unwrap();
        assert_eq!(size, data.len() as u64);

        // Test delete
        backend.delete("test-key").await.unwrap();
        assert!(!backend.exists("test-key").await.unwrap());
    }
}
