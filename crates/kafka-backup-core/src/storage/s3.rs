//! S3-compatible storage backend using object_store.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::sync::Arc;
use tracing::debug;

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
    /// Use path-style requests (bucket in the path, not the host). Implied by
    /// a custom `endpoint`; set explicitly for path-style-only AWS setups.
    pub path_style: bool,
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
            path_style: false,
            allow_http: false,
        }
    }
}

/// Path-style requests are used when asked for explicitly, or implied by a
/// custom endpoint (MinIO / Ceph RGW). Before 0.22.0 the flag was silently
/// discarded and only the endpoint side effect applied (issue #166).
pub(crate) fn use_path_style(endpoint: Option<&str>, path_style: bool) -> bool {
    path_style || endpoint.is_some()
}

/// S3 storage backend
pub struct S3Backend {
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
}

impl S3Backend {
    /// Create a new S3 backend
    pub fn new(config: S3Config) -> Result<Self> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);

        if let Some(region) = &config.region {
            builder = builder.with_region(region);
        }

        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint);
        }

        if use_path_style(config.endpoint.as_deref(), config.path_style) {
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

        // The endpoint object_store will actually use (AWS_ENDPOINT_URL_S3 beats
        // AWS_ENDPOINT_URL / `endpoint`), so a request silently going to AWS
        // instead of MinIO/Ceph shows up in the debug log.
        let endpoint = builder
            .get_config_value(&AmazonS3ConfigKey::S3Endpoint)
            .or_else(|| builder.get_config_value(&AmazonS3ConfigKey::Endpoint))
            .unwrap_or_else(|| "AWS default".to_string());

        let store = builder.build().map_err(|e| {
            Error::Storage(StorageError::Backend(format!(
                "Failed to create S3 client: {}",
                e
            )))
        })?;

        debug!(
            "Created S3 backend for bucket: {}, prefix: {:?}, endpoint: {}",
            config.bucket, config.prefix, endpoint
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

/// Map an object_store error for `key`. A missing key is
/// `StorageError::NotFound`, as on the other backends, so callers can tell
/// "missing" from "failed" (403, network, 5xx) (#218).
fn s3_error(op: &str, key: &str, e: object_store::Error) -> Error {
    match e {
        object_store::Error::NotFound { .. } => {
            Error::Storage(StorageError::NotFound(key.to_string()))
        }
        e => Error::Storage(StorageError::Backend(format!("S3 {op} failed: {e}"))),
    }
}

#[async_trait]
impl StorageBackend for S3Backend {
    fn backend_name(&self) -> &str {
        "s3"
    }

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

        let result = self
            .store
            .get(&path)
            .await
            .map_err(|e| s3_error("GET", key, e))?;

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

        let meta = self
            .store
            .head(&path)
            .await
            .map_err(|e| s3_error("HEAD", key, e))?;

        Ok(meta.size as u64)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let path = self.full_path(key);
        debug!("S3 HEAD: {}", path);

        let meta = self
            .store
            .head(&path)
            .await
            .map_err(|e| s3_error("HEAD", key, e))?;

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
            .map_err(|e| s3_error("COPY", src, e))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #218: a missing key is NotFound; anything else (403, network) is not.
    #[test]
    fn s3_error_maps_only_not_found_to_not_found() {
        let missing = object_store::Error::NotFound {
            path: "b/manifest.json".to_string(),
            source: "404 NoSuchKey".into(),
        };
        let err = s3_error("GET", "b/manifest.json", missing);
        assert!(err.is_not_found());
        assert_eq!(
            err.to_string(),
            "Storage error: Object not found: b/manifest.json"
        );

        for other in [
            object_store::Error::PermissionDenied {
                path: "b/manifest.json".to_string(),
                source: "403 AccessDenied".into(),
            },
            object_store::Error::Unauthenticated {
                path: "b/manifest.json".to_string(),
                source: "401".into(),
            },
            object_store::Error::Generic {
                store: "S3",
                source: "connection refused".into(),
            },
        ] {
            let err = s3_error("GET", "b/manifest.json", other);
            assert!(!err.is_not_found(), "{err}");
            assert!(err.to_string().contains("S3 GET failed"), "{err}");
        }
    }

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

    #[test]
    fn use_path_style_cases() {
        assert!(
            use_path_style(Some("http://minio:9000"), false),
            "endpoint implies path style"
        );
        assert!(
            use_path_style(None, true),
            "explicit flag honoured without endpoint"
        );
        assert!(
            !use_path_style(None, false),
            "plain AWS keeps the client default"
        );
    }
}
