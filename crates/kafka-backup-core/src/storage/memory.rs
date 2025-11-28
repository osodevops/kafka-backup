//! In-memory storage backend for testing.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use std::sync::Arc;

use super::{ObjectMetadata, StorageBackend};
use crate::error::StorageError;
use crate::{Error, Result};

/// In-memory storage backend using object_store
///
/// This backend is primarily useful for testing purposes as it doesn't
/// persist data between runs.
pub struct MemoryBackend {
    store: Arc<InMemory>,
}

impl MemoryBackend {
    /// Create a new in-memory storage backend
    pub fn new() -> Self {
        Self {
            store: Arc::new(InMemory::new()),
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl StorageBackend for MemoryBackend {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let path = Path::from(key);
        self.store
            .put(&path, PutPayload::from_bytes(data))
            .await
            .map_err(|e| Error::Storage(StorageError::Backend(format!("Memory PUT failed: {}", e))))?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = Path::from(key);
        let result = self
            .store
            .get(&path)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    Error::Storage(StorageError::NotFound(key.to_string()))
                }
                _ => Error::Storage(StorageError::Backend(format!("Memory GET failed: {}", e))),
            })?;

        result
            .bytes()
            .await
            .map_err(|e| Error::Storage(StorageError::Backend(format!("Failed to read bytes: {}", e))))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix_path = Path::from(prefix);
        let mut keys = Vec::new();
        let mut stream = self.store.list(Some(&prefix_path));

        use futures::StreamExt;
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    keys.push(meta.location.to_string());
                }
                Err(e) => {
                    return Err(Error::Storage(StorageError::Backend(format!(
                        "Memory LIST failed: {}",
                        e
                    ))));
                }
            }
        }

        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let path = Path::from(key);
        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::Storage(StorageError::Backend(format!(
                "Memory HEAD failed: {}",
                e
            )))),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = Path::from(key);
        self.store
            .delete(&path)
            .await
            .map_err(|e| Error::Storage(StorageError::Backend(format!("Memory DELETE failed: {}", e))))?;
        Ok(())
    }

    async fn size(&self, key: &str) -> Result<u64> {
        let path = Path::from(key);
        let meta = self
            .store
            .head(&path)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    Error::Storage(StorageError::NotFound(key.to_string()))
                }
                _ => Error::Storage(StorageError::Backend(format!("Memory HEAD failed: {}", e))),
            })?;

        Ok(meta.size as u64)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let path = Path::from(key);
        let meta = self
            .store
            .head(&path)
            .await
            .map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    Error::Storage(StorageError::NotFound(key.to_string()))
                }
                _ => Error::Storage(StorageError::Backend(format!("Memory HEAD failed: {}", e))),
            })?;

        Ok(ObjectMetadata {
            size: meta.size as u64,
            last_modified: meta.last_modified.timestamp_millis(),
            e_tag: meta.e_tag.clone(),
        })
    }

    async fn copy(&self, src: &str, dest: &str) -> Result<()> {
        let src_path = Path::from(src);
        let dest_path = Path::from(dest);

        self.store
            .copy(&src_path, &dest_path)
            .await
            .map_err(|e| Error::Storage(StorageError::Backend(format!("Memory COPY failed: {}", e))))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get() {
        let backend = MemoryBackend::new();

        let key = "test/data.txt";
        let data = Bytes::from("Hello, World!");

        backend.put(key, data.clone()).await.unwrap();

        let retrieved = backend.get(key).await.unwrap();
        assert_eq!(data, retrieved);
    }

    #[tokio::test]
    async fn test_exists() {
        let backend = MemoryBackend::new();

        let key = "test/data.txt";
        assert!(!backend.exists(key).await.unwrap());

        backend.put(key, Bytes::from("data")).await.unwrap();
        assert!(backend.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_list() {
        let backend = MemoryBackend::new();

        backend
            .put("backup1/manifest.json", Bytes::from("{}"))
            .await
            .unwrap();
        backend
            .put("backup1/topics/orders/segment.zst", Bytes::from("data"))
            .await
            .unwrap();
        backend
            .put("backup2/manifest.json", Bytes::from("{}"))
            .await
            .unwrap();

        let all = backend.list("").await.unwrap();
        assert_eq!(all.len(), 3);

        let backup1 = backend.list("backup1").await.unwrap();
        assert_eq!(backup1.len(), 2);
    }

    #[tokio::test]
    async fn test_delete() {
        let backend = MemoryBackend::new();

        let key = "test/data.txt";
        backend.put(key, Bytes::from("data")).await.unwrap();
        assert!(backend.exists(key).await.unwrap());

        backend.delete(key).await.unwrap();
        assert!(!backend.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_size() {
        let backend = MemoryBackend::new();

        let key = "test/data.txt";
        let data = Bytes::from("Hello, World!");
        backend.put(key, data.clone()).await.unwrap();

        let size = backend.size(key).await.unwrap();
        assert_eq!(size, data.len() as u64);
    }

    #[tokio::test]
    async fn test_head() {
        let backend = MemoryBackend::new();

        let key = "test/data.txt";
        let data = Bytes::from("Hello, World!");
        backend.put(key, data.clone()).await.unwrap();

        let meta = backend.head(key).await.unwrap();
        assert_eq!(meta.size, data.len() as u64);
        assert!(meta.last_modified > 0);
    }

    #[tokio::test]
    async fn test_copy() {
        let backend = MemoryBackend::new();

        let src = "test/source.txt";
        let dest = "test/dest.txt";
        let data = Bytes::from("Hello, World!");

        backend.put(src, data.clone()).await.unwrap();
        backend.copy(src, dest).await.unwrap();

        let retrieved = backend.get(dest).await.unwrap();
        assert_eq!(data, retrieved);
    }
}
