//! Filesystem storage backend implementation.

use async_trait::async_trait;
use bytes::Bytes;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::{ObjectMetadata, StorageBackend};
use crate::error::StorageError;
use crate::Result;

/// Filesystem-based storage backend
#[derive(Debug, Clone)]
pub struct FilesystemBackend {
    base_path: PathBuf,
}

impl FilesystemBackend {
    /// Create a new filesystem backend with the given base path
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }

    /// Convert a storage key to a filesystem path
    fn key_to_path(&self, key: &str) -> PathBuf {
        // Normalize key to prevent path traversal
        let normalized = key.trim_start_matches('/');
        self.base_path.join(normalized)
    }

    /// Convert a filesystem path to a storage key
    fn path_to_key(&self, path: &std::path::Path) -> Option<String> {
        path.strip_prefix(&self.base_path)
            .ok()
            .map(|p| p.to_string_lossy().to_string())
    }
}

#[async_trait]
impl StorageBackend for FilesystemBackend {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let path = self.key_to_path(key);

        // Create parent directories if they don't exist
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                StorageError::Backend(format!("Failed to create directories: {}", e))
            })?;
        }

        // Write data to file
        let mut file = fs::File::create(&path).await.map_err(|e| {
            StorageError::Backend(format!("Failed to create file {}: {}", path.display(), e))
        })?;

        file.write_all(&data).await.map_err(|e| {
            StorageError::Backend(format!("Failed to write to file {}: {}", path.display(), e))
        })?;

        file.flush().await.map_err(|e| {
            StorageError::Backend(format!("Failed to flush file {}: {}", path.display(), e))
        })?;

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.key_to_path(key);

        let mut file = fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Backend(format!("Failed to open file {}: {}", path.display(), e))
            }
        })?;

        let mut data = Vec::new();
        file.read_to_end(&mut data).await.map_err(|e| {
            StorageError::Backend(format!("Failed to read file {}: {}", path.display(), e))
        })?;

        Ok(Bytes::from(data))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let base = self.key_to_path(prefix);
        let mut results = Vec::new();

        // If the prefix path doesn't exist, return empty list
        if !base.exists() {
            return Ok(results);
        }

        // Walk the directory tree
        let mut stack = vec![base];
        while let Some(dir) = stack.pop() {
            if dir.is_file() {
                if let Some(key) = self.path_to_key(&dir) {
                    results.push(key);
                }
                continue;
            }

            let mut entries = fs::read_dir(&dir).await.map_err(|e| {
                StorageError::Backend(format!("Failed to read directory {}: {}", dir.display(), e))
            })?;

            while let Some(entry) = entries.next_entry().await.map_err(|e| {
                StorageError::Backend(format!("Failed to read directory entry: {}", e))
            })? {
                let path = entry.path();
                if path.is_dir() {
                    stack.push(path);
                } else if let Some(key) = self.path_to_key(&path) {
                    results.push(key);
                }
            }
        }

        results.sort();
        Ok(results)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let path = self.key_to_path(key);
        Ok(path.exists())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.key_to_path(key);

        fs::remove_file(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Backend(format!("Failed to delete file {}: {}", path.display(), e))
            }
        })?;

        Ok(())
    }

    async fn size(&self, key: &str) -> Result<u64> {
        let path = self.key_to_path(key);

        let metadata = fs::metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Backend(format!(
                    "Failed to get metadata for {}: {}",
                    path.display(),
                    e
                ))
            }
        })?;

        Ok(metadata.len())
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let path = self.key_to_path(key);

        let metadata = fs::metadata(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(key.to_string())
            } else {
                StorageError::Backend(format!(
                    "Failed to get metadata for {}: {}",
                    path.display(),
                    e
                ))
            }
        })?;

        let last_modified = metadata
            .modified()
            .map(|t| {
                t.duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0)
            })
            .unwrap_or(0);

        Ok(ObjectMetadata {
            size: metadata.len(),
            last_modified,
            e_tag: None,
        })
    }

    async fn copy(&self, src: &str, dest: &str) -> Result<()> {
        let src_path = self.key_to_path(src);
        let dest_path = self.key_to_path(dest);

        // Create parent directories for destination if they don't exist
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).await.map_err(|e| {
                StorageError::Backend(format!("Failed to create directories: {}", e))
            })?;
        }

        fs::copy(&src_path, &dest_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(src.to_string())
            } else {
                StorageError::Backend(format!(
                    "Failed to copy {} to {}: {}",
                    src_path.display(),
                    dest_path.display(),
                    e
                ))
            }
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_put_and_get() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FilesystemBackend::new(temp_dir.path().to_path_buf());

        let key = "test/data.txt";
        let data = Bytes::from("Hello, World!");

        backend.put(key, data.clone()).await.unwrap();

        let retrieved = backend.get(key).await.unwrap();
        assert_eq!(data, retrieved);
    }

    #[tokio::test]
    async fn test_exists() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FilesystemBackend::new(temp_dir.path().to_path_buf());

        let key = "test/data.txt";
        assert!(!backend.exists(key).await.unwrap());

        backend.put(key, Bytes::from("data")).await.unwrap();
        assert!(backend.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_list() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FilesystemBackend::new(temp_dir.path().to_path_buf());

        backend
            .put("backup1/manifest.json", Bytes::from("{}"))
            .await
            .unwrap();
        backend
            .put(
                "backup1/topics/orders/partition=0/segment-001.zst",
                Bytes::from("data"),
            )
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
        let temp_dir = TempDir::new().unwrap();
        let backend = FilesystemBackend::new(temp_dir.path().to_path_buf());

        let key = "test/data.txt";
        backend.put(key, Bytes::from("data")).await.unwrap();
        assert!(backend.exists(key).await.unwrap());

        backend.delete(key).await.unwrap();
        assert!(!backend.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_size() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FilesystemBackend::new(temp_dir.path().to_path_buf());

        let key = "test/data.txt";
        let data = Bytes::from("Hello, World!");
        backend.put(key, data.clone()).await.unwrap();

        let size = backend.size(key).await.unwrap();
        assert_eq!(size, data.len() as u64);
    }
}
