//! Filesystem storage backend implementation.

use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::debug;

use super::{ObjectMetadata, StorageBackend};
use crate::error::StorageError;
use crate::Result;

/// Marker in the names of the temp files `put` writes before renaming them
/// into place (`.{file name}.kb-tmp-{pid}-{n}`). Files carrying it are
/// left over from an interrupted write and are never listed as keys.
const TEMP_MARKER: &str = ".kb-tmp-";

/// Distinguishes concurrent temp files within this process.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn is_temp_file(path: &Path) -> bool {
    path.file_name()
        .map(|n| n.to_string_lossy())
        .is_some_and(|n| n.starts_with('.') && n.contains(TEMP_MARKER))
}

/// Write `data` to a new file at `path` and flush it to disk.
async fn write_synced(path: &Path, data: &[u8]) -> std::io::Result<()> {
    let mut file = fs::File::create(path).await?;
    file.write_all(data).await?;
    file.sync_all().await
}

/// Persist a rename in `dir` (best effort). On Unix the directory entry is
/// only durable once the directory itself is fsynced. Other platforms
/// cannot open a directory this way; NTFS journals the rename itself.
async fn sync_dir(dir: &Path) {
    #[cfg(unix)]
    {
        let result = match fs::File::open(dir).await {
            Ok(handle) => handle.sync_all().await,
            Err(e) => Err(e),
        };
        if let Err(e) = result {
            debug!("fsync of directory {} failed: {}", dir.display(), e);
        }
    }
    #[cfg(not(unix))]
    let _ = dir;
}

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
    fn backend_name(&self) -> &str {
        "filesystem"
    }

    /// Write `data` atomically: to a temp file in the same directory,
    /// fsynced, then renamed over `key`, then the directory is fsynced.
    /// A reader sees the old content or the new content, never a partial
    /// file, and a crash leaves at most an unlisted temp file behind.
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let path = self.key_to_path(key);
        let (Some(parent), Some(file_name)) = (path.parent(), path.file_name()) else {
            return Err(StorageError::Backend(format!("Invalid key for filesystem: {key}")).into());
        };

        fs::create_dir_all(parent)
            .await
            .map_err(|e| StorageError::Backend(format!("Failed to create directories: {}", e)))?;

        let tmp = parent.join(format!(
            ".{}{}{}-{}",
            file_name.to_string_lossy(),
            TEMP_MARKER,
            std::process::id(),
            TEMP_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));

        if let Err(e) = write_synced(&tmp, &data).await {
            let _ = fs::remove_file(&tmp).await;
            return Err(StorageError::Backend(format!(
                "Failed to write file {}: {}",
                path.display(),
                e
            ))
            .into());
        }

        if let Err(e) = fs::rename(&tmp, &path).await {
            let _ = fs::remove_file(&tmp).await;
            return Err(StorageError::Backend(format!(
                "Failed to move {} into place: {}",
                path.display(),
                e
            ))
            .into());
        }

        sync_dir(parent).await;
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
                } else if is_temp_file(&path) {
                    continue;
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

    /// Every entry under `dir`, recursively (file names only).
    fn all_file_names(dir: &std::path::Path) -> Vec<String> {
        let mut names = Vec::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                names.extend(all_file_names(&path));
            } else {
                names.push(path.file_name().unwrap().to_string_lossy().to_string());
            }
        }
        names.sort();
        names
    }

    #[tokio::test]
    async fn put_replaces_the_file_and_leaves_no_temp_files() {
        let temp_dir = TempDir::new().unwrap();
        let backend = FilesystemBackend::new(temp_dir.path().to_path_buf());
        backend
            .put("b/manifest.json", Bytes::from("first"))
            .await
            .unwrap();
        backend
            .put("b/manifest.json", Bytes::from("second, longer"))
            .await
            .unwrap();
        assert_eq!(
            backend.get("b/manifest.json").await.unwrap(),
            Bytes::from("second, longer")
        );
        assert_eq!(all_file_names(temp_dir.path()), vec!["manifest.json"]);
    }

    #[tokio::test]
    async fn interrupted_writes_are_invisible() {
        // A crash between writing the temp file and the rename leaves the
        // temp file behind; it must never surface as a key.
        let temp_dir = TempDir::new().unwrap();
        let backend = FilesystemBackend::new(temp_dir.path().to_path_buf());
        backend.put("t/seg-1", Bytes::from("ok")).await.unwrap();
        let stale = temp_dir
            .path()
            .join("t")
            .join(format!(".seg-2{}999-0", TEMP_MARKER));
        std::fs::write(&stale, b"half a segm").unwrap();
        assert_eq!(backend.list("t/").await.unwrap(), vec!["t/seg-1"]);
        assert!(!backend.exists("t/seg-2").await.unwrap());
    }

    #[tokio::test]
    async fn concurrent_puts_never_leave_a_torn_file() {
        let temp_dir = TempDir::new().unwrap();
        let backend = std::sync::Arc::new(FilesystemBackend::new(temp_dir.path().to_path_buf()));
        let payloads: Vec<Bytes> = (0u8..16)
            .map(|i| Bytes::from(vec![i; 256 * 1024]))
            .collect();
        let tasks: Vec<_> = payloads
            .iter()
            .cloned()
            .map(|p| {
                let backend = backend.clone();
                tokio::spawn(async move { backend.put("x/offsets.db", p).await })
            })
            .collect();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        let got = backend.get("x/offsets.db").await.unwrap();
        assert!(
            payloads.contains(&got),
            "content is one complete payload, never a mix"
        );
        assert_eq!(all_file_names(temp_dir.path()), vec!["offsets.db"]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_put_keeps_the_previous_content() {
        use std::os::unix::fs::PermissionsExt;
        let temp_dir = TempDir::new().unwrap();
        let backend = FilesystemBackend::new(temp_dir.path().to_path_buf());
        backend.put("d/state", Bytes::from("good")).await.unwrap();

        let dir = temp_dir.path().join("d");
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
        // Root ignores directory permissions; nothing to test then.
        let writable = std::fs::write(dir.join("probe"), b"").is_ok();
        let result = backend.put("d/state", Bytes::from("partial")).await;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        if writable {
            return;
        }
        assert!(result.is_err());
        assert_eq!(backend.get("d/state").await.unwrap(), Bytes::from("good"));
        assert_eq!(all_file_names(temp_dir.path()), vec!["state"]);
    }

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
