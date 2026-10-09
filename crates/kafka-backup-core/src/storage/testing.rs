//! Test-only storage wrapper that injects failures and records writes.

use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;

use super::{MemoryBackend, ObjectMetadata, StorageBackend};
use crate::error::StorageError;
use crate::{Error, Result};

/// A [`MemoryBackend`] whose reads of chosen keys fail, e.g. with an S3 403
/// (`Backend`) rather than `NotFound`, and that records every `put`.
#[derive(Default)]
pub struct FaultyStorage {
    inner: MemoryBackend,
    /// (key suffix, error) for get / exists / head / size.
    failing_reads: Mutex<Vec<(String, StorageError)>>,
    puts: Mutex<Vec<String>>,
}

impl FaultyStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fail reads of keys ending in `suffix` with `error`.
    pub fn fail_reads(&self, suffix: &str, error: StorageError) {
        self.failing_reads
            .lock()
            .unwrap()
            .push((suffix.to_string(), error));
    }

    /// Fail reads of keys ending in `suffix` like S3 403 AccessDenied.
    pub fn deny_reads(&self, suffix: &str) {
        self.fail_reads(
            suffix,
            StorageError::Backend("S3 GET failed: 403 AccessDenied".to_string()),
        );
    }

    /// Stop failing reads.
    pub fn heal(&self) {
        self.failing_reads.lock().unwrap().clear();
    }

    /// Keys written so far, in order.
    pub fn puts(&self) -> Vec<String> {
        self.puts.lock().unwrap().clone()
    }

    fn check(&self, key: &str) -> Result<()> {
        let failing = self.failing_reads.lock().unwrap();
        match failing
            .iter()
            .find(|(suffix, _)| key.ends_with(suffix.as_str()))
        {
            Some((_, e)) => Err(Error::Storage(match e {
                StorageError::NotFound(_) => StorageError::NotFound(key.to_string()),
                StorageError::PermissionDenied(m) => StorageError::PermissionDenied(m.clone()),
                StorageError::Backend(m) => StorageError::Backend(m.clone()),
                StorageError::InvalidPath(m) => StorageError::InvalidPath(m.clone()),
            })),
            None => Ok(()),
        }
    }
}

#[async_trait]
impl StorageBackend for FaultyStorage {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        self.puts.lock().unwrap().push(key.to_string());
        self.inner.put(key, data).await
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        self.check(key)?;
        self.inner.get(key).await
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list(prefix).await
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        self.check(key)?;
        self.inner.exists(key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn size(&self, key: &str) -> Result<u64> {
        self.check(key)?;
        self.inner.size(key).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        self.check(key)?;
        self.inner.head(key).await
    }

    async fn copy(&self, src: &str, dest: &str) -> Result<()> {
        self.check(src)?;
        self.inner.copy(src, dest).await
    }
}
