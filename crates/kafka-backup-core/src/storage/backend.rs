//! Storage backend trait definition.

use async_trait::async_trait;
use bytes::Bytes;

use crate::Result;

/// Metadata about a stored object
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// Size in bytes
    pub size: u64,
    /// Last modified timestamp (epoch milliseconds)
    pub last_modified: i64,
    /// ETag or content hash (if available)
    pub e_tag: Option<String>,
}

/// Trait for storage backends
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Write data to a key
    async fn put(&self, key: &str, data: Bytes) -> Result<()>;

    /// Read data from a key
    async fn get(&self, key: &str) -> Result<Bytes>;

    /// List keys with a given prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Check if a key exists
    async fn exists(&self, key: &str) -> Result<bool>;

    /// Delete a key
    async fn delete(&self, key: &str) -> Result<()>;

    /// Get the size of an object
    async fn size(&self, key: &str) -> Result<u64>;

    /// Get object metadata (size, last modified, etc.)
    async fn head(&self, key: &str) -> Result<ObjectMetadata>;

    /// Copy object from source to destination
    async fn copy(&self, src: &str, dest: &str) -> Result<()>;
}
