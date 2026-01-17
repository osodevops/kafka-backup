//! Instrumented storage backend decorator.
//!
//! This module provides a decorator that wraps any `StorageBackend` implementation
//! with metrics instrumentation, recording latency, bytes transferred, and errors.

use async_trait::async_trait;
use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;

use super::labels::{ErrorType, StorageOperation};
use super::registry::PrometheusMetrics;
use crate::storage::{ObjectMetadata, StorageBackend};
use crate::Result;

/// A storage backend wrapper that records metrics for all operations.
///
/// This decorator wraps any `StorageBackend` implementation and automatically
/// records the following metrics:
/// - Write latency (histograms)
/// - Read latency (histograms)
/// - Bytes written/read (counters)
/// - Errors by type (counters)
///
/// # Example
///
/// ```rust,ignore
/// use kafka_backup_core::storage::create_backend;
/// use kafka_backup_core::metrics::{PrometheusMetrics, InstrumentedStorageBackend};
///
/// let config = StorageBackendConfig::Memory;
/// let backend = create_backend(&config)?;
/// let metrics = Arc::new(PrometheusMetrics::new());
///
/// let instrumented = InstrumentedStorageBackend::new(backend, "memory", metrics, "backup-001");
/// ```
pub struct InstrumentedStorageBackend {
    /// The wrapped storage backend.
    inner: Arc<dyn StorageBackend>,

    /// The backend name for metric labels (s3, azure, gcs, filesystem, memory).
    backend_name: String,

    /// Reference to the Prometheus metrics registry.
    metrics: Arc<PrometheusMetrics>,

    /// The backup ID for metrics labels.
    backup_id: String,
}

impl InstrumentedStorageBackend {
    /// Create a new instrumented storage backend.
    ///
    /// # Arguments
    /// * `inner` - The storage backend to wrap
    /// * `backend_name` - Name for metric labels (e.g., "s3", "azure", "gcs", "filesystem", "memory")
    /// * `metrics` - Reference to the Prometheus metrics registry
    /// * `backup_id` - The backup ID for metrics labels
    pub fn new(
        inner: Arc<dyn StorageBackend>,
        backend_name: impl Into<String>,
        metrics: Arc<PrometheusMetrics>,
        backup_id: impl Into<String>,
    ) -> Self {
        Self {
            inner,
            backend_name: backend_name.into(),
            metrics,
            backup_id: backup_id.into(),
        }
    }

    /// Get the backend name.
    pub fn backend_name(&self) -> &str {
        &self.backend_name
    }

    /// Get the inner storage backend.
    pub fn inner(&self) -> &Arc<dyn StorageBackend> {
        &self.inner
    }

    /// Record a storage error.
    fn record_error(&self, error: &crate::Error) {
        let error_type = ErrorType::from_error(error);
        self.metrics
            .inc_storage_error(&self.backend_name, error_type);
    }

    /// Classify the operation type from a storage key.
    fn classify_operation(&self, key: &str) -> StorageOperation {
        StorageOperation::from_key(key)
    }
}

#[async_trait]
impl StorageBackend for InstrumentedStorageBackend {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let operation = self.classify_operation(key);
        let bytes_len = data.len() as u64;
        let start = Instant::now();

        let result = self.inner.put(key, data).await;

        let latency = start.elapsed().as_secs_f64();
        self.metrics
            .record_storage_write_latency(&self.backend_name, operation, latency);

        match &result {
            Ok(()) => {
                self.metrics.inc_storage_write_bytes(
                    &self.backend_name,
                    &self.backup_id,
                    bytes_len,
                );
            }
            Err(e) => {
                self.record_error(e);
            }
        }

        result
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let operation = self.classify_operation(key);
        let start = Instant::now();

        let result = self.inner.get(key).await;

        let latency = start.elapsed().as_secs_f64();
        self.metrics
            .record_storage_read_latency(&self.backend_name, operation, latency);

        match &result {
            Ok(data) => {
                self.metrics.inc_storage_read_bytes(
                    &self.backend_name,
                    &self.backup_id,
                    data.len() as u64,
                );
            }
            Err(e) => {
                self.record_error(e);
            }
        }

        result
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let start = Instant::now();

        let result = self.inner.list(prefix).await;

        let latency = start.elapsed().as_secs_f64();
        self.metrics.record_storage_read_latency(
            &self.backend_name,
            StorageOperation::Other,
            latency,
        );

        if let Err(e) = &result {
            self.record_error(e);
        }

        result
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let start = Instant::now();

        let result = self.inner.exists(key).await;

        let latency = start.elapsed().as_secs_f64();
        self.metrics.record_storage_read_latency(
            &self.backend_name,
            StorageOperation::Other,
            latency,
        );

        if let Err(e) = &result {
            self.record_error(e);
        }

        result
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let operation = self.classify_operation(key);
        let start = Instant::now();

        let result = self.inner.delete(key).await;

        let latency = start.elapsed().as_secs_f64();
        self.metrics
            .record_storage_write_latency(&self.backend_name, operation, latency);

        if let Err(e) = &result {
            self.record_error(e);
        }

        result
    }

    async fn size(&self, key: &str) -> Result<u64> {
        let start = Instant::now();

        let result = self.inner.size(key).await;

        let latency = start.elapsed().as_secs_f64();
        self.metrics.record_storage_read_latency(
            &self.backend_name,
            StorageOperation::Other,
            latency,
        );

        if let Err(e) = &result {
            self.record_error(e);
        }

        result
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let start = Instant::now();

        let result = self.inner.head(key).await;

        let latency = start.elapsed().as_secs_f64();
        self.metrics.record_storage_read_latency(
            &self.backend_name,
            StorageOperation::Other,
            latency,
        );

        if let Err(e) = &result {
            self.record_error(e);
        }

        result
    }

    async fn copy(&self, src: &str, dest: &str) -> Result<()> {
        let operation = self.classify_operation(dest);
        let start = Instant::now();

        let result = self.inner.copy(src, dest).await;

        let latency = start.elapsed().as_secs_f64();
        self.metrics
            .record_storage_write_latency(&self.backend_name, operation, latency);

        if let Err(e) = &result {
            self.record_error(e);
        }

        result
    }
}

/// Determine the backend name from a `StorageBackendConfig`.
pub fn backend_name_from_config(config: &crate::storage::StorageBackendConfig) -> &'static str {
    match config {
        crate::storage::StorageBackendConfig::S3 { .. } => "s3",
        crate::storage::StorageBackendConfig::Azure { .. } => "azure",
        crate::storage::StorageBackendConfig::Gcs { .. } => "gcs",
        crate::storage::StorageBackendConfig::Filesystem { .. } => "filesystem",
        crate::storage::StorageBackendConfig::Memory => "memory",
    }
}

/// Create an instrumented storage backend from configuration.
///
/// This is a convenience function that creates a storage backend and wraps it
/// with metrics instrumentation.
///
/// # Arguments
/// * `config` - The storage backend configuration
/// * `metrics` - Reference to the Prometheus metrics registry
/// * `backup_id` - The backup ID for metrics labels
///
/// # Returns
/// An `Arc<dyn StorageBackend>` that records metrics for all operations.
pub fn create_instrumented_backend(
    config: &crate::storage::StorageBackendConfig,
    metrics: Arc<PrometheusMetrics>,
    backup_id: impl Into<String>,
) -> Result<Arc<dyn StorageBackend>> {
    let inner = crate::storage::create_backend(config)?;
    let backend_name = backend_name_from_config(config);

    Ok(Arc::new(InstrumentedStorageBackend::new(
        inner,
        backend_name,
        metrics,
        backup_id,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::StorageBackendConfig;

    #[tokio::test]
    async fn test_instrumented_put_get() {
        let config = StorageBackendConfig::Memory;
        let metrics = Arc::new(PrometheusMetrics::new());

        let instrumented =
            create_instrumented_backend(&config, metrics.clone(), "test-backup").unwrap();

        // Put some data
        let key = "test/data.txt";
        let data = Bytes::from("Hello, World!");
        instrumented.put(key, data.clone()).await.unwrap();

        // Get the data back
        let retrieved = instrumented.get(key).await.unwrap();
        assert_eq!(data, retrieved);

        // Check metrics were recorded
        let encoded = metrics.encode();
        assert!(encoded.contains("kafka_backup_storage_write_latency_seconds"));
        assert!(encoded.contains("kafka_backup_storage_read_latency_seconds"));
        assert!(encoded.contains("kafka_backup_storage_write_bytes_total"));
        assert!(encoded.contains("kafka_backup_storage_read_bytes_total"));
    }

    #[tokio::test]
    async fn test_instrumented_list() {
        let config = StorageBackendConfig::Memory;
        let metrics = Arc::new(PrometheusMetrics::new());

        let instrumented =
            create_instrumented_backend(&config, metrics.clone(), "test-backup").unwrap();

        // Put some data
        instrumented
            .put("prefix/file1.txt", Bytes::from("data1"))
            .await
            .unwrap();
        instrumented
            .put("prefix/file2.txt", Bytes::from("data2"))
            .await
            .unwrap();

        // List
        let files = instrumented.list("prefix/").await.unwrap();
        assert_eq!(files.len(), 2);

        // Check metrics
        let encoded = metrics.encode();
        assert!(encoded.contains("kafka_backup_storage_read_latency_seconds"));
    }

    #[tokio::test]
    async fn test_instrumented_exists_delete() {
        let config = StorageBackendConfig::Memory;
        let metrics = Arc::new(PrometheusMetrics::new());

        let instrumented =
            create_instrumented_backend(&config, metrics.clone(), "test-backup").unwrap();

        let key = "test/file.txt";
        instrumented.put(key, Bytes::from("data")).await.unwrap();

        assert!(instrumented.exists(key).await.unwrap());

        instrumented.delete(key).await.unwrap();

        assert!(!instrumented.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn test_operation_classification() {
        let config = StorageBackendConfig::Memory;
        let metrics = Arc::new(PrometheusMetrics::new());
        let backup_id = "test-backup";

        let instrumented =
            create_instrumented_backend(&config, metrics.clone(), backup_id).unwrap();

        // Segment file
        instrumented
            .put(
                "backup-001/topics/orders/partition=0/segment-000001.zst",
                Bytes::from("segment data"),
            )
            .await
            .unwrap();

        // Manifest file
        instrumented
            .put("backup-001/manifest.json", Bytes::from("{}"))
            .await
            .unwrap();

        // Checkpoint file
        instrumented
            .put("backup-001/offsets.db", Bytes::from("checkpoint"))
            .await
            .unwrap();

        let encoded = metrics.encode();
        assert!(encoded.contains("segment"));
        assert!(encoded.contains("manifest"));
        assert!(encoded.contains("checkpoint"));
    }

    #[test]
    fn test_backend_name_from_config() {
        assert_eq!(
            backend_name_from_config(&StorageBackendConfig::Memory),
            "memory"
        );
        assert_eq!(
            backend_name_from_config(&StorageBackendConfig::Filesystem {
                path: std::path::PathBuf::from("/tmp")
            }),
            "filesystem"
        );
    }
}
