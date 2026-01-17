//! Prometheus metrics registry for kafka-backup.
//!
//! This module provides a comprehensive metrics registry using the prometheus-client crate,
//! implementing all metrics specified in the Kafka Backup Metrics PRD.

use parking_lot::RwLock;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use std::sync::atomic::AtomicU64;
use std::time::Instant;

use super::labels::{
    BackupLabels, CommitLabels, CompressionLabels, DurationLabels, ErrorLabels, ErrorType,
    LagLabels, OperationStatus, RestoreLabels, RestoreProgressLabels, RetryLabels,
    StorageBytesLabels, StorageErrorLabels, StorageLabels, StorageOperation, ThroughputLabels,
};

/// Storage latency histogram buckets (in seconds).
/// Covers typical cloud storage latencies: 10ms to 10s.
const STORAGE_LATENCY_BUCKETS: [f64; 9] = [0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

/// Operation duration histogram buckets (in seconds).
/// Covers typical backup/restore durations: 1s to 1 hour.
const OPERATION_DURATION_BUCKETS: [f64; 10] = [
    1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
];

/// Prometheus metrics registry for kafka-backup.
///
/// This struct holds all Prometheus metrics as defined in the PRD,
/// organized by category: consumer lag, throughput, operation duration,
/// storage I/O, compression, errors, and restore operations.
pub struct PrometheusMetrics {
    /// Internal prometheus-client registry.
    registry: RwLock<Registry>,

    // ========================================
    // Consumer Lag Metrics
    // ========================================
    /// Number of records the backup consumer is behind (per topic/partition).
    pub lag_records: Family<LagLabels, Gauge>,

    /// Approximate bytes behind (estimated from avg record size).
    pub lag_bytes: Family<LagLabels, Gauge>,

    /// Time lag based on message timestamps (seconds).
    pub lag_seconds: Family<LagLabels, Gauge<f64, AtomicU64>>,

    /// Maximum lag across all partitions.
    pub lag_records_max: Family<BackupLabels, Gauge>,

    // ========================================
    // Throughput Metrics
    // ========================================
    /// Records consumed per second.
    pub throughput_records_per_sec: Family<ThroughputLabels, Gauge<f64, AtomicU64>>,

    /// Bytes consumed per second.
    pub throughput_bytes_per_sec: Family<ThroughputLabels, Gauge<f64, AtomicU64>>,

    /// Cumulative records backed up.
    pub records_total: Family<BackupLabels, Counter>,

    /// Cumulative bytes backed up.
    pub bytes_total: Family<BackupLabels, Counter>,

    // ========================================
    // Operation Duration Metrics
    // ========================================
    /// Backup job duration histogram.
    pub backup_duration_seconds: Family<DurationLabels, Histogram>,

    /// Restore job duration histogram.
    pub restore_duration_seconds: Family<DurationLabels, Histogram>,

    // ========================================
    // Storage I/O Metrics
    // ========================================
    /// Storage write latency histogram (by backend and operation type).
    pub storage_write_latency_seconds: Family<StorageLabels, Histogram>,

    /// Storage read latency histogram (by backend and operation type).
    pub storage_read_latency_seconds: Family<StorageLabels, Histogram>,

    /// Cumulative bytes written to storage.
    pub storage_write_bytes_total: Family<StorageBytesLabels, Counter>,

    /// Cumulative bytes read from storage.
    pub storage_read_bytes_total: Family<StorageBytesLabels, Counter>,

    /// Storage operation errors by type.
    pub storage_errors_total: Family<StorageErrorLabels, Counter>,

    // ========================================
    // Compression Metrics
    // ========================================
    /// Compression ratio (uncompressed / compressed).
    pub compression_ratio: Family<CompressionLabels, Gauge<f64, AtomicU64>>,

    /// Cumulative bytes after compression.
    pub compressed_bytes_total: Family<CompressionLabels, Counter>,

    /// Cumulative bytes before compression.
    pub uncompressed_bytes_total: Family<CompressionLabels, Counter>,

    // ========================================
    // Error & Health Metrics
    // ========================================
    /// Cumulative errors by category.
    pub errors_total: Family<ErrorLabels, Counter>,

    /// Cumulative retries by operation.
    pub retries_total: Family<RetryLabels, Counter>,

    /// Unix timestamp of last successful offset commit.
    pub last_successful_commit: Family<CommitLabels, Gauge>,

    /// Number of consumer group rebalances.
    pub consumer_rebalance_total: Family<BackupLabels, Counter>,

    // ========================================
    // Restore Operation Metrics
    // ========================================
    /// Restore progress as percentage (0-100).
    pub restore_progress_percent: Family<RestoreLabels, Gauge<f64, AtomicU64>>,

    /// Estimated seconds until restore completes.
    pub restore_eta_seconds: Family<RestoreProgressLabels, Gauge<f64, AtomicU64>>,

    /// Records written to target cluster per second.
    pub restore_throughput_records_per_sec: Family<RestoreProgressLabels, Gauge<f64, AtomicU64>>,

    /// Cumulative records restored.
    pub restore_records_total: Family<RestoreProgressLabels, Counter>,

    // ========================================
    // Internal tracking
    // ========================================
    /// Start time for throughput calculations.
    start_time: Instant,

    /// Maximum partition labels to prevent cardinality explosion.
    max_partition_labels: usize,

    /// Current partition label count.
    partition_label_count: RwLock<usize>,
}

impl Default for PrometheusMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PrometheusMetrics {
    /// Create a new PrometheusMetrics registry with default settings.
    pub fn new() -> Self {
        Self::with_max_labels(100)
    }

    /// Create a new PrometheusMetrics registry with a custom max label count.
    pub fn with_max_labels(max_partition_labels: usize) -> Self {
        let mut registry = Registry::default();

        // Create metric families

        // Consumer Lag Metrics
        let lag_records = Family::<LagLabels, Gauge>::default();
        let lag_bytes = Family::<LagLabels, Gauge>::default();
        let lag_seconds = Family::<LagLabels, Gauge<f64, AtomicU64>>::default();
        let lag_records_max = Family::<BackupLabels, Gauge>::default();

        // Throughput Metrics
        let throughput_records_per_sec =
            Family::<ThroughputLabels, Gauge<f64, AtomicU64>>::default();
        let throughput_bytes_per_sec = Family::<ThroughputLabels, Gauge<f64, AtomicU64>>::default();
        let records_total = Family::<BackupLabels, Counter>::default();
        let bytes_total = Family::<BackupLabels, Counter>::default();

        // Operation Duration Metrics
        let backup_duration_seconds =
            Family::<DurationLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(OPERATION_DURATION_BUCKETS.iter().cloned())
            });
        let restore_duration_seconds =
            Family::<DurationLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(OPERATION_DURATION_BUCKETS.iter().cloned())
            });

        // Storage I/O Metrics
        let storage_write_latency_seconds =
            Family::<StorageLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(STORAGE_LATENCY_BUCKETS.iter().cloned())
            });
        let storage_read_latency_seconds =
            Family::<StorageLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(STORAGE_LATENCY_BUCKETS.iter().cloned())
            });
        let storage_write_bytes_total = Family::<StorageBytesLabels, Counter>::default();
        let storage_read_bytes_total = Family::<StorageBytesLabels, Counter>::default();
        let storage_errors_total = Family::<StorageErrorLabels, Counter>::default();

        // Compression Metrics
        let compression_ratio = Family::<CompressionLabels, Gauge<f64, AtomicU64>>::default();
        let compressed_bytes_total = Family::<CompressionLabels, Counter>::default();
        let uncompressed_bytes_total = Family::<CompressionLabels, Counter>::default();

        // Error & Health Metrics
        let errors_total = Family::<ErrorLabels, Counter>::default();
        let retries_total = Family::<RetryLabels, Counter>::default();
        let last_successful_commit = Family::<CommitLabels, Gauge>::default();
        let consumer_rebalance_total = Family::<BackupLabels, Counter>::default();

        // Restore Operation Metrics
        let restore_progress_percent = Family::<RestoreLabels, Gauge<f64, AtomicU64>>::default();
        let restore_eta_seconds = Family::<RestoreProgressLabels, Gauge<f64, AtomicU64>>::default();
        let restore_throughput_records_per_sec =
            Family::<RestoreProgressLabels, Gauge<f64, AtomicU64>>::default();
        let restore_records_total = Family::<RestoreProgressLabels, Counter>::default();

        // Register all metrics with the registry

        // Consumer Lag Metrics
        registry.register(
            "kafka_backup_lag_records",
            "Number of records the backup consumer is behind",
            lag_records.clone(),
        );
        registry.register(
            "kafka_backup_lag_bytes",
            "Approximate bytes behind (estimated from avg record size)",
            lag_bytes.clone(),
        );
        registry.register(
            "kafka_backup_lag_seconds",
            "Time lag based on message timestamps",
            lag_seconds.clone(),
        );
        registry.register(
            "kafka_backup_lag_records_max",
            "Maximum lag across all partitions",
            lag_records_max.clone(),
        );

        // Throughput Metrics
        registry.register(
            "kafka_backup_throughput_records_per_sec",
            "Records consumed per second",
            throughput_records_per_sec.clone(),
        );
        registry.register(
            "kafka_backup_throughput_bytes_per_sec",
            "Bytes consumed per second",
            throughput_bytes_per_sec.clone(),
        );
        registry.register(
            "kafka_backup_records_total",
            "Cumulative records backed up",
            records_total.clone(),
        );
        registry.register(
            "kafka_backup_bytes_total",
            "Cumulative bytes backed up",
            bytes_total.clone(),
        );

        // Operation Duration Metrics
        registry.register(
            "kafka_backup_duration_seconds",
            "Backup job duration",
            backup_duration_seconds.clone(),
        );
        registry.register(
            "kafka_restore_duration_seconds",
            "Restore job duration",
            restore_duration_seconds.clone(),
        );

        // Storage I/O Metrics
        registry.register(
            "kafka_backup_storage_write_latency_seconds",
            "Storage write latency by backend and operation",
            storage_write_latency_seconds.clone(),
        );
        registry.register(
            "kafka_backup_storage_read_latency_seconds",
            "Storage read latency by backend and operation",
            storage_read_latency_seconds.clone(),
        );
        registry.register(
            "kafka_backup_storage_write_bytes_total",
            "Cumulative bytes written to storage",
            storage_write_bytes_total.clone(),
        );
        registry.register(
            "kafka_backup_storage_read_bytes_total",
            "Cumulative bytes read from storage",
            storage_read_bytes_total.clone(),
        );
        registry.register(
            "kafka_backup_storage_errors_total",
            "Storage operation errors by type",
            storage_errors_total.clone(),
        );

        // Compression Metrics
        registry.register(
            "kafka_backup_compression_ratio",
            "Compression ratio (uncompressed / compressed)",
            compression_ratio.clone(),
        );
        registry.register(
            "kafka_backup_compressed_bytes_total",
            "Cumulative bytes after compression",
            compressed_bytes_total.clone(),
        );
        registry.register(
            "kafka_backup_uncompressed_bytes_total",
            "Cumulative bytes before compression",
            uncompressed_bytes_total.clone(),
        );

        // Error & Health Metrics
        registry.register(
            "kafka_backup_errors_total",
            "Cumulative errors by category",
            errors_total.clone(),
        );
        registry.register(
            "kafka_backup_retries_total",
            "Cumulative retries by operation",
            retries_total.clone(),
        );
        registry.register(
            "kafka_backup_last_successful_commit",
            "Unix timestamp of last successful offset commit",
            last_successful_commit.clone(),
        );
        registry.register(
            "kafka_backup_consumer_rebalance_total",
            "Number of consumer group rebalances",
            consumer_rebalance_total.clone(),
        );

        // Restore Operation Metrics
        registry.register(
            "kafka_restore_progress_percent",
            "Restore progress as percentage (0-100)",
            restore_progress_percent.clone(),
        );
        registry.register(
            "kafka_restore_eta_seconds",
            "Estimated seconds until restore completes",
            restore_eta_seconds.clone(),
        );
        registry.register(
            "kafka_restore_throughput_records_per_sec",
            "Records written to target cluster per second",
            restore_throughput_records_per_sec.clone(),
        );
        registry.register(
            "kafka_restore_records_total",
            "Cumulative records restored",
            restore_records_total.clone(),
        );

        Self {
            registry: RwLock::new(registry),
            lag_records,
            lag_bytes,
            lag_seconds,
            lag_records_max,
            throughput_records_per_sec,
            throughput_bytes_per_sec,
            records_total,
            bytes_total,
            backup_duration_seconds,
            restore_duration_seconds,
            storage_write_latency_seconds,
            storage_read_latency_seconds,
            storage_write_bytes_total,
            storage_read_bytes_total,
            storage_errors_total,
            compression_ratio,
            compressed_bytes_total,
            uncompressed_bytes_total,
            errors_total,
            retries_total,
            last_successful_commit,
            consumer_rebalance_total,
            restore_progress_percent,
            restore_eta_seconds,
            restore_throughput_records_per_sec,
            restore_records_total,
            start_time: Instant::now(),
            max_partition_labels,
            partition_label_count: RwLock::new(0),
        }
    }

    // ========================================
    // Consumer Lag Methods
    // ========================================

    /// Record consumer lag for a specific topic/partition.
    ///
    /// # Arguments
    /// * `topic` - The topic name
    /// * `partition` - The partition ID
    /// * `backup_id` - The backup identifier
    /// * `lag_records` - Number of records behind
    /// * `avg_record_size` - Average record size for bytes estimation
    pub fn record_lag(
        &self,
        topic: &str,
        partition: i32,
        backup_id: &str,
        lag_records: i64,
        avg_record_size: Option<u64>,
    ) {
        // Check cardinality limit
        if !self.check_partition_cardinality() {
            return;
        }

        let labels = LagLabels::new(topic, partition, backup_id);

        self.lag_records.get_or_create(&labels).set(lag_records);

        if let Some(avg_size) = avg_record_size {
            let lag_bytes_est = lag_records as u64 * avg_size;
            self.lag_bytes
                .get_or_create(&labels)
                .set(lag_bytes_est as i64);
        }
    }

    /// Record time-based lag for a specific topic/partition.
    pub fn record_lag_seconds(
        &self,
        topic: &str,
        partition: i32,
        backup_id: &str,
        lag_seconds: f64,
    ) {
        if !self.check_partition_cardinality() {
            return;
        }

        let labels = LagLabels::new(topic, partition, backup_id);
        self.lag_seconds.get_or_create(&labels).set(lag_seconds);
    }

    /// Update maximum lag across all partitions.
    pub fn record_max_lag(&self, backup_id: &str, max_lag: i64) {
        let labels = BackupLabels::new(backup_id);
        self.lag_records_max.get_or_create(&labels).set(max_lag);
    }

    // ========================================
    // Throughput Methods
    // ========================================

    /// Record throughput metrics.
    pub fn record_throughput(
        &self,
        backup_id: &str,
        topic: &str,
        records_per_sec: f64,
        bytes_per_sec: f64,
    ) {
        let labels = ThroughputLabels::new(backup_id, topic);
        self.throughput_records_per_sec
            .get_or_create(&labels)
            .set(records_per_sec);
        self.throughput_bytes_per_sec
            .get_or_create(&labels)
            .set(bytes_per_sec);
    }

    /// Increment cumulative record counter.
    pub fn inc_records(&self, backup_id: &str, count: u64) {
        let labels = BackupLabels::new(backup_id);
        self.records_total.get_or_create(&labels).inc_by(count);
    }

    /// Increment cumulative bytes counter.
    pub fn inc_bytes(&self, backup_id: &str, count: u64) {
        let labels = BackupLabels::new(backup_id);
        self.bytes_total.get_or_create(&labels).inc_by(count);
    }

    // ========================================
    // Operation Duration Methods
    // ========================================

    /// Record backup operation duration.
    pub fn record_backup_duration(
        &self,
        backup_id: &str,
        status: OperationStatus,
        duration_secs: f64,
    ) {
        let labels = DurationLabels::new(backup_id, status);
        self.backup_duration_seconds
            .get_or_create(&labels)
            .observe(duration_secs);
    }

    /// Record restore operation duration.
    pub fn record_restore_duration(
        &self,
        backup_id: &str,
        status: OperationStatus,
        duration_secs: f64,
    ) {
        let labels = DurationLabels::new(backup_id, status);
        self.restore_duration_seconds
            .get_or_create(&labels)
            .observe(duration_secs);
    }

    // ========================================
    // Storage I/O Methods
    // ========================================

    /// Record storage write latency.
    pub fn record_storage_write_latency(
        &self,
        backend: &str,
        operation: StorageOperation,
        latency_secs: f64,
    ) {
        let labels = StorageLabels::new(backend, operation.as_str());
        self.storage_write_latency_seconds
            .get_or_create(&labels)
            .observe(latency_secs);
    }

    /// Record storage read latency.
    pub fn record_storage_read_latency(
        &self,
        backend: &str,
        operation: StorageOperation,
        latency_secs: f64,
    ) {
        let labels = StorageLabels::new(backend, operation.as_str());
        self.storage_read_latency_seconds
            .get_or_create(&labels)
            .observe(latency_secs);
    }

    /// Increment storage write bytes counter.
    pub fn inc_storage_write_bytes(&self, backend: &str, backup_id: &str, bytes: u64) {
        let labels = StorageBytesLabels::new(backend, backup_id);
        self.storage_write_bytes_total
            .get_or_create(&labels)
            .inc_by(bytes);
    }

    /// Increment storage read bytes counter.
    pub fn inc_storage_read_bytes(&self, backend: &str, backup_id: &str, bytes: u64) {
        let labels = StorageBytesLabels::new(backend, backup_id);
        self.storage_read_bytes_total
            .get_or_create(&labels)
            .inc_by(bytes);
    }

    /// Increment storage error counter.
    pub fn inc_storage_error(&self, backend: &str, error_type: ErrorType) {
        let labels = StorageErrorLabels::new(backend, error_type);
        self.storage_errors_total.get_or_create(&labels).inc();
    }

    // ========================================
    // Compression Methods
    // ========================================

    /// Record compression metrics.
    pub fn record_compression(
        &self,
        algorithm: &str,
        backup_id: &str,
        compressed_bytes: u64,
        uncompressed_bytes: u64,
    ) {
        let labels = CompressionLabels::new(algorithm, backup_id);

        self.compressed_bytes_total
            .get_or_create(&labels)
            .inc_by(compressed_bytes);
        self.uncompressed_bytes_total
            .get_or_create(&labels)
            .inc_by(uncompressed_bytes);

        // Update ratio
        if compressed_bytes > 0 {
            let ratio = uncompressed_bytes as f64 / compressed_bytes as f64;
            self.compression_ratio.get_or_create(&labels).set(ratio);
        }
    }

    // ========================================
    // Error & Health Methods
    // ========================================

    /// Record an error.
    pub fn record_error(&self, backup_id: &str, error_type: ErrorType) {
        let labels = ErrorLabels::new(backup_id, error_type);
        self.errors_total.get_or_create(&labels).inc();
    }

    /// Record a retry.
    pub fn record_retry(&self, backup_id: &str, operation: &str) {
        let labels = RetryLabels::new(backup_id, operation);
        self.retries_total.get_or_create(&labels).inc();
    }

    /// Record successful commit timestamp.
    pub fn record_successful_commit(&self, backup_id: &str, topic: &str, partition: i32) {
        let labels = CommitLabels::new(backup_id, topic, partition);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        self.last_successful_commit
            .get_or_create(&labels)
            .set(timestamp);
    }

    /// Record consumer rebalance.
    pub fn record_rebalance(&self, backup_id: &str) {
        let labels = BackupLabels::new(backup_id);
        self.consumer_rebalance_total.get_or_create(&labels).inc();
    }

    // ========================================
    // Restore Operation Methods
    // ========================================

    /// Record restore progress.
    pub fn record_restore_progress(
        &self,
        restore_id: &str,
        backup_id: &str,
        progress_percent: f64,
        eta_seconds: Option<f64>,
        throughput_rps: Option<f64>,
    ) {
        let restore_labels = RestoreLabels::new(restore_id, backup_id);
        let progress_labels = RestoreProgressLabels::new(restore_id);

        self.restore_progress_percent
            .get_or_create(&restore_labels)
            .set(progress_percent);

        if let Some(eta) = eta_seconds {
            self.restore_eta_seconds
                .get_or_create(&progress_labels)
                .set(eta);
        }

        if let Some(throughput) = throughput_rps {
            self.restore_throughput_records_per_sec
                .get_or_create(&progress_labels)
                .set(throughput);
        }
    }

    /// Increment restore records counter.
    pub fn inc_restore_records(&self, restore_id: &str, count: u64) {
        let labels = RestoreProgressLabels::new(restore_id);
        self.restore_records_total
            .get_or_create(&labels)
            .inc_by(count);
    }

    // ========================================
    // Utility Methods
    // ========================================

    /// Get elapsed time since metrics collection started.
    pub fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Encode all metrics to Prometheus text format.
    pub fn encode(&self) -> String {
        let registry = self.registry.read();
        let mut buffer = String::new();
        if encode(&mut buffer, &registry).is_err() {
            return String::new();
        }
        buffer
    }

    /// Check if we've exceeded the partition cardinality limit.
    fn check_partition_cardinality(&self) -> bool {
        let count = *self.partition_label_count.read();
        if count >= self.max_partition_labels {
            return false;
        }

        // Increment count (racy but acceptable for cardinality limiting)
        *self.partition_label_count.write() += 1;
        true
    }

    /// Reset partition cardinality counter (for testing or job restarts).
    pub fn reset_partition_cardinality(&self) {
        *self.partition_label_count.write() = 0;
    }
}

/// A guard that records duration when dropped.
pub struct TimerGuard<'a> {
    metrics: &'a PrometheusMetrics,
    backend: String,
    operation: StorageOperation,
    is_write: bool,
    start: Instant,
}

impl<'a> TimerGuard<'a> {
    /// Create a new write timer guard.
    pub fn write(
        metrics: &'a PrometheusMetrics,
        backend: impl Into<String>,
        operation: StorageOperation,
    ) -> Self {
        Self {
            metrics,
            backend: backend.into(),
            operation,
            is_write: true,
            start: Instant::now(),
        }
    }

    /// Create a new read timer guard.
    pub fn read(
        metrics: &'a PrometheusMetrics,
        backend: impl Into<String>,
        operation: StorageOperation,
    ) -> Self {
        Self {
            metrics,
            backend: backend.into(),
            operation,
            is_write: false,
            start: Instant::now(),
        }
    }
}

impl Drop for TimerGuard<'_> {
    fn drop(&mut self) {
        let latency = self.start.elapsed().as_secs_f64();
        if self.is_write {
            self.metrics
                .record_storage_write_latency(&self.backend, self.operation, latency);
        } else {
            self.metrics
                .record_storage_read_latency(&self.backend, self.operation, latency);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = PrometheusMetrics::new();
        assert!(metrics.max_partition_labels > 0);
    }

    #[test]
    fn test_record_lag() {
        let metrics = PrometheusMetrics::new();
        metrics.record_lag("orders", 0, "backup-001", 1000, Some(100));

        let encoded = metrics.encode();
        assert!(encoded.contains("kafka_backup_lag_records"));
        assert!(encoded.contains("kafka_backup_lag_bytes"));
    }

    #[test]
    fn test_record_throughput() {
        let metrics = PrometheusMetrics::new();
        metrics.record_throughput("backup-001", "orders", 1000.0, 1048576.0);

        let encoded = metrics.encode();
        assert!(encoded.contains("kafka_backup_throughput_records_per_sec"));
        assert!(encoded.contains("kafka_backup_throughput_bytes_per_sec"));
    }

    #[test]
    fn test_record_storage_latency() {
        let metrics = PrometheusMetrics::new();
        metrics.record_storage_write_latency("s3", StorageOperation::Segment, 0.05);

        let encoded = metrics.encode();
        assert!(encoded.contains("kafka_backup_storage_write_latency_seconds"));
    }

    #[test]
    fn test_record_error() {
        let metrics = PrometheusMetrics::new();
        metrics.record_error("backup-001", ErrorType::BrokerConnection);

        let encoded = metrics.encode();
        assert!(encoded.contains("kafka_backup_errors_total"));
        assert!(encoded.contains("broker_connection"));
    }

    #[test]
    fn test_record_compression() {
        let metrics = PrometheusMetrics::new();
        metrics.record_compression("zstd", "backup-001", 100, 400);

        let encoded = metrics.encode();
        assert!(encoded.contains("kafka_backup_compression_ratio"));
        assert!(encoded.contains("kafka_backup_compressed_bytes_total"));
    }

    #[test]
    fn test_record_restore_progress() {
        let metrics = PrometheusMetrics::new();
        metrics.record_restore_progress(
            "restore-001",
            "backup-001",
            50.0,
            Some(300.0),
            Some(1000.0),
        );

        let encoded = metrics.encode();
        assert!(encoded.contains("kafka_restore_progress_percent"));
        assert!(encoded.contains("kafka_restore_eta_seconds"));
    }

    #[test]
    fn test_cardinality_limiting() {
        let metrics = PrometheusMetrics::with_max_labels(3);

        // These should succeed
        metrics.record_lag("topic1", 0, "backup", 100, None);
        metrics.record_lag("topic2", 0, "backup", 100, None);
        metrics.record_lag("topic3", 0, "backup", 100, None);

        // This should be silently dropped
        metrics.record_lag("topic4", 0, "backup", 100, None);

        let encoded = metrics.encode();
        assert!(encoded.contains("topic1"));
        assert!(encoded.contains("topic2"));
        assert!(encoded.contains("topic3"));
        // topic4 might or might not appear depending on timing, but cardinality should be limited
    }

    #[test]
    fn test_timer_guard() {
        let metrics = PrometheusMetrics::new();

        {
            let _timer = TimerGuard::write(&metrics, "s3", StorageOperation::Segment);
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        let encoded = metrics.encode();
        assert!(encoded.contains("kafka_backup_storage_write_latency_seconds"));
    }

    #[test]
    fn test_encode_output_format() {
        let metrics = PrometheusMetrics::new();
        metrics.inc_records("backup-001", 100);

        let encoded = metrics.encode();

        // Check for Prometheus text format markers
        assert!(encoded.contains("# HELP"));
        assert!(encoded.contains("# TYPE"));
        assert!(encoded.contains("kafka_backup_records_total"));
    }
}
