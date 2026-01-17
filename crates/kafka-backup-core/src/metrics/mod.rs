//! Performance metrics collection and reporting.
//!
//! This module provides comprehensive metrics collection for monitoring backup/restore
//! performance, including consumer lag, throughput, compression ratios, latencies,
//! storage I/O, and error tracking.
//!
//! ## Modules
//!
//! - [`labels`] - Label types for Prometheus metrics dimensions
//! - [`registry`] - The main `PrometheusMetrics` registry with all metrics
//! - [`instrumented_storage`] - Storage backend decorator with metrics instrumentation
//! - [`server`] - HTTP server for exposing metrics via `/metrics` endpoint
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use kafka_backup_core::metrics::{PrometheusMetrics, MetricsServer, MetricsServerConfig};
//! use std::sync::Arc;
//!
//! // Create metrics registry
//! let metrics = Arc::new(PrometheusMetrics::new());
//!
//! // Record some metrics
//! metrics.record_lag("orders", 0, "backup-001", 1000, Some(100));
//! metrics.inc_records("backup-001", 500);
//!
//! // Start metrics server
//! let config = MetricsServerConfig::default();
//! let server = MetricsServer::new(config, metrics);
//! // server.run(shutdown_rx).await?;
//! ```

pub mod instrumented_storage;
pub mod labels;
pub mod registry;
pub mod server;

use parking_lot::RwLock;
use serde::Serialize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

// Re-export new Prometheus metrics types
pub use instrumented_storage::{
    backend_name_from_config, create_instrumented_backend, InstrumentedStorageBackend,
};
pub use labels::{
    BackupLabels, CommitLabels, CompressionLabels, DurationLabels, ErrorLabels, ErrorType,
    LagLabels, OperationStatus, RestoreLabels, RestoreProgressLabels, RetryLabels,
    StorageBytesLabels, StorageErrorLabels, StorageLabels, StorageOperation, ThroughputLabels,
};
pub use registry::{PrometheusMetrics, TimerGuard};
pub use server::{MetricsServer, MetricsServerConfig};

// Legacy exports (deprecated but kept for backward compatibility)
#[deprecated(since = "0.5.0", note = "Use PrometheusMetrics instead")]
pub use self::PerformanceMetrics as LegacyPerformanceMetrics;

/// Performance metrics collector
pub struct PerformanceMetrics {
    /// Total records processed
    pub records_processed: AtomicU64,
    /// Total bytes written (compressed)
    pub bytes_written: AtomicU64,
    /// Total bytes before compression
    pub bytes_uncompressed: AtomicU64,
    /// Checkpoint operation latencies in nanoseconds
    checkpoint_latencies_ns: RwLock<Vec<u64>>,
    /// Segment write latencies in nanoseconds
    segment_write_latencies_ns: RwLock<Vec<u64>>,
    /// Fetch latencies in nanoseconds
    fetch_latencies_ns: RwLock<Vec<u64>>,
    /// Start time of metrics collection
    start_time: Instant,
    /// Segments written
    pub segments_written: AtomicU64,
    /// Errors encountered
    pub errors: AtomicU64,
}

impl Default for PerformanceMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PerformanceMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            records_processed: AtomicU64::new(0),
            bytes_written: AtomicU64::new(0),
            bytes_uncompressed: AtomicU64::new(0),
            checkpoint_latencies_ns: RwLock::new(Vec::with_capacity(1000)),
            segment_write_latencies_ns: RwLock::new(Vec::with_capacity(1000)),
            fetch_latencies_ns: RwLock::new(Vec::with_capacity(1000)),
            start_time: Instant::now(),
            segments_written: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    /// Record records processed
    pub fn record_records(&self, count: u64) {
        self.records_processed.fetch_add(count, Ordering::Relaxed);
    }

    /// Record bytes written
    pub fn record_bytes(&self, compressed: u64, uncompressed: u64) {
        self.bytes_written.fetch_add(compressed, Ordering::Relaxed);
        self.bytes_uncompressed
            .fetch_add(uncompressed, Ordering::Relaxed);
    }

    /// Record segment write
    pub fn record_segment(&self) {
        self.segments_written.fetch_add(1, Ordering::Relaxed);
    }

    /// Record error
    pub fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record checkpoint latency
    pub fn record_checkpoint_latency(&self, duration: Duration) {
        let mut latencies = self.checkpoint_latencies_ns.write();
        latencies.push(duration.as_nanos() as u64);
        // Keep last 1000 samples
        if latencies.len() > 1000 {
            latencies.remove(0);
        }
    }

    /// Record segment write latency
    pub fn record_segment_write_latency(&self, duration: Duration) {
        let mut latencies = self.segment_write_latencies_ns.write();
        latencies.push(duration.as_nanos() as u64);
        if latencies.len() > 1000 {
            latencies.remove(0);
        }
    }

    /// Record fetch latency
    pub fn record_fetch_latency(&self, duration: Duration) {
        let mut latencies = self.fetch_latencies_ns.write();
        latencies.push(duration.as_nanos() as u64);
        if latencies.len() > 1000 {
            latencies.remove(0);
        }
    }

    /// Get elapsed time since start
    pub fn elapsed(&self) -> Duration {
        self.start_time.elapsed()
    }

    /// Calculate throughput in MB/s
    pub fn throughput_mbps(&self) -> f64 {
        let bytes = self.bytes_written.load(Ordering::Relaxed) as f64;
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            (bytes / 1_048_576.0) / elapsed
        } else {
            0.0
        }
    }

    /// Calculate compression ratio
    pub fn compression_ratio(&self) -> f64 {
        let compressed = self.bytes_written.load(Ordering::Relaxed) as f64;
        let uncompressed = self.bytes_uncompressed.load(Ordering::Relaxed) as f64;
        if compressed > 0.0 {
            uncompressed / compressed
        } else {
            1.0
        }
    }

    /// Get records per second
    pub fn records_per_second(&self) -> f64 {
        let records = self.records_processed.load(Ordering::Relaxed) as f64;
        let elapsed = self.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            records / elapsed
        } else {
            0.0
        }
    }

    /// Get checkpoint latency percentiles
    pub fn checkpoint_latency_stats(&self) -> LatencyStats {
        let latencies = self.checkpoint_latencies_ns.read();
        LatencyStats::from_samples(&latencies)
    }

    /// Get segment write latency percentiles
    pub fn segment_write_latency_stats(&self) -> LatencyStats {
        let latencies = self.segment_write_latencies_ns.read();
        LatencyStats::from_samples(&latencies)
    }

    /// Get fetch latency percentiles
    pub fn fetch_latency_stats(&self) -> LatencyStats {
        let latencies = self.fetch_latencies_ns.read();
        LatencyStats::from_samples(&latencies)
    }

    /// Generate a metrics report
    pub fn report(&self) -> MetricsReport {
        MetricsReport {
            elapsed_secs: self.elapsed().as_secs_f64(),
            records_processed: self.records_processed.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            bytes_uncompressed: self.bytes_uncompressed.load(Ordering::Relaxed),
            segments_written: self.segments_written.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            throughput_mbps: self.throughput_mbps(),
            compression_ratio: self.compression_ratio(),
            records_per_second: self.records_per_second(),
            checkpoint_latency: self.checkpoint_latency_stats(),
            segment_write_latency: self.segment_write_latency_stats(),
            fetch_latency: self.fetch_latency_stats(),
        }
    }

    /// Reset all metrics
    pub fn reset(&self) {
        self.records_processed.store(0, Ordering::Relaxed);
        self.bytes_written.store(0, Ordering::Relaxed);
        self.bytes_uncompressed.store(0, Ordering::Relaxed);
        self.segments_written.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.checkpoint_latencies_ns.write().clear();
        self.segment_write_latencies_ns.write().clear();
        self.fetch_latencies_ns.write().clear();
    }
}

/// Latency statistics
#[derive(Debug, Clone, Default, Serialize)]
pub struct LatencyStats {
    pub count: usize,
    pub avg_ms: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

impl LatencyStats {
    /// Calculate stats from samples (in nanoseconds)
    fn from_samples(samples: &[u64]) -> Self {
        if samples.is_empty() {
            return Self::default();
        }

        let mut sorted: Vec<u64> = samples.to_vec();
        sorted.sort_unstable();

        let count = sorted.len();
        let sum: u64 = sorted.iter().sum();

        let ns_to_ms = |ns: u64| ns as f64 / 1_000_000.0;

        Self {
            count,
            avg_ms: ns_to_ms(sum / count as u64),
            p50_ms: ns_to_ms(sorted[count / 2]),
            p95_ms: ns_to_ms(sorted[(count as f64 * 0.95) as usize]),
            p99_ms: ns_to_ms(sorted[(count as f64 * 0.99) as usize]),
            max_ms: ns_to_ms(*sorted.last().unwrap()),
        }
    }
}

/// Metrics report
#[derive(Debug, Clone, Serialize)]
pub struct MetricsReport {
    pub elapsed_secs: f64,
    pub records_processed: u64,
    pub bytes_written: u64,
    pub bytes_uncompressed: u64,
    pub segments_written: u64,
    pub errors: u64,
    pub throughput_mbps: f64,
    pub compression_ratio: f64,
    pub records_per_second: f64,
    pub checkpoint_latency: LatencyStats,
    pub segment_write_latency: LatencyStats,
    pub fetch_latency: LatencyStats,
}

impl std::fmt::Display for MetricsReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "=== Performance Metrics ===")?;
        writeln!(f, "Duration: {:.2}s", self.elapsed_secs)?;
        writeln!(f, "Records processed: {}", self.records_processed)?;
        writeln!(f, "Records/sec: {:.0}", self.records_per_second)?;
        writeln!(f, "Bytes written: {} MB", self.bytes_written / 1_048_576)?;
        writeln!(f, "Throughput: {:.2} MB/s", self.throughput_mbps)?;
        writeln!(f, "Compression ratio: {:.2}x", self.compression_ratio)?;
        writeln!(f, "Segments written: {}", self.segments_written)?;
        writeln!(f, "Errors: {}", self.errors)?;
        writeln!(f)?;
        writeln!(f, "Checkpoint latency (ms):")?;
        writeln!(
            f,
            "  avg={:.2} p50={:.2} p95={:.2} p99={:.2} max={:.2}",
            self.checkpoint_latency.avg_ms,
            self.checkpoint_latency.p50_ms,
            self.checkpoint_latency.p95_ms,
            self.checkpoint_latency.p99_ms,
            self.checkpoint_latency.max_ms
        )?;
        writeln!(f, "Segment write latency (ms):")?;
        writeln!(
            f,
            "  avg={:.2} p50={:.2} p95={:.2} p99={:.2} max={:.2}",
            self.segment_write_latency.avg_ms,
            self.segment_write_latency.p50_ms,
            self.segment_write_latency.p95_ms,
            self.segment_write_latency.p99_ms,
            self.segment_write_latency.max_ms
        )?;
        Ok(())
    }
}

impl MetricsReport {
    /// Export metrics in Prometheus text format
    pub fn to_prometheus(&self) -> String {
        let mut output = String::new();

        // Counters
        output.push_str("# HELP kafka_backup_records_total Total number of records processed\n");
        output.push_str("# TYPE kafka_backup_records_total counter\n");
        output.push_str(&format!(
            "kafka_backup_records_total {}\n\n",
            self.records_processed
        ));

        output
            .push_str("# HELP kafka_backup_bytes_written_total Total bytes written (compressed)\n");
        output.push_str("# TYPE kafka_backup_bytes_written_total counter\n");
        output.push_str(&format!(
            "kafka_backup_bytes_written_total {}\n\n",
            self.bytes_written
        ));

        output.push_str(
            "# HELP kafka_backup_bytes_uncompressed_total Total bytes before compression\n",
        );
        output.push_str("# TYPE kafka_backup_bytes_uncompressed_total counter\n");
        output.push_str(&format!(
            "kafka_backup_bytes_uncompressed_total {}\n\n",
            self.bytes_uncompressed
        ));

        output.push_str("# HELP kafka_backup_segments_total Total segments written\n");
        output.push_str("# TYPE kafka_backup_segments_total counter\n");
        output.push_str(&format!(
            "kafka_backup_segments_total {}\n\n",
            self.segments_written
        ));

        output.push_str("# HELP kafka_backup_errors_total Total errors encountered\n");
        output.push_str("# TYPE kafka_backup_errors_total counter\n");
        output.push_str(&format!("kafka_backup_errors_total {}\n\n", self.errors));

        // Gauges
        output.push_str("# HELP kafka_backup_throughput_mbps Current throughput in MB/s\n");
        output.push_str("# TYPE kafka_backup_throughput_mbps gauge\n");
        output.push_str(&format!(
            "kafka_backup_throughput_mbps {:.2}\n\n",
            self.throughput_mbps
        ));

        output.push_str("# HELP kafka_backup_records_per_second Records processed per second\n");
        output.push_str("# TYPE kafka_backup_records_per_second gauge\n");
        output.push_str(&format!(
            "kafka_backup_records_per_second {:.2}\n\n",
            self.records_per_second
        ));

        output.push_str("# HELP kafka_backup_compression_ratio Compression ratio\n");
        output.push_str("# TYPE kafka_backup_compression_ratio gauge\n");
        output.push_str(&format!(
            "kafka_backup_compression_ratio {:.2}\n\n",
            self.compression_ratio
        ));

        output.push_str("# HELP kafka_backup_elapsed_seconds Elapsed time since start\n");
        output.push_str("# TYPE kafka_backup_elapsed_seconds gauge\n");
        output.push_str(&format!(
            "kafka_backup_elapsed_seconds {:.2}\n\n",
            self.elapsed_secs
        ));

        // Checkpoint latency histogram summary
        output.push_str(
            "# HELP kafka_backup_checkpoint_latency_ms Checkpoint latency in milliseconds\n",
        );
        output.push_str("# TYPE kafka_backup_checkpoint_latency_ms summary\n");
        output.push_str(&format!(
            "kafka_backup_checkpoint_latency_ms{{quantile=\"0.5\"}} {:.2}\n",
            self.checkpoint_latency.p50_ms
        ));
        output.push_str(&format!(
            "kafka_backup_checkpoint_latency_ms{{quantile=\"0.95\"}} {:.2}\n",
            self.checkpoint_latency.p95_ms
        ));
        output.push_str(&format!(
            "kafka_backup_checkpoint_latency_ms{{quantile=\"0.99\"}} {:.2}\n",
            self.checkpoint_latency.p99_ms
        ));
        output.push_str(&format!(
            "kafka_backup_checkpoint_latency_ms_max {:.2}\n",
            self.checkpoint_latency.max_ms
        ));
        output.push_str(&format!(
            "kafka_backup_checkpoint_latency_ms_count {}\n\n",
            self.checkpoint_latency.count
        ));

        // Segment write latency histogram summary
        output.push_str(
            "# HELP kafka_backup_segment_write_latency_ms Segment write latency in milliseconds\n",
        );
        output.push_str("# TYPE kafka_backup_segment_write_latency_ms summary\n");
        output.push_str(&format!(
            "kafka_backup_segment_write_latency_ms{{quantile=\"0.5\"}} {:.2}\n",
            self.segment_write_latency.p50_ms
        ));
        output.push_str(&format!(
            "kafka_backup_segment_write_latency_ms{{quantile=\"0.95\"}} {:.2}\n",
            self.segment_write_latency.p95_ms
        ));
        output.push_str(&format!(
            "kafka_backup_segment_write_latency_ms{{quantile=\"0.99\"}} {:.2}\n",
            self.segment_write_latency.p99_ms
        ));
        output.push_str(&format!(
            "kafka_backup_segment_write_latency_ms_max {:.2}\n",
            self.segment_write_latency.max_ms
        ));
        output.push_str(&format!(
            "kafka_backup_segment_write_latency_ms_count {}\n\n",
            self.segment_write_latency.count
        ));

        // Fetch latency histogram summary
        output.push_str("# HELP kafka_backup_fetch_latency_ms Fetch latency in milliseconds\n");
        output.push_str("# TYPE kafka_backup_fetch_latency_ms summary\n");
        output.push_str(&format!(
            "kafka_backup_fetch_latency_ms{{quantile=\"0.5\"}} {:.2}\n",
            self.fetch_latency.p50_ms
        ));
        output.push_str(&format!(
            "kafka_backup_fetch_latency_ms{{quantile=\"0.95\"}} {:.2}\n",
            self.fetch_latency.p95_ms
        ));
        output.push_str(&format!(
            "kafka_backup_fetch_latency_ms{{quantile=\"0.99\"}} {:.2}\n",
            self.fetch_latency.p99_ms
        ));
        output.push_str(&format!(
            "kafka_backup_fetch_latency_ms_max {:.2}\n",
            self.fetch_latency.max_ms
        ));
        output.push_str(&format!(
            "kafka_backup_fetch_latency_ms_count {}\n",
            self.fetch_latency.count
        ));

        output
    }

    /// Export metrics as JSON
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|_| "{}".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_basic() {
        let metrics = PerformanceMetrics::new();

        metrics.record_records(1000);
        metrics.record_bytes(500, 2000);
        metrics.record_segment();

        assert_eq!(metrics.records_processed.load(Ordering::Relaxed), 1000);
        assert_eq!(metrics.bytes_written.load(Ordering::Relaxed), 500);
        assert_eq!(metrics.bytes_uncompressed.load(Ordering::Relaxed), 2000);
        assert_eq!(metrics.compression_ratio(), 4.0);
    }

    #[test]
    fn test_latency_stats() {
        let metrics = PerformanceMetrics::new();

        for i in 0..100 {
            metrics.record_checkpoint_latency(Duration::from_millis(i));
        }

        let stats = metrics.checkpoint_latency_stats();
        assert_eq!(stats.count, 100);
        assert!(stats.p50_ms >= 49.0 && stats.p50_ms <= 51.0);
        assert!(stats.p99_ms >= 98.0);
    }
}
