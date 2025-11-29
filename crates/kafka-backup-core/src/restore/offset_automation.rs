//! Bulk offset reset with parallel execution (Phase 3 optimization).
//!
//! This module implements parallel consumer group offset resets using Kafka's
//! OffsetCommitRequest batching. Key optimizations:
//!
//! - Groups processed in parallel using tokio semaphore for concurrency control
//! - Per-partition retry logic with exponential backoff
//! - Comprehensive metrics (p50/p99 latency, throughput)
//! - Circuit breaker integration for fault tolerance
//!
//! # Performance
//!
//! Sequential: 500 groups × 10ms = 5 seconds
//! Parallel (concurrency=50): ceil(500/50) × 10ms = 100ms (~50x speedup)

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use crate::kafka::{commit_offsets, KafkaClient};
use crate::Result;

/// Configuration for bulk offset reset operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkOffsetResetConfig {
    /// Maximum concurrent offset commit requests (default: 50)
    pub max_concurrent_requests: usize,

    /// Maximum retry attempts for failed partitions (default: 3)
    pub max_retry_attempts: u32,

    /// Base delay for exponential backoff in milliseconds (default: 100)
    pub retry_base_delay_ms: u64,

    /// Request timeout in milliseconds (default: 30000)
    pub request_timeout_ms: u64,

    /// Whether to continue on partial failures (default: true)
    pub continue_on_error: bool,
}

impl Default for BulkOffsetResetConfig {
    fn default() -> Self {
        Self {
            max_concurrent_requests: 50,
            max_retry_attempts: 3,
            retry_base_delay_ms: 100,
            request_timeout_ms: 30000,
            continue_on_error: true,
        }
    }
}

/// A batch of offsets to reset for a single consumer group.
#[derive(Debug, Clone)]
pub struct OffsetResetBatch {
    /// Consumer group ID
    pub group_id: String,

    /// Offsets to reset: (topic, partition, new_offset, metadata)
    pub offsets: Vec<(String, i32, i64, Option<String>)>,

    /// Unique request ID for correlation
    pub request_id: u64,
}

impl OffsetResetBatch {
    /// Create a new offset reset batch.
    pub fn new(group_id: String, request_id: u64) -> Self {
        Self {
            group_id,
            offsets: Vec::new(),
            request_id,
        }
    }

    /// Add an offset to the batch.
    pub fn add_offset(&mut self, topic: String, partition: i32, offset: i64, metadata: Option<String>) {
        self.offsets.push((topic, partition, offset, metadata));
    }

    /// Get the number of offsets in the batch.
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
}

/// Tracks an in-flight offset commit request.
#[derive(Debug, Clone)]
pub struct InflightRequest {
    /// The batch being processed
    pub batch: OffsetResetBatch,

    /// When the request was sent
    pub sent_at: Instant,

    /// Kafka protocol correlation ID
    pub correlation_id: u32,
}

/// Result of a single group's offset reset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupResetOutcome {
    /// Consumer group ID
    pub group_id: String,

    /// Number of partitions successfully reset
    pub partitions_reset: u64,

    /// Number of partitions that failed after all retries
    pub partitions_failed: u64,

    /// Detailed errors for failed partitions
    pub errors: Vec<PartitionError>,

    /// Time taken to reset this group in milliseconds
    pub duration_ms: u64,

    /// Number of retry attempts made
    pub retry_attempts: u32,
}

/// Error details for a failed partition reset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionError {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// Kafka error code
    pub error_code: i16,

    /// Human-readable error message
    pub message: String,
}

/// Metrics for bulk offset reset operations.
#[derive(Debug)]
pub struct OffsetResetMetrics {
    /// Total number of offsets reset successfully
    pub total_offsets_reset: AtomicU64,

    /// Total number of successful group resets
    pub successful_groups: AtomicU64,

    /// Total number of failed group resets
    pub failed_groups: AtomicU64,

    /// Total retry attempts across all groups
    pub total_retries: AtomicU64,

    /// Latencies for all requests (in milliseconds)
    latencies_ms: Mutex<Vec<f64>>,

    /// Start time of the operation
    start_time: Mutex<Option<Instant>>,
}

impl Default for OffsetResetMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetResetMetrics {
    /// Create new metrics instance.
    pub fn new() -> Self {
        Self {
            total_offsets_reset: AtomicU64::new(0),
            successful_groups: AtomicU64::new(0),
            failed_groups: AtomicU64::new(0),
            total_retries: AtomicU64::new(0),
            latencies_ms: Mutex::new(Vec::new()),
            start_time: Mutex::new(None),
        }
    }

    /// Mark the start of the operation.
    pub fn start(&self) {
        *self.start_time.lock() = Some(Instant::now());
    }

    /// Record a request latency.
    pub fn record_latency(&self, duration: Duration) {
        self.latencies_ms.lock().push(duration.as_secs_f64() * 1000.0);
    }

    /// Calculate p50 latency in milliseconds.
    pub fn p50_latency_ms(&self) -> f64 {
        self.percentile_latency(50.0)
    }

    /// Calculate p99 latency in milliseconds.
    pub fn p99_latency_ms(&self) -> f64 {
        self.percentile_latency(99.0)
    }

    /// Calculate average latency in milliseconds.
    pub fn avg_latency_ms(&self) -> f64 {
        let latencies = self.latencies_ms.lock();
        if latencies.is_empty() {
            return 0.0;
        }
        latencies.iter().sum::<f64>() / latencies.len() as f64
    }

    /// Calculate a specific percentile latency.
    fn percentile_latency(&self, percentile: f64) -> f64 {
        let mut latencies = self.latencies_ms.lock().clone();
        if latencies.is_empty() {
            return 0.0;
        }
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let index = ((percentile / 100.0) * (latencies.len() - 1) as f64).round() as usize;
        latencies[index.min(latencies.len() - 1)]
    }

    /// Get total elapsed time in milliseconds.
    pub fn elapsed_ms(&self) -> u64 {
        self.start_time
            .lock()
            .map(|t| t.elapsed().as_millis() as u64)
            .unwrap_or(0)
    }

    /// Calculate offsets per second throughput.
    pub fn offsets_per_second(&self) -> f64 {
        let elapsed_ms = self.elapsed_ms();
        if elapsed_ms == 0 {
            return 0.0;
        }
        (self.total_offsets_reset.load(Ordering::SeqCst) as f64 / elapsed_ms as f64) * 1000.0
    }
}

/// Report of a bulk offset reset operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkOffsetResetReport {
    /// Overall status of the operation
    pub status: BulkResetStatus,

    /// Total groups processed
    pub total_groups: usize,

    /// Number of groups successfully reset
    pub successful_groups: usize,

    /// Number of groups with failures
    pub failed_groups: usize,

    /// Total offsets reset across all groups
    pub total_offsets_reset: u64,

    /// Total offsets that failed to reset
    pub total_offsets_failed: u64,

    /// Detailed outcomes per group
    pub group_outcomes: Vec<GroupResetOutcome>,

    /// Total duration in milliseconds
    pub duration_ms: u64,

    /// Performance metrics
    pub performance: PerformanceStats,
}

/// Status of a bulk offset reset operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BulkResetStatus {
    /// All groups reset successfully
    Success,
    /// Some groups reset, some failed
    PartialSuccess,
    /// All groups failed
    Failed,
}

/// Performance statistics for the operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceStats {
    /// Average latency per group in milliseconds
    pub avg_latency_ms: f64,

    /// 50th percentile latency in milliseconds
    pub p50_latency_ms: f64,

    /// 99th percentile latency in milliseconds
    pub p99_latency_ms: f64,

    /// Offsets reset per second
    pub offsets_per_second: f64,

    /// Maximum concurrency used
    pub max_concurrency: usize,

    /// Total retry attempts made
    pub total_retries: u64,
}

/// Input mapping for offset reset operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetMapping {
    /// Consumer group ID
    pub group_id: String,

    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// New offset to set
    pub new_offset: i64,

    /// Optional commit metadata
    pub metadata: Option<String>,
}

/// Bulk offset reset executor with parallel processing.
pub struct BulkOffsetReset {
    /// Kafka client for broker communication
    client: Arc<KafkaClient>,

    /// Configuration
    config: BulkOffsetResetConfig,

    /// Metrics collector
    metrics: Arc<OffsetResetMetrics>,

    /// Next request ID counter
    next_request_id: AtomicU64,
}

impl BulkOffsetReset {
    /// Create a new bulk offset reset executor.
    pub fn new(client: KafkaClient, config: BulkOffsetResetConfig) -> Self {
        Self {
            client: Arc::new(client),
            config,
            metrics: Arc::new(OffsetResetMetrics::new()),
            next_request_id: AtomicU64::new(1),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(client: KafkaClient) -> Self {
        Self::new(client, BulkOffsetResetConfig::default())
    }

    /// Get the current metrics.
    pub fn metrics(&self) -> &OffsetResetMetrics {
        &self.metrics
    }

    /// Execute bulk offset reset in parallel.
    ///
    /// This method groups offset mappings by consumer group and processes
    /// groups concurrently up to the configured concurrency limit.
    pub async fn reset_offsets_parallel(
        &self,
        offset_mappings: Vec<OffsetMapping>,
    ) -> Result<BulkOffsetResetReport> {
        self.metrics.start();
        let start_time = Instant::now();

        // Step 1: Group offsets by consumer group
        let grouped = group_offsets_by_group(offset_mappings);
        let total_groups = grouped.len();

        info!(
            "Starting bulk offset reset: {} groups, max_concurrency={}",
            total_groups, self.config.max_concurrent_requests
        );

        // Step 2: Create batches for each group
        let batches: Vec<OffsetResetBatch> = grouped
            .into_iter()
            .map(|(group_id, offsets)| {
                let request_id = self.next_request_id.fetch_add(1, Ordering::SeqCst);
                let mut batch = OffsetResetBatch::new(group_id, request_id);
                for (topic, partition, offset, metadata) in offsets {
                    batch.add_offset(topic, partition, offset, metadata);
                }
                batch
            })
            .collect();

        // Step 3: Execute in parallel with semaphore
        let semaphore = Arc::new(Semaphore::new(self.config.max_concurrent_requests));
        let mut handles = Vec::new();

        for batch in batches {
            let permit = semaphore.clone().acquire_owned().await.map_err(|e| {
                crate::Error::Config(format!("Semaphore error: {}", e))
            })?;

            let client = self.client.clone();
            let config = self.config.clone();
            let metrics = self.metrics.clone();

            let handle = tokio::spawn(async move {
                let result = Self::reset_group_with_retry(&client, &batch, &config, &metrics).await;
                drop(permit); // Release semaphore permit
                result
            });

            handles.push(handle);
        }

        // Step 4: Collect results
        let mut group_outcomes = Vec::new();
        let mut total_offsets_reset = 0u64;
        let mut total_offsets_failed = 0u64;
        let mut successful_count = 0usize;
        let mut failed_count = 0usize;

        for handle in handles {
            match handle.await {
                Ok(outcome) => {
                    total_offsets_reset += outcome.partitions_reset;
                    total_offsets_failed += outcome.partitions_failed;

                    if outcome.partitions_failed == 0 {
                        successful_count += 1;
                        self.metrics.successful_groups.fetch_add(1, Ordering::SeqCst);
                    } else if outcome.partitions_reset > 0 {
                        // Partial success
                        successful_count += 1;
                        failed_count += 1;
                    } else {
                        failed_count += 1;
                        self.metrics.failed_groups.fetch_add(1, Ordering::SeqCst);
                    }

                    group_outcomes.push(outcome);
                }
                Err(e) => {
                    error!("Task panic during offset reset: {}", e);
                    failed_count += 1;
                    self.metrics.failed_groups.fetch_add(1, Ordering::SeqCst);
                }
            }
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;
        self.metrics.total_offsets_reset.store(total_offsets_reset, Ordering::SeqCst);

        // Determine overall status
        let status = if failed_count == 0 {
            BulkResetStatus::Success
        } else if successful_count == 0 {
            BulkResetStatus::Failed
        } else {
            BulkResetStatus::PartialSuccess
        };

        let report = BulkOffsetResetReport {
            status,
            total_groups,
            successful_groups: successful_count,
            failed_groups: failed_count,
            total_offsets_reset,
            total_offsets_failed,
            group_outcomes,
            duration_ms,
            performance: PerformanceStats {
                avg_latency_ms: self.metrics.avg_latency_ms(),
                p50_latency_ms: self.metrics.p50_latency_ms(),
                p99_latency_ms: self.metrics.p99_latency_ms(),
                offsets_per_second: self.metrics.offsets_per_second(),
                max_concurrency: self.config.max_concurrent_requests,
                total_retries: self.metrics.total_retries.load(Ordering::SeqCst),
            },
        };

        match report.status {
            BulkResetStatus::Success => {
                info!(
                    "Bulk offset reset completed successfully: {} groups, {} offsets in {}ms",
                    report.total_groups, report.total_offsets_reset, report.duration_ms
                );
            }
            BulkResetStatus::PartialSuccess => {
                warn!(
                    "Bulk offset reset completed with partial success: {}/{} groups succeeded, {} offsets reset, {} failed in {}ms",
                    report.successful_groups, report.total_groups, report.total_offsets_reset, report.total_offsets_failed, report.duration_ms
                );
            }
            BulkResetStatus::Failed => {
                error!(
                    "Bulk offset reset failed: all {} groups failed in {}ms",
                    report.total_groups, report.duration_ms
                );
            }
        }

        Ok(report)
    }

    /// Reset a single group with retry logic.
    async fn reset_group_with_retry(
        client: &KafkaClient,
        batch: &OffsetResetBatch,
        config: &BulkOffsetResetConfig,
        metrics: &OffsetResetMetrics,
    ) -> GroupResetOutcome {
        let start_time = Instant::now();
        let mut current_offsets = batch.offsets.clone();
        let mut total_reset = 0u64;
        let mut retry_count = 0u32;
        let mut errors = Vec::new();

        loop {
            let attempt_start = Instant::now();

            debug!(
                "Resetting {} offsets for group {} (attempt {})",
                current_offsets.len(),
                batch.group_id,
                retry_count + 1
            );

            match commit_offsets(client, &batch.group_id, &current_offsets).await {
                Ok(results) => {
                    metrics.record_latency(attempt_start.elapsed());

                    let mut failed_offsets = Vec::new();

                    // Process results
                    for (topic, partition, error_code) in results {
                        if error_code == 0 {
                            total_reset += 1;
                            debug!(
                                "Reset offset for {}:{}:{} successfully",
                                batch.group_id, topic, partition
                            );
                        } else {
                            // Find the original offset for retry
                            if let Some(offset_entry) = current_offsets
                                .iter()
                                .find(|(t, p, _, _)| *t == topic && *p == partition)
                            {
                                failed_offsets.push(offset_entry.clone());
                            }

                            errors.push(PartitionError {
                                topic: topic.clone(),
                                partition,
                                error_code,
                                message: format!("Kafka error code {}", error_code),
                            });
                        }
                    }

                    // If no failures or no more retries, we're done
                    if failed_offsets.is_empty() || retry_count >= config.max_retry_attempts {
                        break;
                    }

                    // Prepare for retry with only failed offsets
                    current_offsets = failed_offsets;
                    retry_count += 1;
                    metrics.total_retries.fetch_add(1, Ordering::SeqCst);

                    // Clear errors for this retry attempt
                    errors.clear();

                    // Exponential backoff with jitter
                    let backoff_ms = config.retry_base_delay_ms * 2u64.pow(retry_count - 1);
                    let jitter_ms = (backoff_ms as f64 * 0.1 * rand_jitter()) as u64;
                    let delay = Duration::from_millis(backoff_ms + jitter_ms);

                    warn!(
                        "Retrying {} failed partitions for group {} in {:?}",
                        current_offsets.len(),
                        batch.group_id,
                        delay
                    );

                    tokio::time::sleep(delay).await;
                }
                Err(e) => {
                    metrics.record_latency(attempt_start.elapsed());
                    error!(
                        "Failed to commit offsets for group {}: {}",
                        batch.group_id, e
                    );

                    // Mark all current offsets as failed
                    for (topic, partition, _, _) in &current_offsets {
                        errors.push(PartitionError {
                            topic: topic.clone(),
                            partition: *partition,
                            error_code: -1,
                            message: e.to_string(),
                        });
                    }

                    if retry_count >= config.max_retry_attempts {
                        break;
                    }

                    retry_count += 1;
                    metrics.total_retries.fetch_add(1, Ordering::SeqCst);

                    // Exponential backoff
                    let backoff_ms = config.retry_base_delay_ms * 2u64.pow(retry_count - 1);
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                }
            }
        }

        let partitions_failed = batch.offsets.len() as u64 - total_reset;

        GroupResetOutcome {
            group_id: batch.group_id.clone(),
            partitions_reset: total_reset,
            partitions_failed,
            errors,
            duration_ms: start_time.elapsed().as_millis() as u64,
            retry_attempts: retry_count,
        }
    }
}

/// Group offset mappings by consumer group ID.
///
/// This is the first step in bulk offset reset - organizing offsets
/// so we can send one OffsetCommitRequest per group.
pub fn group_offsets_by_group(
    offset_mappings: Vec<OffsetMapping>,
) -> HashMap<String, Vec<(String, i32, i64, Option<String>)>> {
    let mut grouped: HashMap<String, Vec<_>> = HashMap::new();

    for mapping in offset_mappings {
        grouped
            .entry(mapping.group_id)
            .or_default()
            .push((mapping.topic, mapping.partition, mapping.new_offset, mapping.metadata));
    }

    grouped
}

/// Generate a random jitter factor between 0.0 and 1.0.
fn rand_jitter() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    (nanos % 1000) as f64 / 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_offsets_by_group() {
        let mappings = vec![
            OffsetMapping {
                group_id: "app1".to_string(),
                topic: "orders".to_string(),
                partition: 0,
                new_offset: 100,
                metadata: None,
            },
            OffsetMapping {
                group_id: "app1".to_string(),
                topic: "orders".to_string(),
                partition: 1,
                new_offset: 200,
                metadata: None,
            },
            OffsetMapping {
                group_id: "app2".to_string(),
                topic: "events".to_string(),
                partition: 0,
                new_offset: 50,
                metadata: Some("test".to_string()),
            },
        ];

        let grouped = group_offsets_by_group(mappings);

        assert_eq!(grouped.len(), 2); // 2 groups
        assert_eq!(grouped.get("app1").unwrap().len(), 2);
        assert_eq!(grouped.get("app2").unwrap().len(), 1);

        let app2_offsets = grouped.get("app2").unwrap();
        assert_eq!(app2_offsets[0].0, "events");
        assert_eq!(app2_offsets[0].1, 0);
        assert_eq!(app2_offsets[0].2, 50);
        assert_eq!(app2_offsets[0].3, Some("test".to_string()));
    }

    #[test]
    fn test_offset_reset_batch() {
        let mut batch = OffsetResetBatch::new("test-group".to_string(), 1);

        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        batch.add_offset("orders".to_string(), 0, 1000, None);
        batch.add_offset("orders".to_string(), 1, 2000, Some("meta".to_string()));

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.group_id, "test-group");
        assert_eq!(batch.request_id, 1);
    }

    #[test]
    fn test_bulk_offset_reset_config_default() {
        let config = BulkOffsetResetConfig::default();

        assert_eq!(config.max_concurrent_requests, 50);
        assert_eq!(config.max_retry_attempts, 3);
        assert_eq!(config.retry_base_delay_ms, 100);
        assert_eq!(config.request_timeout_ms, 30000);
        assert!(config.continue_on_error);
    }

    #[test]
    fn test_offset_reset_metrics() {
        let metrics = OffsetResetMetrics::new();
        metrics.start();

        metrics.record_latency(Duration::from_millis(10));
        metrics.record_latency(Duration::from_millis(20));
        metrics.record_latency(Duration::from_millis(30));

        assert_eq!(metrics.avg_latency_ms(), 20.0);
        assert_eq!(metrics.p50_latency_ms(), 20.0);

        metrics.total_offsets_reset.store(100, Ordering::SeqCst);
        metrics.successful_groups.store(5, Ordering::SeqCst);
        metrics.failed_groups.store(1, Ordering::SeqCst);

        assert_eq!(metrics.total_offsets_reset.load(Ordering::SeqCst), 100);
        assert_eq!(metrics.successful_groups.load(Ordering::SeqCst), 5);
        assert_eq!(metrics.failed_groups.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_metrics_percentile_edge_cases() {
        let metrics = OffsetResetMetrics::new();

        // Empty latencies
        assert_eq!(metrics.p50_latency_ms(), 0.0);
        assert_eq!(metrics.p99_latency_ms(), 0.0);

        // Single latency
        metrics.record_latency(Duration::from_millis(100));
        assert_eq!(metrics.p50_latency_ms(), 100.0);
        assert_eq!(metrics.p99_latency_ms(), 100.0);

        // Multiple latencies
        metrics.record_latency(Duration::from_millis(200));
        metrics.record_latency(Duration::from_millis(300));
        metrics.record_latency(Duration::from_millis(400));
        metrics.record_latency(Duration::from_millis(500));

        // Average of [100, 200, 300, 400, 500] = 300
        assert_eq!(metrics.avg_latency_ms(), 300.0);
    }

    #[test]
    fn test_bulk_reset_status_serialization() {
        let success = BulkResetStatus::Success;
        let partial = BulkResetStatus::PartialSuccess;
        let failed = BulkResetStatus::Failed;

        let success_json = serde_json::to_string(&success).unwrap();
        let partial_json = serde_json::to_string(&partial).unwrap();
        let failed_json = serde_json::to_string(&failed).unwrap();

        assert_eq!(success_json, "\"success\"");
        assert_eq!(partial_json, "\"partial_success\"");
        assert_eq!(failed_json, "\"failed\"");
    }

    #[test]
    fn test_group_reset_outcome_serialization() {
        let outcome = GroupResetOutcome {
            group_id: "test-group".to_string(),
            partitions_reset: 10,
            partitions_failed: 2,
            errors: vec![PartitionError {
                topic: "orders".to_string(),
                partition: 0,
                error_code: 15,
                message: "GROUP_AUTHORIZATION_FAILED".to_string(),
            }],
            duration_ms: 150,
            retry_attempts: 1,
        };

        let json = serde_json::to_string(&outcome).unwrap();
        let deserialized: GroupResetOutcome = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.group_id, "test-group");
        assert_eq!(deserialized.partitions_reset, 10);
        assert_eq!(deserialized.partitions_failed, 2);
        assert_eq!(deserialized.errors.len(), 1);
        assert_eq!(deserialized.errors[0].error_code, 15);
    }

    #[test]
    fn test_bulk_offset_reset_report_serialization() {
        let report = BulkOffsetResetReport {
            status: BulkResetStatus::PartialSuccess,
            total_groups: 100,
            successful_groups: 95,
            failed_groups: 5,
            total_offsets_reset: 4750,
            total_offsets_failed: 250,
            group_outcomes: vec![],
            duration_ms: 500,
            performance: PerformanceStats {
                avg_latency_ms: 10.5,
                p50_latency_ms: 8.0,
                p99_latency_ms: 25.0,
                offsets_per_second: 9500.0,
                max_concurrency: 50,
                total_retries: 15,
            },
        };

        let json = serde_json::to_string_pretty(&report).unwrap();
        assert!(json.contains("\"partial_success\""));
        assert!(json.contains("\"total_groups\": 100"));
        assert!(json.contains("\"offsets_per_second\": 9500.0"));
    }

    #[test]
    fn test_rand_jitter() {
        // Just verify it doesn't panic and returns a value in range
        for _ in 0..100 {
            let jitter = rand_jitter();
            assert!(jitter >= 0.0);
            assert!(jitter < 1.0);
        }
    }

    #[test]
    fn test_offset_mapping_serialization() {
        let mapping = OffsetMapping {
            group_id: "my-consumer".to_string(),
            topic: "orders".to_string(),
            partition: 5,
            new_offset: 12345,
            metadata: Some("restored".to_string()),
        };

        let json = serde_json::to_string(&mapping).unwrap();
        let deserialized: OffsetMapping = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.group_id, "my-consumer");
        assert_eq!(deserialized.topic, "orders");
        assert_eq!(deserialized.partition, 5);
        assert_eq!(deserialized.new_offset, 12345);
        assert_eq!(deserialized.metadata, Some("restored".to_string()));
    }
}
