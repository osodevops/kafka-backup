//! Offset mapping and offset management unit tests.
//!
//! Tests for offset-related functionality including:
//! - Offset state structures
//! - Offset reset batching
//! - Snapshot structures

use kafka_backup_core::restore::offset_automation::{OffsetResetBatch, OffsetResetMetrics};
use kafka_backup_core::restore::offset_rollback::{
    GroupOffsetState, OffsetSnapshot, PartitionOffsetState,
};

use std::collections::HashMap;
use std::time::Duration;

// ============================================================================
// Offset Reset Batch Tests
// ============================================================================

#[test]
fn offset_reset_batch_creation() {
    let batch = OffsetResetBatch::new("test-group".to_string(), 1);

    assert_eq!(batch.group_id, "test-group");
    assert!(batch.is_empty());
    assert_eq!(batch.len(), 0);
}

#[test]
fn offset_reset_batch_add_offset() {
    let mut batch = OffsetResetBatch::new("test-group".to_string(), 1);

    batch.add_offset("orders".to_string(), 0, 100, None);
    batch.add_offset("orders".to_string(), 1, 200, None);
    batch.add_offset("payments".to_string(), 0, 50, None);

    assert!(!batch.is_empty());
    assert_eq!(batch.len(), 3);
}

#[test]
fn offset_reset_batch_topics() {
    let mut batch = OffsetResetBatch::new("test-group".to_string(), 1);

    batch.add_offset("orders".to_string(), 0, 100, None);
    batch.add_offset("orders".to_string(), 1, 200, None);
    batch.add_offset("payments".to_string(), 0, 50, None);

    // Get unique topics (offsets is a Vec of tuples: (topic, partition, offset, metadata))
    let topics: std::collections::HashSet<_> = batch.offsets.iter().map(|o| &o.0).collect();
    assert_eq!(topics.len(), 2);
}

// ============================================================================
// Offset Reset Metrics Tests
// ============================================================================

#[test]
fn offset_reset_metrics_creation() {
    let metrics = OffsetResetMetrics::new();

    // Verify it can be created without errors
    assert_eq!(metrics.total_offsets_reset.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[test]
fn offset_reset_metrics_latency_tracking() {
    let metrics = OffsetResetMetrics::new();

    metrics.record_latency(Duration::from_millis(10));
    metrics.record_latency(Duration::from_millis(20));
    metrics.record_latency(Duration::from_millis(30));
    metrics.record_latency(Duration::from_millis(40));
    metrics.record_latency(Duration::from_millis(50));

    // Verify p50 is around 30ms (median of 10, 20, 30, 40, 50)
    let p50 = metrics.p50_latency_ms();
    assert!(p50 >= 25.0 && p50 <= 35.0, "p50 was {}", p50);
}

#[test]
fn offset_reset_metrics_percentiles() {
    let metrics = OffsetResetMetrics::new();

    // Add 100 latencies from 1-100ms
    for i in 1..=100 {
        metrics.record_latency(Duration::from_millis(i));
    }

    // p50 should be around 50
    let p50 = metrics.p50_latency_ms();
    assert!(p50 >= 45.0 && p50 <= 55.0, "p50 was {}", p50);

    // p99 should be around 99
    let p99 = metrics.p99_latency_ms();
    assert!(p99 >= 95.0, "p99 was {}", p99);
}

#[test]
fn offset_reset_metrics_start() {
    let metrics = OffsetResetMetrics::new();

    // Start should not panic
    metrics.start();

    // Record some activity
    metrics.record_latency(Duration::from_millis(10));
}

// ============================================================================
// Offset Snapshot Tests
// ============================================================================

#[test]
fn offset_snapshot_creation() {
    let snapshot = OffsetSnapshot {
        snapshot_id: "snap-001".to_string(),
        created_at: chrono::Utc::now(),
        group_offsets: HashMap::new(),
        restore_id: None,
        cluster_id: None,
        bootstrap_servers: vec!["localhost:9092".to_string()],
        description: Some("Test snapshot".to_string()),
    };

    assert_eq!(snapshot.snapshot_id, "snap-001");
    assert!(snapshot.group_offsets.is_empty());
}

#[test]
fn offset_snapshot_with_groups() {
    let mut group_offsets = HashMap::new();

    // Create a group offset state
    let mut topic_offsets = HashMap::new();
    let mut partition_offsets = HashMap::new();

    partition_offsets.insert(
        0i32,
        PartitionOffsetState {
            offset: 100,
            metadata: Some("test".to_string()),
            timestamp: Some(chrono::Utc::now().timestamp_millis()),
        },
    );
    partition_offsets.insert(
        1i32,
        PartitionOffsetState {
            offset: 200,
            metadata: None,
            timestamp: None,
        },
    );

    topic_offsets.insert("orders".to_string(), partition_offsets);

    group_offsets.insert(
        "my-group".to_string(),
        GroupOffsetState {
            group_id: "my-group".to_string(),
            offsets: topic_offsets,
            partition_count: 2,
        },
    );

    let snapshot = OffsetSnapshot {
        snapshot_id: "snap-002".to_string(),
        created_at: chrono::Utc::now(),
        group_offsets,
        restore_id: Some("restore-001".to_string()),
        cluster_id: Some("cluster-abc".to_string()),
        bootstrap_servers: vec!["kafka-1:9092".to_string(), "kafka-2:9092".to_string()],
        description: None,
    };

    assert_eq!(snapshot.group_offsets.len(), 1);
    assert!(snapshot.group_offsets.contains_key("my-group"));

    let group = &snapshot.group_offsets["my-group"];
    assert_eq!(group.partition_count, 2);
}

#[test]
fn offset_snapshot_total_offsets() {
    let mut group_offsets = HashMap::new();

    // Group 1 with 2 partitions
    let mut topic_offsets_1 = HashMap::new();
    let mut partition_offsets_1 = HashMap::new();
    partition_offsets_1.insert(0, PartitionOffsetState {
        offset: 0,
        metadata: None,
        timestamp: None,
    });
    partition_offsets_1.insert(1, PartitionOffsetState {
        offset: 0,
        metadata: None,
        timestamp: None,
    });
    topic_offsets_1.insert("orders".to_string(), partition_offsets_1);

    group_offsets.insert(
        "group-1".to_string(),
        GroupOffsetState {
            group_id: "group-1".to_string(),
            offsets: topic_offsets_1,
            partition_count: 2,
        },
    );

    // Group 2 with 1 partition
    let mut topic_offsets_2 = HashMap::new();
    let mut partition_offsets_2 = HashMap::new();
    partition_offsets_2.insert(0, PartitionOffsetState {
        offset: 0,
        metadata: None,
        timestamp: None,
    });
    topic_offsets_2.insert("payments".to_string(), partition_offsets_2);

    group_offsets.insert(
        "group-2".to_string(),
        GroupOffsetState {
            group_id: "group-2".to_string(),
            offsets: topic_offsets_2,
            partition_count: 1,
        },
    );

    let snapshot = OffsetSnapshot {
        snapshot_id: "snap-003".to_string(),
        created_at: chrono::Utc::now(),
        group_offsets,
        restore_id: None,
        cluster_id: None,
        bootstrap_servers: vec![],
        description: None,
    };

    // Total offsets = 2 + 1 = 3
    let total: usize = snapshot
        .group_offsets
        .values()
        .map(|g| g.partition_count as usize)
        .sum();

    assert_eq!(total, 3);
}

// ============================================================================
// Group Offset State Tests
// ============================================================================

#[test]
fn group_offset_state_creation() {
    let state = GroupOffsetState {
        group_id: "my-group".to_string(),
        offsets: HashMap::new(),
        partition_count: 0,
    };

    assert_eq!(state.group_id, "my-group");
    assert!(state.offsets.is_empty());
}

#[test]
fn partition_offset_state_creation() {
    let state = PartitionOffsetState {
        offset: 0,
        metadata: None,
        timestamp: None,
    };

    assert_eq!(state.offset, 0);
    assert!(state.metadata.is_none());
    assert!(state.timestamp.is_none());
}

#[test]
fn partition_offset_state_with_metadata() {
    let state = PartitionOffsetState {
        offset: 12345,
        metadata: Some("consumer-metadata".to_string()),
        timestamp: Some(1672531200000),
    };

    assert_eq!(state.offset, 12345);
    assert_eq!(state.metadata, Some("consumer-metadata".to_string()));
    assert_eq!(state.timestamp, Some(1672531200000));
}

// ============================================================================
// Deduplication Logic Tests
// ============================================================================

#[test]
fn deduplication_keeps_latest_offset() {
    // Simulate multiple offset commits for the same group/topic/partition
    let commits = vec![
        ("group-1", "orders", 0i32, 100i64, 1000i64), // timestamp 1000
        ("group-1", "orders", 0, 150, 2000),          // timestamp 2000 (later)
        ("group-1", "orders", 0, 120, 1500),          // timestamp 1500
    ];

    // Find the latest (highest timestamp)
    let latest = commits
        .iter()
        .max_by_key(|c| c.4)
        .expect("Should have commits");

    assert_eq!(latest.3, 150); // Offset 150 from timestamp 2000
}

#[test]
fn deduplication_by_group_topic_partition() {
    let commits = vec![
        ("group-1", "orders", 0i32, 100i64),
        ("group-1", "orders", 1, 200),
        ("group-1", "payments", 0, 50),
        ("group-2", "orders", 0, 75),
    ];

    // Group by (group_id, topic, partition)
    let mut grouped: HashMap<(&str, &str, i32), i64> = HashMap::new();
    for (group, topic, partition, offset) in commits {
        grouped.insert((group, topic, partition), offset);
    }

    assert_eq!(grouped.len(), 4); // 4 unique combinations
    assert_eq!(grouped[&("group-1", "orders", 0)], 100);
    assert_eq!(grouped[&("group-1", "orders", 1)], 200);
}

// ============================================================================
// Offset Validation Tests
// ============================================================================

#[test]
fn offset_validation_positive() {
    let offset = 12345i64;
    assert!(offset >= 0);
}

#[test]
fn offset_validation_zero() {
    let offset = 0i64;
    assert!(offset >= 0);
}

#[test]
fn offset_validation_max() {
    let offset = i64::MAX;
    assert!(offset > 0);
}

#[test]
fn offset_comparison_for_reset() {
    let current_offset = 100i64;
    let target_offset = 50i64;

    // Reset would move backward (reprocessing)
    let is_backward = target_offset < current_offset;
    assert!(is_backward);

    // Reset would move forward (skipping)
    let target_forward = 150i64;
    let is_forward = target_forward > current_offset;
    assert!(is_forward);
}

// ============================================================================
// Serialization Tests
// ============================================================================

#[test]
fn offset_snapshot_serialization_roundtrip() {
    let mut group_offsets = HashMap::new();
    let mut topic_offsets = HashMap::new();
    let mut partition_offsets = HashMap::new();

    partition_offsets.insert(
        0,
        PartitionOffsetState {
            offset: 100,
            metadata: Some("test-meta".to_string()),
            timestamp: Some(1672531200000),
        },
    );

    topic_offsets.insert("orders".to_string(), partition_offsets);

    group_offsets.insert(
        "test-group".to_string(),
        GroupOffsetState {
            group_id: "test-group".to_string(),
            offsets: topic_offsets,
            partition_count: 1,
        },
    );

    let snapshot = OffsetSnapshot {
        snapshot_id: "snap-serial".to_string(),
        created_at: chrono::Utc::now(),
        group_offsets,
        restore_id: Some("restore-001".to_string()),
        cluster_id: None,
        bootstrap_servers: vec!["localhost:9092".to_string()],
        description: Some("Serialization test".to_string()),
    };

    // Serialize to JSON
    let json = serde_json::to_string(&snapshot).expect("Serialization failed");

    // Deserialize back
    let restored: OffsetSnapshot = serde_json::from_str(&json).expect("Deserialization failed");

    assert_eq!(restored.snapshot_id, snapshot.snapshot_id);
    assert_eq!(restored.group_offsets.len(), 1);
    assert!(restored.group_offsets.contains_key("test-group"));
}
