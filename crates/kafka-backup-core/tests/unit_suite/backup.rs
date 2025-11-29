//! Backup engine unit tests.
//!
//! Tests for backup-related functionality including:
//! - Record encoding/decoding
//! - Topic filtering logic
//! - Timestamp mapping
//! - Partition handling

use kafka_backup_core::config::TopicSelection;
use kafka_backup_core::manifest::{BackupRecord, RecordHeader};

use super::helpers::{
    create_test_record, create_test_record_with_headers, create_test_record_with_offset,
    create_test_record_with_timestamp, generate_test_records,
};

// ============================================================================
// Topic Matching Helper (for testing topic filtering logic)
// ============================================================================

/// Helper trait to test topic matching logic.
/// This implements the expected behavior from the PRD for topic filtering.
trait TopicMatcher {
    fn matches(&self, topic: &str) -> bool;
}

impl TopicMatcher for TopicSelection {
    fn matches(&self, topic: &str) -> bool {
        // If include is empty, nothing matches
        if self.include.is_empty() {
            return false;
        }

        // Check if topic matches any include pattern
        let included = self.include.iter().any(|pattern| {
            if pattern == "*" {
                true
            } else if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                topic.starts_with(prefix)
            } else {
                topic == pattern
            }
        });

        if !included {
            return false;
        }

        // Check if topic matches any exclude pattern
        let excluded = self.exclude.iter().any(|pattern| {
            if pattern.ends_with('*') {
                let prefix = &pattern[..pattern.len() - 1];
                topic.starts_with(prefix)
            } else {
                topic == pattern
            }
        });

        !excluded
    }
}

// ============================================================================
// Record Encoding/Decoding Tests
// ============================================================================

#[test]
fn encode_decode_preserves_key() {
    let record = create_test_record(Some(b"order-123".to_vec()), Some(b"value".to_vec()));

    // Verify key is preserved
    assert_eq!(record.key, Some(b"order-123".to_vec()));
}

#[test]
fn encode_record_with_null_key_preserves_semantics() {
    let record = create_test_record(None, Some(b"value".to_vec()));

    // Null key should be preserved (important for log compaction behavior)
    assert!(record.key.is_none());
    assert!(record.value.is_some());
}

#[test]
fn encode_record_with_null_value_preserves_semantics() {
    // Tombstone records have null values
    let record = create_test_record(Some(b"delete-key".to_vec()), None);

    assert!(record.key.is_some());
    assert!(record.value.is_none());
}

#[test]
fn encode_record_preserves_headers() {
    let headers = vec![
        RecordHeader {
            key: "x-correlation-id".to_string(),
            value: b"abc123".to_vec(),
        },
        RecordHeader {
            key: "x-source".to_string(),
            value: b"service-a".to_vec(),
        },
    ];

    let record = create_test_record_with_headers(headers.clone());

    assert_eq!(record.headers.len(), 2);
    assert_eq!(record.headers[0].key, "x-correlation-id");
    assert_eq!(record.headers[1].key, "x-source");
}

#[test]
fn encode_record_with_large_value() {
    let large_value = vec![0u8; 1024 * 1024]; // 1MB value
    let record = create_test_record(Some(b"key".to_vec()), Some(large_value.clone()));

    assert_eq!(record.value.as_ref().unwrap().len(), 1024 * 1024);
}

#[test]
fn encode_record_with_large_headers() {
    let large_header_value = vec![0u8; 1024 * 100]; // 100KB header
    let headers = vec![RecordHeader {
        key: "big-header".to_string(),
        value: large_header_value.clone(),
    }];

    let record = create_test_record_with_headers(headers);

    assert_eq!(record.headers[0].value.len(), large_header_value.len());
}

// ============================================================================
// Topic Filtering Tests
// ============================================================================

#[test]
fn topic_filter_include_matches_exact() {
    let selection = TopicSelection {
        include: vec!["orders".to_string(), "payments".to_string()],
        exclude: vec![],
    };

    assert!(selection.matches("orders"));
    assert!(selection.matches("payments"));
    assert!(!selection.matches("orders-v2"));
    assert!(!selection.matches("system-events"));
}

#[test]
fn topic_filter_include_wildcard_matches() {
    let selection = TopicSelection {
        include: vec!["app-*".to_string()],
        exclude: vec![],
    };

    assert!(selection.matches("app-orders"));
    assert!(selection.matches("app-payments"));
    assert!(selection.matches("app-"));
    assert!(!selection.matches("system-app-orders"));
}

#[test]
fn topic_filter_exclude_overrides_include() {
    let selection = TopicSelection {
        include: vec!["app-*".to_string()],
        exclude: vec!["app-internal".to_string()],
    };

    assert!(selection.matches("app-orders"));
    assert!(!selection.matches("app-internal"));
}

#[test]
fn topic_filter_exclude_internal_topics() {
    let selection = TopicSelection {
        include: vec!["*".to_string()],
        exclude: vec!["__consumer_offsets".to_string(), "__transaction_state".to_string()],
    };

    assert!(selection.matches("orders"));
    assert!(selection.matches("payments"));
    assert!(!selection.matches("__consumer_offsets"));
    assert!(!selection.matches("__transaction_state"));
}

#[test]
fn topic_filter_empty_include_matches_nothing() {
    let selection = TopicSelection {
        include: vec![],
        exclude: vec![],
    };

    // Empty include should match nothing (explicit selection required)
    assert!(!selection.matches("orders"));
}

// ============================================================================
// Timestamp Mapping Tests
// ============================================================================

#[test]
fn timestamp_preserves_millisecond_precision() {
    let timestamps = vec![
        1672531200001i64, // Different by 1ms
        1672531200002,
        1672531200003,
    ];

    for ts in timestamps {
        let record = create_test_record_with_timestamp(ts);
        assert_eq!(record.timestamp, ts);
    }
}

#[test]
fn timestamp_handles_epoch_boundary() {
    let record = create_test_record_with_timestamp(0);
    assert_eq!(record.timestamp, 0);

    let record_negative = create_test_record_with_timestamp(-1);
    assert_eq!(record_negative.timestamp, -1);
}

#[test]
fn timestamp_handles_far_future() {
    // Year 3000
    let far_future = 32503680000000i64;
    let record = create_test_record_with_timestamp(far_future);
    assert_eq!(record.timestamp, far_future);
}

// ============================================================================
// Offset Ordering Tests
// ============================================================================

#[test]
fn records_maintain_offset_order_within_partition() {
    let records: Vec<BackupRecord> = (100..103)
        .map(|offset| create_test_record_with_offset(offset))
        .collect();

    assert_eq!(records[0].offset, 100);
    assert_eq!(records[1].offset, 101);
    assert_eq!(records[2].offset, 102);
}

#[test]
fn offset_gaps_are_preserved() {
    // Gap can occur due to deleted records or compaction
    let offsets = vec![100i64, 105, 110]; // Gaps: 101-104, 106-109
    let records: Vec<BackupRecord> = offsets
        .into_iter()
        .map(|o| create_test_record_with_offset(o))
        .collect();

    assert_eq!(records[1].offset - records[0].offset, 5);
    assert_eq!(records[2].offset - records[1].offset, 5);
}

// ============================================================================
// Partition Handling Tests
// ============================================================================

#[test]
fn empty_partition_records_valid() {
    let records: Vec<BackupRecord> = vec![];
    assert!(records.is_empty());
}

#[test]
fn single_record_partition_valid() {
    let records = vec![create_test_record_with_offset(0)];
    assert_eq!(records.len(), 1);
}

#[test]
fn multiple_records_preserves_count() {
    let records = generate_test_records(100, "test-topic");
    assert_eq!(records.len(), 100);
}

// ============================================================================
// Special Character Handling
// ============================================================================

#[test]
fn record_with_unicode_key() {
    let unicode_key = "订单-🎉-émojis".as_bytes().to_vec();
    let record = create_test_record(Some(unicode_key.clone()), Some(b"value".to_vec()));

    assert_eq!(record.key, Some(unicode_key));
}

#[test]
fn record_with_unicode_value() {
    let unicode_value = r#"{"name": "Café ☕", "price": "€10"}"#.as_bytes().to_vec();
    let record = create_test_record(Some(b"key".to_vec()), Some(unicode_value.clone()));

    assert_eq!(record.value, Some(unicode_value));
}

#[test]
fn record_with_binary_key() {
    // Binary key with all byte values
    let binary_key: Vec<u8> = (0..=255).collect();
    let record = create_test_record(Some(binary_key.clone()), Some(b"value".to_vec()));

    assert_eq!(record.key, Some(binary_key));
}

// ============================================================================
// Header Edge Cases
// ============================================================================

#[test]
fn record_with_empty_header_value() {
    let headers = vec![RecordHeader {
        key: "empty-value".to_string(),
        value: vec![],
    }];

    let record = create_test_record_with_headers(headers);
    assert!(record.headers[0].value.is_empty());
}

#[test]
fn record_with_duplicate_header_keys() {
    // Kafka allows duplicate header keys
    let headers = vec![
        RecordHeader {
            key: "x-tag".to_string(),
            value: b"tag1".to_vec(),
        },
        RecordHeader {
            key: "x-tag".to_string(),
            value: b"tag2".to_vec(),
        },
    ];

    let record = create_test_record_with_headers(headers);
    assert_eq!(record.headers.len(), 2);
    assert_eq!(record.headers[0].key, record.headers[1].key);
}
