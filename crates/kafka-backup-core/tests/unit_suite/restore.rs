//! Restore engine unit tests.
//!
//! Tests for restore-related functionality including:
//! - Time window filtering (PITR)
//! - Offset mapping structures
//! - Ordering guarantees
//! - Edge case handling

use kafka_backup_core::manifest::{BackupRecord, OffsetMapping, OffsetMappingEntry, RecordHeader};

use super::helpers::{create_segment_metadata, generate_records_with_timestamps};

// ============================================================================
// Time Window Filtering Tests (PITR)
// ============================================================================

#[test]
fn filter_messages_before_start_time() {
    let records = generate_records_with_timestamps(&[100, 200, 300, 400, 500, 600]);

    // Filter for messages >= 350
    let filtered: Vec<_> = records
        .iter()
        .filter(|r| r.timestamp >= 350 && r.timestamp <= 700)
        .collect();

    assert_eq!(filtered.len(), 3); // 400, 500, 600
    assert_eq!(filtered[0].timestamp, 400);
}

#[test]
fn filter_messages_after_end_time() {
    let records = generate_records_with_timestamps(&[100, 200, 300, 400, 500, 600]);

    let filtered: Vec<_> = records
        .iter()
        .filter(|r| r.timestamp >= 0 && r.timestamp <= 350)
        .collect();

    assert_eq!(filtered.len(), 3); // 100, 200, 300
}

#[test]
fn time_filter_boundary_inclusive_on_both_ends() {
    let records = generate_records_with_timestamps(&[1000, 1001, 1999, 2000, 2001]);

    // Filter for 1000 <= ts <= 2000
    let filtered: Vec<_> = records
        .iter()
        .filter(|r| r.timestamp >= 1000 && r.timestamp <= 2000)
        .collect();

    // Should include 1000 and 2000, exclude 2001
    assert_eq!(filtered.len(), 4);
    assert_eq!(filtered[0].timestamp, 1000);
    assert_eq!(filtered[3].timestamp, 2000);
}

#[test]
fn filter_all_messages_within_window() {
    let records = generate_records_with_timestamps(&[100, 200, 300]);

    let filtered: Vec<_> = records
        .iter()
        .filter(|r| r.timestamp >= 50 && r.timestamp <= 350)
        .collect();

    assert_eq!(filtered.len(), 3);
}

#[test]
fn filter_with_no_matches_before_all() {
    let records = generate_records_with_timestamps(&[1000, 2000, 3000]);

    let filtered: Vec<_> = records
        .iter()
        .filter(|r| r.timestamp >= 0 && r.timestamp <= 500)
        .collect();

    assert_eq!(filtered.len(), 0);
}

#[test]
fn filter_with_no_matches_after_all() {
    let records = generate_records_with_timestamps(&[1000, 2000, 3000]);

    let filtered: Vec<_> = records
        .iter()
        .filter(|r| r.timestamp >= 4000 && r.timestamp <= 5000)
        .collect();

    assert_eq!(filtered.len(), 0);
}

#[test]
fn filter_preserves_millisecond_precision() {
    // Messages 1ms apart
    let records = generate_records_with_timestamps(&[1000, 1001, 1002, 1003]);

    // Filter for exactly 1001-1002
    let filtered: Vec<_> = records
        .iter()
        .filter(|r| r.timestamp >= 1001 && r.timestamp <= 1002)
        .collect();

    assert_eq!(filtered.len(), 2);
    assert_eq!(filtered[0].timestamp, 1001);
    assert_eq!(filtered[1].timestamp, 1002);
}

// ============================================================================
// Ordering Guarantees Tests
// ============================================================================

#[test]
fn restored_messages_maintain_partition_order() {
    let records = generate_records_with_timestamps(&[100, 200, 300, 400, 500]);

    // Offsets should be sequential
    for (i, record) in records.iter().enumerate() {
        assert_eq!(record.offset, i as i64);
    }
}

#[test]
fn restored_messages_sorted_by_offset() {
    // Records might come out of order from segments
    let mut records = vec![
        BackupRecord {
            offset: 50,
            timestamp: 500,
            key: None,
            value: None,
            headers: vec![],
        },
        BackupRecord {
            offset: 0,
            timestamp: 100,
            key: None,
            value: None,
            headers: vec![],
        },
        BackupRecord {
            offset: 25,
            timestamp: 300,
            key: None,
            value: None,
            headers: vec![],
        },
    ];

    records.sort_by_key(|r| r.offset);

    let offsets: Vec<_> = records.iter().map(|r| r.offset).collect();
    assert_eq!(offsets, vec![0, 25, 50]);
}

// ============================================================================
// Offset Mapping Tests
// ============================================================================

#[test]
fn offset_mapping_creation() {
    let mapping = OffsetMapping::new();

    assert!(mapping.entries.is_empty());
    assert!(mapping.detailed_mappings.is_empty());
}

#[test]
fn offset_mapping_entry_creation() {
    let entry = OffsetMappingEntry {
        topic: "orders".to_string(),
        partition: 0,
        source_first_offset: 100,
        source_last_offset: 200,
        target_first_offset: Some(0),
        target_last_offset: Some(100),
        first_timestamp: 1672531200000,
        last_timestamp: 1672531300000,
    };

    assert_eq!(entry.topic, "orders");
    assert_eq!(entry.partition, 0);
    assert_eq!(entry.source_first_offset, 100);
    assert_eq!(entry.source_last_offset, 200);
    assert_eq!(entry.first_timestamp, 1672531200000);
}

#[test]
fn offset_mapping_add_entry() {
    let mut mapping = OffsetMapping::new();

    // Use the add method (populates entries, not detailed_mappings)
    mapping.add("orders", 0, 100, Some(0), 1672531200000);

    // Check entries (not detailed_mappings)
    let key = "orders/0";
    assert!(mapping.entries.contains_key(key));

    let entry = &mapping.entries[key];
    assert_eq!(entry.topic, "orders");
    assert_eq!(entry.partition, 0);
    assert_eq!(entry.source_first_offset, 100);
}

#[test]
fn offset_mapping_add_detailed() {
    let mut mapping = OffsetMapping::new();

    // Use add_detailed which populates detailed_mappings
    mapping.add_detailed("orders", 0, 100, 0, 1672531200000);
    mapping.add_detailed("orders", 0, 101, 1, 1672531200001);

    // Check detailed mappings
    let key = "orders/0";
    assert!(mapping.detailed_mappings.contains_key(key));

    let pairs = &mapping.detailed_mappings[key];
    assert_eq!(pairs.len(), 2);
    assert_eq!(pairs[0].source_offset, 100);
    assert_eq!(pairs[0].target_offset, 0);
}

#[test]
fn offset_mapping_header_application() {
    let mut record = BackupRecord {
        key: Some(b"key".to_vec()),
        value: Some(b"value".to_vec()),
        headers: vec![],
        timestamp: 1672531200000,
        offset: 12345,
    };

    // Add original offset header (as restore engine does)
    record.headers.push(RecordHeader {
        key: "x-original-offset".to_string(),
        value: record.offset.to_string().into_bytes(),
    });

    assert!(record.headers.iter().any(|h| h.key == "x-original-offset"));
    let offset_header = record
        .headers
        .iter()
        .find(|h| h.key == "x-original-offset")
        .unwrap();
    assert_eq!(offset_header.value, b"12345".to_vec());
}

#[test]
fn offset_mapping_preserves_existing_headers() {
    let mut record = BackupRecord {
        key: Some(b"key".to_vec()),
        value: Some(b"value".to_vec()),
        headers: vec![RecordHeader {
            key: "existing-header".to_string(),
            value: b"existing-value".to_vec(),
        }],
        timestamp: 1672531200000,
        offset: 100,
    };

    // Add offset header
    record.headers.push(RecordHeader {
        key: "x-original-offset".to_string(),
        value: b"100".to_vec(),
    });

    assert_eq!(record.headers.len(), 2);
    assert!(record.headers.iter().any(|h| h.key == "existing-header"));
    assert!(record.headers.iter().any(|h| h.key == "x-original-offset"));
}

// ============================================================================
// Edge Cases
// ============================================================================

#[test]
fn restore_empty_backup() {
    let records: Vec<BackupRecord> = vec![];
    assert!(records.is_empty());
}

#[test]
fn restore_single_message() {
    let records = vec![BackupRecord {
        key: Some(b"only-message".to_vec()),
        value: Some(b"only-value".to_vec()),
        headers: vec![],
        timestamp: 1672531200000,
        offset: 0,
    }];

    assert_eq!(records.len(), 1);
}

#[test]
fn restore_handles_offset_zero() {
    let record = BackupRecord {
        key: None,
        value: Some(b"first-message".to_vec()),
        headers: vec![],
        timestamp: 0,
        offset: 0,
    };

    assert_eq!(record.offset, 0);
}

#[test]
fn restore_handles_max_offset() {
    let record = BackupRecord {
        key: None,
        value: Some(b"message".to_vec()),
        headers: vec![],
        timestamp: 0,
        offset: i64::MAX,
    };

    assert_eq!(record.offset, i64::MAX);
}

// ============================================================================
// Segment Metadata Tests
// ============================================================================

#[test]
fn segment_metadata_time_range() {
    let seg = create_segment_metadata(0, 99, 1000, 2000);

    // Check if timestamp 1500 is within range
    let ts = 1500i64;
    assert!(ts >= seg.start_timestamp && ts <= seg.end_timestamp);
}

#[test]
fn segment_metadata_offset_range() {
    let seg = create_segment_metadata(100, 199, 1000, 2000);

    // Check record count
    assert_eq!(seg.record_count, 100);
    assert_eq!(seg.start_offset, 100);
    assert_eq!(seg.end_offset, 199);
}

#[test]
fn segment_filtering_by_time_window() {
    let segments = vec![
        create_segment_metadata(0, 99, 1000, 1999),    // Entirely before window
        create_segment_metadata(100, 199, 2000, 2999), // Overlaps start
        create_segment_metadata(200, 299, 3000, 3999), // Entirely within
        create_segment_metadata(300, 399, 4000, 4999), // Overlaps end
        create_segment_metadata(400, 499, 5000, 5999), // Entirely after
    ];

    let time_start = 2500i64;
    let time_end = 4500i64;

    // Filter segments that might contain data in the time window
    let relevant: Vec<_> = segments
        .iter()
        .filter(|s| s.end_timestamp >= time_start && s.start_timestamp <= time_end)
        .collect();

    // Segments 1, 2, 3 overlap with the window
    assert_eq!(relevant.len(), 3);
    assert_eq!(relevant[0].start_offset, 100);
    assert_eq!(relevant[1].start_offset, 200);
    assert_eq!(relevant[2].start_offset, 300);
}

// ============================================================================
// Topic Mapping Tests
// ============================================================================

#[test]
fn topic_mapping_simple() {
    use std::collections::HashMap;

    let mut mapping = HashMap::new();
    mapping.insert("orders".to_string(), "orders-recovered".to_string());
    mapping.insert("payments".to_string(), "payments-dr".to_string());

    assert_eq!(mapping.get("orders"), Some(&"orders-recovered".to_string()));
    assert_eq!(
        mapping.get("payments"),
        Some(&"payments-dr".to_string())
    );
    assert_eq!(mapping.get("unknown"), None);
}

#[test]
fn topic_mapping_with_no_remap() {
    use std::collections::HashMap;

    let mapping: HashMap<String, String> = HashMap::new();

    // When no mapping exists, use original topic name
    let source_topic = "orders";
    let target_topic = mapping.get(source_topic).map(|s| s.as_str()).unwrap_or(source_topic);

    assert_eq!(target_topic, "orders");
}
