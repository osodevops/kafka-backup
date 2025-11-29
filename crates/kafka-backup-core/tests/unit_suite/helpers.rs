//! Test helper utilities.
//!
//! Provides common test data generation and utility functions
//! used across unit tests.

use kafka_backup_core::manifest::{BackupRecord, RecordHeader, SegmentMetadata};

/// Generate a test record with the given offset
pub fn create_test_record_with_offset(offset: i64) -> BackupRecord {
    BackupRecord {
        key: Some(format!("key-{}", offset).into_bytes()),
        value: Some(format!("value-{}", offset).into_bytes()),
        headers: vec![],
        timestamp: 1672531200000 + offset, // Base timestamp + offset for ordering
        offset,
    }
}

/// Generate a test record with a specific timestamp
pub fn create_test_record_with_timestamp(timestamp: i64) -> BackupRecord {
    BackupRecord {
        key: Some(b"test-key".to_vec()),
        value: Some(b"test-value".to_vec()),
        headers: vec![],
        timestamp,
        offset: 0,
    }
}

/// Generate a test record with custom key and value
pub fn create_test_record(key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> BackupRecord {
    BackupRecord {
        key,
        value,
        headers: vec![],
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
    }
}

/// Generate a test record with headers
pub fn create_test_record_with_headers(headers: Vec<RecordHeader>) -> BackupRecord {
    BackupRecord {
        key: Some(b"test-key".to_vec()),
        value: Some(b"test-value".to_vec()),
        headers,
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
    }
}

/// Generate multiple test records with sequential offsets
pub fn generate_test_records(count: usize, topic: &str) -> Vec<BackupRecord> {
    (0..count)
        .map(|i| BackupRecord {
            key: Some(format!("key-{}", i).into_bytes()),
            value: Some(format!("value-{} for topic {}", i, topic).into_bytes()),
            headers: vec![],
            timestamp: chrono::Utc::now().timestamp_millis() + i as i64,
            offset: i as i64,
        })
        .collect()
}

/// Generate test records with specific timestamps
pub fn generate_records_with_timestamps(timestamps: &[i64]) -> Vec<BackupRecord> {
    timestamps
        .iter()
        .enumerate()
        .map(|(i, &ts)| BackupRecord {
            key: Some(format!("key-{}", i).into_bytes()),
            value: Some(format!("value-{}", i).into_bytes()),
            headers: vec![],
            timestamp: ts,
            offset: i as i64,
        })
        .collect()
}

/// Create a mock segment metadata
pub fn create_segment_metadata(
    start_offset: i64,
    end_offset: i64,
    start_timestamp: i64,
    end_timestamp: i64,
) -> SegmentMetadata {
    SegmentMetadata {
        key: format!(
            "topics/test-topic/partition=0/segment-{:010}.zst",
            start_offset
        ),
        start_offset,
        end_offset,
        start_timestamp,
        end_timestamp,
        record_count: end_offset - start_offset + 1,
        compressed_size: 1024,
        uncompressed_size: 4096,
    }
}

/// Generate random bytes for compression tests
pub fn generate_random_bytes(size: usize) -> Vec<u8> {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut bytes = Vec::with_capacity(size);
    let mut hasher = DefaultHasher::new();

    for i in 0..size {
        i.hash(&mut hasher);
        bytes.push((hasher.finish() % 256) as u8);
    }

    bytes
}

/// Generate highly repetitive data for compression ratio tests
pub fn generate_repetitive_bytes(size: usize) -> Vec<u8> {
    let pattern = b"ABCD";
    pattern.iter().cycle().take(size).cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_record_with_offset() {
        let record = create_test_record_with_offset(100);
        assert_eq!(record.offset, 100);
        assert!(record.key.is_some());
        assert!(record.value.is_some());
    }

    #[test]
    fn test_generate_test_records() {
        let records = generate_test_records(5, "test-topic");
        assert_eq!(records.len(), 5);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.offset, i as i64);
        }
    }

    #[test]
    fn test_generate_random_bytes() {
        let bytes = generate_random_bytes(100);
        assert_eq!(bytes.len(), 100);

        // Should have reasonable entropy (not all same byte)
        let unique_bytes: std::collections::HashSet<_> = bytes.iter().collect();
        assert!(unique_bytes.len() > 1);
    }

    #[test]
    fn test_generate_repetitive_bytes() {
        let bytes = generate_repetitive_bytes(100);
        assert_eq!(bytes.len(), 100);

        // Should be highly repetitive
        let unique_bytes: std::collections::HashSet<_> = bytes.iter().collect();
        assert!(unique_bytes.len() <= 4); // Pattern is "ABCD"
    }
}
