//! Shared helpers for restore operations.
//!
//! These functions are used by both the standard restore engine and the
//! repartitioning fan-out module.

use tokio::sync::Mutex;

use crate::compression::{decompress, detect_from_extension};
use crate::config::{OffsetStrategy, RestoreOptions};
use crate::manifest::{BackupRecord, RecordHeader, RestoreCheckpoint, SegmentMetadata};
use crate::segment::format::MAGIC_BYTES;
use crate::segment::SegmentReader;
use crate::storage::StorageBackend;
use crate::Result;

/// Read a segment from storage, handling both binary and legacy JSON formats.
pub async fn read_segment(
    storage: &dyn StorageBackend,
    segment: &SegmentMetadata,
) -> Result<Vec<BackupRecord>> {
    let data = storage.get(&segment.key).await?;

    if data.len() >= 4 && data[0..4] == MAGIC_BYTES {
        // Binary format
        let mut reader = SegmentReader::open(data)?;
        let binary_records = reader.read_all()?;
        Ok(binary_records
            .into_iter()
            .map(|br| BackupRecord {
                key: br.key.map(|b| b.to_vec()),
                value: br.value.map(|b| b.to_vec()),
                headers: br
                    .headers
                    .into_iter()
                    .map(|(k, v)| RecordHeader {
                        key: k,
                        value: v.map(|b| b.to_vec()).unwrap_or_default(),
                    })
                    .collect(),
                timestamp: br.timestamp,
                offset: br.offset,
            })
            .collect())
    } else {
        // Legacy JSON format — detect compression from extension
        let extension = segment.key.rsplit('.').next().unwrap_or("");
        let algo = detect_from_extension(extension);
        let decompressed = decompress(&data, algo)?;
        let records: Vec<BackupRecord> = serde_json::from_slice(&decompressed)?;
        Ok(records)
    }
}

/// Filter records by the time window configured in restore options.
pub fn filter_records_by_time(
    records: Vec<BackupRecord>,
    options: &RestoreOptions,
) -> Vec<BackupRecord> {
    records
        .into_iter()
        .filter(|r| {
            let after_start = options
                .time_window_start
                .map(|s| r.timestamp >= s)
                .unwrap_or(true);
            let before_end = options
                .time_window_end
                .map(|e| r.timestamp <= e)
                .unwrap_or(true);
            after_start && before_end
        })
        .collect()
}

/// Inject original-offset tracking headers into records.
///
/// Adds `x-original-offset`, `x-original-timestamp`, and `x-source-partition`
/// headers as binary little-endian i64/i32 values.
pub fn inject_offset_headers(
    records: Vec<BackupRecord>,
    source_partition: i32,
    options: &RestoreOptions,
) -> Vec<BackupRecord> {
    if options.include_original_offset_header
        || options.consumer_group_strategy == OffsetStrategy::HeaderBased
    {
        records
            .into_iter()
            .map(|mut r| {
                r.headers.push(RecordHeader {
                    key: "x-original-offset".to_string(),
                    value: r.offset.to_le_bytes().to_vec(),
                });
                r.headers.push(RecordHeader {
                    key: "x-original-timestamp".to_string(),
                    value: r.timestamp.to_le_bytes().to_vec(),
                });
                r.headers.push(RecordHeader {
                    key: "x-source-partition".to_string(),
                    value: source_partition.to_le_bytes().to_vec(),
                });
                r
            })
            .collect()
    } else {
        records
    }
}

/// Mark a segment as completed in the restore checkpoint.
pub async fn mark_segment_completed(checkpoint: &Mutex<Option<RestoreCheckpoint>>, key: &str) {
    let mut cp = checkpoint.lock().await;
    if let Some(c) = cp.as_mut() {
        c.mark_segment_completed(key);
    }
}
