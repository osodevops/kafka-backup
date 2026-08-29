//! Shared helpers for restore operations.
//!
//! These functions are used by both the standard restore engine and the
//! repartitioning fan-out module.

use tokio::sync::Mutex;

use crate::compression::{decompress, detect_from_extension};
use crate::config::{OffsetStrategy, RestoreOptions};
use crate::manifest::{BackupRecord, RecordHeader, RestoreCheckpoint, SegmentMetadata};
use crate::offset_headers;
use crate::segment::format::{BinaryRecord, MAGIC_BYTES};
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
            .map(binary_to_backup_record)
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

/// Convert a binary segment record into the in-memory [`BackupRecord`].
///
/// Null header values (`-1` length in the segment) stay `None`: they are a
/// different value from an empty header and must be restored as such
/// (issue #155).
pub(crate) fn binary_to_backup_record(br: BinaryRecord) -> BackupRecord {
    BackupRecord {
        key: br.key.map(|b| b.to_vec()),
        value: br.value.map(|b| b.to_vec()),
        headers: br
            .headers
            .into_iter()
            .map(|(key, value)| RecordHeader {
                key,
                value: value.map(|b| b.to_vec()),
            })
            .collect(),
        timestamp: br.timestamp,
        offset: br.offset,
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

/// Remove the headers kafka-backup itself adds to records
/// (`x-original-offset`, `x-original-timestamp`, `x-source-cluster`,
/// `x-source-partition`) when `restore.strip_offset_headers` is set, so a
/// restored record carries exactly the headers its source record had
/// (issue #154). Every other header — including the same names with a
/// different case — is kept, in order. A no-op when the option is off.
///
/// Called before [`inject_offset_headers`], so a restore that both strips
/// and injects ends up with exactly one fresh set of restore-side headers.
pub(crate) fn strip_offset_headers(
    records: Vec<BackupRecord>,
    options: &RestoreOptions,
) -> Vec<BackupRecord> {
    if !options.strip_offset_headers {
        return records;
    }
    records
        .into_iter()
        .map(|mut r| {
            r.headers
                .retain(|h| !offset_headers::is_offset_header(&h.key));
            r
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
                    key: offset_headers::X_ORIGINAL_OFFSET.to_string(),
                    value: Some(r.offset.to_le_bytes().to_vec()),
                });
                r.headers.push(RecordHeader {
                    key: offset_headers::X_ORIGINAL_TIMESTAMP.to_string(),
                    value: Some(r.timestamp.to_le_bytes().to_vec()),
                });
                r.headers.push(RecordHeader {
                    key: offset_headers::X_SOURCE_PARTITION.to_string(),
                    value: Some(source_partition.to_le_bytes().to_vec()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::PerformanceMetrics;
    use crate::segment::{SegmentWriter, SegmentWriterConfig};
    use crate::storage::MemoryBackend;
    use bytes::Bytes;
    use std::sync::Arc;

    fn header(key: &str, value: Option<&[u8]>) -> RecordHeader {
        RecordHeader {
            key: key.to_string(),
            value: value.map(|v| v.to_vec()),
        }
    }

    #[test]
    fn binary_to_backup_record_preserves_null_and_empty_header_values() {
        let br = BinaryRecord {
            timestamp: 1,
            offset: 9,
            key: None,
            value: Some(Bytes::from_static(b"v")),
            headers: vec![
                ("trace-id".to_string(), None),
                ("empty".to_string(), Some(Bytes::new())),
                ("tenant".to_string(), Some(Bytes::from_static(b"42"))),
            ],
        };

        let record = binary_to_backup_record(br);

        assert_eq!(
            record.headers,
            vec![
                header("trace-id", None),
                header("empty", Some(b"")),
                header("tenant", Some(b"42")),
            ]
        );
        assert_eq!(record.key, None);
        assert_eq!(record.value.as_deref(), Some(&b"v"[..]));
        assert_eq!((record.timestamp, record.offset), (1, 9));
    }

    /// Issue #155 end-to-end through the segment layer: a record with a null
    /// header value written by the segment writer must come back from
    /// `read_segment` with that header still null (and an empty one still
    /// empty) — for every compression codec.
    #[tokio::test]
    async fn read_segment_roundtrips_null_and_empty_header_values() {
        for compression in [
            crate::config::CompressionType::None,
            crate::config::CompressionType::Zstd,
            crate::config::CompressionType::Lz4,
        ] {
            let storage = Arc::new(MemoryBackend::new());
            let mut writer = SegmentWriter::new(
                SegmentWriterConfig {
                    compression,
                    ..SegmentWriterConfig::default()
                },
                storage.clone(),
                Arc::new(PerformanceMetrics::new()),
            );
            writer
                .add_record(BinaryRecord {
                    timestamp: 1_700_000_000_000,
                    offset: 7,
                    key: Some(Bytes::from_static(b"k")),
                    value: None,
                    headers: vec![
                        ("trace-id".to_string(), None),
                        ("empty".to_string(), Some(Bytes::new())),
                        ("tenant".to_string(), Some(Bytes::from_static(b"42"))),
                    ],
                })
                .unwrap();
            let segment = writer
                .flush("topics/t/partition=0/segment-0.bin")
                .await
                .unwrap()
                .expect("one record was buffered");

            let records = read_segment(storage.as_ref(), &segment).await.unwrap();

            assert_eq!(records.len(), 1, "{compression:?}");
            assert_eq!(
                records[0].headers,
                vec![
                    header("trace-id", None),
                    header("empty", Some(b"")),
                    header("tenant", Some(b"42")),
                ],
                "{compression:?}"
            );
            assert_eq!(records[0].key.as_deref(), Some(&b"k"[..]));
            assert_eq!(records[0].value, None);
            assert_eq!(records[0].offset, 7);
        }
    }

    #[test]
    fn inject_offset_headers_keeps_existing_null_header_and_appends_three() {
        let options = RestoreOptions {
            include_original_offset_header: true,
            ..RestoreOptions::default()
        };
        let record = BackupRecord {
            key: None,
            value: None,
            headers: vec![header("trace-id", None)],
            timestamp: 1_700_000_000_000,
            offset: 5,
        };

        let out = inject_offset_headers(vec![record], 3, &options);

        let headers = &out[0].headers;
        let keys: Vec<&str> = headers.iter().map(|h| h.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "trace-id",
                "x-original-offset",
                "x-original-timestamp",
                "x-source-partition"
            ]
        );
        assert_eq!(headers[0].value, None);
        assert_eq!(headers[1].value.as_deref(), Some(&5i64.to_le_bytes()[..]));
        assert_eq!(
            headers[2].value.as_deref(),
            Some(&1_700_000_000_000i64.to_le_bytes()[..])
        );
        assert_eq!(headers[3].value.as_deref(), Some(&3i32.to_le_bytes()[..]));
    }

    /// A record as it comes out of an archive taken with the default
    /// `include_offset_headers: true`: user headers first, then the two
    /// backup-side headers.
    fn archived_record() -> BackupRecord {
        BackupRecord {
            key: Some(b"k".to_vec()),
            value: Some(b"v".to_vec()),
            headers: vec![
                header("event-type", Some(b"created")),
                header("trace-id", None),
                header("X-Original-Offset", Some(b"user header, different case")),
                header("x-original-offset", Some(&9i64.to_le_bytes())),
                header(
                    "x-original-timestamp",
                    Some(&1_700_000_000_000i64.to_le_bytes()),
                ),
                header("x-source-cluster", Some(b"eu-prod")),
            ],
            timestamp: 1_700_000_000_000,
            offset: 9,
        }
    }

    /// Issue #154: `strip_offset_headers` drops exactly the headers
    /// kafka-backup adds and nothing else, preserving order.
    #[test]
    fn strip_offset_headers_removes_only_kafka_backup_headers() {
        let options = RestoreOptions {
            strip_offset_headers: true,
            ..RestoreOptions::default()
        };

        let out = strip_offset_headers(vec![archived_record()], &options);

        assert_eq!(
            out[0].headers,
            vec![
                header("event-type", Some(b"created")),
                header("trace-id", None),
                header("X-Original-Offset", Some(b"user header, different case")),
            ]
        );
        // Everything else about the record is untouched.
        assert_eq!(out[0].offset, 9);
        assert_eq!(out[0].key.as_deref(), Some(&b"k"[..]));
    }

    #[test]
    fn strip_offset_headers_also_drops_restore_side_headers_from_chained_restores() {
        let options = RestoreOptions {
            strip_offset_headers: true,
            ..RestoreOptions::default()
        };
        let mut record = archived_record();
        record
            .headers
            .push(header("x-source-partition", Some(&3i32.to_le_bytes())));

        let out = strip_offset_headers(vec![record], &options);

        assert!(out[0]
            .headers
            .iter()
            .all(|h| !offset_headers::is_offset_header(&h.key)));
        assert_eq!(out[0].headers.len(), 3);
    }

    #[test]
    fn strip_offset_headers_is_a_no_op_by_default() {
        let before = archived_record();
        let out = strip_offset_headers(vec![before.clone()], &RestoreOptions::default());
        assert_eq!(out[0].headers, before.headers);
    }

    #[test]
    fn strip_offset_headers_leaves_records_without_such_headers_alone() {
        let options = RestoreOptions {
            strip_offset_headers: true,
            ..RestoreOptions::default()
        };
        let record = BackupRecord {
            key: None,
            value: None,
            headers: vec![header("trace-id", None), header("tenant", Some(b"42"))],
            timestamp: 0,
            offset: 0,
        };
        let out = strip_offset_headers(vec![record.clone()], &options);
        assert_eq!(out[0].headers, record.headers);
    }

    /// Strip + inject (header-based strategy) yields exactly one fresh set of
    /// restore-side headers rather than archived ones plus injected ones.
    #[test]
    fn strip_then_inject_yields_a_single_fresh_set_of_headers() {
        let options = RestoreOptions {
            strip_offset_headers: true,
            consumer_group_strategy: OffsetStrategy::HeaderBased,
            ..RestoreOptions::default()
        };

        let stripped = strip_offset_headers(vec![archived_record()], &options);
        let out = inject_offset_headers(stripped, 4, &options);

        let keys: Vec<&str> = out[0].headers.iter().map(|h| h.key.as_str()).collect();
        assert_eq!(
            keys,
            [
                "event-type",
                "trace-id",
                "X-Original-Offset",
                "x-original-offset",
                "x-original-timestamp",
                "x-source-partition",
            ]
        );
        assert_eq!(
            out[0].headers[3].value.as_deref(),
            Some(&9i64.to_le_bytes()[..])
        );
        assert_eq!(
            out[0].headers[5].value.as_deref(),
            Some(&4i32.to_le_bytes()[..])
        );
    }

    #[test]
    fn inject_offset_headers_is_a_no_op_when_not_configured() {
        let record = BackupRecord {
            key: None,
            value: None,
            headers: vec![header("trace-id", None)],
            timestamp: 0,
            offset: 0,
        };
        let out = inject_offset_headers(vec![record], 0, &RestoreOptions::default());
        assert_eq!(out[0].headers, vec![header("trace-id", None)]);
    }
}
