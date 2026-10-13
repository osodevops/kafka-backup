//! Shared helpers for restore operations.
//!
//! These functions are used by both the standard restore engine and the
//! repartitioning fan-out module.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::compression::{decompress, detect_from_extension};
use crate::config::{OffsetStrategy, RestoreOptions};
use crate::manifest::{BackupRecord, RecordHeader, RestoreCheckpoint, SegmentMetadata};
use crate::offset_headers;
use crate::segment::format::{BinaryRecord, MAGIC_BYTES};
use crate::segment::SegmentReader;
use crate::storage::StorageBackend;
use crate::{Error, Result};

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
        // Legacy JSON format — detect compression from the key's extension.
        // `detect_from_extension` matches on `.zst` / `.lz4` suffixes, so it
        // must see the full key (a bare `zst` never matched, which made every
        // compressed legacy segment fail to decompress).
        let algo = detect_from_extension(&segment.key);
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

/// Segments in restore order: by first source offset (stable for ties).
pub(crate) fn segments_in_offset_order<'a>(
    segments: impl IntoIterator<Item = &'a SegmentMetadata>,
) -> Vec<&'a SegmentMetadata> {
    let mut ordered: Vec<&SegmentMetadata> = segments.into_iter().collect();
    ordered.sort_by_key(|s| s.start_offset);
    ordered
}

/// Remembers the highest source offset already restored for one source
/// partition, so a record is never produced twice when a manifest written
/// before 0.25 lists overlapping segments (a segment key rewritten on
/// resume with different boundaries). Segments must be read in
/// [`segments_in_offset_order`].
#[derive(Debug, Default)]
pub(crate) struct OverlapGuard {
    last: Option<i64>,
}

impl OverlapGuard {
    /// Drop records at or below the highest offset already taken, then
    /// remember the new highest. Returns how many were dropped.
    pub(crate) fn skip_restored(&mut self, records: &mut Vec<BackupRecord>) -> u64 {
        let before = records.len();
        if let Some(last) = self.last {
            records.retain(|r| r.offset > last);
        }
        if let Some(max) = records.iter().map(|r| r.offset).max() {
            self.last = Some(self.last.map_or(max, |last| last.max(max)));
        }
        (before - records.len()) as u64
    }
}

/// Confluent schema IDs rewritten in one topic's keys and values, and keys
/// and values that carried an ID absent from the mapping. Shared by the
/// topic's partition tasks (1:1 path) or readers (repartition path).
#[derive(Debug, Default)]
pub(crate) struct SchemaIdCounts {
    rewritten: AtomicU64,
    unmapped: AtomicU64,
}

impl SchemaIdCounts {
    fn add(&self, rewritten: u64, unmapped: u64) {
        self.rewritten.fetch_add(rewritten, Ordering::Relaxed);
        self.unmapped.fetch_add(unmapped, Ordering::Relaxed);
    }

    /// `(rewritten, unmapped)` for the topic's report, logged when non-zero.
    pub(crate) fn finish(&self, source_topic: &str) -> (u64, u64) {
        let rewritten = self.rewritten.load(Ordering::Relaxed);
        let unmapped = self.unmapped.load(Ordering::Relaxed);
        if rewritten > 0 {
            info!("{source_topic}: rewrote {rewritten} Confluent schema ID(s)");
        }
        if unmapped > 0 {
            warn!(
                "{source_topic}: {unmapped} key(s)/value(s) start with the Confluent magic byte \
                 but carry a schema ID absent from restore.schema_id_mapping; they were \
                 restored unchanged (restore.schema_id_rewrite_strict: false)"
            );
        }
        (rewritten, unmapped)
    }
}

/// Whether `restore.rewrite_schema_ids` applies to `source_topic`: every
/// topic, or those `schema_id_rewrite_topics` matches (glob, or regex with a
/// `~` prefix).
pub(crate) fn schema_id_rewrite_applies(options: &RestoreOptions, source_topic: &str) -> bool {
    options.rewrite_schema_ids
        && (options.schema_id_rewrite_topics.is_empty()
            || options
                .schema_id_rewrite_topics
                .iter()
                .any(|pattern| super::engine::pattern_match(pattern, source_topic)))
}

/// Rewrite the Confluent schema IDs of records about to be produced
/// (`restore.rewrite_schema_ids`) and count them in `counts`.
///
/// Records of topics outside `schema_id_rewrite_topics` are left alone. With
/// `schema_id_rewrite_strict` (the default, as in 0.24) a key or value that
/// starts with the magic byte but is shorter than the 5-byte frame, or
/// carries an ID absent from `schema_id_mapping`, fails the restore before
/// any record of the batch is produced; with `schema_id_rewrite_strict:
/// false` it is left unchanged (an unmapped ID is counted). A mapping to an
/// ID that is not positive always fails.
///
/// Both restore paths call this after the record transform and before the
/// offset headers and partition routing.
pub(crate) fn rewrite_schema_ids(
    records: &mut [BackupRecord],
    source_topic: &str,
    source_partition: i32,
    options: &RestoreOptions,
    counts: &SchemaIdCounts,
) -> Result<()> {
    if !schema_id_rewrite_applies(options, source_topic) {
        return Ok(());
    }
    let (mut rewritten, mut unmapped) = (0u64, 0u64);
    for record in records.iter_mut() {
        let offset = record.offset;
        for (part, payload) in [
            ("key", record.key.as_mut()),
            ("value", record.value.as_mut()),
        ] {
            let strict = options.schema_id_rewrite_strict;
            let failure = match rewrite_confluent_payload(payload, &options.schema_id_mapping) {
                PayloadRewrite::NotFramed => None,
                PayloadRewrite::Rewritten => {
                    rewritten += 1;
                    None
                }
                PayloadRewrite::InvalidTarget { source, target } => Some(format!(
                    "restore.schema_id_mapping maps schema ID {source} (in the {part} of \
                     {source_topic} partition {source_partition} offset {offset}) to {target}; \
                     schema IDs are positive"
                )),
                PayloadRewrite::Truncated if strict => Some(format!(
                    "the {part} of {source_topic} partition {source_partition} offset {offset} \
                     starts with the Confluent magic byte but is shorter than a schema ID frame; \
                     restore.schema_id_rewrite_strict refuses to produce it (narrow \
                     schema_id_rewrite_topics, or set schema_id_rewrite_strict: false, for topics \
                     with raw keys or values)"
                )),
                PayloadRewrite::Truncated => None,
                PayloadRewrite::Unmapped(id) if strict => Some(format!(
                    "schema ID {id} in the {part} of {source_topic} partition \
                     {source_partition} offset {offset} is not in restore.schema_id_mapping; \
                     restore.schema_id_rewrite_strict refuses to produce it with the source \
                     registry's ID (narrow schema_id_rewrite_topics, or set \
                     schema_id_rewrite_strict: false, for topics with raw keys or values)"
                )),
                PayloadRewrite::Unmapped(_) => {
                    unmapped += 1;
                    None
                }
            };
            if let Some(message) = failure {
                counts.add(rewritten, unmapped);
                return Err(Error::Validation(message));
            }
        }
    }
    counts.add(rewritten, unmapped);
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum PayloadRewrite {
    /// Null, empty, or no magic byte 0.
    NotFramed,
    /// Magic byte 0 but shorter than the 5-byte frame header.
    Truncated,
    Rewritten,
    /// Magic byte 0 followed by this ID, absent from the mapping.
    Unmapped(i32),
    /// The mapping sends `source` to an ID that is not positive.
    InvalidTarget {
        source: i32,
        target: i32,
    },
}

/// Rewrite the 4-byte schema ID following Confluent's magic byte. Avro, JSON
/// Schema and Protobuf all share this five-byte prefix; any format-specific
/// bytes after it are preserved verbatim.
fn rewrite_confluent_payload(
    payload: Option<&mut Vec<u8>>,
    mapping: &HashMap<i32, i32>,
) -> PayloadRewrite {
    let Some(payload) = payload else {
        return PayloadRewrite::NotFramed;
    };
    if payload.first() != Some(&0) {
        return PayloadRewrite::NotFramed;
    }
    if payload.len() < 5 {
        return PayloadRewrite::Truncated;
    }
    let source_id = i32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
    match mapping.get(&source_id) {
        Some(&target) if target <= 0 => PayloadRewrite::InvalidTarget {
            source: source_id,
            target,
        },
        Some(target_id) => {
            payload[1..5].copy_from_slice(&target_id.to_be_bytes());
            PayloadRewrite::Rewritten
        }
        None => PayloadRewrite::Unmapped(source_id),
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

    /// Legacy (pre-binary) segments are a compressed JSON array of records.
    /// The codec is taken from the key's extension — the full key must be
    /// passed to `detect_from_extension`, which matches on `.zst` / `.lz4`
    /// (issue: compressed legacy segments failed to decompress).
    #[tokio::test]
    async fn read_segment_reads_legacy_json_segments_for_every_codec() {
        use crate::compression::compress;
        use crate::config::CompressionType;

        let records = vec![
            BackupRecord {
                key: Some(b"k".to_vec()),
                value: None,
                headers: vec![header("trace-id", None), header("tenant", Some(b"42"))],
                timestamp: 1_700_000_000_000,
                offset: 7,
            },
            BackupRecord {
                key: None,
                value: Some(b"v".to_vec()),
                headers: vec![],
                timestamp: 1_700_000_000_001,
                offset: 8,
            },
        ];
        let json = serde_json::to_vec(&records).unwrap();

        for (codec, ext) in [
            (CompressionType::None, ""),
            (CompressionType::Zstd, ".zst"),
            (CompressionType::Lz4, ".lz4"),
        ] {
            let storage = MemoryBackend::new();
            let key = format!("b/topics/t/partition=0/segment-00000000.json{ext}");
            storage
                .put(&key, compress(&json, codec).unwrap().into())
                .await
                .unwrap();
            let segment = SegmentMetadata {
                key: key.clone(),
                start_offset: 7,
                end_offset: 8,
                start_timestamp: 1_700_000_000_000,
                end_timestamp: 1_700_000_000_001,
                record_count: 2,
                uncompressed_size: json.len() as u64,
                compressed_size: 0,
                sha256: String::new(),
                uploaded_at: 0,
                covered_start_offset: None,
                covered_end_offset: None,
                control_records_skipped: 0,
                aborted_records_dropped: 0,
            };

            let out = read_segment(&storage, &segment)
                .await
                .unwrap_or_else(|e| panic!("{codec:?} legacy segment {key}: {e}"));

            assert_eq!(out.len(), 2, "{codec:?}");
            assert_eq!(out[0].headers, records[0].headers, "{codec:?}");
            assert_eq!(out[0].value, None, "{codec:?}");
            assert_eq!(out[1].offset, 8, "{codec:?}");
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

    fn at(offsets: std::ops::RangeInclusive<i64>) -> Vec<BackupRecord> {
        offsets
            .map(|offset| BackupRecord {
                key: None,
                value: Some(offset.to_string().into_bytes()),
                headers: vec![],
                timestamp: offset,
                offset,
            })
            .collect()
    }

    fn seg(key: &str, start: i64, end: i64) -> SegmentMetadata {
        SegmentMetadata {
            key: key.to_string(),
            start_offset: start,
            end_offset: end,
            start_timestamp: start,
            end_timestamp: end,
            record_count: end - start + 1,
            uncompressed_size: 0,
            compressed_size: 0,
            sha256: String::new(),
            uploaded_at: 0,
            covered_start_offset: None,
            covered_end_offset: None,
            control_records_skipped: 0,
            aborted_records_dropped: 0,
        }
    }

    /// A pre-0.25 manifest can list overlapping segments (a key rewritten on
    /// resume with other boundaries, kept by the merge). Restore must
    /// produce every source offset exactly once.
    #[test]
    fn overlapping_segments_produce_each_source_offset_once() {
        let segments = [seg("c", 150, 249), seg("a", 0, 199), seg("b", 100, 149)];
        let ordered = segments_in_offset_order(segments.iter());
        assert_eq!(
            ordered.iter().map(|s| s.key.as_str()).collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );

        let mut guard = OverlapGuard::default();
        let mut produced = Vec::new();
        let mut skipped = 0;
        for segment in ordered {
            let mut records = at(segment.start_offset..=segment.end_offset);
            skipped += guard.skip_restored(&mut records);
            produced.extend(records.into_iter().map(|r| r.offset));
        }
        assert_eq!(produced, (0..=249).collect::<Vec<_>>());
        assert_eq!(skipped, 50 + 50, "b entirely, and 150..=199 of c");
    }

    #[test]
    fn overlap_guard_keeps_disjoint_segments_untouched() {
        let mut guard = OverlapGuard::default();
        let mut first = at(0..=9);
        let mut second = at(12..=20);
        assert_eq!(guard.skip_restored(&mut first), 0);
        assert_eq!(guard.skip_restored(&mut second), 0);
        assert_eq!(second.len(), 9);
        let mut empty = Vec::new();
        assert_eq!(guard.skip_restored(&mut empty), 0);
        let mut replay = at(5..=25);
        assert_eq!(guard.skip_restored(&mut replay), 16);
        assert_eq!(replay.first().map(|r| r.offset), Some(21));
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

    fn record(key: Option<Vec<u8>>, value: Option<Vec<u8>>) -> BackupRecord {
        BackupRecord {
            key,
            value,
            headers: Vec::new(),
            timestamp: 0,
            offset: 3,
        }
    }

    fn rewrite_options(mapping: &[(i32, i32)]) -> RestoreOptions {
        RestoreOptions {
            rewrite_schema_ids: true,
            schema_id_mapping: mapping.iter().copied().collect(),
            ..Default::default()
        }
    }

    #[test]
    fn rewrites_confluent_key_and_value_without_touching_payload() {
        let mut records = vec![record(
            Some(vec![0, 0, 0, 0, 7, 10]),
            Some(vec![0, 0, 0, 0, 7, 20, 21]),
        )];
        let counts = SchemaIdCounts::default();
        rewrite_schema_ids(&mut records, "t", 0, &rewrite_options(&[(7, 42)]), &counts).unwrap();
        assert_eq!(records[0].key.as_deref(), Some(&[0, 0, 0, 0, 42, 10][..]));
        assert_eq!(
            records[0].value.as_deref(),
            Some(&[0, 0, 0, 0, 42, 20, 21][..])
        );
        assert_eq!(counts.finish("t"), (2, 0));
    }

    #[test]
    fn lenient_mode_ignores_null_short_and_non_confluent_payloads_and_counts_unmapped() {
        let mut records = vec![
            record(None, Some(vec![0, 1, 2])),
            record(Some(vec![1, 0, 0, 0, 7]), Some(vec![0, 0, 0, 0, 8])),
        ];
        let original = records.clone();
        let counts = SchemaIdCounts::default();
        let mut options = rewrite_options(&[(7, 42)]);
        options.schema_id_rewrite_strict = false;
        rewrite_schema_ids(&mut records, "t", 0, &options, &counts).unwrap();
        assert_eq!(records[0].key, original[0].key);
        assert_eq!(records[0].value, original[0].value);
        assert_eq!(records[1].key, original[1].key);
        assert_eq!(records[1].value, original[1].value);
        // Only `00 00 00 00 08` is framed, with an ID the mapping lacks.
        assert_eq!(counts.finish("t"), (0, 1));
    }

    #[test]
    fn rewrite_is_off_unless_enabled_and_scoped_by_topic_patterns() {
        let framed = || vec![record(None, Some(vec![0, 0, 0, 0, 7, 1]))];
        let counts = SchemaIdCounts::default();

        let mut disabled = framed();
        let mut options = rewrite_options(&[(7, 42)]);
        options.rewrite_schema_ids = false;
        rewrite_schema_ids(&mut disabled, "orders", 0, &options, &counts).unwrap();
        assert_eq!(disabled[0].value, framed()[0].value);

        options.rewrite_schema_ids = true;
        options.schema_id_rewrite_topics = vec!["payments-*".into(), "~^orders-v\\d+$".into()];
        assert!(!schema_id_rewrite_applies(&options, "orders"));
        assert!(schema_id_rewrite_applies(&options, "payments-eu"));
        assert!(schema_id_rewrite_applies(&options, "orders-v2"));

        let mut outside = framed();
        rewrite_schema_ids(&mut outside, "orders", 0, &options, &counts).unwrap();
        assert_eq!(outside[0].value, framed()[0].value);
        let mut inside = framed();
        rewrite_schema_ids(&mut inside, "orders-v2", 0, &options, &counts).unwrap();
        assert_eq!(inside[0].value.as_deref(), Some(&[0, 0, 0, 0, 42, 1][..]));
        assert_eq!(counts.finish("t"), (1, 0));
    }

    /// Strict is the default (0.24 failed on truncated and unmapped frames).
    #[test]
    fn strict_mode_fails_on_an_unmapped_id_naming_the_record() {
        let options = rewrite_options(&[(7, 42)]);
        assert!(options.schema_id_rewrite_strict);
        let counts = SchemaIdCounts::default();

        // Mapped and non-framed payloads pass.
        let mut ok = vec![record(Some(b"plain".to_vec()), Some(vec![0, 0, 0, 0, 7]))];
        rewrite_schema_ids(&mut ok, "orders", 2, &options, &counts).unwrap();

        let mut bad = vec![record(Some(vec![0, 0, 0, 1, 0, 9]), None)];
        let err = rewrite_schema_ids(&mut bad, "orders", 2, &options, &counts)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("schema ID 256 in the key of orders partition 2 offset 3"),
            "{err}"
        );
        assert!(err.contains("restore.schema_id_mapping"), "{err}");
    }

    #[test]
    fn strict_mode_fails_on_a_truncated_frame_and_any_mode_on_a_non_positive_target() {
        let counts = SchemaIdCounts::default();
        let mut truncated = vec![record(None, Some(vec![0, 1, 2]))];
        let err = rewrite_schema_ids(
            &mut truncated,
            "t",
            0,
            &rewrite_options(&[(7, 42)]),
            &counts,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("shorter than a schema ID frame"), "{err}");

        // Raw payloads that do not start with 0 pass, with or without a mapping.
        let mut raw = vec![record(None, Some(vec![1, 2]))];
        rewrite_schema_ids(&mut raw, "t", 0, &rewrite_options(&[]), &counts).unwrap();
        assert_eq!(raw[0].value, Some(vec![1, 2]));

        for strict in [true, false] {
            let mut options = rewrite_options(&[(7, 0)]);
            options.schema_id_rewrite_strict = strict;
            let mut zero = vec![record(None, Some(vec![0, 0, 0, 0, 7, 1]))];
            let err = rewrite_schema_ids(&mut zero, "t", 0, &options, &counts)
                .unwrap_err()
                .to_string();
            assert!(err.contains("schema IDs are positive"), "{err}");
            assert_eq!(zero[0].value, Some(vec![0, 0, 0, 0, 7, 1]));
        }
    }
}
