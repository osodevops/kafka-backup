//! Backup manifest and record structures.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::config::IsolationLevel;

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// Top-level backup manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Unique backup identifier
    pub backup_id: String,

    /// Creation timestamp (epoch milliseconds)
    pub created_at: i64,

    /// Source cluster ID (if available)
    #[serde(default)]
    pub source_cluster_id: Option<String>,

    /// Source bootstrap servers
    #[serde(default)]
    pub source_brokers: Vec<String>,

    /// Compression algorithm used
    #[serde(default)]
    pub compression: String,

    /// Topics included in this backup
    pub topics: Vec<TopicBackup>,

    /// Literal `topics.include` entries that were absent from the cluster at
    /// the last discovery pass and skipped because `backup.on_missing_topic`
    /// is `warn` (issue #167). Empty — and omitted from JSON — otherwise.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub missing_topics: Vec<String>,

    /// Transaction isolation of the most recent backup run
    /// (`backup.isolation_level`). `None` for manifests written before 0.24,
    /// which always read uncommitted: their segments can hold records of
    /// aborted transactions and transaction markers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation_level: Option<IsolationLevel>,
}

impl BackupManifest {
    /// Create a new empty manifest
    pub fn new(backup_id: String) -> Self {
        Self {
            backup_id,
            created_at: chrono::Utc::now().timestamp_millis(),
            source_cluster_id: None,
            source_brokers: Vec::new(),
            compression: "zstd".to_string(),
            topics: Vec::new(),
            missing_topics: Vec::new(),
            isolation_level: None,
        }
    }

    /// Get or create a topic backup entry
    pub fn get_or_create_topic(&mut self, name: &str) -> &mut TopicBackup {
        if !self.topics.iter().any(|t| t.name == name) {
            self.topics.push(TopicBackup {
                name: name.to_string(),
                original_partition_count: None,
                source_replication_factor: None,
                configurations: BTreeMap::new(),
                partitions: Vec::new(),
            });
        }
        self.topics.iter_mut().find(|t| t.name == name).unwrap()
    }

    /// Get total record count across all segments
    pub fn total_records(&self) -> i64 {
        self.topics
            .iter()
            .flat_map(|t| &t.partitions)
            .flat_map(|p| &p.segments)
            .map(|s| s.record_count)
            .sum()
    }

    /// Transaction markers a `read_committed` backup passed over, across all
    /// partitions (segments and trailing ranges).
    pub fn total_control_records_skipped(&self) -> u64 {
        self.topics
            .iter()
            .flat_map(|t| &t.partitions)
            .map(PartitionBackup::control_records_skipped)
            .sum()
    }

    /// Records of aborted transactions a `read_committed` backup dropped,
    /// across all partitions (segments and trailing ranges).
    pub fn total_aborted_records_dropped(&self) -> u64 {
        self.topics
            .iter()
            .flat_map(|t| &t.partitions)
            .map(PartitionBackup::aborted_records_dropped)
            .sum()
    }

    /// Get total segment count
    pub fn total_segments(&self) -> usize {
        self.topics
            .iter()
            .flat_map(|t| &t.partitions)
            .map(|p| p.segments.len())
            .sum()
    }

    /// Number of recorded offset gaps across all partitions (see [`OffsetGap`]).
    ///
    /// Zero means the backup captured every offset it set out to. Non-zero
    /// means the source no longer had some records by the time they were
    /// fetched, and this backup is knowingly incomplete for those ranges.
    /// Total pruned ranges across all partitions.
    pub fn total_pruned(&self) -> usize {
        self.topics
            .iter()
            .flat_map(|t| &t.partitions)
            .map(|p| p.pruned.len())
            .sum()
    }

    /// Iterate every pruned range with its topic and partition.
    pub fn pruned(&self) -> impl Iterator<Item = (&str, i32, &PrunedRange)> {
        self.topics.iter().flat_map(|t| {
            t.partitions.iter().flat_map(move |p| {
                p.pruned
                    .iter()
                    .map(move |range| (t.name.as_str(), p.partition_id, range))
            })
        })
    }

    pub fn total_gaps(&self) -> usize {
        self.topics
            .iter()
            .flat_map(|t| &t.partitions)
            .map(|p| p.gaps.len())
            .sum()
    }

    /// Iterate over every recorded offset gap as `(topic, partition, gap)`.
    pub fn gaps(&self) -> impl Iterator<Item = (&str, i32, &OffsetGap)> {
        self.topics.iter().flat_map(|t| {
            t.partitions.iter().flat_map(move |p| {
                p.gaps
                    .iter()
                    .map(move |g| (t.name.as_str(), p.partition_id, g))
            })
        })
    }
}

/// Per-topic backup metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicBackup {
    /// Topic name
    pub name: String,

    /// Original number of partitions in the source topic.
    ///
    /// Stored at backup time from Kafka metadata so that restore can recreate
    /// the topic with the correct partition count even when some partitions
    /// hold no data and are therefore absent from `partitions`.
    /// Old manifests that lack this field deserialize as `None`, and the
    /// restore engine falls back to `max(partition_id) + 1`.
    #[serde(default)]
    pub original_partition_count: Option<i32>,

    /// Source replication factor, retained as advisory recovery metadata. The
    /// target restore policy still controls the actual replication factor.
    #[serde(default)]
    pub source_replication_factor: Option<i16>,

    /// Explicit, mutable source topic configuration overrides. Broker-default,
    /// sensitive, and read-only entries are intentionally excluded.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub configurations: BTreeMap<String, String>,

    /// Partitions in this topic
    pub partitions: Vec<PartitionBackup>,
}

impl TopicBackup {
    /// Get or create a partition backup entry
    pub fn get_or_create_partition(&mut self, partition_id: i32) -> &mut PartitionBackup {
        if !self
            .partitions
            .iter()
            .any(|p| p.partition_id == partition_id)
        {
            self.partitions.push(PartitionBackup {
                partition_id,
                segments: Vec::new(),
                gaps: Vec::new(),
                pruned: Vec::new(),
                covered_through: None,
                tail_control_records_skipped: 0,
                tail_aborted_records_dropped: 0,
                snapshot_end_offset: None,
                high_watermark_at_capture: None,
            });
        }
        self.partitions
            .iter_mut()
            .find(|p| p.partition_id == partition_id)
            .unwrap()
    }
}

/// Per-partition backup metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionBackup {
    /// Partition ID
    pub partition_id: i32,

    /// Segments for this partition
    pub segments: Vec<SegmentMetadata>,

    /// Source offset ranges this backup could not capture (see [`OffsetGap`]).
    ///
    /// Empty for a backup with no known data loss. Omitted from the JSON when
    /// empty, and absent from manifests written before 0.17 — both read back
    /// as an empty list. Sorted by `start_offset`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gaps: Vec<OffsetGap>,

    /// Offset ranges deliberately deleted from this backup by retention
    /// (`kafka-backup prune` / `backup.retention` — see [`PrunedRange`]).
    /// Unlike [`gaps`](Self::gaps), these were captured and later removed on
    /// purpose; `validate`/`describe` report them without failing. Sorted by
    /// `start_offset`; omitted from JSON when empty (absent before 0.21).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pruned: Vec<PrunedRange>,

    /// Highest source offset this backup has accounted for (inclusive),
    /// including a tail that holds no record — trailing transaction markers
    /// or compacted-away offsets after the last segment. Only ever advanced
    /// after the segments (or gaps) covering everything up to it were
    /// written and listed, so it is a safe resume point.
    ///
    /// Absent from manifests written before 0.24 (reads as `None`; the
    /// segments' own ranges then give the reach — see [`reach`](Self::reach)).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covered_through: Option<i64>,

    /// Transaction markers a `read_committed` backup passed over in the
    /// trailing range after the last segment (through
    /// [`covered_through`](Self::covered_through)). The next segment's
    /// covered range absorbs that range and takes these counts over into its
    /// own [`SegmentMetadata::control_records_skipped`]. 0 (and omitted)
    /// otherwise.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub tail_control_records_skipped: u64,

    /// Records of aborted transactions dropped in the same trailing range
    /// (see [`tail_control_records_skipped`](Self::tail_control_records_skipped)).
    #[serde(default, skip_serializing_if = "is_zero")]
    pub tail_aborted_records_dropped: u64,

    /// Snapshot runs (`stop_at_current_offsets`) only: the offset the most
    /// recent snapshot captured as its end (exclusive) — the last stable
    /// offset under `read_committed`, the high watermark under
    /// `read_uncommitted`. Not recorded for a partition that was empty at
    /// capture time. Absent before 0.24.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_end_offset: Option<i64>,

    /// Snapshot runs only: the partition's high watermark when that snapshot
    /// was captured. Above [`snapshot_end_offset`](Self::snapshot_end_offset)
    /// when transactions were open at capture time: their records, and any
    /// record after them, are left for a later run. Absent before 0.24.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub high_watermark_at_capture: Option<i64>,
}

impl PartitionBackup {
    /// Transaction markers a `read_committed` backup passed over in this
    /// partition: the segments' counts plus the trailing range's.
    pub fn control_records_skipped(&self) -> u64 {
        self.segments
            .iter()
            .map(|s| s.control_records_skipped)
            .sum::<u64>()
            + self.tail_control_records_skipped
    }

    /// Records of aborted transactions a `read_committed` backup dropped in
    /// this partition: the segments' counts plus the trailing range's.
    pub fn aborted_records_dropped(&self) -> u64 {
        self.segments
            .iter()
            .map(|s| s.aborted_records_dropped)
            .sum::<u64>()
            + self.tail_aborted_records_dropped
    }

    /// Get the last offset backed up for this partition
    pub fn last_offset(&self) -> Option<i64> {
        self.segments.iter().map(|s| s.end_offset).max()
    }

    /// The durable reach of this partition: the highest source offset that
    /// is accounted for by the manifest — inside a listed segment's covered
    /// range, a recorded [`OffsetGap`], a [`PrunedRange`], or
    /// [`covered_through`](Self::covered_through). A backup resumes at
    /// `reach + 1`. `None` when the partition accounts for nothing yet.
    pub fn reach(&self) -> Option<i64> {
        let explicit = self.explicit_reach();
        match (explicit, self.covered_through) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }

    /// Like [`reach`](Self::reach) but over the explicit intervals only
    /// (segments, gaps, pruned ranges), ignoring `covered_through`. A new
    /// run starts its first segment's covered range here + 1 so the
    /// intervals stay contiguous even when the previous run ended on a
    /// coverage-only advance.
    pub fn explicit_reach(&self) -> Option<i64> {
        let segments = self.segments.iter().map(SegmentMetadata::covered_end);
        // OffsetGap.end_offset is exclusive; PrunedRange.end_offset inclusive.
        let gaps = self.gaps.iter().map(|g| g.end_offset - 1);
        let pruned = self.pruned.iter().map(|p| p.end_offset);
        segments.chain(gaps).chain(pruned).max()
    }

    /// Add a new segment
    pub fn add_segment(&mut self, segment: SegmentMetadata) {
        self.segments.push(segment);
    }

    /// Record an offset gap, keeping `gaps` sorted and free of duplicates.
    ///
    /// A gap with the same `start_offset` as an existing one is ignored: the
    /// same range can be re-detected when a run is resumed before its
    /// checkpoint advanced past the gap, and merging manifests must not
    /// double-count it.
    pub fn add_gap(&mut self, gap: OffsetGap) {
        if self.gaps.iter().any(|g| g.start_offset == gap.start_offset) {
            return;
        }
        self.gaps.push(gap);
        self.gaps.sort_by_key(|g| g.start_offset);
    }

    /// Record a pruned range, keeping `pruned` sorted and free of duplicates
    /// (same rule as [`add_gap`](Self::add_gap): manifest merges must not
    /// double-count a range).
    pub fn add_pruned(&mut self, range: PrunedRange) {
        if self
            .pruned
            .iter()
            .any(|p| p.start_offset == range.start_offset)
        {
            return;
        }
        self.pruned.push(range);
        self.pruned.sort_by_key(|p| p.start_offset);
    }
}

/// A contiguous range of offsets deliberately deleted from a backup by
/// retention (`kafka-backup prune` or `backup.retention`).
///
/// The bounds are the pruned segments' covered ranges (since 0.24; their
/// first and last record offsets before that).
///
/// The segments covering `[start_offset, end_offset]` were captured and then
/// removed on purpose; the range is recorded so `describe`/`validate` can
/// explain the hole and `restore` can distinguish "pruned" from "lost".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrunedRange {
    /// First pruned offset (inclusive).
    pub start_offset: i64,

    /// Last pruned offset (inclusive — matches `SegmentMetadata.end_offset`).
    pub end_offset: i64,

    /// Number of segments deleted for this range.
    pub segments: u32,

    /// Compressed bytes deleted for this range.
    pub bytes: u64,

    /// When the prune ran (epoch milliseconds).
    pub pruned_at: i64,

    /// The age cutoff the prune used (epoch milliseconds; `0` for a purely
    /// size-based prune).
    pub cutoff_timestamp: i64,

    /// Why the range was pruned.
    pub reason: PruneReason,
}

/// Why a [`PrunedRange`] was recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PruneReason {
    /// `backup.retention` applied at the end of a backup run.
    Retention,
    /// An operator ran `kafka-backup prune`.
    Manual,
}

impl std::fmt::Display for PruneReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PruneReason::Retention => write!(f, "retention"),
            PruneReason::Manual => write!(f, "manual"),
        }
    }
}

/// A contiguous range of source offsets that this backup could not capture
/// because the broker no longer had them when the partition was fetched.
///
/// Recorded when the broker returns `OFFSET_OUT_OF_RANGE` for the offset the
/// backup wanted next and its log start offset has already moved past it —
/// typically retention (or `DeleteRecords`) deleting data between snapshot
/// capture / checkpoint and the fetch (issue #144). The backup resumes from
/// `end_offset`; records in `[start_offset, end_offset)` are permanently
/// absent from this backup and cannot be recovered from the source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffsetGap {
    /// First missing offset (inclusive).
    pub start_offset: i64,

    /// First offset after the gap (exclusive) — the offset the backup
    /// resumed from.
    pub end_offset: i64,

    /// Why the gap occurred.
    pub reason: OffsetGapReason,

    /// When the gap was detected (epoch milliseconds).
    pub detected_at: i64,
}

impl OffsetGap {
    /// Number of offsets in `[start_offset, end_offset)`.
    ///
    /// This is an upper bound on the number of records lost: on compacted or
    /// transactional topics some offsets in the range may never have held a
    /// record by the time they were deleted.
    pub fn offset_span(&self) -> i64 {
        (self.end_offset - self.start_offset).max(0)
    }
}

/// Why an [`OffsetGap`] was recorded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OffsetGapReason {
    /// The broker returned `OFFSET_OUT_OF_RANGE` for the requested offset and
    /// its log start offset had advanced past it: the records were deleted by
    /// retention or `DeleteRecords` before the backup reached them.
    OffsetOutOfRange,
    /// A checkpoint written by an older version claimed these offsets were
    /// backed up, but no listed segment holds them (they were buffered or
    /// uploaded but never recorded in a saved manifest). The backup went
    /// back to re-read them and the source had already deleted them.
    ResumeBeyondDurable,
}

impl std::fmt::Display for OffsetGapReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OffsetGapReason::OffsetOutOfRange => write!(f, "offset_out_of_range"),
            OffsetGapReason::ResumeBeyondDurable => write!(f, "resume_beyond_durable"),
        }
    }
}

/// Segment metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentMetadata {
    /// Storage key for this segment
    pub key: String,

    /// First offset in segment
    pub start_offset: i64,

    /// Last offset in segment (inclusive)
    pub end_offset: i64,

    /// First record timestamp (epoch milliseconds)
    pub start_timestamp: i64,

    /// Last record timestamp (epoch milliseconds)
    pub end_timestamp: i64,

    /// Number of records in segment
    pub record_count: i64,

    /// Uncompressed size in bytes
    #[serde(default)]
    pub uncompressed_size: u64,

    /// Compressed size in bytes
    #[serde(default)]
    pub compressed_size: u64,

    /// SHA-256 of the stored segment bytes (header + compressed data + CRC
    /// footer), hex-encoded. Written since 0.21; empty for older segments.
    /// `validate --deep` verifies it when present.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub sha256: String,

    /// When the segment was written to storage (epoch milliseconds). `0` for
    /// segments written before 0.21. Retention prefers this over
    /// `end_timestamp` (which is producer record time) when present.
    #[serde(default)]
    pub uploaded_at: i64,

    /// First source offset this segment accounts for (inclusive). Can be
    /// below `start_offset` when the offsets before the first record held
    /// no record (compacted away, transaction markers). `None` for segments
    /// written before 0.24: `start_offset` applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covered_start_offset: Option<i64>,

    /// Last source offset this segment accounts for (inclusive) — the fetch
    /// position when it was sealed, minus one. Can be above `end_offset`
    /// for the same reasons. `None` before 0.24: `end_offset` applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covered_end_offset: Option<i64>,

    /// Transaction markers (commit/abort control records) inside the covered
    /// range that a `read_committed` backup passed over: they hold no data
    /// and are not archived. 0 (and omitted) for `read_uncommitted` backups,
    /// which archive markers as records, and for segments written before
    /// 0.24.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub control_records_skipped: u64,

    /// Records of aborted transactions inside the covered range that a
    /// `read_committed` backup dropped. 0 (and omitted) otherwise.
    #[serde(default, skip_serializing_if = "is_zero")]
    pub aborted_records_dropped: u64,
}

impl SegmentMetadata {
    /// First offset accounted for by this segment (see
    /// [`covered_start_offset`](Self::covered_start_offset)).
    pub fn covered_start(&self) -> i64 {
        self.covered_start_offset.unwrap_or(self.start_offset)
    }

    /// Last offset accounted for by this segment (see
    /// [`covered_end_offset`](Self::covered_end_offset)).
    pub fn covered_end(&self) -> i64 {
        self.covered_end_offset.unwrap_or(self.end_offset)
    }

    /// Whether the segment records its covered range (written by 0.24+).
    pub fn has_covered_range(&self) -> bool {
        self.covered_start_offset.is_some() && self.covered_end_offset.is_some()
    }

    /// Check if this segment overlaps with a time window
    pub fn overlaps_time_window(&self, start: Option<i64>, end: Option<i64>) -> bool {
        let segment_start = self.start_timestamp;
        let segment_end = self.end_timestamp;

        match (start, end) {
            (None, None) => true,
            (Some(s), None) => segment_end >= s,
            (None, Some(e)) => segment_start <= e,
            (Some(s), Some(e)) => segment_end >= s && segment_start <= e,
        }
    }
}

/// Individual backup record (stored in segments)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupRecord {
    /// Record key (optional)
    #[serde(default)]
    #[serde(with = "optional_bytes")]
    pub key: Option<Vec<u8>>,

    /// Record value (optional)
    #[serde(default)]
    #[serde(with = "optional_bytes")]
    pub value: Option<Vec<u8>>,

    /// Record headers
    #[serde(default)]
    pub headers: Vec<RecordHeader>,

    /// Record timestamp (epoch milliseconds)
    pub timestamp: i64,

    /// Original offset in source partition
    pub offset: i64,
}

/// Record header
///
/// Kafka distinguishes a header whose value is **null** from one whose value
/// is **empty**; both are legal and consumers can (and do) branch on the
/// difference. `value` is therefore `Option<Vec<u8>>`: `None` is a null value
/// (`-1` length on the wire and in the binary segment format),
/// `Some(vec![])` is an empty value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordHeader {
    /// Header key
    pub key: String,

    /// Header value (`None` = null, `Some(vec![])` = empty)
    #[serde(with = "optional_bytes", default)]
    pub value: Option<Vec<u8>>,
}

/// Serde helper for optional byte arrays (base64 encoded)
mod optional_bytes {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(value: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(bytes) => STANDARD.encode(bytes).serialize(serializer),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt: Option<String> = Option::deserialize(deserializer)?;
        match opt {
            Some(s) => STANDARD
                .decode(&s)
                .map(Some)
                .map_err(serde::de::Error::custom),
            None => Ok(None),
        }
    }
}

/// Restore checkpoint for resumable restores
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreCheckpoint {
    /// Backup ID being restored
    pub backup_id: String,

    /// Restore start time (epoch milliseconds)
    pub start_time: i64,

    /// Last checkpoint time (epoch milliseconds)
    pub last_checkpoint_time: i64,

    /// Segments that have been fully restored (storage keys)
    pub segments_completed: Vec<String>,

    /// Segments currently in progress: (key, bytes_processed)
    pub segments_in_progress: Vec<(String, u64)>,

    /// Total records restored so far
    pub records_restored: u64,

    /// Total bytes restored so far
    pub bytes_restored: u64,

    /// Restore configuration hash (to detect config changes)
    pub config_hash: String,
}

impl RestoreCheckpoint {
    /// Create a new checkpoint
    pub fn new(backup_id: String, config_hash: String) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            backup_id,
            start_time: now,
            last_checkpoint_time: now,
            segments_completed: Vec::new(),
            segments_in_progress: Vec::new(),
            records_restored: 0,
            bytes_restored: 0,
            config_hash,
        }
    }

    /// Mark a segment as completed
    pub fn mark_segment_completed(&mut self, key: &str) {
        self.segments_in_progress.retain(|(k, _)| k != key);
        if !self.segments_completed.contains(&key.to_string()) {
            self.segments_completed.push(key.to_string());
        }
    }

    /// Update segment progress
    pub fn update_segment_progress(&mut self, key: &str, bytes: u64) {
        if let Some(entry) = self.segments_in_progress.iter_mut().find(|(k, _)| k == key) {
            entry.1 = bytes;
        } else {
            self.segments_in_progress.push((key.to_string(), bytes));
        }
    }

    /// Check if a segment is already completed
    pub fn is_segment_completed(&self, key: &str) -> bool {
        self.segments_completed.contains(&key.to_string())
    }

    /// Update checkpoint timestamp
    pub fn touch(&mut self) {
        self.last_checkpoint_time = chrono::Utc::now().timestamp_millis();
    }
}

/// Restore report generated after restore completes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreReport {
    /// Backup ID that was restored
    pub backup_id: String,

    /// Whether this was a dry-run
    pub dry_run: bool,

    /// Start time of restore (epoch milliseconds)
    pub start_time: i64,

    /// End time of restore (epoch milliseconds)
    pub end_time: i64,

    /// Duration in milliseconds
    pub duration_ms: u64,

    /// Topics restored
    pub topics_restored: Vec<TopicRestoreReport>,

    /// Total segments processed
    pub segments_processed: u64,

    /// Total records restored
    pub records_restored: u64,

    /// Total bytes restored (uncompressed)
    pub bytes_restored: u64,

    /// Average throughput (records/sec)
    pub throughput_records_per_sec: f64,

    /// Average throughput (bytes/sec)
    pub throughput_bytes_per_sec: f64,

    /// Errors encountered (if any)
    pub errors: Vec<String>,

    /// Offset mapping (for consumer group reset)
    pub offset_mapping: OffsetMapping,

    /// Consumer groups resolved during restore (includes groups auto-loaded from snapshot)
    #[serde(default)]
    pub resolved_consumer_groups: Vec<String>,

    /// Records removed by the configured record filter (see
    /// `restore::filter`); `0` when no filter was set.
    #[serde(default)]
    pub records_dropped_by_filter: u64,

    /// Records turned into tombstones by the configured record filter.
    #[serde(default)]
    pub records_tombstoned_by_filter: u64,

    /// Name of the record filter that ran, when one was configured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_filter: Option<String>,

    /// Whether records were produced with an idempotent producer (a
    /// producer id and per-partition sequence numbers, so retries were
    /// de-duplicated by the broker).
    #[serde(default)]
    pub idempotent_producer: bool,

    /// Set when `restore.idempotent_producer` was requested but the target
    /// cluster denied or did not support it: why the restore fell back to
    /// producing without a producer id (retries may have duplicated
    /// records).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotent_producer_fallback: Option<String>,
}

/// Per-topic restore report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicRestoreReport {
    /// Source topic name
    pub source_topic: String,

    /// Target topic name
    pub target_topic: String,

    /// Partitions restored
    pub partitions: Vec<PartitionRestoreReport>,

    /// Total records for this topic
    pub records: u64,

    /// Total bytes for this topic
    pub bytes: u64,

    /// Records removed by the configured record filter for this topic.
    #[serde(default)]
    pub records_dropped_by_filter: u64,

    /// Records tombstoned by the configured record filter for this topic.
    #[serde(default)]
    pub records_tombstoned_by_filter: u64,
}

/// Per-partition restore report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionRestoreReport {
    /// Source partition ID
    pub source_partition: i32,

    /// Target partition ID
    pub target_partition: i32,

    /// Segments processed
    pub segments_processed: u64,

    /// Records restored
    pub records: u64,

    /// Bytes restored
    pub bytes: u64,

    /// First offset restored
    pub first_offset: i64,

    /// Last offset restored
    pub last_offset: i64,

    /// First timestamp restored
    pub first_timestamp: i64,

    /// Last timestamp restored
    pub last_timestamp: i64,

    /// Records removed by the configured record filter for this partition.
    #[serde(default)]
    pub records_dropped_by_filter: u64,

    /// Records tombstoned by the configured record filter for this partition.
    #[serde(default)]
    pub records_tombstoned_by_filter: u64,
}

/// Offset mapping for consumer group reset
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OffsetMapping {
    /// Mapping entries: topic/partition -> offset info
    pub entries: std::collections::HashMap<String, OffsetMappingEntry>,

    /// Detailed per-record offset mappings (for exact offset lookup)
    /// Key: "topic/partition", Value: Vec of (source_offset, target_offset, timestamp)
    #[serde(default)]
    pub detailed_mappings: std::collections::HashMap<String, Vec<OffsetPair>>,

    /// Consumer group offsets from source cluster (if backed up)
    #[serde(default)]
    pub consumer_groups: std::collections::HashMap<String, ConsumerGroupOffsets>,

    /// Source cluster identifier
    #[serde(default)]
    pub source_cluster_id: Option<String>,

    /// Target cluster identifier
    #[serde(default)]
    pub target_cluster_id: Option<String>,

    /// Mapping creation timestamp
    #[serde(default)]
    pub created_at: i64,
}

/// How [`OffsetMapping::resolve_target_offset`] derived a target offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum OffsetLookupPrecision {
    /// From the per-record mapping: the target offset of the first restored
    /// record at or after the source offset (one past the last restored
    /// record when there is none). Exact for every kind of gap in the
    /// restored source offsets.
    Exact,
    /// No per-record mapping for the partition, so the target offset was
    /// linearly interpolated over the restored offset range. Approximate:
    /// any gap inside the range makes it land early or late.
    Interpolated,
}

/// A target offset found by [`OffsetMapping::resolve_target_offset`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct TargetOffsetLookup {
    /// Offset on the target partition a consumer should resume from.
    pub target_offset: i64,
    /// Whether `target_offset` is exact or interpolated.
    pub precision: OffsetLookupPrecision,
}

impl TargetOffsetLookup {
    /// `true` when the offset came from the per-record mapping.
    pub fn is_exact(&self) -> bool {
        self.precision == OffsetLookupPrecision::Exact
    }
}

/// Individual offset pair for detailed mapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetPair {
    /// Original source offset
    pub source_offset: i64,
    /// Target offset after restore
    pub target_offset: i64,
    /// Record timestamp
    pub timestamp: i64,
}

/// Consumer group offset state
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConsumerGroupOffsets {
    /// Consumer group ID
    pub group_id: String,

    /// Per-partition committed offsets: topic -> partition -> offset info
    pub offsets:
        std::collections::HashMap<String, std::collections::HashMap<i32, ConsumerGroupOffset>>,
}

/// Individual consumer group partition offset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupOffset {
    /// Source cluster committed offset
    pub source_offset: i64,

    /// Target cluster offset (calculated from mapping)
    #[serde(default)]
    pub target_offset: Option<i64>,

    /// Commit timestamp
    pub timestamp: i64,

    /// Optional metadata from offset commit
    #[serde(default)]
    pub metadata: Option<String>,
}

impl ConsumerGroupOffsets {
    /// Create a new consumer group offsets with the given group ID
    pub fn new(group_id: &str) -> Self {
        Self {
            group_id: group_id.to_string(),
            offsets: std::collections::HashMap::new(),
        }
    }

    /// Add an offset for a topic/partition
    pub fn add_offset(&mut self, topic: &str, partition: i32, offset: ConsumerGroupOffset) {
        self.offsets
            .entry(topic.to_string())
            .or_default()
            .insert(partition, offset);
    }
}

impl OffsetMapping {
    /// Create a new empty offset mapping
    pub fn new() -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            detailed_mappings: std::collections::HashMap::new(),
            consumer_groups: std::collections::HashMap::new(),
            source_cluster_id: None,
            target_cluster_id: None,
            created_at: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Add an offset mapping entry
    pub fn add(
        &mut self,
        topic: &str,
        partition: i32,
        source_offset: i64,
        target_offset: Option<i64>,
        timestamp: i64,
    ) {
        let key = format!("{}/{}", topic, partition);
        self.entries.insert(
            key,
            OffsetMappingEntry {
                topic: topic.to_string(),
                partition,
                source_first_offset: source_offset,
                source_last_offset: source_offset,
                target_first_offset: target_offset,
                target_last_offset: target_offset,
                first_timestamp: timestamp,
                last_timestamp: timestamp,
            },
        );
    }

    /// Add a detailed offset mapping (per-record granularity)
    pub fn add_detailed(
        &mut self,
        topic: &str,
        partition: i32,
        source_offset: i64,
        target_offset: i64,
        timestamp: i64,
    ) {
        self.add_detailed_batch(
            topic,
            partition,
            vec![OffsetPair {
                source_offset,
                target_offset,
                timestamp,
            }],
        );
    }

    /// Add many detailed offset mappings under a single key (one lock holder's
    /// worth of work). Prefer this over repeated [`Self::add_detailed`] calls
    /// during restore so concurrent partitions do not serialize on the
    /// mapping mutex per record (issue #197).
    ///
    /// The per-partition list stays sorted by `source_offset` — the invariant
    /// [`Self::lookup_target_offset`]'s binary search relies on — regardless
    /// of the order pairs arrive in. Only the tail that overlaps the new batch
    /// is re-sorted, so appending in offset order (the common case) moves
    /// nothing.
    pub fn add_detailed_batch(&mut self, topic: &str, partition: i32, pairs: Vec<OffsetPair>) {
        if pairs.is_empty() {
            return;
        }

        let key = format!("{}/{}", topic, partition);

        // Update the range from the batch extremes: offsets by value, and the
        // batch's earliest / latest timestamps (not the timestamps of the
        // extreme-offset records) so `first_timestamp` / `last_timestamp`
        // still bound every record when timestamps are not monotonic in
        // offset order, exactly as the per-record path did.
        let mut min_pair = pairs[0].clone();
        let mut max_pair = pairs[0].clone();
        let mut min_ts = pairs[0].timestamp;
        let mut max_ts = pairs[0].timestamp;
        for p in &pairs[1..] {
            if p.source_offset < min_pair.source_offset {
                min_pair = p.clone();
            }
            if p.source_offset > max_pair.source_offset {
                max_pair = p.clone();
            }
            min_ts = min_ts.min(p.timestamp);
            max_ts = max_ts.max(p.timestamp);
        }
        self.update_range(
            topic,
            partition,
            min_pair.source_offset,
            Some(min_pair.target_offset),
            min_ts,
        );
        if max_pair.source_offset != min_pair.source_offset || max_ts != min_ts {
            self.update_range(
                topic,
                partition,
                max_pair.source_offset,
                Some(max_pair.target_offset),
                max_ts,
            );
        }

        let entry = self.detailed_mappings.entry(key).or_default();
        // Entries are only added through this method, so `entry` is sorted and
        // `partition_point` is valid: `start` is where the batch's smallest
        // offset belongs. When a record filter maps a segment's dropped
        // records after that segment's survivors, the overlapping tail is
        // bounded by one segment — never the partition's full history.
        let start = entry.partition_point(|p| p.source_offset < min_pair.source_offset);
        let old_len = entry.len();
        entry.reserve(pairs.len());
        entry.extend(pairs);
        let appended_in_order = start == old_len
            && entry[old_len..]
                .windows(2)
                .all(|w| w[0].source_offset <= w[1].source_offset);
        if !appended_in_order {
            entry[start..].sort_by_key(|p| p.source_offset);
        }
    }

    /// Update offset range for a topic/partition
    pub fn update_range(
        &mut self,
        topic: &str,
        partition: i32,
        source_offset: i64,
        target_offset: Option<i64>,
        timestamp: i64,
    ) {
        let key = format!("{}/{}", topic, partition);
        if let Some(entry) = self.entries.get_mut(&key) {
            if source_offset <= entry.source_first_offset {
                entry.source_first_offset = source_offset;
                if target_offset.is_some() || entry.target_first_offset.is_none() {
                    entry.target_first_offset = target_offset;
                }
                entry.first_timestamp = timestamp.min(entry.first_timestamp);
            }
            if source_offset >= entry.source_last_offset {
                entry.source_last_offset = source_offset;
                if target_offset.is_some() || entry.target_last_offset.is_none() {
                    entry.target_last_offset = target_offset;
                }
                entry.last_timestamp = timestamp.max(entry.last_timestamp);
            }
        } else {
            self.add(topic, partition, source_offset, target_offset, timestamp);
        }
    }

    /// Translate a committed source offset to the target offset a consumer
    /// should resume from, saying whether the answer is exact.
    ///
    /// A committed offset `S` means "the next record to read is `S`". With a
    /// per-record mapping the answer is the target offset of the first
    /// restored record whose source offset is `>= S`, or one past the last
    /// restored record when every restored record precedes `S` (the group had
    /// consumed everything that was restored). That is the number of
    /// restored records preceding `S` counted from the first target offset,
    /// so it stays exact whatever made the restored source offsets
    /// non-contiguous: compaction holes, records dropped by a record filter,
    /// transaction markers and aborted records, or retention. It never
    /// resumes a group past a record it has not consumed. When the same
    /// source offset was restored twice (a segment re-produced after a
    /// resumed restore) the earliest copy is used, so nothing is skipped.
    ///
    /// Without a per-record mapping for the partition the target offset is
    /// linearly interpolated over the restored range and reported as
    /// [`OffsetLookupPrecision::Interpolated`], with a warning: gaps inside
    /// the range make it land early or late. Offsets outside the range clamp
    /// to the first target offset or one past the last.
    ///
    /// Returns `None` when the partition has no target offsets at all.
    pub fn resolve_target_offset(
        &self,
        topic: &str,
        partition: i32,
        source_offset: i64,
    ) -> Option<TargetOffsetLookup> {
        let key = format!("{}/{}", topic, partition);

        // Per-record mapping. Kept sorted by source offset (stable for equal
        // source offsets) by `add_detailed_batch`, so this is a binary search.
        if let Some(detailed) = self.detailed_mappings.get(&key).filter(|d| !d.is_empty()) {
            let idx = detailed.partition_point(|p| p.source_offset < source_offset);
            let target_offset = match detailed.get(idx) {
                Some(next) => next.target_offset,
                None => detailed[detailed.len() - 1].target_offset + 1,
            };
            return Some(TargetOffsetLookup {
                target_offset,
                precision: OffsetLookupPrecision::Exact,
            });
        }

        // Approximate fallback: linear interpolation over the range.
        let entry = self.entries.get(&key)?;
        let (Some(target_first), Some(target_last)) =
            (entry.target_first_offset, entry.target_last_offset)
        else {
            return None;
        };
        let target_offset = if source_offset <= entry.source_first_offset {
            target_first
        } else if source_offset > entry.source_last_offset {
            target_last + 1
        } else {
            let source_range = (entry.source_last_offset - entry.source_first_offset) as f64;
            let position = (source_offset - entry.source_first_offset) as f64 / source_range;
            target_first + (position * (target_last - target_first) as f64).floor() as i64
        };
        tracing::warn!(
            "Offset mapping for {}:{} has no per-record entries; source offset {} \
             maps to target offset {} by linear interpolation, which is approximate \
             when the restored offsets have gaps",
            topic,
            partition,
            source_offset,
            target_offset
        );
        Some(TargetOffsetLookup {
            target_offset,
            precision: OffsetLookupPrecision::Interpolated,
        })
    }

    /// Precision a lookup on this partition would have: `Exact` with a
    /// per-record mapping, `Interpolated` with only a range that has target
    /// offsets, `None` when there are no target offsets for it.
    pub fn lookup_precision(&self, topic: &str, partition: i32) -> Option<OffsetLookupPrecision> {
        let key = format!("{}/{}", topic, partition);
        if self
            .detailed_mappings
            .get(&key)
            .is_some_and(|d| !d.is_empty())
        {
            return Some(OffsetLookupPrecision::Exact);
        }
        let entry = self.entries.get(&key)?;
        (entry.target_first_offset.is_some() && entry.target_last_offset.is_some())
            .then_some(OffsetLookupPrecision::Interpolated)
    }

    /// Target offset for a committed source offset; see
    /// [`Self::resolve_target_offset`], which also says whether the result
    /// is exact or interpolated.
    pub fn lookup_target_offset(
        &self,
        topic: &str,
        partition: i32,
        source_offset: i64,
    ) -> Option<i64> {
        self.resolve_target_offset(topic, partition, source_offset)
            .map(|found| found.target_offset)
    }

    /// Find the nearest offset by timestamp
    pub fn get_nearest_offset_by_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Option<(i64, i64)> {
        let key = format!("{}/{}", topic, partition);

        if let Some(detailed) = self.detailed_mappings.get(&key) {
            // Find the first offset with timestamp >= requested timestamp
            let nearest = detailed
                .iter()
                .filter(|p| p.timestamp >= timestamp)
                .min_by_key(|p| p.timestamp);

            if let Some(pair) = nearest {
                return Some((pair.source_offset, pair.target_offset));
            }

            // If no exact match, return the last offset
            if let Some(last) = detailed.last() {
                return Some((last.source_offset, last.target_offset));
            }
        }

        None
    }

    /// Add consumer group offset from source cluster
    pub fn add_consumer_group_offset(
        &mut self,
        group_id: &str,
        topic: &str,
        partition: i32,
        source_offset: i64,
        timestamp: i64,
        metadata: Option<String>,
    ) {
        // Calculate target offset first (before borrowing consumer_groups mutably)
        let target_offset = self.lookup_target_offset(topic, partition, source_offset);

        let group = self
            .consumer_groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroupOffsets {
                group_id: group_id.to_string(),
                offsets: std::collections::HashMap::new(),
            });

        let topic_offsets = group.offsets.entry(topic.to_string()).or_default();

        topic_offsets.insert(
            partition,
            ConsumerGroupOffset {
                source_offset,
                target_offset,
                timestamp,
                metadata,
            },
        );
    }

    /// Recalculate all consumer group target offsets based on current mapping
    pub fn recalculate_consumer_group_offsets(&mut self) {
        // Collect all the lookups we need to do first
        let mut lookups: Vec<(String, String, i32, i64)> = Vec::new(); // (group_id, topic, partition, source_offset)

        for (group_id, group) in &self.consumer_groups {
            for (topic, partitions) in &group.offsets {
                for (partition, offset) in partitions {
                    lookups.push((
                        group_id.clone(),
                        topic.clone(),
                        *partition,
                        offset.source_offset,
                    ));
                }
            }
        }

        // Calculate all target offsets
        let results: Vec<(String, String, i32, Option<i64>)> = lookups
            .into_iter()
            .map(|(group_id, topic, partition, source_offset)| {
                let target = self.lookup_target_offset(&topic, partition, source_offset);
                (group_id, topic, partition, target)
            })
            .collect();

        // Apply the results
        for (group_id, topic, partition, target_offset) in results {
            if let Some(group) = self.consumer_groups.get_mut(&group_id) {
                if let Some(topic_offsets) = group.offsets.get_mut(&topic) {
                    if let Some(offset) = topic_offsets.get_mut(&partition) {
                        offset.target_offset = target_offset;
                    }
                }
            }
        }
    }

    /// Get all entries as a sorted list
    pub fn sorted_entries(&self) -> Vec<&OffsetMappingEntry> {
        let mut entries: Vec<_> = self.entries.values().collect();
        entries.sort_by(|a, b| {
            a.topic
                .cmp(&b.topic)
                .then_with(|| a.partition.cmp(&b.partition))
        });
        entries
    }

    /// Get total number of detailed offset pairs
    pub fn detailed_mapping_count(&self) -> usize {
        self.detailed_mappings.values().map(|v| v.len()).sum()
    }

    /// Check if detailed mappings are available
    pub fn has_detailed_mappings(&self) -> bool {
        !self.detailed_mappings.is_empty()
    }
}

/// Single offset mapping entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetMappingEntry {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// First source offset in range
    pub source_first_offset: i64,

    /// Last source offset in range
    pub source_last_offset: i64,

    /// First target offset (after restore)
    pub target_first_offset: Option<i64>,

    /// Last target offset (after restore)
    pub target_last_offset: Option<i64>,

    /// Earliest record timestamp at the low end of the range (bounds every
    /// record that established `source_first_offset`)
    pub first_timestamp: i64,

    /// Latest record timestamp seen while extending the range (the maximum
    /// record timestamp for an in-order restore)
    pub last_timestamp: i64,
}

/// Dry-run validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunReport {
    /// Backup ID being validated
    pub backup_id: String,

    /// Whether the restore would succeed
    pub valid: bool,

    /// Validation errors (if any)
    pub errors: Vec<String>,

    /// Warnings (non-fatal issues)
    pub warnings: Vec<String>,

    /// Segments that would be processed
    pub segments_to_process: u64,

    /// Records that would be restored
    pub records_to_restore: u64,

    /// Bytes that would be restored
    pub bytes_to_restore: u64,

    /// Time range that would be covered
    pub time_range: Option<(i64, i64)>,

    /// Topics that would be restored
    pub topics_to_restore: Vec<DryRunTopicReport>,

    /// Consumer offsets that would need to be reset
    pub consumer_offset_actions: Vec<String>,

    /// Phase 1 header preflight result (present when the restore
    /// configuration required a tracking-metadata scan; see issue #137).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_preflight: Option<crate::restore::preflight::HeaderPreflightReport>,
}

/// Per-topic dry-run report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunTopicReport {
    /// Source topic name
    pub source_topic: String,

    /// Target topic name
    pub target_topic: String,

    /// Repartitioning info (if configured for this topic)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repartitioning: Option<DryRunRepartitioningInfo>,

    /// Partitions to restore
    pub partitions: Vec<DryRunPartitionReport>,
}

/// Repartitioning details shown in dry-run report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunRepartitioningInfo {
    /// Strategy used (murmur2 or automatic)
    pub strategy: String,
    /// Number of source partitions
    pub source_partitions: i32,
    /// Number of target partitions
    pub target_partitions: i32,
}

/// Per-partition dry-run report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunPartitionReport {
    /// Source partition ID
    pub source_partition: i32,

    /// Target partition ID
    pub target_partition: i32,

    /// Segments to process
    pub segments: u64,

    /// Records to restore
    pub records: u64,

    /// Offset range (start, end)
    pub offset_range: (i64, i64),

    /// Timestamp range (start, end)
    pub timestamp_range: (i64, i64),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Issue #155: a null header value must serialize as JSON `null` and
    /// deserialize back to `None` — never collapse into an empty value.
    #[test]
    fn record_header_null_value_roundtrips_as_json_null() {
        let header = RecordHeader {
            key: "trace-id".to_string(),
            value: None,
        };
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, r#"{"key":"trace-id","value":null}"#);
        let back: RecordHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(back, header);
    }

    #[test]
    fn record_header_empty_value_stays_distinct_from_null() {
        let header = RecordHeader {
            key: "empty".to_string(),
            value: Some(Vec::new()),
        };
        let json = serde_json::to_string(&header).unwrap();
        assert_eq!(json, r#"{"key":"empty","value":""}"#);
        let back: RecordHeader = serde_json::from_str(&json).unwrap();
        assert_eq!(back.value, Some(Vec::new()));
    }

    #[test]
    fn record_header_legacy_json_segments_still_deserialize() {
        // Legacy JSON segments (pre-binary format) always wrote a base64 string.
        let back: RecordHeader = serde_json::from_str(r#"{"key":"k","value":"YWJj"}"#).unwrap();
        assert_eq!(back.value.as_deref(), Some(&b"abc"[..]));
        // A header written without the field decodes as null.
        let back: RecordHeader = serde_json::from_str(r#"{"key":"k"}"#).unwrap();
        assert_eq!(back.value, None);
    }

    #[test]
    fn test_offset_mapping_basic() {
        let mut mapping = OffsetMapping::new();
        mapping.add("orders", 0, 0, None, 1700000000000);
        mapping.update_range("orders", 0, 100, None, 1700001000000);

        assert_eq!(mapping.entries.len(), 1);

        let entry = mapping.entries.get("orders/0").unwrap();
        assert_eq!(entry.source_first_offset, 0);
        assert_eq!(entry.source_last_offset, 100);
    }

    #[test]
    fn test_offset_mapping_detailed() {
        let mut mapping = OffsetMapping::new();

        // Add detailed mappings
        mapping.add_detailed("orders", 0, 0, 5000, 1700000000000);
        mapping.add_detailed("orders", 0, 1, 5001, 1700000001000);
        mapping.add_detailed("orders", 0, 2, 5002, 1700000002000);

        assert_eq!(mapping.detailed_mapping_count(), 3);

        // Lookup should find exact match
        let target = mapping.lookup_target_offset("orders", 0, 1);
        assert_eq!(target, Some(5001));

        // Offset beyond the restored range resumes one past the last
        // restored record (never extrapolated past the target's log end)
        let target = mapping.lookup_target_offset("orders", 0, 999);
        assert_eq!(target, Some(5003));

        // Non-existent topic/partition returns None
        let target = mapping.lookup_target_offset("unknown", 0, 1);
        assert_eq!(target, None);

        let target = mapping.lookup_target_offset("orders", 99, 1);
        assert_eq!(target, None);
    }

    #[test]
    fn test_offset_mapping_range_interpolation() {
        let mut mapping = OffsetMapping::new();

        // Set up a range-based mapping (no detailed mapping)
        mapping.add("orders", 0, 0, Some(5000), 1700000000000);
        mapping.update_range("orders", 0, 100, Some(5100), 1700001000000);

        // Interpolation should work for offsets in range
        let target = mapping.lookup_target_offset("orders", 0, 50);
        assert_eq!(target, Some(5050)); // Midpoint

        let target = mapping.lookup_target_offset("orders", 0, 0);
        assert_eq!(target, Some(5000));

        let target = mapping.lookup_target_offset("orders", 0, 100);
        assert_eq!(target, Some(5100));
    }

    #[test]
    fn test_offset_mapping_by_timestamp() {
        let mut mapping = OffsetMapping::new();

        mapping.add_detailed("orders", 0, 0, 5000, 1700000000000);
        mapping.add_detailed("orders", 0, 1, 5001, 1700000001000);
        mapping.add_detailed("orders", 0, 2, 5002, 1700000002000);

        // Find by exact timestamp
        let result = mapping.get_nearest_offset_by_timestamp("orders", 0, 1700000001000);
        assert_eq!(result, Some((1, 5001)));

        // Find by timestamp between records (should get next higher)
        let result = mapping.get_nearest_offset_by_timestamp("orders", 0, 1700000000500);
        assert_eq!(result, Some((1, 5001)));

        // Find by timestamp before first record
        let result = mapping.get_nearest_offset_by_timestamp("orders", 0, 1699999999000);
        assert_eq!(result, Some((0, 5000)));
    }

    #[test]
    fn test_consumer_group_offsets() {
        let mut group = ConsumerGroupOffsets::new("test-group");

        group.add_offset(
            "orders",
            0,
            ConsumerGroupOffset {
                source_offset: 50,
                target_offset: Some(5050),
                timestamp: 1700000050000,
                metadata: Some("test-metadata".to_string()),
            },
        );

        assert_eq!(group.group_id, "test-group");
        assert!(group.offsets.contains_key("orders"));
        assert!(group.offsets.get("orders").unwrap().contains_key(&0));

        let offset = group.offsets.get("orders").unwrap().get(&0).unwrap();
        assert_eq!(offset.source_offset, 50);
        assert_eq!(offset.target_offset, Some(5050));
    }

    #[test]
    fn test_offset_mapping_with_consumer_groups() {
        let mut mapping = OffsetMapping::new();

        // Add detailed mappings
        mapping.add_detailed("orders", 0, 0, 5000, 1700000000000);
        mapping.add_detailed("orders", 0, 50, 5050, 1700000050000);
        mapping.add_detailed("orders", 0, 100, 5100, 1700000100000);

        // Add consumer group with source offsets
        mapping.add_consumer_group_offset(
            "order-processor",
            "orders",
            0,
            50,
            1700000050000,
            Some("".to_string()),
        );

        // Consumer group should have target offset calculated
        let groups = &mapping.consumer_groups;
        assert!(groups.contains_key("order-processor"));

        let group = groups.get("order-processor").unwrap();
        let offset = group.offsets.get("orders").unwrap().get(&0).unwrap();
        assert_eq!(offset.source_offset, 50);
        assert_eq!(offset.target_offset, Some(5050));
    }

    #[test]
    fn test_offset_mapping_sorted_entries() {
        let mut mapping = OffsetMapping::new();

        mapping.add("orders", 2, 0, None, 1700000000000);
        mapping.add("orders", 0, 0, None, 1700000000000);
        mapping.add("payments", 0, 0, None, 1700000000000);
        mapping.add("orders", 1, 0, None, 1700000000000);

        let sorted = mapping.sorted_entries();

        assert_eq!(sorted.len(), 4);
        assert_eq!(sorted[0].topic, "orders");
        assert_eq!(sorted[0].partition, 0);
        assert_eq!(sorted[1].topic, "orders");
        assert_eq!(sorted[1].partition, 1);
        assert_eq!(sorted[2].topic, "orders");
        assert_eq!(sorted[2].partition, 2);
        assert_eq!(sorted[3].topic, "payments");
        assert_eq!(sorted[3].partition, 0);
    }

    #[test]
    fn test_offset_pair() {
        let pair = OffsetPair {
            source_offset: 100,
            target_offset: 5100,
            timestamp: 1700000000000,
        };

        assert_eq!(pair.source_offset, 100);
        assert_eq!(pair.target_offset, 5100);
        assert_eq!(pair.timestamp, 1700000000000);
    }

    #[test]
    fn add_detailed_batch_matches_per_record_add_detailed() {
        let mut per_record = OffsetMapping::new();
        let mut batched = OffsetMapping::new();
        let pairs: Vec<OffsetPair> = (0..10)
            .map(|i| OffsetPair {
                source_offset: i,
                target_offset: 1000 + i,
                timestamp: 1_700_000_000_000 + i,
            })
            .collect();

        for p in &pairs {
            per_record.add_detailed("orders", 0, p.source_offset, p.target_offset, p.timestamp);
        }
        batched.add_detailed_batch("orders", 0, pairs);

        assert_eq!(
            per_record.detailed_mapping_count(),
            batched.detailed_mapping_count()
        );
        assert_eq!(
            per_record.lookup_target_offset("orders", 0, 5),
            batched.lookup_target_offset("orders", 0, 5)
        );
        let key = "orders/0";
        assert_eq!(
            per_record.detailed_mappings[key].len(),
            batched.detailed_mappings[key].len()
        );
        let pe = &per_record.entries[key];
        let be = &batched.entries[key];
        assert_eq!(pe.source_first_offset, be.source_first_offset);
        assert_eq!(pe.source_last_offset, be.source_last_offset);
        assert_eq!(pe.target_first_offset, be.target_first_offset);
        assert_eq!(pe.target_last_offset, be.target_last_offset);
    }

    fn pairs(list: &[(i64, i64)]) -> Vec<OffsetPair> {
        list.iter()
            .map(|&(source_offset, target_offset)| OffsetPair {
                source_offset,
                target_offset,
                timestamp: 1_700_000_000_000 + source_offset,
            })
            .collect()
    }

    #[test]
    fn add_detailed_batch_keeps_detailed_mappings_sorted() {
        let mut mapping = OffsetMapping::new();

        // Segment survivors arrive in offset order (records 1 and 3 were
        // dropped by a record filter).
        mapping.add_detailed_batch("orders", 0, pairs(&[(0, 100), (2, 101), (4, 102)]));
        // The restore engine then maps the dropped records to the next
        // survivor's target — behind the survivors, i.e. out of offset order.
        mapping.add_detailed_batch("orders", 0, pairs(&[(1, 101), (3, 102)]));
        // A batch that is itself unsorted.
        mapping.add_detailed_batch("orders", 0, pairs(&[(7, 104), (5, 103), (6, 104)]));
        // Single out-of-order insert through the per-record API.
        mapping.add_detailed("orders", 0, 8, 105, 1_700_000_000_008);

        let offsets: Vec<i64> = mapping.detailed_mappings["orders/0"]
            .iter()
            .map(|p| p.source_offset)
            .collect();
        assert_eq!(offsets, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);

        // Exact lookups resolve via the binary-search path.
        assert_eq!(mapping.lookup_target_offset("orders", 0, 1), Some(101));
        assert_eq!(mapping.lookup_target_offset("orders", 0, 3), Some(102));
        assert_eq!(mapping.lookup_target_offset("orders", 0, 5), Some(103));
        assert_eq!(mapping.lookup_target_offset("orders", 0, 8), Some(105));

        // Range mapping still reflects the true extremes.
        let entry = &mapping.entries["orders/0"];
        assert_eq!(entry.source_first_offset, 0);
        assert_eq!(entry.source_last_offset, 8);
        assert_eq!(entry.target_first_offset, Some(100));
        assert_eq!(entry.target_last_offset, Some(105));
    }

    #[test]
    fn add_detailed_batch_range_timestamps_bound_the_whole_batch() {
        // Timestamps are not monotonic in offset order (CreateTime records
        // can arrive out of order). The per-record path tracked the maximum
        // timestamp while extending the range; the batch path must not
        // regress to "timestamp of the highest offset".
        let batch = vec![
            OffsetPair {
                source_offset: 0,
                target_offset: 100,
                timestamp: 1_000,
            },
            OffsetPair {
                source_offset: 1,
                target_offset: 101,
                timestamp: 3_000,
            },
            OffsetPair {
                source_offset: 2,
                target_offset: 102,
                timestamp: 2_000,
            },
        ];
        let mut per_record = OffsetMapping::new();
        for p in &batch {
            per_record.add_detailed("orders", 0, p.source_offset, p.target_offset, p.timestamp);
        }
        let mut batched = OffsetMapping::new();
        batched.add_detailed_batch("orders", 0, batch);

        let pe = &per_record.entries["orders/0"];
        let be = &batched.entries["orders/0"];
        assert_eq!((be.first_timestamp, be.last_timestamp), (1_000, 3_000));
        assert_eq!(
            (be.first_timestamp, be.last_timestamp),
            (pe.first_timestamp, pe.last_timestamp)
        );
        assert_eq!((be.source_first_offset, be.source_last_offset), (0, 2));
        assert_eq!(
            (be.target_first_offset, be.target_last_offset),
            (Some(100), Some(102))
        );
    }

    #[test]
    fn add_detailed_batch_in_order_append_preserves_input_order() {
        let mut mapping = OffsetMapping::new();
        mapping.add_detailed_batch("orders", 0, pairs(&[(10, 500), (11, 501)]));
        mapping.add_detailed_batch("orders", 0, pairs(&[(12, 502), (13, 503)]));
        let detailed = &mapping.detailed_mappings["orders/0"];
        let offsets: Vec<i64> = detailed.iter().map(|p| p.source_offset).collect();
        assert_eq!(offsets, vec![10, 11, 12, 13]);
        assert_eq!(detailed[3].target_offset, 503);
    }

    #[test]
    fn update_range_none_then_some_preserves_targets() {
        // Reproduces the RestoreEngine pattern: a pre-produce loop calls
        // update_range with None targets for all records in a segment, then
        // the post-produce add_detailed calls update_range with Some targets.
        // Before the fix, the pre-produce loop advanced source_last_offset so
        // that post-produce calls never matched the > boundary, leaving
        // target_first_offset and target_last_offset as None.
        let mut m = OffsetMapping::new();

        // Pre-produce: register source range without target offsets
        for src in 0..1000i64 {
            m.update_range("t", 0, src, None, 1000 + src);
        }

        let e = m.entries.get("t/0").unwrap();
        assert_eq!(e.source_first_offset, 0);
        assert_eq!(e.source_last_offset, 999);
        assert_eq!(e.target_first_offset, None, "no target yet");
        assert_eq!(e.target_last_offset, None, "no target yet");

        // Post-produce: add_detailed fills in actual target offsets
        for src in 0..1000i64 {
            m.add_detailed("t", 0, src, src, 1000 + src);
        }

        let e = m.entries.get("t/0").unwrap();
        assert_eq!(e.target_first_offset, Some(0), "seed first target");
        assert_eq!(e.target_last_offset, Some(999), "seed last target");
    }

    #[test]
    fn update_range_some_not_overwritten_by_none() {
        // When a second segment's pre-produce loop calls update_range(None)
        // for offsets beyond the first segment, it must not overwrite the
        // Some(target_last) that the first segment's post-produce set.
        let mut m = OffsetMapping::new();

        // Segment 1 (offsets 0-999): pre-produce then post-produce
        for src in 0..1000i64 {
            m.update_range("t", 0, src, None, 1000 + src);
        }
        for src in 0..1000i64 {
            m.add_detailed("t", 0, src, src, 1000 + src);
        }

        let e = m.entries.get("t/0").unwrap();
        assert_eq!(e.target_first_offset, Some(0));
        assert_eq!(e.target_last_offset, Some(999));

        // Segment 2 (offsets 1000-1999): pre-produce with None
        for src in 1000..2000i64 {
            m.update_range("t", 0, src, None, 1000 + src);
        }

        let e = m.entries.get("t/0").unwrap();
        assert_eq!(
            e.target_last_offset,
            Some(999),
            "None must not overwrite Some"
        );

        // Segment 2: post-produce with Some
        for src in 1000..2000i64 {
            m.add_detailed("t", 0, src, src, 1000 + src);
        }

        let e = m.entries.get("t/0").unwrap();
        assert_eq!(e.source_first_offset, 0);
        assert_eq!(e.source_last_offset, 1999);
        assert_eq!(e.target_first_offset, Some(0));
        assert_eq!(e.target_last_offset, Some(1999));
    }

    #[test]
    fn update_range_shifted_target_offsets() {
        // Target offsets can differ from source (e.g. target partition not
        // empty). Verify the range tracks correctly.
        let mut m = OffsetMapping::new();
        let base = 5000i64;

        for src in 0..100i64 {
            m.update_range("t", 0, src, None, 1000 + src);
        }
        for src in 0..100i64 {
            m.add_detailed("t", 0, src, base + src, 1000 + src);
        }

        let e = m.entries.get("t/0").unwrap();
        assert_eq!(e.target_first_offset, Some(5000));
        assert_eq!(e.target_last_offset, Some(5099));
    }

    // ------------------------------------------------------------------
    // Source -> target lookup across gaps
    //
    // A committed consumer offset S means "the next record to read is S".
    // On the target, that is the first restored record whose source offset
    // is >= S; when every restored record is below S, it is one past the
    // last restored record. Equivalently: target base + the number of
    // restored records that precede S. Gaps in the restored source offsets
    // (compaction holes, record-filter drops, transaction markers, aborted
    // records) must never push the result past a record the group has not
    // consumed yet.
    // ------------------------------------------------------------------

    /// Simulate a restore: `restored` source offsets (ascending) are produced
    /// densely to the target starting at `base`, recorded the way the restore
    /// engine records them (one `add_detailed_batch` per produce batch).
    fn restore_sim(restored: &[i64], base: i64, batch: usize) -> OffsetMapping {
        let mut m = OffsetMapping::new();
        let pairs: Vec<OffsetPair> = restored
            .iter()
            .enumerate()
            .map(|(i, &s)| OffsetPair {
                source_offset: s,
                target_offset: base + i as i64,
                timestamp: 1_700_000_000_000 + s,
            })
            .collect();
        for chunk in pairs.chunks(batch.max(1)) {
            m.add_detailed_batch("t", 0, chunk.to_vec());
        }
        m
    }

    /// Ground truth: the target offset a consumer committed at source `s`
    /// must resume from.
    fn expected_target(restored: &[i64], base: i64, s: i64) -> i64 {
        base + restored.iter().filter(|&&r| r < s).count() as i64
    }

    #[test]
    fn lookup_inside_compaction_gap_resumes_at_next_restored_record() {
        // Source 0..=4 and 10..=14 restored; 5..=9 compacted away.
        let restored: Vec<i64> = (0..5).chain(10..15).collect();
        let m = restore_sim(&restored, 0, 100);
        // Inside the hole: the next record to read is source 10 = target 5.
        // The old nearest-lower + delta rule gave 4 + 3 = 7, skipping
        // source 10 and 11.
        assert_eq!(m.lookup_target_offset("t", 0, 7), Some(5));
        assert_eq!(m.lookup_target_offset("t", 0, 9), Some(5));
    }

    #[test]
    fn lookup_at_gap_start_resumes_at_next_restored_record() {
        let restored: Vec<i64> = (0..5).chain(10..15).collect();
        let m = restore_sim(&restored, 0, 100);
        assert_eq!(m.lookup_target_offset("t", 0, 5), Some(5));
    }

    #[test]
    fn lookup_right_after_gap_is_exact() {
        let restored: Vec<i64> = (0..5).chain(10..15).collect();
        let m = restore_sim(&restored, 0, 100);
        assert_eq!(m.lookup_target_offset("t", 0, 10), Some(5));
        assert_eq!(m.lookup_target_offset("t", 0, 11), Some(6));
    }

    #[test]
    fn lookup_at_end_of_log_is_one_past_last_restored_record() {
        // Group fully caught up: committed offset = source high watermark.
        let restored: Vec<i64> = (0..5).chain(10..15).collect();
        let m = restore_sim(&restored, 100, 100);
        assert_eq!(m.lookup_target_offset("t", 0, 15), Some(110));
    }

    #[test]
    fn lookup_beyond_end_does_not_overshoot_the_restored_log() {
        // Records 15..=19 existed on the source (e.g. aborted transaction
        // + marker, or produced after the snapshot) but were not restored.
        // The old rule gave 109 + (20 - 14) = 115: five offsets past the
        // target's log end.
        let restored: Vec<i64> = (0..5).chain(10..15).collect();
        let m = restore_sim(&restored, 100, 100);
        assert_eq!(m.lookup_target_offset("t", 0, 20), Some(110));
        assert_eq!(m.lookup_target_offset("t", 0, 1_000_000), Some(110));
    }

    #[test]
    fn lookup_before_first_restored_record_starts_at_first_target() {
        // Retention purged 0..=49 before the backup; group committed at 10.
        let restored: Vec<i64> = (50..60).collect();
        let m = restore_sim(&restored, 7, 100);
        assert_eq!(m.lookup_target_offset("t", 0, 10), Some(7));
        assert_eq!(m.lookup_target_offset("t", 0, 50), Some(7));
    }

    #[test]
    fn lookup_counts_preceding_records_across_many_compacted_holes() {
        // Deterministic pseudo-random survivors (~40% kept) over 0..500,
        // recorded in uneven produce batches.
        let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
        let mut restored = Vec::new();
        for s in 0..500i64 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            if (state >> 33) % 10 < 4 {
                restored.push(s);
            }
        }
        assert!(restored.len() > 100);
        let m = restore_sim(&restored, 42, 7);
        for s in -1..=505i64 {
            assert_eq!(
                m.lookup_target_offset("t", 0, s),
                Some(expected_target(&restored, 42, s)),
                "source offset {s}"
            );
        }
    }

    /// The source partition of a read_committed backup: producer A commits
    /// one transaction and aborts another, producer B aborts one and commits
    /// one, with plain records in between. Only the committed and plain
    /// records are restored.
    ///
    /// ```text
    ///  0 n0   1 a0   2 a1   3 n1   4 b0 (aborted)   5 a2   6 COMMIT(A)
    ///  7 b1 (aborted)   8 ABORT(B)   9 n2   10 a3 (aborted)   11 ABORT(A)
    /// 12 b2  13 COMMIT(B)  14 n3   — high watermark 15
    /// ```
    fn read_committed_restore() -> (Vec<i64>, OffsetMapping) {
        let restored = vec![0, 1, 2, 3, 5, 9, 12, 14];
        let mapping = restore_sim(&restored, 0, 3);
        (restored, mapping)
    }

    #[test]
    fn committed_offset_at_a_transaction_marker_resumes_at_the_next_restored_record() {
        let (restored, m) = read_committed_restore();
        // At the COMMIT marker 6: the group consumed through 5 (a2); next
        // is n2 (source 9) = target 5.
        assert_eq!(m.lookup_target_offset("t", 0, 6), Some(5));
        // At the ABORT markers 8 and 11, and the COMMIT marker 13.
        assert_eq!(m.lookup_target_offset("t", 0, 8), Some(5));
        assert_eq!(m.lookup_target_offset("t", 0, 11), Some(6)); // b2
        assert_eq!(m.lookup_target_offset("t", 0, 13), Some(7)); // n3
        for s in [6, 8, 11, 13] {
            assert_eq!(
                m.lookup_target_offset("t", 0, s),
                Some(expected_target(&restored, 0, s))
            );
        }
    }

    #[test]
    fn committed_offset_just_after_a_marker_resumes_at_the_next_restored_record() {
        let (_, m) = read_committed_restore();
        // marker + 1 = 7 is an aborted record: still n2 (target 5).
        assert_eq!(m.lookup_target_offset("t", 0, 7), Some(5));
        // ABORT(B) + 1 = 9 is restored: exact.
        assert_eq!(m.lookup_target_offset("t", 0, 9), Some(5));
        // COMMIT(B) + 1 = 14 is restored: exact.
        assert_eq!(m.lookup_target_offset("t", 0, 14), Some(7));
    }

    #[test]
    fn committed_offset_inside_an_aborted_transaction_resumes_at_the_next_restored_record() {
        let (_, m) = read_committed_restore();
        // b0 (4) was aborted: next is a2 (5) = target 4.
        assert_eq!(m.lookup_target_offset("t", 0, 4), Some(4));
        // a3 (10) was aborted: next is b2 (12) = target 6.
        assert_eq!(m.lookup_target_offset("t", 0, 10), Some(6));
    }

    #[test]
    fn committed_offset_at_the_lso_or_high_watermark_resumes_at_the_target_log_end() {
        let (_, m) = read_committed_restore();
        // Caught up: committed at the high watermark (= LSO, no open
        // transaction) → one past the last restored record.
        assert_eq!(m.lookup_target_offset("t", 0, 15), Some(8));
        // A hanging transaction from 15 kept the backup at LSO 15 while the
        // high watermark reached 20; a group committed at 20 (it read the
        // plain records after the open transaction with read_uncommitted)
        // resumes at the target's log end, never past it.
        assert_eq!(m.lookup_target_offset("t", 0, 20), Some(8));
        assert_eq!(
            m.resolve_target_offset("t", 0, 20).unwrap().precision,
            OffsetLookupPrecision::Exact
        );
    }

    #[test]
    fn lookup_record_filter_drops_resume_at_next_survivor() {
        // Source 0..=9; a record filter dropped 3, 4 and the tail 8, 9.
        // Dropped offsets are simply absent from the mapping.
        let restored = vec![0, 1, 2, 5, 6, 7];
        let m = restore_sim(&restored, 0, 2);
        assert_eq!(m.lookup_target_offset("t", 0, 3), Some(3)); // source 5
        assert_eq!(m.lookup_target_offset("t", 0, 4), Some(3));
        assert_eq!(m.lookup_target_offset("t", 0, 5), Some(3));
        assert_eq!(m.lookup_target_offset("t", 0, 8), Some(6)); // log end
        assert_eq!(m.lookup_target_offset("t", 0, 10), Some(6)); // HW
    }

    #[test]
    fn lookup_reports_exact_precision_for_record_mappings() {
        let restored: Vec<i64> = (0..5).chain(10..15).collect();
        let m = restore_sim(&restored, 0, 100);
        let found = m.resolve_target_offset("t", 0, 7).unwrap();
        assert_eq!(found.target_offset, 5);
        assert_eq!(found.precision, OffsetLookupPrecision::Exact);
        assert!(found.is_exact());
        assert_eq!(
            m.lookup_precision("t", 0),
            Some(OffsetLookupPrecision::Exact)
        );
    }

    #[test]
    fn lookup_without_record_mappings_interpolates_and_says_so() {
        // Range-only mapping (e.g. built by hand, or a migration seed map):
        // the only option left is an approximation.
        let mut m = OffsetMapping::new();
        m.add("t", 0, 0, Some(5000), 1);
        m.update_range("t", 0, 100, Some(5100), 2);

        let mid = m.resolve_target_offset("t", 0, 50).unwrap();
        assert_eq!(mid.target_offset, 5050);
        assert_eq!(mid.precision, OffsetLookupPrecision::Interpolated);
        assert!(!mid.is_exact());
        assert_eq!(
            m.lookup_precision("t", 0),
            Some(OffsetLookupPrecision::Interpolated)
        );

        // Never extrapolated outside the restored range: before the first
        // restored record -> first target; past the last -> one past it.
        assert_eq!(m.lookup_target_offset("t", 0, -5), Some(5000));
        assert_eq!(m.lookup_target_offset("t", 0, 0), Some(5000));
        assert_eq!(m.lookup_target_offset("t", 0, 100), Some(5100));
        assert_eq!(m.lookup_target_offset("t", 0, 101), Some(5101));
        assert_eq!(m.lookup_target_offset("t", 0, 10_000), Some(5101));
    }

    #[test]
    fn lookup_record_mappings_win_over_the_range() {
        // Both exist for the partition (the normal restore case): the
        // record-level answer is used, never the interpolation.
        let restored: Vec<i64> = (0..5).chain(10..15).collect();
        let m = restore_sim(&restored, 0, 3);
        assert!(m.entries.contains_key("t/0"));
        // Interpolation over source 0..=14 -> target 0..=9 would give 4 for
        // source 7 (rounded down); the exact answer is 5.
        let found = m.resolve_target_offset("t", 0, 7).unwrap();
        assert_eq!(found.target_offset, 5);
        assert_eq!(found.precision, OffsetLookupPrecision::Exact);
    }

    #[test]
    fn lookup_unknown_partition_is_none() {
        let m = restore_sim(&[0, 1, 2], 0, 10);
        assert_eq!(m.resolve_target_offset("t", 1, 0), None);
        assert_eq!(m.resolve_target_offset("other", 0, 0), None);
        assert_eq!(m.lookup_precision("t", 1), None);

        // Range without target offsets (built from a manifest only).
        let mut ranged = OffsetMapping::new();
        ranged.add("t", 0, 0, None, 1);
        ranged.update_range("t", 0, 10, None, 2);
        assert_eq!(ranged.lookup_target_offset("t", 0, 5), None);
        assert_eq!(ranged.lookup_precision("t", 0), None);
    }

    #[test]
    fn lookup_after_resume_reproduced_segment_does_not_skip() {
        // Source 0..=9 -> target 0..=9, then a resumed restore re-produced
        // source 5..=9 (target 10..=14) before continuing with 10..=19
        // (target 15..=24). Resuming a group at source 7 must not skip
        // anything: the earliest copy is used.
        let mut m = OffsetMapping::new();
        let batch = |range: std::ops::Range<i64>, base: i64| -> Vec<OffsetPair> {
            range
                .clone()
                .enumerate()
                .map(|(i, s)| OffsetPair {
                    source_offset: s,
                    target_offset: base + i as i64,
                    timestamp: s,
                })
                .collect()
        };
        m.add_detailed_batch("t", 0, batch(0..10, 0));
        m.add_detailed_batch("t", 0, batch(5..10, 10));
        m.add_detailed_batch("t", 0, batch(10..20, 15));
        assert_eq!(m.lookup_target_offset("t", 0, 7), Some(7));
        assert_eq!(m.lookup_target_offset("t", 0, 10), Some(15));
        assert_eq!(m.lookup_target_offset("t", 0, 20), Some(25));
    }

    // ------------------------------------------------------------------
    // Offset gaps (issue #144)
    // ------------------------------------------------------------------

    fn gap(start: i64, end: i64) -> OffsetGap {
        OffsetGap {
            start_offset: start,
            end_offset: end,
            reason: OffsetGapReason::OffsetOutOfRange,
            detected_at: 1_700_000_000_000,
        }
    }

    #[test]
    fn offset_gap_span_is_non_negative() {
        assert_eq!(gap(100, 250).offset_span(), 150);
        assert_eq!(gap(100, 100).offset_span(), 0);
        assert_eq!(gap(100, 50).offset_span(), 0);
    }

    #[test]
    fn partition_add_gap_dedups_by_start_and_sorts() {
        let mut manifest = BackupManifest::new("b".to_string());
        let part = manifest
            .get_or_create_topic("orders")
            .get_or_create_partition(0);
        assert!(part.gaps.is_empty());

        part.add_gap(gap(300, 400));
        part.add_gap(gap(100, 200));
        part.add_gap(gap(100, 250)); // same start — ignored, first wins
        assert_eq!(part.gaps, vec![gap(100, 200), gap(300, 400)]);

        assert_eq!(manifest.total_gaps(), 2);
        let collected: Vec<_> = manifest.gaps().collect();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0].0, "orders");
        assert_eq!(collected[0].1, 0);
        assert_eq!(collected[0].2, &gap(100, 200));
    }

    #[test]
    fn offset_gap_serde_round_trip() {
        let mut manifest = BackupManifest::new("b".to_string());
        manifest
            .get_or_create_topic("orders")
            .get_or_create_partition(2)
            .add_gap(gap(100, 200));

        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.contains("\"gaps\""), "{json}");
        assert!(
            json.contains("\"reason\":\"offset_out_of_range\""),
            "reason must be a stable snake_case string: {json}"
        );

        let back: BackupManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.topics[0].partitions[0].gaps, vec![gap(100, 200)]);
    }

    #[test]
    fn empty_gaps_are_omitted_from_json() {
        let mut manifest = BackupManifest::new("b".to_string());
        manifest
            .get_or_create_topic("orders")
            .get_or_create_partition(0);
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(
            !json.contains("gaps"),
            "a backup with no gaps must not advertise a gaps field: {json}"
        );
    }

    fn pruned_range(start: i64, end: i64) -> PrunedRange {
        PrunedRange {
            start_offset: start,
            end_offset: end,
            segments: 2,
            bytes: 2048,
            pruned_at: 1_700_000_000_000,
            cutoff_timestamp: 1_699_000_000_000,
            reason: PruneReason::Retention,
        }
    }

    #[test]
    fn pruned_range_serde_round_trip() {
        let range = pruned_range(0, 19);
        let json = serde_json::to_string(&range).unwrap();
        assert!(json.contains("\"reason\":\"retention\""), "{json}");
        let back: PrunedRange = serde_json::from_str(&json).unwrap();
        assert_eq!(back, range);
    }

    #[test]
    fn empty_pruned_is_omitted_from_json() {
        let partition = PartitionBackup {
            partition_id: 0,
            segments: Vec::new(),
            gaps: Vec::new(),
            pruned: Vec::new(),
            covered_through: None,
            tail_control_records_skipped: 0,
            tail_aborted_records_dropped: 0,
            snapshot_end_offset: None,
            high_watermark_at_capture: None,
        };
        let json = serde_json::to_string(&partition).unwrap();
        assert!(!json.contains("pruned"), "{json}");
    }

    #[test]
    fn add_pruned_dedups_by_start_and_sorts() {
        let mut partition = PartitionBackup {
            partition_id: 0,
            segments: Vec::new(),
            gaps: Vec::new(),
            pruned: Vec::new(),
            covered_through: None,
            tail_control_records_skipped: 0,
            tail_aborted_records_dropped: 0,
            snapshot_end_offset: None,
            high_watermark_at_capture: None,
        };
        partition.add_pruned(pruned_range(20, 29));
        partition.add_pruned(pruned_range(0, 19));
        partition.add_pruned(pruned_range(0, 25)); // duplicate start ignored
        assert_eq!(partition.pruned.len(), 2);
        assert_eq!(partition.pruned[0].start_offset, 0);
        assert_eq!(partition.pruned[0].end_offset, 19);
        assert_eq!(partition.pruned[1].start_offset, 20);
    }

    #[test]
    fn manifest_written_before_pruned_and_sha256_existed_still_parses() {
        // A 0.20-era segment/partition: no pruned, no sha256, no uploaded_at.
        let legacy = r#"{
            "backup_id": "legacy",
            "created_at": 1700000000000,
            "compression": "zstd",
            "topics": [{
                "name": "orders",
                "partitions": [{
                    "partition_id": 0,
                    "segments": [{
                        "key": "legacy/topics/orders/partition=0/segment-00000000000000000000.bin.zst",
                        "start_offset": 0,
                        "end_offset": 9,
                        "start_timestamp": 1,
                        "end_timestamp": 2,
                        "record_count": 10
                    }]
                }]
            }]
        }"#;
        let manifest: BackupManifest = serde_json::from_str(legacy).unwrap();
        let segment = &manifest.topics[0].partitions[0].segments[0];
        assert_eq!(segment.sha256, "");
        assert_eq!(segment.uploaded_at, 0);
        assert!(manifest.topics[0].partitions[0].pruned.is_empty());
        assert_eq!(manifest.total_pruned(), 0);
    }

    #[test]
    fn manifest_written_before_gaps_existed_still_parses() {
        // Exactly what a 0.16 manifest looks like: no `gaps` key at all.
        let legacy = r#"{
            "backup_id": "old",
            "created_at": 1700000000000,
            "topics": [{
                "name": "orders",
                "partitions": [{
                    "partition_id": 0,
                    "segments": [{
                        "key": "old/topics/orders/partition=0/segment-0.bin",
                        "start_offset": 0,
                        "end_offset": 99,
                        "start_timestamp": 0,
                        "end_timestamp": 99000,
                        "record_count": 100,
                        "uncompressed_size": 1,
                        "compressed_size": 1
                    }]
                }]
            }]
        }"#;
        let manifest: BackupManifest = serde_json::from_str(legacy).unwrap();
        assert!(manifest.topics[0].partitions[0].gaps.is_empty());
        assert_eq!(manifest.total_gaps(), 0);
        assert_eq!(manifest.gaps().count(), 0);
    }

    // ------------------------------------------------------------------
    // Covered ranges and the durable reach (checkpoint durability)
    // ------------------------------------------------------------------

    fn segment(start: i64, end: i64) -> SegmentMetadata {
        SegmentMetadata {
            key: format!("b/topics/t/partition=0/segment-{start:020}.bin.zst"),
            start_offset: start,
            end_offset: end,
            start_timestamp: 0,
            end_timestamp: 0,
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

    fn covered(start: i64, end: i64, covered_start: i64, covered_end: i64) -> SegmentMetadata {
        SegmentMetadata {
            covered_start_offset: Some(covered_start),
            covered_end_offset: Some(covered_end),
            control_records_skipped: 0,
            aborted_records_dropped: 0,
            ..segment(start, end)
        }
    }

    fn empty_partition() -> PartitionBackup {
        PartitionBackup {
            partition_id: 0,
            segments: Vec::new(),
            gaps: Vec::new(),
            pruned: Vec::new(),
            covered_through: None,
            tail_control_records_skipped: 0,
            tail_aborted_records_dropped: 0,
            snapshot_end_offset: None,
            high_watermark_at_capture: None,
        }
    }

    #[test]
    fn legacy_segment_covers_its_record_range() {
        let seg = segment(10, 19);
        assert_eq!(seg.covered_start(), 10);
        assert_eq!(seg.covered_end(), 19);
        assert!(!seg.has_covered_range());

        // Compaction hole before the first record and markers after the last.
        let seg = covered(12, 19, 10, 24);
        assert_eq!(seg.covered_start(), 10);
        assert_eq!(seg.covered_end(), 24);
        assert!(seg.has_covered_range());
    }

    #[test]
    fn reach_is_none_for_an_empty_partition() {
        assert_eq!(empty_partition().reach(), None);
        assert_eq!(empty_partition().explicit_reach(), None);
    }

    #[test]
    fn reach_takes_the_highest_accounted_offset() {
        let mut part = empty_partition();
        part.segments.push(segment(0, 9));
        assert_eq!(part.reach(), Some(9));

        // A covered end beyond the last record (trailing markers / compaction).
        part.segments.push(covered(10, 19, 10, 29));
        assert_eq!(part.reach(), Some(29));

        // An OffsetGap is end-exclusive: [30, 50) accounts through 49.
        part.add_gap(gap(30, 50));
        assert_eq!(part.reach(), Some(49));

        // A coverage-only advance with no segment.
        part.covered_through = Some(60);
        assert_eq!(part.reach(), Some(60));
        // ...is not an explicit interval (validate cannot see what it covered).
        assert_eq!(part.explicit_reach(), Some(49));
    }

    #[test]
    fn reach_counts_pruned_ranges_inclusive() {
        let mut part = empty_partition();
        part.add_pruned(pruned_range(0, 99));
        assert_eq!(part.reach(), Some(99));
        part.segments.push(segment(100, 149));
        assert_eq!(part.reach(), Some(149));
    }

    #[test]
    fn reach_is_the_max_even_with_overlapping_legacy_segments() {
        // A legacy manifest can list overlapping segments (a key rewritten on
        // resume with different boundaries). Reach is still the maximum.
        let mut part = empty_partition();
        part.segments.push(segment(0, 199));
        part.segments.push(segment(100, 149));
        assert_eq!(part.reach(), Some(199));
    }

    #[test]
    fn covered_fields_are_omitted_when_unset() {
        let json = serde_json::to_string(&segment(0, 9)).unwrap();
        assert!(!json.contains("covered_"), "{json}");
        let json = serde_json::to_string(&empty_partition()).unwrap();
        assert!(!json.contains("covered_through"), "{json}");
    }

    #[test]
    fn covered_fields_round_trip() {
        let seg = covered(12, 19, 10, 24);
        let back: SegmentMetadata =
            serde_json::from_str(&serde_json::to_string(&seg).unwrap()).unwrap();
        assert_eq!(back.covered_start_offset, Some(10));
        assert_eq!(back.covered_end_offset, Some(24));

        let mut part = empty_partition();
        part.covered_through = Some(42);
        let back: PartitionBackup =
            serde_json::from_str(&serde_json::to_string(&part).unwrap()).unwrap();
        assert_eq!(back.covered_through, Some(42));
    }

    #[test]
    fn manifest_written_before_covered_ranges_existed_still_parses() {
        // A 0.23 manifest: no covered_* on segments, no covered_through.
        let legacy = r#"{
            "backup_id": "b",
            "created_at": 1700000000000,
            "compression": "zstd",
            "topics": [{
                "name": "t",
                "partitions": [{
                    "partition_id": 0,
                    "segments": [{
                        "key": "b/topics/t/partition=0/segment-00000000000000000000.bin.zst",
                        "start_offset": 0,
                        "end_offset": 9,
                        "start_timestamp": 1,
                        "end_timestamp": 2,
                        "record_count": 10,
                        "sha256": "ab",
                        "uploaded_at": 5
                    }]
                }]
            }]
        }"#;
        let manifest: BackupManifest = serde_json::from_str(legacy).unwrap();
        let part = &manifest.topics[0].partitions[0];
        assert_eq!(part.covered_through, None);
        assert_eq!(part.segments[0].covered_start_offset, None);
        assert_eq!(part.segments[0].covered_end(), 9);
        assert_eq!(part.reach(), Some(9));
    }

    /// Older binaries (0.23 and earlier) must still read manifests written
    /// with covered ranges: the structs never set `deny_unknown_fields`, so
    /// the new keys are ignored. `OldSegment` / `OldPartition` mirror the
    /// 0.23 field set exactly.
    #[test]
    fn manifest_with_covered_ranges_parses_with_the_old_field_set() {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct OldSegment {
            key: String,
            start_offset: i64,
            end_offset: i64,
            start_timestamp: i64,
            end_timestamp: i64,
            record_count: i64,
            #[serde(default)]
            uncompressed_size: u64,
            #[serde(default)]
            compressed_size: u64,
            #[serde(default)]
            sha256: String,
            #[serde(default)]
            uploaded_at: i64,
        }
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct OldPartition {
            partition_id: i32,
            segments: Vec<OldSegment>,
            #[serde(default)]
            gaps: Vec<OffsetGap>,
            #[serde(default)]
            pruned: Vec<PrunedRange>,
        }
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct OldTopic {
            name: String,
            partitions: Vec<OldPartition>,
        }
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct OldManifest {
            backup_id: String,
            created_at: i64,
            topics: Vec<OldTopic>,
        }

        let mut manifest = BackupManifest::new("b".to_string());
        let part = manifest.get_or_create_topic("t").get_or_create_partition(0);
        part.add_segment(covered(12, 19, 10, 24));
        part.covered_through = Some(30);
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.contains("covered_end_offset"), "{json}");
        assert!(json.contains("covered_through"), "{json}");

        let old: OldManifest = serde_json::from_str(&json).unwrap();
        let seg = &old.topics[0].partitions[0].segments[0];
        assert_eq!((seg.start_offset, seg.end_offset), (12, 19));
        assert_eq!(seg.record_count, 8);
    }

    #[test]
    fn resume_beyond_durable_gap_reason_round_trips() {
        let gap = OffsetGap {
            reason: OffsetGapReason::ResumeBeyondDurable,
            ..gap(10, 20)
        };
        let json = serde_json::to_string(&gap).unwrap();
        assert!(json.contains("\"resume_beyond_durable\""), "{json}");
        let back: OffsetGap = serde_json::from_str(&json).unwrap();
        assert_eq!(back.reason, OffsetGapReason::ResumeBeyondDurable);
        assert_eq!(
            OffsetGapReason::ResumeBeyondDurable.to_string(),
            "resume_beyond_durable"
        );
    }

    #[test]
    fn missing_topics_round_trip_and_omitted_when_empty() {
        let mut manifest = BackupManifest::new("m".to_string());
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(!json.contains("missing_topics"), "{json}");

        manifest.missing_topics = vec!["ghost".to_string()];
        let json = serde_json::to_string(&manifest).unwrap();
        assert!(json.contains("\"missing_topics\":[\"ghost\"]"), "{json}");
        let back: BackupManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.missing_topics, vec!["ghost".to_string()]);

        // Manifests written before the field existed still load.
        let legacy = r#"{"backup_id":"old","created_at":1,"topics":[]}"#;
        let old: BackupManifest = serde_json::from_str(legacy).unwrap();
        assert!(old.missing_topics.is_empty());
    }

    #[test]
    fn transaction_fields_are_omitted_when_unset_and_absent_in_legacy_manifests() {
        let json = serde_json::to_string(&BackupManifest::new("m".to_string())).unwrap();
        assert!(!json.contains("isolation_level"), "{json}");
        let json = serde_json::to_string(&segment(0, 9)).unwrap();
        assert!(!json.contains("control_records_skipped"), "{json}");
        assert!(!json.contains("aborted_records_dropped"), "{json}");
        let json = serde_json::to_string(&empty_partition()).unwrap();
        for field in [
            "tail_control_records_skipped",
            "tail_aborted_records_dropped",
            "snapshot_end_offset",
            "high_watermark_at_capture",
        ] {
            assert!(!json.contains(field), "{json}");
        }

        // A 0.23 manifest reads with no isolation level (it read
        // uncommitted) and zero counts.
        let legacy = r#"{"backup_id":"old","created_at":1,"topics":[{"name":"t",
            "partitions":[{"partition_id":0,"segments":[{"key":"k","start_offset":0,
            "end_offset":9,"start_timestamp":1,"end_timestamp":2,"record_count":10}]}]}]}"#;
        let old: BackupManifest = serde_json::from_str(legacy).unwrap();
        assert_eq!(old.isolation_level, None);
        let part = &old.topics[0].partitions[0];
        assert_eq!(part.snapshot_end_offset, None);
        assert_eq!(part.high_watermark_at_capture, None);
        assert_eq!(part.control_records_skipped(), 0);
        assert_eq!(part.aborted_records_dropped(), 0);
    }

    #[test]
    fn transaction_fields_round_trip() {
        let mut manifest = BackupManifest::new("m".to_string());
        manifest.isolation_level = Some(IsolationLevel::ReadCommitted);
        let part = manifest.get_or_create_topic("t").get_or_create_partition(0);
        let mut seg = covered(4, 9, 0, 10);
        seg.control_records_skipped = 2;
        seg.aborted_records_dropped = 3;
        part.add_segment(seg);
        part.covered_through = Some(12);
        part.tail_control_records_skipped = 1;
        part.tail_aborted_records_dropped = 1;
        part.snapshot_end_offset = Some(13);
        part.high_watermark_at_capture = Some(20);

        let json = serde_json::to_string(&manifest).unwrap();
        assert!(
            json.contains("\"isolation_level\":\"read_committed\""),
            "{json}"
        );
        let back: BackupManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(back.isolation_level, Some(IsolationLevel::ReadCommitted));
        let part = &back.topics[0].partitions[0];
        assert_eq!(part.control_records_skipped(), 3);
        assert_eq!(part.aborted_records_dropped(), 4);
        assert_eq!(part.snapshot_end_offset, Some(13));
        assert_eq!(part.high_watermark_at_capture, Some(20));
        assert_eq!(back.total_control_records_skipped(), 3);
        assert_eq!(back.total_aborted_records_dropped(), 4);
    }
}
