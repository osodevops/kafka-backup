//! Backup manifest and record structures.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
}

impl PartitionBackup {
    /// Get the last offset backed up for this partition
    pub fn last_offset(&self) -> Option<i64> {
        self.segments.iter().map(|s| s.end_offset).max()
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
}

impl std::fmt::Display for OffsetGapReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OffsetGapReason::OffsetOutOfRange => write!(f, "offset_out_of_range"),
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
}

impl SegmentMetadata {
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

/// Current on-disk format of the offset-mapping report (`format_version`).
///
/// - 1 (implicit, pre-0.24): `detailed_mappings` held one `OffsetPair` per
///   restored record.
/// - 2: `detailed_mappings` holds [`OffsetRuns`] — contiguous runs of
///   records whose source→target mapping is a constant shift. Version-1
///   reports still deserialize (pairs are folded into runs on load).
pub const OFFSET_MAPPING_FORMAT_VERSION: u32 = 2;

fn legacy_offset_mapping_format_version() -> u32 {
    1
}

/// Offset mapping for consumer group reset
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetMapping {
    /// Report format version — see [`OFFSET_MAPPING_FORMAT_VERSION`].
    #[serde(default = "legacy_offset_mapping_format_version")]
    pub format_version: u32,

    /// Mapping entries: topic/partition -> offset info
    pub entries: std::collections::HashMap<String, OffsetMappingEntry>,

    /// Per-record source→target offset mapping, stored as runs (see
    /// [`OffsetRuns`]): memory is proportional to the number of
    /// discontinuities (produce batches that did not land contiguously,
    /// filtered records, resumed segments), not to the number of records.
    /// Key: "topic/partition".
    #[serde(default, deserialize_with = "deserialize_detailed_mappings")]
    pub detailed_mappings: std::collections::HashMap<String, OffsetRuns>,

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

/// A contiguous run of restored records whose source→target mapping is a
/// constant shift: `source_start + i ↦ target_start + i` for `i < len`.
///
/// A normal restore of one partition is a handful of runs — one per stretch
/// of produce batches that landed contiguously on the target — instead of one
/// entry per record (24 bytes × records; ~32 GB at 1.35 billion records, all
/// of it appended under one mutex; issue #197).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OffsetRun {
    /// First source offset covered by the run.
    pub source_start: i64,
    /// Target offset of `source_start`.
    pub target_start: i64,
    /// Number of records in the run (`> 0`).
    pub len: u64,
    /// Earliest record timestamp in the run.
    pub first_timestamp: i64,
    /// Latest record timestamp in the run.
    pub last_timestamp: i64,
}

impl OffsetRun {
    fn from_pair(p: &OffsetPair) -> Self {
        Self {
            source_start: p.source_offset,
            target_start: p.target_offset,
            len: 1,
            first_timestamp: p.timestamp,
            last_timestamp: p.timestamp,
        }
    }

    /// One past the last source offset in the run.
    pub fn source_end(&self) -> i64 {
        self.source_start + self.len as i64
    }

    /// Whether `source` is one of the run's records.
    pub fn contains(&self, source: i64) -> bool {
        source >= self.source_start && source < self.source_end()
    }

    /// Target offset for `source`: exact inside the run, "last record plus
    /// delta" beyond its end (the nearest-lower-record rule the per-record
    /// lookup used).
    pub fn target_for(&self, source: i64) -> i64 {
        self.target_start + (source - self.source_start)
    }

    /// Whether `run` starts exactly where this one ends with the same shift.
    fn can_absorb(&self, run: &OffsetRun) -> bool {
        run.source_start == self.source_end()
            && run.target_start == self.target_start + self.len as i64
    }

    fn absorb(&mut self, run: &OffsetRun) {
        self.len += run.len;
        self.first_timestamp = self.first_timestamp.min(run.first_timestamp);
        self.last_timestamp = self.last_timestamp.max(run.last_timestamp);
    }

    /// The part of the run starting at `source` (which must be inside it).
    fn tail_from(&self, source: i64) -> OffsetRun {
        let skip = source - self.source_start;
        OffsetRun {
            source_start: source,
            target_start: self.target_start + skip,
            len: self.len - skip as u64,
            first_timestamp: self.first_timestamp,
            last_timestamp: self.last_timestamp,
        }
    }
}

/// Sorted, non-overlapping [`OffsetRun`]s for one partition.
///
/// Appending in source-offset order (the restore's normal case) is O(1) and
/// merges into the last run whenever the shift is unchanged. Out-of-order
/// inserts (a record filter mapping a segment's dropped records after its
/// survivors) are placed by binary search. A source offset that is already
/// covered — a segment re-produced after a checkpoint resume — keeps its
/// first mapping.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OffsetRuns {
    runs: Vec<OffsetRun>,
}

impl OffsetRuns {
    /// The runs, sorted by `source_start`, non-overlapping.
    pub fn runs(&self) -> &[OffsetRun] {
        &self.runs
    }

    /// Number of runs (the memory footprint), not records.
    pub fn len(&self) -> usize {
        self.runs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty()
    }

    /// Number of records covered.
    pub fn record_count(&self) -> u64 {
        self.runs.iter().map(|r| r.len).sum()
    }

    /// Add one record mapping.
    pub fn push(&mut self, pair: &OffsetPair) {
        self.push_run(OffsetRun::from_pair(pair));
    }

    /// Add many record mappings (any order).
    pub fn extend<'a>(&mut self, pairs: impl IntoIterator<Item = &'a OffsetPair>) {
        for p in pairs {
            self.push(p);
        }
    }

    /// Merge a whole run, keeping the list sorted and non-overlapping.
    /// Overlap with existing runs is trimmed away (first mapping wins).
    pub fn push_run(&mut self, mut run: OffsetRun) {
        if run.len == 0 {
            return;
        }
        // Fast path: absorb into / append after the tail.
        match self.runs.last_mut() {
            None => {
                self.runs.push(run);
                return;
            }
            Some(last) if last.can_absorb(&run) => {
                last.absorb(&run);
                return;
            }
            Some(last) if run.source_start >= last.source_end() => {
                self.runs.push(run);
                return;
            }
            Some(_) => {}
        }

        // Out-of-order: `idx` = number of runs starting at or before `run`.
        let idx = self
            .runs
            .partition_point(|r| r.source_start <= run.source_start);

        // Trim the front against the previous run (first mapping wins).
        if idx > 0 {
            let prev = self.runs[idx - 1];
            if prev.source_end() > run.source_start {
                if prev.source_end() >= run.source_end() {
                    return; // entirely covered already
                }
                run = run.tail_from(prev.source_end());
            }
        }
        // Split against the next run: keep the part before it, and re-insert
        // whatever extends beyond it (it may span several existing runs).
        let mut rest = None;
        if let Some(next) = self.runs.get(idx).copied() {
            if run.source_end() > next.source_start {
                if run.source_end() > next.source_end() {
                    rest = Some(run.tail_from(next.source_end()));
                }
                // `next.source_start > run.source_start` here (equal starts
                // sort before `idx`), so at least one record is kept.
                run.len = (next.source_start - run.source_start) as u64;
            }
        }
        // Absorb into the previous run if the shift continues, else insert;
        // then close a gap with the next run if the shift continues there.
        if idx > 0 && self.runs[idx - 1].can_absorb(&run) {
            self.runs[idx - 1].absorb(&run);
            if idx < self.runs.len() {
                let next = self.runs[idx];
                if self.runs[idx - 1].can_absorb(&next) {
                    self.runs[idx - 1].absorb(&next);
                    self.runs.remove(idx);
                }
            }
        } else if self
            .runs
            .get(idx)
            .map(|next| run.can_absorb(next))
            .unwrap_or(false)
        {
            let next = self.runs[idx];
            run.absorb(&next);
            self.runs[idx] = run;
        } else {
            self.runs.insert(idx, run);
        }
        if let Some(rest) = rest {
            self.push_run(rest);
        }
    }

    /// Target offset for `source`: exact when a run covers it, otherwise the
    /// nearest lower run's last record plus the delta (the same rule the
    /// per-record mapping used). `None` when no run starts at or below
    /// `source`.
    pub fn lookup(&self, source: i64) -> Option<i64> {
        let idx = self.runs.partition_point(|r| r.source_start <= source);
        idx.checked_sub(1).map(|i| self.runs[i].target_for(source))
    }

    /// Whether `source` is exactly one of the mapped records.
    pub fn contains(&self, source: i64) -> bool {
        let idx = self.runs.partition_point(|r| r.source_start <= source);
        idx.checked_sub(1)
            .map(|i| self.runs[i].contains(source))
            .unwrap_or(false)
    }

    /// `(source, target)` of the first record at or after `timestamp`,
    /// interpolated within a run by timestamp position; falls back to the
    /// last mapped record. Approximate within a run.
    pub fn nearest_by_timestamp(&self, timestamp: i64) -> Option<(i64, i64)> {
        for r in &self.runs {
            if r.last_timestamp >= timestamp {
                if r.first_timestamp >= timestamp || r.len == 1 {
                    return Some((r.source_start, r.target_start));
                }
                // Assume evenly spaced timestamps and round up: "the first
                // record at or after the timestamp".
                let span = (r.last_timestamp - r.first_timestamp).max(1) as f64;
                let pos = ((timestamp - r.first_timestamp) as f64 / span * (r.len - 1) as f64)
                    .ceil()
                    .clamp(0.0, (r.len - 1) as f64) as i64;
                return Some((r.source_start + pos, r.target_start + pos));
            }
        }
        self.runs
            .last()
            .map(|r| (r.source_end() - 1, r.target_for(r.source_end() - 1)))
    }

    /// Expand back into per-record pairs (timestamps are the run's
    /// `first_timestamp`; intended for tests and small reports).
    pub fn iter_pairs(&self) -> impl Iterator<Item = OffsetPair> + '_ {
        self.runs.iter().flat_map(|r| {
            (0..r.len as i64).map(move |i| OffsetPair {
                source_offset: r.source_start + i,
                target_offset: r.target_start + i,
                timestamp: r.first_timestamp,
            })
        })
    }
}

/// Accept both the version-2 run form and the version-1 per-record list.
fn deserialize_detailed_mappings<'de, D>(
    deserializer: D,
) -> std::result::Result<std::collections::HashMap<String, OffsetRuns>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum RunsOrPairs {
        Runs(Vec<OffsetRun>),
        Pairs(Vec<OffsetPair>),
    }
    let raw: std::collections::HashMap<String, RunsOrPairs> =
        Deserialize::deserialize(deserializer)?;
    Ok(raw
        .into_iter()
        .map(|(k, v)| {
            let mut runs = OffsetRuns::default();
            match v {
                RunsOrPairs::Runs(rs) => {
                    for r in rs {
                        runs.push_run(r);
                    }
                }
                RunsOrPairs::Pairs(ps) => runs.extend(ps.iter()),
            }
            (k, runs)
        })
        .collect())
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

impl Default for OffsetMapping {
    fn default() -> Self {
        Self::new()
    }
}

impl OffsetMapping {
    /// Create a new empty offset mapping
    pub fn new() -> Self {
        Self {
            format_version: OFFSET_MAPPING_FORMAT_VERSION,
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
    /// Pairs are folded into [`OffsetRuns`] in any order; appending in offset
    /// order (the common case) extends the last run in place.
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

        self.detailed_mappings
            .entry(key)
            .or_default()
            .extend(pairs.iter());
    }

    /// Merge a partition's runs collected locally by a restore task (one
    /// lock acquisition per partition instead of one per batch). The range
    /// entry is updated from the runs' extremes.
    pub fn merge_runs(&mut self, topic: &str, partition: i32, runs: OffsetRuns) {
        let (Some(first), Some(last)) = (runs.runs().first().copied(), runs.runs().last().copied())
        else {
            return;
        };
        let min_ts = runs
            .runs()
            .iter()
            .map(|r| r.first_timestamp)
            .min()
            .unwrap_or(0);
        let max_ts = runs
            .runs()
            .iter()
            .map(|r| r.last_timestamp)
            .max()
            .unwrap_or(0);
        self.update_range(
            topic,
            partition,
            first.source_start,
            Some(first.target_start),
            min_ts,
        );
        self.update_range(
            topic,
            partition,
            last.source_end() - 1,
            Some(last.target_for(last.source_end() - 1)),
            max_ts,
        );
        let key = format!("{}/{}", topic, partition);
        let entry = self.detailed_mappings.entry(key).or_default();
        if entry.is_empty() {
            *entry = runs;
        } else {
            for r in runs.runs() {
                entry.push_run(*r);
            }
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

    /// Lookup target offset for a given source offset
    /// Uses detailed mappings for exact lookup, falls back to linear interpolation
    pub fn lookup_target_offset(
        &self,
        topic: &str,
        partition: i32,
        source_offset: i64,
    ) -> Option<i64> {
        let key = format!("{}/{}", topic, partition);

        // Exact inside a run; nearest lower record plus delta otherwise.
        if let Some(target) = self
            .detailed_mappings
            .get(&key)
            .and_then(|runs| runs.lookup(source_offset))
        {
            return Some(target);
        }

        // Fall back to range-based interpolation
        if let Some(entry) = self.entries.get(&key) {
            if let (Some(target_first), Some(target_last)) =
                (entry.target_first_offset, entry.target_last_offset)
            {
                // Linear interpolation within the range
                let source_range = entry.source_last_offset - entry.source_first_offset;
                if source_range > 0 {
                    let target_range = target_last - target_first;
                    let position =
                        (source_offset - entry.source_first_offset) as f64 / source_range as f64;
                    return Some(target_first + (position * target_range as f64) as i64);
                } else {
                    return Some(target_first);
                }
            }
        }

        None
    }

    /// Find the nearest offset by timestamp
    pub fn get_nearest_offset_by_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> Option<(i64, i64)> {
        let key = format!("{}/{}", topic, partition);

        self.detailed_mappings
            .get(&key)
            .and_then(|runs| runs.nearest_by_timestamp(timestamp))
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

    /// Number of records with an exact source→target mapping.
    pub fn detailed_mapping_count(&self) -> usize {
        self.detailed_mappings
            .values()
            .map(|v| v.record_count() as usize)
            .sum()
    }

    /// Number of runs held across all partitions (the memory footprint).
    pub fn detailed_run_count(&self) -> usize {
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

        // Offset beyond range extrapolates from nearest (999 - 2 + 5002 = 5999)
        let target = mapping.lookup_target_offset("orders", 0, 999);
        assert_eq!(target, Some(5999));

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
            per_record.detailed_mappings[key].record_count(),
            batched.detailed_mappings[key].record_count()
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
            .iter_pairs()
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
        let offsets: Vec<i64> = detailed.iter_pairs().map(|p| p.source_offset).collect();
        assert_eq!(offsets, vec![10, 11, 12, 13]);
        assert_eq!(detailed.lookup(13), Some(503));
        // Two contiguous batches with the same shift fold into one run.
        assert_eq!(detailed.len(), 1);
    }

    fn run(source_start: i64, target_start: i64, len: u64) -> OffsetRun {
        OffsetRun {
            source_start,
            target_start,
            len,
            first_timestamp: 1_700_000_000_000 + source_start,
            last_timestamp: 1_700_000_000_000 + source_start + len as i64 - 1,
        }
    }

    #[test]
    fn offset_runs_split_only_on_shift_change() {
        let mut runs = OffsetRuns::default();
        // Batch 1 lands at 100.., batch 2 contiguous, batch 3 after a gap on
        // the target (someone else produced in between).
        runs.extend(pairs(&[(0, 100), (1, 101), (2, 102)]).iter());
        runs.extend(pairs(&[(3, 103), (4, 104)]).iter());
        runs.extend(pairs(&[(5, 205), (6, 206)]).iter());
        assert_eq!(runs.len(), 2);
        assert_eq!(runs.record_count(), 7);
        assert_eq!(runs.runs()[0], run(0, 100, 5));
        assert_eq!(runs.runs()[1], run(5, 205, 2));
        // Exact inside; nearest-lower-plus-delta beyond the end; none before.
        assert_eq!(runs.lookup(4), Some(104));
        assert_eq!(runs.lookup(6), Some(206));
        assert_eq!(runs.lookup(9), Some(209));
        assert_eq!(runs.lookup(-1), None);
        assert!(runs.contains(4) && !runs.contains(9));
    }

    #[test]
    fn offset_runs_out_of_order_insert_keeps_sorted_and_merges() {
        let mut runs = OffsetRuns::default();
        runs.extend(pairs(&[(10, 500), (12, 502)]).iter());
        // The missing middle record arrives later with the same shift: the
        // three fold into one run.
        runs.push(&pairs(&[(11, 501)])[0]);
        assert_eq!(runs.runs(), &[run(10, 500, 3)]);
        // A record before everything with a different shift is inserted at
        // the front.
        runs.push(&pairs(&[(3, 90)])[0]);
        assert_eq!(runs.runs(), &[run(3, 90, 1), run(10, 500, 3)]);
    }

    #[test]
    fn offset_runs_duplicate_source_keeps_first_mapping() {
        // A segment re-produced after a checkpoint resume maps the same
        // source offsets again, now at higher target offsets.
        let mut runs = OffsetRuns::default();
        runs.extend(pairs(&[(0, 100), (1, 101), (2, 102)]).iter());
        runs.extend(pairs(&[(1, 201), (2, 202), (3, 203)]).iter());
        assert_eq!(runs.lookup(1), Some(101));
        assert_eq!(runs.lookup(2), Some(102));
        assert_eq!(runs.lookup(3), Some(203));
        assert_eq!(runs.record_count(), 4);
        // Whole-run overlap on both sides is trimmed too.
        runs.push_run(run(-2, 50, 8)); // covers -2..6, overlaps 0..4
        assert_eq!(runs.lookup(-2), Some(50));
        assert_eq!(runs.lookup(0), Some(100));
        assert_eq!(runs.lookup(3), Some(203));
        assert_eq!(runs.lookup(4), Some(56));
        assert_eq!(runs.record_count(), 8);
    }

    #[test]
    fn offset_runs_lookup_matches_per_record_semantics() {
        // Random discontinuous data: compare with the old per-record rule
        // (exact match, else nearest lower record plus delta).
        let mut seed = 0x9e37_79b9_u64;
        let mut next = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let mut src = 0i64;
        let mut tgt = 1_000i64;
        let mut list = Vec::new();
        for _ in 0..2_000 {
            src += 1 + (next() % 3) as i64; // gaps in source offsets
            tgt += 1 + if next() % 10 == 0 {
                (next() % 50) as i64
            } else {
                0
            };
            list.push((src, tgt));
        }
        let mut runs = OffsetRuns::default();
        runs.extend(pairs(&list).iter());
        assert!(
            runs.len() < list.len(),
            "runs={} records={}",
            runs.len(),
            list.len()
        );
        for q in -5..src + 5 {
            let expected = list
                .iter()
                .filter(|(s, _)| *s <= q)
                .max_by_key(|(s, _)| *s)
                .map(|(s, t)| t + (q - s));
            assert_eq!(runs.lookup(q), expected, "source {q}");
        }
    }

    #[test]
    fn offset_runs_ten_million_sequential_records_are_one_run() {
        let mut runs = OffsetRuns::default();
        for i in 0..10_000_000i64 {
            runs.push(&OffsetPair {
                source_offset: i,
                target_offset: 5_000 + i,
                timestamp: 1_700_000_000_000 + i,
            });
        }
        assert_eq!(runs.len(), 1);
        assert_eq!(runs.record_count(), 10_000_000);
        assert_eq!(runs.lookup(9_999_999), Some(5_000 + 9_999_999));
        assert_eq!(runs.runs()[0].last_timestamp, 1_700_000_000_000 + 9_999_999);
    }

    #[test]
    fn offset_mapping_report_round_trips_and_reads_legacy_pairs() {
        let mut mapping = OffsetMapping::new();
        mapping.add_detailed_batch("orders", 0, pairs(&[(0, 100), (1, 101), (5, 200)]));
        let json = serde_json::to_string(&mapping).unwrap();
        assert!(json.contains("\"format_version\":2"));
        assert!(json.contains("\"source_start\":0"));
        let back: OffsetMapping = serde_json::from_str(&json).unwrap();
        assert_eq!(back.format_version, OFFSET_MAPPING_FORMAT_VERSION);
        assert_eq!(
            back.detailed_mappings["orders/0"],
            mapping.detailed_mappings["orders/0"]
        );
        assert_eq!(back.lookup_target_offset("orders", 0, 1), Some(101));

        // A version-1 report (one pair per record, no format_version) still
        // loads, folded into runs.
        let legacy = r#"{
            "entries": {},
            "detailed_mappings": {
                "orders/0": [
                    {"source_offset": 0, "target_offset": 100, "timestamp": 1},
                    {"source_offset": 1, "target_offset": 101, "timestamp": 2},
                    {"source_offset": 5, "target_offset": 200, "timestamp": 3}
                ]
            }
        }"#;
        let old: OffsetMapping = serde_json::from_str(legacy).unwrap();
        assert_eq!(old.format_version, 1);
        assert_eq!(old.detailed_mappings["orders/0"].len(), 2);
        assert_eq!(old.detailed_mapping_count(), 3);
        assert_eq!(old.lookup_target_offset("orders", 0, 5), Some(200));
        assert_eq!(old.lookup_target_offset("orders", 0, 3), Some(103));
    }

    #[test]
    fn merge_runs_updates_range_and_folds_into_existing() {
        let mut mapping = OffsetMapping::new();
        let mut a = OffsetRuns::default();
        a.extend(pairs(&[(0, 100), (1, 101)]).iter());
        mapping.merge_runs("orders", 0, a);
        let mut b = OffsetRuns::default();
        b.extend(pairs(&[(2, 102), (3, 103)]).iter());
        mapping.merge_runs("orders", 0, b);
        assert_eq!(mapping.detailed_mappings["orders/0"].len(), 1);
        assert_eq!(mapping.detailed_mapping_count(), 4);
        let e = &mapping.entries["orders/0"];
        assert_eq!((e.source_first_offset, e.source_last_offset), (0, 3));
        assert_eq!(
            (e.target_first_offset, e.target_last_offset),
            (Some(100), Some(103))
        );
        assert_eq!(e.first_timestamp, 1_700_000_000_000);
        assert_eq!(e.last_timestamp, 1_700_000_000_003);
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
}
