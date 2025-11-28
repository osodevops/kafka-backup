//! Backup manifest and record structures.

use serde::{Deserialize, Serialize};

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
        }
    }

    /// Get or create a topic backup entry
    pub fn get_or_create_topic(&mut self, name: &str) -> &mut TopicBackup {
        if !self.topics.iter().any(|t| t.name == name) {
            self.topics.push(TopicBackup {
                name: name.to_string(),
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
}

/// Per-topic backup metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicBackup {
    /// Topic name
    pub name: String,

    /// Partitions in this topic
    pub partitions: Vec<PartitionBackup>,
}

impl TopicBackup {
    /// Get or create a partition backup entry
    pub fn get_or_create_partition(&mut self, partition_id: i32) -> &mut PartitionBackup {
        if !self.partitions.iter().any(|p| p.partition_id == partition_id) {
            self.partitions.push(PartitionBackup {
                partition_id,
                segments: Vec::new(),
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordHeader {
    /// Header key
    pub key: String,

    /// Header value
    #[serde(with = "bytes_base64")]
    pub value: Vec<u8>,
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

/// Serde helper for byte arrays (base64 encoded)
mod bytes_base64 {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(value: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(value))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        STANDARD.decode(&s).map_err(serde::de::Error::custom)
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
    pub offsets: std::collections::HashMap<String, std::collections::HashMap<i32, ConsumerGroupOffset>>,
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
        let key = format!("{}/{}", topic, partition);

        // Update range mapping
        self.update_range(topic, partition, source_offset, Some(target_offset), timestamp);

        // Add detailed mapping
        self.detailed_mappings
            .entry(key)
            .or_insert_with(Vec::new)
            .push(OffsetPair {
                source_offset,
                target_offset,
                timestamp,
            });
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
            if source_offset < entry.source_first_offset {
                entry.source_first_offset = source_offset;
                entry.target_first_offset = target_offset;
                entry.first_timestamp = timestamp;
            }
            if source_offset > entry.source_last_offset {
                entry.source_last_offset = source_offset;
                entry.target_last_offset = target_offset;
                entry.last_timestamp = timestamp;
            }
        } else {
            self.add(topic, partition, source_offset, target_offset, timestamp);
        }
    }

    /// Lookup target offset for a given source offset
    /// Uses detailed mappings for exact lookup, falls back to linear interpolation
    pub fn lookup_target_offset(&self, topic: &str, partition: i32, source_offset: i64) -> Option<i64> {
        let key = format!("{}/{}", topic, partition);

        // First try detailed mapping for exact match
        if let Some(detailed) = self.detailed_mappings.get(&key) {
            // Binary search for the offset
            if let Ok(idx) = detailed.binary_search_by_key(&source_offset, |p| p.source_offset) {
                return Some(detailed[idx].target_offset);
            }

            // Find nearest offset that's <= source_offset
            let nearest = detailed
                .iter()
                .filter(|p| p.source_offset <= source_offset)
                .max_by_key(|p| p.source_offset);

            if let Some(nearest) = nearest {
                // Calculate offset delta
                let delta = source_offset - nearest.source_offset;
                return Some(nearest.target_offset + delta);
            }
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

    /// First timestamp in range
    pub first_timestamp: i64,

    /// Last timestamp in range
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
}

/// Per-topic dry-run report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DryRunTopicReport {
    /// Source topic name
    pub source_topic: String,

    /// Target topic name
    pub target_topic: String,

    /// Partitions to restore
    pub partitions: Vec<DryRunPartitionReport>,
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
}
