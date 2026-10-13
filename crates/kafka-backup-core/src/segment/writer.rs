//! Segment writer with mini-batching support.

use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;
use tracing::info;

use super::format::{BinaryRecord, SegmentHeader, FOOTER_SIZE, HEADER_SIZE, MAGIC_END};
use crate::compression;
use crate::config::CompressionType;
use crate::manifest::SegmentMetadata;
use crate::metrics::{PerformanceMetrics, PrometheusMetrics, StorageOperation};
use crate::storage::StorageBackend;
use crate::Result;

/// Configuration for segment writer
#[derive(Debug, Clone)]
pub struct SegmentWriterConfig {
    /// Maximum segment size before rotation (default: 128MB)
    pub max_segment_bytes: u64,
    /// Maximum time before segment rotation in ms (default: 60000)
    pub max_segment_interval_ms: u64,
    /// Compression type
    pub compression: CompressionType,
    /// Compression level (for zstd: 1-22, default 3)
    pub compression_level: i32,
    /// Maximum records per segment before rotation (default: unlimited)
    pub max_segment_records: Option<u64>,
}

impl Default for SegmentWriterConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 128 * 1024 * 1024, // 128MB
            max_segment_interval_ms: 60_000,      // 60 seconds
            compression: CompressionType::Zstd,
            compression_level: 3,
            max_segment_records: None,
        }
    }
}

/// A full segment taken out of a [`SegmentWriter`], ready to be compressed
/// and uploaded independently of the writer (e.g. from a spawned task).
pub struct SealedSegment {
    key: String,
    buffer: BytesMut,
    record_count: u64,
    start_offset: i64,
    end_offset: i64,
    start_timestamp: i64,
    end_timestamp: i64,
    covered_start: i64,
    covered_end: i64,
}

impl SealedSegment {
    /// Storage key the segment will be written to.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Source offsets this segment accounts for, inclusive (see
    /// [`SegmentMetadata::covered_start_offset`]).
    pub fn covered(&self) -> (i64, i64) {
        (self.covered_start, self.covered_end)
    }
}

/// Compresses and uploads [`SealedSegment`]s. Cheap to clone; holds only
/// shared handles, so it can be moved into spawned tasks while the
/// originating [`SegmentWriter`] keeps accumulating records.
#[derive(Clone)]
pub struct SegmentFlusher {
    config: SegmentWriterConfig,
    storage: Arc<dyn StorageBackend>,
    metrics: Arc<PerformanceMetrics>,
    prometheus_metrics: Option<Arc<PrometheusMetrics>>,
    backup_id: String,
}

impl SegmentFlusher {
    /// Compress a sealed segment and write it to storage.
    pub async fn write(&self, sealed: SealedSegment) -> Result<SegmentMetadata> {
        let start = std::time::Instant::now();

        // Build header
        let header = SegmentHeader {
            version: super::format::VERSION,
            compression: self.config.compression,
            record_count: sealed.record_count,
            start_offset: sealed.start_offset,
            end_offset: sealed.end_offset,
        };

        // Compress record data off the async runtime (zstd on a 128MB
        // segment takes hundreds of milliseconds of pure CPU)
        let uncompressed_size = sealed.buffer.len();
        let compression = self.config.compression;
        let compression_level = self.config.compression_level;
        let buffer = sealed.buffer;
        let compressed_data = tokio::task::spawn_blocking(move || {
            compression::compress_with_level(&buffer, compression, compression_level)
        })
        .await
        .map_err(|e| crate::Error::Compression(format!("compression task failed: {e}")))??;

        // Build final segment
        let mut segment =
            BytesMut::with_capacity(HEADER_SIZE + compressed_data.len() + FOOTER_SIZE);
        segment.extend_from_slice(&header.to_bytes());
        segment.extend_from_slice(&compressed_data);

        // Calculate CRC and add footer
        let crc = super::format::crc32(&segment);
        segment.put_u32_le(crc);
        segment.extend_from_slice(&MAGIC_END);

        let compressed_size = segment.len();

        // Digest of the exact bytes stored, for manifest-level integrity
        // (verified by `validate --deep`) and safe retention bookkeeping.
        let sha256 = {
            use sha2::{Digest, Sha256};
            let digest = Sha256::digest(&segment);
            digest
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()
        };
        let uploaded_at = chrono::Utc::now().timestamp_millis();

        // Write to storage
        self.storage.put(&sealed.key, Bytes::from(segment)).await?;
        failpoint!("backup::segment_put::after");

        // Update legacy metrics
        self.metrics
            .record_bytes(compressed_size as u64, uncompressed_size as u64);
        self.metrics.record_records(sealed.record_count);
        self.metrics.record_segment();
        self.metrics.record_segment_write_latency(start.elapsed());

        // Update Prometheus metrics
        if let Some(ref prom) = self.prometheus_metrics {
            let algorithm = format!("{:?}", self.config.compression).to_lowercase();
            prom.record_compression(
                &algorithm,
                &self.backup_id,
                compressed_size as u64,
                uncompressed_size as u64,
            );
            let backend = self.storage.backend_name();
            prom.record_storage_write_latency(
                backend,
                StorageOperation::Segment,
                start.elapsed().as_secs_f64(),
            );
            prom.inc_storage_write_bytes(backend, &self.backup_id, compressed_size as u64);
        }

        // Build metadata
        let metadata = SegmentMetadata {
            key: sealed.key.clone(),
            start_offset: sealed.start_offset,
            end_offset: sealed.end_offset,
            start_timestamp: sealed.start_timestamp,
            end_timestamp: sealed.end_timestamp,
            record_count: sealed.record_count as i64,
            uncompressed_size: uncompressed_size as u64,
            compressed_size: compressed_size as u64,
            sha256,
            uploaded_at,
            covered_start_offset: Some(sealed.covered_start),
            covered_end_offset: Some(sealed.covered_end),
        };

        info!(
            "Wrote segment {} with {} records ({} -> {} bytes, {:.1}x compression)",
            sealed.key,
            sealed.record_count,
            uncompressed_size,
            compressed_size,
            uncompressed_size as f64 / compressed_size as f64
        );

        Ok(metadata)
    }
}

/// Segment writer that accumulates records and writes segments to storage
pub struct SegmentWriter {
    config: SegmentWriterConfig,
    storage: Arc<dyn StorageBackend>,
    metrics: Arc<PerformanceMetrics>,
    prometheus_metrics: Option<Arc<PrometheusMetrics>>,
    backup_id: String,

    /// Current buffer for accumulating records
    buffer: BytesMut,
    /// Number of records in current buffer
    record_count: u64,
    /// Start offset of current segment
    start_offset: Option<i64>,
    /// End offset of current segment
    end_offset: Option<i64>,
    /// Start timestamp of current segment
    start_timestamp: Option<i64>,
    /// End timestamp of current segment
    end_timestamp: Option<i64>,
    /// When the current segment was started
    segment_start_time: Option<std::time::Instant>,
    /// Uncompressed size counter
    uncompressed_bytes: u64,
    /// First source offset not yet accounted for by a sealed segment.
    coverage_start: Option<i64>,
    /// Highest source offset passed so far (records added or fetch position
    /// advanced via [`extend_coverage`](SegmentWriter::extend_coverage)).
    coverage_end: Option<i64>,
}

impl SegmentWriter {
    /// Create a new segment writer
    pub fn new(
        config: SegmentWriterConfig,
        storage: Arc<dyn StorageBackend>,
        metrics: Arc<PerformanceMetrics>,
    ) -> Self {
        Self::with_prometheus(config, storage, metrics, None, String::new())
    }

    /// Create a new segment writer with Prometheus metrics
    pub fn with_prometheus(
        config: SegmentWriterConfig,
        storage: Arc<dyn StorageBackend>,
        metrics: Arc<PerformanceMetrics>,
        prometheus_metrics: Option<Arc<PrometheusMetrics>>,
        backup_id: String,
    ) -> Self {
        Self {
            config,
            storage,
            metrics,
            prometheus_metrics,
            backup_id,
            buffer: BytesMut::with_capacity(1024 * 1024), // 1MB initial capacity
            record_count: 0,
            start_offset: None,
            end_offset: None,
            start_timestamp: None,
            end_timestamp: None,
            segment_start_time: None,
            uncompressed_bytes: 0,
            coverage_start: None,
            coverage_end: None,
        }
    }

    /// Start tracking the source offsets the next segments account for.
    ///
    /// `first_uncovered` is the first offset no listed interval accounts for
    /// yet; `covered_through` the highest offset already accounted for (at
    /// least `first_uncovered - 1`; higher when a coverage-only advance
    /// followed the last segment). Without this call coverage starts at the
    /// first record added.
    pub fn begin_coverage(&mut self, first_uncovered: i64, covered_through: i64) {
        self.coverage_start = Some(first_uncovered);
        self.coverage_end = Some(covered_through.max(first_uncovered - 1));
    }

    /// Record that the fetch position has passed `last_offset` (inclusive):
    /// every offset up to it has either been added or held no record
    /// (compacted away, a transaction marker). Call after adding a fetch's
    /// records, never before. Ignored until coverage has started.
    pub fn extend_coverage(&mut self, last_offset: i64) {
        if self.coverage_start.is_some() {
            self.coverage_end = Some(
                self.coverage_end
                    .map_or(last_offset, |e| e.max(last_offset)),
            );
        }
    }

    /// First source offset the next sealed segment will account for.
    pub fn coverage_start(&self) -> Option<i64> {
        self.coverage_start
    }

    /// When no record is buffered but the fetch position has advanced past
    /// the last sealed segment, the inclusive range covered since — a
    /// coverage-only advance. Not consumed: the next sealed segment's covered
    /// range still starts at the same offset, keeping intervals contiguous.
    pub fn uncovered_tail(&self) -> Option<(i64, i64)> {
        if self.record_count > 0 {
            return None;
        }
        match (self.coverage_start, self.coverage_end) {
            (Some(start), Some(end)) if end >= start => Some((start, end)),
            _ => None,
        }
    }

    /// Move coverage past a range accounted for elsewhere (a recorded gap
    /// ending before `offset`). Only meaningful with no records buffered.
    pub fn skip_coverage_to(&mut self, offset: i64) {
        debug_assert_eq!(
            self.record_count, 0,
            "skip_coverage_to with buffered records"
        );
        self.coverage_start = Some(offset);
        self.coverage_end = Some(offset - 1);
    }

    /// Add a record to the current segment
    pub fn add_record(&mut self, record: BinaryRecord) -> Result<()> {
        if self.coverage_start.is_none() {
            self.coverage_start = Some(record.offset);
        }
        self.coverage_end = Some(
            self.coverage_end
                .map_or(record.offset, |e| e.max(record.offset)),
        );

        // Update metadata
        if self.start_offset.is_none() {
            self.start_offset = Some(record.offset);
            self.start_timestamp = Some(record.timestamp);
            self.segment_start_time = Some(std::time::Instant::now());
        }
        self.end_offset = Some(record.offset);
        self.end_timestamp = Some(record.timestamp);

        // Serialize and append
        let record_bytes = record.to_bytes();
        self.uncompressed_bytes += record_bytes.len() as u64;
        self.buffer.extend_from_slice(&record_bytes);
        self.record_count += 1;

        Ok(())
    }

    /// Check if the current segment should be rotated
    pub fn should_rotate(&self) -> bool {
        // Check size threshold
        if self.buffer.len() as u64 >= self.config.max_segment_bytes {
            return true;
        }

        // Check record-count threshold
        if let Some(max_records) = self.config.max_segment_records {
            if self.record_count >= max_records {
                return true;
            }
        }

        // Check time threshold
        if let Some(start_time) = self.segment_start_time {
            if start_time.elapsed().as_millis() as u64 >= self.config.max_segment_interval_ms {
                return true;
            }
        }

        false
    }

    /// Check if there are any buffered records
    pub fn has_data(&self) -> bool {
        self.record_count > 0
    }

    /// Get the current buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Get the current record count
    pub fn current_record_count(&self) -> u64 {
        self.record_count
    }

    /// Get the start offset of the current segment (first record's offset).
    /// Returns `None` if no records have been added yet.
    pub fn start_offset(&self) -> Option<i64> {
        self.start_offset
    }

    /// Get a flusher that can compress/upload sealed segments independently
    /// of this writer (e.g. from a spawned task).
    pub fn flusher(&self) -> SegmentFlusher {
        SegmentFlusher {
            config: self.config.clone(),
            storage: self.storage.clone(),
            metrics: self.metrics.clone(),
            prometheus_metrics: self.prometheus_metrics.clone(),
            backup_id: self.backup_id.clone(),
        }
    }

    /// Take the current segment out of the writer and reset it, so the
    /// segment can be compressed/uploaded while new records accumulate.
    /// Returns `None` if no records are buffered.
    pub fn seal(&mut self, key: &str) -> Option<SealedSegment> {
        if self.record_count == 0 {
            return None;
        }

        let start_offset = self.start_offset.unwrap_or(-1);
        let end_offset = self.end_offset.unwrap_or(-1);
        // Coverage reaches from the first unaccounted offset to the fetch
        // position, and never less than the records it holds.
        let covered_start = self
            .coverage_start
            .map_or(start_offset, |c| c.min(start_offset));
        let covered_end = self.coverage_end.map_or(end_offset, |c| c.max(end_offset));
        let sealed = SealedSegment {
            key: key.to_string(),
            buffer: std::mem::replace(&mut self.buffer, BytesMut::with_capacity(1024 * 1024)),
            record_count: self.record_count,
            start_offset,
            end_offset,
            start_timestamp: self.start_timestamp.unwrap_or(0),
            end_timestamp: self.end_timestamp.unwrap_or(0),
            covered_start,
            covered_end,
        };
        self.reset();
        self.coverage_start = Some(covered_end + 1);
        self.coverage_end = Some(covered_end);
        Some(sealed)
    }

    /// Flush the current segment to storage
    pub async fn flush(&mut self, key: &str) -> Result<Option<SegmentMetadata>> {
        match self.seal(key) {
            Some(sealed) => {
                let metadata = self.flusher().write(sealed).await?;
                Ok(Some(metadata))
            }
            None => Ok(None),
        }
    }

    /// Reset the buffered segment without writing (coverage tracking is
    /// kept).
    pub fn reset(&mut self) {
        self.buffer.clear();
        self.record_count = 0;
        self.start_offset = None;
        self.end_offset = None;
        self.start_timestamp = None;
        self.end_timestamp = None;
        self.segment_start_time = None;
        self.uncompressed_bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::FilesystemBackend;
    use tempfile::TempDir;

    #[tokio::test]
    async fn no_record_rotation_when_limit_unset() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(FilesystemBackend::new(temp_dir.path().to_path_buf()));
        let metrics = Arc::new(PerformanceMetrics::new());
        // Defaults: max_segment_records is None — record count must never
        // trigger rotation on its own (pre-v0.16.0 behavior preserved).
        let config = SegmentWriterConfig {
            max_segment_bytes: u64::MAX,
            max_segment_interval_ms: u64::MAX,
            ..SegmentWriterConfig::default()
        };
        let mut writer = SegmentWriter::new(config, storage, metrics);
        for i in 0..1000 {
            writer
                .add_record(BinaryRecord {
                    timestamp: 1000 + i,
                    offset: i,
                    key: None,
                    value: Some(Bytes::from("v")),
                    headers: vec![],
                })
                .unwrap();
        }
        assert!(
            !writer.should_rotate(),
            "record count must not trigger rotation when the limit is unset"
        );
    }

    #[tokio::test]
    async fn rotates_at_max_segment_records() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(FilesystemBackend::new(temp_dir.path().to_path_buf()));
        let metrics = Arc::new(PerformanceMetrics::new());
        let config = SegmentWriterConfig {
            max_segment_bytes: u64::MAX,
            max_segment_interval_ms: u64::MAX,
            max_segment_records: Some(10),
            ..SegmentWriterConfig::default()
        };

        let mut writer = SegmentWriter::new(config, storage, metrics);
        for i in 0..9 {
            writer
                .add_record(BinaryRecord {
                    timestamp: 1000 + i,
                    offset: i,
                    key: None,
                    value: Some(Bytes::from("v")),
                    headers: vec![],
                })
                .unwrap();
        }
        assert!(
            !writer.should_rotate(),
            "must not rotate below the record limit"
        );
        writer
            .add_record(BinaryRecord {
                timestamp: 2000,
                offset: 9,
                key: None,
                value: Some(Bytes::from("v")),
                headers: vec![],
            })
            .unwrap();
        assert!(writer.should_rotate(), "must rotate at the record limit");
    }

    #[tokio::test]
    async fn test_segment_writer_basic() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(FilesystemBackend::new(temp_dir.path().to_path_buf()));
        let metrics = Arc::new(PerformanceMetrics::new());
        let config = SegmentWriterConfig::default();

        let mut writer = SegmentWriter::new(config, storage.clone(), metrics);

        // Add some records
        for i in 0..100 {
            let record = BinaryRecord {
                timestamp: 1000 + i,
                offset: i,
                key: Some(Bytes::from(format!("key-{}", i))),
                value: Some(Bytes::from(format!("value-{}", i))),
                headers: vec![],
            };
            writer.add_record(record).unwrap();
        }

        assert_eq!(writer.current_record_count(), 100);
        assert!(writer.has_data());

        // Flush
        let metadata = writer.flush("test-segment.bin").await.unwrap().unwrap();
        assert_eq!(metadata.record_count, 100);
        assert_eq!(metadata.start_offset, 0);
        assert_eq!(metadata.end_offset, 99);

        // Verify file exists
        assert!(storage.exists("test-segment.bin").await.unwrap());
    }

    fn rec(offset: i64) -> BinaryRecord {
        BinaryRecord {
            timestamp: 1_000 + offset,
            offset,
            key: None,
            value: Some(Bytes::from("v")),
            headers: vec![],
        }
    }

    fn memory_writer() -> SegmentWriter {
        SegmentWriter::new(
            SegmentWriterConfig::default(),
            Arc::new(crate::storage::MemoryBackend::new()),
            Arc::new(PerformanceMetrics::new()),
        )
    }

    #[tokio::test]
    async fn covered_range_defaults_to_the_record_range() {
        let mut writer = memory_writer();
        for o in 5..10 {
            writer.add_record(rec(o)).unwrap();
        }
        let meta = writer.flush("k").await.unwrap().unwrap();
        assert_eq!((meta.start_offset, meta.end_offset), (5, 9));
        assert_eq!(meta.covered_start_offset, Some(5));
        assert_eq!(meta.covered_end_offset, Some(9));
    }

    #[tokio::test]
    async fn covered_range_spans_holes_before_and_after_the_records() {
        let mut writer = memory_writer();
        // Fetch from 100: 100..=104 were compacted away, 150..=159 are
        // markers; the fetch position afterwards is 160.
        writer.begin_coverage(100, 99);
        for o in 105..150 {
            writer.add_record(rec(o)).unwrap();
        }
        writer.extend_coverage(159);
        let meta = writer.flush("k").await.unwrap().unwrap();
        assert_eq!((meta.start_offset, meta.end_offset), (105, 149));
        assert_eq!(
            (meta.covered_start_offset, meta.covered_end_offset),
            (Some(100), Some(159))
        );
        // The next segment starts right after.
        assert_eq!(writer.coverage_start(), Some(160));
    }

    #[test]
    fn rotation_mid_batch_splits_coverage_at_the_last_record() {
        let mut writer = memory_writer();
        writer.begin_coverage(0, -1);
        for o in 0..10 {
            writer.add_record(rec(o)).unwrap();
        }
        // Rotation inside the batch: the rest of the batch goes to the next
        // segment, so this one covers only up to its last record.
        let first = writer.seal("a").unwrap();
        for o in 10..20 {
            writer.add_record(rec(o)).unwrap();
        }
        writer.extend_coverage(24);
        let second = writer.seal("b").unwrap();
        assert_eq!(first.covered(), (0, 9));
        assert_eq!(second.covered(), (10, 24));
    }

    #[test]
    fn uncovered_tail_is_reported_but_absorbed_by_the_next_segment() {
        let mut writer = memory_writer();
        writer.begin_coverage(0, -1);
        writer.add_record(rec(0)).unwrap();
        assert_eq!(writer.uncovered_tail(), None, "records are buffered");
        let _ = writer.seal("a").unwrap();
        assert_eq!(writer.uncovered_tail(), None, "nothing past the seal yet");

        // An all-compacted fetch advances the position with no record.
        writer.extend_coverage(30);
        assert_eq!(writer.uncovered_tail(), Some((1, 30)));
        // Reporting does not consume: the next segment still covers it, so
        // the manifest's intervals stay contiguous.
        assert_eq!(writer.uncovered_tail(), Some((1, 30)));
        writer.add_record(rec(31)).unwrap();
        assert_eq!(writer.uncovered_tail(), None);
        assert_eq!(writer.seal("b").unwrap().covered(), (1, 31));
    }

    #[test]
    fn resumed_coverage_can_start_below_the_fetch_position() {
        // The previous run ended on a coverage-only advance through 59 after
        // its last segment (..=49): resume fetching at 60, but the first new
        // segment's covered range starts at 50.
        let mut writer = memory_writer();
        writer.begin_coverage(50, 59);
        assert_eq!(writer.uncovered_tail(), Some((50, 59)));
        writer.add_record(rec(60)).unwrap();
        assert_eq!(writer.seal("a").unwrap().covered(), (50, 60));
    }

    #[test]
    fn skip_coverage_moves_past_a_gap() {
        let mut writer = memory_writer();
        writer.begin_coverage(10, 9);
        writer.extend_coverage(14);
        // [10, 15) was compacted; [15, 40) is a retention gap recorded
        // separately starting at coverage_start.
        assert_eq!(writer.coverage_start(), Some(10));
        writer.skip_coverage_to(40);
        assert_eq!(writer.uncovered_tail(), None);
        writer.add_record(rec(40)).unwrap();
        assert_eq!(writer.seal("a").unwrap().covered(), (40, 40));
    }

    #[tokio::test]
    async fn test_segment_flush_labels_actual_backend() {
        use crate::metrics::PrometheusMetrics;
        use crate::storage::MemoryBackend;

        let storage = Arc::new(MemoryBackend::new());
        let metrics = Arc::new(PerformanceMetrics::new());
        let prometheus = Arc::new(PrometheusMetrics::new());
        let config = SegmentWriterConfig::default();

        let mut writer = SegmentWriter::with_prometheus(
            config,
            storage,
            metrics,
            Some(prometheus.clone()),
            "backend-label-test".to_string(),
        );

        writer
            .add_record(BinaryRecord {
                timestamp: 1000,
                offset: 0,
                key: None,
                value: Some(Bytes::from("value")),
                headers: vec![],
            })
            .unwrap();
        writer.flush("test-segment.bin").await.unwrap().unwrap();

        // The storage metrics must be labelled with the backend that was
        // actually written to, not a hardcoded "filesystem" (operator issue #50).
        let encoded = prometheus.encode();
        assert!(encoded.contains("kafka_backup_storage_write_bytes_total{backend=\"memory\",backup_id=\"backend-label-test\"}"));
        assert!(!encoded.contains("backend=\"filesystem\""));
    }

    #[tokio::test]
    async fn test_segment_writer_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(FilesystemBackend::new(temp_dir.path().to_path_buf()));
        let metrics = Arc::new(PerformanceMetrics::new());

        // Small segment size for testing
        let config = SegmentWriterConfig {
            max_segment_bytes: 1024,
            ..Default::default()
        };

        let mut writer = SegmentWriter::new(config, storage, metrics);

        // Add records until rotation needed
        let mut count = 0;
        while !writer.should_rotate() {
            let record = BinaryRecord {
                timestamp: 1000 + count,
                offset: count,
                key: Some(Bytes::from(format!("key-{}", count))),
                value: Some(Bytes::from("a".repeat(100))),
                headers: vec![],
            };
            writer.add_record(record).unwrap();
            count += 1;
        }

        assert!(writer.should_rotate());
        assert!(count < 100); // Should rotate before 100 records with small segments
    }
}
