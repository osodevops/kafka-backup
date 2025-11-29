//! Segment writer with mini-batching support.

use bytes::{BufMut, Bytes, BytesMut};
use std::sync::Arc;
use tracing::info;

use super::format::{BinaryRecord, SegmentHeader, FOOTER_SIZE, HEADER_SIZE, MAGIC_END};
use crate::compression;
use crate::config::CompressionType;
use crate::manifest::SegmentMetadata;
use crate::metrics::PerformanceMetrics;
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
}

impl Default for SegmentWriterConfig {
    fn default() -> Self {
        Self {
            max_segment_bytes: 128 * 1024 * 1024, // 128MB
            max_segment_interval_ms: 60_000,      // 60 seconds
            compression: CompressionType::Zstd,
            compression_level: 3,
        }
    }
}

/// Segment writer that accumulates records and writes segments to storage
pub struct SegmentWriter {
    config: SegmentWriterConfig,
    storage: Arc<dyn StorageBackend>,
    metrics: Arc<PerformanceMetrics>,

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
}

impl SegmentWriter {
    /// Create a new segment writer
    pub fn new(
        config: SegmentWriterConfig,
        storage: Arc<dyn StorageBackend>,
        metrics: Arc<PerformanceMetrics>,
    ) -> Self {
        Self {
            config,
            storage,
            metrics,
            buffer: BytesMut::with_capacity(1024 * 1024), // 1MB initial capacity
            record_count: 0,
            start_offset: None,
            end_offset: None,
            start_timestamp: None,
            end_timestamp: None,
            segment_start_time: None,
            uncompressed_bytes: 0,
        }
    }

    /// Add a record to the current segment
    pub fn add_record(&mut self, record: BinaryRecord) -> Result<()> {
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

    /// Flush the current segment to storage
    pub async fn flush(&mut self, key: &str) -> Result<Option<SegmentMetadata>> {
        if self.record_count == 0 {
            return Ok(None);
        }

        let start = std::time::Instant::now();

        // Build header
        let header = SegmentHeader {
            version: super::format::VERSION,
            compression: self.config.compression,
            record_count: self.record_count,
            start_offset: self.start_offset.unwrap_or(-1),
            end_offset: self.end_offset.unwrap_or(-1),
        };

        // Compress record data
        let uncompressed_size = self.buffer.len();
        let compressed_data = compression::compress_with_level(
            &self.buffer,
            self.config.compression,
            self.config.compression_level,
        )?;

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

        // Write to storage
        self.storage.put(key, Bytes::from(segment)).await?;

        // Update metrics
        self.metrics
            .record_bytes(compressed_size as u64, uncompressed_size as u64);
        self.metrics.record_records(self.record_count);
        self.metrics.record_segment();
        self.metrics.record_segment_write_latency(start.elapsed());

        // Build metadata
        let metadata = SegmentMetadata {
            key: key.to_string(),
            start_offset: self.start_offset.unwrap_or(0),
            end_offset: self.end_offset.unwrap_or(0),
            start_timestamp: self.start_timestamp.unwrap_or(0),
            end_timestamp: self.end_timestamp.unwrap_or(0),
            record_count: self.record_count as i64,
            uncompressed_size: uncompressed_size as u64,
            compressed_size: compressed_size as u64,
        };

        info!(
            "Wrote segment {} with {} records ({} -> {} bytes, {:.1}x compression)",
            key,
            self.record_count,
            uncompressed_size,
            compressed_size,
            uncompressed_size as f64 / compressed_size as f64
        );

        // Reset state
        self.buffer.clear();
        self.record_count = 0;
        self.start_offset = None;
        self.end_offset = None;
        self.start_timestamp = None;
        self.end_timestamp = None;
        self.segment_start_time = None;
        self.uncompressed_bytes = 0;

        Ok(Some(metadata))
    }

    /// Reset writer state without writing
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
