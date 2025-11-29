//! Segment reader for reading binary segments.

use bytes::Bytes;
use tracing::debug;

use super::format::{BinaryRecord, SegmentHeader, FOOTER_SIZE, HEADER_SIZE, MAGIC_END};
use crate::compression;
use crate::{Error, Result};

/// Segment reader for reading binary format segments
pub struct SegmentReader {
    header: SegmentHeader,
    data: Bytes,
    position: usize,
    records_read: u64,
}

impl SegmentReader {
    /// Open a segment from raw bytes
    pub fn open(data: Bytes) -> Result<Self> {
        if data.len() < HEADER_SIZE + FOOTER_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Segment too small",
            )));
        }

        // Parse header
        let header = SegmentHeader::from_bytes(&data[..HEADER_SIZE])?;

        // Verify footer magic
        let footer_start = data.len() - FOOTER_SIZE;
        if data[footer_start + 4..] != MAGIC_END {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid segment footer magic",
            )));
        }

        // Verify CRC
        let stored_crc =
            u32::from_le_bytes(data[footer_start..footer_start + 4].try_into().unwrap());
        let computed_crc = super::format::crc32(&data[..footer_start]);
        if stored_crc != computed_crc {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "CRC mismatch: stored={}, computed={}",
                    stored_crc, computed_crc
                ),
            )));
        }

        // Decompress record data
        let compressed_data = &data[HEADER_SIZE..footer_start];
        let decompressed = compression::decompress(compressed_data, header.compression)?;

        debug!(
            "Opened segment: {} records, offsets {}-{}, {} bytes decompressed",
            header.record_count,
            header.start_offset,
            header.end_offset,
            decompressed.len()
        );

        Ok(Self {
            header,
            data: Bytes::from(decompressed),
            position: 0,
            records_read: 0,
        })
    }

    /// Get the segment header
    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    /// Get total record count
    pub fn record_count(&self) -> u64 {
        self.header.record_count
    }

    /// Get start offset
    pub fn start_offset(&self) -> i64 {
        self.header.start_offset
    }

    /// Get end offset
    pub fn end_offset(&self) -> i64 {
        self.header.end_offset
    }

    /// Get number of records read so far
    pub fn records_read(&self) -> u64 {
        self.records_read
    }

    /// Read the next record
    pub fn next_record(&mut self) -> Result<Option<BinaryRecord>> {
        if self.records_read >= self.header.record_count {
            return Ok(None);
        }

        if self.position + 4 > self.data.len() {
            return Ok(None);
        }

        // Read length prefix
        let len = u32::from_le_bytes(
            self.data[self.position..self.position + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        self.position += 4;

        if self.position + len > self.data.len() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Record data truncated",
            )));
        }

        // Parse record
        let record_data = self.data.slice(self.position..self.position + len);
        let record = BinaryRecord::from_bytes(record_data)?;
        self.position += len;
        self.records_read += 1;

        Ok(Some(record))
    }

    /// Read all remaining records
    pub fn read_all(&mut self) -> Result<Vec<BinaryRecord>> {
        let mut records =
            Vec::with_capacity((self.header.record_count - self.records_read) as usize);
        while let Some(record) = self.next_record()? {
            records.push(record);
        }
        Ok(records)
    }

    /// Reset reader to beginning
    pub fn reset(&mut self) {
        self.position = 0;
        self.records_read = 0;
    }
}

impl Iterator for SegmentReader {
    type Item = Result<BinaryRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next_record() {
            Ok(Some(record)) => Some(Ok(record)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::PerformanceMetrics;
    use crate::segment::writer::SegmentWriterConfig;
    use crate::segment::SegmentWriter;
    use crate::storage::{FilesystemBackend, StorageBackend};
    use std::sync::Arc;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_segment_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let storage = Arc::new(FilesystemBackend::new(temp_dir.path().to_path_buf()));
        let metrics = Arc::new(PerformanceMetrics::new());
        let config = SegmentWriterConfig::default();

        let mut writer = SegmentWriter::new(config, storage.clone(), metrics);

        // Write records
        let original_records: Vec<_> = (0..50)
            .map(|i| BinaryRecord {
                timestamp: 1000 + i,
                offset: i,
                key: Some(Bytes::from(format!("key-{}", i))),
                value: Some(Bytes::from(format!("value-{}", i))),
                headers: vec![("h1".to_string(), Some(Bytes::from("v1")))],
            })
            .collect();

        for record in &original_records {
            writer.add_record(record.clone()).unwrap();
        }

        writer.flush("test.bin").await.unwrap();

        // Read back
        let data = storage.get("test.bin").await.unwrap();
        let mut reader = SegmentReader::open(data).unwrap();

        assert_eq!(reader.record_count(), 50);
        assert_eq!(reader.start_offset(), 0);
        assert_eq!(reader.end_offset(), 49);

        let read_records = reader.read_all().unwrap();
        assert_eq!(read_records.len(), 50);

        for (orig, read) in original_records.iter().zip(read_records.iter()) {
            assert_eq!(orig.offset, read.offset);
            assert_eq!(orig.timestamp, read.timestamp);
            assert_eq!(orig.key, read.key);
            assert_eq!(orig.value, read.value);
        }
    }
}
