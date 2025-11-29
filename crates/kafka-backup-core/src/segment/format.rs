//! Binary segment format definitions.
//!
//! Segment Format (v1):
//! ```text
//! +------------------+
//! | Header (32 bytes)|
//! +------------------+
//! | Record 1         |
//! +------------------+
//! | Record 2         |
//! +------------------+
//! | ...              |
//! +------------------+
//! | Footer (8 bytes) |
//! +------------------+
//! ```
//!
//! Header Format:
//! - magic: [u8; 4] = "KBAK"
//! - version: u8
//! - compression: u8 (0=none, 1=zstd, 2=lz4)
//! - reserved: [u8; 2]
//! - record_count: u64 (little-endian)
//! - start_offset: i64 (little-endian)
//! - end_offset: i64 (little-endian)
//!
//! Record Format:
//! - total_len: u32 (little-endian) - length of remaining record data
//! - timestamp: i64 (little-endian)
//! - offset: i64 (little-endian)
//! - key_len: i32 (little-endian, -1 for null)
//! - key: [u8; key_len] (if key_len >= 0)
//! - value_len: i32 (little-endian, -1 for null)
//! - value: [u8; value_len] (if value_len >= 0)
//! - header_count: u16 (little-endian)
//! - headers: [Header; header_count]
//!
//! Header (within record):
//! - key_len: u16 (little-endian)
//! - key: [u8; key_len]
//! - value_len: i32 (little-endian, -1 for null)
//! - value: [u8; value_len] (if value_len >= 0)
//!
//! Footer Format:
//! - crc32: u32 (little-endian) - CRC32 of all preceding bytes
//! - magic_end: [u8; 4] = "BKAE"

use crate::config::CompressionType;
use crate::{Error, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// Magic bytes at start of segment
pub const MAGIC_BYTES: [u8; 4] = *b"KBAK";

/// Magic bytes at end of segment
pub const MAGIC_END: [u8; 4] = *b"BKAE";

/// Current segment format version
pub const VERSION: u8 = 1;

/// Header size in bytes
pub const HEADER_SIZE: usize = 32;

/// Footer size in bytes
pub const FOOTER_SIZE: usize = 8;

/// Segment header
#[derive(Debug, Clone)]
pub struct SegmentHeader {
    pub version: u8,
    pub compression: CompressionType,
    pub record_count: u64,
    pub start_offset: i64,
    pub end_offset: i64,
}

impl SegmentHeader {
    /// Create a new segment header
    pub fn new(compression: CompressionType) -> Self {
        Self {
            version: VERSION,
            compression,
            record_count: 0,
            start_offset: -1,
            end_offset: -1,
        }
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..4].copy_from_slice(&MAGIC_BYTES);
        buf[4] = self.version;
        buf[5] = compression_to_byte(self.compression);
        buf[6..8].copy_from_slice(&[0, 0]); // reserved
        buf[8..16].copy_from_slice(&self.record_count.to_le_bytes());
        buf[16..24].copy_from_slice(&self.start_offset.to_le_bytes());
        buf[24..32].copy_from_slice(&self.end_offset.to_le_bytes());
        buf
    }

    /// Parse header from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < HEADER_SIZE {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Segment header too short",
            )));
        }

        if data[0..4] != MAGIC_BYTES {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid segment magic bytes",
            )));
        }

        let version = data[4];
        if version != VERSION {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported segment version: {}", version),
            )));
        }

        Ok(Self {
            version,
            compression: byte_to_compression(data[5])?,
            record_count: u64::from_le_bytes(data[8..16].try_into().unwrap()),
            start_offset: i64::from_le_bytes(data[16..24].try_into().unwrap()),
            end_offset: i64::from_le_bytes(data[24..32].try_into().unwrap()),
        })
    }
}

/// Binary record representation
#[derive(Debug, Clone)]
pub struct BinaryRecord {
    pub timestamp: i64,
    pub offset: i64,
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Option<Bytes>)>,
}

impl BinaryRecord {
    /// Calculate serialized size of this record (excluding length prefix)
    pub fn serialized_size(&self) -> usize {
        let mut size = 8 + 8 + 4 + 4 + 2; // timestamp + offset + key_len + value_len + header_count

        if let Some(ref key) = self.key {
            size += key.len();
        }

        if let Some(ref value) = self.value {
            size += value.len();
        }

        for (key, value) in &self.headers {
            size += 2 + key.len() + 4; // key_len + key + value_len
            if let Some(ref v) = value {
                size += v.len();
            }
        }

        size
    }

    /// Serialize record to bytes (including length prefix)
    pub fn to_bytes(&self) -> BytesMut {
        let content_size = self.serialized_size();
        let mut buf = BytesMut::with_capacity(4 + content_size);

        // Length prefix
        buf.put_u32_le(content_size as u32);

        // Timestamp and offset
        buf.put_i64_le(self.timestamp);
        buf.put_i64_le(self.offset);

        // Key
        match &self.key {
            Some(key) => {
                buf.put_i32_le(key.len() as i32);
                buf.put_slice(key);
            }
            None => {
                buf.put_i32_le(-1);
            }
        }

        // Value
        match &self.value {
            Some(value) => {
                buf.put_i32_le(value.len() as i32);
                buf.put_slice(value);
            }
            None => {
                buf.put_i32_le(-1);
            }
        }

        // Headers
        buf.put_u16_le(self.headers.len() as u16);
        for (key, value) in &self.headers {
            buf.put_u16_le(key.len() as u16);
            buf.put_slice(key.as_bytes());
            match value {
                Some(v) => {
                    buf.put_i32_le(v.len() as i32);
                    buf.put_slice(v);
                }
                None => {
                    buf.put_i32_le(-1);
                }
            }
        }

        buf
    }

    /// Parse record from bytes (expects length prefix already consumed)
    pub fn from_bytes(mut data: Bytes) -> Result<Self> {
        if data.len() < 20 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Record data too short",
            )));
        }

        let timestamp = data.get_i64_le();
        let offset = data.get_i64_le();

        // Key
        let key_len = data.get_i32_le();
        let key = if key_len >= 0 {
            let key_len = key_len as usize;
            if data.len() < key_len {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Key data truncated",
                )));
            }
            Some(data.split_to(key_len))
        } else {
            None
        };

        // Value
        let value_len = data.get_i32_le();
        let value = if value_len >= 0 {
            let value_len = value_len as usize;
            if data.len() < value_len {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Value data truncated",
                )));
            }
            Some(data.split_to(value_len))
        } else {
            None
        };

        // Headers
        if data.len() < 2 {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Header count truncated",
            )));
        }
        let header_count = data.get_u16_le() as usize;
        let mut headers = Vec::with_capacity(header_count);

        for _ in 0..header_count {
            if data.len() < 2 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Header key length truncated",
                )));
            }
            let key_len = data.get_u16_le() as usize;
            if data.len() < key_len {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Header key truncated",
                )));
            }
            let key = String::from_utf8_lossy(&data.split_to(key_len)).into_owned();

            if data.len() < 4 {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Header value length truncated",
                )));
            }
            let value_len = data.get_i32_le();
            let value = if value_len >= 0 {
                let value_len = value_len as usize;
                if data.len() < value_len {
                    return Err(Error::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "Header value truncated",
                    )));
                }
                Some(data.split_to(value_len))
            } else {
                None
            };

            headers.push((key, value));
        }

        Ok(Self {
            timestamp,
            offset,
            key,
            value,
            headers,
        })
    }
}

/// Convert compression type to byte
fn compression_to_byte(compression: CompressionType) -> u8 {
    match compression {
        CompressionType::None => 0,
        CompressionType::Zstd => 1,
        CompressionType::Lz4 => 2,
    }
}

/// Convert byte to compression type
fn byte_to_compression(byte: u8) -> Result<CompressionType> {
    match byte {
        0 => Ok(CompressionType::None),
        1 => Ok(CompressionType::Zstd),
        2 => Ok(CompressionType::Lz4),
        _ => Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Unknown compression type: {}", byte),
        ))),
    }
}

/// Calculate CRC32 of data
pub fn crc32(data: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_roundtrip() {
        let header = SegmentHeader {
            version: VERSION,
            compression: CompressionType::Zstd,
            record_count: 12345,
            start_offset: 100,
            end_offset: 200,
        };

        let bytes = header.to_bytes();
        let parsed = SegmentHeader::from_bytes(&bytes).unwrap();

        assert_eq!(parsed.version, header.version);
        assert_eq!(parsed.compression, header.compression);
        assert_eq!(parsed.record_count, header.record_count);
        assert_eq!(parsed.start_offset, header.start_offset);
        assert_eq!(parsed.end_offset, header.end_offset);
    }

    #[test]
    fn test_record_roundtrip() {
        let record = BinaryRecord {
            timestamp: 1234567890,
            offset: 42,
            key: Some(Bytes::from("test-key")),
            value: Some(Bytes::from("test-value")),
            headers: vec![
                ("header1".to_string(), Some(Bytes::from("value1"))),
                ("header2".to_string(), None),
            ],
        };

        let bytes = record.to_bytes();
        // Skip length prefix
        let data = Bytes::from(bytes.to_vec()).slice(4..);
        let parsed = BinaryRecord::from_bytes(data).unwrap();

        assert_eq!(parsed.timestamp, record.timestamp);
        assert_eq!(parsed.offset, record.offset);
        assert_eq!(parsed.key, record.key);
        assert_eq!(parsed.value, record.value);
        assert_eq!(parsed.headers.len(), record.headers.len());
    }

    #[test]
    fn test_record_with_nulls() {
        let record = BinaryRecord {
            timestamp: 1234567890,
            offset: 42,
            key: None,
            value: None,
            headers: vec![],
        };

        let bytes = record.to_bytes();
        let data = Bytes::from(bytes.to_vec()).slice(4..);
        let parsed = BinaryRecord::from_bytes(data).unwrap();

        assert_eq!(parsed.key, None);
        assert_eq!(parsed.value, None);
        assert!(parsed.headers.is_empty());
    }
}
