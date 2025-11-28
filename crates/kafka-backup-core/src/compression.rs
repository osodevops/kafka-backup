//! Compression utilities for backup segments.

use crate::{config::CompressionType, Error, Result};
use std::io::{Read, Write};

/// Default compression level for zstd
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Compress data using the specified algorithm
pub fn compress(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    compress_with_level(data, compression, DEFAULT_ZSTD_LEVEL)
}

/// Compress data using the specified algorithm and compression level
pub fn compress_with_level(
    data: &[u8],
    compression: CompressionType,
    level: i32,
) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zstd => compress_zstd_with_level(data, level),
        CompressionType::Lz4 => compress_lz4(data),
    }
}

/// Decompress data using the specified algorithm
pub fn decompress(data: &[u8], compression: CompressionType) -> Result<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Zstd => decompress_zstd(data),
        CompressionType::Lz4 => decompress_lz4(data),
    }
}

/// Get file extension for compression type
pub fn extension(compression: CompressionType) -> &'static str {
    match compression {
        CompressionType::None => "",
        CompressionType::Zstd => ".zst",
        CompressionType::Lz4 => ".lz4",
    }
}

/// Detect compression type from file extension
pub fn detect_from_extension(key: &str) -> CompressionType {
    if key.ends_with(".zst") {
        CompressionType::Zstd
    } else if key.ends_with(".lz4") {
        CompressionType::Lz4
    } else {
        CompressionType::None
    }
}

#[allow(dead_code)] // Convenience wrapper for default compression level
fn compress_zstd(data: &[u8]) -> Result<Vec<u8>> {
    compress_zstd_with_level(data, DEFAULT_ZSTD_LEVEL)
}

fn compress_zstd_with_level(data: &[u8], level: i32) -> Result<Vec<u8>> {
    let mut encoder = zstd::Encoder::new(Vec::new(), level)
        .map_err(|e| Error::Compression(format!("Failed to create zstd encoder: {}", e)))?;

    encoder
        .write_all(data)
        .map_err(|e| Error::Compression(format!("Failed to write to zstd encoder: {}", e)))?;

    encoder
        .finish()
        .map_err(|e| Error::Compression(format!("Failed to finish zstd compression: {}", e)))
}

fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = zstd::Decoder::new(data)
        .map_err(|e| Error::Compression(format!("Failed to create zstd decoder: {}", e)))?;

    let mut output = Vec::new();
    decoder
        .read_to_end(&mut output)
        .map_err(|e| Error::Compression(format!("Failed to decompress zstd data: {}", e)))?;

    Ok(output)
}

fn compress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    Ok(lz4_flex::compress_prepend_size(data))
}

fn decompress_lz4(data: &[u8]) -> Result<Vec<u8>> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| Error::Compression(format!("Failed to decompress lz4 data: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zstd_roundtrip() {
        let data = b"Hello, World! This is a test of zstd compression.";
        let compressed = compress(data, CompressionType::Zstd).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Zstd).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"Hello, World! This is a test of lz4 compression.";
        let compressed = compress(data, CompressionType::Lz4).unwrap();
        let decompressed = decompress(&compressed, CompressionType::Lz4).unwrap();
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_no_compression() {
        let data = b"Hello, World!";
        let compressed = compress(data, CompressionType::None).unwrap();
        let decompressed = decompress(&compressed, CompressionType::None).unwrap();
        assert_eq!(data.as_slice(), compressed.as_slice());
        assert_eq!(data.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_extension() {
        assert_eq!(extension(CompressionType::None), "");
        assert_eq!(extension(CompressionType::Zstd), ".zst");
        assert_eq!(extension(CompressionType::Lz4), ".lz4");
    }

    #[test]
    fn test_detect_from_extension() {
        assert_eq!(
            detect_from_extension("segment-001.zst"),
            CompressionType::Zstd
        );
        assert_eq!(
            detect_from_extension("segment-001.lz4"),
            CompressionType::Lz4
        );
        assert_eq!(
            detect_from_extension("segment-001.bin"),
            CompressionType::None
        );
    }
}
