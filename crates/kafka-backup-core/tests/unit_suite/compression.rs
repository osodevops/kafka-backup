//! Compression unit tests.
//!
//! Tests for compression/decompression roundtrips covering:
//! - All compression algorithms (zstd, lz4, none)
//! - Various data sizes (small, large, empty)
//! - Compression ratio expectations
//! - Error handling for corrupted data

use kafka_backup_core::compression::{compress, decompress};
use kafka_backup_core::config::CompressionType;

use super::helpers::{generate_random_bytes, generate_repetitive_bytes};

// ============================================================================
// Zstd Compression Tests
// ============================================================================

#[test]
fn compression_zstd_roundtrip_small_message() {
    let data = b"small message".to_vec();
    let compressed = compress(&data, CompressionType::Zstd).expect("Compression failed");
    let decompressed = decompress(&compressed, CompressionType::Zstd).expect("Decompression failed");

    assert_eq!(decompressed, data);
}

#[test]
fn compression_zstd_roundtrip_large_message() {
    let data = vec![0u8; 1024 * 1024]; // 1MB of zeros
    let compressed = compress(&data, CompressionType::Zstd).expect("Compression failed");
    let decompressed = decompress(&compressed, CompressionType::Zstd).expect("Decompression failed");

    assert_eq!(decompressed.len(), data.len());
    assert_eq!(decompressed, data);
}

#[test]
fn compression_zstd_compresses_repetitive_data() {
    let repetitive = generate_repetitive_bytes(40000); // 40KB of repetitive "ABCD"
    let compressed = compress(&repetitive, CompressionType::Zstd).expect("Compression failed");

    // zstd should compress highly repetitive data significantly
    assert!(
        compressed.len() < repetitive.len() / 10,
        "Expected compression ratio > 10:1 for repetitive data, got {}:1",
        repetitive.len() / compressed.len()
    );
}

#[test]
fn compression_zstd_handles_random_data() {
    let random = generate_random_bytes(1024);
    let compressed = compress(&random, CompressionType::Zstd).expect("Compression failed");
    let decompressed = decompress(&compressed, CompressionType::Zstd).expect("Decompression failed");

    // Random data shouldn't compress much, but roundtrip must work
    assert_eq!(decompressed, random);
    // Allow for compression overhead on random data
    assert!(compressed.len() <= random.len() + 100);
}

#[test]
fn compression_zstd_empty_input() {
    let empty: Vec<u8> = vec![];
    let compressed = compress(&empty, CompressionType::Zstd).expect("Compression failed");
    let decompressed = decompress(&compressed, CompressionType::Zstd).expect("Decompression failed");

    assert_eq!(decompressed, empty);
}

// ============================================================================
// LZ4 Compression Tests
// ============================================================================

#[test]
fn compression_lz4_roundtrip_small_message() {
    let data = b"small message for lz4".to_vec();
    let compressed = compress(&data, CompressionType::Lz4).expect("Compression failed");
    let decompressed = decompress(&compressed, CompressionType::Lz4).expect("Decompression failed");

    assert_eq!(decompressed, data);
}

#[test]
fn compression_lz4_roundtrip_large_message() {
    let data = vec![42u8; 1024 * 1024]; // 1MB
    let compressed = compress(&data, CompressionType::Lz4).expect("Compression failed");
    let decompressed = decompress(&compressed, CompressionType::Lz4).expect("Decompression failed");

    assert_eq!(decompressed.len(), data.len());
    assert_eq!(decompressed, data);
}

#[test]
fn compression_lz4_compresses_repetitive_data() {
    let repetitive = generate_repetitive_bytes(40000);
    let compressed = compress(&repetitive, CompressionType::Lz4).expect("Compression failed");

    // LZ4 should also compress repetitive data well
    assert!(
        compressed.len() < repetitive.len() / 5,
        "Expected reasonable compression for repetitive data"
    );
}

#[test]
fn compression_lz4_empty_input() {
    let empty: Vec<u8> = vec![];
    let compressed = compress(&empty, CompressionType::Lz4).expect("Compression failed");
    let decompressed = decompress(&compressed, CompressionType::Lz4).expect("Decompression failed");

    assert_eq!(decompressed, empty);
}

// ============================================================================
// No Compression Tests
// ============================================================================

#[test]
fn compression_none_passes_through() {
    let data = b"uncompressed data".to_vec();
    let compressed = compress(&data, CompressionType::None).expect("Compression failed");
    let decompressed = decompress(&compressed, CompressionType::None).expect("Decompression failed");

    assert_eq!(compressed, data, "No compression should pass through unchanged");
    assert_eq!(decompressed, data);
}

#[test]
fn compression_none_large_data() {
    let data = generate_random_bytes(1024 * 100); // 100KB
    let compressed = compress(&data, CompressionType::None).expect("Compression failed");

    assert_eq!(compressed.len(), data.len());
    assert_eq!(compressed, data);
}

// ============================================================================
// Cross-Compression Tests
// ============================================================================

#[test]
fn compression_algorithms_produce_different_output() {
    let data = generate_repetitive_bytes(10000);

    let zstd_compressed = compress(&data, CompressionType::Zstd).expect("Zstd failed");
    let lz4_compressed = compress(&data, CompressionType::Lz4).expect("Lz4 failed");
    let none_compressed = compress(&data, CompressionType::None).expect("None failed");

    // All should decompress to the same data
    let zstd_decompressed =
        decompress(&zstd_compressed, CompressionType::Zstd).expect("Zstd decompress failed");
    let lz4_decompressed =
        decompress(&lz4_compressed, CompressionType::Lz4).expect("Lz4 decompress failed");
    let none_decompressed =
        decompress(&none_compressed, CompressionType::None).expect("None decompress failed");

    assert_eq!(zstd_decompressed, data);
    assert_eq!(lz4_decompressed, data);
    assert_eq!(none_decompressed, data);

    // Compressed sizes should differ
    assert_ne!(zstd_compressed.len(), lz4_compressed.len());
    assert_ne!(none_compressed.len(), zstd_compressed.len());
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[test]
fn decompression_corrupted_zstd_returns_error() {
    let corrupted = b"not valid zstd compressed data".to_vec();
    let result = decompress(&corrupted, CompressionType::Zstd);

    assert!(result.is_err(), "Expected error for corrupted zstd data");
}

#[test]
fn decompression_corrupted_lz4_returns_error() {
    let corrupted = b"not valid lz4 compressed data".to_vec();
    let result = decompress(&corrupted, CompressionType::Lz4);

    assert!(result.is_err(), "Expected error for corrupted lz4 data");
}

#[test]
fn decompression_truncated_zstd_returns_error() {
    let data = generate_repetitive_bytes(1000);
    let compressed = compress(&data, CompressionType::Zstd).expect("Compression failed");

    // Truncate the compressed data
    let truncated = &compressed[..compressed.len() / 2];
    let result = decompress(truncated, CompressionType::Zstd);

    assert!(result.is_err(), "Expected error for truncated zstd data");
}

// ============================================================================
// Kafka Record-Like Data Tests
// ============================================================================

#[test]
fn compression_kafka_like_json_records() {
    // Simulate typical JSON Kafka messages
    let records: Vec<String> = (0i64..100)
        .map(|i| {
            format!(
                r#"{{"id":{},"name":"item_{}","price":{},"timestamp":{}}}"#,
                i,
                i,
                i * 10,
                1672531200000i64 + i
            )
        })
        .collect();

    let data = records.join("\n").into_bytes();
    let compressed = compress(&data, CompressionType::Zstd).expect("Compression failed");
    let decompressed = decompress(&compressed, CompressionType::Zstd).expect("Decompression failed");

    assert_eq!(decompressed, data);

    // JSON data typically compresses well
    assert!(
        compressed.len() < data.len() / 2,
        "Expected >2:1 compression for JSON data"
    );
}

#[test]
fn compression_preserves_binary_data() {
    // Binary data with all byte values
    let mut binary_data: Vec<u8> = (0..=255).collect();
    binary_data.extend((0..=255).collect::<Vec<u8>>());

    let compressed = compress(&binary_data, CompressionType::Zstd).expect("Compression failed");
    let decompressed = decompress(&compressed, CompressionType::Zstd).expect("Decompression failed");

    assert_eq!(decompressed, binary_data);
}
