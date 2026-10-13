//! Kafka Produce API implementation.

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::compression::{self as cmpr, Compressor};
use kafka_protocol::messages::{
    ApiKey, ProduceRequest, ProduceResponse as KafkaProduceResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH,
};
use tracing::{debug, trace};

use super::transactions::ProducerBatch;
use super::KafkaClient;
use crate::error::KafkaError;
use crate::manifest::BackupRecord;
use crate::Result;

/// Response from a produce operation
#[derive(Debug)]
pub struct ProduceResponse {
    /// Base offset assigned to the first record in the first sub-batch
    pub base_offset: i64,
    /// Error code (always 0; errors are returned via `Err(...)`)
    pub error_code: i16,
    /// Total number of records produced
    pub record_count: usize,
    /// Per-sub-batch offsets: (base_offset, record_count) for each sub-batch.
    /// When no splitting occurs, this contains a single entry.
    pub sub_batch_offsets: Vec<(i64, usize)>,
}

/// Max timestamp delta within a single produced record batch.
///
/// The wire format stores each record's timestamp delta as a varlong, so a
/// batch may legally span any range. This limit exists because
/// kafka-protocol's `RecordBatchEncoder` up to and including the 0.17.0
/// crates.io release refuses to encode deltas outside `i32` ("Timestamps
/// within batch are too far apart"); splitting keeps restores working even
/// when built against that release. i32::MAX ms ≈ 24.8 days.
const MAX_TIMESTAMP_DELTA_MS: i64 = i32::MAX as i64;

/// Size of a v2 record batch header: base offset (8), batch length (4),
/// partition leader epoch (4), magic (1), CRC (4), attributes (2), last
/// offset delta (4), first and max timestamp (8 + 8), producer id (8),
/// producer epoch (2), base sequence (4) and record count (4).
pub(crate) const RECORD_BATCH_OVERHEAD: usize = 61;

/// Bytes of a zig-zag varlong, the encoding of every length and delta inside
/// a v2 record.
fn varint_size(value: i64) -> usize {
    let mut zigzag = ((value << 1) ^ (value >> 63)) as u64;
    let mut size = 1;
    while zigzag >= 0x80 {
        zigzag >>= 7;
        size += 1;
    }
    size
}

/// A nullable byte field: varint length (`-1` for null) plus the bytes.
fn nullable_bytes_size(len: Option<usize>) -> usize {
    match len {
        None => varint_size(-1),
        Some(len) => varint_size(len as i64) + len,
    }
}

/// Size of `record` encoded inside an uncompressed v2 batch, given its
/// timestamp and offset deltas from the batch's first record.
fn encoded_record_size(record: &BackupRecord, timestamp_delta: i64, offset_delta: i64) -> usize {
    let headers: usize = record
        .headers
        .iter()
        .map(|h| {
            varint_size(h.key.len() as i64)
                + h.key.len()
                + nullable_bytes_size(h.value.as_ref().map(Vec::len))
        })
        .sum();
    let body = 1 // attributes
        + varint_size(timestamp_delta)
        + varint_size(offset_delta)
        + nullable_bytes_size(record.key.as_ref().map(Vec::len))
        + nullable_bytes_size(record.value.as_ref().map(Vec::len))
        + varint_size(record.headers.len() as i64)
        + headers;
    varint_size(body as i64) + body
}

/// Upper bound of `record`'s encoded size in any batch (worst-case deltas).
pub(crate) fn record_size_upper_bound(record: &BackupRecord) -> usize {
    encoded_record_size(record, i64::MAX, i64::from(i32::MAX))
}

/// Exact size of an uncompressed batch holding only `record`.
pub(crate) fn single_record_batch_size(record: &BackupRecord) -> usize {
    RECORD_BATCH_OVERHEAD + encoded_record_size(record, 0, 0)
}

/// Uncompressed bytes of `records` as the restore counts them for
/// `produce_max_batch_bytes` and `rate_limit_bytes_per_sec`: the upper bound
/// of each record's encoded size (key, value, headers and framing).
pub(crate) fn records_encoded_size(records: &[BackupRecord]) -> usize {
    records.iter().map(record_size_upper_bound).sum()
}

/// Largest batch, in uncompressed bytes, that is guaranteed to stay within
/// `max_message_bytes` once compressed: zstd can grow incompressible data
/// by up to `len / 256` plus a small frame overhead (`ZSTD_compressBound`).
pub(crate) fn batch_bytes_within_max_message_bytes(max_message_bytes: usize) -> usize {
    max_message_bytes
        .saturating_sub(max_message_bytes / 256 + 128)
        .max(RECORD_BATCH_OVERHEAD + 1)
}

/// The first record of `records` that cannot be produced to a topic whose
/// `max.message.bytes` is `max_message_bytes`, even alone in a
/// zstd-compressed batch: `(index, compressed single-record batch size)`.
/// Only records too large for an uncompressed batch are compressed to
/// check, so the common case costs one size computation per record.
pub(crate) fn find_oversized_record(
    records: &[BackupRecord],
    max_message_bytes: usize,
) -> Result<Option<(usize, usize)>> {
    let uncompressed_limit = batch_bytes_within_max_message_bytes(max_message_bytes);
    for (i, record) in records.iter().enumerate() {
        if single_record_batch_size(record) <= uncompressed_limit {
            continue;
        }
        let compressed = encode_batch(
            std::slice::from_ref(record),
            &ProducerBatch::NON_IDEMPOTENT,
            Compression::Zstd,
        )?
        .len();
        if compressed > max_message_bytes {
            return Ok(Some((i, compressed)));
        }
    }
    Ok(None)
}

/// Split `records` into consecutive produce batches holding at most
/// `max_records` records and at most `max_bytes` uncompressed bytes
/// ([`RECORD_BATCH_OVERHEAD`] plus [`record_size_upper_bound`] per record).
///
/// A record that alone exceeds `max_bytes` gets a batch of its own; callers
/// check records against the target's `max.message.bytes` beforehand.
pub(crate) fn plan_batches(
    records: &[BackupRecord],
    max_records: usize,
    max_bytes: usize,
) -> Vec<std::ops::Range<usize>> {
    let max_records = max_records.max(1);
    let mut batches = Vec::new();
    let mut start = 0;
    let mut bytes = RECORD_BATCH_OVERHEAD;
    for (i, record) in records.iter().enumerate() {
        let size = record_size_upper_bound(record);
        let count = i - start;
        if count > 0 && (count >= max_records || bytes + size > max_bytes) {
            batches.push(start..i);
            start = i;
            bytes = RECORD_BATCH_OVERHEAD;
        }
        bytes += size;
    }
    if start < records.len() {
        batches.push(start..records.len());
    }
    batches
}

/// The next sequence number after `sequence` advanced by `increment`
/// records, wrapping past `i32::MAX` to 0 the way the Kafka producer and
/// broker do (`DefaultRecordBatch.incrementSequence`).
pub(crate) fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    if sequence > i32::MAX - increment {
        increment - (i32::MAX - sequence) - 1
    } else {
        sequence + increment
    }
}

/// Ranges of `records` whose timestamps all lie within
/// [`MAX_TIMESTAMP_DELTA_MS`] of each other (tracking min/max). Records keep
/// their order.
pub(crate) fn split_by_timestamp_ranges(records: &[BackupRecord]) -> Vec<std::ops::Range<usize>> {
    let Some(first) = records.first() else {
        return vec![];
    };

    let mut ranges = Vec::new();
    let mut start = 0;
    let mut ts_min = first.timestamp;
    let mut ts_max = ts_min;

    for (i, record) in records.iter().enumerate() {
        let new_min = ts_min.min(record.timestamp);
        let new_max = ts_max.max(record.timestamp);

        if i > start && (new_max - new_min) > MAX_TIMESTAMP_DELTA_MS {
            ranges.push(start..i);
            start = i;
            ts_min = record.timestamp;
            ts_max = record.timestamp;
        } else {
            ts_min = new_min;
            ts_max = new_max;
        }
    }

    ranges.push(start..records.len());
    ranges
}

/// Split records into sub-batches where all timestamps within a sub-batch
/// are within MAX_TIMESTAMP_DELTA_MS of each other (tracking min/max).
/// Records maintain their original order.
#[cfg(test)]
fn split_by_timestamp(records: Vec<BackupRecord>) -> Vec<Vec<BackupRecord>> {
    split_by_timestamp_ranges(&records)
        .into_iter()
        .map(|range| records[range].to_vec())
        .collect()
}

/// Convert BackupRecords to kafka-protocol Records stamped with the
/// producer fields of `batch`. Record `i` gets offset delta `i` and sequence
/// `base_sequence + i`, so the encoder writes them as one batch whose base
/// sequence is `batch.base_sequence`.
pub(crate) fn build_kafka_records(records: &[BackupRecord], batch: &ProducerBatch) -> Vec<Record> {
    records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let headers: IndexMap<StrBytes, Option<Bytes>> = r
                .headers
                .iter()
                // `None` is a null header value and must be produced as
                // such (issue #155) — it is not the same as an empty value.
                .map(|h| {
                    (
                        StrBytes::from_string(h.key.clone()),
                        h.value.clone().map(Bytes::from),
                    )
                })
                .collect();

            Record {
                transactional: batch.transactional,
                control: false,
                // KIP-534 batch attribute; only set by the broker's log cleaner
                // on tombstone/abort-marker batches, never on produced records.
                delete_horizon: false,
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: batch.producer.producer_id,
                producer_epoch: batch.producer.producer_epoch,
                timestamp_type: TimestampType::Creation,
                offset: i as i64,
                // offset - sequence is constant, so every record lands in a
                // single batch.
                sequence: batch.base_sequence.wrapping_add(i as i32),
                timestamp: r.timestamp,
                key: r.key.clone().map(Bytes::from),
                value: r.value.clone().map(Bytes::from),
                headers,
            }
        })
        .collect()
}

/// Encode `records` once as a single v2 record batch.
///
/// kafka-protocol's `Record` holds headers in a map, which collapses
/// duplicate keys. The batch framing, timestamps, compression and CRC stay
/// with the upstream encoder; only each record's header list is rewritten
/// (in order, duplicates and null values included) in the uncompressed
/// record bytes before they are compressed. Every produce path — plain,
/// idempotent and transactional — encodes through here exactly once, so a
/// retry resends these identical bytes.
pub(crate) fn encode_batch(
    records: &[BackupRecord],
    batch: &ProducerBatch,
    compression: Compression,
) -> Result<Bytes> {
    if records.is_empty() {
        return Err(KafkaError::Protocol("cannot encode an empty record batch".to_string()).into());
    }
    let kafka_records = build_kafka_records(records, batch);
    let headers: Vec<_> = records.iter().map(|r| r.headers.clone()).collect();
    let header_index = std::cell::Cell::new(0);
    let compressor =
        |raw: &mut BytesMut, out: &mut BytesMut, compression: Compression| -> anyhow::Result<()> {
            let count = super::header_codec::decode(raw)?.len();
            let start = header_index.get();
            let end = start + count;
            let raw = super::header_codec::replace(
                raw,
                headers
                    .get(start..end)
                    .ok_or_else(|| anyhow::anyhow!("Kafka record/header count mismatch"))?,
            )?;
            header_index.set(end);
            let body = |buf: &mut BytesMut| -> anyhow::Result<()> {
                buf.extend_from_slice(&raw);
                Ok(())
            };
            match compression {
                Compression::None => cmpr::None::compress(out, body),
                Compression::Gzip => cmpr::Gzip::compress(out, body),
                Compression::Snappy => cmpr::Snappy::compress(out, body),
                Compression::Lz4 => cmpr::Lz4::compress(out, body),
                Compression::Zstd => cmpr::Zstd::compress(out, body),
            }
        };
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode_with_custom_compression(
        &mut buf,
        kafka_records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
        Some(&compressor),
    )
    .map_err(|e| KafkaError::Protocol(format!("Failed to encode records: {e:?}")))?;
    if header_index.get() != headers.len() {
        return Err(KafkaError::Protocol("Kafka record/header count mismatch".to_string()).into());
    }
    Ok(buf.freeze())
}

/// Send one already-encoded record batch to `topic`/`partition` on
/// `client` and return the base offset the broker assigned. `client`
/// retries a lost connection once with the identical request bytes.
pub(crate) async fn produce_encoded(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    batch: Bytes,
    acks: i16,
    timeout_ms: i32,
) -> Result<i64> {
    let partition_data = kafka_protocol::messages::produce_request::PartitionProduceData::default()
        .with_index(partition)
        .with_records(Some(batch));

    let topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partition_data(vec![partition_data]);

    let request = ProduceRequest::default()
        .with_acks(acks)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(vec![topic_data]);

    let response: KafkaProduceResponse = client.send_request(ApiKey::Produce, request).await?;
    Ok(parse_produce_response(&response, topic, partition)?.base_offset)
}

/// Produce records to a topic/partition without a producer id.
///
/// Records spanning more than ~24.8 days of timestamps are automatically
/// split into sub-batches sent as separate produce requests (see
/// [`MAX_TIMESTAMP_DELTA_MS`]). Each sub-batch is encoded once; a retry
/// resends identical bytes and an acknowledged sub-batch is never resent.
pub async fn produce(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    records: Vec<BackupRecord>,
    acks: i16,
    timeout_ms: i32,
) -> Result<ProduceResponse> {
    if records.is_empty() {
        return Ok(ProduceResponse {
            base_offset: -1,
            error_code: 0,
            record_count: 0,
            sub_batch_offsets: vec![],
        });
    }

    let ranges = split_by_timestamp_ranges(&records);
    if ranges.len() > 1 {
        debug!(
            "Split {} records into {} sub-batches for {}:{} (timestamp range exceeds i32::MAX ms)",
            records.len(),
            ranges.len(),
            topic,
            partition
        );
    }

    let mut sub_batch_offsets = Vec::with_capacity(ranges.len());
    for range in ranges {
        let sub_batch = &records[range];
        let bytes = encode_batch(sub_batch, &ProducerBatch::NON_IDEMPOTENT, Compression::Zstd)?;
        let base_offset =
            produce_encoded(client, topic, partition, bytes, acks, timeout_ms).await?;
        trace!(
            "Produced {} records to {}:{} at offset {}",
            sub_batch.len(),
            topic,
            partition,
            base_offset
        );
        sub_batch_offsets.push((base_offset, sub_batch.len()));
    }

    Ok(ProduceResponse {
        base_offset: sub_batch_offsets[0].0,
        error_code: 0,
        record_count: records.len(),
        sub_batch_offsets,
    })
}

/// Parse a produce response for a specific topic/partition.
fn parse_produce_response(
    response: &KafkaProduceResponse,
    topic: &str,
    partition: i32,
) -> Result<ProduceResponse> {
    for topic_response in &response.responses {
        if topic_response.name.as_str() != topic {
            continue;
        }
        for partition_response in &topic_response.partition_responses {
            if partition_response.index != partition {
                continue;
            }
            if partition_response.error_code != 0 {
                return Err(KafkaError::BrokerError {
                    code: partition_response.error_code,
                    message: format!(
                        "Produce error for {}:{}: code {} (base_offset={}, log_append_time={}, log_start_offset={}){}",
                        topic, partition, partition_response.error_code,
                        partition_response.base_offset,
                        partition_response.log_append_time_ms,
                        partition_response.log_start_offset,
                        partition_response
                            .error_message
                            .as_ref()
                            .map(|m| format!(": {}", m.as_str()))
                            .unwrap_or_default(),
                    ),
                }
                .into());
            }
            // record_count and sub_batch_offsets are filled in by the
            // caller (produce()) which aggregates across sub-batches.
            return Ok(ProduceResponse {
                base_offset: partition_response.base_offset,
                error_code: 0,
                record_count: 0,
                sub_batch_offsets: vec![],
            });
        }
    }
    Err(KafkaError::Protocol("No partition response in produce response".to_string()).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::RecordHeader;

    fn make_record(timestamp: i64) -> BackupRecord {
        BackupRecord {
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            headers: vec![],
            timestamp,
            offset: 0,
        }
    }

    /// Issue #155: a null header value must be produced as null, an empty
    /// one as empty — they are different values to a Kafka consumer.
    #[test]
    fn build_kafka_records_preserves_null_and_empty_header_values() {
        let mut record = make_record(1);
        record.headers = vec![
            RecordHeader {
                key: "trace-id".to_string(),
                value: None,
            },
            RecordHeader {
                key: "empty".to_string(),
                value: Some(vec![]),
            },
            RecordHeader {
                key: "tenant".to_string(),
                value: Some(b"42".to_vec()),
            },
        ];

        let records = build_kafka_records(&[record], &ProducerBatch::NON_IDEMPOTENT);
        let headers = &records[0].headers;

        assert_eq!(headers.len(), 3);
        assert_eq!(
            headers.get(&StrBytes::from_static_str("trace-id")),
            Some(&None)
        );
        assert_eq!(
            headers.get(&StrBytes::from_static_str("empty")),
            Some(&Some(Bytes::new()))
        );
        assert_eq!(
            headers.get(&StrBytes::from_static_str("tenant")),
            Some(&Some(Bytes::from_static(b"42")))
        );
    }

    fn sized_record(key_len: usize, value_len: usize) -> BackupRecord {
        BackupRecord {
            key: Some(vec![b'k'; key_len]),
            value: Some(vec![b'v'; value_len]),
            headers: vec![RecordHeader {
                key: "h".to_string(),
                value: None,
            }],
            timestamp: 1_700_000_000_000,
            offset: 0,
        }
    }

    fn encoded_len(records: &[BackupRecord]) -> usize {
        encode_batch(records, &ProducerBatch::NON_IDEMPOTENT, Compression::None)
            .unwrap()
            .len()
    }

    /// The size helpers agree with what the encoder actually writes.
    #[test]
    fn single_record_batch_size_is_exact_and_the_upper_bound_holds() {
        for (k, v) in [(0, 0), (3, 10), (100, 5_000), (1, 70_000)] {
            let record = sized_record(k, v);
            assert_eq!(
                single_record_batch_size(&record),
                encoded_len(std::slice::from_ref(&record)),
                "key {k} value {v}"
            );
        }
        let mut null_record = sized_record(0, 0);
        null_record.key = None;
        null_record.value = None;
        assert_eq!(
            single_record_batch_size(&null_record),
            encoded_len(std::slice::from_ref(&null_record))
        );

        // Spread timestamps and many records: the bound covers the real size.
        let records: Vec<BackupRecord> = (0..500)
            .map(|i| {
                let mut r = sized_record(8, 100 + i % 37);
                r.timestamp = 1_700_000_000_000 + (i as i64) * 3_600_000;
                r
            })
            .collect();
        let bound = RECORD_BATCH_OVERHEAD + records_encoded_size(&records);
        let actual = encoded_len(&records);
        assert!(actual <= bound, "actual {actual} > bound {bound}");
    }

    #[test]
    fn plan_batches_caps_by_record_count() {
        let records: Vec<BackupRecord> = (0..25).map(|_| sized_record(1, 10)).collect();
        let batches = plan_batches(&records, 10, usize::MAX);
        assert_eq!(batches, vec![0..10, 10..20, 20..25]);
    }

    #[test]
    fn plan_batches_caps_by_encoded_bytes() {
        // ~1 KiB records, 10 KiB cap: no batch may exceed the cap.
        let records: Vec<BackupRecord> = (0..100).map(|_| sized_record(16, 1_000)).collect();
        let cap = 10 * 1024;
        let batches = plan_batches(&records, 1_000, cap);
        assert!(batches.len() > 1);
        let mut covered = 0;
        for range in &batches {
            assert_eq!(range.start, covered, "batches are contiguous");
            covered = range.end;
            let batch = &records[range.clone()];
            assert!(RECORD_BATCH_OVERHEAD + records_encoded_size(batch) <= cap);
            assert!(encoded_len(batch) <= cap);
        }
        assert_eq!(covered, records.len());
        // Every batch but the last is full: one more record would not fit.
        for range in &batches[..batches.len() - 1] {
            let with_next = &records[range.start..range.end + 1];
            assert!(RECORD_BATCH_OVERHEAD + records_encoded_size(with_next) > cap);
        }
    }

    #[test]
    fn plan_batches_gives_an_oversized_record_its_own_batch() {
        let records = vec![
            sized_record(1, 10),
            sized_record(1, 50_000),
            sized_record(1, 10),
            sized_record(1, 10),
        ];
        let batches = plan_batches(&records, 1_000, 4_096);
        assert_eq!(batches, vec![0..1, 1..2, 2..4]);
        assert!(plan_batches(&[], 10, 4_096).is_empty());
    }

    #[test]
    fn oversized_record_is_found_by_its_compressed_size() {
        let max = 4_096;
        // Compressible: 40 KB of the same byte compresses far below 4 KiB,
        // so the broker accepts it and it must not be rejected.
        let compressible = sized_record(1, 40_000);
        // Incompressible: pseudo-random bytes stay above the limit.
        let mut incompressible = sized_record(1, 0);
        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        incompressible.value = Some(
            (0..8_000)
                .map(|_| {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    x as u8
                })
                .collect(),
        );
        let small = sized_record(1, 100);

        assert_eq!(
            find_oversized_record(&[small.clone(), compressible.clone()], max).unwrap(),
            None
        );
        let found = find_oversized_record(&[small, compressible, incompressible], max).unwrap();
        let (index, size) = found.expect("incompressible record is too large");
        assert_eq!(index, 2);
        assert!(size > max);
    }

    #[test]
    fn batch_cap_leaves_room_for_compression_overhead() {
        let max = 1_048_576;
        let cap = batch_bytes_within_max_message_bytes(max);
        assert!(cap < max);
        assert!(cap > max - 8_192);
        assert_eq!(
            batch_bytes_within_max_message_bytes(10),
            RECORD_BATCH_OVERHEAD + 1
        );
    }

    #[test]
    fn sequence_numbers_wrap_to_zero_past_i32_max() {
        assert_eq!(increment_sequence(0, 1), 1);
        assert_eq!(increment_sequence(41, 1000), 1041);
        assert_eq!(increment_sequence(i32::MAX - 10, 10), i32::MAX);
        assert_eq!(increment_sequence(i32::MAX, 1), 0);
        assert_eq!(increment_sequence(i32::MAX - 2, 5), 2);
        assert_eq!(increment_sequence(i32::MAX - 999, 1_000), 0);
    }

    /// Duplicate, null and empty headers survive the single encode every
    /// produce path uses (plain, idempotent, transactional), in order, for
    /// every codec, and fetch decodes them back the same way. The encoding
    /// is deterministic, so a retry resends identical bytes.
    #[test]
    fn encode_batch_keeps_ordered_duplicate_headers_on_every_produce_path() {
        use super::super::fetch::{decode_fetch_data, TransactionFilter};
        use super::super::transactions::ProducerIdAndEpoch;

        let header = |key: &str, value: Option<&[u8]>| RecordHeader {
            key: key.to_string(),
            value: value.map(<[u8]>::to_vec),
        };
        let mut first = make_record(1_700_000_000_000);
        first.headers = vec![
            header("x", None),
            header("x", Some(b"")),
            header("y", Some(b"2")),
            header("x", Some(b"3")),
        ];
        let mut second = make_record(1_700_000_000_001);
        second.headers = vec![header("y", Some(b"a")), header("y", Some(b"a"))];
        let records = vec![first, make_record(1_700_000_000_002), second];

        let idempotent = ProducerBatch {
            producer: ProducerIdAndEpoch {
                producer_id: 7,
                producer_epoch: 1,
            },
            base_sequence: 40,
            transactional: false,
        };
        let transactional = ProducerBatch {
            transactional: true,
            ..idempotent
        };
        for batch in [ProducerBatch::NON_IDEMPOTENT, idempotent, transactional] {
            for compression in [
                Compression::None,
                Compression::Gzip,
                Compression::Snappy,
                Compression::Lz4,
                Compression::Zstd,
            ] {
                let bytes = encode_batch(&records, &batch, compression).unwrap();
                assert_eq!(
                    bytes,
                    encode_batch(&records, &batch, compression).unwrap(),
                    "{compression:?}: encoding must be deterministic"
                );
                let decoded =
                    decode_fetch_data(&bytes, 0, &mut TransactionFilter::ReadUncommitted).unwrap();
                let headers: Vec<_> = decoded.records.iter().map(|r| r.headers.clone()).collect();
                let expected: Vec<_> = records.iter().map(|r| r.headers.clone()).collect();
                assert_eq!(headers, expected, "{compression:?} {batch:?}");
            }
        }
    }

    /// A batch whose sequences cross `i32::MAX` is still encoded as one
    /// batch carrying its base sequence (the broker derives the rest).
    #[test]
    fn a_batch_crossing_the_sequence_wrap_is_one_batch() {
        use super::super::transactions::ProducerIdAndEpoch;
        use kafka_protocol::records::RecordBatchDecoder;

        let records: Vec<BackupRecord> = (0..5).map(|_| sized_record(1, 10)).collect();
        let batch = ProducerBatch {
            producer: ProducerIdAndEpoch {
                producer_id: 9,
                producer_epoch: 2,
            },
            base_sequence: i32::MAX - 1,
            transactional: false,
        };
        let bytes = encode_batch(&records, &batch, Compression::Zstd).unwrap();
        let infos = RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).unwrap();
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].base_sequence, i32::MAX - 1);
        assert_eq!(infos[0].record_count, 5);
        assert_eq!(infos[0].producer_id, 9);
    }

    #[test]
    fn non_idempotent_batch_has_no_producer_id_or_sequence() {
        use kafka_protocol::records::RecordBatchDecoder;

        let bytes = encode_batch(
            &[sized_record(1, 10), sized_record(1, 10)],
            &ProducerBatch::NON_IDEMPOTENT,
            Compression::Zstd,
        )
        .unwrap();
        let info = &RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).unwrap()[0];
        assert_eq!(info.producer_id, -1);
        assert_eq!(info.producer_epoch, -1);
        assert_eq!(info.base_sequence, -1);
    }

    #[test]
    fn split_by_timestamp_ranges_cover_every_record_in_order() {
        let day_ms: i64 = 86_400_000;
        let records: Vec<BackupRecord> = (0..50)
            .map(|i| make_record(1_700_000_000_000 + i * day_ms))
            .collect();
        let ranges = split_by_timestamp_ranges(&records);
        assert!(ranges.len() >= 2);
        let mut next = 0;
        for range in &ranges {
            assert_eq!(range.start, next);
            next = range.end;
        }
        assert_eq!(next, records.len());
        assert!(split_by_timestamp_ranges(&[]).is_empty());
    }

    #[test]
    fn test_split_by_timestamp_single_batch() {
        let records: Vec<BackupRecord> =
            (0..5).map(|i| make_record(1_000_000 + i * 1000)).collect();
        let splits = split_by_timestamp(records);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].len(), 5);
    }

    #[test]
    fn test_split_by_timestamp_multiple_batches() {
        let day_ms: i64 = 86_400_000;
        let records: Vec<BackupRecord> = (0..50)
            .map(|i| make_record(1_700_000_000_000 + i * day_ms))
            .collect();
        let splits = split_by_timestamp(records);
        assert!(
            splits.len() >= 2,
            "Expected multiple sub-batches, got {}",
            splits.len()
        );
        let total: usize = splits.iter().map(|s| s.len()).sum();
        assert_eq!(total, 50);
    }

    #[test]
    fn test_split_by_timestamp_single_record() {
        let records = vec![make_record(1_000_000)];
        let splits = split_by_timestamp(records);
        assert_eq!(splits.len(), 1);
        assert_eq!(splits[0].len(), 1);
    }

    #[test]
    fn test_split_by_timestamp_empty() {
        let records: Vec<BackupRecord> = vec![];
        let splits = split_by_timestamp(records);
        assert!(splits.is_empty());
    }

    #[test]
    fn test_build_kafka_records_groups_into_single_batch() {
        use kafka_protocol::records::{RecordBatchDecoder, RecordBatchEncoder};

        let records: Vec<BackupRecord> = (0..5)
            .map(|i| make_record(1_700_000_000_000 + i * 1000))
            .collect();

        let kafka_records = build_kafka_records(&records, &ProducerBatch::NON_IDEMPOTENT);

        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(&mut buf, kafka_records.iter(), &options).unwrap();

        let mut read_buf = buf.freeze();
        let batches = RecordBatchDecoder::decode_all(&mut read_buf).unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].records.len(), 5);

        for (i, r) in batches[0].records.iter().enumerate() {
            assert_eq!(r.offset, i as i64);
        }
    }

    #[test]
    fn test_wide_timestamps_split_and_encode_separately() {
        use kafka_protocol::records::{RecordBatchDecoder, RecordBatchEncoder};

        let day_ms: i64 = 86_400_000;
        let records: Vec<BackupRecord> = (0..50)
            .map(|i| make_record(1_700_000_000_000 + i * day_ms))
            .collect();

        let sub_batches = split_by_timestamp(records);
        assert!(sub_batches.len() >= 2);

        let mut all_encoded = BytesMut::new();
        for sub_batch in sub_batches {
            let kafka_records = build_kafka_records(&sub_batch, &ProducerBatch::NON_IDEMPOTENT);
            let options = RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            };
            RecordBatchEncoder::encode(&mut all_encoded, kafka_records.iter(), &options).unwrap();
        }

        let mut read_buf = all_encoded.freeze();
        let batches = RecordBatchDecoder::decode_all(&mut read_buf).unwrap();
        assert!(batches.len() >= 2);

        let total: usize = batches.iter().map(|b| b.records.len()).sum();
        assert_eq!(total, 50);
    }

    #[test]
    fn test_narrow_timestamps_single_batch() {
        use kafka_protocol::records::{RecordBatchDecoder, RecordBatchEncoder};

        let records: Vec<BackupRecord> = (0..10)
            .map(|i| make_record(1_700_000_000_000 + i * 100))
            .collect();

        let kafka_records = build_kafka_records(&records, &ProducerBatch::NON_IDEMPOTENT);

        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(&mut buf, kafka_records.iter(), &options).unwrap();

        let mut read_buf = buf.freeze();
        let batches = RecordBatchDecoder::decode_all(&mut read_buf).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].records.len(), 10);
    }

    #[test]
    fn test_split_preserves_record_count_per_sub_batch() {
        let day_ms: i64 = 86_400_000;
        // 50 records spanning 50 days — must split into multiple sub-batches
        let records: Vec<BackupRecord> = (0..50)
            .map(|i| make_record(1_700_000_000_000 + i * day_ms))
            .collect();

        let sub_batches = split_by_timestamp(records);
        assert!(sub_batches.len() >= 2);

        // Verify total record count is preserved
        let total: usize = sub_batches.iter().map(|s| s.len()).sum();
        assert_eq!(total, 50);

        // Simulate what produce() would track as sub_batch_offsets
        let simulated_offsets: Vec<(i64, usize)> = sub_batches
            .iter()
            .enumerate()
            .map(|(i, batch)| (i as i64 * 1000, batch.len()))
            .collect();

        // Walk the simulated offsets the same way restore engine does
        let mut record_idx = 0;
        for (base, count) in &simulated_offsets {
            for j in 0..*count {
                let target_offset = base + j as i64;
                assert!(target_offset >= *base);
                assert!((target_offset - base) < *count as i64);
                record_idx += 1;
            }
        }
        assert_eq!(record_idx, 50);
    }

    /// Issue #150: the encoder must accept records whose timestamps are more
    /// than i32::MAX ms apart within one batch and write the delta as a
    /// varlong (kafka-protocol 0.17.0 bailed with "Timestamps within batch
    /// are too far apart"); the decoder must read it back exactly.
    #[test]
    fn test_encoder_round_trips_timestamp_delta_beyond_i32() {
        use kafka_protocol::records::{RecordBatchDecoder, RecordBatchEncoder};

        let now = 1_787_086_231_121i64;
        let kafka_records = build_kafka_records(
            &[make_record(0), make_record(now)],
            &ProducerBatch::NON_IDEMPOTENT,
        );
        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(&mut buf, kafka_records.iter(), &options)
            .expect("batch spanning decades must encode (varlong timestampDelta)");

        let mut read_buf = buf.freeze();
        let batches = RecordBatchDecoder::decode_all(&mut read_buf).unwrap();
        assert_eq!(batches.len(), 1);
        let ts: Vec<i64> = batches[0].records.iter().map(|r| r.timestamp).collect();
        assert_eq!(ts, vec![0, now]);
        assert!(now > i32::MAX as i64);
    }
}
