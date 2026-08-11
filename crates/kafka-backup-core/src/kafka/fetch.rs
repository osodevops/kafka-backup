//! Kafka Fetch API implementation.

use bytes::Bytes;
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse as KafkaFetchResponse, ListOffsetsRequest,
    TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Record, RecordBatchDecoder};
use tracing::debug;

use super::KafkaClient;
use crate::error::KafkaError;
use crate::manifest::BackupRecord;
use crate::Result;

/// Response from a fetch operation
#[derive(Debug)]
pub struct FetchResponse {
    /// Records fetched
    pub records: Vec<BackupRecord>,
    /// High watermark for the partition
    pub high_watermark: i64,
    /// Log start offset for the partition
    pub log_start_offset: i64,
    /// Next offset to fetch
    pub next_offset: i64,
}

/// Fetch records from a topic/partition
pub async fn fetch(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    offset: i64,
    max_bytes: i32,
) -> Result<FetchResponse> {
    let fetch_partition = kafka_protocol::messages::fetch_request::FetchPartition::default()
        .with_partition(partition)
        .with_fetch_offset(offset)
        .with_partition_max_bytes(max_bytes)
        .with_log_start_offset(-1);

    let fetch_topic = kafka_protocol::messages::fetch_request::FetchTopic::default()
        .with_topic(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partitions(vec![fetch_partition]);

    let request = FetchRequest::default()
        .with_replica_id(BrokerId(-1)) // Client mode
        .with_max_wait_ms(500)
        .with_min_bytes(1)
        .with_max_bytes(max_bytes)
        .with_isolation_level(0) // READ_UNCOMMITTED
        .with_topics(vec![fetch_topic]);

    let response: KafkaFetchResponse = client.send_request(ApiKey::Fetch, request).await?;

    if response.throttle_time_ms > 0 {
        debug!(
            "fetch_throttled: {}:{} throttle_time_ms={} offset={}",
            topic, partition, response.throttle_time_ms, offset
        );
    }

    // Parse the response
    let mut records = Vec::new();
    let mut high_watermark = 0i64;
    let mut log_start_offset = 0i64;
    let mut next_offset = offset;

    for topic_response in &response.responses {
        if topic_response.topic.as_str() != topic {
            continue;
        }

        for partition_response in &topic_response.partitions {
            if partition_response.partition_index != partition {
                continue;
            }

            // Check for errors
            if partition_response.error_code != 0 {
                return Err(KafkaError::BrokerError {
                    code: partition_response.error_code,
                    message: format!(
                        "Fetch error for {}:{}: code {}",
                        topic, partition, partition_response.error_code
                    ),
                }
                .into());
            }

            high_watermark = partition_response.high_watermark;
            log_start_offset = partition_response.log_start_offset;

            // Decode record batches
            if let Some(ref records_data) = partition_response.records {
                if !records_data.is_empty() {
                    let (decoded, batch_next_offset) = decode_fetch_data(records_data, offset)?;
                    records = decoded;
                    next_offset = batch_next_offset;
                }
            }
        }
    }

    debug!(
        "fetch_result: {}:{} records={} high_watermark={} next_offset={} offset_start={}",
        topic,
        partition,
        records.len(),
        high_watermark,
        next_offset,
        offset
    );

    Ok(FetchResponse {
        records,
        high_watermark,
        log_start_offset,
        next_offset,
    })
}

/// Byte positions within a v2 record batch: base_offset 0..8,
/// batch_length 8..12 (length of everything after itself), then
/// partition_leader_epoch, magic, crc, attributes, last_offset_delta 23..27.
const BATCH_LENGTH_END: usize = 12;
const LAST_OFFSET_DELTA_RANGE: std::ops::Range<usize> = 23..27;

/// Decode fetched record data and compute the next offset to fetch.
///
/// Records below `fetch_offset` are filtered out: the broker returns whole
/// batches, so a fetch offset inside a batch also returns the records before
/// it, which the caller has already processed.
///
/// The next offset is derived from each batch's header offset range
/// (base_offset + last_offset_delta + 1), NOT from the decoded records. On a
/// compacted topic the log cleaner can remove records from a batch — including
/// its entire tail — while the batch keeps its original offset range. Deriving
/// the next offset from the last decoded record would make the fetch loop
/// re-request the same batch forever (stalled backup, issue #54 in the
/// operator repo).
///
/// The Kafka Fetch response may contain a truncated batch at the end when the
/// response hits max_bytes; it is detected via the batch_length header and
/// ignored.
fn decode_fetch_data(data: &Bytes, fetch_offset: i64) -> Result<(Vec<BackupRecord>, i64)> {
    let mut records = Vec::new();
    let mut next_offset = fetch_offset;

    let total = data.len();
    let mut pos = 0usize;

    while total - pos >= LAST_OFFSET_DELTA_RANGE.end {
        let base_offset = i64::from_be_bytes(data[pos..pos + 8].try_into().unwrap());
        let batch_length =
            i32::from_be_bytes(data[pos + 8..pos + BATCH_LENGTH_END].try_into().unwrap());
        if batch_length < 0 {
            return Err(KafkaError::Protocol(format!(
                "Negative record batch length: {batch_length}"
            ))
            .into());
        }
        let batch_total = BATCH_LENGTH_END + batch_length as usize;
        if batch_total > total - pos {
            // Truncated trailing batch — safe to ignore; it will be fetched
            // again in full on the next request.
            break;
        }

        let mut batch_buf = data.slice(pos..pos + batch_total);
        let record_set = RecordBatchDecoder::decode(&mut batch_buf).map_err(|e| {
            crate::Error::from(KafkaError::Protocol(format!(
                "Failed to decode records: {:?}",
                e
            )))
        })?;

        for record in &record_set.records {
            if record.offset >= fetch_offset {
                records.push(convert_record(record));
            }
        }

        let last_offset_delta = i32::from_be_bytes(
            data[pos + LAST_OFFSET_DELTA_RANGE.start..pos + LAST_OFFSET_DELTA_RANGE.end]
                .try_into()
                .unwrap(),
        );
        next_offset = next_offset.max(base_offset + last_offset_delta as i64 + 1);

        pos += batch_total;
    }

    Ok((records, next_offset))
}

/// Convert a kafka-protocol Record to our BackupRecord
fn convert_record(record: &Record) -> BackupRecord {
    let headers: Vec<_> = record
        .headers
        .iter()
        .map(|(key, value)| crate::manifest::RecordHeader {
            key: key.to_string(),
            value: value.as_ref().map(|v| v.to_vec()).unwrap_or_default(),
        })
        .collect();

    BackupRecord {
        key: record.key.as_ref().map(|k| k.to_vec()),
        value: record.value.as_ref().map(|v| v.to_vec()),
        headers,
        timestamp: record.timestamp,
        offset: record.offset,
    }
}

/// Batch get offsets for multiple topic-partitions in a single ListOffsets request.
///
/// Takes a list of (topic, partition, timestamp) tuples and sends them all in one
/// batched request to the broker. This is critical for performance on high-latency
/// connections where per-partition requests would serialize through the connection mutex.
///
/// Timestamp values: -2 = earliest, -1 = latest
pub async fn batch_get_offsets(
    client: &KafkaClient,
    requests: &[(String, i32, i64)], // (topic, partition, timestamp)
) -> Result<std::collections::HashMap<(String, i32), i64>> {
    use std::collections::HashMap;

    // Group by topic
    let mut topics_map: HashMap<&str, Vec<(i32, i64)>> = HashMap::new();
    for (topic, partition, timestamp) in requests {
        topics_map
            .entry(topic.as_str())
            .or_default()
            .push((*partition, *timestamp));
    }

    let topics: Vec<_> = topics_map
        .into_iter()
        .map(|(topic, partitions)| {
            let partition_data: Vec<_> = partitions
                .into_iter()
                .map(|(partition, timestamp)| {
                    kafka_protocol::messages::list_offsets_request::ListOffsetsPartition::default()
                        .with_partition_index(partition)
                        .with_timestamp(timestamp)
                })
                .collect();

            kafka_protocol::messages::list_offsets_request::ListOffsetsTopic::default()
                .with_name(TopicName(StrBytes::from_string(topic.to_string())))
                .with_partitions(partition_data)
        })
        .collect();

    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1))
        .with_isolation_level(0)
        .with_topics(topics);

    let response: kafka_protocol::messages::ListOffsetsResponse =
        client.send_request(ApiKey::ListOffsets, request).await?;

    let mut results = HashMap::new();
    for topic_response in &response.topics {
        for partition_response in &topic_response.partitions {
            if partition_response.error_code != 0 {
                return Err(KafkaError::BrokerError {
                    code: partition_response.error_code,
                    message: format!(
                        "ListOffsets error for {}:{}: code {}",
                        topic_response.name.as_str(),
                        partition_response.partition_index,
                        partition_response.error_code
                    ),
                }
                .into());
            }

            results.insert(
                (
                    topic_response.name.to_string(),
                    partition_response.partition_index,
                ),
                partition_response.offset,
            );
        }
    }

    debug!(
        "batch_get_offsets: {} results from {} requests",
        results.len(),
        requests.len()
    );

    Ok(results)
}

/// Get the earliest and latest offsets for a partition
pub async fn get_offsets(client: &KafkaClient, topic: &str, partition: i32) -> Result<(i64, i64)> {
    // Fetch earliest offset (timestamp = -2)
    let earliest = list_offset(client, topic, partition, -2).await?;
    // Fetch latest offset (timestamp = -1)
    let latest = list_offset(client, topic, partition, -1).await?;

    debug!(
        "Offsets for {}:{}: earliest={}, latest={}",
        topic, partition, earliest, latest
    );

    Ok((earliest, latest))
}

/// List offset for a specific timestamp
async fn list_offset(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    timestamp: i64,
) -> Result<i64> {
    let list_partition =
        kafka_protocol::messages::list_offsets_request::ListOffsetsPartition::default()
            .with_partition_index(partition)
            .with_timestamp(timestamp);

    let list_topic = kafka_protocol::messages::list_offsets_request::ListOffsetsTopic::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partitions(vec![list_partition]);

    let request = ListOffsetsRequest::default()
        .with_replica_id(BrokerId(-1)) // Client mode
        .with_isolation_level(0) // READ_UNCOMMITTED
        .with_topics(vec![list_topic]);

    let response: kafka_protocol::messages::ListOffsetsResponse =
        client.send_request(ApiKey::ListOffsets, request).await?;

    for topic_response in &response.topics {
        if topic_response.name.as_str() != topic {
            continue;
        }

        for partition_response in &topic_response.partitions {
            if partition_response.partition_index != partition {
                continue;
            }

            if partition_response.error_code != 0 {
                return Err(KafkaError::BrokerError {
                    code: partition_response.error_code,
                    message: format!(
                        "ListOffsets error for {}:{}: code {}",
                        topic, partition, partition_response.error_code
                    ),
                }
                .into());
            }

            return Ok(partition_response.offset);
        }
    }

    Err(KafkaError::PartitionNotAvailable {
        topic: topic.to_string(),
        partition,
    }
    .into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use indexmap::IndexMap;
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
        NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID,
    };

    /// Byte layout of a v2 record batch header (relative to batch start):
    /// base_offset 0..8, batch_length 8..12, partition_leader_epoch 12..16,
    /// magic 16, crc 17..21, attributes 21..23, last_offset_delta 23..27.
    const CRC_RANGE_START: usize = 21;
    const CRC_OFFSET: usize = 17;
    const LAST_OFFSET_DELTA_OFFSET: usize = 23;

    fn make_record(offset: i64) -> Record {
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset,
            // Keep offset - sequence constant so the encoder packs all
            // records into a single batch instead of one batch per record.
            sequence: offset as i32,
            timestamp: 1_700_000_000_000 + offset,
            key: Some(Bytes::from(format!("key-{offset}"))),
            value: Some(Bytes::from(format!("value-{offset}"))),
            headers: IndexMap::new(),
        }
    }

    /// Encode a single v2 batch containing records at the given offsets.
    fn encode_batch(offsets: &[i64]) -> Vec<u8> {
        let records: Vec<Record> = offsets.iter().copied().map(make_record).collect();
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode failed");
        buf.to_vec()
    }

    /// Simulate log compaction removing the tail records of a batch: the
    /// broker preserves the batch's original last_offset_delta even though
    /// the records covering those offsets are gone. Patches the header and
    /// recomputes the CRC (crc32c over attributes..end).
    fn patch_last_offset_delta(batch: &mut [u8], new_delta: i32) {
        batch[LAST_OFFSET_DELTA_OFFSET..LAST_OFFSET_DELTA_OFFSET + 4]
            .copy_from_slice(&new_delta.to_be_bytes());
        let crc = crc32c::crc32c(&batch[CRC_RANGE_START..]);
        batch[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
    }

    fn parse(data: Vec<u8>, fetch_offset: i64) -> (Vec<BackupRecord>, i64) {
        decode_fetch_data(&Bytes::from(data), fetch_offset).expect("decode failed")
    }

    #[test]
    fn normal_batch_returns_all_records_and_advances() {
        let data = encode_batch(&[100, 101, 102, 103, 104]);
        let (records, next_offset) = parse(data, 100);
        assert_eq!(records.len(), 5);
        assert_eq!(records[0].offset, 100);
        assert_eq!(records[4].offset, 104);
        assert_eq!(next_offset, 105);
    }

    #[test]
    fn multiple_batches_decode_in_order() {
        let mut data = encode_batch(&[100, 101, 102]);
        data.extend_from_slice(&encode_batch(&[103, 104]));
        let (records, next_offset) = parse(data, 100);
        assert_eq!(records.len(), 5);
        assert_eq!(next_offset, 105);
    }

    /// Issue osodevops/strimzi-backup-operator#54: on a compacted topic the
    /// tail records of a batch can be removed while the batch keeps its
    /// original offset range. Fetching at an offset inside that removed tail
    /// returns the same batch again; next_offset must advance past the whole
    /// batch (base_offset + last_offset_delta + 1), not stall at the last
    /// surviving record.
    #[test]
    fn compacted_batch_tail_advances_past_batch_end() {
        let mut batch = encode_batch(&[100, 101, 102, 103, 104]);
        // Pretend offsets 105..=109 existed but were compacted away.
        patch_last_offset_delta(&mut batch, 9);

        let (records, next_offset) = parse(batch, 105);
        assert!(
            records.is_empty(),
            "records below the fetch offset must be filtered out, got {:?}",
            records.iter().map(|r| r.offset).collect::<Vec<_>>()
        );
        assert_eq!(
            next_offset, 110,
            "next_offset must advance past the batch's full offset range"
        );
    }

    /// The exact stall shape from the issue #54 log: fetch at offset N
    /// repeatedly returns records=1 with next_offset == N.
    #[test]
    fn compacted_batch_makes_progress_from_mid_batch_offset() {
        let mut batch = encode_batch(&[2_961_390, 2_961_395]);
        // Batch originally spanned 2961390..=2961399; tail compacted away.
        patch_last_offset_delta(&mut batch, 9);

        let (records, next_offset) = parse(batch, 2_961_396);
        assert!(records.is_empty());
        assert!(
            next_offset > 2_961_396,
            "next_offset {next_offset} must make progress past the fetch offset"
        );
        assert_eq!(next_offset, 2_961_400);
    }

    /// The broker returns whole batches: a fetch offset in the middle of a
    /// batch returns records before it, which must not be re-emitted
    /// (they were already backed up).
    #[test]
    fn records_below_fetch_offset_are_filtered() {
        let data = encode_batch(&[100, 101, 102, 103, 104]);
        let (records, next_offset) = parse(data, 103);
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![103, 104]
        );
        assert_eq!(next_offset, 105);
    }

    /// A batch whose records were ALL compacted away (record_count = 0)
    /// must still advance next_offset so the engine does not treat it as
    /// end-of-partition.
    #[test]
    fn fully_compacted_batch_still_advances() {
        let mut full = encode_batch(&[200, 201, 202]);
        // Rewrite the batch to contain zero records but keep its offset
        // range, as the log cleaner does: record_count (bytes 57..61) = 0,
        // truncate the record payload after the 61-byte header.
        full.truncate(61);
        full[57..61].copy_from_slice(&0i32.to_be_bytes());
        // Fix batch_length (bytes 8..12): total - 12.
        let batch_length = (full.len() - 12) as i32;
        full[8..12].copy_from_slice(&batch_length.to_be_bytes());
        patch_last_offset_delta(&mut full, 2);

        let (records, next_offset) = parse(full, 200);
        assert!(records.is_empty());
        assert_eq!(next_offset, 203);
    }

    /// A truncated trailing batch (response hit max_bytes mid-batch) is
    /// ignored; next_offset only covers the complete batches.
    #[test]
    fn truncated_trailing_batch_is_ignored() {
        let mut data = encode_batch(&[100, 101, 102]);
        let second = encode_batch(&[103, 104]);
        data.extend_from_slice(&second[..second.len() - 7]);
        let (records, next_offset) = parse(data, 100);
        assert_eq!(records.len(), 3);
        assert_eq!(next_offset, 103);
    }

    #[test]
    fn empty_data_returns_no_progress() {
        let (records, next_offset) = parse(Vec::new(), 42);
        assert!(records.is_empty());
        assert_eq!(next_offset, 42);
    }
}
