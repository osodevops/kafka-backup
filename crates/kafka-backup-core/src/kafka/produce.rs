//! Kafka Produce API implementation.

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::messages::{
    ApiKey, ProduceRequest, ProduceResponse as KafkaProduceResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType, NO_PRODUCER_EPOCH,
    NO_PRODUCER_ID,
};
use tracing::{debug, trace};

use super::KafkaClient;
use crate::error::KafkaError;
use crate::manifest::BackupRecord;
use crate::Result;

/// Response from a produce operation
#[derive(Debug)]
pub struct ProduceResponse {
    /// Base offset assigned to the first record
    pub base_offset: i64,
    /// Error code (0 = success)
    pub error_code: i16,
    /// Number of records produced
    pub record_count: usize,
}

/// Max timestamp delta within a single record batch.
/// Record batch v2 stores timestamp deltas as i32 (milliseconds).
/// i32::MAX ≈ 24.8 days.
const MAX_TIMESTAMP_DELTA_MS: i64 = i32::MAX as i64;

/// Split records into sub-batches where all timestamps within a sub-batch
/// are within MAX_TIMESTAMP_DELTA_MS of each other (tracking min/max).
/// Records maintain their original order.
fn split_by_timestamp(records: Vec<BackupRecord>) -> Vec<Vec<BackupRecord>> {
    if records.is_empty() {
        return vec![];
    }

    let mut batches = Vec::new();
    let mut current_batch = Vec::new();
    let mut ts_min = records[0].timestamp;
    let mut ts_max = ts_min;

    for record in records {
        let new_min = ts_min.min(record.timestamp);
        let new_max = ts_max.max(record.timestamp);

        if !current_batch.is_empty() && (new_max - new_min) > MAX_TIMESTAMP_DELTA_MS {
            batches.push(current_batch);
            current_batch = Vec::new();
            ts_min = record.timestamp;
            ts_max = record.timestamp;
        } else {
            ts_min = new_min;
            ts_max = new_max;
        }

        current_batch.push(record);
    }

    batches.push(current_batch);
    batches
}

/// Convert BackupRecords to kafka-protocol Records for non-idempotent produce.
///
/// Key field assignments:
/// - `partition_leader_epoch = 0` (v2 requires >= 0; -1 is only valid for v0/v1)
/// - `sequence = i - 1` (starts at -1 so base_sequence = -1/NO_SEQUENCE,
///   required by Kafka 2.7+ when producer_id = NO_PRODUCER_ID)
/// - `offset = i` (contiguous from 0; constant offset-sequence ensures
///   the encoder groups all records into a single batch)
fn build_kafka_records(records: Vec<BackupRecord>) -> Vec<Record> {
    records
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            let headers: IndexMap<StrBytes, Option<Bytes>> = r
                .headers
                .into_iter()
                .map(|h| (StrBytes::from_string(h.key), Some(Bytes::from(h.value))))
                .collect();

            Record {
                transactional: false,
                control: false,
                partition_leader_epoch: 0,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: i as i64,
                sequence: i as i32 - 1,
                timestamp: r.timestamp,
                key: r.key.map(Bytes::from),
                value: r.value.map(Bytes::from),
                headers,
            }
        })
        .collect()
}

/// Produce records to a topic/partition.
///
/// Records spanning more than ~24.8 days of timestamps are automatically
/// split into sub-batches sent as separate produce requests (record batch v2
/// stores timestamp deltas as i32).
pub async fn produce(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    records: Vec<BackupRecord>,
) -> Result<ProduceResponse> {
    if records.is_empty() {
        return Ok(ProduceResponse {
            base_offset: -1,
            error_code: 0,
            record_count: 0,
        });
    }

    let record_count = records.len();
    let sub_batches = split_by_timestamp(records);

    if sub_batches.len() > 1 {
        debug!(
            "Split {} records into {} sub-batches for {}:{} (timestamp range exceeds i32::MAX ms)",
            record_count,
            sub_batches.len(),
            topic,
            partition
        );
    }

    // Send each sub-batch as a separate produce request.
    // Each sub-batch encodes to a single record batch (same offset-sequence
    // invariant). Sending separately avoids issues with non-idempotent
    // producers and multiple record batches per request.
    let mut first_base_offset: Option<i64> = None;
    let mut total_produced = 0;

    for sub_batch in sub_batches {
        let sub_batch_len = sub_batch.len();
        let kafka_records = build_kafka_records(sub_batch);

        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };

        let mut records_buf = BytesMut::new();
        RecordBatchEncoder::encode(&mut records_buf, kafka_records.iter(), &options)
            .map_err(|e| KafkaError::Protocol(format!("Failed to encode records: {:?}", e)))?;

        let records_bytes = records_buf.freeze();

        let partition_data =
            kafka_protocol::messages::produce_request::PartitionProduceData::default()
                .with_index(partition)
                .with_records(Some(records_bytes));

        let topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default()
            .with_name(TopicName(StrBytes::from_string(topic.to_string())))
            .with_partition_data(vec![partition_data]);

        let request = ProduceRequest::default()
            .with_acks(-1)
            .with_timeout_ms(30000)
            .with_topic_data(vec![topic_data]);

        let response: KafkaProduceResponse = client.send_request(ApiKey::Produce, request).await?;

        let produce_result = parse_produce_response(&response, topic, partition)?;

        if first_base_offset.is_none() {
            first_base_offset = Some(produce_result.base_offset);
        }
        total_produced += sub_batch_len;

        trace!(
            "Produced {} records to {}:{} at offset {}",
            sub_batch_len,
            topic,
            partition,
            produce_result.base_offset
        );
    }

    Ok(ProduceResponse {
        base_offset: first_base_offset.unwrap_or(-1),
        error_code: 0,
        record_count: total_produced,
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
                        "Produce error for {}:{}: code {} (base_offset={}, log_append_time={}, log_start_offset={})",
                        topic, partition, partition_response.error_code,
                        partition_response.base_offset,
                        partition_response.log_append_time_ms,
                        partition_response.log_start_offset,
                    ),
                }
                .into());
            }
            return Ok(ProduceResponse {
                base_offset: partition_response.base_offset,
                error_code: 0,
                record_count: 0,
            });
        }
    }
    Err(KafkaError::Protocol("No partition response in produce response".to_string()).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use kafka_protocol::records::{
        Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions,
    };

    fn make_record(timestamp: i64) -> BackupRecord {
        BackupRecord {
            key: Some(b"key".to_vec()),
            value: Some(b"value".to_vec()),
            headers: vec![],
            timestamp,
            offset: 0,
        }
    }

    // --- split_by_timestamp ---

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

    // --- Encoding correctness ---

    #[test]
    fn test_original_encoding_created_one_batch_per_record() {
        // The original code set sequence = -1 (NO_SEQUENCE) for ALL records.
        // Since the encoder groups by constant (offset - sequence), and
        // offset-sequence = 1, 2, 3, ... (all different), each record got
        // its own batch. Combined with partition_leader_epoch = -1, every
        // batch was invalid for v2 → error 87.
        let records: Vec<Record> = (0..5)
            .map(|i| Record {
                transactional: false,
                control: false,
                partition_leader_epoch: -1,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: i as i64,
                sequence: -1,
                timestamp: 1_700_000_000_000 + i as i64 * 1000,
                key: Some(Bytes::from(b"key".to_vec())),
                value: Some(Bytes::from(b"value".to_vec())),
                headers: IndexMap::new(),
            })
            .collect();

        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(&mut buf, records.iter(), &options).unwrap();

        let mut read_buf = buf.freeze();
        let batches = RecordBatchDecoder::decode_all(&mut read_buf).unwrap();

        // Bug: 5 separate batches instead of 1
        assert_eq!(batches.len(), 5);
        // Bug: partition_leader_epoch = -1 (invalid for v2)
        assert_eq!(batches[0].records[0].partition_leader_epoch, -1);
    }

    #[test]
    fn test_build_kafka_records_groups_into_single_batch() {
        let records: Vec<BackupRecord> = (0..5)
            .map(|i| make_record(1_700_000_000_000 + i * 1000))
            .collect();

        let kafka_records = build_kafka_records(records);

        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(&mut buf, kafka_records.iter(), &options).unwrap();

        let mut read_buf = buf.freeze();
        let batches = RecordBatchDecoder::decode_all(&mut read_buf).unwrap();

        // All records in 1 batch (offset-sequence is constant)
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].records.len(), 5);

        // partition_leader_epoch = 0 (valid for v2)
        assert_eq!(batches[0].records[0].partition_leader_epoch, 0);

        // base_sequence = -1 (NO_SEQUENCE, required when producer_id = -1)
        assert_eq!(batches[0].records[0].sequence, -1);

        // Contiguous offsets
        for (i, r) in batches[0].records.iter().enumerate() {
            assert_eq!(r.offset, i as i64);
        }
    }

    #[test]
    fn test_wide_timestamps_split_and_encode_separately() {
        let day_ms: i64 = 86_400_000;
        let records: Vec<BackupRecord> = (0..50)
            .map(|i| make_record(1_700_000_000_000 + i * day_ms))
            .collect();

        let sub_batches = split_by_timestamp(records);
        assert!(sub_batches.len() >= 2);

        // Encode each sub-batch separately (as produce() does)
        let mut all_encoded = BytesMut::new();
        for sub_batch in sub_batches {
            let kafka_records = build_kafka_records(sub_batch);
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

        // Each sub-batch has valid fields
        for batch in &batches {
            assert_eq!(batch.records[0].partition_leader_epoch, 0);
            assert_eq!(batch.records[0].sequence, -1);
        }
    }

    #[test]
    fn test_narrow_timestamps_single_batch() {
        let records: Vec<BackupRecord> = (0..10)
            .map(|i| make_record(1_700_000_000_000 + i * 100))
            .collect();

        let kafka_records = build_kafka_records(records);

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
}
