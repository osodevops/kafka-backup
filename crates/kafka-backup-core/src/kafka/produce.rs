//! Kafka Produce API implementation.

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::messages::{
    ApiKey, ProduceRequest, ProduceResponse as KafkaProduceResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};
use tracing::trace;

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

/// Produce records to a topic/partition
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

    // Convert BackupRecords to kafka-protocol Records
    let kafka_records: Vec<Record> = records
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
                partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: i as i64, // Will be assigned by broker, relative offset for encoding
                sequence: NO_SEQUENCE,
                timestamp: r.timestamp,
                key: r.key.map(Bytes::from),
                value: r.value.map(Bytes::from),
                headers,
            }
        })
        .collect();

    // Encode records into a record batch
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };

    let mut records_buf = BytesMut::new();
    RecordBatchEncoder::encode(&mut records_buf, kafka_records.iter(), &options)
        .map_err(|e| KafkaError::Protocol(format!("Failed to encode records: {:?}", e)))?;

    let records_bytes = records_buf.freeze();

    // Build produce request
    let partition_data = kafka_protocol::messages::produce_request::PartitionProduceData::default()
        .with_index(partition)
        .with_records(Some(records_bytes));

    let topic_data = kafka_protocol::messages::produce_request::TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partition_data(vec![partition_data]);

    let request = ProduceRequest::default()
        .with_acks(-1) // Wait for all replicas
        .with_timeout_ms(30000)
        .with_topic_data(vec![topic_data]);

    let response: KafkaProduceResponse = client.send_request(ApiKey::Produce, request).await?;

    // Parse response
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
                        "Produce error for {}:{}: code {}",
                        topic, partition, partition_response.error_code
                    ),
                }
                .into());
            }

            trace!(
                "Produced {} records to {}:{} at offset {}",
                record_count,
                topic,
                partition,
                partition_response.base_offset
            );

            return Ok(ProduceResponse {
                base_offset: partition_response.base_offset,
                error_code: partition_response.error_code,
                record_count,
            });
        }
    }

    Err(KafkaError::Protocol("No partition response in produce response".to_string()).into())
}
