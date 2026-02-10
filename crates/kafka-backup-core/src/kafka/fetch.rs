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
                    let decoded = decode_records(records_data)?;
                    for record in decoded {
                        next_offset = record.offset + 1;
                        records.push(record);
                    }
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

/// Decode records from raw bytes
fn decode_records(data: &Bytes) -> Result<Vec<BackupRecord>> {
    let mut buf = data.clone();

    let record_set = RecordBatchDecoder::decode(&mut buf)
        .map_err(|e| KafkaError::Protocol(format!("Failed to decode records: {:?}", e)))?;

    let records: Vec<BackupRecord> = record_set.records.iter().map(convert_record).collect();

    Ok(records)
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
