//! Producer-identity plumbing for idempotent and transactional producers
//! (KIP-98): the transaction coordinator lookup, `InitProducerId`,
//! `AddPartitionsToTxn`, `EndTxn`, and record batches stamped with a producer
//! id, epoch and base sequence.
//!
//! Each call is one request: retriable broker errors (see
//! [`is_retriable_transaction_error`]) are returned to the caller, which
//! decides how to back off. Batches are encoded once by
//! [`encode_producer_batch`] and sent by [`produce_encoded_batch`], so a
//! retry resends identical bytes — what the broker's sequence check expects.

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::messages::add_partitions_to_txn_request::AddPartitionsToTxnTopic;
use kafka_protocol::messages::{
    AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, ApiKey, EndTxnRequest, EndTxnResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, InitProducerIdRequest, InitProducerIdResponse,
    ProduceRequest, ProduceResponse as KafkaProduceResponse, ProducerId, TopicName,
    TransactionalId,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH,
};
use std::sync::Arc;

use super::consumer_groups::coordinator_from_response;
use super::{GroupCoordinator, KafkaClient};
use crate::config::CompressionType;
use crate::error::KafkaError;
use crate::manifest::BackupRecord;
use crate::Result;

/// `COORDINATOR_LOAD_IN_PROGRESS`
pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
/// `COORDINATOR_NOT_AVAILABLE`
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// `NOT_COORDINATOR`
pub const NOT_COORDINATOR: i16 = 16;
/// `CONCURRENT_TRANSACTIONS`: the coordinator is still completing the
/// producer's previous transaction.
pub const CONCURRENT_TRANSACTIONS: i16 = 51;

/// FindCoordinator `key_type` of a transaction coordinator.
const KEY_TYPE_TRANSACTION: i8 = 1;

/// A producer's identity from `InitProducerId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerIdAndEpoch {
    /// Producer id assigned by the broker.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
}

impl ProducerIdAndEpoch {
    /// No identity yet: what `InitProducerId` is first sent with.
    pub const NONE: ProducerIdAndEpoch = ProducerIdAndEpoch {
        producer_id: -1,
        producer_epoch: -1,
    };
}

/// The producer fields stamped on one record batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatch {
    /// Producer id and epoch.
    pub producer: ProducerIdAndEpoch,
    /// Sequence number of the batch's first record for this partition.
    pub base_sequence: i32,
    /// Whether the batch is part of a transaction.
    pub transactional: bool,
}

/// Whether a broker error code is worth retrying for the transactional
/// requests in this module: the coordinator is loading or moved, or is still
/// completing the previous transaction.
pub fn is_retriable_transaction_error(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS
            | COORDINATOR_NOT_AVAILABLE
            | NOT_COORDINATOR
            | CONCURRENT_TRANSACTIONS
    )
}

fn broker_error(code: i16, message: String) -> crate::Error {
    KafkaError::BrokerError { code, message }.into()
}

/// Find the transaction coordinator for `transactional_id`.
pub async fn find_transaction_coordinator(
    client: &KafkaClient,
    transactional_id: &str,
) -> Result<GroupCoordinator> {
    let request = FindCoordinatorRequest::default()
        .with_key(StrBytes::from_string(transactional_id.to_string()))
        .with_key_type(KEY_TYPE_TRANSACTION);
    let response: FindCoordinatorResponse = client
        .send_request(ApiKey::FindCoordinator, request)
        .await?;
    coordinator_from_response("transactional id", transactional_id, response)
}

/// A connection to the transaction coordinator for `transactional_id`
/// (cached on `client` while the coordinator's address is unchanged).
pub async fn transaction_coordinator_connection(
    client: &KafkaClient,
    transactional_id: &str,
) -> Result<Arc<KafkaClient>> {
    let coordinator = find_transaction_coordinator(client, transactional_id).await?;
    if coordinator.node_id < 0 {
        return Err(broker_error(
            COORDINATOR_NOT_AVAILABLE,
            format!(
                "FindCoordinator returned no coordinator for transactional id {transactional_id}"
            ),
        ));
    }
    let addr = format!("{}:{}", coordinator.host, coordinator.port);
    client
        .coordinator_connection(coordinator.node_id, &addr)
        .await
}

/// `InitProducerId`: obtain a producer id and epoch.
///
/// - Idempotent producer: `transactional_id` is `None`; any broker answers.
/// - Transactional producer: send to the transaction coordinator with the
///   transactional id. This fences older producers with the same id and
///   aborts any transaction they left open.
///
/// `current` is [`ProducerIdAndEpoch::NONE`] for a new producer, or the
/// producer's current identity to bump its epoch (KIP-360, after
/// `UNKNOWN_PRODUCER_ID` or `INVALID_PRODUCER_EPOCH`).
pub async fn init_producer_id(
    client: &KafkaClient,
    transactional_id: Option<&str>,
    transaction_timeout_ms: i32,
    current: ProducerIdAndEpoch,
) -> Result<ProducerIdAndEpoch> {
    let request = InitProducerIdRequest::default()
        .with_transactional_id(
            transactional_id.map(|id| TransactionalId(StrBytes::from_string(id.to_string()))),
        )
        .with_transaction_timeout_ms(transaction_timeout_ms)
        .with_producer_id(ProducerId(current.producer_id))
        .with_producer_epoch(current.producer_epoch);
    let response: InitProducerIdResponse =
        client.send_request(ApiKey::InitProducerId, request).await?;
    if response.error_code != 0 {
        return Err(broker_error(
            response.error_code,
            format!(
                "InitProducerId failed{}: error code {}",
                transactional_id
                    .map(|id| format!(" for transactional id {id}"))
                    .unwrap_or_default(),
                response.error_code
            ),
        ));
    }
    Ok(ProducerIdAndEpoch {
        producer_id: response.producer_id.0,
        producer_epoch: response.producer_epoch,
    })
}

/// `AddPartitionsToTxn`: register partitions with the producer's open
/// transaction before producing to them. Send to the transaction
/// coordinator. Fails with the first partition error.
pub async fn add_partitions_to_txn(
    coordinator: &KafkaClient,
    transactional_id: &str,
    producer: ProducerIdAndEpoch,
    partitions: &[(String, i32)],
) -> Result<()> {
    let mut by_topic: IndexMap<&str, Vec<i32>> = IndexMap::new();
    for (topic, partition) in partitions {
        by_topic.entry(topic.as_str()).or_default().push(*partition);
    }
    let topics = by_topic
        .into_iter()
        .map(|(topic, partitions)| {
            AddPartitionsToTxnTopic::default()
                .with_name(TopicName(StrBytes::from_string(topic.to_string())))
                .with_partitions(partitions)
        })
        .collect();
    let request = AddPartitionsToTxnRequest::default()
        .with_v3_and_below_transactional_id(TransactionalId(StrBytes::from_string(
            transactional_id.to_string(),
        )))
        .with_v3_and_below_producer_id(ProducerId(producer.producer_id))
        .with_v3_and_below_producer_epoch(producer.producer_epoch)
        .with_v3_and_below_topics(topics);
    let response: AddPartitionsToTxnResponse = coordinator
        .send_request(ApiKey::AddPartitionsToTxn, request)
        .await?;
    if response.error_code != 0 {
        return Err(broker_error(
            response.error_code,
            format!(
                "AddPartitionsToTxn failed for {transactional_id}: error code {}",
                response.error_code
            ),
        ));
    }
    for topic in &response.results_by_topic_v3_and_below {
        for result in &topic.results_by_partition {
            if result.partition_error_code != 0 {
                return Err(broker_error(
                    result.partition_error_code,
                    format!(
                        "AddPartitionsToTxn failed for {transactional_id} on {}[{}]: error code {}",
                        topic.name.as_str(),
                        result.partition_index,
                        result.partition_error_code
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// `EndTxn`: commit (`commit = true`) or abort the producer's open
/// transaction. Send to the transaction coordinator. The coordinator writes
/// the COMMIT/ABORT markers to the partitions afterwards; the partitions'
/// last stable offsets advance once they are written.
pub async fn end_txn(
    coordinator: &KafkaClient,
    transactional_id: &str,
    producer: ProducerIdAndEpoch,
    commit: bool,
) -> Result<()> {
    let request = EndTxnRequest::default()
        .with_transactional_id(TransactionalId(StrBytes::from_string(
            transactional_id.to_string(),
        )))
        .with_producer_id(ProducerId(producer.producer_id))
        .with_producer_epoch(producer.producer_epoch)
        .with_committed(commit);
    let response: EndTxnResponse = coordinator.send_request(ApiKey::EndTxn, request).await?;
    if response.error_code != 0 {
        return Err(broker_error(
            response.error_code,
            format!(
                "EndTxn({}) failed for {transactional_id}: error code {}",
                if commit { "commit" } else { "abort" },
                response.error_code
            ),
        ));
    }
    Ok(())
}

/// Encode `records` as one v2 record batch carrying the producer fields of
/// `batch`. The bytes are what [`produce_encoded_batch`] sends; keep them
/// to resend the identical batch on a retry.
pub fn encode_producer_batch(
    records: &[BackupRecord],
    batch: &ProducerBatch,
    compression: CompressionType,
) -> Result<Bytes> {
    let compression = match compression {
        CompressionType::None => Compression::None,
        CompressionType::Zstd => Compression::Zstd,
        CompressionType::Lz4 => Compression::Lz4,
    };
    if records.is_empty() {
        return Err(KafkaError::Protocol("cannot encode an empty record batch".to_string()).into());
    }
    let kafka_records: Vec<Record> = records
        .iter()
        .enumerate()
        .map(|(i, r)| Record {
            transactional: batch.transactional,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: batch.producer.producer_id,
            producer_epoch: batch.producer.producer_epoch,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            // offset - sequence is constant, so the encoder writes a single
            // batch whose base sequence is `base_sequence`.
            sequence: batch.base_sequence.wrapping_add(i as i32),
            timestamp: r.timestamp,
            key: r.key.clone().map(Bytes::from),
            value: r.value.clone().map(Bytes::from),
            headers: r
                .headers
                .iter()
                .map(|h| {
                    (
                        StrBytes::from_string(h.key.clone()),
                        h.value.clone().map(Bytes::from),
                    )
                })
                .collect(),
        })
        .collect();
    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        kafka_records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression,
        },
    )
    .map_err(|e| KafkaError::Protocol(format!("Failed to encode records: {:?}", e)))?;
    Ok(buf.freeze())
}

/// Produce one encoded batch (see [`encode_producer_batch`]) to the
/// partition's leader. `transactional_id` is required for a transactional
/// batch. Returns the base offset the broker assigned.
pub async fn produce_encoded_batch(
    leader: &KafkaClient,
    topic: &str,
    partition: i32,
    batch: Bytes,
    transactional_id: Option<&str>,
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
        .with_transactional_id(
            transactional_id.map(|id| TransactionalId(StrBytes::from_string(id.to_string()))),
        )
        .with_acks(acks)
        .with_timeout_ms(timeout_ms)
        .with_topic_data(vec![topic_data]);
    let response: KafkaProduceResponse = leader.send_request(ApiKey::Produce, request).await?;
    for topic_response in &response.responses {
        if topic_response.name.as_str() != topic {
            continue;
        }
        for p in &topic_response.partition_responses {
            if p.index != partition {
                continue;
            }
            if p.error_code != 0 {
                return Err(broker_error(
                    p.error_code,
                    format!(
                        "Produce error for {topic}:{partition}: code {}",
                        p.error_code
                    ),
                ));
            }
            return Ok(p.base_offset);
        }
    }
    Err(KafkaError::Protocol("No partition response in produce response".to_string()).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::records::RecordBatchDecoder;

    fn record(i: i64) -> BackupRecord {
        BackupRecord {
            key: Some(format!("k{i}").into_bytes()),
            value: Some(format!("v{i}").into_bytes()),
            headers: vec![crate::manifest::RecordHeader {
                key: "h".to_string(),
                value: None,
            }],
            timestamp: 1_700_000_000_000 + i,
            offset: 0,
        }
    }

    #[test]
    fn encoded_batch_carries_the_producer_fields_in_one_batch() {
        let records: Vec<BackupRecord> = (0..5).map(record).collect();
        let batch = ProducerBatch {
            producer: ProducerIdAndEpoch {
                producer_id: 4242,
                producer_epoch: 7,
            },
            base_sequence: 30,
            transactional: true,
        };
        let mut bytes = encode_producer_batch(&records, &batch, CompressionType::None).unwrap();
        let infos = RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).unwrap();
        assert_eq!(infos.len(), 1, "one batch");
        let info = &infos[0];
        assert_eq!(info.producer_id, 4242);
        assert_eq!(info.producer_epoch, 7);
        assert_eq!(info.base_sequence, 30);
        assert!(info.transactional);
        assert!(!info.control);
        assert_eq!(info.record_count, 5);

        let set = RecordBatchDecoder::decode(&mut bytes).unwrap();
        assert_eq!(set.records.len(), 5);
        assert_eq!(set.records[4].sequence, 34);
        assert_eq!(set.records[2].key.as_deref(), Some(&b"k2"[..]));
        assert_eq!(
            set.records[0].headers.get(&StrBytes::from_static_str("h")),
            Some(&None)
        );
    }

    #[test]
    fn idempotent_batch_is_not_transactional() {
        let batch = ProducerBatch {
            producer: ProducerIdAndEpoch {
                producer_id: 1,
                producer_epoch: 0,
            },
            base_sequence: 0,
            transactional: false,
        };
        let bytes = encode_producer_batch(&[record(0)], &batch, CompressionType::None).unwrap();
        let info = &RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).unwrap()[0];
        assert!(!info.transactional);
        assert_eq!(info.producer_id, 1);
        assert!(encode_producer_batch(&[], &batch, CompressionType::None).is_err());
    }

    #[test]
    fn retriable_transaction_errors() {
        for code in [14, 15, 16, 51] {
            assert!(is_retriable_transaction_error(code), "{code}");
        }
        for code in [0, 6, 47, 48, 53] {
            assert!(!is_retriable_transaction_error(code), "{code}");
        }
    }
}
