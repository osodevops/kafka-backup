//! In-process single-broker mock that implements the broker side of the
//! idempotent producer, for restore produce-path tests (no Docker).
//!
//! It answers Metadata, ApiVersions, InitProducerId, DescribeConfigs,
//! ListOffsets and Produce. Produce requests are decoded and applied to a
//! per-partition log with Kafka's sequence rules (the last five batches of
//! each producer are remembered for de-duplication), and every attempt's
//! record-batch bytes are recorded so tests can assert that a retry resent
//! identical bytes. Faults can be queued for the next Produce requests:
//! apply-then-drop-the-connection (the response is lost), drop before
//! applying, an error code, a hang.
#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult,
};
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsResponse, BrokerId, DescribeConfigsResponse, FetchRequest, FetchResponse,
    InitProducerIdResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataResponse,
    ProduceRequest, ProduceResponse, ProducerId, RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use kafka_protocol::records::{
    Compression, Record, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
pub const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
pub const UNKNOWN_PRODUCER_ID: i16 = 59;
pub const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;

/// What the broker does with the next Produce request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// No fault: handle the request normally (a placeholder in a queue).
    Pass,
    /// Append the batch, then close the connection without answering.
    ApplyThenDrop,
    /// Close the connection without appending.
    DropBeforeApply,
    /// Answer with this error code without appending.
    Error(i16),
    /// Forget every producer's state, then handle the request normally.
    ForgetProducers,
    /// Never answer (the connection stays open).
    Hang,
}

/// One appended record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRecord {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    pub headers: Vec<(String, Option<Bytes>)>,
    pub timestamp: i64,
    pub producer_id: i64,
}

/// One Produce attempt as received.
#[derive(Debug, Clone)]
pub struct ProduceAttempt {
    pub partition: i32,
    pub batch: Bytes,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub record_count: i32,
}

#[derive(Debug, Default)]
struct ProducerEntry {
    epoch: i16,
    last_sequence: i32,
    recent: VecDeque<((i32, i32), i64)>,
}

#[derive(Debug)]
pub struct MockState {
    pub topic: String,
    pub partitions: i32,
    /// `max.message.bytes` DescribeConfigs reports; `None` answers with
    /// TOPIC_AUTHORIZATION_FAILED.
    pub max_message_bytes: Option<i64>,
    /// Error InitProducerId answers with, if any.
    pub init_producer_id_error: Option<i16>,
    /// Max InitProducerId version ApiVersions advertises.
    pub init_producer_id_max_version: i16,
    pub next_producer_id: i64,
    pub init_requests: usize,
    pub logs: HashMap<i32, Vec<StoredRecord>>,
    producers: HashMap<(i32, i64), ProducerEntry>,
    pub attempts: Vec<ProduceAttempt>,
    pub faults: VecDeque<Fault>,
    /// Faults applied only to Produce requests for this partition.
    pub partition_faults: HashMap<i32, VecDeque<Fault>>,
    /// Faults for the Produce attempt with this index (0-based, counting
    /// every attempt received), checked first.
    pub scheduled: HashMap<usize, Fault>,
}

impl MockState {
    pub fn high_watermark(&self, partition: i32) -> i64 {
        self.logs.get(&partition).map_or(0, |l| l.len() as i64)
    }
}

pub struct MockBroker {
    pub addr: SocketAddr,
    pub state: Arc<Mutex<MockState>>,
    handle: JoinHandle<()>,
}

impl MockBroker {
    pub async fn start(topic: &str, partitions: i32) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let state = Arc::new(Mutex::new(MockState {
            topic: topic.to_string(),
            partitions,
            max_message_bytes: Some(1_048_588),
            init_producer_id_error: None,
            init_producer_id_max_version: 4,
            next_producer_id: 4000,
            init_requests: 0,
            logs: HashMap::new(),
            producers: HashMap::new(),
            attempts: Vec::new(),
            faults: VecDeque::new(),
            partition_faults: HashMap::new(),
            scheduled: HashMap::new(),
        }));
        let st = state.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(serve(stream, addr, st.clone()));
            }
        });
        Self {
            addr,
            state,
            handle,
        }
    }

    pub fn bootstrap(&self) -> String {
        self.addr.to_string()
    }

    pub fn queue_faults(&self, faults: &[Fault]) {
        self.state
            .lock()
            .unwrap()
            .faults
            .extend(faults.iter().copied());
    }

    /// Apply `fault` to the Produce attempt with index `attempt`.
    pub fn schedule(&self, attempt: usize, fault: Fault) {
        self.state.lock().unwrap().scheduled.insert(attempt, fault);
    }

    pub fn queue_partition_faults(&self, partition: i32, faults: &[Fault]) {
        self.state
            .lock()
            .unwrap()
            .partition_faults
            .entry(partition)
            .or_default()
            .extend(faults.iter().copied());
    }

    pub fn log(&self, partition: i32) -> Vec<StoredRecord> {
        self.state
            .lock()
            .unwrap()
            .logs
            .get(&partition)
            .cloned()
            .unwrap_or_default()
    }

    pub fn attempts(&self) -> Vec<ProduceAttempt> {
        self.state.lock().unwrap().attempts.clone()
    }

    /// Append records as another producer would (no producer id).
    pub fn append_foreign(&self, partition: i32, values: &[&[u8]]) {
        let mut state = self.state.lock().unwrap();
        let log = state.logs.entry(partition).or_default();
        for v in values {
            log.push(StoredRecord {
                key: None,
                value: Some(Bytes::copy_from_slice(v)),
                headers: vec![],
                timestamp: 0,
                producer_id: -1,
            });
        }
    }
}

impl Drop for MockBroker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    if sequence > i32::MAX - increment {
        increment - (i32::MAX - sequence) - 1
    } else {
        sequence + increment
    }
}

/// Kafka's idempotent append rules, simplified to one broker.
fn append(state: &mut MockState, partition: i32, batch: &Bytes) -> Result<i64, i16> {
    let info =
        RecordBatchDecoder::decode_batch_info(&mut batch.clone()).expect("batch info")[0].clone();
    let count = info.record_count;
    let base = state.high_watermark(partition);
    if info.producer_id >= 0 {
        let key = (partition, info.producer_id);
        match state.producers.get(&key) {
            None if info.base_sequence != 0 => return Err(UNKNOWN_PRODUCER_ID),
            Some(e) if info.producer_epoch < e.epoch => return Err(INVALID_PRODUCER_EPOCH),
            Some(e) if info.producer_epoch == e.epoch => {
                if let Some((_, offset)) = e
                    .recent
                    .iter()
                    .find(|((seq, n), _)| *seq == info.base_sequence && *n == count)
                {
                    return Ok(*offset);
                }
                if info.base_sequence != increment_sequence(e.last_sequence, 1) {
                    return Err(if info.base_sequence <= e.last_sequence {
                        DUPLICATE_SEQUENCE_NUMBER
                    } else {
                        OUT_OF_ORDER_SEQUENCE_NUMBER
                    });
                }
            }
            Some(_) if info.base_sequence != 0 => return Err(OUT_OF_ORDER_SEQUENCE_NUMBER),
            _ => {}
        }
        let entry = state.producers.entry(key).or_default();
        if entry.epoch != info.producer_epoch {
            entry.recent.clear();
        }
        entry.epoch = info.producer_epoch;
        entry.last_sequence = increment_sequence(info.base_sequence, count - 1);
        entry.recent.push_back(((info.base_sequence, count), base));
        if entry.recent.len() > 5 {
            entry.recent.pop_front();
        }
    }
    let records = RecordBatchDecoder::decode(&mut batch.clone()).expect("decode records");
    let log = state.logs.entry(partition).or_default();
    for r in records.records {
        log.push(StoredRecord {
            key: r.key,
            value: r.value,
            headers: r
                .headers
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            timestamp: r.timestamp,
            producer_id: info.producer_id,
        });
    }
    Ok(base)
}

async fn serve(mut stream: TcpStream, addr: SocketAddr, state: Arc<Mutex<MockState>>) {
    while let Some((api_key, version, correlation_id, mut body)) = read_request(&mut stream).await {
        let mut out = BytesMut::new();
        match api_key {
            ApiKey::Metadata => {
                let state = state.lock().unwrap();
                let partitions = (0..state.partitions)
                    .map(|p| {
                        MetadataResponsePartition::default()
                            .with_partition_index(p)
                            .with_leader_id(BrokerId(1))
                            .with_replica_nodes(vec![BrokerId(1)])
                            .with_isr_nodes(vec![BrokerId(1)])
                    })
                    .collect();
                MetadataResponse::default()
                    .with_brokers(vec![MetadataResponseBroker::default()
                        .with_node_id(BrokerId(1))
                        .with_host(StrBytes::from_string(addr.ip().to_string()))
                        .with_port(i32::from(addr.port()))])
                    .with_controller_id(BrokerId(1))
                    .with_topics(vec![MetadataResponseTopic::default()
                        .with_name(Some(TopicName(StrBytes::from_string(state.topic.clone()))))
                        .with_partitions(partitions)])
                    .encode(&mut out, version)
                    .unwrap();
            }
            ApiKey::ApiVersions => {
                let max = state.lock().unwrap().init_producer_id_max_version;
                let keys = [
                    (ApiKey::Produce, 0, 9),
                    (ApiKey::Metadata, 0, 12),
                    (ApiKey::ListOffsets, 0, 8),
                    (ApiKey::DescribeConfigs, 0, 4),
                    (ApiKey::InitProducerId, 0, max),
                    (ApiKey::ApiVersions, 0, 3),
                ];
                ApiVersionsResponse::default()
                    .with_api_keys(
                        keys.iter()
                            .map(|(k, min, max)| {
                                ApiVersion::default()
                                    .with_api_key(*k as i16)
                                    .with_min_version(*min)
                                    .with_max_version(*max)
                            })
                            .collect(),
                    )
                    .encode(&mut out, version)
                    .unwrap();
            }
            ApiKey::InitProducerId => {
                let mut state = state.lock().unwrap();
                state.init_requests += 1;
                let response = match state.init_producer_id_error {
                    Some(code) => InitProducerIdResponse::default()
                        .with_error_code(code)
                        .with_producer_id(ProducerId(-1))
                        .with_producer_epoch(-1),
                    None => {
                        state.next_producer_id += 1;
                        InitProducerIdResponse::default()
                            .with_producer_id(ProducerId(state.next_producer_id))
                            .with_producer_epoch(0)
                    }
                };
                response.encode(&mut out, version).unwrap();
            }
            ApiKey::DescribeConfigs => {
                let state = state.lock().unwrap();
                let result = match state.max_message_bytes {
                    Some(max) => DescribeConfigsResult::default()
                        .with_resource_type(2)
                        .with_resource_name(StrBytes::from_string(state.topic.clone()))
                        .with_configs(vec![DescribeConfigsResourceResult::default()
                            .with_name(StrBytes::from_static_str("max.message.bytes"))
                            .with_value(Some(StrBytes::from_string(max.to_string())))
                            .with_config_source(1)]),
                    None => DescribeConfigsResult::default()
                        .with_error_code(29)
                        .with_resource_type(2)
                        .with_resource_name(StrBytes::from_string(state.topic.clone())),
                };
                DescribeConfigsResponse::default()
                    .with_results(vec![result])
                    .encode(&mut out, version)
                    .unwrap();
            }
            ApiKey::ListOffsets => {
                let request = ListOffsetsRequest::decode(&mut body, version).unwrap();
                let state = state.lock().unwrap();
                let topics = request
                    .topics
                    .iter()
                    .map(|t| {
                        ListOffsetsTopicResponse::default()
                            .with_name(t.name.clone())
                            .with_partitions(
                                t.partitions
                                    .iter()
                                    .map(|p| {
                                        let offset = if p.timestamp == -2 {
                                            0
                                        } else {
                                            state.high_watermark(p.partition_index)
                                        };
                                        ListOffsetsPartitionResponse::default()
                                            .with_partition_index(p.partition_index)
                                            .with_timestamp(-1)
                                            .with_offset(offset)
                                    })
                                    .collect(),
                            )
                    })
                    .collect();
                ListOffsetsResponse::default()
                    .with_topics(topics)
                    .encode(&mut out, version)
                    .unwrap();
            }
            ApiKey::Fetch => {
                let request = FetchRequest::decode(&mut body, version).unwrap();
                let state = state.lock().unwrap();
                let topics = request
                    .topics
                    .iter()
                    .map(|t| {
                        let partitions = t
                            .partitions
                            .iter()
                            .map(|p| {
                                let log = state.logs.get(&p.partition).cloned().unwrap_or_default();
                                let hw = log.len() as i64;
                                let start = p.fetch_offset.max(0) as usize;
                                let records = encode_log(&log, start, 100);
                                PartitionData::default()
                                    .with_partition_index(p.partition)
                                    .with_high_watermark(hw)
                                    .with_last_stable_offset(hw)
                                    .with_log_start_offset(0)
                                    .with_records(Some(records))
                            })
                            .collect();
                        FetchableTopicResponse::default()
                            .with_topic(t.topic.clone())
                            .with_partitions(partitions)
                    })
                    .collect();
                FetchResponse::default()
                    .with_responses(topics)
                    .encode(&mut out, version)
                    .unwrap();
            }
            ApiKey::Produce => {
                let request = ProduceRequest::decode(&mut body, version).unwrap();
                let topic = &request.topic_data[0];
                let pdata = &topic.partition_data[0];
                let partition = pdata.index;
                let batch = pdata.records.clone().expect("records");
                let fault = {
                    let mut state = state.lock().unwrap();
                    let info = RecordBatchDecoder::decode_batch_info(&mut batch.clone())
                        .expect("batch info")[0]
                        .clone();
                    let index = state.attempts.len();
                    let scheduled = state.scheduled.remove(&index);
                    state.attempts.push(ProduceAttempt {
                        partition,
                        batch: batch.clone(),
                        producer_id: info.producer_id,
                        producer_epoch: info.producer_epoch,
                        base_sequence: info.base_sequence,
                        record_count: info.record_count,
                    });
                    scheduled
                        .or_else(|| {
                            state
                                .partition_faults
                                .get_mut(&partition)
                                .and_then(VecDeque::pop_front)
                        })
                        .or_else(|| state.faults.pop_front())
                };
                let result = match fault {
                    Some(Fault::Hang) => {
                        tokio::time::sleep(Duration::from_secs(3600)).await;
                        return;
                    }
                    Some(Fault::DropBeforeApply) => return,
                    Some(Fault::ApplyThenDrop) => {
                        let _ = append(&mut state.lock().unwrap(), partition, &batch);
                        return;
                    }
                    Some(Fault::Error(code)) => Err(code),
                    Some(Fault::ForgetProducers) => {
                        let mut state = state.lock().unwrap();
                        state.producers.clear();
                        append(&mut state, partition, &batch)
                    }
                    None | Some(Fault::Pass) => {
                        append(&mut state.lock().unwrap(), partition, &batch)
                    }
                };
                let (code, base) = match result {
                    Ok(base) => (0, base),
                    Err(code) => (code, -1),
                };
                ProduceResponse::default()
                    .with_responses(vec![TopicProduceResponse::default()
                        .with_name(topic.name.clone())
                        .with_partition_responses(vec![PartitionProduceResponse::default()
                            .with_index(partition)
                            .with_error_code(code)
                            .with_base_offset(base)])])
                    .encode(&mut out, version)
                    .unwrap();
            }
            other => panic!("produce mock broker: unexpected {other:?}"),
        }
        write_frame(&mut stream, api_key, version, correlation_id, &out).await;
    }
}

/// Up to `max` records of `log` from offset `start`, as one record batch.
fn encode_log(log: &[StoredRecord], start: usize, max: usize) -> Bytes {
    let records: Vec<Record> = log
        .iter()
        .enumerate()
        .skip(start)
        .take(max)
        .map(|(offset, r)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: offset as i64,
            sequence: offset as i32,
            timestamp: r.timestamp,
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r
                .headers
                .iter()
                .map(|(k, v)| (StrBytes::from_string(k.clone()), v.clone()))
                .collect(),
        })
        .collect();
    let mut buf = BytesMut::new();
    if !records.is_empty() {
        RecordBatchEncoder::encode(
            &mut buf,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .unwrap();
    }
    buf.freeze()
}

async fn read_request(stream: &mut TcpStream) -> Option<(ApiKey, i16, i32, Bytes)> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.ok()?;
    let mut frame = vec![0u8; i32::from_be_bytes(len_buf) as usize];
    stream.read_exact(&mut frame).await.ok()?;
    let api_key = ApiKey::try_from(i16::from_be_bytes([frame[0], frame[1]])).ok()?;
    let version = i16::from_be_bytes([frame[2], frame[3]]);
    let mut body = Bytes::from(frame);
    let header = RequestHeader::decode(&mut body, api_key.request_header_version(version)).ok()?;
    Some((api_key, version, header.correlation_id, body))
}

async fn write_frame(
    stream: &mut TcpStream,
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    body: &[u8],
) {
    let mut buf = BytesMut::new();
    buf.put_i32(0);
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut buf, api_key.response_header_version(version))
        .unwrap();
    buf.extend_from_slice(body);
    let len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());
    let _ = stream.write_all(&buf).await;
}
