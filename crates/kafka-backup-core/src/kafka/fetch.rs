//! Kafka Fetch API implementation.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};

use bytes::{Buf, Bytes};
use kafka_protocol::compression::{self as cmpr, Decompressor};
use kafka_protocol::messages::{
    ApiKey, BrokerId, FetchRequest, FetchResponse as KafkaFetchResponse, ListOffsetsRequest,
    TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::Compression;
use kafka_protocol::records::{Record, RecordBatchDecoder};
use tracing::debug;

use super::KafkaClient;
use crate::config::IsolationLevel;
use crate::error::KafkaError;
use crate::manifest::BackupRecord;
use crate::Result;

/// Response from a fetch operation
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct FetchResponse {
    /// Records fetched
    pub records: Vec<BackupRecord>,
    /// High watermark for the partition
    pub high_watermark: i64,
    /// Last stable offset (LSO) for the partition: the first offset of the
    /// earliest transaction still open on it, or the high watermark when
    /// none is. `-1` when the broker did not report one. A `read_committed`
    /// fetch never returns data at or beyond it.
    pub last_stable_offset: i64,
    /// Log start offset for the partition
    pub log_start_offset: i64,
    /// Next offset to fetch
    pub next_offset: i64,
    /// Transaction control records (commit and abort markers) passed over.
    /// Always 0 for a `read_uncommitted` fetch, which returns them as records.
    pub control_records_skipped: u64,
    /// Records of aborted transactions dropped. Always 0 for a
    /// `read_uncommitted` fetch.
    pub aborted_records_dropped: u64,
    /// The batches behind the two counters above, in offset order, so a
    /// caller can attribute them to the segment whose offset range holds them.
    pub skipped: Vec<SkippedBatch>,
}

/// A record batch a `read_committed` fetch passed over without returning its
/// records: a transaction marker, or a batch of an aborted transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SkippedBatch {
    /// Last offset of the batch (inclusive).
    pub last_offset: i64,
    /// Control records in the batch at or after the fetch offset.
    pub control_records: u64,
    /// Aborted records in the batch at or after the fetch offset.
    pub aborted_records: u64,
}

/// Fetch records from a topic/partition.
///
/// `isolation` is the Fetch request's `isolation_level`. With
/// [`IsolationLevel::ReadCommitted`] the broker returns data only up to the
/// last stable offset, together with the aborted transactions in the
/// returned range, and the records of those transactions and every
/// transaction marker are filtered out here, the way the Java consumer
/// filters them.
/// [`IsolationLevel::ReadUncommitted`] returns every record, markers
/// included, as versions before 0.24 did.
pub async fn fetch(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    offset: i64,
    max_bytes: i32,
    isolation: IsolationLevel,
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
        .with_isolation_level(isolation.as_protocol())
        .with_topics(vec![fetch_topic]);

    let response: KafkaFetchResponse = client.send_request(ApiKey::Fetch, request).await?;

    if response.throttle_time_ms > 0 {
        debug!(
            "fetch_throttled: {}:{} throttle_time_ms={} offset={}",
            topic, partition, response.throttle_time_ms, offset
        );
    }

    // Parse the response
    let mut result = FetchResponse {
        last_stable_offset: -1,
        next_offset: offset,
        ..FetchResponse::default()
    };

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

            result.high_watermark = partition_response.high_watermark;
            result.last_stable_offset = partition_response.last_stable_offset;
            result.log_start_offset = partition_response.log_start_offset;

            // Decode record batches
            if let Some(ref records_data) = partition_response.records {
                if !records_data.is_empty() {
                    let mut filter = match isolation {
                        IsolationLevel::ReadUncommitted => TransactionFilter::ReadUncommitted,
                        IsolationLevel::ReadCommitted => {
                            TransactionFilter::ReadCommitted(AbortedTransactions::new(
                                partition_response
                                    .aborted_transactions
                                    .iter()
                                    .flatten()
                                    .map(|t| (t.producer_id.0, t.first_offset)),
                            ))
                        }
                    };
                    let decoded = decode_fetch_data(records_data, offset, &mut filter)?;
                    result.records = decoded.records;
                    result.next_offset = decoded.next_offset;
                    result.control_records_skipped = decoded.control_records_skipped;
                    result.aborted_records_dropped = decoded.aborted_records_dropped;
                    result.skipped = decoded.skipped;
                }
            }
        }
    }

    debug!(
        "fetch_result: {}:{} isolation={} records={} high_watermark={} last_stable_offset={} \
         next_offset={} offset_start={} control_skipped={} aborted_dropped={}",
        topic,
        partition,
        isolation,
        result.records.len(),
        result.high_watermark,
        result.last_stable_offset,
        result.next_offset,
        offset,
        result.control_records_skipped,
        result.aborted_records_dropped
    );

    Ok(result)
}

/// Byte positions within a v2 record batch: base_offset 0..8,
/// batch_length 8..12 (length of everything after itself),
/// partition_leader_epoch 12..16, magic 16, crc 17..21, attributes 21..23,
/// last_offset_delta 23..27, base_timestamp 27..35, max_timestamp 35..43,
/// producer_id 43..51, producer_epoch 51..53, base_sequence 53..57,
/// records_count 57..61.
const BATCH_LENGTH_END: usize = 12;
const MAGIC_OFFSET: usize = 16;
const ATTRIBUTES_RANGE: std::ops::Range<usize> = 21..23;
const LAST_OFFSET_DELTA_RANGE: std::ops::Range<usize> = 23..27;
const PRODUCER_ID_RANGE: std::ops::Range<usize> = 43..51;
const RECORDS_COUNT_RANGE: std::ops::Range<usize> = 57..61;
/// Size of a v2 record batch header (everything before the records).
const V2_BATCH_HEADER_SIZE: usize = 61;
/// Batch attribute bit: the batch belongs to a transaction.
const TRANSACTIONAL_FLAG: i16 = 0x10;
/// Batch attribute bit: the batch holds control records (transaction markers).
const CONTROL_FLAG: i16 = 0x20;
/// Control record type of an ABORT marker. A control record's key is
/// `version: i16, type: i16`; type 0 is ABORT, 1 is COMMIT.
const CONTROL_TYPE_ABORT: i16 = 0;

/// The transaction fields of a v2 batch header that the read_committed
/// filter needs, read from the header bytes. They are there even when
/// compaction has removed every record of the batch.
#[derive(Debug, Clone, Copy)]
struct TransactionHeader {
    transactional: bool,
    control: bool,
    producer_id: i64,
    records_count: i32,
}

impl TransactionHeader {
    /// `None` for a batch that is not a complete v2 batch (message format
    /// v0/v1 has no transactions).
    fn parse(batch: &[u8]) -> Option<Self> {
        if batch.len() < V2_BATCH_HEADER_SIZE || batch[MAGIC_OFFSET] != 2 {
            return None;
        }
        let attributes = i16::from_be_bytes(batch[ATTRIBUTES_RANGE].try_into().unwrap());
        Some(Self {
            transactional: attributes & TRANSACTIONAL_FLAG != 0,
            control: attributes & CONTROL_FLAG != 0,
            producer_id: i64::from_be_bytes(batch[PRODUCER_ID_RANGE].try_into().unwrap()),
            records_count: i32::from_be_bytes(batch[RECORDS_COUNT_RANGE].try_into().unwrap()),
        })
    }
}

/// The aborted transactions a read_committed fetch reported, tracked the way
/// the Java consumer does (`CompletedFetch`): a min-heap of
/// `(first_offset, producer_id)` not yet reached, and the producers whose
/// aborted transaction has begun but whose ABORT marker has not been passed.
#[derive(Debug, Default)]
pub(crate) struct AbortedTransactions {
    pending: BinaryHeap<Reverse<(i64, i64)>>,
    aborted_producers: HashSet<i64>,
}

impl AbortedTransactions {
    /// From `(producer_id, first_offset)` pairs as the broker lists them.
    pub(crate) fn new(transactions: impl IntoIterator<Item = (i64, i64)>) -> Self {
        Self {
            pending: transactions
                .into_iter()
                .map(|(producer_id, first_offset)| Reverse((first_offset, producer_id)))
                .collect(),
            aborted_producers: HashSet::new(),
        }
    }

    /// Every aborted transaction that began at or before `last_offset` is
    /// now in progress: its producer's transactional batches are aborted
    /// until its ABORT marker.
    fn begin_up_to(&mut self, last_offset: i64) {
        while let Some(Reverse((first_offset, producer_id))) = self.pending.peek().copied() {
            if first_offset > last_offset {
                break;
            }
            self.pending.pop();
            self.aborted_producers.insert(producer_id);
        }
    }
}

/// How `decode_fetch_data` treats transactional data.
pub(crate) enum TransactionFilter {
    /// Return every record, control records (markers) included — the
    /// behaviour of versions before 0.24.
    ReadUncommitted,
    /// Return what a `read_committed` consumer sees.
    ReadCommitted(AbortedTransactions),
}

/// Records decoded from one partition's fetched data.
#[derive(Debug, Default)]
pub(crate) struct DecodedFetch {
    pub(crate) records: Vec<BackupRecord>,
    pub(crate) next_offset: i64,
    pub(crate) control_records_skipped: u64,
    pub(crate) aborted_records_dropped: u64,
    pub(crate) skipped: Vec<SkippedBatch>,
}

/// What the read_committed filter does with one batch.
enum BatchDisposition {
    /// Return its records.
    Keep,
    /// A transaction marker: skip it.
    Control,
    /// A batch of an aborted transaction: drop it.
    Aborted,
}

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
/// operator repo). For the same reason the next offset advances past the
/// batches the read_committed filter skips.
///
/// With [`TransactionFilter::ReadCommitted`] the batches are filtered the way
/// the Java consumer does it, from the batch headers (which survive
/// compaction): before a batch with a producer id is handled, every aborted
/// transaction whose first offset is at or before the batch's last offset
/// adds its producer to the aborted set. A control batch is skipped; when it
/// is an ABORT marker its producer leaves the set. A transactional batch of
/// a producer in the set is dropped. Everything else is returned.
///
/// The Kafka Fetch response may contain a truncated batch at the end when the
/// response hits max_bytes; it is detected via the batch_length header and
/// ignored.
pub(crate) fn decode_fetch_data(
    data: &Bytes,
    fetch_offset: i64,
    filter: &mut TransactionFilter,
) -> Result<DecodedFetch> {
    let mut out = DecodedFetch {
        next_offset: fetch_offset,
        ..DecodedFetch::default()
    };

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

        let last_offset_delta = i32::from_be_bytes(
            data[pos + LAST_OFFSET_DELTA_RANGE.start..pos + LAST_OFFSET_DELTA_RANGE.end]
                .try_into()
                .unwrap(),
        );
        let last_offset = base_offset + last_offset_delta as i64;
        let batch = data.slice(pos..pos + batch_total);
        pos += batch_total;
        out.next_offset = out.next_offset.max(last_offset + 1);

        let header = match &*filter {
            TransactionFilter::ReadUncommitted => None,
            TransactionFilter::ReadCommitted(_) => TransactionHeader::parse(&batch),
        };

        // Decoded lazily: an aborted batch whose records all lie at or after
        // the fetch offset is counted from its header and never decompressed.
        let mut records: Option<DecodedBatch> = None;
        let disposition = match (&mut *filter, header) {
            (TransactionFilter::ReadCommitted(aborted), Some(h)) if h.producer_id >= 0 => {
                aborted.begin_up_to(last_offset);
                if h.control {
                    let decoded = decode_batch(&batch)?;
                    if is_abort_marker(&decoded.records) {
                        aborted.aborted_producers.remove(&h.producer_id);
                    }
                    records = Some(decoded);
                    BatchDisposition::Control
                } else if h.transactional && aborted.aborted_producers.contains(&h.producer_id) {
                    BatchDisposition::Aborted
                } else {
                    BatchDisposition::Keep
                }
            }
            (TransactionFilter::ReadCommitted(_), Some(h)) if h.control => {
                BatchDisposition::Control
            }
            _ => BatchDisposition::Keep,
        };

        match disposition {
            BatchDisposition::Keep => {
                let decoded = match records {
                    Some(decoded) => decoded,
                    None => decode_batch(&batch)?,
                };
                decoded.push_from(fetch_offset, &mut out.records);
            }
            BatchDisposition::Control | BatchDisposition::Aborted => {
                let header_count = header.map_or(0, |h| h.records_count.max(0) as u64);
                let count = if base_offset >= fetch_offset {
                    header_count
                } else {
                    let decoded = match records {
                        Some(decoded) => decoded,
                        None => decode_batch(&batch)?,
                    };
                    decoded
                        .records
                        .iter()
                        .filter(|r| r.offset >= fetch_offset)
                        .count() as u64
                };
                let (control_records, aborted_records) = match disposition {
                    BatchDisposition::Control => (count, 0),
                    _ => (0, count),
                };
                out.control_records_skipped += control_records;
                out.aborted_records_dropped += aborted_records;
                if last_offset >= fetch_offset {
                    out.skipped.push(SkippedBatch {
                        last_offset,
                        control_records,
                        aborted_records,
                    });
                }
            }
        }
    }

    Ok(out)
}

/// The records of one complete batch.
struct DecodedBatch {
    records: Vec<Record>,
    /// Each record's headers in wire order, duplicates and null values
    /// included. kafka-protocol's `Record` keeps headers in a map, which
    /// collapses repeated keys, so they are read from the decompressed
    /// record bytes instead. `None` when the codec did not hand the record
    /// bytes over (no v2 records); the map headers are used then.
    headers: Option<Vec<Vec<crate::manifest::RecordHeader>>>,
}

impl DecodedBatch {
    /// Append the records at or after `fetch_offset` to `out`.
    fn push_from(self, fetch_offset: i64, out: &mut Vec<BackupRecord>) {
        let mut ordered = self.headers.map(Vec::into_iter);
        for record in &self.records {
            let headers = ordered.as_mut().and_then(Iterator::next);
            if record.offset >= fetch_offset {
                let mut converted = convert_record(record);
                if let Some(headers) = headers {
                    converted.headers = headers;
                }
                out.push(converted);
            }
        }
    }
}

/// Decode the records of one complete batch. Batch framing, CRC checks,
/// decompression and timestamps stay with the upstream codec; the ordered
/// header lists are taken from the decompressed records on the way through.
fn decode_batch(batch: &Bytes) -> Result<DecodedBatch> {
    let mut buf = batch.clone();
    let ordered_headers = std::cell::RefCell::new(None);
    let decompress = |input: &mut Bytes, compression: Compression| -> anyhow::Result<Bytes> {
        let body = match compression {
            Compression::None => {
                cmpr::None::decompress(input, |b| Ok(b.copy_to_bytes(b.remaining())))?
            }
            Compression::Gzip => {
                cmpr::Gzip::decompress(input, |b| Ok(b.copy_to_bytes(b.remaining())))?
            }
            Compression::Snappy => {
                cmpr::Snappy::decompress(input, |b| Ok(b.copy_to_bytes(b.remaining())))?
            }
            Compression::Lz4 => {
                cmpr::Lz4::decompress(input, |b| Ok(b.copy_to_bytes(b.remaining())))?
            }
            Compression::Zstd => {
                cmpr::Zstd::decompress(input, |b| Ok(b.copy_to_bytes(b.remaining())))?
            }
        };
        *ordered_headers.borrow_mut() = Some(super::header_codec::decode(&body)?);
        Ok(body)
    };
    let set = RecordBatchDecoder::decode_with_custom_compression(&mut buf, Some(&decompress))
        .map_err(|e| KafkaError::Protocol(format!("Failed to decode records: {e:?}")))?;
    let headers = ordered_headers.into_inner();
    if headers
        .as_ref()
        .is_some_and(|h| h.len() != set.records.len())
    {
        return Err(KafkaError::Protocol("Kafka record/header list count mismatch".into()).into());
    }
    Ok(DecodedBatch {
        records: set.records,
        headers,
    })
}

/// Whether a control batch is an ABORT marker: its first record's key is
/// `version: i16, type: i16` with type 0. A control batch emptied by
/// compaction is not (the Java consumer's `containsAbortMarker`).
fn is_abort_marker(records: &[Record]) -> bool {
    records
        .first()
        .and_then(|r| r.key.as_ref())
        .filter(|key| key.len() >= 4)
        .is_some_and(|key| i16::from_be_bytes([key[2], key[3]]) == CONTROL_TYPE_ABORT)
}

/// Convert a kafka-protocol Record to our BackupRecord
fn convert_record(record: &Record) -> BackupRecord {
    let headers: Vec<_> = record
        .headers
        .iter()
        .map(|(key, value)| crate::manifest::RecordHeader {
            key: key.to_string(),
            // Preserve Kafka's null-vs-empty distinction (issue #155): a
            // null header value must not be flattened into an empty one.
            value: value.as_ref().map(|v| v.to_vec()),
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
/// Timestamp values: -2 = earliest, -1 = latest. With
/// [`IsolationLevel::ReadCommitted`], "latest" is the last stable offset;
/// with [`IsolationLevel::ReadUncommitted`] it is the high watermark.
pub async fn batch_get_offsets(
    client: &KafkaClient,
    requests: &[(String, i32, i64)], // (topic, partition, timestamp)
    isolation: IsolationLevel,
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
        .with_isolation_level(isolation.as_protocol())
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
        "batch_get_offsets: {} results from {} requests (isolation={})",
        results.len(),
        requests.len(),
        isolation
    );

    Ok(results)
}

/// Get the earliest and latest offsets for a partition. "Latest" is the last
/// stable offset with [`IsolationLevel::ReadCommitted`] and the high
/// watermark with [`IsolationLevel::ReadUncommitted`].
pub async fn get_offsets(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    isolation: IsolationLevel,
) -> Result<(i64, i64)> {
    // Fetch earliest offset (timestamp = -2)
    let earliest = list_offset(client, topic, partition, -2, isolation).await?;
    // Fetch latest offset (timestamp = -1)
    let latest = list_offset(client, topic, partition, -1, isolation).await?;

    debug!(
        "Offsets for {}:{}: earliest={}, latest={} (isolation={})",
        topic, partition, earliest, latest, isolation
    );

    Ok((earliest, latest))
}

/// List offset for a specific timestamp
async fn list_offset(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    timestamp: i64,
    isolation: IsolationLevel,
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
        .with_isolation_level(isolation.as_protocol())
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
            delete_horizon: false,
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

    /// Decode as a `read_uncommitted` fetch (the behaviour before 0.24).
    fn parse(data: Vec<u8>, fetch_offset: i64) -> (Vec<BackupRecord>, i64) {
        let decoded = decode_fetch_data(
            &Bytes::from(data),
            fetch_offset,
            &mut TransactionFilter::ReadUncommitted,
        )
        .expect("decode failed");
        (decoded.records, decoded.next_offset)
    }

    /// Issue #155: a null header value (`-1` length on the wire) must be
    /// decoded as `None`, distinct from an empty value (`Some(vec![])`).
    #[test]
    fn null_and_empty_header_values_are_preserved_from_the_wire() {
        let mut record = make_record(7);
        record
            .headers
            .insert(StrBytes::from_static_str("trace-id"), None);
        record
            .headers
            .insert(StrBytes::from_static_str("empty"), Some(Bytes::new()));
        record.headers.insert(
            StrBytes::from_static_str("tenant"),
            Some(Bytes::from_static(b"42")),
        );

        let mut buf = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut buf,
            std::iter::once(&record),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::None,
            },
        )
        .expect("encode failed");

        let (records, _) = parse(buf.to_vec(), 7);
        assert_eq!(records.len(), 1);
        let headers: Vec<(&str, Option<&[u8]>)> = records[0]
            .headers
            .iter()
            .map(|h| (h.key.as_str(), h.value.as_deref()))
            .collect();
        assert_eq!(
            headers,
            vec![
                ("trace-id", None),
                ("empty", Some(&[][..])),
                ("tenant", Some(&b"42"[..])),
            ]
        );
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

    // ------------------------------------------------------------------
    // Issue #150: per-record timestampDelta is a varlong on the wire.
    // ------------------------------------------------------------------

    /// One v2 record batch exactly as Apache Kafka's Java client
    /// (kafka-clients 4.3.1) produced it and the broker stored it: 20
    /// uncompressed records on partition 0, alternating a 1970-01-01
    /// placeholder timestamp (0) with "now" (1787086231121 + i). The batch's
    /// base timestamp is 0, so every odd record's `timestampDelta` is
    /// ~1.79e12 ms — far beyond `i32::MAX` — and is encoded as a **6-byte**
    /// zigzag varlong (record 1: `a2 d9 9e ea 82 68`).
    ///
    /// kafka-protocol 0.17.0 decoded that field as a 5-byte-capped `VarInt`,
    /// consumed 5 of the 6 bytes, and misread every field after it
    /// ("Unexpected negative record value length (-54 bytes)", "invalid
    /// utf-8 …"). Captured from the broker log segment (on-disk v2 format is
    /// identical to the Fetch wire format); CRC is intact.
    const JAVA_WIDE_TIMESTAMP_BATCH_HEX: &str = concat!(
        "0000000000000000000001b7000000000269f94cec0000000000130000000000000000000001",
        "a016a3d66300000000000003eb0000000000000000001420000000046b301076302d65706f63",
        "68002600a2d99eea826802046b310c76312d6e6f770020000004046b321076322d65706f6368",
        "002600a6d99eea826806046b330c76332d6e6f770020000008046b341076342d65706f636800",
        "2600aad99eea82680a046b350c76352d6e6f77002000000c046b361076362d65706f63680026",
        "00aed99eea82680e046b370c76372d6e6f770020000010046b381076382d65706f6368002600",
        "b2d99eea826812046b390c76392d6e6f770024000014066b3130127631302d65706f6368002a",
        "00b6d99eea826816066b31310e7631312d6e6f770024000018066b3132127631322d65706f63",
        "68002a00bad99eea82681a066b31330e7631332d6e6f77002400001c066b3134127631342d65",
        "706f6368002a00bed99eea82681e066b31350e7631352d6e6f770024000020066b3136127631",
        "362d65706f6368002a00c2d99eea826822066b31370e7631372d6e6f770024000024066b3138",
        "127631382d65706f6368002a00c6d99eea826826066b31390e7631392d6e6f7700"
    );

    #[test]
    fn decodes_java_batch_whose_records_span_decades() {
        let raw = hex_to_bytes(JAVA_WIDE_TIMESTAMP_BATCH_HEX);
        assert_eq!(raw.len(), 451, "fixture is a single 451-byte batch");

        let decoded = decode_fetch_data(&raw, 0, &mut TransactionFilter::ReadUncommitted)
            .expect("batch produced by the Java client must decode");
        let (records, next_offset) = (decoded.records, decoded.next_offset);

        assert_eq!(records.len(), 20);
        assert_eq!(next_offset, 20);
        let now = 1_787_086_231_121i64; // record 1's CreateTime as reported by kafka-console-consumer
        for (i, r) in records.iter().enumerate() {
            assert_eq!(r.offset, i as i64);
            let expected_ts = if i % 2 == 0 { 0 } else { now + (i as i64 - 1) };
            assert_eq!(r.timestamp, expected_ts, "record {i} timestamp");
            assert_eq!(
                r.key.as_deref(),
                Some(format!("k{i}").as_bytes()),
                "record {i} key"
            );
            let expected_value = format!("v{i}{}", if i % 2 == 0 { "-epoch" } else { "-now" });
            assert_eq!(
                r.value.as_deref(),
                Some(expected_value.as_bytes()),
                "record {i} value"
            );
            assert!(r.headers.is_empty());
        }
        // Sanity: the delta really is beyond i32 — this is the case that broke.
        assert!(records[1].timestamp - records[0].timestamp > i32::MAX as i64);
    }

    fn hex_to_bytes(hex: &str) -> Bytes {
        let clean: String = hex.chars().filter(|c| !c.is_whitespace()).collect();
        assert_eq!(clean.len() % 2, 0);
        Bytes::from(
            (0..clean.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&clean[i..i + 2], 16).expect("hex"))
                .collect::<Vec<u8>>(),
        )
    }

    // ------------------------------------------------------------------
    // read_committed: aborted transactions and transaction markers.
    // ------------------------------------------------------------------

    const PID_A: i64 = 1_000;
    const PID_B: i64 = 2_000;

    /// A record of producer `producer_id` (`NO_PRODUCER_ID` for a plain
    /// producer).
    fn producer_record(offset: i64, producer_id: i64, transactional: bool) -> Record {
        Record {
            transactional,
            producer_id,
            producer_epoch: if producer_id >= 0 {
                0
            } else {
                NO_PRODUCER_EPOCH
            },
            ..make_record(offset)
        }
    }

    fn encode_records(records: &[Record]) -> Vec<u8> {
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

    /// One batch of a transactional producer.
    fn txn_batch(offsets: &[i64], producer_id: i64) -> Vec<u8> {
        let records: Vec<Record> = offsets
            .iter()
            .map(|&o| producer_record(o, producer_id, true))
            .collect();
        encode_records(&records)
    }

    /// One batch of an idempotent (non-transactional) producer.
    fn idempotent_batch(offsets: &[i64], producer_id: i64) -> Vec<u8> {
        let records: Vec<Record> = offsets
            .iter()
            .map(|&o| producer_record(o, producer_id, false))
            .collect();
        encode_records(&records)
    }

    /// A COMMIT or ABORT marker as the broker writes it: a transactional
    /// control batch holding one control record whose key is
    /// `version: i16 = 0, type: i16` (0 = ABORT, 1 = COMMIT) and whose value
    /// is `version: i16 = 0, coordinator_epoch: i32`.
    fn marker(offset: i64, producer_id: i64, commit: bool) -> Vec<u8> {
        let record = Record {
            control: true,
            key: Some(Bytes::from(vec![0, 0, 0, u8::from(commit)])),
            value: Some(Bytes::from_static(&[0, 0, 0, 0, 0, 0])),
            ..producer_record(offset, producer_id, true)
        };
        encode_records(std::slice::from_ref(&record))
    }

    /// Remove every record from a batch while keeping its offset range and
    /// producer fields, as the log cleaner does.
    fn emptied(mut batch: Vec<u8>, last_offset_delta: i32) -> Vec<u8> {
        batch.truncate(V2_BATCH_HEADER_SIZE);
        batch[RECORDS_COUNT_RANGE].copy_from_slice(&0i32.to_be_bytes());
        let batch_length = (batch.len() - BATCH_LENGTH_END) as i32;
        batch[8..12].copy_from_slice(&batch_length.to_be_bytes());
        patch_last_offset_delta(&mut batch, last_offset_delta);
        batch
    }

    /// One batch of a plain (non-idempotent) producer.
    fn plain_batch(offsets: &[i64]) -> Vec<u8> {
        encode_batch(offsets)
    }

    fn concat(batches: &[Vec<u8>]) -> Vec<u8> {
        batches.concat()
    }

    fn read_committed(data: Vec<u8>, fetch_offset: i64, aborted: &[(i64, i64)]) -> DecodedFetch {
        let mut filter =
            TransactionFilter::ReadCommitted(AbortedTransactions::new(aborted.iter().copied()));
        decode_fetch_data(&Bytes::from(data), fetch_offset, &mut filter).expect("decode failed")
    }

    fn offsets_of(decoded: &DecodedFetch) -> Vec<i64> {
        decoded.records.iter().map(|r| r.offset).collect()
    }

    #[test]
    fn commit_marker_is_skipped_and_committed_records_are_kept() {
        let data = concat(&[
            txn_batch(&[0, 1, 2], PID_A),
            marker(3, PID_A, true),
            plain_batch(&[4]),
        ]);
        let d = read_committed(data, 0, &[]);
        assert_eq!(offsets_of(&d), vec![0, 1, 2, 4]);
        assert_eq!(d.next_offset, 5);
        assert_eq!(d.control_records_skipped, 1);
        assert_eq!(d.aborted_records_dropped, 0);
        assert_eq!(
            d.skipped,
            vec![SkippedBatch {
                last_offset: 3,
                control_records: 1,
                aborted_records: 0
            }]
        );
    }

    #[test]
    fn abort_marker_drops_the_aborted_transaction() {
        let data = concat(&[
            txn_batch(&[0, 1], PID_A),
            plain_batch(&[2]),
            txn_batch(&[3], PID_A),
            marker(4, PID_A, false),
            plain_batch(&[5]),
        ]);
        let d = read_committed(data, 0, &[(PID_A, 0)]);
        assert_eq!(offsets_of(&d), vec![2, 5]);
        assert_eq!(d.next_offset, 6, "advances past the marker");
        assert_eq!(d.aborted_records_dropped, 3);
        assert_eq!(d.control_records_skipped, 1);
        assert_eq!(
            d.skipped.iter().map(|s| s.last_offset).collect::<Vec<_>>(),
            vec![1, 3, 4]
        );
    }

    #[test]
    fn interleaved_producers_drop_only_the_aborted_transaction() {
        // A commits, B aborts, then B starts a new transaction that commits.
        let data = concat(&[
            txn_batch(&[0], PID_A),
            txn_batch(&[1], PID_B),
            plain_batch(&[2]),
            txn_batch(&[3], PID_A),
            txn_batch(&[4], PID_B),
            marker(5, PID_A, true),
            marker(6, PID_B, false),
            plain_batch(&[7]),
            txn_batch(&[8], PID_B),
            marker(9, PID_B, true),
        ]);
        let d = read_committed(data, 0, &[(PID_B, 1)]);
        assert_eq!(offsets_of(&d), vec![0, 2, 3, 7, 8]);
        assert_eq!(d.aborted_records_dropped, 2);
        assert_eq!(d.control_records_skipped, 3);
        assert_eq!(d.next_offset, 10);
    }

    #[test]
    fn a_producer_aborting_twice_is_tracked_per_transaction() {
        let data = concat(&[
            txn_batch(&[0, 1], PID_A),
            marker(2, PID_A, false),
            txn_batch(&[3], PID_A),
            marker(4, PID_A, true),
            txn_batch(&[5], PID_A),
            marker(6, PID_A, false),
            plain_batch(&[7]),
        ]);
        let d = read_committed(data, 0, &[(PID_A, 5), (PID_A, 0)]);
        assert_eq!(offsets_of(&d), vec![3, 7]);
        assert_eq!(d.aborted_records_dropped, 3);
        assert_eq!(d.control_records_skipped, 3);
    }

    /// Resuming inside an aborted transaction: the broker lists the
    /// transaction (it began before the fetch offset) and returns the batch
    /// holding the fetch offset from its start.
    #[test]
    fn resume_mid_transaction_where_the_aborted_list_overlaps_the_fetch_start() {
        let data = concat(&[
            txn_batch(&[0, 1, 2, 3, 4], PID_A),
            plain_batch(&[5]),
            txn_batch(&[6, 7], PID_A),
            marker(8, PID_A, false),
            plain_batch(&[9]),
        ]);
        let d = read_committed(data, 3, &[(PID_A, 0)]);
        assert_eq!(offsets_of(&d), vec![5, 9]);
        assert_eq!(
            d.aborted_records_dropped, 4,
            "3, 4, 6 and 7 — not the offsets before the fetch offset"
        );
        assert_eq!(d.control_records_skipped, 1);
        assert_eq!(d.next_offset, 10);
        assert_eq!(
            d.skipped[0],
            SkippedBatch {
                last_offset: 4,
                control_records: 0,
                aborted_records: 2
            }
        );
    }

    #[test]
    fn resume_mid_batch_of_a_committed_transaction() {
        let data = concat(&[txn_batch(&[0, 1, 2, 3, 4], PID_A), marker(5, PID_A, true)]);
        let d = read_committed(data, 3, &[]);
        assert_eq!(offsets_of(&d), vec![3, 4]);
        assert_eq!(d.control_records_skipped, 1);
        assert_eq!(d.next_offset, 6);
    }

    /// Resuming exactly at a marker: it is skipped and counted once.
    #[test]
    fn resume_at_a_marker_skips_it() {
        let data = concat(&[marker(5, PID_A, true), plain_batch(&[6])]);
        let d = read_committed(data, 5, &[]);
        assert_eq!(offsets_of(&d), vec![6]);
        assert_eq!(d.control_records_skipped, 1);
        assert_eq!(d.next_offset, 7);
    }

    /// The Java consumer only aborts *transactional* batches: an idempotent
    /// batch carrying the same producer id is kept.
    #[test]
    fn idempotent_batch_of_an_aborted_producer_id_is_kept() {
        let data = concat(&[
            txn_batch(&[0], PID_A),
            idempotent_batch(&[1], PID_A),
            marker(2, PID_A, false),
        ]);
        let d = read_committed(data, 0, &[(PID_A, 0)]);
        assert_eq!(offsets_of(&d), vec![1]);
        assert_eq!(d.aborted_records_dropped, 1);
    }

    /// Compaction can empty a batch of an aborted transaction (and a
    /// marker): the header still identifies it, and the fetch advances past
    /// its whole offset range.
    #[test]
    fn compaction_emptied_batches_are_filtered_and_advance() {
        let data = concat(&[
            emptied(txn_batch(&[0, 1, 2], PID_A), 2),
            marker(3, PID_A, false),
            emptied(marker(4, PID_B, true), 0),
            plain_batch(&[5]),
        ]);
        let d = read_committed(data, 0, &[(PID_A, 0)]);
        assert_eq!(offsets_of(&d), vec![5]);
        assert_eq!(d.next_offset, 6);
        assert_eq!(d.aborted_records_dropped, 0, "no record was left to drop");
        assert_eq!(d.control_records_skipped, 1, "the emptied marker held none");

        // A batch emptied entirely: no record, but progress.
        let d = read_committed(emptied(txn_batch(&[7, 8, 9], PID_A), 2), 7, &[(PID_A, 7)]);
        assert!(d.records.is_empty());
        assert_eq!(d.next_offset, 10);
    }

    #[test]
    fn truncated_tail_batch_is_ignored_under_read_committed() {
        let tail = txn_batch(&[3, 4], PID_A);
        let mut data = concat(&[txn_batch(&[0, 1], PID_A), marker(2, PID_A, true)]);
        data.extend_from_slice(&tail[..tail.len() - 5]);
        let d = read_committed(data, 0, &[(PID_A, 3)]);
        assert_eq!(offsets_of(&d), vec![0, 1]);
        assert_eq!(d.next_offset, 3, "the truncated batch is fetched again");
        assert_eq!(d.aborted_records_dropped, 0);
        assert_eq!(d.control_records_skipped, 1);
    }

    #[test]
    fn aborted_transactions_beyond_the_fetched_range_do_not_apply() {
        let data = concat(&[txn_batch(&[0, 1], PID_A), marker(2, PID_A, true)]);
        let d = read_committed(data, 0, &[(PID_A, 100)]);
        assert_eq!(offsets_of(&d), vec![0, 1]);
    }

    /// An aborted batch whose records all lie at or after the fetch offset
    /// is counted from its header and never decompressed: corrupt record
    /// data (with a valid CRC) does not fail the fetch.
    #[test]
    fn aborted_batch_is_dropped_without_decoding_its_records() {
        let mut aborted = txn_batch(&[0, 1, 2], PID_A);
        // Garble the first record's length varint, then fix the CRC.
        aborted[V2_BATCH_HEADER_SIZE] = 0x7f;
        let crc = crc32c::crc32c(&aborted[CRC_RANGE_START..]);
        aborted[CRC_OFFSET..CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());
        assert!(
            decode_batch(&Bytes::from(aborted.clone())).is_err(),
            "fixture must not decode"
        );

        let data = concat(&[aborted, marker(3, PID_A, false), plain_batch(&[4])]);
        let d = read_committed(data, 0, &[(PID_A, 0)]);
        assert_eq!(offsets_of(&d), vec![4]);
        assert_eq!(d.aborted_records_dropped, 3);
    }

    /// The opt-out keeps the behaviour of versions before 0.24 byte for
    /// byte: aborted records are returned, and so is every marker, as a
    /// record whose key is the control key.
    #[test]
    fn read_uncommitted_returns_aborted_records_and_markers_as_before() {
        let data = concat(&[
            txn_batch(&[0, 1], PID_A),
            plain_batch(&[2]),
            marker(3, PID_A, false),
        ]);
        let mut filter = TransactionFilter::ReadUncommitted;
        let d = decode_fetch_data(&Bytes::from(data), 0, &mut filter).unwrap();
        assert_eq!(offsets_of(&d), vec![0, 1, 2, 3]);
        assert_eq!(d.records[3].key.as_deref(), Some(&[0u8, 0, 0, 0][..]));
        assert_eq!(d.records[3].value.as_deref(), Some(&[0u8; 6][..]));
        assert_eq!(d.next_offset, 4);
        assert_eq!(d.control_records_skipped, 0);
        assert_eq!(d.aborted_records_dropped, 0);
        assert!(d.skipped.is_empty());
    }

    /// Message format v0/v1 batches have no transaction fields: they are
    /// never filtered.
    #[test]
    fn non_v2_batch_header_is_not_read_as_transactional() {
        let mut batch = txn_batch(&[0], PID_A);
        batch[MAGIC_OFFSET] = 1;
        assert!(TransactionHeader::parse(&batch).is_none());
        batch[MAGIC_OFFSET] = 2;
        let header = TransactionHeader::parse(&batch).unwrap();
        assert!(header.transactional && !header.control);
        assert_eq!(header.producer_id, PID_A);
        assert_eq!(header.records_count, 1);
        assert!(TransactionHeader::parse(&batch[..V2_BATCH_HEADER_SIZE - 1]).is_none());
    }
}
