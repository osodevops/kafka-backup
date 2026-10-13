//! The restore producer: idempotent produce (KIP-98 without transactions)
//! so a batch resent after a timeout or a lost connection is de-duplicated
//! by the broker instead of written twice.
//!
//! - One `InitProducerId` (null transactional id) per restore gives the
//!   producer id and epoch.
//! - Every target partition keeps its own sequence: a batch's base sequence
//!   is the partition's next sequence, which then advances by the batch's
//!   record count, wrapping past `i32::MAX` to 0 as Kafka does. Batches to a
//!   partition are sent one at a time (one in flight), under a per-partition
//!   lock, so even two source partitions mapped onto one target partition
//!   cannot interleave sequences.
//! - Each batch (each timestamp sub-batch) is encoded exactly once for its
//!   producer identity and sequence. Every resend — by
//!   `KafkaClient::send_request` after a lost connection, by the partition
//!   router after NOT_LEADER or a connection error, and here after a
//!   retriable broker error — sends those identical bytes.
//!
//! Broker answers:
//! - `DUPLICATE_SEQUENCE_NUMBER` (46): the batch was already written; the
//!   broker no longer has its offset. It counts as acknowledged and its
//!   base offset is derived as the end of the partition's previous batch
//!   (the high watermark read before the partition's first batch, then
//!   base + count of every acknowledged batch). This is exact while the
//!   restore is the partition's only writer, so the offset mapping entries
//!   of that batch stay correct.
//! - `OUT_OF_ORDER_SEQUENCE_NUMBER` (45): fatal for the partition (records
//!   would be lost or reordered).
//! - `UNKNOWN_PRODUCER_ID` (59) / `INVALID_PRODUCER_EPOCH` (47): the broker
//!   lost or fenced the producer state (for example retention deleted the
//!   segments holding it). The producer is re-initialised with an epoch bump
//!   (KIP-360: `InitProducerId` v3 with the current id and epoch; a local
//!   bump if that request fails), every partition restarts its sequence at
//!   0, and the batch, whose earlier attempts may or may not have been
//!   written, is reconciled: after the request timeout has passed since it
//!   was first sent, the partition's high watermark is compared with the
//!   offset the batch would start at. Unchanged: resend (re-encoded for the
//!   new epoch). Advanced by exactly the batch: acknowledged. Anything else:
//!   another writer touched the partition, which is fatal.
//! - Retriable produce errors (`LEADER_NOT_AVAILABLE`, `REQUEST_TIMED_OUT`,
//!   `NOT_ENOUGH_REPLICAS`, `NOT_ENOUGH_REPLICAS_AFTER_APPEND`) are retried
//!   with the same bytes a few times; idempotence makes that safe.
//!
//! If the cluster does not permit or support idempotence
//! (`CLUSTER_AUTHORIZATION_FAILED`, `UNSUPPORTED_VERSION`, a broker older
//! than Kafka 2.5), the producer logs a warning, records the reason for the
//! restore report, and produces without a producer id (the behaviour before
//! 0.25: a retried batch may be written twice).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::records::Compression;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::config::{IsolationLevel, RestoreOptions};
use crate::error::KafkaError;
use crate::kafka::produce::{encode_batch, increment_sequence, split_by_timestamp_ranges};
use crate::kafka::transactions::{ProducerBatch, ProducerIdAndEpoch};
use crate::kafka::PartitionLeaderRouter;
use crate::manifest::BackupRecord;
use crate::Result;

/// `LEADER_NOT_AVAILABLE`
pub(crate) const LEADER_NOT_AVAILABLE: i16 = 5;
/// `REQUEST_TIMED_OUT`: with acks=-1 the batch may have been appended.
pub(crate) const REQUEST_TIMED_OUT: i16 = 7;
/// `NOT_ENOUGH_REPLICAS`: rejected before the append.
pub(crate) const NOT_ENOUGH_REPLICAS: i16 = 19;
/// `NOT_ENOUGH_REPLICAS_AFTER_APPEND`: appended, not replicated in time.
pub(crate) const NOT_ENOUGH_REPLICAS_AFTER_APPEND: i16 = 20;
/// `CLUSTER_AUTHORIZATION_FAILED`: idempotent write is not permitted.
pub(crate) const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
/// `UNSUPPORTED_VERSION`
pub(crate) const UNSUPPORTED_VERSION: i16 = 35;
/// `OUT_OF_ORDER_SEQUENCE_NUMBER`
pub(crate) const OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
/// `DUPLICATE_SEQUENCE_NUMBER`
pub(crate) const DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
/// `INVALID_PRODUCER_EPOCH`
pub(crate) const INVALID_PRODUCER_EPOCH: i16 = 47;
/// `UNKNOWN_PRODUCER_ID`
pub(crate) const UNKNOWN_PRODUCER_ID: i16 = 59;

/// Retries of a retriable produce error with the same bytes.
const MAX_RETRIABLE_ATTEMPTS: u32 = 5;
/// Epoch bumps one batch may trigger before the partition fails.
const MAX_EPOCH_BUMPS_PER_BATCH: u32 = 3;
/// Attempts of the initial `InitProducerId` on retriable errors.
const MAX_INIT_ATTEMPTS: u32 = 3;

/// What the restore producer needs from the target cluster. Implemented by
/// [`PartitionLeaderRouter`]; tests drive it with scripted fakes.
#[async_trait::async_trait]
pub(crate) trait ProduceTransport: Send + Sync {
    /// Send one encoded batch to the partition leader (retrying NOT_LEADER
    /// and connection errors with the same bytes); the base offset.
    async fn produce_encoded(
        &self,
        topic: &str,
        partition: i32,
        batch: Bytes,
        acks: i16,
        timeout_ms: i32,
    ) -> Result<i64>;

    /// `InitProducerId` with a null transactional id; `current` is
    /// [`ProducerIdAndEpoch::NONE`] or the identity to bump.
    async fn init_producer_id(&self, current: ProducerIdAndEpoch) -> Result<ProducerIdAndEpoch>;

    /// The partition's high watermark.
    async fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64>;

    /// Whether the cluster supports `InitProducerId` v3 (Kafka 2.5+).
    async fn supports_idempotence(&self) -> Result<bool>;
}

#[async_trait::async_trait]
impl ProduceTransport for PartitionLeaderRouter {
    async fn produce_encoded(
        &self,
        topic: &str,
        partition: i32,
        batch: Bytes,
        acks: i16,
        timeout_ms: i32,
    ) -> Result<i64> {
        PartitionLeaderRouter::produce_encoded(self, topic, partition, batch, acks, timeout_ms)
            .await
    }

    async fn init_producer_id(&self, current: ProducerIdAndEpoch) -> Result<ProducerIdAndEpoch> {
        crate::kafka::transactions::init_producer_id(self.bootstrap_client(), None, 60_000, current)
            .await
    }

    async fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64> {
        Ok(self
            .get_offsets(topic, partition, IsolationLevel::ReadUncommitted)
            .await?
            .1)
    }

    async fn supports_idempotence(&self) -> Result<bool> {
        crate::kafka::transactions::supports_idempotent_epoch_bump(self.bootstrap_client()).await
    }
}

/// Producer settings taken from the restore options.
#[derive(Debug, Clone)]
pub(crate) struct ProducerSettings {
    pub acks: i16,
    pub timeout_ms: i32,
    pub compression: Compression,
    /// How long after a batch was first sent its outcome is certain: the
    /// broker-side produce timeout plus a margin.
    pub reconcile_after: Duration,
    /// Back-off between retries of a retriable produce error.
    pub retry_backoff: Duration,
}

impl ProducerSettings {
    pub(crate) fn from_options(options: &RestoreOptions) -> Self {
        Self {
            acks: options.produce_acks,
            timeout_ms: options.produce_timeout_ms,
            compression: Compression::Zstd,
            reconcile_after: Duration::from_millis(options.produce_timeout_ms.max(0) as u64)
                + Duration::from_secs(5),
            retry_backoff: Duration::from_millis(500),
        }
    }
}

/// The per-target-partition producer state.
#[derive(Debug)]
struct PartitionSequence {
    /// The producer identity `next_sequence` belongs to.
    producer: ProducerIdAndEpoch,
    /// Base sequence of the partition's next batch.
    next_sequence: i32,
    /// Offset the partition's next batch starts at, when known: the high
    /// watermark before its first batch, then the end of every
    /// acknowledged batch.
    next_offset: Option<i64>,
}

/// The result of producing one restore batch.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct ProducedBatch {
    /// `(base offset, record count)` per sub-batch, in order.
    pub sub_batches: Vec<(i64, usize)>,
    /// Whether any sub-batch's offsets were derived (acknowledged through
    /// `DUPLICATE_SEQUENCE_NUMBER` or reconciliation) rather than reported
    /// by the broker.
    pub offsets_derived: bool,
}

/// Target (topic, partition) -> its producer state, locked while a batch
/// to the partition is in flight.
type PartitionStates = HashMap<(String, i32), Arc<tokio::sync::Mutex<PartitionSequence>>>;

/// The restore's producer, shared by every partition task.
pub(crate) struct RestoreProducer {
    transport: Arc<dyn ProduceTransport>,
    settings: ProducerSettings,
    /// `None` when producing without a producer id.
    identity: tokio::sync::RwLock<Option<ProducerIdAndEpoch>>,
    /// Why idempotent produce was requested but is not in use.
    fallback_reason: Option<String>,
    partitions: std::sync::Mutex<PartitionStates>,
    epoch_bumps: AtomicU64,
    derived_batches: AtomicU64,
}

impl RestoreProducer {
    /// Start the producer. With `idempotent`, obtain a producer id; fall
    /// back to non-idempotent produce (with a warning and a recorded
    /// reason) when the cluster denies or does not support idempotence.
    pub(crate) async fn start(
        transport: Arc<dyn ProduceTransport>,
        settings: ProducerSettings,
        idempotent: bool,
    ) -> Result<Self> {
        let (identity, fallback_reason) = if idempotent {
            match Self::init_identity(transport.as_ref()).await? {
                Ok(identity) => {
                    info!(
                        "Idempotent produce enabled: producer id {} epoch {}",
                        identity.producer_id, identity.producer_epoch
                    );
                    (Some(identity), None)
                }
                Err(reason) => {
                    warn!(
                        "Idempotent produce is not available on the target cluster ({}). Falling \
                         back to producing without a producer id: a batch retried after a timeout \
                         or a lost connection may be written twice. Grant the restore principal \
                         idempotent write (IdempotentWrite on the cluster before Kafka 2.8, Write \
                         on the topics since; DeveloperWrite on Confluent Cloud), or set \
                         restore.idempotent_producer: false to accept this explicitly.",
                        reason
                    );
                    (None, Some(reason))
                }
            }
        } else {
            (None, None)
        };
        Ok(Self {
            transport,
            settings,
            identity: tokio::sync::RwLock::new(identity),
            fallback_reason,
            partitions: std::sync::Mutex::new(HashMap::new()),
            epoch_bumps: AtomicU64::new(0),
            derived_batches: AtomicU64::new(0),
        })
    }

    /// `Ok(Ok(identity))`, `Ok(Err(reason))` for a fallback, `Err` for a
    /// failure that should stop the restore.
    async fn init_identity(
        transport: &dyn ProduceTransport,
    ) -> Result<std::result::Result<ProducerIdAndEpoch, String>> {
        let version_known =
            match transport.supports_idempotence().await {
                Ok(true) => true,
                Ok(false) => return Ok(Err(
                    "the brokers do not support InitProducerId v3 (Kafka 2.5 or later is required)"
                        .to_string(),
                )),
                Err(e) => {
                    debug!("ApiVersions failed ({e}); trying InitProducerId anyway");
                    false
                }
            };

        let mut attempt = 0;
        loop {
            attempt += 1;
            match transport.init_producer_id(ProducerIdAndEpoch::NONE).await {
                Ok(identity) => return Ok(Ok(identity)),
                Err(e) => match broker_code(&e) {
                    Some(CLUSTER_AUTHORIZATION_FAILED) => {
                        return Ok(Err(format!(
                            "InitProducerId was denied: CLUSTER_AUTHORIZATION_FAILED ({e})"
                        )))
                    }
                    Some(UNSUPPORTED_VERSION) => {
                        return Ok(Err(format!(
                            "InitProducerId is not supported: UNSUPPORTED_VERSION ({e})"
                        )))
                    }
                    // A broker that does not know the request version
                    // closes the connection rather than answering.
                    _ if !version_known && crate::kafka::is_connection_error(&e) => {
                        return Ok(Err(format!(
                            "InitProducerId failed and ApiVersions could not confirm support ({e})"
                        )))
                    }
                    Some(code)
                        if attempt < MAX_INIT_ATTEMPTS
                            && (crate::kafka::transactions::is_retriable_transaction_error(
                                code,
                            ) || code == REQUEST_TIMED_OUT) =>
                    {
                        tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await;
                    }
                    _ => return Err(e),
                },
            }
        }
    }

    /// Whether batches carry a producer id.
    pub(crate) async fn is_idempotent(&self) -> bool {
        self.identity.read().await.is_some()
    }

    /// Why idempotent produce was requested but is not in use.
    pub(crate) fn fallback_reason(&self) -> Option<&str> {
        self.fallback_reason.as_deref()
    }

    /// Epoch bumps so far.
    pub(crate) fn epoch_bumps(&self) -> u64 {
        self.epoch_bumps.load(Ordering::Relaxed)
    }

    /// Batches whose offsets were derived rather than reported.
    pub(crate) fn derived_batches(&self) -> u64 {
        self.derived_batches.load(Ordering::Relaxed)
    }

    fn partition_state(
        &self,
        topic: &str,
        partition: i32,
    ) -> Arc<tokio::sync::Mutex<PartitionSequence>> {
        let mut partitions = self.partitions.lock().expect("producer partitions lock");
        Arc::clone(
            partitions
                .entry((topic.to_string(), partition))
                .or_insert_with(|| {
                    Arc::new(tokio::sync::Mutex::new(PartitionSequence {
                        producer: ProducerIdAndEpoch::NONE,
                        next_sequence: 0,
                        next_offset: None,
                    }))
                }),
        )
    }

    /// Produce `records` to `topic`/`partition` as one restore batch (split
    /// into timestamp sub-batches if needed). Waits for any batch in flight
    /// to the same partition first.
    pub(crate) async fn produce(
        &self,
        topic: &str,
        partition: i32,
        records: &[BackupRecord],
    ) -> Result<ProducedBatch> {
        let state = self.partition_state(topic, partition);
        let mut state = state.lock().await;
        let mut produced = ProducedBatch::default();
        for range in split_by_timestamp_ranges(records) {
            let sub_batch = &records[range];
            let (base_offset, derived) = self
                .produce_sub_batch(&mut state, topic, partition, sub_batch)
                .await?;
            produced.sub_batches.push((base_offset, sub_batch.len()));
            produced.offsets_derived |= derived;
        }
        if produced.offsets_derived {
            self.derived_batches.fetch_add(1, Ordering::Relaxed);
        }
        Ok(produced)
    }

    async fn produce_sub_batch(
        &self,
        state: &mut PartitionSequence,
        topic: &str,
        partition: i32,
        records: &[BackupRecord],
    ) -> Result<(i64, bool)> {
        let count = records.len() as i32;
        let mut epoch_bumps = 0;

        loop {
            let Some(identity) = *self.identity.read().await else {
                let bytes = encode_batch(
                    records,
                    &ProducerBatch::NON_IDEMPOTENT,
                    self.settings.compression,
                )?;
                let base = self
                    .transport
                    .produce_encoded(
                        topic,
                        partition,
                        bytes,
                        self.settings.acks,
                        self.settings.timeout_ms,
                    )
                    .await?;
                return Ok((base, false));
            };

            if state.producer != identity {
                // A new producer id or epoch: sequences restart at 0.
                state.producer = identity;
                state.next_sequence = 0;
            }
            if state.next_offset.is_none() {
                state.next_offset = Some(self.transport.high_watermark(topic, partition).await?);
            }
            let expected_offset = state.next_offset.expect("set above");

            // Encoded exactly once for this identity and sequence; every
            // resend below, and inside the transport, sends these bytes.
            let bytes = encode_batch(
                records,
                &ProducerBatch {
                    producer: identity,
                    base_sequence: state.next_sequence,
                    transactional: false,
                },
                self.settings.compression,
            )?;
            let first_sent = Instant::now();
            let mut retriable_attempts = 0;

            loop {
                let error = match self
                    .transport
                    .produce_encoded(
                        topic,
                        partition,
                        bytes.clone(),
                        self.settings.acks,
                        self.settings.timeout_ms,
                    )
                    .await
                {
                    Ok(base) => {
                        acknowledge(state, base, count);
                        return Ok((base, false));
                    }
                    Err(error) => error,
                };

                match broker_code(&error) {
                    Some(DUPLICATE_SEQUENCE_NUMBER) => {
                        debug!(
                            "{}[{}]: batch at sequence {} was already written \
                             (DUPLICATE_SEQUENCE_NUMBER); its offsets are derived from {}",
                            topic, partition, state.next_sequence, expected_offset
                        );
                        acknowledge(state, expected_offset, count);
                        return Ok((expected_offset, true));
                    }
                    Some(OUT_OF_ORDER_SEQUENCE_NUMBER) => {
                        return Err(KafkaError::BrokerError {
                            code: OUT_OF_ORDER_SEQUENCE_NUMBER,
                            message: format!(
                                "OUT_OF_ORDER_SEQUENCE_NUMBER producing to {topic}[{partition}] \
                                 (producer id {} epoch {}, base sequence {}): the broker's \
                                 producer state does not match this restore's; the partition \
                                 is stopped rather than risk lost or reordered records. Resume \
                                 the restore to continue from the checkpoint ({error})",
                                identity.producer_id, identity.producer_epoch, state.next_sequence
                            ),
                        }
                        .into());
                    }
                    Some(code @ (UNKNOWN_PRODUCER_ID | INVALID_PRODUCER_EPOCH)) => {
                        epoch_bumps += 1;
                        if epoch_bumps > MAX_EPOCH_BUMPS_PER_BATCH {
                            return Err(error);
                        }
                        warn!(
                            "{}[{}]: {} for producer id {} epoch {}; re-initialising the producer \
                             and reconciling the batch",
                            topic,
                            partition,
                            if code == UNKNOWN_PRODUCER_ID {
                                "UNKNOWN_PRODUCER_ID"
                            } else {
                                "INVALID_PRODUCER_EPOCH"
                            },
                            identity.producer_id,
                            identity.producer_epoch
                        );
                        self.bump_epoch(identity).await?;
                        if self
                            .reconcile(topic, partition, expected_offset, count, first_sent)
                            .await?
                        {
                            acknowledge(state, expected_offset, count);
                            return Ok((expected_offset, true));
                        }
                        // Not written: resend under the new identity.
                        break;
                    }
                    Some(code)
                        if is_retriable_produce_error(code)
                            && retriable_attempts < MAX_RETRIABLE_ATTEMPTS =>
                    {
                        retriable_attempts += 1;
                        let backoff = self.settings.retry_backoff * retriable_attempts;
                        warn!(
                            "{}[{}]: retriable produce error {} (attempt {}/{}), resending the same \
                             batch after {:?}",
                            topic,
                            partition,
                            code,
                            retriable_attempts,
                            MAX_RETRIABLE_ATTEMPTS,
                            backoff
                        );
                        tokio::time::sleep(backoff).await;
                    }
                    _ => return Err(error),
                }
            }
        }
    }

    /// Replace `seen` with a bumped identity, unless another partition did
    /// already. KIP-360: `InitProducerId` v3 with the current id and epoch;
    /// when that fails, bump the epoch locally (the broker accepts a higher
    /// epoch starting at sequence 0).
    async fn bump_epoch(&self, seen: ProducerIdAndEpoch) -> Result<ProducerIdAndEpoch> {
        let mut identity = self.identity.write().await;
        if let Some(current) = *identity {
            if current != seen {
                return Ok(current);
            }
        }
        let next = match self.transport.init_producer_id(seen).await {
            Ok(next) => next,
            Err(e) if seen.producer_epoch < i16::MAX => {
                warn!("InitProducerId for the epoch bump failed ({e}); bumping the epoch locally");
                ProducerIdAndEpoch {
                    producer_id: seen.producer_id,
                    producer_epoch: seen.producer_epoch + 1,
                }
            }
            Err(e) => return Err(e),
        };
        info!(
            "Producer re-initialised: id {} epoch {} -> id {} epoch {}",
            seen.producer_id, seen.producer_epoch, next.producer_id, next.producer_epoch
        );
        *identity = Some(next);
        self.epoch_bumps.fetch_add(1, Ordering::Relaxed);
        Ok(next)
    }

    /// Decide whether an in-doubt batch of `count` records, which would
    /// start at `expected_offset`, was written: wait until the request
    /// timeout has passed since it was first sent (so no earlier attempt is
    /// still in flight), then read the high watermark. `true` = written,
    /// `false` = not written.
    async fn reconcile(
        &self,
        topic: &str,
        partition: i32,
        expected_offset: i64,
        count: i32,
        first_sent: Instant,
    ) -> Result<bool> {
        let remaining = self
            .settings
            .reconcile_after
            .saturating_sub(first_sent.elapsed());
        if !remaining.is_zero() {
            info!(
                "{}[{}]: waiting {:?} for earlier attempts of the batch to settle before \
                 reconciling",
                topic, partition, remaining
            );
            tokio::time::sleep(remaining).await;
        }
        let high_watermark = self.transport.high_watermark(topic, partition).await?;
        reconcile_decision(topic, partition, expected_offset, count, high_watermark)
    }
}

/// The reconciliation rule: the high watermark at the batch's start offset
/// means it was not written, at its end means it was, anything else means
/// another writer.
pub(crate) fn reconcile_decision(
    topic: &str,
    partition: i32,
    expected_offset: i64,
    count: i32,
    high_watermark: i64,
) -> Result<bool> {
    if high_watermark == expected_offset {
        Ok(false)
    } else if high_watermark == expected_offset + i64::from(count) {
        Ok(true)
    } else {
        Err(KafkaError::BrokerError {
            code: UNKNOWN_PRODUCER_ID,
            message: format!(
                "cannot reconcile the in-doubt batch of {count} records on {topic}[{partition}]: \
                 the high watermark is {high_watermark}, expected {expected_offset} (not written) \
                 or {} (written). Another writer may be producing to the partition",
                expected_offset + i64::from(count)
            ),
        }
        .into())
    }
}

fn acknowledge(state: &mut PartitionSequence, base_offset: i64, count: i32) {
    state.next_sequence = increment_sequence(state.next_sequence, count);
    state.next_offset = Some(base_offset + i64::from(count));
}

/// The broker error code of `error`, if it is a broker error.
pub(crate) fn broker_code(error: &crate::Error) -> Option<i16> {
    match error {
        crate::Error::Kafka(KafkaError::BrokerError { code, .. }) => Some(*code),
        _ => None,
    }
}

/// Produce errors safe to resend with the same idempotent batch.
fn is_retriable_produce_error(code: i16) -> bool {
    matches!(
        code,
        LEADER_NOT_AVAILABLE
            | REQUEST_TIMED_OUT
            | NOT_ENOUGH_REPLICAS
            | NOT_ENOUGH_REPLICAS_AFTER_APPEND
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::records::RecordBatchDecoder;
    use std::collections::VecDeque;

    /// What the fake broker does with the next produce request.
    #[derive(Debug, Clone, Copy)]
    enum Step {
        /// Apply the normal idempotent broker rules.
        Normal,
        /// Apply the request, then lose the response (the client sees
        /// REQUEST_TIMED_OUT, as after a replication timeout).
        ApplyThenTimeout,
        /// Reject with this code without applying.
        Reject(i16),
        /// Forget all producer state, then apply the rules (models the
        /// producer's last batch being deleted by retention).
        ForgetProducers,
    }

    #[derive(Debug, Default)]
    struct ProducerEntry {
        epoch: i16,
        last_sequence: i32,
        /// (base sequence, record count) -> base offset of recent batches.
        recent: VecDeque<((i32, i32), i64)>,
    }

    #[derive(Debug, Default)]
    struct FakeState {
        supported: bool,
        next_producer_id: i64,
        init_errors: VecDeque<i16>,
        init_requests: Vec<ProducerIdAndEpoch>,
        script: VecDeque<Step>,
        /// Every produce attempt's batch bytes, in order.
        sent: Vec<Bytes>,
        /// partition -> appended (producer id, base sequence, count) batches.
        logs: HashMap<i32, Vec<(i64, i32, i32)>>,
        high_watermarks: HashMap<i32, i64>,
        producers: HashMap<(i32, i64), ProducerEntry>,
    }

    struct FakeCluster(std::sync::Mutex<FakeState>);

    impl FakeCluster {
        fn new() -> Arc<Self> {
            Arc::new(Self(std::sync::Mutex::new(FakeState {
                supported: true,
                next_producer_id: 1000,
                ..Default::default()
            })))
        }

        fn script(&self, steps: &[Step]) {
            self.0.lock().unwrap().script.extend(steps.iter().copied());
        }

        fn records(&self, partition: i32) -> i64 {
            *self
                .0
                .lock()
                .unwrap()
                .high_watermarks
                .get(&partition)
                .unwrap_or(&0)
        }

        fn sent(&self) -> Vec<Bytes> {
            self.0.lock().unwrap().sent.clone()
        }
    }

    fn apply(
        state: &mut FakeState,
        partition: i32,
        batch: &Bytes,
    ) -> std::result::Result<i64, i16> {
        let info = RecordBatchDecoder::decode_batch_info(&mut batch.clone()).unwrap()[0].clone();
        let count = info.record_count;
        let hw = state.high_watermarks.entry(partition).or_insert(0);
        let base = *hw;
        if info.producer_id < 0 {
            *hw += i64::from(count);
            state
                .logs
                .entry(partition)
                .or_default()
                .push((-1, -1, count));
            return Ok(base);
        }
        let key = (partition, info.producer_id);
        let entry = state.producers.get(&key);
        match entry {
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
        *hw += i64::from(count);
        state.logs.entry(partition).or_default().push((
            info.producer_id,
            info.base_sequence,
            count,
        ));
        Ok(base)
    }

    fn broker_err(code: i16) -> crate::Error {
        KafkaError::BrokerError {
            code,
            message: format!("code {code}"),
        }
        .into()
    }

    #[async_trait::async_trait]
    impl ProduceTransport for FakeCluster {
        async fn produce_encoded(
            &self,
            _topic: &str,
            partition: i32,
            batch: Bytes,
            acks: i16,
            _timeout_ms: i32,
        ) -> Result<i64> {
            let mut state = self.0.lock().unwrap();
            state.sent.push(batch.clone());
            assert_eq!(acks, -1);
            match state.script.pop_front().unwrap_or(Step::Normal) {
                Step::Normal => apply(&mut state, partition, &batch).map_err(broker_err),
                Step::ApplyThenTimeout => {
                    let _ = apply(&mut state, partition, &batch);
                    Err(broker_err(REQUEST_TIMED_OUT))
                }
                Step::Reject(code) => Err(broker_err(code)),
                Step::ForgetProducers => {
                    state.producers.clear();
                    apply(&mut state, partition, &batch).map_err(broker_err)
                }
            }
        }

        async fn init_producer_id(
            &self,
            current: ProducerIdAndEpoch,
        ) -> Result<ProducerIdAndEpoch> {
            let mut state = self.0.lock().unwrap();
            state.init_requests.push(current);
            if let Some(code) = state.init_errors.pop_front() {
                return Err(broker_err(code));
            }
            // Like the broker for a null transactional id: a fresh id.
            state.next_producer_id += 1;
            Ok(ProducerIdAndEpoch {
                producer_id: state.next_producer_id,
                producer_epoch: 0,
            })
        }

        async fn high_watermark(&self, _topic: &str, partition: i32) -> Result<i64> {
            Ok(*self
                .0
                .lock()
                .unwrap()
                .high_watermarks
                .get(&partition)
                .unwrap_or(&0))
        }

        async fn supports_idempotence(&self) -> Result<bool> {
            Ok(self.0.lock().unwrap().supported)
        }
    }

    fn settings() -> ProducerSettings {
        ProducerSettings {
            acks: -1,
            timeout_ms: 30_000,
            compression: Compression::None,
            reconcile_after: Duration::from_secs(35),
            retry_backoff: Duration::from_millis(100),
        }
    }

    fn records(n: usize) -> Vec<BackupRecord> {
        (0..n)
            .map(|i| BackupRecord {
                key: Some(format!("k{i}").into_bytes()),
                value: Some(vec![b'v'; 16]),
                headers: vec![],
                timestamp: 1_700_000_000_000 + i as i64,
                offset: i as i64,
            })
            .collect()
    }

    fn info(bytes: &Bytes) -> kafka_protocol::records::BatchDecodeInfo {
        RecordBatchDecoder::decode_batch_info(&mut bytes.clone()).unwrap()[0].clone()
    }

    async fn producer(cluster: &Arc<FakeCluster>) -> RestoreProducer {
        RestoreProducer::start(cluster.clone(), settings(), true)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn base_sequence_advances_by_the_batch_record_count_per_partition() {
        let cluster = FakeCluster::new();
        let producer = producer(&cluster).await;
        assert!(producer.is_idempotent().await);

        producer.produce("t", 0, &records(10)).await.unwrap();
        producer.produce("t", 1, &records(4)).await.unwrap();
        producer.produce("t", 0, &records(7)).await.unwrap();
        producer.produce("t", 0, &records(1)).await.unwrap();

        let sent = cluster.sent();
        let seqs: Vec<(i64, i32, i32)> = sent
            .iter()
            .map(|b| {
                let i = info(b);
                (i.producer_id, i.base_sequence, i.record_count)
            })
            .collect();
        assert_eq!(
            seqs,
            vec![(1001, 0, 10), (1001, 0, 4), (1001, 10, 7), (1001, 17, 1)]
        );
        assert_eq!(cluster.records(0), 18);
        assert_eq!(cluster.records(1), 4);
    }

    #[tokio::test]
    async fn sequence_wraps_to_zero_past_i32_max() {
        let cluster = FakeCluster::new();
        let producer = producer(&cluster).await;
        {
            let state = producer.partition_state("t", 0);
            let mut state = state.lock().await;
            state.producer = producer.identity.read().await.unwrap();
            state.next_sequence = i32::MAX - 2;
            state.next_offset = Some(0);
        }
        // Teach the fake broker the same position.
        cluster.0.lock().unwrap().producers.insert(
            (0, 1001),
            ProducerEntry {
                epoch: 0,
                last_sequence: i32::MAX - 3,
                recent: VecDeque::new(),
            },
        );

        producer.produce("t", 0, &records(5)).await.unwrap();
        producer.produce("t", 0, &records(3)).await.unwrap();

        let sent = cluster.sent();
        assert_eq!(info(&sent[0]).base_sequence, i32::MAX - 2);
        // MAX-2, MAX-1, MAX, 0, 1 -> next is 2.
        assert_eq!(info(&sent[1]).base_sequence, 2);
        assert_eq!(cluster.records(0), 8);
    }

    /// A batch appended by the broker whose acknowledgement is lost is
    /// resent with byte-identical content and de-duplicated: no duplicate.
    #[tokio::test]
    async fn retried_batch_is_resent_with_identical_bytes_and_deduplicated() {
        let cluster = FakeCluster::new();
        let producer = producer(&cluster).await;
        producer.produce("t", 0, &records(3)).await.unwrap();
        cluster.script(&[Step::ApplyThenTimeout, Step::ApplyThenTimeout]);

        let produced = producer.produce("t", 0, &records(5)).await.unwrap();

        let sent = cluster.sent();
        assert_eq!(sent.len(), 4, "1 + the batch sent three times");
        assert_eq!(sent[1], sent[2], "resend is byte-identical");
        assert_eq!(sent[2], sent[3], "resend is byte-identical");
        assert_eq!(cluster.records(0), 8, "the batch was written once");
        assert_eq!(produced.sub_batches, vec![(3, 5)]);
        assert!(!produced.offsets_derived);
    }

    #[tokio::test]
    async fn duplicate_sequence_counts_as_acknowledged_with_derived_offsets() {
        let cluster = FakeCluster::new();
        let producer = producer(&cluster).await;
        producer.produce("t", 0, &records(4)).await.unwrap();
        // The broker wrote the batch on an earlier attempt and no longer
        // has its offset: it answers DUPLICATE_SEQUENCE_NUMBER.
        cluster.script(&[
            Step::ApplyThenTimeout,
            Step::Reject(DUPLICATE_SEQUENCE_NUMBER),
        ]);

        let produced = producer.produce("t", 0, &records(6)).await.unwrap();
        assert_eq!(
            produced.sub_batches,
            vec![(4, 6)],
            "offset derived from 4 + 0"
        );
        assert!(produced.offsets_derived);
        assert_eq!(producer.derived_batches(), 1);
        assert_eq!(cluster.records(0), 10);

        // The sequence advanced past the duplicate batch.
        producer.produce("t", 0, &records(2)).await.unwrap();
        let sent = cluster.sent();
        assert_eq!(info(sent.last().unwrap()).base_sequence, 10);
        assert_eq!(cluster.records(0), 12);
    }

    #[tokio::test]
    async fn out_of_order_sequence_is_fatal_for_the_partition() {
        let cluster = FakeCluster::new();
        let producer = producer(&cluster).await;
        cluster.script(&[Step::Reject(OUT_OF_ORDER_SEQUENCE_NUMBER)]);
        let err = producer.produce("t", 0, &records(3)).await.unwrap_err();
        assert_eq!(broker_code(&err), Some(OUT_OF_ORDER_SEQUENCE_NUMBER));
        assert!(
            err.to_string().contains("OUT_OF_ORDER_SEQUENCE_NUMBER"),
            "{err}"
        );
        assert_eq!(cluster.sent().len(), 1, "never resent");
        assert_eq!(cluster.records(0), 0);
    }

    /// UNKNOWN_PRODUCER_ID: bump, reconcile (not written), resend at
    /// sequence 0 under the new identity, every partition restarts at 0.
    #[tokio::test(start_paused = true)]
    async fn unknown_producer_id_bumps_the_epoch_and_resends_an_unwritten_batch() {
        let cluster = FakeCluster::new();
        let producer = producer(&cluster).await;
        producer.produce("t", 0, &records(5)).await.unwrap();
        producer.produce("t", 1, &records(2)).await.unwrap();
        cluster.script(&[Step::ForgetProducers]);

        let started = Instant::now();
        let produced = producer.produce("t", 0, &records(3)).await.unwrap();
        assert!(
            started.elapsed() >= Duration::from_secs(35),
            "reconciled only after the request timeout"
        );
        assert_eq!(produced.sub_batches, vec![(5, 3)]);
        assert_eq!(cluster.records(0), 8, "written once");
        assert_eq!(producer.epoch_bumps(), 1);

        {
            let state = cluster.0.lock().unwrap();
            assert_eq!(
                state.init_requests[1],
                ProducerIdAndEpoch {
                    producer_id: 1001,
                    producer_epoch: 0
                },
                "InitProducerId v3 carries the current id and epoch"
            );
            let resent = info(state.sent.last().unwrap());
            assert_eq!((resent.producer_id, resent.base_sequence), (1002, 0));
        }

        // Partition 1 restarts its sequence under the new identity.
        producer.produce("t", 1, &records(2)).await.unwrap();
        let last = info(cluster.sent().last().unwrap());
        assert_eq!((last.producer_id, last.base_sequence), (1002, 0));
        assert_eq!(cluster.records(1), 4);
    }

    /// INVALID_PRODUCER_EPOCH after the batch was in fact written on an
    /// earlier attempt: reconciliation sees the high watermark advanced by
    /// exactly the batch and does not resend it.
    #[tokio::test(start_paused = true)]
    async fn invalid_epoch_after_an_earlier_write_is_reconciled_without_resending() {
        let cluster = FakeCluster::new();
        let producer = producer(&cluster).await;
        producer.produce("t", 0, &records(5)).await.unwrap();
        cluster.script(&[Step::ApplyThenTimeout, Step::Reject(INVALID_PRODUCER_EPOCH)]);

        let produced = producer.produce("t", 0, &records(4)).await.unwrap();
        assert_eq!(produced.sub_batches, vec![(5, 4)]);
        assert!(produced.offsets_derived);
        assert_eq!(cluster.records(0), 9, "no duplicate");
        assert_eq!(cluster.sent().len(), 3, "not resent after reconciliation");

        // The next batch starts at sequence 0 under the bumped identity.
        producer.produce("t", 0, &records(1)).await.unwrap();
        let last = info(cluster.sent().last().unwrap());
        assert_eq!((last.producer_id, last.base_sequence), (1002, 0));
        assert_eq!(cluster.records(0), 10);
    }

    #[test]
    fn reconciliation_maths() {
        assert!(!reconcile_decision("t", 0, 100, 10, 100).unwrap());
        assert!(reconcile_decision("t", 0, 100, 10, 110).unwrap());
        let err = reconcile_decision("t", 0, 100, 10, 105).unwrap_err();
        assert!(err.to_string().contains("Another writer"), "{err}");
        assert!(reconcile_decision("t", 0, 100, 10, 99).is_err());
        assert!(reconcile_decision("t", 0, 100, 10, 111).is_err());
    }

    #[tokio::test]
    async fn failed_epoch_bump_request_falls_back_to_a_local_bump() {
        let cluster = FakeCluster::new();
        let producer = producer(&cluster).await;
        cluster
            .0
            .lock()
            .unwrap()
            .init_errors
            .push_back(CLUSTER_AUTHORIZATION_FAILED);
        let seen = producer.identity.read().await.unwrap();
        let next = producer.bump_epoch(seen).await.unwrap();
        assert_eq!(
            next,
            ProducerIdAndEpoch {
                producer_id: seen.producer_id,
                producer_epoch: seen.producer_epoch + 1
            }
        );
        // A second partition reporting the same stale identity reuses it.
        assert_eq!(producer.bump_epoch(seen).await.unwrap(), next);
        assert_eq!(producer.epoch_bumps(), 1);
    }

    #[tokio::test]
    async fn denied_init_producer_id_falls_back_to_non_idempotent_produce() {
        let cluster = FakeCluster::new();
        cluster
            .0
            .lock()
            .unwrap()
            .init_errors
            .push_back(CLUSTER_AUTHORIZATION_FAILED);
        let producer = producer(&cluster).await;
        assert!(!producer.is_idempotent().await);
        assert!(producer
            .fallback_reason()
            .unwrap()
            .contains("CLUSTER_AUTHORIZATION_FAILED"));

        producer.produce("t", 0, &records(3)).await.unwrap();
        let i = info(&cluster.sent()[0]);
        assert_eq!((i.producer_id, i.base_sequence), (-1, -1));
    }

    #[tokio::test]
    async fn old_brokers_fall_back_to_non_idempotent_produce() {
        let cluster = FakeCluster::new();
        cluster.0.lock().unwrap().supported = false;
        let producer = producer(&cluster).await;
        assert!(!producer.is_idempotent().await);
        assert!(producer.fallback_reason().unwrap().contains("Kafka 2.5"));
        assert!(cluster.0.lock().unwrap().init_requests.is_empty());
    }

    #[tokio::test]
    async fn disabled_idempotence_has_no_fallback_reason() {
        let cluster = FakeCluster::new();
        let producer = RestoreProducer::start(cluster.clone(), settings(), false)
            .await
            .unwrap();
        assert!(!producer.is_idempotent().await);
        assert_eq!(producer.fallback_reason(), None);
        assert!(cluster.0.lock().unwrap().init_requests.is_empty());
    }

    #[tokio::test]
    async fn other_init_errors_stop_the_restore() {
        let cluster = FakeCluster::new();
        cluster.0.lock().unwrap().init_errors.push_back(87);
        assert!(RestoreProducer::start(cluster.clone(), settings(), true)
            .await
            .is_err());
    }

    /// Timestamp sub-batches each get their own sequence range and are
    /// each encoded once.
    #[tokio::test]
    async fn timestamp_sub_batches_get_consecutive_sequence_ranges() {
        let cluster = FakeCluster::new();
        let producer = producer(&cluster).await;
        let day_ms: i64 = 86_400_000;
        let wide: Vec<BackupRecord> = (0..50)
            .map(|i| BackupRecord {
                key: None,
                value: Some(vec![1]),
                headers: vec![],
                timestamp: 1_700_000_000_000 + i * day_ms,
                offset: i,
            })
            .collect();
        let produced = producer.produce("t", 0, &wide).await.unwrap();
        assert!(produced.sub_batches.len() >= 2);
        let mut expected_seq = 0;
        let mut expected_offset = 0;
        for (bytes, (base, count)) in cluster.sent().iter().zip(&produced.sub_batches) {
            assert_eq!(info(bytes).base_sequence, expected_seq);
            assert_eq!(*base, expected_offset);
            expected_seq += *count as i32;
            expected_offset += *count as i64;
        }
        assert_eq!(cluster.records(0), 50);
    }
}
