//! Test-support transactional producer (KIP-98).
//!
//! Drives the protocol plumbing in `kafka_backup_core::kafka::transactions`
//! the way the Java producer does: FindCoordinator (key type 1) and
//! InitProducerId with a transactional id, AddPartitionsToTxn before the first
//! write to a partition in a transaction, Produce of batches stamped with the
//! producer id, epoch and per-partition sequence, and EndTxn to commit or
//! abort. Used to put real transactions, markers and aborted records on a
//! Testcontainers broker.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use kafka_backup_core::config::{CompressionType, KafkaConfig, SecurityConfig, TopicSelection};
use kafka_backup_core::error::KafkaError;
use kafka_backup_core::kafka::transactions::{
    add_partitions_to_txn, encode_producer_batch, end_txn, init_producer_id,
    is_retriable_transaction_error, produce_encoded_batch, transaction_coordinator_connection,
    ProducerBatch, ProducerIdAndEpoch,
};
use kafka_backup_core::kafka::{IsolationLevel, KafkaClient, PartitionLeaderRouter};
use kafka_backup_core::manifest::BackupRecord;
use tokio::time::sleep;

/// A transactional producer for one transactional id.
pub struct TransactionalProducer {
    transactional_id: String,
    bootstrap: KafkaClient,
    router: PartitionLeaderRouter,
    coordinator: Arc<KafkaClient>,
    producer: ProducerIdAndEpoch,
    sequences: HashMap<(String, i32), i32>,
    partitions_in_txn: HashSet<(String, i32)>,
    in_transaction: bool,
}

fn kafka_config(bootstrap_servers: &str) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![bootstrap_servers.to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection::default(),
        connection: Default::default(),
    }
}

/// Retry `op` while the broker answers with a retriable transaction error
/// (coordinator loading or moving, or the previous transaction still
/// completing).
async fn retry<T, F, Fut>(what: &str, mut op: F) -> anyhow::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = kafka_backup_core::Result<T>>,
{
    const ATTEMPTS: u32 = 100;
    for attempt in 1..=ATTEMPTS {
        match op().await {
            Ok(value) => return Ok(value),
            Err(kafka_backup_core::Error::Kafka(KafkaError::BrokerError { code, message }))
                if is_retriable_transaction_error(code) && attempt < ATTEMPTS =>
            {
                tracing::debug!("{what}: retriable error {code} ({message}), retrying");
                sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(anyhow::anyhow!("{what}: {e}")),
        }
    }
    unreachable!()
}

impl TransactionalProducer {
    /// Find the transaction coordinator and initialise the producer id
    /// (fencing any earlier producer with the same transactional id).
    pub async fn new(
        bootstrap_servers: &str,
        transactional_id: &str,
        transaction_timeout_ms: i32,
    ) -> anyhow::Result<Self> {
        let bootstrap = KafkaClient::new(kafka_config(bootstrap_servers));
        bootstrap.connect().await?;
        let router = PartitionLeaderRouter::new(kafka_config(bootstrap_servers)).await?;
        let coordinator = retry("FindCoordinator", || {
            transaction_coordinator_connection(&bootstrap, transactional_id)
        })
        .await?;
        let producer = retry("InitProducerId", || {
            init_producer_id(
                &coordinator,
                Some(transactional_id),
                transaction_timeout_ms,
                ProducerIdAndEpoch::NONE,
            )
        })
        .await?;
        assert!(producer.producer_id >= 0, "producer id assigned");
        Ok(Self {
            transactional_id: transactional_id.to_string(),
            bootstrap,
            router,
            coordinator,
            producer,
            sequences: HashMap::new(),
            partitions_in_txn: HashSet::new(),
            in_transaction: false,
        })
    }

    /// The producer id and epoch the broker assigned.
    pub fn producer(&self) -> ProducerIdAndEpoch {
        self.producer
    }

    /// Start a transaction (client-side only, as in the Java producer).
    pub fn begin(&mut self) {
        assert!(!self.in_transaction, "transaction already open");
        self.in_transaction = true;
        self.partitions_in_txn.clear();
    }

    /// Write `records` (key, value) to `topic[partition]` as one
    /// transactional batch. Returns the batch's base offset.
    pub async fn send(
        &mut self,
        topic: &str,
        partition: i32,
        records: &[(&str, &str)],
    ) -> anyhow::Result<i64> {
        assert!(self.in_transaction, "send outside a transaction");
        let tp = (topic.to_string(), partition);
        if !self.partitions_in_txn.contains(&tp) {
            let partitions = vec![tp.clone()];
            retry("AddPartitionsToTxn", || {
                add_partitions_to_txn(
                    &self.coordinator,
                    &self.transactional_id,
                    self.producer,
                    &partitions,
                )
            })
            .await?;
            self.partitions_in_txn.insert(tp.clone());
        }

        let base_sequence = *self.sequences.get(&tp).unwrap_or(&0);
        let now = chrono::Utc::now().timestamp_millis();
        let backup_records: Vec<BackupRecord> = records
            .iter()
            .map(|(k, v)| BackupRecord {
                key: Some(k.as_bytes().to_vec()),
                value: Some(v.as_bytes().to_vec()),
                headers: vec![],
                timestamp: now,
                offset: 0,
            })
            .collect();
        let batch = encode_producer_batch(
            &backup_records,
            &ProducerBatch {
                producer: self.producer,
                base_sequence,
                transactional: true,
            },
            CompressionType::None,
        )?;
        let leader = self.router.get_leader_client(topic, partition).await?;
        let base_offset = produce_encoded_batch(
            &leader,
            topic,
            partition,
            batch,
            Some(&self.transactional_id),
            -1,
            30_000,
        )
        .await?;
        self.sequences
            .insert(tp, base_sequence + records.len() as i32);
        Ok(base_offset)
    }

    /// Commit the open transaction.
    pub async fn commit(&mut self) -> anyhow::Result<()> {
        self.end(true).await
    }

    /// Abort the open transaction.
    pub async fn abort(&mut self) -> anyhow::Result<()> {
        self.end(false).await
    }

    async fn end(&mut self, commit: bool) -> anyhow::Result<()> {
        assert!(self.in_transaction, "no open transaction");
        retry("EndTxn", || {
            end_txn(
                &self.coordinator,
                &self.transactional_id,
                self.producer,
                commit,
            )
        })
        .await?;
        self.in_transaction = false;
        Ok(())
    }

    /// The bootstrap connection (for plain produces and offset checks).
    pub fn client(&self) -> &KafkaClient {
        &self.bootstrap
    }
}

/// `(last stable offset, high watermark)` of a partition.
pub async fn lso_and_high_watermark(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
) -> anyhow::Result<(i64, i64)> {
    let (_, lso) = client
        .get_offsets(topic, partition, IsolationLevel::ReadCommitted)
        .await?;
    let (_, hw) = client
        .get_offsets(topic, partition, IsolationLevel::ReadUncommitted)
        .await?;
    Ok((lso, hw))
}

/// Wait until the partition's `(LSO, high watermark)` satisfies `done`:
/// the coordinator writes transaction markers after EndTxn returns.
pub async fn wait_for_offsets(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    done: impl Fn(i64, i64) -> bool,
) -> anyhow::Result<(i64, i64)> {
    let mut last = (-1, -1);
    for _ in 0..200 {
        last = lso_and_high_watermark(client, topic, partition).await?;
        if done(last.0, last.1) {
            return Ok(last);
        }
        sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("{topic}[{partition}]: offsets (LSO, HW) stayed at {last:?}")
}
