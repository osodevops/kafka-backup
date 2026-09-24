//! Issue #197 — circuit-breaker config is advisory; scoped connection
//! eviction must not stall unrelated partitions.
//!
//! These tests run in-process with no Docker. A two-broker mock speaks
//! enough of Metadata + Produce for `PartitionLeaderRouter` and a full
//! `RestoreEngine` path.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ApiKey, BrokerId, MetadataResponse, ProduceResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use kafka_backup_core::circuit_breaker::CircuitState;
use kafka_backup_core::config::{
    CircuitBreakerSettings, CompressionType, Config, ConnectionConfig, KafkaConfig, Mode,
    RestoreOptions, SecurityConfig, TopicSelection,
};
use kafka_backup_core::kafka::PartitionLeaderRouter;
use kafka_backup_core::manifest::{BackupManifest, BackupRecord, OffsetPair};
use kafka_backup_core::metrics::PerformanceMetrics;
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::segment::{BinaryRecord, SegmentWriter, SegmentWriterConfig};
use kafka_backup_core::storage::{create_backend, StorageBackendConfig};
use kafka_backup_core::CircuitBreaker;

use super::sasl_mock_broker::{read_request, write_response};

const TOPIC: &str = "issue-197-topic";

#[derive(Clone, Copy, Debug)]
enum ProduceMode {
    /// Always succeed with a monotonically increasing base offset.
    Ok,
    /// Kill the next `n` Produce requests (TCP RST). Use `n >= 2` to outlast
    /// `KafkaClient::send_request`'s single reconnect-and-retry so the
    /// router's produce loop sees the failure and runs eviction.
    KillRemaining(u32),
    /// Return broker error code 7 (REQUEST_TIMED_OUT) — terminal, no retry.
    BrokerError,
}

struct BrokerMock {
    addr: std::net::SocketAddr,
    connections: Arc<AtomicUsize>,
    produce_count: Arc<AtomicUsize>,
    produce_timestamps_ms: Arc<Mutex<Vec<u64>>>,
    produce_mode: Arc<Mutex<ProduceMode>>,
    handle: JoinHandle<()>,
}

impl BrokerMock {
    async fn start(broker_id: i32, peers: Arc<Mutex<Vec<(i32, std::net::SocketAddr)>>>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let connections = Arc::new(AtomicUsize::new(0));
        let produce_count = Arc::new(AtomicUsize::new(0));
        let produce_timestamps_ms = Arc::new(Mutex::new(Vec::new()));
        let produce_mode = Arc::new(Mutex::new(ProduceMode::Ok));
        let start = Instant::now();

        let connections_c = connections.clone();
        let produce_count_c = produce_count.clone();
        let produce_ts_c = produce_timestamps_ms.clone();
        let produce_mode_c = produce_mode.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                connections_c.fetch_add(1, Ordering::SeqCst);
                let peers = peers.clone();
                let produce_count = produce_count_c.clone();
                let produce_ts = produce_ts_c.clone();
                let produce_mode = produce_mode_c.clone();
                tokio::spawn(async move {
                    serve_broker(
                        stream,
                        broker_id,
                        peers,
                        produce_count,
                        produce_ts,
                        produce_mode,
                        start,
                    )
                    .await;
                });
            }
        });

        Self {
            addr,
            connections,
            produce_count,
            produce_timestamps_ms,
            produce_mode,
            handle,
        }
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    fn produce_count(&self) -> usize {
        self.produce_count.load(Ordering::SeqCst)
    }

    async fn set_mode(&self, mode: ProduceMode) {
        *self.produce_mode.lock().await = mode;
    }

    async fn produce_timestamps_ms(&self) -> Vec<u64> {
        self.produce_timestamps_ms.lock().await.clone()
    }

    async fn shutdown(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

async fn serve_broker(
    mut stream: TcpStream,
    broker_id: i32,
    peers: Arc<Mutex<Vec<(i32, std::net::SocketAddr)>>>,
    produce_count: Arc<AtomicUsize>,
    produce_timestamps_ms: Arc<Mutex<Vec<u64>>>,
    produce_mode: Arc<Mutex<ProduceMode>>,
    start: Instant,
) {
    while let Some((api_key, api_version, correlation_id, _body)) = read_request(&mut stream).await
    {
        match api_key {
            ApiKey::Metadata => {
                let peers = peers.lock().await.clone();
                let resp = metadata_response(&peers);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            ApiKey::Produce => {
                let mode = *produce_mode.lock().await;
                match mode {
                    ProduceMode::KillRemaining(n) if n > 0 => {
                        *produce_mode.lock().await = if n == 1 {
                            ProduceMode::Ok
                        } else {
                            ProduceMode::KillRemaining(n - 1)
                        };
                        kill_rst(stream);
                        return;
                    }
                    ProduceMode::KillRemaining(_) => {
                        // n == 0 falls through as Ok
                        let n = produce_count.fetch_add(1, Ordering::SeqCst);
                        produce_timestamps_ms
                            .lock()
                            .await
                            .push(start.elapsed().as_millis() as u64);
                        let resp = produce_ok_response(TOPIC, partition_for(broker_id), n as i64);
                        write_response(&mut stream, api_key, api_version, correlation_id, &resp)
                            .await;
                    }
                    ProduceMode::BrokerError => {
                        let resp = produce_error_response(TOPIC, partition_for(broker_id), 7);
                        write_response(&mut stream, api_key, api_version, correlation_id, &resp)
                            .await;
                    }
                    ProduceMode::Ok => {
                        let n = produce_count.fetch_add(1, Ordering::SeqCst);
                        produce_timestamps_ms
                            .lock()
                            .await
                            .push(start.elapsed().as_millis() as u64);
                        let resp = produce_ok_response(TOPIC, partition_for(broker_id), n as i64);
                        write_response(&mut stream, api_key, api_version, correlation_id, &resp)
                            .await;
                    }
                }
            }
            other => panic!("issue-197 mock broker {broker_id}: unexpected request {other:?}"),
        }
    }
}

fn partition_for(broker_id: i32) -> i32 {
    // broker 1 → partition 0, broker 2 → partition 1
    broker_id - 1
}

fn kill_rst(stream: TcpStream) {
    let sock = socket2::SockRef::from(&stream);
    let _ = sock.set_linger(Some(Duration::ZERO));
    drop(stream);
}

fn metadata_response(peers: &[(i32, std::net::SocketAddr)]) -> MetadataResponse {
    let brokers: Vec<_> = peers
        .iter()
        .map(|(id, addr)| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(*id))
                .with_host(StrBytes::from_string(addr.ip().to_string()))
                .with_port(i32::from(addr.port()))
        })
        .collect();

    let partitions: Vec<_> = peers
        .iter()
        .map(|(id, _)| {
            MetadataResponsePartition::default()
                .with_error_code(0)
                .with_partition_index(partition_for(*id))
                .with_leader_id(BrokerId(*id))
                .with_replica_nodes(vec![BrokerId(*id)])
                .with_isr_nodes(vec![BrokerId(*id)])
        })
        .collect();

    MetadataResponse::default()
        .with_brokers(brokers)
        .with_controller_id(BrokerId(peers[0].0))
        .with_topics(vec![MetadataResponseTopic::default()
            .with_error_code(0)
            .with_name(Some(TopicName(StrBytes::from_static_str(TOPIC))))
            .with_is_internal(false)
            .with_partitions(partitions)])
}

fn produce_ok_response(topic: &str, partition: i32, base_offset: i64) -> ProduceResponse {
    ProduceResponse::default().with_responses(vec![TopicProduceResponse::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partition_responses(vec![PartitionProduceResponse::default()
            .with_index(partition)
            .with_error_code(0)
            .with_base_offset(base_offset)])])
}

fn produce_error_response(topic: &str, partition: i32, code: i16) -> ProduceResponse {
    ProduceResponse::default().with_responses(vec![TopicProduceResponse::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partition_responses(vec![PartitionProduceResponse::default()
            .with_index(partition)
            .with_error_code(code)
            .with_base_offset(-1)])])
}

fn client_config(bootstrap: &str) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![bootstrap.to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection {
            include: vec![TOPIC.to_string()],
            exclude: vec![],
        },
        connection: ConnectionConfig {
            connections_per_broker: 1,
            ..Default::default()
        },
    }
}

fn record(offset: i64) -> BackupRecord {
    BackupRecord {
        key: Some(format!("k{offset}").into_bytes()),
        value: Some(format!("v{offset}").into_bytes()),
        headers: vec![],
        timestamp: 1_700_000_000_000 + offset,
        offset,
    }
}

async fn write_two_partition_backup(storage_path: &std::path::Path, backup_id: &str) {
    let storage = create_backend(&StorageBackendConfig::Filesystem {
        path: storage_path.to_path_buf(),
    })
    .expect("filesystem backend");
    let metrics = Arc::new(PerformanceMetrics::new());
    let mut manifest = BackupManifest::new(backup_id.to_string());
    manifest.compression = "none".to_string();

    for partition in [0i32, 1] {
        let key = format!(
            "{backup_id}/topics/{TOPIC}/partition={partition}/segment-{:020}.bin",
            0
        );
        let config = SegmentWriterConfig {
            compression: CompressionType::None,
            max_segment_bytes: 1024 * 1024,
            ..Default::default()
        };
        let mut writer = SegmentWriter::new(config, storage.clone(), metrics.clone());
        for i in 0..20 {
            let offset = i as i64;
            let r = record(offset);
            writer
                .add_record(BinaryRecord {
                    timestamp: r.timestamp,
                    offset: r.offset,
                    key: r.key.map(Bytes::from),
                    value: r.value.map(Bytes::from),
                    headers: vec![],
                })
                .unwrap();
        }
        let meta = writer.flush(&key).await.unwrap().expect("segment written");
        let topic = manifest.get_or_create_topic(TOPIC);
        topic.original_partition_count = Some(2);
        topic.get_or_create_partition(partition).add_segment(meta);
    }

    let json = serde_json::to_vec_pretty(&manifest).unwrap();
    storage
        .put(&format!("{backup_id}/manifest.json"), Bytes::from(json))
        .await
        .unwrap();
}

/// A connection error on broker 1 must not rebuild broker 2's pool.
#[tokio::test]
async fn produce_connection_error_evicts_only_the_failing_broker() {
    let peers = Arc::new(Mutex::new(Vec::new()));
    let b1 = BrokerMock::start(1, peers.clone()).await;
    let b2 = BrokerMock::start(2, peers.clone()).await;
    *peers.lock().await = vec![(1, b1.addr), (2, b2.addr)];

    let router = PartitionLeaderRouter::new(client_config(&b1.addr.to_string()))
        .await
        .expect("router bootstrap");

    // Warm both pools (connections_per_broker = 1).
    router
        .produce(TOPIC, 0, vec![record(0)], 1, 5_000)
        .await
        .expect("warm produce p0");
    router
        .produce(TOPIC, 1, vec![record(0)], 1, 5_000)
        .await
        .expect("warm produce p1");

    let b2_after_warm = b2.connections();
    assert!(b2_after_warm >= 1, "broker 2 should have a warm pool");

    // Kill the next 2 Produce requests so the failure outlasts
    // send_request's single reconnect and the router's eviction path runs.
    b1.set_mode(ProduceMode::KillRemaining(2)).await;
    router
        .produce(TOPIC, 0, vec![record(1)], 1, 5_000)
        .await
        .expect("produce after kills should retry at router layer and succeed");

    // Touch partition 1 again. With scoped eviction the warm broker-2 pool is
    // reused (connection count unchanged). With the old global
    // clear_connection_cache the pool was dropped and this produce rebuilds
    // it, bumping broker 2's accept count.
    router
        .produce(TOPIC, 1, vec![record(1)], 1, 5_000)
        .await
        .expect("produce p1 after broker1 blip");

    assert_eq!(
        b2.connections(),
        b2_after_warm,
        "broker 2 pool must not be rebuilt when broker 1 fails (scoped eviction); \
         after_warm={b2_after_warm}, now={}",
        b2.connections()
    );

    b1.shutdown().await;
    b2.shutdown().await;
}

/// Opening the advisory Kafka breaker must not pause the healthy partition.
#[tokio::test]
async fn open_breaker_does_not_pause_other_partitions() {
    run_breaker_pause_scenario(true).await;
}

#[tokio::test]
async fn disabled_breaker_stays_closed_on_partition_failure() {
    run_breaker_pause_scenario(false).await;
}

async fn run_breaker_pause_scenario(breaker_enabled: bool) {
    let peers = Arc::new(Mutex::new(Vec::new()));
    let b1 = BrokerMock::start(1, peers.clone()).await;
    let b2 = BrokerMock::start(2, peers.clone()).await;
    *peers.lock().await = vec![(1, b1.addr), (2, b2.addr)];

    // Partition 0 (broker 1) fails terminally; partition 1 keeps producing.
    b1.set_mode(ProduceMode::BrokerError).await;

    let tmp = TempDir::new().unwrap();
    let backup_id = "issue-197-restore";
    write_two_partition_backup(tmp.path(), backup_id).await;

    let config = Config {
        mode: Mode::Restore,
        backup_id: backup_id.to_string(),
        source: None,
        target: Some(client_config(&b1.addr.to_string())),
        storage: StorageBackendConfig::Filesystem {
            path: tmp.path().to_path_buf(),
        },
        backup: None,
        restore: Some(RestoreOptions {
            restore_topic_configs: false,
            create_topics: false,
            max_concurrent_partitions: 2,
            produce_batch_size: 5,
            produce_acks: 1,
            circuit_breaker: CircuitBreakerSettings {
                enabled: breaker_enabled,
                failure_threshold: 1,
                reset_timeout_ms: 20_000,
                success_threshold: 1,
            },
            ..Default::default()
        }),
        offset_storage: None,
        metrics: None,
    };

    let engine = RestoreEngine::new(config).expect("engine");
    let started = Instant::now();
    let result = engine.run().await;
    let elapsed = started.elapsed();

    assert!(result.is_err(), "partition 0 failure should fail the run");

    if breaker_enabled {
        assert_eq!(
            engine.kafka_circuit_state(),
            CircuitState::Open,
            "failure_threshold=1 should open the advisory breaker"
        );
    } else {
        assert_eq!(
            engine.kafka_circuit_state(),
            CircuitState::Closed,
            "disabled breaker must stay Closed"
        );
    }

    let ts = b2.produce_timestamps_ms().await;
    assert!(
        !ts.is_empty(),
        "healthy partition (broker 2) must have produced; count={}",
        b2.produce_count()
    );
    let mut max_gap = 0u64;
    for w in ts.windows(2) {
        max_gap = max_gap.max(w[1].saturating_sub(w[0]));
    }
    assert!(
        max_gap < 5_000,
        "broker 2 produce gap {max_gap}ms must be well under the 20s reset_timeout \
         (breaker must not pause healthy partitions); elapsed={elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(15),
        "restore must fail fast without waiting reset_timeout; elapsed={elapsed:?}"
    );

    b1.shutdown().await;
    b2.shutdown().await;
}

#[test]
fn disabled_breaker_unit_is_inert() {
    let cb = CircuitBreaker::disabled("kafka");
    for _ in 0..10 {
        cb.record_failure();
    }
    assert_eq!(cb.state(), CircuitState::Closed);
    assert!(cb.is_allowed());
}

#[test]
fn add_detailed_batch_equivalence_smoke() {
    use kafka_backup_core::OffsetMapping;
    let mut a = OffsetMapping::new();
    let mut b = OffsetMapping::new();
    let pairs: Vec<OffsetPair> = (0..5)
        .map(|i| OffsetPair {
            source_offset: i,
            target_offset: 100 + i,
            timestamp: i,
        })
        .collect();
    for p in &pairs {
        a.add_detailed("t", 0, p.source_offset, p.target_offset, p.timestamp);
    }
    b.add_detailed_batch("t", 0, pairs);
    assert_eq!(a.detailed_mapping_count(), b.detailed_mapping_count());
    assert_eq!(
        a.lookup_target_offset("t", 0, 3),
        b.lookup_target_offset("t", 0, 3)
    );
}
