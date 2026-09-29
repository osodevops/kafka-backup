//! Issue #201 — a partition with no leader (metadata `leader_id = -1` /
//! LEADER_NOT_AVAILABLE) or a leader whose address refuses connections must
//! be waited out, not treated as terminal.
//!
//! In-process mock cluster (no Docker): brokers share one leader map that a
//! test can change at runtime (simulating an election), and a broker can be
//! hard-killed (listener closed, open connections dropped) to simulate a
//! crashed pod rather than a graceful shutdown.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ApiKey, BrokerId, MetadataResponse, ProduceResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use kafka_backup_core::config::{ConnectionConfig, KafkaConfig, SecurityConfig, TopicSelection};
use kafka_backup_core::error::KafkaError;
use kafka_backup_core::kafka::PartitionLeaderRouter;
use kafka_backup_core::manifest::BackupRecord;

use super::sasl_mock_broker::{read_request, write_response};

const TOPIC: &str = "issue-201-topic";
const NOT_LEADER_FOR_PARTITION: i16 = 6;
const LEADER_NOT_AVAILABLE: i16 = 5;

/// Cluster-wide state shared by every mock broker.
#[derive(Clone, Default)]
struct Cluster {
    /// Advertised brokers (id, addr). A killed broker stays advertised — its
    /// address simply refuses connections, like a crashed pod's service IP.
    peers: Arc<Mutex<Vec<(i32, std::net::SocketAddr)>>>,
    /// partition -> leader broker id; `-1` = no leader elected.
    leaders: Arc<Mutex<HashMap<i32, i32>>>,
}

impl Cluster {
    async fn set_leader(&self, partition: i32, leader: i32) {
        self.leaders.lock().await.insert(partition, leader);
    }
}

struct Broker {
    addr: std::net::SocketAddr,
    accept: JoinHandle<()>,
    conns: Arc<Mutex<Vec<JoinHandle<()>>>>,
    produces: Arc<AtomicUsize>,
}

impl Broker {
    async fn start(id: i32, cluster: Cluster) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let conns: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let produces = Arc::new(AtomicUsize::new(0));
        let (conns_c, produces_c) = (conns.clone(), produces.clone());
        let accept = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let (cluster, produces) = (cluster.clone(), produces_c.clone());
                let h = tokio::spawn(async move { serve(stream, id, cluster, produces).await });
                conns_c.lock().await.push(h);
            }
        });
        Self {
            addr,
            accept,
            conns,
            produces,
        }
    }

    fn produces(&self) -> usize {
        self.produces.load(Ordering::SeqCst)
    }

    /// Hard kill: stop accepting (address now refuses) and drop every open
    /// connection (clients see EOF / reset on their next request).
    async fn kill(&self) {
        self.accept.abort();
        for h in self.conns.lock().await.drain(..) {
            h.abort();
            let _ = h.await;
        }
    }
}

async fn serve(
    mut stream: TcpStream,
    broker_id: i32,
    cluster: Cluster,
    produces: Arc<AtomicUsize>,
) {
    while let Some((api_key, api_version, correlation_id, _body)) = read_request(&mut stream).await
    {
        match api_key {
            ApiKey::Metadata => {
                let peers = cluster.peers.lock().await.clone();
                let leaders = cluster.leaders.lock().await.clone();
                let resp = metadata_response(&peers, &leaders);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            ApiKey::Produce => {
                // Answer for every known partition: OK if this broker leads
                // it, NOT_LEADER otherwise. (The client matches on the
                // partition index it asked for.)
                let leaders = cluster.leaders.lock().await.clone();
                let n = produces.fetch_add(1, Ordering::SeqCst);
                let mut parts = Vec::new();
                for (partition, leader) in leaders {
                    let p = if leader == broker_id {
                        PartitionProduceResponse::default()
                            .with_index(partition)
                            .with_error_code(0)
                            .with_base_offset(n as i64)
                    } else {
                        PartitionProduceResponse::default()
                            .with_index(partition)
                            .with_error_code(NOT_LEADER_FOR_PARTITION)
                            .with_base_offset(-1)
                    };
                    parts.push(p);
                }
                let resp =
                    ProduceResponse::default()
                        .with_responses(vec![TopicProduceResponse::default()
                            .with_name(TopicName(StrBytes::from_static_str(TOPIC)))
                            .with_partition_responses(parts)]);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            other => panic!("issue-201 mock broker {broker_id}: unexpected request {other:?}"),
        }
    }
}

fn metadata_response(
    peers: &[(i32, std::net::SocketAddr)],
    leaders: &HashMap<i32, i32>,
) -> MetadataResponse {
    let brokers: Vec<_> = peers
        .iter()
        .map(|(id, addr)| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(*id))
                .with_host(StrBytes::from_string(addr.ip().to_string()))
                .with_port(i32::from(addr.port()))
        })
        .collect();
    let mut partitions: Vec<_> = leaders
        .iter()
        .map(|(partition, leader)| {
            let (code, replicas) = if *leader < 0 {
                (LEADER_NOT_AVAILABLE, vec![])
            } else {
                (0, vec![BrokerId(*leader)])
            };
            MetadataResponsePartition::default()
                .with_error_code(code)
                .with_partition_index(*partition)
                .with_leader_id(BrokerId(*leader))
                .with_replica_nodes(replicas.clone())
                .with_isr_nodes(replicas)
        })
        .collect();
    partitions.sort_by_key(|p| p.partition_index);
    MetadataResponse::default()
        .with_brokers(brokers)
        .with_controller_id(BrokerId(peers[0].0))
        .with_topics(vec![MetadataResponseTopic::default()
            .with_error_code(0)
            .with_name(Some(TopicName(StrBytes::from_static_str(TOPIC))))
            .with_is_internal(false)
            .with_partitions(partitions)])
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

async fn two_broker_cluster(leaders: &[(i32, i32)]) -> (Cluster, Broker, Broker) {
    let cluster = Cluster::default();
    for (p, l) in leaders {
        cluster.set_leader(*p, *l).await;
    }
    let b1 = Broker::start(1, cluster.clone()).await;
    let b2 = Broker::start(2, cluster.clone()).await;
    *cluster.peers.lock().await = vec![(1, b1.addr), (2, b2.addr)];
    (cluster, b1, b2)
}

/// Metadata says partition 0 has no leader (`-1`). The router must not
/// cache `-1` (which used to surface as the terminal "Unknown broker ID:
/// -1"), and a produce must wait for the election and then succeed.
#[tokio::test]
async fn produce_waits_for_leader_election_instead_of_failing() {
    let (cluster, b1, b2) = two_broker_cluster(&[(0, -1), (1, 2)]).await;
    let router = PartitionLeaderRouter::new(client_config(&b1.addr.to_string()))
        .await
        .expect("router bootstrap");

    // No leader → PartitionNotAvailable (not "Unknown broker ID: -1"), and
    // nothing cached for the partition.
    match router.get_leader(TOPIC, 0).await {
        Err(kafka_backup_core::Error::Kafka(KafkaError::PartitionNotAvailable {
            partition: 0,
            ..
        })) => {}
        other => panic!("expected PartitionNotAvailable, got {other:?}"),
    }
    // The partition with a leader is unaffected.
    assert_eq!(router.get_leader(TOPIC, 1).await.expect("p1 leader"), 2);

    // Election completes after ~800 ms.
    let c = cluster.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(800)).await;
        c.set_leader(0, 1).await;
    });

    let started = Instant::now();
    router
        .produce(TOPIC, 0, vec![record(0)], 1, 5_000)
        .await
        .expect("produce must wait for the leader and succeed");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_millis(700) && elapsed < Duration::from_secs(10),
        "should have waited for the election, elapsed={elapsed:?}"
    );
    assert_eq!(b1.produces(), 1, "record produced on the elected leader");
    assert_eq!(b2.produces(), 0);

    b1.kill().await;
    b2.kill().await;
}

/// The leader is hard-killed mid-run (no controlled shutdown, so the client
/// never sees NOT_LEADER — just EOF and then "connection refused"). Leadership
/// moves to broker 2. The produce must recover via the connection-retry loop
/// (refused connect classified as retriable) plus a metadata refresh, instead
/// of failing after one refused reconnect.
#[tokio::test]
async fn produce_recovers_when_leader_dies_hard_and_leadership_moves() {
    let (cluster, b1, b2) = two_broker_cluster(&[(0, 1), (1, 2)]).await;
    // Bootstrap through broker 2 so metadata stays reachable after the kill.
    let router = PartitionLeaderRouter::new(client_config(&b2.addr.to_string()))
        .await
        .expect("router bootstrap");

    router
        .produce(TOPIC, 0, vec![record(0)], 1, 5_000)
        .await
        .expect("warm produce on broker 1");
    assert_eq!(b1.produces(), 1);

    // Crash broker 1; the controller elects broker 2 half a second later.
    b1.kill().await;
    let c = cluster.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        c.set_leader(0, 2).await;
    });

    let started = Instant::now();
    router
        .produce(TOPIC, 0, vec![record(1)], 1, 5_000)
        .await
        .expect("produce must fail over to the new leader");
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(15),
        "failover should be quick, elapsed={elapsed:?}"
    );
    assert_eq!(b2.produces(), 1, "record produced on the new leader");

    b2.kill().await;
}

/// A partition whose only replica is down for longer than the wait budget
/// still fails — but with the metadata error, not "Unknown broker ID: -1".
/// (Uses the real budget; kept fast by asserting only the first refresh.)
#[tokio::test]
async fn no_leader_error_is_partition_not_available() {
    let (_cluster, b1, b2) = two_broker_cluster(&[(0, -1)]).await;
    let router = PartitionLeaderRouter::new(client_config(&b1.addr.to_string()))
        .await
        .expect("router bootstrap");
    let err = router.get_leader(TOPIC, 0).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("not available") && !msg.contains("Unknown broker ID"),
        "unexpected error text: {msg}"
    );
    b1.kill().await;
    b2.kill().await;
}
