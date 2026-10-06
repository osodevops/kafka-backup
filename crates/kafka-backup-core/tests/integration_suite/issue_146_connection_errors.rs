//! Issue #146 — connection-error classification drives reconnect-and-retry.
//!
//! `KafkaClient::send_request` reconnects and retries once when a request
//! fails with a connection-level error. Before the fix that decision was made
//! by substring-matching the OS error *message*, which is localized and
//! platform-specific — Windows' `WSAECONNABORTED` / `WSAECONNRESET` texts
//! never matched, so a dropped socket failed the whole run. Classification is
//! now structural (`io::ErrorKind` + raw OS code, see
//! `kafka::connection_error`).
//!
//! These tests exercise the *whole* path with real sockets and real OS
//! errors, in-process and without Docker: a tiny broker mock accepts the
//! client, kills the first connection in a scripted way (TCP RST → `ECONNRESET`
//! / `WSAECONNRESET`, or a clean FIN → `UnexpectedEof`), and answers
//! `Metadata` on the next one. The client must classify, reconnect, retry,
//! and succeed. Because the classification is by kind, the same test is
//! meaningful on Linux, macOS and Windows.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{ApiKey, BrokerId, FetchResponse, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use kafka_backup_core::config::{KafkaConfig, SecurityConfig, TopicSelection};
use kafka_backup_core::error::KafkaError;
use kafka_backup_core::kafka::{is_connection_error, KafkaClient, PartitionLeaderRouter};

use super::sasl_mock_broker::{read_request, write_response};

/// How the mock kills a connection after reading one request.
#[derive(Clone, Copy, Debug)]
enum Kill {
    /// `SO_LINGER = 0` then drop: the peer sees a TCP RST, i.e.
    /// `ECONNRESET` on Unix / `WSAECONNRESET` (10054) on Windows.
    Reset,
    /// Plain drop: the peer sees FIN, i.e. `UnexpectedEof` ("early eof")
    /// from tokio's `read_exact`.
    Close,
}

const TOPIC: &str = "issue-146-topic";

struct FlakyBroker {
    addr: std::net::SocketAddr,
    connections: Arc<AtomicUsize>,
    handle: JoinHandle<()>,
}

impl FlakyBroker {
    /// Start a mock that kills the first `kill_first_n` connections after
    /// reading one request, then serves `Metadata` normally on any further
    /// connection.
    async fn start(kill: Kill, kill_first_n: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("local addr");
        let connections = Arc::new(AtomicUsize::new(0));
        let counter = connections.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
                if n <= kill_first_n {
                    tokio::spawn(kill_after_one_request(stream, kill));
                } else {
                    tokio::spawn(serve_metadata(stream, addr));
                }
            }
        });
        Self {
            addr,
            connections,
            handle,
        }
    }

    /// Start a mock that always serves `Metadata` (one topic, one partition,
    /// itself as leader) but kills the connection on the first
    /// `kill_first_n_fetches` `Fetch` requests it sees, then answers `Fetch`
    /// with an empty partition. Drives the router's fetch retry loop the way
    /// a proxy or broker resetting connections for a while would.
    async fn start_killing_fetches(kill: Kill, kill_first_n_fetches: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().expect("local addr");
        let connections = Arc::new(AtomicUsize::new(0));
        let counter = connections.clone();
        let fetch_kills_left = Arc::new(AtomicUsize::new(kill_first_n_fetches));
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                counter.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve_metadata_and_fetch(
                    stream,
                    addr,
                    kill,
                    fetch_kills_left.clone(),
                ));
            }
        });
        Self {
            addr,
            connections,
            handle,
        }
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    async fn shutdown(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

fn kill_stream(stream: TcpStream, kill: Kill) {
    match kill {
        Kill::Reset => {
            let sock = socket2::SockRef::from(&stream);
            sock.set_linger(Some(Duration::ZERO))
                .expect("set SO_LINGER=0 to force RST");
            drop(stream);
        }
        Kill::Close => drop(stream),
    }
}

async fn kill_after_one_request(mut stream: TcpStream, kill: Kill) {
    // Read (and ignore) exactly one request so the client is parked in
    // `read_exact` waiting for the response when the socket dies.
    let _ = read_request(&mut stream).await;
    kill_stream(stream, kill);
}

fn metadata_response(addr: std::net::SocketAddr, with_topic: bool) -> MetadataResponse {
    let topics = if with_topic {
        vec![MetadataResponseTopic::default()
            .with_error_code(0)
            .with_name(Some(TopicName(StrBytes::from_static_str(TOPIC))))
            .with_is_internal(false)
            .with_partitions(vec![MetadataResponsePartition::default()
                .with_error_code(0)
                .with_partition_index(0)
                .with_leader_id(BrokerId(1))
                .with_replica_nodes(vec![BrokerId(1)])
                .with_isr_nodes(vec![BrokerId(1)])])]
    } else {
        vec![]
    };
    MetadataResponse::default()
        .with_brokers(vec![MetadataResponseBroker::default()
            .with_node_id(BrokerId(1))
            .with_host(StrBytes::from_string(addr.ip().to_string()))
            .with_port(i32::from(addr.port()))])
        .with_controller_id(BrokerId(1))
        .with_topics(topics)
}

async fn serve_metadata(mut stream: TcpStream, addr: std::net::SocketAddr) {
    while let Some((api_key, api_version, correlation_id, _body)) = read_request(&mut stream).await
    {
        match api_key {
            ApiKey::Metadata => {
                let resp = metadata_response(addr, false);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            other => panic!("mock broker: unexpected request {other:?}"),
        }
    }
}

async fn serve_metadata_and_fetch(
    mut stream: TcpStream,
    addr: std::net::SocketAddr,
    kill: Kill,
    fetch_kills_left: Arc<AtomicUsize>,
) {
    while let Some((api_key, api_version, correlation_id, _body)) = read_request(&mut stream).await
    {
        match api_key {
            ApiKey::Metadata => {
                let resp = metadata_response(addr, true);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            ApiKey::Fetch => {
                // `fetch_update` is deprecated (renamed `try_update`) on newer
                // stables, but `try_update` is missing on older ones.
                #[allow(deprecated)]
                let kill_this = fetch_kills_left
                    .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok();
                if kill_this {
                    kill_stream(stream, kill);
                    return;
                }
                let resp =
                    FetchResponse::default().with_responses(vec![FetchableTopicResponse::default(
                    )
                    .with_topic(TopicName(StrBytes::from_static_str(TOPIC)))
                    .with_partitions(vec![PartitionData::default()
                        .with_partition_index(0)
                        .with_error_code(0)
                        .with_high_watermark(0)
                        .with_last_stable_offset(0)
                        .with_log_start_offset(0)
                        .with_records(None)])]);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            other => panic!("mock broker: unexpected request {other:?}"),
        }
    }
}

fn client_config(addr: &std::net::SocketAddr) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![addr.to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection::default(),
        connection: Default::default(),
    }
}

fn client_for(addr: &std::net::SocketAddr) -> KafkaClient {
    KafkaClient::new(client_config(addr))
}

async fn run_metadata_against(
    kill: Kill,
    kill_first_n: usize,
) -> (kafka_backup_core::Result<usize>, usize) {
    let broker = FlakyBroker::start(kill, kill_first_n).await;
    let client = client_for(&broker.addr);
    client.connect().await.expect("initial TCP connect");

    let result = tokio::time::timeout(Duration::from_secs(20), client.fetch_metadata(None))
        .await
        .expect("fetch_metadata should not hang")
        .map(|topics| topics.len());

    let connections = broker.connections();
    broker.shutdown().await;
    (result, connections)
}

/// TCP RST mid-request → `ConnectionReset` → reconnect → retry succeeds.
#[tokio::test]
async fn test_reconnects_after_tcp_reset() {
    let (result, connections) = run_metadata_against(Kill::Reset, 1).await;
    assert!(
        result.is_ok(),
        "request after a TCP reset should be retried on a fresh connection, got {result:?}"
    );
    assert_eq!(connections, 2, "one killed connection + one reconnect");
}

/// Clean FIN mid-request → `UnexpectedEof` → reconnect → retry succeeds.
#[tokio::test]
async fn test_reconnects_after_peer_close() {
    let (result, connections) = run_metadata_against(Kill::Close, 1).await;
    assert!(
        result.is_ok(),
        "request after a peer close should be retried on a fresh connection, got {result:?}"
    );
    assert_eq!(connections, 2, "one killed connection + one reconnect");
}

/// When the retry also dies, the error that surfaces is the structured
/// `ConnectionIo` carrying the real `io::ErrorKind` — classifiable without
/// reading the (localized) message — and it names the operation.
#[tokio::test]
async fn test_surfaced_error_is_structured_and_classifiable() {
    let (result, connections) = run_metadata_against(Kill::Reset, 2).await;
    let err = result.expect_err("both attempts were reset, so the request must fail");
    assert_eq!(connections, 2, "send_request retries exactly once");
    assert!(
        is_connection_error(&err),
        "surfaced error must classify as a connection error: {err}"
    );
    match &err {
        kafka_backup_core::Error::Kafka(KafkaError::ConnectionIo {
            operation, kind, ..
        }) => {
            assert_eq!(operation, "read response length");
            assert!(
                matches!(
                    kind,
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
                ),
                "unexpected kind {kind:?} (a RST normally surfaces as ConnectionReset; some \
                 stacks deliver EOF first)"
            );
        }
        other => panic!("expected KafkaError::ConnectionIo, got {other:?}"),
    }
}

/// The router's fetch loop must outlast a reset window longer than the
/// client's single immediate reconnect: three consecutive `Fetch`s die
/// (initial + `send_request`'s retry + the router's first retry) before the
/// broker behaves again. This is the backup path — before the loop existed a
/// proxy or broker resetting connections for a second or two failed the
/// partition and the whole run.
#[tokio::test]
async fn test_router_fetch_survives_repeated_connection_resets() {
    let broker = FlakyBroker::start_killing_fetches(Kill::Reset, 3).await;
    let router = PartitionLeaderRouter::new(client_config(&broker.addr))
        .await
        .expect("router bootstrap via Metadata");

    let result = tokio::time::timeout(
        Duration::from_secs(30),
        router.fetch(TOPIC, 0, 0, 1024 * 1024),
    )
    .await
    .expect("fetch should not hang");

    let connections = broker.connections();
    broker.shutdown().await;

    let resp = result.expect("fetch should succeed once the resets stop");
    assert!(resp.records.is_empty());
    assert!(
        connections >= 4,
        "expected bootstrap + at least three killed fetch connections + one good one, saw {connections}"
    );
}
