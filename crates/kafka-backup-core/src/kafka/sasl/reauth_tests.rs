//! KIP-368 re-auth lifecycle across reconnects, against an in-process
//! Kafka-wire mock that accepts any number of connections.
//!
//! These live inside the crate (rather than `tests/integration_suite`,
//! whose `sasl_mock_broker` serves one scripted connection) so they can
//! inject a short [`ReauthTiming`] and force reconnects directly; nothing
//! here waits for the production 30 s floor.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, MetadataResponse, RequestHeader, ResponseHeader, SaslAuthenticateResponse,
    SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use super::plugin::{SaslMechanismPlugin, SaslPluginError, SharedPluginFactory};
use super::reauth::ReauthTiming;
use crate::config::{KafkaConfig, SaslMechanism, SecurityConfig, SecurityProtocol};
use crate::kafka::KafkaClient;

/// Session lifetime the mock advertises. With [`fast_timing`] the client
/// re-authenticates every ~100 ms.
const LIFETIME_MS: i64 = 200;

fn fast_timing() -> ReauthTiming {
    ReauthTiming {
        fraction: 0.5,
        min_interval: Duration::from_millis(20),
        max_jitter: Duration::ZERO,
    }
}

/// What the mock saw, per connection index (0 = first accepted).
#[derive(Default)]
struct MockState {
    connections: AtomicUsize,
    /// `SaslHandshake`s that started an initial authentication.
    handshakes: AtomicUsize,
    /// `SaslHandshake`s on an authenticated connection (KIP-368 re-auth).
    reauth_handshakes: AtomicUsize,
    /// `SaslAuthenticate`s refused because no `SaslHandshake` preceded
    /// them on an authenticated connection.
    illegal_state: AtomicUsize,
    /// Connection index of every `SaslAuthenticate`.
    authenticates: StdMutex<Vec<usize>>,
    /// Connection index of every `Metadata` request.
    metadata: StdMutex<Vec<usize>>,
}

struct MockBroker {
    addr: SocketAddr,
    state: Arc<MockState>,
    handle: JoinHandle<()>,
}

impl MockBroker {
    /// `reject_reauth_on`: connection indices whose first re-auth (second
    /// `SaslAuthenticate`) is rejected the way Apache Kafka rejects a
    /// re-auth that presents a different principal. The connection is
    /// left open, so only the client can retire it.
    async fn start(reject_reauth_on: &[usize]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(MockState::default());
        let reject: Arc<HashSet<usize>> = Arc::new(reject_reauth_on.iter().copied().collect());
        let accept_state = state.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let idx = accept_state.connections.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(serve(stream, idx, accept_state.clone(), reject.clone()));
            }
        });
        Self {
            addr,
            state,
            handle,
        }
    }

    fn authenticates_on(&self, conn: usize) -> usize {
        let all = self.state.authenticates.lock().unwrap();
        all.iter().filter(|&&c| c == conn).count()
    }
}

impl Drop for MockBroker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn serve(
    mut stream: TcpStream,
    conn: usize,
    state: Arc<MockState>,
    reject: Arc<HashSet<usize>>,
) {
    // Apache Kafka's per-connection SASL state: a re-authentication begins
    // only with a SaslHandshake on the authenticated connection.
    let mut auths_here = 0usize;
    let mut authenticated = false;
    let mut handshake_pending = false;
    while let Some((api_key, version, correlation_id)) = read_request(&mut stream).await {
        match api_key {
            ApiKey::SaslHandshake => {
                if authenticated {
                    state.reauth_handshakes.fetch_add(1, Ordering::SeqCst);
                } else {
                    state.handshakes.fetch_add(1, Ordering::SeqCst);
                }
                handshake_pending = true;
                let resp = SaslHandshakeResponse::default().with_error_code(0);
                write_response(&mut stream, api_key, version, correlation_id, &resp).await;
            }
            ApiKey::SaslAuthenticate if authenticated && !handshake_pending => {
                state.illegal_state.fetch_add(1, Ordering::SeqCst);
                let resp = SaslAuthenticateResponse::default()
                    .with_error_code(34) // ILLEGAL_SASL_STATE
                    .with_error_message(Some(StrBytes::from_static_str(
                        "SaslAuthenticate request received after successful authentication",
                    )));
                write_response(&mut stream, api_key, version, correlation_id, &resp).await;
            }
            ApiKey::SaslAuthenticate => {
                handshake_pending = false;
                state.authenticates.lock().unwrap().push(conn);
                auths_here += 1;
                let resp = if auths_here == 2 && reject.contains(&conn) {
                    SaslAuthenticateResponse::default()
                        .with_error_code(58) // SASL_AUTHENTICATION_FAILED
                        .with_error_message(Some(StrBytes::from_string(
                            "Cannot change principals during re-authentication from \
                             User:alice: User:mallory"
                                .to_string(),
                        )))
                } else {
                    authenticated = true;
                    SaslAuthenticateResponse::default()
                        .with_error_code(0)
                        .with_session_lifetime_ms(LIFETIME_MS)
                };
                write_response(&mut stream, api_key, version, correlation_id, &resp).await;
            }
            ApiKey::Metadata => {
                state.metadata.lock().unwrap().push(conn);
                let resp = MetadataResponse::default();
                write_response(&mut stream, api_key, version, correlation_id, &resp).await;
            }
            other => panic!("mock broker: unexpected {other:?}"),
        }
    }
}

async fn read_request(stream: &mut TcpStream) -> Option<(ApiKey, i16, i32)> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.ok()?;
    let mut frame = vec![0u8; i32::from_be_bytes(len) as usize];
    stream.read_exact(&mut frame).await.ok()?;
    let api_key = ApiKey::try_from(i16::from_be_bytes([frame[0], frame[1]])).ok()?;
    let version = i16::from_be_bytes([frame[2], frame[3]]);
    let mut buf = Bytes::from(frame);
    let header = RequestHeader::decode(&mut buf, api_key.request_header_version(version)).ok()?;
    Some((api_key, version, header.correlation_id))
}

async fn write_response<R: Encodable>(
    stream: &mut TcpStream,
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    resp: &R,
) {
    let mut buf = BytesMut::new();
    buf.put_i32(0);
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut buf, api_key.response_header_version(version))
        .unwrap();
    resp.encode(&mut buf, version).unwrap();
    let len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());
    // The client may already have dropped a retired connection.
    let _ = stream.write_all(&buf).await;
}

/// OAUTHBEARER-shaped plugin that counts its calls.
#[derive(Debug, Default)]
struct CountingPlugin {
    initial: AtomicUsize,
    reauth: AtomicUsize,
}

#[async_trait]
impl SaslMechanismPlugin for CountingPlugin {
    fn mechanism_name(&self) -> &str {
        "OAUTHBEARER"
    }
    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        self.initial.fetch_add(1, Ordering::SeqCst);
        Ok(b"n,,\x01auth=Bearer token\x01\x01".to_vec())
    }
    async fn reauth_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        self.reauth.fetch_add(1, Ordering::SeqCst);
        Ok(b"n,,\x01auth=Bearer fresh\x01\x01".to_vec())
    }
}

fn client(mock: &MockBroker, plugin: Arc<CountingPlugin>) -> KafkaClient {
    let config = KafkaConfig {
        bootstrap_servers: vec![mock.addr.to_string()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            sasl_mechanism_plugin_factory: Some(SharedPluginFactory::new(plugin).into_handle()),
            ..Default::default()
        },
        topics: Default::default(),
        connection: Default::default(),
    };
    KafkaClient::new(config).with_reauth_timing(fast_timing())
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// N forced reconnects leave exactly one re-auth task, bound to the live
/// connection, and only the live connection is ever re-authenticated.
#[tokio::test]
async fn reauth_task_count_stays_one_across_reconnects() {
    const RECONNECTS: usize = 5;
    let mock = MockBroker::start(&[]).await;
    let plugin = Arc::new(CountingPlugin::default());
    let client = client(&mock, plugin.clone());

    client.connect().await.expect("connect");
    assert_eq!(client.live_reauth_tasks(), 1);

    for _ in 0..RECONNECTS {
        client.reconnect().await.expect("reconnect");
        assert_eq!(
            client.live_reauth_tasks(),
            1,
            "a reconnect must retire the old connection's re-auth task"
        );
    }
    let live = RECONNECTS; // index of the current connection
    assert_eq!(
        client.current_connection_generation().await,
        Some(RECONNECTS as u64 + 1)
    );

    // Let several re-auth periods pass (~100 ms each).
    wait_until("re-auths on the live connection", || {
        mock.authenticates_on(live) >= 4
    })
    .await;
    assert_eq!(client.live_reauth_tasks(), 1);
    for retired in 0..live {
        assert_eq!(
            mock.authenticates_on(retired),
            1,
            "retired connection {retired} saw only its initial authentication"
        );
    }
    // Every re-auth went through the one live task: initial auths are one
    // per connection, re-auths all landed on the live connection.
    assert_eq!(plugin.initial.load(Ordering::SeqCst), RECONNECTS + 1);
    let reauths = plugin.reauth.load(Ordering::SeqCst);
    let live_reauth_frames = mock.authenticates_on(live) - 1;
    assert!(
        reauths == live_reauth_frames || reauths == live_reauth_frames + 1,
        "re-auths {reauths} vs live re-auth frames {live_reauth_frames}"
    );
    // KIP-368: every re-auth began with a SaslHandshake, so the broker
    // never refused one for being out of sequence.
    assert!(mock.state.reauth_handshakes.load(Ordering::SeqCst) >= 1);
    assert_eq!(mock.state.illegal_state.load(Ordering::SeqCst), 0);

    // Dropping the client drops the connection, which aborts the task.
    let live_tasks = client.live_reauth_tasks.clone();
    drop(client);
    wait_until("the task to end with the client", || {
        live_tasks.load(Ordering::SeqCst) == 0
    })
    .await;
}

/// The generation check on its own: a task spawned for an older
/// connection that is still running when the slot is swapped (no abort)
/// exits on its next wake without touching the new connection.
#[tokio::test]
async fn stale_generation_task_exits_without_reauthenticating() {
    let mock = MockBroker::start(&[]).await;
    let plugin = Arc::new(CountingPlugin::default());
    let client = client(&mock, plugin.clone());
    client.connect().await.expect("connect");

    // Detach the live task from its connection so dropping it on
    // reconnect cannot abort it, leaving only the generation check.
    {
        let mut slot = client.connection.lock().await;
        slot.as_mut().unwrap().reauth_task.take();
    }
    client.reconnect().await.expect("reconnect");
    assert_eq!(client.live_reauth_tasks(), 2, "detached task still alive");

    wait_until("the stale task to exit", || client.live_reauth_tasks() == 1).await;
    wait_until("the live connection to re-auth", || {
        mock.authenticates_on(1) >= 2
    })
    .await;
    assert_eq!(
        mock.authenticates_on(0),
        1,
        "old connection never re-authenticated"
    );
    assert_eq!(client.live_reauth_tasks(), 1);
    assert_eq!(mock.state.illegal_state.load(Ordering::SeqCst), 0);
}

/// A re-authentication starts with a `SaslHandshake` on the authenticated
/// connection, as Apache Kafka requires; a bare `SaslAuthenticate` there is
/// refused with ILLEGAL_SASL_STATE and the connection would be dropped.
#[tokio::test]
async fn reauth_starts_with_a_sasl_handshake() {
    let mock = MockBroker::start(&[]).await;
    let plugin = Arc::new(CountingPlugin::default());
    let client = client(&mock, plugin.clone());
    client.connect().await.expect("connect");
    wait_until("three re-authentications", || mock.authenticates_on(0) >= 4).await;
    assert!(mock.state.reauth_handshakes.load(Ordering::SeqCst) >= 3);
    assert_eq!(mock.state.illegal_state.load(Ordering::SeqCst), 0);
    assert_eq!(
        mock.state.connections.load(Ordering::SeqCst),
        1,
        "no reconnects"
    );
    assert!(client.current_connection_generation().await.is_some());
}

/// A re-auth the broker rejects ("Cannot change principals during
/// re-authentication") retires the connection; the next request
/// reconnects with a full SASL handshake and succeeds.
#[tokio::test]
async fn rejected_reauth_reconnects_with_full_handshake() {
    let mock = MockBroker::start(&[0]).await;
    let plugin = Arc::new(CountingPlugin::default());
    let client = client(&mock, plugin.clone());
    client.connect().await.expect("connect");
    assert_eq!(mock.state.handshakes.load(Ordering::SeqCst), 1);

    wait_until("the rejected re-auth to clear the slot", || {
        mock.authenticates_on(0) >= 2
    })
    .await;
    let mut cleared = false;
    for _ in 0..500 {
        if client.current_connection_generation().await.is_none() {
            cleared = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(cleared, "rejected re-auth must clear the connection slot");
    assert_eq!(client.live_reauth_tasks(), 0);

    // The mock left connection 0 open, so only a reconnect explains a
    // second handshake and the request landing on connection 1.
    client
        .fetch_metadata(None)
        .await
        .expect("next request succeeds");
    assert_eq!(mock.state.connections.load(Ordering::SeqCst), 2);
    assert_eq!(mock.state.handshakes.load(Ordering::SeqCst), 2);
    assert_eq!(plugin.initial.load(Ordering::SeqCst), 2);
    assert_eq!(*mock.state.metadata.lock().unwrap(), vec![1]);
    assert_eq!(client.live_reauth_tasks(), 1, "new connection is scheduled");
}

/// `sasl_mechanism: OAUTHBEARER` without a plugin factory fails with an
/// error that says what is missing.
#[tokio::test]
async fn oauthbearer_without_plugin_names_enterprise_or_a_plugin() {
    let mock = MockBroker::start(&[]).await;
    let config = KafkaConfig {
        bootstrap_servers: vec![mock.addr.to_string()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            ..Default::default()
        },
        topics: Default::default(),
        connection: Default::default(),
    };
    let err = KafkaClient::new(config)
        .connect()
        .await
        .expect_err("no plugin installed")
        .to_string();
    assert!(
        err.contains(
            "SASL OAUTHBEARER requires Kafka Backup Enterprise (licence feature `oauth`) \
             or a custom SaslMechanismPlugin"
        ),
        "{err}"
    );
    assert_eq!(mock.state.handshakes.load(Ordering::SeqCst), 0);
}
