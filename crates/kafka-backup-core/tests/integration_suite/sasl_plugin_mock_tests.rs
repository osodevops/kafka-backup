//! Plugin dispatch tests against the in-process Kafka-wire mock.
//!
//! These are the integration counterparts to `sasl/plugin.rs` unit
//! tests: they exercise the full dispatch loop in
//! `KafkaClient::sasl_plugin_auth` end-to-end, including the KIP-368
//! re-auth scheduler path. No Docker, no network — `MockKafkaBroker`
//! serves one scripted connection on a local TCP socket.
//!
//! Layers covered:
//! 1. `SaslHandshake` wire framing (plugin-reported mechanism name makes
//!    it onto the wire).
//! 2. Single-round `SaslAuthenticate` dispatch (OAUTHBEARER-shape).
//! 3. Multi-round dispatch (broker `auth_bytes` round-trips through
//!    `continue_payload`).
//! 4. Broker-side `error_code != 0` routes to `interpret_server_error`.
//! 5. KIP-368 reauth scheduler fires after the advertised
//!    `session_lifetime_ms` expires.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use kafka_backup_core::config::{KafkaConfig, SecurityConfig, SecurityProtocol, TopicSelection};
use kafka_backup_core::kafka::KafkaClient;

use super::sasl_mock_broker::{Exchange, MockKafkaBroker};
use super::sasl_test_fixtures::{
    factory_for, NoReauthCountingPlugin, RecordingErrorPlugin, StaticOAuthBearerPlugin,
    TwoRoundPlugin,
};

fn plugin_config(
    bootstrap: String,
    plugin: kafka_backup_core::kafka::SaslMechanismPluginHandle,
) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![bootstrap],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism_plugin_factory: Some(factory_for(plugin)),
            ..Default::default()
        },
        topics: TopicSelection::default(),
        connection: Default::default(),
    }
}

/// Single-round OAUTHBEARER-shape handshake completes cleanly.
/// Verifies: handshake carries the plugin's mechanism name; exactly one
/// `SaslAuthenticate` round; plugin's initial payload reaches the wire.
#[tokio::test]
async fn dispatch_single_round_success() {
    let mock = MockKafkaBroker::start(vec![
        Exchange::HandshakeSuccess,
        Exchange::AuthenticateSuccess {
            auth_bytes: vec![],
            session_lifetime_ms: 0,
        },
    ])
    .await;

    let plugin = Arc::new(StaticOAuthBearerPlugin::for_test_user());
    let config = plugin_config(mock.bootstrap(), plugin);
    let client = KafkaClient::new(config);

    client.connect().await.expect("connect succeeds");

    let captured = mock.captured().await;
    assert_eq!(
        captured.handshake_mechanism.as_deref(),
        Some("OAUTHBEARER"),
        "handshake must carry the plugin's mechanism name"
    );
    assert_eq!(
        captured.authenticate_payloads.len(),
        1,
        "single-round dispatch sends exactly one SaslAuthenticate"
    );
    // The payload must be an RFC 7628 `n,a=...` bearer blob.
    let payload = &captured.authenticate_payloads[0];
    let s = std::str::from_utf8(payload).expect("payload is utf8");
    assert!(s.starts_with("n,a=test-user,"), "RFC 7628 prefix");
    assert!(s.contains("auth=Bearer "), "bearer token present");

    mock.shutdown().await;
}

/// Two-round dispatch: broker returns `auth_bytes` on the first round,
/// plugin emits a second `SaslAuthenticate` in response.
#[tokio::test]
async fn dispatch_multi_round_success() {
    let mock = MockKafkaBroker::start(vec![
        Exchange::HandshakeSuccess,
        Exchange::AuthenticateSuccess {
            auth_bytes: b"challenge-1".to_vec(),
            session_lifetime_ms: 0,
        },
        Exchange::AuthenticateSuccess {
            auth_bytes: b"challenge-2".to_vec(),
            session_lifetime_ms: 0,
        },
    ])
    .await;

    let plugin = Arc::new(TwoRoundPlugin);
    let config = plugin_config(mock.bootstrap(), plugin);
    let client = KafkaClient::new(config);

    client
        .connect()
        .await
        .expect("multi-round connect succeeds");

    let captured = mock.captured().await;
    assert_eq!(
        captured.authenticate_payloads,
        vec![b"round-1".to_vec(), b"round-2".to_vec()],
        "dispatch loop forwards broker's server_response bytes back through continue_payload"
    );

    mock.shutdown().await;
}

/// Broker rejects the credentials; dispatch routes `error_message` to
/// the plugin's `interpret_server_error`.
#[tokio::test]
async fn dispatch_server_error_routes_to_interpret() {
    let mock = MockKafkaBroker::start(vec![
        Exchange::HandshakeSuccess,
        Exchange::AuthenticateError {
            error_code: 58, // SASL_AUTHENTICATION_FAILED
            error_message: "bad token".to_string(),
        },
    ])
    .await;

    let plugin = Arc::new(RecordingErrorPlugin::default());
    let recorded = plugin.recorded.clone();
    let config = plugin_config(mock.bootstrap(), plugin);
    let client = KafkaClient::new(config);

    let err = client.connect().await.expect_err("broker rejected auth");
    let msg = err.to_string();
    assert!(
        msg.contains("bad token"),
        "error chain should surface interpreted detail, got: {msg}"
    );

    let recorded_bytes = recorded.lock().unwrap().clone();
    assert_eq!(
        recorded_bytes.as_deref(),
        Some(b"bad token" as &[u8]),
        "dispatch must hand raw error_message bytes to interpret_server_error"
    );

    mock.shutdown().await;
}

/// KIP-368: a non-zero `session_lifetime_ms` must spawn a reauth task
/// that fires another `SaslAuthenticate` before the broker-advertised
/// expiry.
///
/// This test runs in real time rather than `start_paused` mode because
/// the mock broker uses `TcpStream` IO, and tokio's auto-advance in
/// paused mode jumps past the client's 10s RPC timeout before the IO
/// completes. We `pause()` *after* the initial connect to time-travel
/// through the scheduler's sleep, then `resume()` so the reauth RPC
/// can flow over real sockets.
#[tokio::test]
async fn plugin_reauth_fires_at_80_percent_via_scheduler() {
    let mock = MockKafkaBroker::start(vec![
        Exchange::HandshakeSuccess,
        Exchange::AuthenticateSuccess {
            auth_bytes: vec![],
            session_lifetime_ms: 60_000,
        },
        Exchange::AuthenticateSuccess {
            auth_bytes: vec![],
            session_lifetime_ms: 60_000,
        },
    ])
    .await;

    let (plugin, counter) = StaticOAuthBearerPlugin::for_test_user().into_handle_with_counter();
    let config = plugin_config(mock.bootstrap(), plugin);
    let client = KafkaClient::new(config);

    client.connect().await.expect("initial connect");
    assert_eq!(
        counter.load(Ordering::SeqCst),
        1,
        "initial_payload called once during connect"
    );

    // Let the scheduler task get its first poll and register its sleep
    // deadline relative to *real* time, BEFORE we pause and advance.
    // Without this yield, the scheduler's sleep would be registered
    // after the clock jump and its deadline would still be ~45s in the
    // future, meaning the reauth never fires during the test window.
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(10)).await;

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();

    // Poll in real time for the reauth round-trip to land. The scheduler
    // task is now runnable; it needs a few real ms to lock, send, read.
    let mut attempts = 0;
    while counter.load(Ordering::SeqCst) < 2 && attempts < 100 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        attempts += 1;
    }

    assert!(
        counter.load(Ordering::SeqCst) >= 2,
        "reauth scheduler should have invoked reauth_payload (counter={})",
        counter.load(Ordering::SeqCst)
    );

    let captured = mock.captured().await;
    assert!(
        captured.authenticate_payloads.len() >= 2,
        "reauth must send a second SaslAuthenticate frame (got {})",
        captured.authenticate_payloads.len()
    );

    mock.shutdown().await;
}

/// The factory contract: `KafkaClient::authenticate` must call
/// `SaslMechanismPluginFactory::build` exactly once, passing the
/// configured endpoint that the client successfully dialed.
///
/// This is the test that gates the multi-broker GSSAPI correctness:
/// when `PartitionLeaderRouter` rewrites `bootstrap_servers` to the
/// advertised broker host, that host must flow into the factory so
/// GSSAPI can derive the right per-broker SPN.
#[tokio::test]
async fn factory_receives_per_broker_endpoint() {
    use async_trait::async_trait;
    use kafka_backup_core::kafka::{
        SaslMechanismPlugin, SaslMechanismPluginFactory, SaslMechanismPluginHandle, SaslPluginError,
    };
    use std::sync::Mutex;

    #[derive(Debug)]
    struct StubPlugin;
    #[async_trait]
    impl SaslMechanismPlugin for StubPlugin {
        fn mechanism_name(&self) -> &str {
            "OAUTHBEARER"
        }
        async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
            Ok(b"n,a=test-user,\x01auth=Bearer stub\x01\x01".to_vec())
        }
    }

    #[derive(Debug)]
    struct CapturingFactory {
        calls: Arc<Mutex<Vec<(String, u16)>>>,
    }

    impl SaslMechanismPluginFactory for CapturingFactory {
        fn mechanism_name(&self) -> &str {
            "OAUTHBEARER"
        }
        fn build(
            &self,
            host: &str,
            port: u16,
        ) -> Result<SaslMechanismPluginHandle, SaslPluginError> {
            self.calls.lock().unwrap().push((host.to_string(), port));
            Ok(Arc::new(StubPlugin))
        }
    }

    let mock = MockKafkaBroker::start(vec![
        Exchange::HandshakeSuccess,
        Exchange::AuthenticateSuccess {
            auth_bytes: vec![],
            session_lifetime_ms: 0,
        },
    ])
    .await;

    let calls = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(CapturingFactory {
        calls: calls.clone(),
    });

    let bootstrap = mock.bootstrap();
    let config = KafkaConfig {
        bootstrap_servers: vec![bootstrap.clone()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism_plugin_factory: Some(factory),
            ..Default::default()
        },
        topics: TopicSelection::default(),
        connection: Default::default(),
    };
    let client = KafkaClient::new(config);
    client.connect().await.expect("connect succeeds");

    let observed = calls.lock().unwrap().clone();
    assert_eq!(
        observed.len(),
        1,
        "factory::build must be called exactly once per KafkaClient authenticate"
    );
    let (host, port) = &observed[0];
    // MockKafkaBroker binds 127.0.0.1:<ephemeral>. Assert the factory
    // receives the exact host + port the client successfully dialed,
    // proving the connected endpoint flows through `parse_broker_endpoint`
    // to the factory.
    assert_eq!(
        format!("{host}:{port}"),
        bootstrap,
        "factory endpoint must match configured bootstrap exactly"
    );

    mock.shutdown().await;
}

/// Regression: if the first bootstrap entry is down and KafkaClient connects
/// to a later entry, the plugin factory must receive the endpoint that was
/// actually dialed. GSSAPI derives the broker SPN from this host, and MSK IAM
/// signs this host/port into the OAuth payload.
#[tokio::test]
async fn factory_receives_successful_bootstrap_endpoint_not_first_configured() {
    use async_trait::async_trait;
    use kafka_backup_core::kafka::{
        SaslMechanismPlugin, SaslMechanismPluginFactory, SaslMechanismPluginHandle, SaslPluginError,
    };
    use std::sync::Mutex;

    #[derive(Debug)]
    struct StubPlugin;
    #[async_trait]
    impl SaslMechanismPlugin for StubPlugin {
        fn mechanism_name(&self) -> &str {
            "OAUTHBEARER"
        }
        async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
            Ok(b"n,a=test-user,\x01auth=Bearer stub\x01\x01".to_vec())
        }
    }

    #[derive(Debug)]
    struct CapturingFactory {
        calls: Arc<Mutex<Vec<(String, u16)>>>,
    }

    impl SaslMechanismPluginFactory for CapturingFactory {
        fn mechanism_name(&self) -> &str {
            "OAUTHBEARER"
        }
        fn build(
            &self,
            host: &str,
            port: u16,
        ) -> Result<SaslMechanismPluginHandle, SaslPluginError> {
            self.calls.lock().unwrap().push((host.to_string(), port));
            Ok(Arc::new(StubPlugin))
        }
    }

    let mock = MockKafkaBroker::start(vec![
        Exchange::HandshakeSuccess,
        Exchange::AuthenticateSuccess {
            auth_bytes: vec![],
            session_lifetime_ms: 0,
        },
    ])
    .await;

    let calls = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(CapturingFactory {
        calls: calls.clone(),
    });
    let dead_first_bootstrap = "127.0.0.1:1".to_string();

    let config = KafkaConfig {
        bootstrap_servers: vec![dead_first_bootstrap.clone(), mock.bootstrap()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism_plugin_factory: Some(factory),
            ..Default::default()
        },
        topics: TopicSelection::default(),
        connection: Default::default(),
    };
    let client = KafkaClient::new(config);
    client
        .connect()
        .await
        .expect("connect should skip dead bootstrap and authenticate to mock");

    let observed = calls.lock().unwrap().clone();
    assert_eq!(observed.len(), 1);
    let (host, port) = &observed[0];
    assert_eq!(
        format!("{host}:{port}"),
        mock.bootstrap(),
        "factory must receive the successfully connected bootstrap endpoint, not {dead_first_bootstrap}"
    );

    mock.shutdown().await;
}

/// Plugins that return `supports_reauth() = false` must not get a
/// KIP-368 reauth task spawned, even when the broker advertises a
/// non-zero `session_lifetime_ms`. This is the GSSAPI opt-out contract,
/// exercised here through a mechanism-agnostic fixture so it runs
/// without the `gssapi` feature.
///
/// We advance virtual time past the 80 % reauth deadline and confirm
/// that `reauth_payload` was never called and the mock broker only ever
/// saw a single `SaslAuthenticate` frame.
#[tokio::test]
async fn reauth_scheduler_not_spawned_when_plugin_opts_out() {
    let mock = MockKafkaBroker::start(vec![
        Exchange::HandshakeSuccess,
        Exchange::AuthenticateSuccess {
            auth_bytes: vec![],
            // Non-zero lifetime would trigger the scheduler if the
            // plugin opted in — here we prove the opt-out suppresses it.
            session_lifetime_ms: 60_000,
        },
    ])
    .await;

    let (plugin, initial_calls, reauth_calls) = NoReauthCountingPlugin::handle_with_counters();
    let config = plugin_config(mock.bootstrap(), plugin);
    let client = KafkaClient::new(config);

    client.connect().await.expect("initial connect");
    assert_eq!(
        initial_calls.load(Ordering::SeqCst),
        1,
        "initial_payload called exactly once during connect"
    );

    // Let any background tasks get their first poll before we jump the
    // clock — mirrors the timing discipline of the scheduler test so a
    // would-be reauth task (if one had been spawned) would actually
    // wake up inside our time window.
    for _ in 0..5 {
        tokio::task::yield_now().await;
    }
    tokio::time::sleep(Duration::from_millis(10)).await;

    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(61)).await;
    tokio::time::resume();

    // Give the runtime real time to process anything that would have
    // fired. Nothing should, because `spawn_reauth_task` was skipped.
    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        reauth_calls.load(Ordering::SeqCst),
        0,
        "reauth_payload must not be called when plugin opts out"
    );
    assert_eq!(
        initial_calls.load(Ordering::SeqCst),
        1,
        "initial_payload must stay at 1 (no second handshake via scheduler)"
    );

    let captured = mock.captured().await;
    assert_eq!(
        captured.authenticate_payloads.len(),
        1,
        "broker must see exactly one SaslAuthenticate frame (got {})",
        captured.authenticate_payloads.len()
    );

    mock.shutdown().await;
}

/// Pool-isolation contract: when N `KafkaClient`s share a single
/// `SaslMechanismPluginFactory`, each client's `authenticate` must call
/// `build` once and receive a pointer-distinct plugin Arc.
///
/// This turns the CHANGELOG claim that the factory "removes a latent
/// risk of shared plugin state across the `connections_per_broker`
/// pool" into a tested guarantee. If someone in the future wires a
/// `SharedPluginFactory` (which deliberately returns the same Arc) into
/// the GSSAPI install path, this test fails on the pointer-distinctness
/// assertion.
#[tokio::test]
async fn pool_produces_distinct_plugin_per_kafkaclient() {
    use async_trait::async_trait;
    use kafka_backup_core::kafka::{
        SaslMechanismPlugin, SaslMechanismPluginFactory, SaslMechanismPluginHandle, SaslPluginError,
    };
    use std::collections::HashSet;
    use std::sync::Mutex;

    #[derive(Debug)]
    struct StubPlugin;
    #[async_trait]
    impl SaslMechanismPlugin for StubPlugin {
        fn mechanism_name(&self) -> &str {
            "OAUTHBEARER"
        }
        async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
            Ok(b"n,a=test-user,\x01auth=Bearer stub\x01\x01".to_vec())
        }
    }

    /// Factory that hands out a fresh Arc per call — the real contract
    /// mirrors `GssapiPluginFactory`. Retains a clone of every built Arc
    /// so the test can compare their identities after all clients have
    /// been constructed (retention keeps every allocation alive, which
    /// prevents the allocator from reusing a pointer slot and defeating
    /// the distinctness check — especially important for ZST plugins).
    #[derive(Debug)]
    struct FreshArcFactory {
        endpoints: Arc<Mutex<Vec<(String, u16)>>>,
        built: Arc<Mutex<Vec<SaslMechanismPluginHandle>>>,
    }

    impl SaslMechanismPluginFactory for FreshArcFactory {
        fn mechanism_name(&self) -> &str {
            "OAUTHBEARER"
        }
        fn build(
            &self,
            host: &str,
            port: u16,
        ) -> Result<SaslMechanismPluginHandle, SaslPluginError> {
            // Non-ZST per-call state — forces a unique heap allocation
            // for each returned Arc so pointer distinctness is a
            // meaningful identity check.
            let plugin: SaslMechanismPluginHandle = Arc::new(StubPlugin);
            self.endpoints
                .lock()
                .unwrap()
                .push((host.to_string(), port));
            self.built.lock().unwrap().push(Arc::clone(&plugin));
            Ok(plugin)
        }
    }

    const POOL_SIZE: usize = 3;

    let mut mocks = Vec::with_capacity(POOL_SIZE);
    for _ in 0..POOL_SIZE {
        mocks.push(
            MockKafkaBroker::start(vec![
                Exchange::HandshakeSuccess,
                Exchange::AuthenticateSuccess {
                    auth_bytes: vec![],
                    session_lifetime_ms: 0,
                },
            ])
            .await,
        );
    }

    let endpoints = Arc::new(Mutex::new(Vec::new()));
    let built = Arc::new(Mutex::new(Vec::new()));
    let factory = Arc::new(FreshArcFactory {
        endpoints: endpoints.clone(),
        built: built.clone(),
    });

    for mock in &mocks {
        let config = KafkaConfig {
            bootstrap_servers: vec![mock.bootstrap()],
            security: SecurityConfig {
                security_protocol: SecurityProtocol::SaslPlaintext,
                sasl_mechanism_plugin_factory: Some(factory.clone()),
                ..Default::default()
            },
            topics: TopicSelection::default(),
            connection: Default::default(),
        };
        let client = KafkaClient::new(config);
        client
            .connect()
            .await
            .expect("pool member connect succeeds");
    }

    let observed_endpoints = endpoints.lock().unwrap().clone();
    let retained = built.lock().unwrap().clone();

    assert_eq!(
        observed_endpoints.len(),
        POOL_SIZE,
        "factory::build must be called once per pool member"
    );
    assert_eq!(
        retained.len(),
        POOL_SIZE,
        "factory must have retained one plugin Arc per build call"
    );

    let expected_endpoints: HashSet<String> = mocks.iter().map(|m| m.bootstrap()).collect();
    let observed: HashSet<String> = observed_endpoints
        .iter()
        .map(|(h, p)| format!("{h}:{p}"))
        .collect();
    assert_eq!(
        observed, expected_endpoints,
        "each pool member's endpoint must flow into the factory"
    );

    // Every retained Arc still points to its own heap allocation (the
    // factory holds them all alive), so pointer equality is a sound
    // identity check here: no two pool members shared a plugin.
    for i in 0..POOL_SIZE {
        for j in (i + 1)..POOL_SIZE {
            assert!(
                !Arc::ptr_eq(&retained[i], &retained[j]),
                "pool members {i} and {j} received the same plugin Arc — \
                 shared-Arc leak regression"
            );
        }
    }

    for mock in mocks {
        mock.shutdown().await;
    }
}
