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
use super::sasl_test_fixtures::{RecordingErrorPlugin, StaticOAuthBearerPlugin, TwoRoundPlugin};

fn plugin_config(
    bootstrap: String,
    plugin: kafka_backup_core::kafka::SaslMechanismPluginHandle,
) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![bootstrap],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism_plugin: Some(plugin),
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
