//! SASL integration tests for kafka-backup-core.
//!
//! Establishes the pre-refactor regression baseline for the
//! `SaslMechanismPlugin` work tracked in `docs/PRD-sasl-mechanism-plugin.md`.
//! These tests exist so that any subsequent refactor of the SASL dispatch
//! path has a green behavioral floor to hold.
//!
//! Coverage today: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` on
//! `SASL_PLAINTEXT` — both the initial-connect path (`authenticate()`) and
//! the reconnect path (`authenticate_raw()` via `docker restart`).
//!
//! ## Prerequisites
//!
//! ```bash
//! cd tests/sasl-test-infra
//! docker-compose -f docker-compose-sasl.yml up -d
//! # Wait for scram-init to finish:
//! docker logs kafka-sasl-scram-init
//! ```
//!
//! ## Running
//!
//! ```bash
//! cargo test -p kafka-backup-core \
//!     --test integration_suite_tests -- --ignored sasl_
//! ```

use std::process::Command;
use std::time::Duration;

use kafka_backup_core::config::{
    KafkaConfig, SaslMechanism, SecurityConfig, SecurityProtocol, TopicSelection,
};
use kafka_backup_core::kafka::KafkaClient;
use tokio::time::sleep;

const SASL_BOOTSTRAP: &str = "localhost:9095";
const SASL_CONTAINER: &str = "kafka-sasl-test";

fn sasl_config(mechanism: SaslMechanism, username: &str, password: &str) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![SASL_BOOTSTRAP.to_string()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(mechanism),
            sasl_username: Some(username.to_string()),
            sasl_password: Some(password.to_string()),
            ..Default::default()
        },
        topics: TopicSelection::default(),
        connection: Default::default(),
    }
}

/// Baseline: SASL PLAIN connect + metadata fetch.
/// Exercises the initial-connect path through `sasl_plain_auth()`.
#[tokio::test]
#[ignore = "requires Docker SASL Kafka - run docker-compose-sasl.yml first"]
async fn sasl_plain_baseline() {
    let config = sasl_config(SaslMechanism::Plain, "alice", "alice-secret");
    let client = KafkaClient::new(config);

    let connect = client.connect().await;
    assert!(
        connect.is_ok(),
        "SASL PLAIN connect should succeed. Error: {:?}",
        connect.err()
    );

    let metadata = client.fetch_metadata(None).await;
    assert!(
        metadata.is_ok(),
        "SASL PLAIN metadata fetch should succeed. Error: {:?}",
        metadata.err()
    );
}

/// Baseline: SASL SCRAM-SHA-256 connect + metadata fetch.
/// Exercises `sasl_scram_auth()` with the SHA-256 hash path.
#[tokio::test]
#[ignore = "requires Docker SASL Kafka - run docker-compose-sasl.yml first"]
async fn sasl_scram_sha256_baseline() {
    let config = sasl_config(SaslMechanism::ScramSha256, "bob", "bob-secret");
    let client = KafkaClient::new(config);

    let connect = client.connect().await;
    assert!(
        connect.is_ok(),
        "SASL SCRAM-SHA-256 connect should succeed. Error: {:?}",
        connect.err()
    );

    let metadata = client.fetch_metadata(None).await;
    assert!(
        metadata.is_ok(),
        "SASL SCRAM-SHA-256 metadata fetch should succeed. Error: {:?}",
        metadata.err()
    );
}

/// Baseline: SASL SCRAM-SHA-512 connect + metadata fetch.
/// Exercises `sasl_scram_auth()` with the SHA-512 hash path.
#[tokio::test]
#[ignore = "requires Docker SASL Kafka - run docker-compose-sasl.yml first"]
async fn sasl_scram_sha512_baseline() {
    let config = sasl_config(SaslMechanism::ScramSha512, "carol", "carol-secret");
    let client = KafkaClient::new(config);

    let connect = client.connect().await;
    assert!(
        connect.is_ok(),
        "SASL SCRAM-SHA-512 connect should succeed. Error: {:?}",
        connect.err()
    );

    let metadata = client.fetch_metadata(None).await;
    assert!(
        metadata.is_ok(),
        "SASL SCRAM-SHA-512 metadata fetch should succeed. Error: {:?}",
        metadata.err()
    );
}

/// Reconnect path: restart the broker mid-session and verify the client
/// re-authenticates cleanly through `authenticate_raw()`.
///
/// This is the harness that proves the refactor in commit 2 (unifying
/// `authenticate()` and `authenticate_raw()`) does not regress reconnect
/// behavior.
#[tokio::test]
#[ignore = "requires Docker SASL Kafka + docker CLI access"]
async fn sasl_plain_survives_broker_restart() {
    let config = sasl_config(SaslMechanism::Plain, "alice", "alice-secret");
    let client = KafkaClient::new(config);

    client.connect().await.expect("initial connect");
    client
        .fetch_metadata(None)
        .await
        .expect("pre-restart metadata fetch");

    let restart = Command::new("docker")
        .args(["restart", SASL_CONTAINER])
        .output()
        .expect("docker restart command to run");
    assert!(
        restart.status.success(),
        "docker restart {} failed: stderr={}",
        SASL_CONTAINER,
        String::from_utf8_lossy(&restart.stderr)
    );

    // Give the broker a moment to start listening again before the client
    // notices and triggers the reconnect path. The actual listener port
    // comes back within 5-10s on a cp-kafka restart.
    sleep(Duration::from_secs(12)).await;

    // The prior TCP connection is dead; the next send_request will detect
    // it via `is_connection_error`, trigger `reconnect()`, which calls
    // `authenticate_raw()` — the path we explicitly want under test.
    let mut last_err = None;
    for attempt in 0..5 {
        match client.fetch_metadata(None).await {
            Ok(_) => {
                return;
            }
            Err(e) => {
                last_err = Some(format!("attempt {attempt}: {e}"));
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!(
        "post-restart metadata fetch never recovered; last error: {}",
        last_err.unwrap_or_else(|| "<none>".into())
    );
}
