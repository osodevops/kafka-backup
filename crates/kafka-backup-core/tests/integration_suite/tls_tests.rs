//! TLS/SSL integration tests for kafka-backup-core.
//!
//! These tests verify that the Kafka client correctly handles:
//! - Custom CA certificates (self-signed/internal)
//! - mTLS client authentication
//! - Error handling for invalid certificates
//!
//! ## Prerequisites
//!
//! These tests require the TLS test infrastructure:
//!
//! 1. Generate certificates:
//!    ```bash
//!    ./tests/tls-test-infra/generate-certs.sh
//!    ```
//!
//! 2. Start TLS-enabled Kafka:
//!    ```bash
//!    docker-compose -f tests/tls-test-infra/docker-compose-tls.yml up -d
//!    ```
//!
//! 3. Run TLS integration tests:
//!    ```bash
//!    cargo test --test integration_suite_tests -- --ignored tls
//!    ```

use std::path::PathBuf;

use kafka_backup_core::config::{KafkaConfig, SecurityConfig, SecurityProtocol, TopicSelection};
use kafka_backup_core::kafka::KafkaClient;

/// Helper to get the path to test certificates.
fn get_certs_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("tests/tls-test-infra/certs")
}

/// Helper to check if test certificates exist.
fn certs_exist() -> bool {
    let certs_path = get_certs_path();
    certs_path.join("ca/ca.crt").exists()
        && certs_path.join("client/client.crt").exists()
        && certs_path.join("client/client.key").exists()
}

/// Test connecting to Kafka with a custom CA certificate.
///
/// This test verifies the fix for GitHub issue #3:
/// "Unable to connect to broker when using ssl configuration"
///
/// Prerequisites:
/// - Run ./tests/tls-test-infra/generate-certs.sh
/// - Run docker-compose -f tests/tls-test-infra/docker-compose-tls.yml up -d
#[tokio::test]
#[ignore = "requires Docker TLS Kafka - run docker-compose-tls.yml first"]
async fn test_kafka_connection_with_custom_ca() {
    if !certs_exist() {
        panic!("Test certificates not found. Run: ./tests/tls-test-infra/generate-certs.sh");
    }

    let certs_path = get_certs_path();

    let config = KafkaConfig {
        bootstrap_servers: vec!["localhost:9093".to_string()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::Ssl,
            ssl_ca_location: Some(certs_path.join("ca/ca.crt")),
            ssl_certificate_location: None,
            ssl_key_location: None,
            ..Default::default()
        },
        topics: TopicSelection::default(),
    };

    let client = KafkaClient::new(config);

    // This should succeed with the custom CA certificate
    // Before the fix, this would fail with "UnknownIssuer" error
    let result = client.connect().await;
    assert!(
        result.is_ok(),
        "Should connect with custom CA cert. Error: {:?}",
        result.err()
    );

    // Verify we can fetch metadata
    let metadata = client.fetch_metadata(None).await;
    assert!(
        metadata.is_ok(),
        "Should fetch metadata over TLS. Error: {:?}",
        metadata.err()
    );
}

/// Test connecting to Kafka with mTLS (client certificate authentication).
///
/// Prerequisites:
/// - Run ./tests/tls-test-infra/generate-certs.sh
/// - Run docker-compose -f tests/tls-test-infra/docker-compose-tls.yml up -d
#[tokio::test]
#[ignore = "requires Docker TLS Kafka with mTLS - run docker-compose-tls.yml first"]
async fn test_kafka_mtls_authentication() {
    if !certs_exist() {
        panic!("Test certificates not found. Run: ./tests/tls-test-infra/generate-certs.sh");
    }

    let certs_path = get_certs_path();

    let config = KafkaConfig {
        bootstrap_servers: vec!["localhost:9094".to_string()], // mTLS broker
        security: SecurityConfig {
            security_protocol: SecurityProtocol::Ssl,
            ssl_ca_location: Some(certs_path.join("ca/ca.crt")),
            ssl_certificate_location: Some(certs_path.join("client/client.crt")),
            ssl_key_location: Some(certs_path.join("client/client.key")),
            ..Default::default()
        },
        topics: TopicSelection::default(),
    };

    let client = KafkaClient::new(config);

    // This should succeed with mTLS
    let result = client.connect().await;
    assert!(
        result.is_ok(),
        "Should connect with mTLS. Error: {:?}",
        result.err()
    );

    // Verify we can fetch metadata
    let metadata = client.fetch_metadata(None).await;
    assert!(
        metadata.is_ok(),
        "Should fetch metadata over mTLS. Error: {:?}",
        metadata.err()
    );
}

/// Test that connection fails without CA cert when broker uses self-signed cert.
///
/// This test verifies the error behavior - without the custom CA,
/// the connection should fail with a certificate verification error.
#[tokio::test]
#[ignore = "requires Docker TLS Kafka - run docker-compose-tls.yml first"]
async fn test_connection_fails_without_custom_ca() {
    let config = KafkaConfig {
        bootstrap_servers: vec!["localhost:9093".to_string()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::Ssl,
            // No custom CA - should fail with UnknownIssuer
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ..Default::default()
        },
        topics: TopicSelection::default(),
    };

    let client = KafkaClient::new(config);

    // This should fail because the self-signed cert isn't trusted
    let result = client.connect().await;
    assert!(
        result.is_err(),
        "Should fail without custom CA for self-signed broker cert"
    );

    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("UnknownIssuer") || err.contains("certificate") || err.contains("TLS"),
        "Error should mention certificate issue: {}",
        err
    );
}

/// Test that mTLS broker rejects connections without client cert.
#[tokio::test]
#[ignore = "requires Docker TLS Kafka with mTLS - run docker-compose-tls.yml first"]
async fn test_mtls_broker_rejects_without_client_cert() {
    if !certs_exist() {
        panic!("Test certificates not found. Run: ./tests/tls-test-infra/generate-certs.sh");
    }

    let certs_path = get_certs_path();

    let config = KafkaConfig {
        bootstrap_servers: vec!["localhost:9094".to_string()], // mTLS broker
        security: SecurityConfig {
            security_protocol: SecurityProtocol::Ssl,
            ssl_ca_location: Some(certs_path.join("ca/ca.crt")),
            // No client cert - mTLS broker should reject
            ssl_certificate_location: None,
            ssl_key_location: None,
            ..Default::default()
        },
        topics: TopicSelection::default(),
    };

    let client = KafkaClient::new(config);

    // This should fail because mTLS broker requires client cert
    let result = client.connect().await;
    assert!(
        result.is_err(),
        "mTLS broker should reject connection without client cert"
    );
}
