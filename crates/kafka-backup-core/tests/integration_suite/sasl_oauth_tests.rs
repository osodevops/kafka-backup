//! `#[ignore]` E2E: OAUTHBEARER plugin against a real Kafka broker.
//!
//! Runs against the unsecured-JWS fixture at
//! `tests/sasl-oauth-test-infra/`. The fixture broker trusts any JWT
//! with `alg: none`, so we drive a static-token plugin over the wire
//! and prove the full dispatch path — handshake, auth, post-auth
//! metadata fetch — works against actual Kafka (not a mock).
//!
//! ## Prerequisites
//!
//! ```bash
//! cd tests/sasl-oauth-test-infra
//! docker-compose -f docker-compose-oauth.yml up -d
//! ```
//!
//! ## Running
//!
//! ```bash
//! cargo test -p kafka-backup-core \
//!     --test integration_suite_tests -- --ignored sasl_oauth_
//! ```

use std::sync::Arc;

use kafka_backup_core::config::{KafkaConfig, SecurityConfig, SecurityProtocol, TopicSelection};
use kafka_backup_core::kafka::KafkaClient;

use super::sasl_test_fixtures::{factory_for, StaticOAuthBearerPlugin};

const OAUTH_BOOTSTRAP: &str = "localhost:9097";

fn oauth_config(plugin: kafka_backup_core::kafka::SaslMechanismPluginHandle) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![OAUTH_BOOTSTRAP.to_string()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism_plugin_factory: Some(factory_for(plugin)),
            ..Default::default()
        },
        topics: TopicSelection::default(),
        connection: Default::default(),
    }
}

/// E2E: static-token OAUTHBEARER plugin against a real Kafka broker.
/// Exercises the full plugin dispatch path end-to-end.
#[tokio::test]
#[ignore = "requires Docker OAUTHBEARER Kafka - run docker-compose-oauth.yml first"]
async fn sasl_oauth_bearer_static_token_e2e() {
    let plugin = Arc::new(StaticOAuthBearerPlugin::for_test_user());
    let client = KafkaClient::new(oauth_config(plugin));

    let connect = client.connect().await;
    assert!(
        connect.is_ok(),
        "OAUTHBEARER connect should succeed. Error: {:?}",
        connect.err()
    );

    // Post-auth metadata fetch proves the authenticated session works
    // for ordinary Kafka RPCs, not just the handshake.
    let metadata = client.fetch_metadata(None).await;
    assert!(
        metadata.is_ok(),
        "OAUTHBEARER metadata fetch should succeed. Error: {:?}",
        metadata.err()
    );
}
