//! Minimal `SaslMechanismPlugin` reference: a static-token `OAUTHBEARER` plugin.
//!
//! This example shows the smallest useful implementation of the
//! [`SaslMechanismPlugin`] extension trait: hand a static JWT (or any opaque
//! bearer token) to the `KafkaClient`, and the handshake completes against
//! any broker configured to accept that token.
//!
//! Real-world plugins typically wrap a token-refresh cache — an OIDC client,
//! an AWS MSK IAM presigner, a Keycloak adapter, etc. The canonical
//! production implementations live in `kafka-backup-enterprise-core` and
//! cover MSK IAM (`aws-sigv4`), Confluent Cloud, and OIDC.
//!
//! Run it with:
//!
//! ```bash
//! cargo build --example custom_sasl_plugin -p kafka-backup-core
//! ```
//!
//! The example does not actually connect to a broker — it just proves the
//! wiring compiles against the public trait surface.

use std::sync::Arc;

use async_trait::async_trait;
use kafka_backup_core::config::{KafkaConfig, SecurityConfig, SecurityProtocol, TopicSelection};
use kafka_backup_core::kafka::{
    KafkaClient, SaslMechanismPlugin, SaslMechanismPluginHandle, SaslPluginError,
};

/// A static-token OAUTHBEARER plugin.
///
/// Constructs an RFC 7628 client-initial-response payload from a fixed
/// principal and bearer token. Suitable for local testing against brokers
/// running the Apache Kafka `OAuthBearerUnsecuredValidatorCallbackHandler`
/// (unsecured-JWS mode) or any broker that accepts static tokens.
#[derive(Debug)]
struct StaticTokenOauthBearer {
    principal: String,
    token: String,
}

impl StaticTokenOauthBearer {
    fn new(principal: impl Into<String>, token: impl Into<String>) -> Self {
        Self {
            principal: principal.into(),
            token: token.into(),
        }
    }

    fn into_handle(self) -> SaslMechanismPluginHandle {
        Arc::new(self)
    }
}

#[async_trait]
impl SaslMechanismPlugin for StaticTokenOauthBearer {
    fn mechanism_name(&self) -> &str {
        "OAUTHBEARER"
    }

    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        Ok(build_rfc7628_cir(&self.principal, &self.token))
    }
}

/// Build an RFC 7628 §3.1 client initial response:
/// `n,a=<authzid>,<0x01>auth=Bearer <token><0x01><0x01>`.
fn build_rfc7628_cir(principal: &str, token: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(principal.len() + token.len() + 16);
    buf.extend_from_slice(b"n,a=");
    buf.extend_from_slice(principal.as_bytes());
    buf.push(b',');
    buf.push(0x01);
    buf.extend_from_slice(b"auth=Bearer ");
    buf.extend_from_slice(token.as_bytes());
    buf.push(0x01);
    buf.push(0x01);
    buf
}

fn main() {
    // An unsecured JWT (`alg: none`) for local testing. Production tokens
    // come from an IdP — Okta, Keycloak, AWS STS, etc.
    let unsecured_jwt = concat!(
        "eyJhbGciOiJub25lIn0",
        ".",
        "eyJzdWIiOiJ0ZXN0LXVzZXIiLCJleHAiOjk5OTk5OTk5OTksImlhdCI6MTAwMH0",
        ".",
    );

    let plugin = StaticTokenOauthBearer::new("test-user", unsecured_jwt).into_handle();

    let config = KafkaConfig {
        bootstrap_servers: vec!["localhost:9097".to_string()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism_plugin: Some(plugin),
            ..Default::default()
        },
        topics: TopicSelection::default(),
        connection: Default::default(),
    };

    let _client = KafkaClient::new(config);
    println!(
        "OAUTHBEARER plugin wired into KafkaClient. \
         Connect would go to localhost:9097 — skipped (no broker in this example)."
    );
}
