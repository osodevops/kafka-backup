//! Shared SASL plugin test fixtures.
//!
//! Lives under `integration_suite` so it can be reused by both the
//! in-process mock-broker tests (`sasl_plugin_mock_tests.rs`) and the
//! `#[ignore]` docker-based OAUTHBEARER E2E (`sasl_oauth_tests.rs`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use kafka_backup_core::kafka::{
    SaslAuthOutcome, SaslMechanismPlugin, SaslMechanismPluginFactoryHandle,
    SaslMechanismPluginHandle, SaslPluginError, SharedPluginFactory,
};

/// Wrap a plugin Arc in a [`SharedPluginFactory`] for direct assignment
/// to `SecurityConfig::sasl_mechanism_plugin_factory`. Use this from
/// the mock-broker and OAUTHBEARER tests — their plugins are either
/// stateless or share state intentionally across connections.
///
/// GSSAPI tests do NOT use this helper — they want a fresh
/// `GssapiPlugin` per connection via `GssapiPluginFactory`.
pub fn factory_for(plugin: SaslMechanismPluginHandle) -> SaslMechanismPluginFactoryHandle {
    SharedPluginFactory::new(plugin).into_handle()
}

/// Static-token OAUTHBEARER plugin for E2E against an unsecured-JWS
/// Apache Kafka broker.
///
/// Builds an RFC 7628 `n,a=<principal>,\x01auth=Bearer <token>\x01\x01`
/// payload using a hardcoded unsecured JWT (`alg: none`). The token's
/// `sub` claim is `test-user` to match the broker's
/// `unsecuredLoginStringClaim_sub` JAAS setting.
#[derive(Debug, Clone)]
pub struct StaticOAuthBearerPlugin {
    principal: String,
    token: String,
}

impl StaticOAuthBearerPlugin {
    /// Construct with the default `test-user` principal — matches the
    /// docker-compose fixture at `tests/sasl-oauth-test-infra/`.
    pub fn for_test_user() -> Self {
        // Header: {"alg":"none"} -> base64url without padding
        let header_b64 = "eyJhbGciOiJub25lIn0";
        // Payload: {"sub":"test-user","exp":9999999999,"iat":1000}
        let payload_b64 = "eyJzdWIiOiJ0ZXN0LXVzZXIiLCJleHAiOjk5OTk5OTk5OTksImlhdCI6MTAwMH0";
        Self {
            principal: "test-user".to_string(),
            token: format!("{}.{}.", header_b64, payload_b64),
        }
    }

    /// Count of `initial_payload` calls — for asserting reauth fires.
    /// Cloned handles share the same counter (Arc semantics).
    pub fn into_handle_with_counter(self) -> (SaslMechanismPluginHandle, Arc<AtomicUsize>) {
        let counter = Arc::new(AtomicUsize::new(0));
        let plugin = CountingStaticOAuthBearerPlugin {
            inner: self,
            initial_calls: counter.clone(),
        };
        (Arc::new(plugin), counter)
    }
}

#[async_trait]
impl SaslMechanismPlugin for StaticOAuthBearerPlugin {
    fn mechanism_name(&self) -> &str {
        "OAUTHBEARER"
    }

    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        Ok(build_oauthbearer_payload(&self.principal, &self.token))
    }
}

#[derive(Debug)]
struct CountingStaticOAuthBearerPlugin {
    inner: StaticOAuthBearerPlugin,
    initial_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl SaslMechanismPlugin for CountingStaticOAuthBearerPlugin {
    fn mechanism_name(&self) -> &str {
        self.inner.mechanism_name()
    }

    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        self.initial_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.initial_payload().await
    }

    async fn reauth_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        // Count reauths the same as initial for simplicity — the test
        // asserts on total plugin invocations.
        self.initial_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.initial_payload().await
    }
}

/// Deterministic multi-round plugin: initial returns `b"round-1"`, then
/// on server bytes `b"challenge-1"` returns Continue(`b"round-2"`), then
/// on `b"challenge-2"` returns Done. Used to exercise the dispatch
/// loop's multi-round path.
#[derive(Debug, Default)]
pub struct TwoRoundPlugin;

#[async_trait]
impl SaslMechanismPlugin for TwoRoundPlugin {
    fn mechanism_name(&self) -> &str {
        "TEST-TWO-ROUND"
    }

    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        Ok(b"round-1".to_vec())
    }

    async fn continue_payload(
        &self,
        server_response: &[u8],
    ) -> Result<SaslAuthOutcome, SaslPluginError> {
        match server_response {
            b"challenge-1" => Ok(SaslAuthOutcome::Continue(b"round-2".to_vec())),
            b"challenge-2" => Ok(SaslAuthOutcome::Done),
            other => Err(SaslPluginError::InvalidServerResponse {
                mechanism: "TEST-TWO-ROUND".to_string(),
                detail: format!("unexpected challenge: {:?}", other),
            }),
        }
    }
}

/// Plugin that opts out of KIP-368 live re-authentication via
/// `supports_reauth() = false`. Counts calls to `initial_payload` and
/// `reauth_payload` so a test can prove the scheduler never fired.
///
/// Mirrors `GssapiPlugin`'s opt-out contract without needing the gssapi
/// feature flag enabled.
#[derive(Debug, Default)]
pub struct NoReauthCountingPlugin {
    pub initial_calls: Arc<AtomicUsize>,
    pub reauth_calls: Arc<AtomicUsize>,
}

impl NoReauthCountingPlugin {
    /// Construct a fresh plugin and return it wrapped as a handle
    /// alongside shared counters for `initial_payload` and
    /// `reauth_payload` invocations.
    pub fn handle_with_counters() -> (
        SaslMechanismPluginHandle,
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
    ) {
        let plugin = NoReauthCountingPlugin::default();
        let initial = plugin.initial_calls.clone();
        let reauth = plugin.reauth_calls.clone();
        (Arc::new(plugin), initial, reauth)
    }
}

#[async_trait]
impl SaslMechanismPlugin for NoReauthCountingPlugin {
    fn mechanism_name(&self) -> &str {
        "TEST-NO-REAUTH"
    }

    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        self.initial_calls.fetch_add(1, Ordering::SeqCst);
        Ok(b"initial".to_vec())
    }

    fn supports_reauth(&self) -> bool {
        false
    }

    async fn reauth_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        self.reauth_calls.fetch_add(1, Ordering::SeqCst);
        Ok(b"reauth".to_vec())
    }
}

/// Plugin that records the bytes passed to `interpret_server_error` so
/// the test can assert the dispatch layer routed the broker error body
/// correctly.
#[derive(Debug, Default)]
pub struct RecordingErrorPlugin {
    pub recorded: Arc<std::sync::Mutex<Option<Vec<u8>>>>,
}

#[async_trait]
impl SaslMechanismPlugin for RecordingErrorPlugin {
    fn mechanism_name(&self) -> &str {
        "TEST-RECORDING"
    }

    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        Ok(b"initial".to_vec())
    }

    fn interpret_server_error(&self, error_bytes: &[u8]) -> SaslPluginError {
        *self.recorded.lock().unwrap() = Some(error_bytes.to_vec());
        SaslPluginError::ServerRejected {
            mechanism: "TEST-RECORDING".to_string(),
            detail: String::from_utf8_lossy(error_bytes).into_owned(),
        }
    }
}

fn build_oauthbearer_payload(principal: &str, token: &str) -> Vec<u8> {
    let mut buf = Vec::new();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oauthbearer_payload_follows_rfc7628() {
        let payload = build_oauthbearer_payload("alice", "tkn");
        let s = std::str::from_utf8(&payload).unwrap();
        assert_eq!(s, "n,a=alice,\x01auth=Bearer tkn\x01\x01");
    }

    #[tokio::test]
    async fn static_plugin_token_has_three_jwt_parts() {
        let p = StaticOAuthBearerPlugin::for_test_user();
        let payload = p.initial_payload().await.unwrap();
        let s = std::str::from_utf8(&payload).unwrap();
        // Extract token between "Bearer " and first 0x01
        let start = s.find("Bearer ").unwrap() + "Bearer ".len();
        let end = s[start..].find('\x01').unwrap() + start;
        let token = &s[start..end];
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT must have 3 parts, got {:?}", parts);
        assert!(!parts[0].is_empty(), "header present");
        assert!(!parts[1].is_empty(), "payload present");
        assert!(parts[2].is_empty(), "unsecured signature empty");
    }
}
