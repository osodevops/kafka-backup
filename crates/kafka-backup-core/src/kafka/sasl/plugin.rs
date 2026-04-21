//! The [`SaslMechanismPlugin`] extension trait.
//!
//! Plugins own the authentication state for a single handshake. The Kafka
//! client drives the wire protocol (`SaslHandshake` + repeated
//! `SaslAuthenticate` RPCs); the plugin computes the bytes that go into
//! each `SaslAuthenticate` request and reads the bytes the broker sends
//! back. Plugins are free to maintain multi-round state across calls to
//! [`SaslMechanismPlugin::continue_payload`].
//!
//! ## Multi-round authentication
//!
//! `PLAIN` is single-round: [`initial_payload`] returns the full credential
//! blob, broker responds with success, done. `OAUTHBEARER` is single-round
//! in the happy path but can be two-round on failure (broker replies with
//! an error JSON body, client may send `kvsep+kvsep` to close). `SCRAM` is
//! three-round.
//!
//! The client loop looks like:
//!
//! ```text
//! send(SaslHandshake{ mechanism: plugin.mechanism_name() })
//! payload = plugin.initial_payload()
//! loop {
//!     resp = send(SaslAuthenticate{ auth_bytes: payload })
//!     if resp.error_code != 0 { return plugin.interpret_server_error(resp.error_message) }
//!     match plugin.continue_payload(resp.auth_bytes) {
//!         Some(next) => payload = next,
//!         None => break,   // handshake complete
//!     }
//! }
//! ```
//!
//! ## Re-authentication (KIP-368)
//!
//! If the broker advertises a non-zero `session_lifetime_ms`, the client
//! schedules a mid-session re-auth before expiry. [`reauth_payload`]
//! builds the initial bytes for the new handshake; by default it
//! delegates to [`initial_payload`]. Tokens with refresh semantics
//! (`OAUTHBEARER`) override it.
//!
//! ## Threading and re-entrancy
//!
//! The client holds its connection mutex for the duration of a handshake.
//! Plugins **must not** call back into the same [`crate::kafka::KafkaClient`]
//! during [`initial_payload`], [`continue_payload`], or [`reauth_payload`] —
//! doing so deadlocks on the connection mutex.

use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

/// Errors a plugin can surface during a SASL handshake or re-authentication.
#[derive(Debug, Error)]
pub enum SaslPluginError {
    /// The plugin could not produce an authentication payload — typically
    /// a token fetch failed, credentials are missing, or a signing step
    /// errored. The connection is closed; the caller decides whether to
    /// retry the connect.
    #[error("SASL plugin ({mechanism}) failed to produce payload: {source}")]
    PayloadFailed {
        mechanism: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },

    /// The broker accepted the connection-layer SASL RPC but rejected the
    /// credentials. `detail` is either the structured RFC 7628 status
    /// (when the broker returns the JSON body prescribed by the spec) or
    /// the broker's free-form `error_message` string (Kafka 3.5+ reality).
    #[error("SASL {mechanism} rejected by broker: {detail}")]
    ServerRejected { mechanism: String, detail: String },

    /// The plugin received a server response it could not parse. Usually
    /// indicates a protocol mismatch or a corrupt token.
    #[error("SASL plugin ({mechanism}) invalid server response: {detail}")]
    InvalidServerResponse { mechanism: String, detail: String },
}

/// Outcome of a single `SaslAuthenticate` round, as seen by the plugin.
///
/// Returned by [`SaslMechanismPlugin::continue_payload`] to tell the
/// client loop whether another round is needed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaslAuthOutcome {
    /// Send the enclosed bytes as the next `SaslAuthenticate.auth_bytes`.
    Continue(Vec<u8>),
    /// Handshake complete; the client should stop sending `SaslAuthenticate`
    /// and proceed with normal Kafka traffic.
    Done,
}

/// Shared handle type used when wiring a plugin into `SecurityConfig`.
///
/// `Arc<dyn>` (not `Box<dyn>`) so multiple `KafkaClient` instances — e.g.
/// the bootstrap client and the per-broker clients — can share a single
/// plugin and, by extension, a single token cache.
pub type SaslMechanismPluginHandle = Arc<dyn SaslMechanismPlugin>;

/// The pluggable SASL mechanism trait.
///
/// Implementors live in downstream crates (notably
/// `kafka-backup-enterprise-core` for `OAUTHBEARER` / MSK IAM). See the
/// module-level docs for the handshake loop the client uses.
#[async_trait]
pub trait SaslMechanismPlugin: Send + Sync + std::fmt::Debug {
    /// The SASL mechanism name as advertised in the `SaslHandshake`
    /// request (e.g. `"OAUTHBEARER"`, `"SCRAM-SHA-256"`). Must be one of
    /// the mechanisms the broker advertises — mismatches surface as a
    /// broker-side error on the handshake.
    fn mechanism_name(&self) -> &str;

    /// The bytes to send as the first `SaslAuthenticate.auth_bytes` frame,
    /// immediately after a successful `SaslHandshake`. For single-round
    /// mechanisms (`PLAIN`, happy-path `OAUTHBEARER`) this is the full
    /// credential payload.
    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError>;

    /// Given the server's response bytes to the most recent
    /// `SaslAuthenticate` round, return the next payload to send — or
    /// [`SaslAuthOutcome::Done`] if no more rounds are needed.
    ///
    /// Default: single-round. Override for multi-round mechanisms.
    #[allow(unused_variables)]
    async fn continue_payload(
        &self,
        server_response: &[u8],
    ) -> Result<SaslAuthOutcome, SaslPluginError> {
        Ok(SaslAuthOutcome::Done)
    }

    /// Translate a broker-side auth failure into a structured error.
    ///
    /// The default handler tolerates both the RFC 7628 JSON error body
    /// (`{"status":"...","scope":"..."}`) and the free-form
    /// `error_message` string that Kafka 3.5+ brokers actually return.
    /// Override if your mechanism has a richer error format.
    fn interpret_server_error(&self, error_bytes: &[u8]) -> SaslPluginError {
        if let Ok(parsed) = serde_json::from_slice::<Rfc7628Error>(error_bytes) {
            return SaslPluginError::ServerRejected {
                mechanism: self.mechanism_name().to_string(),
                detail: format!(
                    "{} (scope={})",
                    parsed.status,
                    parsed.scope.as_deref().unwrap_or("<none>")
                ),
            };
        }
        SaslPluginError::ServerRejected {
            mechanism: self.mechanism_name().to_string(),
            detail: String::from_utf8_lossy(error_bytes).into_owned(),
        }
    }

    /// The bytes for the first `SaslAuthenticate` of a KIP-368 re-auth
    /// handshake. Defaults to [`initial_payload`] — mechanisms with
    /// refreshable tokens (`OAUTHBEARER`) override this to return a fresh
    /// token.
    async fn reauth_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        self.initial_payload().await
    }
}

#[derive(serde::Deserialize)]
struct Rfc7628Error {
    status: String,
    #[serde(default)]
    scope: Option<String>,
    #[allow(dead_code)]
    #[serde(default, rename = "openid-configuration")]
    openid_configuration: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct StubPlugin;

    #[async_trait]
    impl SaslMechanismPlugin for StubPlugin {
        fn mechanism_name(&self) -> &str {
            "OAUTHBEARER"
        }

        async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
            Ok(b"initial".to_vec())
        }
    }

    #[tokio::test]
    async fn default_continue_payload_is_done() {
        let plugin = StubPlugin;
        let outcome = plugin.continue_payload(b"anything").await.unwrap();
        assert_eq!(outcome, SaslAuthOutcome::Done);
    }

    #[tokio::test]
    async fn default_reauth_delegates_to_initial() {
        let plugin = StubPlugin;
        let initial = plugin.initial_payload().await.unwrap();
        let reauth = plugin.reauth_payload().await.unwrap();
        assert_eq!(initial, reauth);
    }

    #[test]
    fn interpret_server_error_parses_rfc7628_json() {
        let plugin = StubPlugin;
        let bytes = br#"{"status":"invalid_token","scope":"kafka-cluster"}"#;
        let err = plugin.interpret_server_error(bytes);
        match err {
            SaslPluginError::ServerRejected { mechanism, detail } => {
                assert_eq!(mechanism, "OAUTHBEARER");
                assert!(detail.contains("invalid_token"));
                assert!(detail.contains("kafka-cluster"));
            }
            other => panic!("expected ServerRejected, got {other:?}"),
        }
    }

    #[test]
    fn interpret_server_error_falls_back_to_utf8_on_nonjson() {
        let plugin = StubPlugin;
        let bytes = b"Authentication failed: bad token (not JSON at all)";
        let err = plugin.interpret_server_error(bytes);
        match err {
            SaslPluginError::ServerRejected { detail, .. } => {
                assert!(detail.contains("Authentication failed"));
            }
            other => panic!("expected ServerRejected, got {other:?}"),
        }
    }

    #[test]
    fn interpret_server_error_handles_empty_payload() {
        let plugin = StubPlugin;
        let err = plugin.interpret_server_error(&[]);
        match err {
            SaslPluginError::ServerRejected { detail, .. } => {
                assert_eq!(detail, "");
            }
            other => panic!("expected ServerRejected, got {other:?}"),
        }
    }

    #[test]
    fn interpret_server_error_ignores_json_without_status() {
        let plugin = StubPlugin;
        // Looks like JSON but missing the required `status` field — must
        // fall through to UTF-8 fallback rather than panic or misparse.
        let bytes = br#"{"scope":"kafka"}"#;
        let err = plugin.interpret_server_error(bytes);
        match err {
            SaslPluginError::ServerRejected { detail, .. } => {
                // Either the raw JSON or a parse-fallback — but a non-empty string.
                assert!(detail.contains("scope") || detail.contains("kafka"));
            }
            other => panic!("expected ServerRejected, got {other:?}"),
        }
    }
}
