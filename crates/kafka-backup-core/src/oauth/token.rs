//! `OauthToken` value type and `OauthTokenProvider` trait.
//!
//! `OauthToken` wraps a bearer-token string in [`Zeroizing`] so the memory is
//! wiped on drop, and has a custom `Debug` impl that redacts the value — the
//! goal is that the token never surfaces in logs or panic backtraces, even
//! when a caller writes `{:?}` on the containing struct.
//!
//! `OauthTokenProvider` is the trait that token backends implement. Any of
//! them plug into [`crate::oauth::plugin::OauthBearerPlugin`] and
//! from there into the OSS `SaslMechanismPlugin` extension point.

use std::collections::HashMap;
use std::time::SystemTime;

use async_trait::async_trait;
use thiserror::Error;
use zeroize::Zeroizing;

/// A short-lived bearer token plus the metadata needed to decide when to
/// refresh it.
pub struct OauthToken {
    value: Zeroizing<String>,
    /// When the token becomes invalid. `None` means the provider does not
    /// know — callers must refresh on a fixed cadence.
    pub expires_at: Option<SystemTime>,
    /// Scope granted to the token by the identity provider, if any.
    pub scope: Option<String>,
    /// Extra key-value pairs to include as RFC 7628 SASL extensions in the
    /// OAUTHBEARER client initial response.
    pub extensions: HashMap<String, String>,
}

impl OauthToken {
    /// Construct a new token. `value` is taken by-value and will be zeroized
    /// when the token is dropped.
    pub fn new(
        value: impl Into<String>,
        expires_at: Option<SystemTime>,
        scope: Option<String>,
        extensions: HashMap<String, String>,
    ) -> Self {
        Self {
            value: Zeroizing::new(value.into()),
            expires_at,
            scope,
            extensions,
        }
    }

    /// Borrow the bearer-token string. Callers should avoid logging this.
    pub fn value(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Debug for OauthToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OauthToken")
            .field("value", &"***REDACTED***")
            .field("expires_at", &self.expires_at)
            .field("scope", &self.scope)
            .field("extensions", &self.extensions)
            .finish()
    }
}

/// Errors returned by a token provider.
#[derive(Debug, Error)]
pub enum OauthProviderError {
    /// The provider produced no usable token (e.g. the token file was empty).
    #[error("token provider returned an invalid token: {0}")]
    InvalidToken(String),

    /// I/O error reading a token source (file, network, etc.).
    #[error("token provider I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// The HTTP request to the identity provider failed before a response
    /// arrived (connect, TLS, timeout), after the transport's retries.
    #[error("token request failed: {0}")]
    Http(String),

    /// The identity provider answered with an error. `error` is the RFC 6749
    /// error code where the body had one (`invalid_client`,
    /// `unauthorized_client`, ...). Never contains a token or secret.
    #[error("token endpoint returned {}{}{}", status.map(|s| format!("HTTP {s} ")).unwrap_or_default(), error, description.as_ref().map(|d| format!(": {d}")).unwrap_or_default())]
    Endpoint {
        status: Option<u16>,
        error: String,
        description: Option<String>,
    },

    /// The identity provider answered 200 but the body was not a usable
    /// token response. The body is never included (it may hold a token).
    #[error("token endpoint returned an unusable response: {0}")]
    InvalidResponse(String),

    /// The provider is misconfigured (missing setting, unreadable file).
    #[error("token provider configuration error: {0}")]
    Config(String),

    /// A recent attempt failed and the next one is rate-limited; carries
    /// that attempt's error.
    #[error("{0} (the last attempt failed less than a second ago; retrying shortly)")]
    RecentFailure(String),
}

/// Provides `OauthToken`s to the `OauthBearerPlugin`.
///
/// Implementors return a fresh token on every call. Caching (single-flight
/// refresh, proactive renewal, stale-but-valid serving on errors) lives in
/// [`crate::oauth::cache::TokenCache`], which every consumer
/// (Kafka connections, Schema Registry) goes through.
#[async_trait]
pub trait OauthTokenProvider: Send + Sync + std::fmt::Debug {
    /// Produce a token suitable for the next OAUTHBEARER handshake. Callers
    /// must not assume idempotence: a provider may return a different
    /// token on every call.
    async fn get_token(&self) -> Result<OauthToken, OauthProviderError>;

    /// A short, stable identifier used in logs and metrics. Examples:
    /// `"oidc_client_credentials"`, `"file"`, `"static"`.
    fn kind(&self) -> &'static str;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_value() {
        let token = OauthToken::new(
            "super-secret-jwt",
            Some(SystemTime::UNIX_EPOCH),
            Some("kafka-cluster".to_string()),
            HashMap::new(),
        );
        let rendered = format!("{token:?}");
        assert!(
            !rendered.contains("super-secret-jwt"),
            "value leaked into Debug output: {rendered}"
        );
        assert!(rendered.contains("REDACTED"));
    }

    #[test]
    fn value_accessor_returns_original() {
        let token = OauthToken::new("abc", None, None, HashMap::new());
        assert_eq!(token.value(), "abc");
    }
}
