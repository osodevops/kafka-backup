//! `OauthBearerPlugin` — the OSS `SaslMechanismPlugin` for OAUTHBEARER, over
//! a shared [`TokenCache`] — and `OauthPluginFactory`, which hands the same
//! plugin (and so the same cache) to every connection of a cluster.
//!
//! Every handshake, initial or KIP-368 re-authentication, asks the cache for
//! a token with at least [`HANDSHAKE_MIN_TTL`] left (bounded by 80 % of the
//! token lifetime), so the session the broker grants is long enough for the
//! core client to re-authenticate in (it re-authenticates at 80 % of the
//! session, at least 30 s after the last handshake).
//!
//! When the broker rejects a token it sends an RFC 7628 error challenge
//! (`{"status":"invalid_token"}`); the plugin answers with the `0x01`
//! separator as the RFC requires, so the broker completes the exchange
//! with a proper authentication error, and an `invalid_token` status
//! invalidates the cached token.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use crate::kafka::{
    SaslAuthOutcome, SaslMechanismPlugin, SaslMechanismPluginFactory,
    SaslMechanismPluginFactoryHandle, SaslMechanismPluginHandle, SaslPluginError,
};
use async_trait::async_trait;
use tracing::warn;

use crate::oauth::cache::{CacheSettings, TokenCache, HANDSHAKE_MIN_TTL};
use crate::oauth::framing;
use crate::oauth::token::{OauthProviderError, OauthTokenProvider};

const MECHANISM: &str = "OAUTHBEARER";

/// What to check when a broker refuses a token the identity provider
/// issued: the token itself is fine, the cluster does not accept it.
pub const REJECTION_HINT: &str =
    "The identity provider issued a token but the cluster refused it. \
     Likely causes: the token's issuer is not the one the cluster trusts (Microsoft Entra ID \
     issues v1 tokens unless the app registration sets requestedAccessTokenVersion: 2); its \
     audience is not the one the cluster expects (check scope / audience); or no identity pool \
     matches it (check security.oauth.extensions.identityPoolId and logicalCluster, and the \
     pool's filter)";

/// A token provider failure, named, as the source of a SASL plugin error.
#[derive(Debug, thiserror::Error)]
#[error("OAuth token provider '{provider}' ({kind}): {source}")]
pub struct ProviderFailure {
    pub provider: String,
    pub kind: &'static str,
    #[source]
    pub source: OauthProviderError,
}

/// OAUTHBEARER over a shared [`TokenCache`].
pub struct OauthBearerPlugin {
    cache: TokenCache,
    /// RFC 7628 SASL extensions sent with every token (`logicalCluster`,
    /// `identityPoolId`, ...). Provider-supplied extensions (a
    /// `User-Agent`, say) are layered on top and win on conflict.
    extensions: BTreeMap<String, String>,
    min_ttl: Duration,
}

impl std::fmt::Debug for OauthBearerPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OauthBearerPlugin")
            .field("cache", &self.cache)
            .field("extensions", &self.extensions)
            .finish()
    }
}

impl OauthBearerPlugin {
    /// A plugin with its own cache around `provider` and no extensions.
    pub fn new(provider: Arc<dyn OauthTokenProvider>) -> Self {
        Self::with_extensions(provider, BTreeMap::new())
    }

    /// A plugin with its own cache around `provider`.
    pub fn with_extensions(
        provider: Arc<dyn OauthTokenProvider>,
        extensions: BTreeMap<String, String>,
    ) -> Self {
        let cache = TokenCache::new(provider.kind(), provider, CacheSettings::default());
        Self::from_cache(cache, extensions)
    }

    /// A plugin over an existing (shared) cache.
    pub fn from_cache(cache: TokenCache, extensions: BTreeMap<String, String>) -> Self {
        Self {
            cache,
            extensions,
            min_ttl: HANDSHAKE_MIN_TTL,
        }
    }

    /// Override the minimum token TTL requested for handshakes.
    pub fn with_min_ttl(mut self, min_ttl: Duration) -> Self {
        self.min_ttl = min_ttl;
        self
    }

    pub fn cache(&self) -> &TokenCache {
        &self.cache
    }

    pub fn extensions(&self) -> &BTreeMap<String, String> {
        &self.extensions
    }

    async fn build_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        let token = self
            .cache
            .get_with_min_ttl(self.min_ttl)
            .await
            .map_err(|source| SaslPluginError::PayloadFailed {
                mechanism: MECHANISM.to_string(),
                source: Box::new(ProviderFailure {
                    provider: self.cache.name().to_string(),
                    kind: self.cache.provider_kind(),
                    source,
                }),
            })?;
        let mut merged = self.extensions.clone();
        for (k, v) in &token.extensions {
            merged.insert(k.clone(), v.clone());
        }
        Ok(framing::build_client_initial_response(
            token.value(),
            &merged,
        ))
    }
}

#[async_trait]
impl SaslMechanismPlugin for OauthBearerPlugin {
    fn mechanism_name(&self) -> &str {
        MECHANISM
    }

    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        self.build_payload().await
    }

    async fn continue_payload(
        &self,
        server_response: &[u8],
    ) -> Result<SaslAuthOutcome, SaslPluginError> {
        if server_response.is_empty() {
            return Ok(SaslAuthOutcome::Done);
        }
        // A non-empty server message after our token is an RFC 7628 error
        // challenge. Reply with the separator so the broker finishes with a
        // proper authentication error (which `interpret_server_error` maps).
        match framing::parse_server_error(server_response) {
            Some(error) => {
                if error.status == "invalid_token" {
                    self.cache.invalidate();
                }
                warn!(
                    provider = %self.cache.name(),
                    status = %error.status,
                    scope = error.scope.as_deref().unwrap_or(""),
                    "Kafka broker rejected the OAuth token"
                );
            }
            None => warn!(
                provider = %self.cache.name(),
                "Kafka broker sent an unexpected OAUTHBEARER server message; ending the exchange"
            ),
        }
        Ok(SaslAuthOutcome::Continue(vec![framing::KVSEP]))
    }

    fn interpret_server_error(&self, error_bytes: &[u8]) -> SaslPluginError {
        let detail = match framing::parse_server_error(error_bytes) {
            Some(e) => {
                if e.status == "invalid_token" {
                    self.cache.invalidate();
                }
                format!(
                    "{} (scope={})",
                    e.status,
                    e.scope.as_deref().unwrap_or("<none>")
                )
            }
            None => String::from_utf8_lossy(error_bytes).into_owned(),
        };
        SaslPluginError::ServerRejected {
            mechanism: MECHANISM.to_string(),
            detail: format!(
                "{detail} [OAuth provider '{}'. {REJECTION_HINT}]",
                self.cache.name()
            ),
        }
    }

    async fn reauth_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        self.build_payload().await
    }
}

/// Hands one shared [`OauthBearerPlugin`] (and so one token cache) to every
/// connection of a cluster: the bootstrap client, every pooled per-broker
/// client, the group coordinators.
#[derive(Debug, Clone)]
pub struct OauthPluginFactory {
    plugin: Arc<OauthBearerPlugin>,
}

impl OauthPluginFactory {
    pub fn new(plugin: OauthBearerPlugin) -> Self {
        Self {
            plugin: Arc::new(plugin),
        }
    }

    /// For `SecurityConfig::sasl_mechanism_plugin_factory`.
    pub fn into_handle(self) -> SaslMechanismPluginFactoryHandle {
        Arc::new(self)
    }

    pub fn plugin(&self) -> &OauthBearerPlugin {
        &self.plugin
    }
}

impl SaslMechanismPluginFactory for OauthPluginFactory {
    fn mechanism_name(&self) -> &str {
        MECHANISM
    }

    fn build(
        &self,
        _broker_host: &str,
        _broker_port: u16,
    ) -> Result<SaslMechanismPluginHandle, SaslPluginError> {
        Ok(self.plugin.clone() as SaslMechanismPluginHandle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::oauth::token::OauthToken;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Debug, Default)]
    struct RecordingProvider {
        calls: AtomicUsize,
    }

    #[async_trait]
    impl OauthTokenProvider for RecordingProvider {
        async fn get_token(&self) -> Result<OauthToken, OauthProviderError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(OauthToken::new(
                format!("test-token-{n}"),
                Some(std::time::SystemTime::now() + Duration::from_secs(3600)),
                None,
                std::collections::HashMap::from([(
                    "User-Agent".to_string(),
                    "unit-test".to_string(),
                )]),
            ))
        }

        fn kind(&self) -> &'static str {
            "recording"
        }
    }

    #[tokio::test]
    async fn initial_payload_calls_provider_once() {
        let provider = Arc::new(RecordingProvider::default());
        let plugin = OauthBearerPlugin::new(provider.clone());

        let payload = plugin.initial_payload().await.expect("initial payload");
        let rendered = String::from_utf8(payload).expect("utf8");
        assert!(rendered.contains("auth=Bearer test-token-1"));
        assert!(rendered.contains("User-Agent=unit-test"));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn reauth_reuses_a_cached_token_with_enough_lifetime() {
        let provider = Arc::new(RecordingProvider::default());
        let plugin = OauthBearerPlugin::new(provider.clone());
        let _ = plugin.initial_payload().await.expect("initial");
        let reauth = plugin.reauth_payload().await.expect("reauth");
        assert!(String::from_utf8(reauth).unwrap().contains("test-token-1"));
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn static_and_provider_extensions_are_merged() {
        let provider = Arc::new(RecordingProvider::default());
        let mut statics = BTreeMap::new();
        statics.insert("logicalCluster".to_string(), "lkc-abc123".to_string());
        let plugin = OauthBearerPlugin::with_extensions(provider, statics);

        let payload = plugin.initial_payload().await.expect("initial payload");
        let rendered = String::from_utf8(payload).expect("utf8");
        assert!(rendered.contains("logicalCluster=lkc-abc123"));
        assert!(rendered.contains("User-Agent=unit-test"));
    }

    #[tokio::test]
    async fn error_challenge_gets_the_separator_and_success_is_done() {
        let plugin = OauthBearerPlugin::new(Arc::new(RecordingProvider::default()));
        assert_eq!(
            plugin.continue_payload(b"").await.unwrap(),
            SaslAuthOutcome::Done
        );
        assert_eq!(
            plugin
                .continue_payload(br#"{"status":"invalid_token"}"#)
                .await
                .unwrap(),
            SaslAuthOutcome::Continue(vec![0x01])
        );
    }

    #[test]
    fn server_errors_name_the_provider() {
        let plugin = OauthBearerPlugin::new(Arc::new(RecordingProvider::default()));
        let err = plugin.interpret_server_error(
            b"Authentication failed during authentication due to invalid credentials",
        );
        let text = err.to_string();
        assert!(text.contains("invalid credentials"), "{text}");
        assert!(text.contains("OAuth provider 'recording'"), "{text}");
        for cause in [
            "issuer",
            "requestedAccessTokenVersion",
            "audience",
            "identity pool",
        ] {
            assert!(text.contains(cause), "names {cause}: {text}");
        }
    }

    #[tokio::test]
    async fn factory_shares_one_plugin_and_cache() {
        let provider = Arc::new(RecordingProvider::default());
        let factory = OauthPluginFactory::new(OauthBearerPlugin::new(provider.clone()));
        let a = factory.build("b0", 9092).unwrap();
        let b = factory.build("b1", 9093).unwrap();
        a.initial_payload().await.unwrap();
        b.initial_payload().await.unwrap();
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(factory.mechanism_name(), "OAUTHBEARER");
    }

    #[test]
    fn mechanism_name_is_oauthbearer() {
        let plugin = OauthBearerPlugin::new(Arc::new(RecordingProvider::default()));
        assert_eq!(plugin.mechanism_name(), "OAUTHBEARER");
    }
}
