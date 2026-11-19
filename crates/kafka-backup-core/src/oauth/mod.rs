//! OAuth 2.0 / OIDC authentication for Kafka (SASL/OAUTHBEARER, RFC 7628),
//! and the bearer-token plumbing a Schema Registry client needs.
//!
//! ## Configuration
//!
//! Named token providers live in the top-level `oauth` section
//! ([`OauthConfig`]); a cluster picks one with `security.oauth`
//! ([`ClusterBinding`]). See [`config`].
//!
//! ## Wiring
//!
//! Every command that connects to Kafka calls [`install`] (or
//! [`install_cluster`] for a single cluster) right after parsing its
//! configuration and before building any client. It:
//!
//! 1. validates the `oauth` section and the bindings (a cluster with
//!    `sasl_mechanism: OAUTHBEARER` and no `security.oauth` binding, a
//!    binding to an unknown provider, or a binding on a cluster that does
//!    not use OAUTHBEARER, is an error);
//! 2. builds one [`TokenCache`] per provider name and installs an
//!    [`OauthPluginFactory`] on each OAUTHBEARER cluster's `SecurityConfig`,
//!    so every connection of the cluster (and every cluster bound to the
//!    same provider) shares the cache.
//!
//! ## Runtime
//!
//! - [`framing`]: the client initial response, server error challenges and
//!   SASL extension validation.
//! - [`token`]: the [`OauthTokenProvider`] trait and [`OauthToken`].
//! - [`cache`]: the shared single-flight [`TokenCache`] (one per provider).
//! - [`providers`]: OIDC client credentials, Azure managed identity (IMDS),
//!   Azure workload identity, file and static.
//! - [`plugin`]: [`OauthBearerPlugin`] (a
//!   [`SaslMechanismPlugin`](crate::kafka::SaslMechanismPlugin)) and
//!   [`OauthPluginFactory`], both over a shared `TokenCache`.
//! - [`registry_auth`]: [`RegistryOauth`], bearer authentication with the
//!   Confluent Cloud routing headers for a Schema Registry client.
//! - [`http`]: the retrying HTTP transport and the `oauth2` crate exchange.
//! - [`jwt`]: unverified `exp` reads for providers that give no expiry.
//!
//! No token and no client secret is ever logged, printed by `Debug` or
//! serialized.

pub mod cache;
pub mod config;
pub mod framing;
pub mod http;
mod install;
pub mod jwt;
pub mod plugin;
pub mod providers;
pub mod registry_auth;
pub mod token;

pub use cache::{CacheSettings, TokenCache};
pub use config::{
    AzureManagedIdentityConfig, AzureWorkloadIdentityConfig, CacheConfig, ClusterBinding,
    FileProviderConfig, OauthConfig, OidcProviderConfig, ProviderConfig, SaslExtensions, Secret,
    StaticProviderConfig,
};
pub use install::{
    build_provider, install, install_cluster, KafkaRole, OauthInstall, ProviderRegistry,
};
pub use plugin::{OauthBearerPlugin, OauthPluginFactory};
pub use registry_auth::RegistryOauth;
pub use token::{OauthProviderError, OauthToken, OauthTokenProvider};

/// Errors from OAuth setup ([`install`]). Raised before anything connects.
#[derive(Debug, thiserror::Error)]
pub enum OauthError {
    /// The configuration is wrong (unknown provider, missing binding,
    /// insecure endpoint, ...).
    #[error("OAuth configuration error: {0}")]
    Config(String),

    /// A token provider could not be built.
    #[error("OAuth provider '{provider}': {source}")]
    Provider {
        provider: String,
        #[source]
        source: OauthProviderError,
    },
}

impl From<OauthError> for crate::Error {
    fn from(e: OauthError) -> Self {
        crate::Error::Config(e.to_string())
    }
}
