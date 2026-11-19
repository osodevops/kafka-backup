//! Bearer-token authentication for a Schema Registry client.
//!
//! Every request gets `Authorization: Bearer <token>` from a shared
//! [`TokenCache`] (the same cache the Kafka connections of a cluster bound
//! to that provider use), plus Confluent Cloud's routing headers:
//! `target-sr-cluster` when a Schema Registry cluster ID is given, and
//! `Confluent-Identity-Pool-Id` only when an identity pool is given.
//!
//! Build the cache with [`ProviderRegistry`](crate::oauth::ProviderRegistry)
//! so it is shared with every other user of the provider.

use crate::oauth::cache::TokenCache;
use crate::oauth::config::normalise_pool_list;
use crate::oauth::token::OauthProviderError;

/// Confluent Cloud header naming the Schema Registry cluster.
pub const TARGET_SR_CLUSTER_HEADER: &str = "target-sr-cluster";
/// Confluent Cloud header naming the identity pool(s).
pub const IDENTITY_POOL_HEADER: &str = "Confluent-Identity-Pool-Id";

#[derive(Debug, Clone)]
pub struct RegistryOauth {
    cache: TokenCache,
    target_sr_cluster: Option<String>,
    identity_pool_id: Option<String>,
}

impl RegistryOauth {
    /// Bearer authentication from `cache`. `target_sr_cluster` is the
    /// Confluent Cloud Schema Registry cluster ID (`lsrc-...`);
    /// `identity_pool_id` is one pool ID (`pool-...`) or a comma-separated
    /// list. Blank values are treated as unset.
    pub fn new(
        cache: TokenCache,
        target_sr_cluster: Option<&str>,
        identity_pool_id: Option<&str>,
    ) -> Self {
        Self {
            cache,
            target_sr_cluster: target_sr_cluster
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string),
            identity_pool_id: identity_pool_id
                .map(normalise_pool_list)
                .filter(|s| !s.is_empty()),
        }
    }

    pub fn cache(&self) -> &TokenCache {
        &self.cache
    }

    /// Add the bearer token and the Confluent headers to `request`.
    pub async fn apply(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, OauthProviderError> {
        let token = self.cache.get().await?;
        let mut request = request.bearer_auth(token.value());
        if let Some(cluster) = &self.target_sr_cluster {
            request = request.header(TARGET_SR_CLUSTER_HEADER, cluster);
        }
        if let Some(pool) = &self.identity_pool_id {
            request = request.header(IDENTITY_POOL_HEADER, pool);
        }
        Ok(request)
    }

    /// The registry answered 401: drop the token (if old enough) so the
    /// next attempt fetches a new one. Returns true if it was dropped.
    pub fn invalidate(&self) -> bool {
        self.cache.invalidate()
    }
}
