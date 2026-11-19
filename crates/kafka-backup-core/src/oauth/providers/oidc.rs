//! OIDC / OAuth 2.0 client credentials (RFC 6749 §4.4) through the `oauth2`
//! crate: Microsoft Entra ID, Okta, Auth0, Keycloak and any other
//! standards-compliant token endpoint.
//!
//! - `client_secret_basic` (default) or `client_secret_post`.
//! - `scope` and (for IdPs that use it) `audience`, plus free-form
//!   `extra_params`.
//! - The token endpoint is configured directly or discovered from
//!   `{issuer}/.well-known/openid-configuration` on first use.
//! - 429 / 5xx / connection errors are retried with backoff
//!   ([`RetryPolicy`]); 4xx fail at once with the RFC 6749 error.

use std::collections::BTreeMap;
use std::path::PathBuf;

use async_trait::async_trait;
use serde::Deserialize;
use tokio::sync::OnceCell;
use zeroize::Zeroizing;

use crate::oauth::http::{
    build_http_client, exchange_client_credentials, scrub, send_with_retry, ClientAuth,
    ClientCredentialsGrant, RetryPolicy,
};
use crate::oauth::token::{OauthProviderError, OauthToken, OauthTokenProvider};

/// Settings for [`OidcClientCredentialsProvider`].
#[derive(Clone, Default)]
pub struct OidcSettings {
    /// The token endpoint. Exactly one of this and `issuer`.
    pub token_endpoint_url: Option<String>,
    /// OIDC issuer for discovery.
    pub issuer: Option<String>,
    pub client_id: String,
    pub client_secret: Zeroizing<String>,
    pub client_auth: ClientAuth,
    pub scope: Option<String>,
    pub audience: Option<String>,
    pub extra_params: BTreeMap<String, String>,
    pub ca_cert: Option<PathBuf>,
    pub retry: RetryPolicy,
}

enum Endpoint {
    Url(String),
    Discover {
        issuer: String,
        discovered: OnceCell<String>,
    },
}

pub struct OidcClientCredentialsProvider {
    endpoint: Endpoint,
    client_id: String,
    client_secret: Zeroizing<String>,
    client_auth: ClientAuth,
    scope: Option<String>,
    extra_params: Vec<(String, String)>,
    http: reqwest::Client,
    retry: RetryPolicy,
}

impl std::fmt::Debug for OidcClientCredentialsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let endpoint = match &self.endpoint {
            Endpoint::Url(url) => format!("token_endpoint_url={url}"),
            Endpoint::Discover { issuer, .. } => format!("issuer={issuer}"),
        };
        f.debug_struct("OidcClientCredentialsProvider")
            .field("endpoint", &endpoint)
            .field("client_id", &self.client_id)
            .field("client_secret", &"***REDACTED***")
            .field("client_auth", &self.client_auth)
            .field("scope", &self.scope)
            .finish()
    }
}

impl OidcClientCredentialsProvider {
    pub fn new(settings: OidcSettings) -> Result<Self, OauthProviderError> {
        let endpoint = match (settings.token_endpoint_url, settings.issuer) {
            (Some(url), None) => Endpoint::Url(url),
            (None, Some(issuer)) => Endpoint::Discover {
                issuer,
                discovered: OnceCell::new(),
            },
            _ => {
                return Err(OauthProviderError::Config(
                    "set exactly one of token_endpoint_url and issuer".into(),
                ))
            }
        };
        if settings.client_id.trim().is_empty() {
            return Err(OauthProviderError::Config("client_id is empty".into()));
        }
        if settings.client_secret.is_empty() {
            return Err(OauthProviderError::Config(
                "client_secret is empty (if it comes from ${ENV_VAR}, is the variable set?)".into(),
            ));
        }
        let mut extra_params: Vec<(String, String)> = settings.extra_params.into_iter().collect();
        if let Some(audience) = settings.audience.filter(|a| !a.is_empty()) {
            extra_params.push(("audience".into(), audience));
        }
        Ok(Self {
            endpoint,
            client_id: settings.client_id,
            client_secret: settings.client_secret,
            client_auth: settings.client_auth,
            scope: settings.scope,
            extra_params,
            http: build_http_client(settings.ca_cert.as_deref())?,
            retry: settings.retry,
        })
    }

    async fn token_url(&self) -> Result<String, OauthProviderError> {
        match &self.endpoint {
            Endpoint::Url(url) => Ok(url.clone()),
            Endpoint::Discover { issuer, discovered } => discovered
                .get_or_try_init(|| discover_token_endpoint(&self.http, issuer, &self.retry))
                .await
                .cloned(),
        }
    }
}

#[derive(Deserialize)]
struct Discovery {
    token_endpoint: String,
}

/// The `token_endpoint` of an OIDC issuer.
pub async fn discover_token_endpoint(
    http: &reqwest::Client,
    issuer: &str,
    retry: &RetryPolicy,
) -> Result<String, OauthProviderError> {
    let url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let response = send_with_retry(retry, || {
        http.get(&url)
            .header(reqwest::header::ACCEPT, "application/json")
    })
    .await?;
    let status = response.status();
    let body = response
        .text()
        .await
        .map_err(|e| OauthProviderError::Http(format!("reading {url}: {e}")))?;
    if !status.is_success() {
        return Err(OauthProviderError::Endpoint {
            status: Some(status.as_u16()),
            error: format!("OIDC discovery failed at {url}"),
            description: Some(scrub(&body, &[])),
        });
    }
    let doc: Discovery = serde_json::from_str(&body).map_err(|e| {
        OauthProviderError::InvalidResponse(format!(
            "OIDC discovery document at {url} has no token_endpoint ({e})"
        ))
    })?;
    tracing::debug!(issuer, token_endpoint = %doc.token_endpoint, "OIDC token endpoint discovered");
    Ok(doc.token_endpoint)
}

#[async_trait]
impl OauthTokenProvider for OidcClientCredentialsProvider {
    async fn get_token(&self) -> Result<OauthToken, OauthProviderError> {
        let token_url = self.token_url().await?;
        exchange_client_credentials(
            &self.http,
            self.retry,
            ClientCredentialsGrant {
                token_url: &token_url,
                client_id: &self.client_id,
                client_secret: Some(self.client_secret.as_str()),
                client_auth: self.client_auth,
                scope: self.scope.as_deref(),
                extra_params: self.extra_params.clone(),
            },
            &[self.client_secret.as_str()],
        )
        .await
    }

    fn kind(&self) -> &'static str {
        "oidc_client_credentials"
    }
}
