//! Azure managed identity through the Instance Metadata Service (IMDS):
//! a plain `GET` with `Metadata: true`, no Azure SDK.
//!
//! IMDS returns `expires_in` and `expires_on` as **strings**; both are
//! accepted as strings or numbers. `expires_on` (absolute) wins when
//! present. 404 / 410 / 429 / 5xx are retried: IMDS answers 404 / 410 while a
//! newly assigned identity is still propagating.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use serde::Deserialize;

use crate::oauth::http::{build_http_client, lenient_u64, scrub, send_with_retry, RetryPolicy};
use crate::oauth::token::{OauthProviderError, OauthToken, OauthTokenProvider};

/// The IMDS token endpoint.
pub const DEFAULT_IMDS_ENDPOINT: &str = "http://169.254.169.254/metadata/identity/oauth2/token";

/// IMDS API version used for token requests.
pub const IMDS_API_VERSION: &str = "2018-02-01";

#[derive(Debug)]
pub struct AzureManagedIdentityProvider {
    endpoint: String,
    resource: String,
    client_id: Option<String>,
    http: reqwest::Client,
    retry: RetryPolicy,
}

/// `resource` from an Entra `scope` (`api://app/.default` → `api://app`).
pub fn resource_from_scope(scope: &str) -> String {
    scope
        .trim()
        .strip_suffix("/.default")
        .unwrap_or(scope.trim())
        .to_string()
}

impl AzureManagedIdentityProvider {
    /// `client_id` selects a user-assigned identity; `None` uses the
    /// system-assigned one. `endpoint` overrides IMDS (tests, proxies).
    pub fn new(
        resource: impl Into<String>,
        client_id: Option<String>,
        endpoint: Option<String>,
        ca_cert: Option<PathBuf>,
        retry: RetryPolicy,
    ) -> Result<Self, OauthProviderError> {
        let resource = resource.into();
        if resource.trim().is_empty() {
            return Err(OauthProviderError::Config(
                "azure_managed_identity: resource (or scope) is required".into(),
            ));
        }
        Ok(Self {
            endpoint: endpoint.unwrap_or_else(|| DEFAULT_IMDS_ENDPOINT.to_string()),
            resource,
            client_id: client_id.filter(|c| !c.trim().is_empty()),
            http: build_http_client(ca_cert.as_deref())?,
            retry,
        })
    }
}

#[derive(Deserialize)]
struct ImdsToken {
    access_token: String,
    #[serde(default, deserialize_with = "lenient_u64")]
    expires_in: Option<u64>,
    #[serde(default, deserialize_with = "lenient_u64")]
    expires_on: Option<u64>,
}

#[derive(Deserialize)]
struct ImdsError {
    error: String,
    #[serde(default)]
    error_description: Option<String>,
}

#[async_trait]
impl OauthTokenProvider for AzureManagedIdentityProvider {
    async fn get_token(&self) -> Result<OauthToken, OauthProviderError> {
        let mut query = vec![
            ("api-version", IMDS_API_VERSION.to_string()),
            ("resource", self.resource.clone()),
        ];
        if let Some(client_id) = &self.client_id {
            query.push(("client_id", client_id.clone()));
        }
        let requested_at = SystemTime::now();
        let response = send_with_retry(&self.retry, || {
            self.http
                .get(&self.endpoint)
                .query(&query)
                .header("Metadata", "true")
        })
        .await?;
        let status = response.status();
        let body = response
            .bytes()
            .await
            .map_err(|e| OauthProviderError::Http(format!("reading the IMDS response: {e}")))?;
        if !status.is_success() {
            let (error, description) = match serde_json::from_slice::<ImdsError>(&body) {
                Ok(e) => (
                    scrub(&e.error, &[]),
                    e.error_description.map(|d| scrub(&d, &[])),
                ),
                Err(_) => (
                    "IMDS error".to_string(),
                    Some(scrub(&String::from_utf8_lossy(&body), &[])),
                ),
            };
            return Err(OauthProviderError::Endpoint {
                status: Some(status.as_u16()),
                error,
                description,
            });
        }
        let token: ImdsToken = serde_json::from_slice(&body).map_err(|e| {
            // Never echo the body: it holds the token.
            OauthProviderError::InvalidResponse(format!(
                "IMDS token response: {}",
                e.classify_name()
            ))
        })?;
        let expires_at = token
            .expires_on
            .map(|secs| SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
            .or_else(|| {
                token
                    .expires_in
                    .map(|secs| requested_at + Duration::from_secs(secs))
            });
        Ok(OauthToken::new(
            token.access_token,
            expires_at,
            None,
            HashMap::new(),
        ))
    }

    fn kind(&self) -> &'static str {
        "azure_managed_identity"
    }
}

trait ClassifyName {
    fn classify_name(&self) -> &'static str;
}

impl ClassifyName for serde_json::Error {
    fn classify_name(&self) -> &'static str {
        match self.classify() {
            serde_json::error::Category::Io => "I/O error",
            serde_json::error::Category::Syntax => "not JSON",
            serde_json::error::Category::Data => "missing or invalid access_token",
            serde_json::error::Category::Eof => "truncated JSON",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_to_resource() {
        assert_eq!(resource_from_scope("api://abc/.default"), "api://abc");
        assert_eq!(resource_from_scope("api://abc"), "api://abc");
    }
}
