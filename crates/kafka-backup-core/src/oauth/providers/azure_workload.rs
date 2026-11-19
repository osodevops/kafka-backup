//! Azure workload identity (AKS): exchange the Kubernetes-projected
//! federated token for an Entra access token with a client assertion
//! (`client_assertion_type=urn:ietf:params:oauth:client-assertion-type:jwt-bearer`),
//! through the `oauth2` crate. No Azure SDK.
//!
//! The federated token file is re-read on every fetch (the kubelet rotates
//! it). `tenant_id`, `client_id`, `token_file` and `authority_host` default to
//! the variables the workload-identity webhook injects: `AZURE_TENANT_ID`,
//! `AZURE_CLIENT_ID`, `AZURE_FEDERATED_TOKEN_FILE`, `AZURE_AUTHORITY_HOST`.

use std::path::PathBuf;

use async_trait::async_trait;

use crate::oauth::http::{
    build_http_client, exchange_client_credentials, ClientAuth, ClientCredentialsGrant, RetryPolicy,
};
use crate::oauth::token::{OauthProviderError, OauthToken, OauthTokenProvider};

/// Entra ID's public-cloud authority.
pub const DEFAULT_AUTHORITY_HOST: &str = "https://login.microsoftonline.com/";

/// The RFC 7523 assertion type Entra expects.
pub const JWT_BEARER_ASSERTION_TYPE: &str =
    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

#[derive(Debug)]
pub struct AzureWorkloadIdentityProvider {
    token_url: String,
    client_id: String,
    scope: String,
    token_file: PathBuf,
    http: reqwest::Client,
    retry: RetryPolicy,
}

impl AzureWorkloadIdentityProvider {
    pub fn new(
        tenant_id: &str,
        client_id: &str,
        token_file: PathBuf,
        authority_host: &str,
        scope: &str,
        retry: RetryPolicy,
    ) -> Result<Self, OauthProviderError> {
        for (name, value) in [
            ("tenant_id", tenant_id),
            ("client_id", client_id),
            ("scope", scope),
        ] {
            if value.trim().is_empty() {
                return Err(OauthProviderError::Config(format!(
                    "azure_workload_identity: {name} is empty"
                )));
            }
        }
        Ok(Self {
            token_url: format!(
                "{}/{}/oauth2/v2.0/token",
                authority_host.trim_end_matches('/'),
                tenant_id.trim()
            ),
            client_id: client_id.trim().to_string(),
            scope: scope.trim().to_string(),
            token_file,
            http: build_http_client(None)?,
            retry,
        })
    }

    /// The Entra token endpoint used.
    pub fn token_url(&self) -> &str {
        &self.token_url
    }
}

#[async_trait]
impl OauthTokenProvider for AzureWorkloadIdentityProvider {
    async fn get_token(&self) -> Result<OauthToken, OauthProviderError> {
        let assertion = tokio::fs::read_to_string(&self.token_file)
            .await
            .map_err(|e| {
                OauthProviderError::Config(format!(
                    "cannot read the federated token file {}: {e}",
                    self.token_file.display()
                ))
            })?;
        let assertion = zeroize::Zeroizing::new(assertion.trim().to_string());
        if assertion.is_empty() {
            return Err(OauthProviderError::InvalidToken(format!(
                "federated token file {} is empty",
                self.token_file.display()
            )));
        }
        exchange_client_credentials(
            &self.http,
            self.retry,
            ClientCredentialsGrant {
                token_url: &self.token_url,
                client_id: &self.client_id,
                client_secret: None,
                client_auth: ClientAuth::Post,
                scope: Some(&self.scope),
                extra_params: vec![
                    (
                        "client_assertion_type".into(),
                        JWT_BEARER_ASSERTION_TYPE.into(),
                    ),
                    ("client_assertion".into(), assertion.to_string()),
                ],
            },
            &[assertion.as_str()],
        )
        .await
    }

    fn kind(&self) -> &'static str {
        "azure_workload_identity"
    }
}
