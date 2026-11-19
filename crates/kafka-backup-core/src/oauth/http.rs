//! HTTP plumbing shared by the network token providers: a reqwest client
//! that never follows redirects, retries with backoff on 429 / 5xx /
//! connection errors, and the RFC 6749 client-credentials exchange through
//! the `oauth2` crate.
//!
//! Nothing here logs or returns a token or a secret: token-endpoint error
//! bodies are reduced to their RFC 6749 `error` / `error_description`, and
//! known secrets are scrubbed from anything that is echoed back.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use oauth2::basic::{
    BasicErrorResponse, BasicRevocationErrorResponse, BasicTokenIntrospectionResponse,
    BasicTokenType,
};
use oauth2::{
    AccessToken, AuthType, ClientId, ClientSecret, RefreshToken, RequestTokenError, Scope,
    StandardRevocableToken, TokenResponse, TokenUrl,
};
use serde::{Deserialize, Deserializer, Serialize};
use tracing::debug;

use super::token::{OauthProviderError, OauthToken};

/// Retry policy for identity-provider requests.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    /// Retries after the first attempt.
    pub max_retries: u32,
    /// First backoff step; doubles each retry, plus up to 50 % jitter.
    pub base_delay: Duration,
    /// Upper bound for one wait (also caps `Retry-After`).
    pub max_delay: Duration,
    /// Also retry 404 and 410 (Azure IMDS answers these while a managed
    /// identity is still being assigned).
    pub retry_not_found: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(10),
            retry_not_found: false,
        }
    }
}

impl RetryPolicy {
    /// The policy for Azure IMDS.
    pub fn imds() -> Self {
        Self {
            max_retries: 4,
            retry_not_found: true,
            ..Self::default()
        }
    }

    fn backoff(&self, attempt: u32) -> Duration {
        let base = self
            .base_delay
            .saturating_mul(2u32.saturating_pow(attempt.min(16)));
        let jitter_ms = (base.as_millis() as u64 / 2).max(1);
        (base + Duration::from_millis(rand::random::<u64>() % jitter_ms)).min(self.max_delay)
    }

    fn retryable(&self, status: reqwest::StatusCode) -> bool {
        status == reqwest::StatusCode::TOO_MANY_REQUESTS
            || status.is_server_error()
            || (self.retry_not_found
                && (status == reqwest::StatusCode::NOT_FOUND
                    || status == reqwest::StatusCode::GONE))
    }
}

/// A reqwest client for identity-provider calls: no redirects (an
/// attacker-controlled redirect must not receive credentials), rustls,
/// bounded timeouts and an optional extra CA.
pub fn build_http_client(ca_cert: Option<&Path>) -> Result<reqwest::Client, OauthProviderError> {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .user_agent(concat!("kafka-backup/", env!("CARGO_PKG_VERSION")));
    if let Some(path) = ca_cert {
        let pem = std::fs::read(path).map_err(|e| {
            OauthProviderError::Config(format!("cannot read ca_cert {}: {e}", path.display()))
        })?;
        let cert = reqwest::Certificate::from_pem(&pem).map_err(|e| {
            OauthProviderError::Config(format!("invalid ca_cert {}: {e}", path.display()))
        })?;
        builder = builder.add_root_certificate(cert);
    }
    builder
        .build()
        .map_err(|e| OauthProviderError::Config(format!("cannot build HTTP client: {e}")))
}

/// Send the request `make` builds, retrying per `policy`. Returns the last
/// response (successful or not) or the last transport error.
pub async fn send_with_retry<F>(
    policy: &RetryPolicy,
    mut make: F,
) -> Result<reqwest::Response, OauthProviderError>
where
    F: FnMut() -> reqwest::RequestBuilder,
{
    let mut attempt = 0u32;
    loop {
        match make().send().await {
            Ok(response) => {
                let status = response.status();
                if policy.retryable(status) && attempt < policy.max_retries {
                    let delay = retry_after(&response)
                        .unwrap_or_else(|| policy.backoff(attempt))
                        .min(policy.max_delay);
                    debug!(
                        status = status.as_u16(),
                        attempt = attempt + 1,
                        delay_ms = delay.as_millis() as u64,
                        "Identity provider answered with a retryable status; retrying"
                    );
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                    continue;
                }
                return Ok(response);
            }
            Err(e) if (e.is_connect() || e.is_timeout()) && attempt < policy.max_retries => {
                let delay = policy.backoff(attempt);
                debug!(
                    error = %e,
                    attempt = attempt + 1,
                    delay_ms = delay.as_millis() as u64,
                    "Identity provider unreachable; retrying"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(e) => return Err(OauthProviderError::Http(e.to_string())),
        }
    }
}

fn retry_after(response: &reqwest::Response) -> Option<Duration> {
    let value = response.headers().get(reqwest::header::RETRY_AFTER)?;
    value
        .to_str()
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
        .map(Duration::from_secs)
}

/// Replace every occurrence of a known secret with `***`.
pub fn redact_secrets(text: &str, secrets: &[&str]) -> String {
    let mut out = text.to_string();
    for secret in secrets.iter().filter(|s| s.len() >= 4) {
        out = out.replace(secret, "***");
    }
    out
}

/// Replace JWT-shaped substrings (`eyJ...` header, payload and signature
/// segments) with `<jwt>`: an identity provider's error text must never
/// carry a token into a log.
pub fn redact_jwts(text: &str) -> String {
    static JWT: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    JWT.get_or_init(|| {
        regex::Regex::new(r"eyJ[A-Za-z0-9_-]{2,}\.[A-Za-z0-9_-]{2,}\.[A-Za-z0-9_-]*")
            .expect("valid regex")
    })
    .replace_all(text, "<jwt>")
    .into_owned()
}

/// Text from an identity provider made safe to show: known secrets and
/// JWT-shaped strings removed, at most 300 characters.
pub fn scrub(text: &str, secrets: &[&str]) -> String {
    truncate(&redact_jwts(&redact_secrets(text, secrets)), 300)
}

/// At most `max` characters of `text`, on a char boundary.
pub fn truncate(text: &str, max: usize) -> String {
    match text.char_indices().nth(max) {
        Some((idx, _)) => format!("{}...", &text[..idx]),
        None => text.to_string(),
    }
}

// ── oauth2 transport ────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct TransportError(String);

/// `oauth2::AsyncHttpClient` over reqwest with [`send_with_retry`]; records
/// the final HTTP status for error messages.
struct RetryingTransport {
    client: reqwest::Client,
    policy: RetryPolicy,
    last_status: Mutex<Option<u16>>,
}

impl<'c> oauth2::AsyncHttpClient<'c> for RetryingTransport {
    type Error = TransportError;
    type Future =
        Pin<Box<dyn Future<Output = Result<oauth2::HttpResponse, TransportError>> + Send + 'c>>;

    fn call(&'c self, request: oauth2::HttpRequest) -> Self::Future {
        Box::pin(async move {
            let (parts, body) = request.into_parts();
            let url = parts.uri.to_string();
            let response = send_with_retry(&self.policy, || {
                self.client
                    .request(parts.method.clone(), &url)
                    .headers(parts.headers.clone())
                    .body(body.clone())
            })
            .await
            .map_err(|e| match e {
                // Keep the bare message: the caller adds its own prefix.
                OauthProviderError::Http(message) => TransportError(message),
                other => TransportError(other.to_string()),
            })?;
            let status = response.status();
            *self.last_status.lock().unwrap_or_else(|p| p.into_inner()) = Some(status.as_u16());
            let headers = response.headers().clone();
            let bytes = response
                .bytes()
                .await
                .map_err(|e| TransportError(format!("reading the response failed: {e}")))?;
            let mut builder = http::Response::builder().status(status.as_u16());
            for (name, value) in headers.iter() {
                builder = builder.header(name, value);
            }
            builder
                .body(bytes.to_vec())
                .map_err(|e| TransportError(e.to_string()))
        })
    }
}

/// Token response with lenient `expires_in` (number or string: Azure AD v1
/// and some gateways send strings).
#[derive(Debug, Deserialize, Serialize)]
pub struct LenientTokenResponse {
    access_token: AccessToken,
    #[serde(default = "bearer")]
    token_type: BasicTokenType,
    #[serde(default, deserialize_with = "lenient_u64")]
    expires_in: Option<u64>,
    #[serde(default)]
    scope: Option<String>,
}

fn bearer() -> BasicTokenType {
    BasicTokenType::Bearer
}

impl TokenResponse for LenientTokenResponse {
    type TokenType = BasicTokenType;

    fn access_token(&self) -> &AccessToken {
        &self.access_token
    }

    fn token_type(&self) -> &BasicTokenType {
        &self.token_type
    }

    fn expires_in(&self) -> Option<Duration> {
        self.expires_in.map(Duration::from_secs)
    }

    fn refresh_token(&self) -> Option<&RefreshToken> {
        None
    }

    fn scopes(&self) -> Option<&Vec<Scope>> {
        None
    }
}

/// `u64` from a JSON number or a numeric string.
pub fn lenient_u64<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrString {
        Num(f64),
        Str(String),
    }
    Ok(match Option::<NumOrString>::deserialize(d)? {
        None => None,
        Some(NumOrString::Num(n)) if n.is_finite() && n >= 0.0 => Some(n as u64),
        Some(NumOrString::Num(_)) => None,
        Some(NumOrString::Str(s)) => s.trim().parse::<f64>().ok().map(|n| n as u64),
    })
}

type CredentialsClient = oauth2::Client<
    BasicErrorResponse,
    LenientTokenResponse,
    BasicTokenIntrospectionResponse,
    StandardRevocableToken,
    BasicRevocationErrorResponse,
>;

/// How the client authenticates to the token endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ClientAuth {
    /// `client_secret_basic`: HTTP Basic with the URL-encoded id and secret.
    #[default]
    Basic,
    /// `client_secret_post`: id and secret in the form body.
    Post,
}

/// One client-credentials grant (RFC 6749 §4.4).
pub struct ClientCredentialsGrant<'a> {
    pub token_url: &'a str,
    pub client_id: &'a str,
    /// `None` for client-assertion flows (the assertion goes in `extra_params`).
    pub client_secret: Option<&'a str>,
    pub client_auth: ClientAuth,
    pub scope: Option<&'a str>,
    pub extra_params: Vec<(String, String)>,
}

/// Run the grant and turn the response into an [`OauthToken`]. `secrets`
/// are scrubbed from any error text.
pub async fn exchange_client_credentials(
    http: &reqwest::Client,
    policy: RetryPolicy,
    grant: ClientCredentialsGrant<'_>,
    secrets: &[&str],
) -> Result<OauthToken, OauthProviderError> {
    let token_url = TokenUrl::new(grant.token_url.to_string()).map_err(|e| {
        OauthProviderError::Config(format!(
            "invalid token endpoint URL {}: {e}",
            grant.token_url
        ))
    })?;
    let auth_type = match grant.client_auth {
        ClientAuth::Basic => AuthType::BasicAuth,
        ClientAuth::Post => AuthType::RequestBody,
    };
    let mut client: CredentialsClient =
        oauth2::Client::new(ClientId::new(grant.client_id.into())).set_auth_type(auth_type);
    if let Some(secret) = grant.client_secret {
        client = client.set_client_secret(ClientSecret::new(secret.into()));
    }
    let client = client.set_token_uri(token_url);

    let mut request = client.exchange_client_credentials();
    if let Some(scope) = grant.scope.filter(|s| !s.trim().is_empty()) {
        request = request.add_scope(Scope::new(scope.trim().to_string()));
    }
    for (name, value) in grant.extra_params {
        request = request.add_extra_param(name, value);
    }

    let transport = RetryingTransport {
        client: http.clone(),
        policy,
        last_status: Mutex::new(None),
    };
    let requested_at = SystemTime::now();
    let result = request.request_async(&transport).await;
    let status = *transport
        .last_status
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    match result {
        Ok(response) => {
            let expires_at = response.expires_in().map(|d| requested_at + d);
            Ok(OauthToken::new(
                response.access_token.secret().clone(),
                expires_at,
                response.scope.clone(),
                Default::default(),
            ))
        }
        Err(e) => Err(map_token_error(e, status, secrets)),
    }
}

fn map_token_error(
    error: RequestTokenError<TransportError, BasicErrorResponse>,
    status: Option<u16>,
    secrets: &[&str],
) -> OauthProviderError {
    let clean = |s: &str| scrub(s, secrets);
    match error {
        RequestTokenError::ServerResponse(response) => OauthProviderError::Endpoint {
            status,
            error: response.error().to_string(),
            description: response.error_description().map(|d| clean(d)),
        },
        RequestTokenError::Request(e) => OauthProviderError::Http(clean(&e.to_string())),
        RequestTokenError::Parse(e, body) => {
            if status == Some(200) {
                // The body may hold a token: never echo it.
                OauthProviderError::InvalidResponse(format!("not a token response ({})", e.path()))
            } else {
                OauthProviderError::Endpoint {
                    status,
                    error: "unparseable error response".into(),
                    description: Some(clean(&String::from_utf8_lossy(&body))),
                }
            }
        }
        RequestTokenError::Other(message) => {
            if status.is_some_and(|s| s != 200) {
                OauthProviderError::Endpoint {
                    status,
                    error: "error response".into(),
                    description: Some(clean(&message)),
                }
            } else {
                OauthProviderError::InvalidResponse(clean(&message))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lenient_expires_in() {
        let r: LenientTokenResponse = serde_json::from_str(
            r#"{"access_token":"a","token_type":"Bearer","expires_in":"3599"}"#,
        )
        .unwrap();
        assert_eq!(r.expires_in, Some(3599));
        let r: LenientTokenResponse =
            serde_json::from_str(r#"{"access_token":"a","token_type":"bearer","expires_in":60}"#)
                .unwrap();
        assert_eq!(r.expires_in, Some(60));
        let r: LenientTokenResponse = serde_json::from_str(r#"{"access_token":"a"}"#).unwrap();
        assert_eq!(r.expires_in, None);
        assert_eq!(r.token_type, BasicTokenType::Bearer);
    }

    #[test]
    fn debug_never_shows_the_access_token() {
        let r: LenientTokenResponse =
            serde_json::from_str(r#"{"access_token":"very-secret-access-token"}"#).unwrap();
        assert!(!format!("{r:?}").contains("very-secret-access-token"));
    }

    #[test]
    fn redaction_and_truncation() {
        assert_eq!(
            redact_secrets("bad secret s3cr3t-value given", &["s3cr3t-value", ""]),
            "bad secret *** given"
        );
        assert_eq!(truncate("abcdef", 3), "abc...");
        assert_eq!(truncate("ab", 3), "ab");
    }

    #[test]
    fn jwts_are_scrubbed() {
        let text = "token eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.c2lnbmF0dXJl rejected";
        assert_eq!(redact_jwts(text), "token <jwt> rejected");
        assert_eq!(
            scrub("pw hunter22 eyJabc.eyJdef.ghi", &["hunter22"]),
            "pw *** <jwt>"
        );
    }

    #[test]
    fn backoff_is_bounded() {
        let p = RetryPolicy::default();
        for attempt in 0..40 {
            assert!(p.backoff(attempt) <= p.max_delay);
        }
        assert!(p.backoff(0) >= p.base_delay);
    }
}
