use std::time::Duration;

use bytes::Bytes;
use reqwest::{Client, Method, StatusCode};
use tracing::{debug, warn};

use super::config::{ApicurioApiVersion, ApicurioRegistryAuth, ApicurioRegistryConfig};
use crate::apicurio_registry::error::ApicurioRegistryError;

use super::models::*;

// reqwest may wrap a failed response body in a Decode error. Inspect the
// chain so transport interruptions retry reads while complete invalid JSON
// still fails immediately.
fn interrupted_response_body(error: &reqwest::Error) -> bool {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
    while let Some(current) = cause {
        if let Some(http) = current.downcast_ref::<reqwest::Error>() {
            if http.is_body() || http.is_timeout() {
                return true;
            }
        }
        cause = current.source();
    }
    false
}

/// Apicurio Registry Core API v3 client.
///
/// Handles authentication (Basic, OIDC, mTLS), rate limiting, and retries.
pub struct ApicurioRegistryClient {
    client: Client,
    api_version: ApicurioApiVersion,
    base_url: String,
    auth: Option<ApicurioRegistryAuth>,
    oidc_token: tokio::sync::RwLock<Option<OidcToken>>,
    rate_limiter: RateLimiter,
    max_retries: u32,
    retry_backoff_ms: u64,
}

struct OidcToken {
    access_token: String,
    expires_at: tokio::time::Instant,
}

/// Simple token-bucket rate limiter using tokio::time.
struct RateLimiter {
    min_interval: Duration,
    last_request: tokio::sync::Mutex<tokio::time::Instant>,
}

impl RateLimiter {
    fn new(requests_per_second: u32) -> Self {
        let rps = requests_per_second.max(1);
        Self {
            min_interval: Duration::from_micros(1_000_000 / rps as u64),
            last_request: tokio::sync::Mutex::new(tokio::time::Instant::now()),
        }
    }

    async fn acquire(&self) {
        let mut last = self.last_request.lock().await;
        let elapsed = last.elapsed();
        if elapsed < self.min_interval {
            tokio::time::sleep(self.min_interval - elapsed).await;
        }
        *last = tokio::time::Instant::now();
    }
}

impl ApicurioRegistryClient {
    /// Create a new Apicurio Registry client from configuration.
    pub fn new(config: &ApicurioRegistryConfig) -> Result<Self, ApicurioRegistryError> {
        let conn = config.connection.clone().unwrap_or_default();

        let mut builder = Client::builder()
            .use_rustls_tls()
            .timeout(Duration::from_millis(conn.timeout_ms))
            .user_agent(concat!("kafka-backup/", env!("CARGO_PKG_VERSION")));

        // Configure TLS (server CA cert)
        if let Some(tls) = &config.tls {
            if let Some(ca_path) = &tls.ca_cert {
                let ca_bytes = std::fs::read(ca_path).map_err(|e| {
                    ApicurioRegistryError::Config(format!(
                        "Failed to read CA cert {}: {}",
                        ca_path, e
                    ))
                })?;
                let ca = reqwest::Certificate::from_pem(&ca_bytes).map_err(|e| {
                    ApicurioRegistryError::Config(format!("Invalid CA cert: {}", e))
                })?;
                builder = builder.add_root_certificate(ca);
            }
        }

        // Configure mTLS client identity
        if let Some(ApicurioRegistryAuth::MutualTls {
            ca_cert,
            client_cert,
            client_key,
        }) = &config.auth
        {
            if client_cert.is_none() || client_key.is_none() {
                return Err(ApicurioRegistryError::Config(
                    "mTLS requires both client_cert and client_key".into(),
                ));
            }
            if let Some(ca_path) = ca_cert {
                let ca_bytes = std::fs::read(ca_path).map_err(|e| {
                    ApicurioRegistryError::Config(format!("Failed to read CA cert: {}", e))
                })?;
                let ca = reqwest::Certificate::from_pem(&ca_bytes).map_err(|e| {
                    ApicurioRegistryError::Config(format!("Invalid CA cert: {}", e))
                })?;
                builder = builder.add_root_certificate(ca);
            }
            if let (Some(cert_path), Some(key_path)) = (client_cert, client_key) {
                let mut pem = std::fs::read(cert_path).map_err(|e| {
                    ApicurioRegistryError::Config(format!("Failed to read client cert: {}", e))
                })?;
                let key_bytes = std::fs::read(key_path).map_err(|e| {
                    ApicurioRegistryError::Config(format!("Failed to read client key: {}", e))
                })?;
                pem.extend_from_slice(&key_bytes);
                let identity = reqwest::Identity::from_pem(&pem).map_err(|e| {
                    ApicurioRegistryError::Config(format!("Invalid client identity: {}", e))
                })?;
                builder = builder.identity(identity);
            }
        }

        let client = builder.build().map_err(|e| {
            ApicurioRegistryError::Config(format!("Failed to build HTTP client: {}", e))
        })?;

        let trimmed = config.url.trim_end_matches('/');
        let suffix = format!("/apis/registry/{}", config.api_version.as_str());
        if (trimmed.ends_with("/v2") || trimmed.ends_with("/v3")) && !trimmed.ends_with(&suffix) {
            return Err(ApicurioRegistryError::Config(
                "API version disagrees with registry URL".into(),
            ));
        }
        let base_url = if trimmed.ends_with(&suffix) {
            trimmed.to_string()
        } else {
            format!("{trimmed}{suffix}")
        };

        Ok(Self {
            client,
            api_version: config.api_version,
            base_url,
            auth: config.auth.clone(),
            oidc_token: tokio::sync::RwLock::new(None),
            rate_limiter: RateLimiter::new(conn.rate_limit_rps),
            max_retries: conn.max_retries,
            retry_backoff_ms: conn.retry_backoff_ms,
        })
    }

    // ========================================================================
    // Groups API
    // ========================================================================

    /// List groups (paginated).
    pub async fn list_groups(
        &self,
        limit: i32,
        offset: i32,
    ) -> Result<GroupSearchResults, ApicurioRegistryError> {
        let path = format!("/groups?limit={}&offset={}", limit, offset);
        self.get_json(&path).await
    }

    /// Get a single group's metadata.
    pub async fn get_group(&self, group_id: &str) -> Result<GroupInfo, ApicurioRegistryError> {
        let path = format!("/groups/{}", urlencoding::encode(group_id));
        match self.get_json(&path).await {
            Err(ApicurioRegistryError::GroupNotFound(_)) if group_id == "default" => {
                Ok(GroupInfo {
                    group_id: "default".into(),
                    description: None,
                    labels: Default::default(),
                    created_on: None,
                    modified_on: None,
                    extra: Default::default(),
                })
            }
            result => result,
        }
    }

    // ========================================================================
    // Artifacts API
    // ========================================================================

    /// List artifacts in a group (paginated).
    pub async fn list_artifacts(
        &self,
        group_id: &str,
        limit: i32,
        offset: i32,
    ) -> Result<ArtifactSearchResults, ApicurioRegistryError> {
        let path = format!(
            "/groups/{}/artifacts?limit={}&offset={}",
            urlencoding::encode(group_id),
            limit,
            offset
        );
        match self.get_json(&path).await {
            Err(ApicurioRegistryError::GroupNotFound(_)) if group_id == "default" => {
                Ok(ArtifactSearchResults {
                    count: 0,
                    artifacts: vec![],
                })
            }
            result => result,
        }
    }

    /// Get artifact metadata.
    pub async fn get_artifact_metadata(
        &self,
        group_id: &str,
        artifact_id: &str,
    ) -> Result<ArtifactInfo, ApicurioRegistryError> {
        let path = format!(
            "/groups/{}/artifacts/{}",
            urlencoding::encode(group_id),
            urlencoding::encode(artifact_id)
        );
        let path = if self.api_version == ApicurioApiVersion::V2 {
            format!("{path}/meta")
        } else {
            path
        };
        match self.get_json(&path).await {
            Ok(metadata) => Ok(metadata),
            Err(e)
                if self.api_version == ApicurioApiVersion::V2
                    && matches!(
                        e,
                        ApicurioRegistryError::ArtifactNotFound { .. }
                            | ApicurioRegistryError::GroupNotFound(_)
                            | ApicurioRegistryError::Api { status: 404, .. }
                    ) =>
            {
                // v2 artifact metadata is the latest version envelope. The
                // artifact-level endpoint hides a disabled latest version,
                // while the exact version metadata endpoint remains readable.
                let mut offset = 0;
                let mut latest = None;
                loop {
                    let page = self
                        .list_versions(group_id, artifact_id, 100, offset)
                        .await?;
                    let count = page.versions.len();
                    for version in page.versions {
                        if latest
                            .as_ref()
                            .is_none_or(|old: &VersionInfo| old.global_id < version.global_id)
                        {
                            latest = Some(version);
                        }
                    }
                    if count == 0 || i64::from(offset + 100) >= page.count {
                        break;
                    }
                    offset += 100;
                }
                if let Some(latest) = latest {
                    self.get_json(&format!(
                        "/groups/{}/artifacts/{}/versions/{}/meta",
                        urlencoding::encode(group_id),
                        urlencoding::encode(artifact_id),
                        urlencoding::encode(&latest.version)
                    ))
                    .await
                } else {
                    Err(e)
                }
            }
            Err(e) => Err(e),
        }
    }

    // ========================================================================
    // Versions API
    // ========================================================================

    /// List versions for an artifact (paginated).
    pub async fn list_versions(
        &self,
        group_id: &str,
        artifact_id: &str,
        limit: i32,
        offset: i32,
    ) -> Result<VersionSearchResults, ApicurioRegistryError> {
        let path = format!(
            "/groups/{}/artifacts/{}/versions?limit={}&offset={}",
            urlencoding::encode(group_id),
            urlencoding::encode(artifact_id),
            limit,
            offset
        );
        self.get_json(&path).await
    }

    /// Get version metadata.
    pub async fn get_version_metadata(
        &self,
        group_id: &str,
        artifact_id: &str,
        version: &str,
    ) -> Result<VersionInfo, ApicurioRegistryError> {
        let path = format!(
            "/groups/{}/artifacts/{}/versions/{}",
            urlencoding::encode(group_id),
            urlencoding::encode(artifact_id),
            urlencoding::encode(version)
        );
        let path = if self.api_version == ApicurioApiVersion::V2 {
            format!("{path}/meta")
        } else {
            path
        };
        self.get_json(&path).await
    }

    /// Get version content (raw schema bytes).
    pub async fn get_version_content(
        &self,
        group_id: &str,
        artifact_id: &str,
        version: &str,
    ) -> Result<Bytes, ApicurioRegistryError> {
        let path = format!(
            "/groups/{}/artifacts/{}/versions/{}/content",
            urlencoding::encode(group_id),
            urlencoding::encode(artifact_id),
            urlencoding::encode(version)
        );
        let path = if self.api_version == ApicurioApiVersion::V2 {
            path.trim_end_matches("/content").to_owned()
        } else {
            path
        };
        self.get_bytes(&path).await
    }

    /// Content-addressed retrieval also works for disabled versions, whose
    /// coordinate content endpoint intentionally behaves as absent.
    pub async fn get_content_by_id(&self, content_id: i64) -> Result<Bytes, ApicurioRegistryError> {
        self.get_bytes(&format!("/ids/contentIds/{content_id}"))
            .await
    }

    /// Get version references (cross-artifact dependencies).
    pub async fn get_version_references(
        &self,
        group_id: &str,
        artifact_id: &str,
        version: &str,
    ) -> Result<Vec<ArtifactReference>, ApicurioRegistryError> {
        let path = format!(
            "/groups/{}/artifacts/{}/versions/{}/references",
            urlencoding::encode(group_id),
            urlencoding::encode(artifact_id),
            urlencoding::encode(version)
        );
        self.get_json(&path).await
    }

    // ========================================================================
    // Admin Export/Import API
    // ========================================================================

    /// Export all registry data as a ZIP archive.
    pub async fn export_zip(&self) -> Result<Bytes, ApicurioRegistryError> {
        self.get_bytes("/admin/export").await
    }

    // ========================================================================
    // Rules API (Global)
    // ========================================================================

    /// List global rule types.
    pub async fn list_global_rules(&self) -> Result<Vec<String>, ApicurioRegistryError> {
        self.get_json("/admin/rules").await
    }

    /// Get a global rule configuration.
    pub async fn get_global_rule(
        &self,
        rule_type: &str,
    ) -> Result<RuleConfig, ApicurioRegistryError> {
        let path = format!("/admin/rules/{}", rule_type);
        self.get_json(&path).await
    }

    // ========================================================================
    // Rules API (Group)
    // ========================================================================

    /// List group-level rule types.
    pub async fn list_group_rules(
        &self,
        group_id: &str,
    ) -> Result<Vec<String>, ApicurioRegistryError> {
        if self.api_version == ApicurioApiVersion::V2 {
            return Ok(vec![]);
        }
        let path = format!("/groups/{}/rules", urlencoding::encode(group_id));
        match self.get_json(&path).await {
            Err(ApicurioRegistryError::GroupNotFound(_)) if group_id == "default" => Ok(vec![]),
            result => result,
        }
    }

    /// Get a group-level rule configuration.
    pub async fn get_group_rule(
        &self,
        group_id: &str,
        rule_type: &str,
    ) -> Result<RuleConfig, ApicurioRegistryError> {
        let path = format!(
            "/groups/{}/rules/{}",
            urlencoding::encode(group_id),
            rule_type
        );
        self.get_json(&path).await
    }

    // ========================================================================
    // Rules API (Artifact)
    // ========================================================================

    /// List artifact-level rule types.
    pub async fn list_artifact_rules(
        &self,
        group_id: &str,
        artifact_id: &str,
    ) -> Result<Vec<String>, ApicurioRegistryError> {
        let path = format!(
            "/groups/{}/artifacts/{}/rules",
            urlencoding::encode(group_id),
            urlencoding::encode(artifact_id)
        );
        self.get_json(&path).await
    }

    /// Get an artifact-level rule configuration.
    pub async fn get_artifact_rule(
        &self,
        group_id: &str,
        artifact_id: &str,
        rule_type: &str,
    ) -> Result<RuleConfig, ApicurioRegistryError> {
        let path = format!(
            "/groups/{}/artifacts/{}/rules/{}",
            urlencoding::encode(group_id),
            urlencoding::encode(artifact_id),
            rule_type
        );
        self.get_json(&path).await
    }

    pub fn api_version(&self) -> ApicurioApiVersion {
        self.api_version
    }

    pub async fn server_version(&self) -> Result<Option<String>, ApicurioRegistryError> {
        match self.get_json::<serde_json::Value>("/system/info").await {
            Ok(info) => Ok(info["version"].as_str().map(str::to_owned)),
            Err(
                ApicurioRegistryError::Api { status: 404, .. }
                | ApicurioRegistryError::GroupNotFound(_),
            ) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// One-shot mutations: uncertain writes must be reconciled by the engine.
    pub async fn mutate(
        &self,
        method: Method,
        path: &str,
        content_type: &str,
        body: Bytes,
        headers: &[(String, String)],
    ) -> Result<serde_json::Value, ApicurioRegistryError> {
        let body = if self.api_version == ApicurioApiVersion::V2
            && path.contains("/rules")
            && !body.is_empty()
        {
            let mut value: serde_json::Value = serde_json::from_slice(&body)
                .map_err(|e| ApicurioRegistryError::Serialization(e.to_string()))?;
            if let Some(map) = value.as_object_mut() {
                if let Some(typ) = map.remove("ruleType") {
                    map.insert("type".into(), typ);
                }
            }
            Bytes::from(
                serde_json::to_vec(&value)
                    .map_err(|e| ApicurioRegistryError::Serialization(e.to_string()))?,
            )
        } else {
            body
        };
        self.rate_limiter.acquire().await;
        let mut req = self
            .apply_auth(
                self.client
                    .request(method, format!("{}{}", self.base_url, path)),
            )
            .await?;
        req = req.header("Content-Type", content_type).body(body);
        for (name, value) in headers {
            req = req.header(name, value);
        }
        let resp = req.send().await?;
        let status = resp.status();
        let data = resp.bytes().await?;
        if !status.is_success() {
            return Err(self.map_error(status, path, &String::from_utf8_lossy(&data)));
        }
        if data.is_empty() {
            return Ok(serde_json::Value::Null);
        }
        let mut value = serde_json::from_slice(&data)
            .map_err(|e| ApicurioRegistryError::Serialization(e.to_string()))?;
        normalize_common(&mut value);
        if self.api_version == ApicurioApiVersion::V2 {
            normalize_v2(&mut value);
        }
        Ok(value)
    }

    // ========================================================================
    // Internal Request Methods
    // ========================================================================

    pub async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<T, ApicurioRegistryError> {
        self.request_json(Method::GET, path, Option::<&()>::None)
            .await
    }

    async fn get_bytes(&self, path: &str) -> Result<Bytes, ApicurioRegistryError> {
        let url = format!("{}{}", self.base_url, path);
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                let base_ms = self.retry_backoff_ms * 2u64.pow(attempt - 1);
                let jitter_ms = rand::random::<u64>() % (base_ms / 2).max(1);
                let backoff = Duration::from_millis(base_ms + jitter_ms);
                debug!(
                    attempt = attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    "Retrying Apicurio Registry request"
                );
                tokio::time::sleep(backoff).await;
            }

            self.rate_limiter.acquire().await;

            let mut req = self.client.get(&url);
            req = self.apply_auth(req).await?;

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        match resp.bytes().await {
                            Ok(data) => return Ok(data),
                            Err(error) if interrupted_response_body(&error) => {
                                last_error = Some(ApicurioRegistryError::Http(error));
                                continue;
                            }
                            Err(error) => return Err(ApicurioRegistryError::Http(error)),
                        }
                    }

                    let body_text = resp.text().await.unwrap_or_default();
                    let err = self.map_error(status, path, &body_text);

                    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                        last_error = Some(err);
                        continue;
                    }

                    return Err(err);
                }
                Err(e) if e.is_connect() || e.is_timeout() || e.is_request() => {
                    warn!(error = %e, attempt = attempt, "Apicurio Registry connection error, will retry");
                    last_error = Some(ApicurioRegistryError::Http(e));
                    continue;
                }
                Err(e) => return Err(ApicurioRegistryError::Http(e)),
            }
        }

        Err(
            last_error.unwrap_or_else(|| ApicurioRegistryError::RateLimitExhausted {
                attempts: self.max_retries + 1,
            }),
        )
    }

    async fn request_json<T, B>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T, ApicurioRegistryError>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize,
    {
        let url = format!("{}{}", self.base_url, path);
        let mut last_error = None;

        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                let base_ms = self.retry_backoff_ms * 2u64.pow(attempt - 1);
                let jitter_ms = rand::random::<u64>() % (base_ms / 2).max(1);
                let backoff = Duration::from_millis(base_ms + jitter_ms);
                debug!(
                    attempt = attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    "Retrying Apicurio Registry request"
                );
                tokio::time::sleep(backoff).await;
            }

            self.rate_limiter.acquire().await;

            let mut req = self
                .client
                .request(method.clone(), &url)
                .header("Accept", "application/json");

            req = self.apply_auth(req).await?;

            if let Some(b) = body {
                req = req.header("Content-Type", "application/json").json(b);
            }

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();

                    if status.is_success() {
                        let mut value: serde_json::Value = match resp.json().await {
                            Ok(value) => value,
                            Err(error)
                                if method == Method::GET && interrupted_response_body(&error) =>
                            {
                                last_error = Some(ApicurioRegistryError::Http(error));
                                continue;
                            }
                            Err(error) => return Err(ApicurioRegistryError::Http(error)),
                        };
                        normalize_common(&mut value);
                        if self.api_version == ApicurioApiVersion::V2 {
                            normalize_v2(&mut value);
                        }
                        return serde_json::from_value::<T>(value).map_err(|e| {
                            ApicurioRegistryError::Serialization(format!(
                                "Failed to deserialize response from {}: {}",
                                path, e
                            ))
                        });
                    }

                    let body_text = resp.text().await.unwrap_or_default();
                    let err = self.map_error(status, path, &body_text);

                    // 429 or 5xx → Retryable
                    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                        last_error = Some(err);
                        continue;
                    }

                    return Err(err);
                }
                Err(e) if e.is_connect() || e.is_timeout() || e.is_request() => {
                    warn!(error = %e, attempt = attempt, "Apicurio Registry connection error, will retry");
                    last_error = Some(ApicurioRegistryError::Http(e));
                    continue;
                }
                Err(e) => return Err(ApicurioRegistryError::Http(e)),
            }
        }

        Err(
            last_error.unwrap_or_else(|| ApicurioRegistryError::RateLimitExhausted {
                attempts: self.max_retries + 1,
            }),
        )
    }

    fn map_error(&self, status: StatusCode, path: &str, body_text: &str) -> ApicurioRegistryError {
        let api_error = serde_json::from_str::<ApicurioApiError>(body_text).ok();
        let error_code = api_error.as_ref().and_then(|e| e.error_code);
        let message = api_error
            .as_ref()
            .and_then(|e| e.message.clone())
            .unwrap_or_else(|| body_text.to_string());

        match status {
            StatusCode::NOT_FOUND => {
                ApicurioRegistryError::GroupNotFound(format!("{}: {}", path, message))
            }
            StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
                ApicurioRegistryError::Auth(message)
            }
            _ => ApicurioRegistryError::Api {
                status: status.as_u16(),
                error_code,
                message,
            },
        }
    }

    async fn apply_auth(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, ApicurioRegistryError> {
        match &self.auth {
            Some(ApicurioRegistryAuth::Basic { username, password }) => {
                Ok(req.basic_auth(username, Some(password.expose())))
            }
            Some(ApicurioRegistryAuth::Oidc {
                token_url,
                client_id,
                client_secret,
                scope,
            }) => {
                let token = self
                    .get_or_refresh_oidc_token(
                        token_url,
                        client_id,
                        client_secret.expose(),
                        scope.as_deref(),
                    )
                    .await?;
                Ok(req.bearer_auth(token))
            }
            Some(ApicurioRegistryAuth::MutualTls { .. }) => {
                // mTLS is configured at the client level (identity), not per-request
                Ok(req)
            }
            Some(ApicurioRegistryAuth::None) | None => Ok(req),
        }
    }

    async fn get_or_refresh_oidc_token(
        &self,
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        scope: Option<&str>,
    ) -> Result<String, ApicurioRegistryError> {
        // Check if we have a valid cached token
        {
            let token_guard = self.oidc_token.read().await;
            if let Some(token) = token_guard.as_ref() {
                // Refresh 60 seconds before expiry
                if token.expires_at > tokio::time::Instant::now() + Duration::from_secs(60) {
                    return Ok(token.access_token.clone());
                }
            }
        }

        // Exchange client credentials for token
        let mut params = vec![
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
        ];
        if let Some(s) = scope {
            params.push(("scope", s));
        }

        let resp = self
            .client
            .post(token_url)
            .form(&params)
            .send()
            .await
            .map_err(|e| {
                ApicurioRegistryError::OidcTokenFailed(format!(
                    "Failed to reach token endpoint {}: {}",
                    token_url, e
                ))
            })?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(ApicurioRegistryError::OidcTokenFailed(format!(
                "Token endpoint returned error: {}",
                body
            )));
        }

        let token_response: serde_json::Value = resp.json().await.map_err(|e| {
            ApicurioRegistryError::OidcTokenFailed(format!("Invalid token response: {}", e))
        })?;

        let access_token = token_response["access_token"]
            .as_str()
            .ok_or_else(|| {
                ApicurioRegistryError::OidcTokenFailed(
                    "No access_token in token response".to_string(),
                )
            })?
            .to_string();

        let expires_in = token_response["expires_in"].as_u64().unwrap_or(300);
        let expires_at = tokio::time::Instant::now() + Duration::from_secs(expires_in);

        // Cache the token
        {
            let mut token_guard = self.oidc_token.write().await;
            *token_guard = Some(OidcToken {
                access_token: access_token.clone(),
                expires_at,
            });
        }

        Ok(access_token)
    }
}

fn normalize_common(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for v in values {
                normalize_common(v);
            }
        }
        serde_json::Value::Object(map) => {
            if map.get("labels").is_some_and(serde_json::Value::is_null) {
                map.insert("labels".into(), serde_json::json!({}));
            }
            if map.get("groupId").is_some_and(serde_json::Value::is_null) {
                map.insert("groupId".into(), serde_json::json!("default"));
            }
            for v in map.values_mut() {
                normalize_common(v);
            }
        }
        _ => {}
    }
}

/// Adapt v2 names while retaining fields that have no v3 equivalent.
fn normalize_v2(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Array(values) => {
            for v in values {
                normalize_v2(v);
            }
        }
        serde_json::Value::Object(map) => {
            if !map.contains_key("config")
                && (map.contains_key("id") || map.contains_key("globalId"))
            {
                if let Some(t) = map.remove("type") {
                    map.insert("artifactType".into(), t);
                }
            }
            if let Some(id) = map.remove("id") {
                map.insert(
                    if map.contains_key("artifactType") {
                        "artifactId"
                    } else {
                        "groupId"
                    }
                    .into(),
                    id,
                );
            }
            if map.get("labels").is_some_and(serde_json::Value::is_array) {
                let labels = map.remove("labels").unwrap();
                map.insert("legacyLabels".into(), labels);
            }
            for (k, v) in map.iter_mut() {
                if !matches!(k.as_str(), "labels" | "properties" | "legacyLabels") {
                    normalize_v2(v);
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_encoding_group_and_artifact_ids() {
        let group = "my.group";
        let encoded = urlencoding::encode(group);
        // Dots are not encoded
        assert_eq!(encoded, "my.group");

        let group_with_special = "com.example/orders:v2";
        let encoded = urlencoding::encode(group_with_special);
        assert!(encoded.contains("%2F"));
        assert!(encoded.contains("%3A"));
    }

    #[test]
    fn test_base_url_construction_without_api_path() {
        // Simulate the URL logic from new()
        let url = "https://apicurio:8080";
        let trimmed = url.trim_end_matches('/');
        let base_url = if trimmed.ends_with("/apis/registry/v3") {
            trimmed.to_string()
        } else {
            format!("{}/apis/registry/v3", trimmed)
        };
        assert_eq!(base_url, "https://apicurio:8080/apis/registry/v3");
    }

    #[test]
    fn test_base_url_construction_with_api_path() {
        let url = "https://apicurio:8080/apis/registry/v3";
        let trimmed = url.trim_end_matches('/');
        let base_url = if trimmed.ends_with("/apis/registry/v3") {
            trimmed.to_string()
        } else {
            format!("{}/apis/registry/v3", trimmed)
        };
        assert_eq!(base_url, "https://apicurio:8080/apis/registry/v3");
    }

    #[test]
    fn test_base_url_construction_trailing_slash() {
        let url = "https://apicurio:8080/";
        let trimmed = url.trim_end_matches('/');
        let base_url = if trimmed.ends_with("/apis/registry/v3") {
            trimmed.to_string()
        } else {
            format!("{}/apis/registry/v3", trimmed)
        };
        assert_eq!(base_url, "https://apicurio:8080/apis/registry/v3");
    }

    #[tokio::test]
    async fn test_rate_limiter_enforces_interval() {
        let limiter = RateLimiter::new(100); // 100 RPS = 10ms between requests
        let start = tokio::time::Instant::now();

        for _ in 0..5 {
            limiter.acquire().await;
        }

        let elapsed = start.elapsed();
        // 5 requests at 100 RPS should take at least ~40ms (4 intervals of 10ms)
        assert!(
            elapsed >= Duration::from_millis(30),
            "Expected >= 30ms, got {:?}",
            elapsed
        );
    }
}
