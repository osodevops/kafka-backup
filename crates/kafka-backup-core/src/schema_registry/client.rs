use std::time::Duration;

use reqwest::{Client, Method, StatusCode};
use tracing::{debug, warn};

use crate::oauth::RegistryOauth;

use super::config::{SchemaRegistryAuth, SchemaRegistryConfig};
use super::error::SchemaRegistryError;
use super::models::*;

/// Confluent Schema Registry REST API client.
///
/// Handles authentication, rate limiting, and retries transparently.
pub struct SchemaRegistryClient {
    client: Client,
    base_url: String,
    auth: Option<SchemaRegistryAuth>,
    /// Bearer-token auth from the shared OAuth token cache.
    oauth: Option<RegistryOauth>,
    rate_limiter: RateLimiter,
    max_retries: u32,
    retry_backoff_ms: u64,
}

/// Simple token-bucket rate limiter using tokio::time.
///
/// Ensures at least `min_interval` between consecutive requests.
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

impl SchemaRegistryClient {
    /// Create a new Schema Registry client from configuration.
    pub fn new(config: &SchemaRegistryConfig) -> Result<Self, SchemaRegistryError> {
        let conn = config.connection.clone().unwrap_or_default();

        let mut builder = Client::builder()
            .use_rustls_tls()
            .timeout(Duration::from_millis(conn.timeout_ms))
            .user_agent(concat!("kafka-backup/", env!("CARGO_PKG_VERSION")));

        // Server CA certificate
        if let Some(ca_path) = config.tls.as_ref().and_then(|tls| tls.ca_cert.as_ref()) {
            builder = builder.add_root_certificate(read_ca(ca_path)?);
        }

        // mTLS client identity
        if let Some(SchemaRegistryAuth::MutualTls {
            ca_cert,
            client_cert,
            client_key,
        }) = &config.auth
        {
            if let Some(ca_path) = ca_cert {
                builder = builder.add_root_certificate(read_ca(ca_path)?);
            }
            let mut pem = std::fs::read(client_cert).map_err(|e| {
                SchemaRegistryError::Config(format!(
                    "Failed to read client cert {client_cert}: {e}"
                ))
            })?;
            let key_bytes = std::fs::read(client_key).map_err(|e| {
                SchemaRegistryError::Config(format!("Failed to read client key {client_key}: {e}"))
            })?;
            pem.push(b'\n');
            pem.extend_from_slice(&key_bytes);
            let identity = reqwest::Identity::from_pem(&pem).map_err(|e| {
                SchemaRegistryError::Config(format!("Invalid client identity: {e}"))
            })?;
            builder = builder.identity(identity);
        }

        let client = builder.build().map_err(|e| {
            SchemaRegistryError::Config(format!("Failed to build HTTP client: {e}"))
        })?;

        // Bearer tokens come from the cache `oauth::install` attached: the
        // one every Kafka cluster bound to the provider uses.
        let oauth = match &config.auth {
            Some(SchemaRegistryAuth::OAuth(oauth)) => {
                let cache = oauth.token_source.clone().ok_or_else(|| {
                    SchemaRegistryError::Config(format!(
                        "auth uses OAuth provider '{}', which was not set up: call \
                         kafka_backup_core::oauth::install on the configuration before \
                         building a Schema Registry client",
                        oauth.provider
                    ))
                })?;
                Some(RegistryOauth::new(
                    cache,
                    oauth.target_sr_cluster.as_deref(),
                    oauth.identity_pool_id.as_deref(),
                ))
            }
            _ => None,
        };

        Ok(Self {
            client,
            base_url: config.url.trim_end_matches('/').to_string(),
            auth: config.auth.clone(),
            oauth,
            rate_limiter: RateLimiter::new(conn.rate_limit_rps),
            max_retries: conn.max_retries,
            retry_backoff_ms: conn.retry_backoff_ms,
        })
    }

    /// The registry URL this client talks to (no trailing slash).
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    // ========================================================================
    // Public API Methods
    // ========================================================================

    /// List all subjects. If `include_deleted` is true, includes soft-deleted subjects.
    pub async fn list_subjects(
        &self,
        include_deleted: bool,
    ) -> Result<Vec<String>, SchemaRegistryError> {
        let path = if include_deleted {
            "/subjects?deleted=true".to_string()
        } else {
            "/subjects".to_string()
        };
        self.get_json(&path).await
    }

    /// List all versions for a subject.
    pub async fn list_versions(
        &self,
        subject: &str,
        include_deleted: bool,
    ) -> Result<Vec<i32>, SchemaRegistryError> {
        let encoded = urlencoding::encode(subject);
        let path = if include_deleted {
            format!("/subjects/{}/versions?deleted=true", encoded)
        } else {
            format!("/subjects/{}/versions", encoded)
        };
        self.get_json(&path).await
    }

    /// Get a schema version, including a soft-deleted one when
    /// `include_deleted` is set (`?deleted=true`).
    pub async fn get_schema_version_with(
        &self,
        subject: &str,
        version: i32,
        include_deleted: bool,
    ) -> Result<SchemaVersion, SchemaRegistryError> {
        let mut path = format!(
            "/subjects/{}/versions/{}",
            urlencoding::encode(subject),
            version
        );
        if include_deleted {
            path.push_str("?deleted=true");
        }
        self.get_json(&path).await
    }

    /// [`Self::get_schema_version_with`], `None` when the subject or version
    /// does not exist (404).
    pub async fn find_schema_version(
        &self,
        subject: &str,
        version: i32,
        include_deleted: bool,
    ) -> Result<Option<SchemaVersion>, SchemaRegistryError> {
        not_found_as_none(
            self.get_schema_version_with(subject, version, include_deleted)
                .await,
        )
    }

    /// [`Self::list_versions`], `None` when the subject does not exist (404).
    pub async fn find_versions(
        &self,
        subject: &str,
        include_deleted: bool,
    ) -> Result<Option<Vec<i32>>, SchemaRegistryError> {
        not_found_as_none(self.list_versions(subject, include_deleted).await)
    }

    /// Look up a schema ID. `context_subject` selects a non-default context
    /// (`:.ctx-a:`); `None` is the default context. `None` when the ID is not
    /// in use (404). Soft-deleted schemas are found too: their IDs stay taken.
    pub async fn find_schema_by_id(
        &self,
        id: i32,
        context_subject: Option<&str>,
    ) -> Result<Option<SchemaById>, SchemaRegistryError> {
        let path = match context_subject {
            Some(subject) => format!(
                "/schemas/ids/{}?subject={}",
                id,
                urlencoding::encode(subject)
            ),
            None => format!("/schemas/ids/{}", id),
        };
        not_found_as_none(self.get_json(&path).await)
    }

    /// Check whether `subject` already holds the schema in `request`
    /// (`POST /subjects/{subject}`). `None` when it does not (404).
    pub async fn lookup_schema(
        &self,
        subject: &str,
        request: &RegisterSchemaRequest,
    ) -> Result<Option<SchemaVersion>, SchemaRegistryError> {
        let lookup = RegisterSchemaRequest {
            id: None,
            version: None,
            ..request.clone()
        };
        not_found_as_none(
            self.request_json(
                Method::POST,
                &format!("/subjects/{}", urlencoding::encode(subject)),
                Some(&lookup),
            )
            .await,
        )
    }

    /// Number of schema versions in the registry, soft-deleted included
    /// (`GET /schemas?deleted=true`). Confluent Cloud counts these against
    /// the Stream Governance package limit.
    pub async fn count_schema_versions(&self) -> Result<u64, SchemaRegistryError> {
        let all: Vec<serde::de::IgnoredAny> = self.get_json("/schemas?deleted=true").await?;
        Ok(all.len() as u64)
    }

    /// The registry's version (`/v1/metadata/version`), when it reports one.
    pub async fn server_version(&self) -> Result<Option<String>, SchemaRegistryError> {
        match self
            .get_json::<serde_json::Value>("/v1/metadata/version")
            .await
        {
            Ok(v) => Ok(v["version"].as_str().map(str::to_owned)),
            Err(SchemaRegistryError::SubjectNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }
    /// Get the global compatibility level.
    pub async fn get_global_config(&self) -> Result<CompatibilityConfig, SchemaRegistryError> {
        self.get_json("/config").await
    }

    /// Get per-subject compatibility level. Returns None if no override is set (404).
    pub async fn get_subject_config(
        &self,
        subject: &str,
    ) -> Result<Option<CompatibilityConfig>, SchemaRegistryError> {
        let path = format!("/config/{}", urlencoding::encode(subject));
        match self.get_json::<CompatibilityConfig>(&path).await {
            Ok(config) => Ok(Some(config)),
            Err(SchemaRegistryError::SubjectNotFound(_)) => Ok(None),
            Err(SchemaRegistryError::Api { status: 404, .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get the global mode. Returns None if mode endpoint is not available.
    pub async fn get_global_mode(&self) -> Result<Option<ModeConfig>, SchemaRegistryError> {
        match self.get_json::<ModeConfig>("/mode").await {
            Ok(mode) => Ok(Some(mode)),
            Err(SchemaRegistryError::Api { status: 404, .. })
            | Err(SchemaRegistryError::SubjectNotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Get per-subject mode. Returns None if no override is set (404).
    pub async fn get_subject_mode(
        &self,
        subject: &str,
    ) -> Result<Option<ModeConfig>, SchemaRegistryError> {
        let path = format!("/mode/{}", urlencoding::encode(subject));
        match self.get_json::<ModeConfig>(&path).await {
            Ok(mode) => Ok(Some(mode)),
            Err(SchemaRegistryError::SubjectNotFound(_)) => Ok(None),
            Err(SchemaRegistryError::Api { status: 404, .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub async fn set_global_config(
        &self,
        compatibility: &str,
    ) -> Result<CompatibilityConfig, SchemaRegistryError> {
        self.request_json(
            Method::PUT,
            "/config",
            Some(&ConfigUpdateRequest {
                compatibility: compatibility.to_string(),
            }),
        )
        .await
    }

    pub async fn set_subject_config(
        &self,
        subject: &str,
        compatibility: &str,
    ) -> Result<CompatibilityConfig, SchemaRegistryError> {
        self.request_json(
            Method::PUT,
            &format!("/config/{}", urlencoding::encode(subject)),
            Some(&ConfigUpdateRequest {
                compatibility: compatibility.to_string(),
            }),
        )
        .await
    }

    pub async fn set_global_mode(&self, mode: &str) -> Result<ModeConfig, SchemaRegistryError> {
        self.request_json(
            Method::PUT,
            "/mode",
            Some(&ModeUpdateRequest {
                mode: mode.to_string(),
            }),
        )
        .await
    }

    /// Set a subject-level (or, with `:.ctx:`, context-level) mode. `force`
    /// allows IMPORT on a subject that already has schemas.
    pub async fn set_subject_mode(
        &self,
        subject: &str,
        mode: &str,
        force: bool,
    ) -> Result<ModeConfig, SchemaRegistryError> {
        let mut path = format!("/mode/{}", urlencoding::encode(subject));
        if force {
            path.push_str("?force=true");
        }
        self.request_json(
            Method::PUT,
            &path,
            Some(&ModeUpdateRequest {
                mode: mode.to_string(),
            }),
        )
        .await
    }

    /// Remove a subject-level (or context-level) mode so the subject falls
    /// back to its context or the global mode. Absent already is fine.
    pub async fn delete_subject_mode(&self, subject: &str) -> Result<(), SchemaRegistryError> {
        let path = format!("/mode/{}", urlencoding::encode(subject));
        not_found_as_none(
            self.request_json::<serde::de::IgnoredAny, ()>(Method::DELETE, &path, None)
                .await,
        )
        .map(|_| ())
    }

    /// Delete one version (`permanent` requires a soft delete first).
    pub async fn delete_schema_version(
        &self,
        subject: &str,
        version: i32,
        permanent: bool,
    ) -> Result<i32, SchemaRegistryError> {
        let mut path = format!(
            "/subjects/{}/versions/{}",
            urlencoding::encode(subject),
            version
        );
        if permanent {
            path.push_str("?permanent=true");
        }
        self.request_json(Method::DELETE, &path, Option::<&()>::None)
            .await
    }

    pub async fn register_schema(
        &self,
        subject: &str,
        request: &RegisterSchemaRequest,
    ) -> Result<RegisterSchemaResponse, SchemaRegistryError> {
        self.request_json(
            Method::POST,
            &format!("/subjects/{}/versions", urlencoding::encode(subject)),
            Some(request),
        )
        .await
    }

    pub async fn delete_subject(
        &self,
        subject: &str,
        permanent: bool,
    ) -> Result<Vec<i32>, SchemaRegistryError> {
        let suffix = if permanent { "?permanent=true" } else { "" };
        let path = format!("/subjects/{}{}", urlencoding::encode(subject), suffix);
        match self
            .request_json(Method::DELETE, &path, Option::<&()>::None)
            .await
        {
            Ok(versions) => Ok(versions),
            Err(SchemaRegistryError::SubjectNotFound(_)) => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    /// Confluent requires a soft delete before a permanent delete.
    pub async fn delete_subject_permanently(
        &self,
        subject: &str,
    ) -> Result<Vec<i32>, SchemaRegistryError> {
        let _ = self.delete_subject(subject, false).await?;
        self.delete_subject(subject, true).await
    }

    /// Whether the registry can delete a mode override (`DELETE
    /// /mode/{subject}`), asked with an `OPTIONS` request. `Some(false)` only
    /// on a definite answer (an `Allow` header without `DELETE`, as from
    /// Confluent Platform 6.2); `None` when the registry does not say.
    pub async fn supports_mode_delete(
        &self,
        subject: &str,
    ) -> Result<Option<bool>, SchemaRegistryError> {
        self.rate_limiter.acquire().await;
        let path = format!("/mode/{}", urlencoding::encode(subject));
        let request = self
            .apply_auth(
                self.client
                    .request(Method::OPTIONS, format!("{}{}", self.base_url, path)),
            )
            .await?;
        let response = match request.send().await {
            Ok(response) => response,
            Err(e) => {
                debug!(error = %e, "OPTIONS on the mode resource failed; assuming mode overrides can be deleted");
                return Ok(None);
            }
        };
        if !response.status().is_success() {
            return Ok(None);
        }
        Ok(response
            .headers()
            .get(reqwest::header::ALLOW)
            .and_then(|v| v.to_str().ok())
            .map(|allow| {
                allow
                    .split(',')
                    .any(|m| m.trim().eq_ignore_ascii_case("DELETE"))
            }))
    }

    // ========================================================================
    // Internal Request Methods
    // ========================================================================

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<T, SchemaRegistryError> {
        self.request_json(Method::GET, path, Option::<&()>::None)
            .await
    }

    async fn request_json<T, B>(
        &self,
        method: Method,
        path: &str,
        body: Option<&B>,
    ) -> Result<T, SchemaRegistryError>
    where
        T: serde::de::DeserializeOwned,
        B: serde::Serialize,
    {
        let url = format!("{}{}", self.base_url, path);
        let mut last_error = None;
        let mut token_refreshed = false;
        // A timeout or 5xx can arrive after a registry mutation committed.
        // Only reads may be repeated; restore reconciles uncertain writes.
        let max_retries = if method == Method::GET {
            self.max_retries
        } else {
            0
        };

        for attempt in 0..=max_retries {
            if attempt > 0 {
                let base_ms = self.retry_backoff_ms * 2u64.pow(attempt - 1);
                let jitter_ms = rand::random::<u64>() % (base_ms / 2).max(1);
                let backoff = Duration::from_millis(base_ms + jitter_ms);
                debug!(
                    attempt = attempt,
                    backoff_ms = backoff.as_millis() as u64,
                    "Retrying Schema Registry request"
                );
                tokio::time::sleep(backoff).await;
            }

            self.rate_limiter.acquire().await;

            let mut req = self
                .client
                .request(method.clone(), &url)
                .header("Accept", "application/vnd.schemaregistry.v1+json");

            // Apply auth
            req = self.apply_auth(req).await?;

            // Apply body if present
            if let Some(b) = body {
                req = req
                    .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                    .json(b);
            }

            match req.send().await {
                Ok(resp) => {
                    let status = resp.status();

                    if status.is_success() {
                        return resp.json::<T>().await.map_err(|e| {
                            SchemaRegistryError::Serialization(format!(
                                "Failed to deserialize response from {}: {}",
                                path, e
                            ))
                        });
                    }

                    // Parse error body
                    let body_text = resp.text().await.unwrap_or_default();
                    let sr_error = serde_json::from_str::<SrApiError>(&body_text).ok();
                    let error_code = sr_error.as_ref().map(|e| e.error_code).unwrap_or(0);
                    let message = sr_error
                        .as_ref()
                        .map(|e| e.message.clone())
                        .unwrap_or(body_text);

                    // 404 → SubjectNotFound
                    if status == StatusCode::NOT_FOUND {
                        return Err(SchemaRegistryError::SubjectNotFound(format!(
                            "{}: {}",
                            path, message
                        )));
                    }

                    // 401/403 → Auth
                    // 401 with OAuth: the token may have been revoked or
                    // expired early; drop it and try once more with a new one.
                    if status == StatusCode::UNAUTHORIZED && !token_refreshed {
                        if let Some(oauth) = &self.oauth {
                            token_refreshed = true;
                            if oauth.invalidate() {
                                last_error = Some(SchemaRegistryError::Auth(message));
                                continue;
                            }
                        }
                    }
                    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
                        return Err(SchemaRegistryError::Auth(message));
                    }

                    // 429 or 5xx → Retryable
                    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
                        last_error = Some(SchemaRegistryError::Api {
                            status: status.as_u16(),
                            error_code,
                            message,
                        });
                        continue;
                    }

                    // Other errors are not retryable
                    return Err(SchemaRegistryError::Api {
                        status: status.as_u16(),
                        error_code,
                        message,
                    });
                }
                Err(e)
                    if method == Method::GET
                        && (e.is_connect() || e.is_timeout() || e.is_request()) =>
                {
                    warn!(error = %e, attempt = attempt, "Schema Registry connection error, will retry");
                    last_error = Some(SchemaRegistryError::Http(e));
                    continue;
                }
                Err(e) => return Err(SchemaRegistryError::Http(e)),
            }
        }

        Err(
            last_error.unwrap_or_else(|| SchemaRegistryError::RateLimitExhausted {
                attempts: max_retries + 1,
            }),
        )
    }

    async fn apply_auth(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, SchemaRegistryError> {
        match &self.auth {
            Some(SchemaRegistryAuth::Basic { username, password }) => {
                Ok(req.basic_auth(username, Some(password.expose())))
            }
            Some(SchemaRegistryAuth::OAuth(_)) => match &self.oauth {
                Some(oauth) => oauth.apply(req).await.map_err(|e| {
                    SchemaRegistryError::Auth(format!("OAuth token for Schema Registry: {e}"))
                }),
                None => Err(SchemaRegistryError::Auth("OAuth is not set up".into())),
            },
            Some(SchemaRegistryAuth::MutualTls { .. }) => {
                // mTLS is configured at the client level (identity), not per-request
                Ok(req)
            }
            Some(SchemaRegistryAuth::None) | None => Ok(req),
        }
    }
}

fn read_ca(path: &str) -> Result<reqwest::Certificate, SchemaRegistryError> {
    let bytes = std::fs::read(path)
        .map_err(|e| SchemaRegistryError::Config(format!("Failed to read CA cert {path}: {e}")))?;
    reqwest::Certificate::from_pem(&bytes)
        .map_err(|e| SchemaRegistryError::Config(format!("Invalid CA cert {path}: {e}")))
}

/// Map a 404 (subject, version or schema not found) to `Ok(None)`.
fn not_found_as_none<T>(
    result: Result<T, SchemaRegistryError>,
) -> Result<Option<T>, SchemaRegistryError> {
    match result {
        Ok(value) => Ok(Some(value)),
        Err(SchemaRegistryError::SubjectNotFound(_))
        | Err(SchemaRegistryError::Api { status: 404, .. }) => Ok(None),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_url_encoding_subjects() {
        let subject = "my.topic-value";
        let encoded = urlencoding::encode(subject);
        assert_eq!(encoded, "my.topic-value"); // dots and hyphens are not encoded

        let subject_with_slash = "context:.:/my-subject";
        let encoded = urlencoding::encode(subject_with_slash);
        assert!(encoded.contains("%3A"));
        assert!(encoded.contains("%2F"));
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
