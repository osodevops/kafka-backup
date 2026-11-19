//! The top-level `apicurio_registry` configuration section.
//!
//! ```yaml
//! apicurio_registry:
//!   url: https://apicurio:8080
//!   api_version: v3                  # v3 (default) or v2; never guessed
//!   auth:
//!     type: basic                    # none | basic | mtls | oidc
//!     username: ${APICURIO_USERNAME}
//!     password: ${APICURIO_PASSWORD}
//!   tls:
//!     ca_cert: /etc/ssl/registry-ca.pem
//!   connection:
//!     timeout_ms: 30000
//!     max_retries: 3
//!     retry_backoff_ms: 1000
//!     rate_limit_rps: 50
//!   backup:
//!     groups: ["*"]
//!     exclude_groups: []
//!     artifacts: ["*"]
//!     exclude_artifacts: []
//!     include_versions: all          # or latest
//!     include_references: true
//!     include_export: true           # the registry's own export archive
//!     fail_on_partial: false
//!     snapshot_interval: 15m         # continuous backups only
//! ```
//!
//! In a backup configuration `url` is the registry to snapshot; in a restore
//! configuration it is the registry to restore into. How a restore treats
//! the target is set in the top-level `registry_restore` section
//! ([`crate::registry_restore::RestoreConfig`]). An unknown key anywhere in
//! the section is an error.

use serde::{Deserialize, Serialize};

use crate::oauth::Secret;
use crate::schema_registry::config::VersionSelection;

/// The `apicurio_registry` section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApicurioRegistryConfig {
    /// The registry's API generation: `v3` (default) or `v2` (Apicurio
    /// Registry 2.x). It is never guessed from the endpoint.
    #[serde(default)]
    pub api_version: ApicurioApiVersion,

    /// Set to `false` to keep the section but skip the registry.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Registry base URL, for example `https://apicurio:8080`. The client
    /// appends `/apis/registry/v3` (or `/v2`).
    pub url: String,

    /// How to authenticate. Default: no authentication.
    #[serde(default)]
    pub auth: Option<ApicurioRegistryAuth>,

    /// Server certificate verification.
    #[serde(default)]
    pub tls: Option<ApicurioRegistryTls>,

    /// What a backup captures.
    #[serde(default)]
    pub backup: Option<ApicurioBackupOptions>,

    /// Timeouts, retries and the rate limit.
    #[serde(default)]
    pub connection: Option<ApicurioConnectionConfig>,
}

impl ApicurioRegistryConfig {
    /// A configuration for `url` with every default (API `v3`).
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            api_version: ApicurioApiVersion::default(),
            enabled: true,
            url: url.into(),
            auth: None,
            tls: None,
            backup: None,
            connection: None,
        }
    }

    /// The backup options, or their defaults.
    pub fn backup_options(&self) -> ApicurioBackupOptions {
        self.backup.clone().unwrap_or_default()
    }

    /// Checks that need no network. Run by [`crate::Config::validate`].
    pub fn validate(&self) -> crate::Result<()> {
        let err = |m: String| Err(crate::Error::Config(format!("apicurio_registry: {m}")));
        let url = self.url.trim();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return err(format!(
                "url must start with http:// or https:// (got `{url}`)"
            ));
        }
        match &self.auth {
            Some(ApicurioRegistryAuth::Basic { username, password }) => {
                if username.trim().is_empty() || password.is_empty() {
                    return err("auth type basic needs a non-empty username and password \
                         (is an environment variable unset?)"
                        .to_string());
                }
            }
            Some(ApicurioRegistryAuth::Oidc {
                token_url,
                client_id,
                client_secret,
                ..
            }) => {
                if token_url.trim().is_empty()
                    || client_id.trim().is_empty()
                    || client_secret.is_empty()
                {
                    return err(
                        "auth type oidc needs token_url, client_id and client_secret".to_string(),
                    );
                }
            }
            Some(ApicurioRegistryAuth::MutualTls { .. })
            | Some(ApicurioRegistryAuth::None)
            | None => {}
        }
        if let Some(backup) = &self.backup {
            for pattern in backup
                .groups
                .iter()
                .chain(&backup.exclude_groups)
                .chain(&backup.artifacts)
                .chain(&backup.exclude_artifacts)
            {
                if let Err(e) = glob::Pattern::new(pattern) {
                    return err(format!("backup: invalid pattern `{pattern}`: {e}"));
                }
            }
        }
        if let Some(connection) = &self.connection {
            if connection.timeout_ms == 0 {
                return err("connection.timeout_ms must be greater than zero".to_string());
            }
            if connection.rate_limit_rps == 0 {
                return err("connection.rate_limit_rps must be greater than zero".to_string());
            }
        }
        Ok(())
    }
}

/// `apicurio_registry.api_version`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ApicurioApiVersion {
    V2,
    #[default]
    V3,
}

impl ApicurioApiVersion {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::V2 => "v2",
            Self::V3 => "v3",
        }
    }
}

/// `apicurio_registry.auth`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
#[non_exhaustive]
pub enum ApicurioRegistryAuth {
    /// No authentication.
    None,

    /// HTTP Basic authentication.
    Basic { username: String, password: Secret },

    /// Mutual TLS: a client certificate and key (PEM).
    #[serde(rename = "mtls")]
    MutualTls {
        #[serde(default)]
        ca_cert: Option<String>,
        #[serde(default)]
        client_cert: Option<String>,
        #[serde(default)]
        client_key: Option<String>,
    },

    /// OAuth 2.0 client credentials against an OIDC provider such as
    /// Keycloak; the token is sent as a bearer token.
    #[serde(rename = "oidc")]
    Oidc {
        token_url: String,
        client_id: String,
        client_secret: Secret,
        #[serde(default)]
        scope: Option<String>,
    },
}

/// `apicurio_registry.tls`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApicurioRegistryTls {
    /// Path to a PEM CA certificate that signs the registry's certificate.
    #[serde(default)]
    pub ca_cert: Option<String>,
}

/// `apicurio_registry.backup`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApicurioBackupOptions {
    /// Groups to capture (glob patterns). Default `["*"]`.
    #[serde(default = "default_patterns")]
    pub groups: Vec<String>,

    /// Groups to leave out (glob patterns).
    #[serde(default)]
    pub exclude_groups: Vec<String>,

    /// Artifact IDs to capture within the matched groups (glob patterns).
    #[serde(default = "default_patterns")]
    pub artifacts: Vec<String>,

    /// Artifact IDs to leave out (glob patterns).
    #[serde(default)]
    pub exclude_artifacts: Vec<String>,

    /// Also store the registry's own export archive (`GET /admin/export`).
    /// An ID-preserving restore imports this archive (default true).
    #[serde(default = "default_true")]
    pub include_export: bool,

    /// `all` (default) or `latest`.
    #[serde(default)]
    pub include_versions: VersionSelection,

    /// Also capture referenced artifacts outside the filter (default true).
    #[serde(default = "default_true")]
    pub include_references: bool,

    /// Fail the backup when the snapshot could not be completed. It is
    /// always marked incomplete and refused by a restore; see
    /// [`crate::schema_registry::config::SchemaBackupOptions::fail_on_partial`].
    #[serde(default)]
    pub fail_on_partial: bool,

    /// How often a continuous backup (`backup.continuous: true`) takes a new
    /// snapshot. Humantime duration (`15m`, `1h`); default 15m.
    #[serde(
        default,
        with = "crate::schema_registry::config::humantime_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub snapshot_interval: Option<std::time::Duration>,
}

impl ApicurioBackupOptions {
    /// [`Self::snapshot_interval`], or the default of 15 minutes.
    pub fn snapshot_interval_or_default(&self) -> std::time::Duration {
        self.snapshot_interval
            .unwrap_or(crate::schema_registry::config::DEFAULT_SNAPSHOT_INTERVAL)
    }
}

impl Default for ApicurioBackupOptions {
    fn default() -> Self {
        Self {
            groups: default_patterns(),
            exclude_groups: Vec::new(),
            artifacts: default_patterns(),
            exclude_artifacts: Vec::new(),
            include_export: true,
            include_versions: VersionSelection::default(),
            include_references: true,
            fail_on_partial: false,
            snapshot_interval: None,
        }
    }
}

/// `apicurio_registry.connection`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApicurioConnectionConfig {
    /// Request timeout in milliseconds (default 30000).
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,

    /// Retries of a read after a 429, a 5xx or a connection error
    /// (default 3). Writes are never retried blindly.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// First retry backoff in milliseconds; doubles per attempt (default 1000).
    #[serde(default = "default_retry_backoff_ms")]
    pub retry_backoff_ms: u64,

    /// Maximum requests per second (default 50).
    #[serde(default = "default_rate_limit_rps")]
    pub rate_limit_rps: u32,
}

impl Default for ApicurioConnectionConfig {
    fn default() -> Self {
        Self {
            timeout_ms: default_timeout_ms(),
            max_retries: default_max_retries(),
            retry_backoff_ms: default_retry_backoff_ms(),
            rate_limit_rps: default_rate_limit_rps(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_patterns() -> Vec<String> {
    vec!["*".to_string()]
}

fn default_timeout_ms() -> u64 {
    30_000
}

fn default_max_retries() -> u32 {
    3
}

fn default_retry_backoff_ms() -> u64 {
    1_000
}

fn default_rate_limit_rps() -> u32 {
    50
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<ApicurioRegistryConfig, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[test]
    fn a_bare_url_gets_every_default() {
        let cfg = parse("url: http://apicurio:8080").unwrap();
        cfg.validate().unwrap();
        assert!(cfg.enabled);
        assert_eq!(cfg.api_version, ApicurioApiVersion::V3);
        let backup = cfg.backup_options();
        assert_eq!(backup.groups, ["*"]);
        assert_eq!(backup.artifacts, ["*"]);
        assert!(backup.include_export && backup.include_references && !backup.fail_on_partial);
        assert_eq!(backup.include_versions, VersionSelection::All);
        assert_eq!(
            backup.snapshot_interval_or_default(),
            std::time::Duration::from_secs(900)
        );
        let timed = parse("url: http://a\nbackup: { snapshot_interval: 5m }").unwrap();
        assert_eq!(
            timed.backup_options().snapshot_interval,
            Some(std::time::Duration::from_secs(300))
        );
        assert!(parse("url: http://a\nbackup: { snapshot_interval: 0s }").is_err());
        let conn = ApicurioConnectionConfig::default();
        assert_eq!(
            (
                conn.timeout_ms,
                conn.max_retries,
                conn.retry_backoff_ms,
                conn.rate_limit_rps
            ),
            (30_000, 3, 1_000, 50)
        );
    }

    #[test]
    fn api_version_and_every_auth_type_parse() {
        let v2 = parse("url: http://a\napi_version: v2").unwrap();
        assert_eq!(v2.api_version, ApicurioApiVersion::V2);
        assert_eq!(v2.api_version.as_str(), "v2");
        assert!(parse("url: http://a\napi_version: v4").is_err());
        for (yaml, ok) in [
            ("{ type: none }", true),
            ("{ type: basic, username: u, password: p }", true),
            ("{ type: mtls, client_cert: /c.pem, client_key: /k.pem }", true),
            (
                "{ type: oidc, token_url: https://kc/token, client_id: c, client_secret: s, scope: registry }",
                true,
            ),
            ("{ type: oidc, token_url: https://kc/token }", false),
            ("{ type: kerberos }", false),
        ] {
            let parsed = parse(&format!("url: http://a\nauth: {yaml}"));
            assert_eq!(parsed.is_ok(), ok, "{yaml}");
            if let Ok(cfg) = parsed {
                cfg.validate().unwrap();
            }
        }
    }

    #[test]
    fn unknown_keys_are_errors_at_every_level() {
        for yaml in [
            "url: http://a\nurll: x",
            "url: http://a\nbackup: { groupz: [a] }",
            "url: http://a\nconnection: { concurrent_requests: 4 }",
            "url: http://a\nstorage_backend: sql",
            "url: http://a\nrestore: { use_import_api: true }",
            "url: http://a\ntls: { ca: x }",
            "url: http://a\nauth: { type: basic, username: u, password: p, token: t }",
        ] {
            let err = parse(yaml).unwrap_err().to_string();
            assert!(err.contains("unknown field"), "{yaml}: {err}");
        }
    }

    #[test]
    fn validation_catches_what_parsing_cannot() {
        let bad = |yaml: &str| parse(yaml).unwrap().validate().unwrap_err().to_string();
        assert!(bad("url: apicurio:8080").contains("http://"));
        assert!(
            bad("url: http://a\nauth: { type: basic, username: u, password: '' }")
                .contains("non-empty")
        );
        assert!(bad("url: http://a\nbackup: { groups: ['[a'] }").contains("pattern"));
        assert!(bad("url: http://a\nconnection: { rate_limit_rps: 0 }").contains("rate_limit"));
    }

    #[test]
    fn secrets_are_never_printed_or_serialized() {
        for yaml in [
            "url: http://a\nauth: { type: basic, username: u, password: hunter2-secret }",
            "url: http://a\nauth: { type: oidc, token_url: https://kc/t, client_id: c, client_secret: hunter2-secret }",
        ] {
            let cfg = parse(yaml).unwrap();
            assert!(!format!("{cfg:?}").contains("hunter2"));
            assert!(!serde_yaml::to_string(&cfg).unwrap().contains("hunter2"));
        }
    }
}
