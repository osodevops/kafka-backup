//! The top-level `schema_registry` configuration section.
//!
//! ```yaml
//! schema_registry:
//!   url: https://psrc-xxxxx.westeurope.azure.confluent.cloud
//!   auth:
//!     type: basic                    # none | basic | mtls | oauth
//!     username: ${SR_API_KEY}
//!     password: ${SR_API_SECRET}
//!   tls:
//!     ca_cert: /etc/ssl/registry-ca.pem
//!   connection:
//!     timeout_ms: 30000
//!     max_retries: 3
//!     retry_backoff_ms: 1000
//!     rate_limit_rps: 25
//!   backup:
//!     subjects: ["*"]
//!     exclude: ["*-test"]
//!     include_versions: all          # or latest
//!     include_soft_deleted: true
//!     include_references: true
//!     fail_on_partial: false
//!     snapshot_interval: 15m         # continuous backups only
//!   restore:
//!     strategy: preserve_ids         # or into_existing
//!     rewrite_ids: false             # into_existing only
//!     restore_soft_deleted: true
//!     verify: true
//!     max_schema_versions: 20000
//!     subject_mapping:
//!       orders-value: orders-v2-value
//!     dry_run: false
//! ```
//!
//! In a backup configuration `url` is the registry to snapshot; in a restore
//! configuration it is the registry to restore into. An unknown key anywhere
//! in the section is an error.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::oauth::{Secret, TokenCache};

/// Default for [`SchemaBackupOptions::snapshot_interval`].
pub const DEFAULT_SNAPSHOT_INTERVAL: Duration = Duration::from_secs(15 * 60);

/// The `schema_registry` section.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistryConfig {
    /// Set to `false` to keep the section but skip the registry.
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Schema Registry URL, for example `https://schema-registry:8081`.
    pub url: String,

    /// How to authenticate. Default: no authentication.
    #[serde(default)]
    pub auth: Option<SchemaRegistryAuth>,

    /// Server certificate verification.
    #[serde(default)]
    pub tls: Option<SchemaRegistryTls>,

    /// Timeouts, retries and the rate limit.
    #[serde(default)]
    pub connection: Option<SchemaConnectionConfig>,

    /// What a backup captures.
    #[serde(default)]
    pub backup: Option<SchemaBackupOptions>,

    /// How a restore treats the target registry.
    #[serde(default)]
    pub restore: Option<SchemaRestoreOptions>,
}

impl SchemaRegistryConfig {
    /// A configuration for `url` with every default.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            enabled: true,
            url: url.into(),
            auth: None,
            tls: None,
            connection: None,
            backup: None,
            restore: None,
        }
    }

    /// The backup options, or their defaults.
    pub fn backup_options(&self) -> SchemaBackupOptions {
        self.backup.clone().unwrap_or_default()
    }

    /// The restore options, or their defaults.
    pub fn restore_options(&self) -> SchemaRestoreOptions {
        self.restore.clone().unwrap_or_default()
    }

    /// Checks that need no network. Run by [`crate::Config::validate`].
    pub fn validate(&self) -> crate::Result<()> {
        let err = |m: String| Err(crate::Error::Config(format!("schema_registry: {m}")));
        let url = self.url.trim();
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return err(format!(
                "url must start with http:// or https:// (got `{url}`)"
            ));
        }
        match &self.auth {
            Some(SchemaRegistryAuth::Basic { username, password }) => {
                if username.trim().is_empty() || password.is_empty() {
                    return err("auth type basic needs a non-empty username and password \
                         (is an environment variable unset?)"
                        .to_string());
                }
            }
            Some(SchemaRegistryAuth::MutualTls {
                client_cert,
                client_key,
                ..
            }) => {
                if client_cert.trim().is_empty() || client_key.trim().is_empty() {
                    return err("auth type mtls needs client_cert and client_key".to_string());
                }
            }
            Some(SchemaRegistryAuth::OAuth(oauth)) => {
                if oauth.provider.trim().is_empty() {
                    return err(
                        "auth type oauth needs `provider`, naming one of oauth.providers"
                            .to_string(),
                    );
                }
            }
            Some(SchemaRegistryAuth::None) | None => {}
        }
        if let Some(backup) = &self.backup {
            for pattern in backup.subjects.iter().chain(&backup.exclude) {
                if let Err(e) = glob::Pattern::new(pattern) {
                    return err(format!("backup: invalid subject pattern `{pattern}`: {e}"));
                }
            }
        }
        if let Some(restore) = &self.restore {
            if restore.rewrite_ids && restore.strategy == SchemaRestoreStrategy::PreserveIds {
                return err(
                    "restore.rewrite_ids applies to strategy: into_existing only; \
                     preserve_ids always keeps the original IDs"
                        .to_string(),
                );
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

/// `schema_registry.auth`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
#[non_exhaustive]
pub enum SchemaRegistryAuth {
    /// No authentication.
    None,

    /// HTTP Basic authentication. For Confluent Cloud the username is the
    /// Schema Registry API key and the password its secret.
    Basic { username: String, password: Secret },

    /// Mutual TLS: a client certificate and key (PEM).
    #[serde(rename = "mtls")]
    MutualTls {
        client_cert: String,
        client_key: String,
        /// CA for the server certificate; the same as `tls.ca_cert`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ca_cert: Option<String>,
    },

    /// OAuth 2.0 bearer token from one of `oauth.providers`.
    #[serde(rename = "oauth")]
    OAuth(SchemaRegistryOAuth),
}

/// `auth: { type: oauth, ... }`.
///
/// ```yaml
/// auth:
///   type: oauth
///   provider: entra                 # oauth.providers.entra
///   target_sr_cluster: lsrc-abc123  # sent as the `target-sr-cluster` header
///   identity_pool_id: pool-Ab1      # sent as `Confluent-Identity-Pool-Id` (optional)
/// ```
///
/// The registry client uses the same token cache as every Kafka cluster
/// bound to the provider; [`crate::oauth::install`] attaches it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct SchemaRegistryOAuth {
    /// A key of `oauth.providers`.
    pub provider: String,
    /// Confluent Cloud Schema Registry cluster ID (`lsrc-...`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_sr_cluster: Option<String>,
    /// Confluent Cloud identity pool ID (`pool-...`), or a comma-separated list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_pool_id: Option<String>,
    /// The shared token cache, attached by [`crate::oauth::install`].
    #[serde(skip)]
    pub token_source: Option<TokenCache>,
}

impl SchemaRegistryOAuth {
    /// Bearer authentication with tokens from `oauth.providers.<provider>`.
    pub fn new(provider: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            ..Default::default()
        }
    }
}

/// `schema_registry.tls`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRegistryTls {
    /// Path to a PEM CA certificate that signs the registry's certificate.
    #[serde(default)]
    pub ca_cert: Option<String>,
}

impl SchemaRegistryTls {
    pub fn with_ca_cert(path: impl Into<String>) -> Self {
        Self {
            ca_cert: Some(path.into()),
        }
    }
}

/// `schema_registry.backup`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaBackupOptions {
    /// Subjects to capture (glob patterns). Default `["*"]`.
    #[serde(default = "default_subject_patterns")]
    pub subjects: Vec<String>,

    /// Subjects to leave out (glob patterns).
    #[serde(default)]
    pub exclude: Vec<String>,

    /// Capture soft-deleted subjects and versions (default true). They hold
    /// schema IDs that records in the backup may still carry; the restore
    /// registers them with their original IDs and soft-deletes them again.
    #[serde(default = "default_true")]
    pub include_soft_deleted: bool,

    /// `all` (default) or `latest`.
    #[serde(default)]
    pub include_versions: VersionSelection,

    /// Also capture the versions that captured schemas reference when their
    /// subject is outside the filter (or, with `latest`, an older version),
    /// so every reference resolves on restore (default true).
    #[serde(default = "default_true")]
    pub include_references: bool,

    /// A snapshot that could not be completed is always marked incomplete
    /// and cannot be restored. With `fail_on_partial: true` the backup
    /// command also fails; with `false` (default) the Kafka data backup's
    /// result decides the exit status and the failure is logged as an error.
    /// `backup --schema-only` always fails on an incomplete snapshot.
    #[serde(default)]
    pub fail_on_partial: bool,

    /// How often a continuous backup (`backup.continuous: true`) takes a new
    /// snapshot. Humantime duration (`15m`, `1h`, `90s`); default 15m.
    /// One-shot backups snapshot once.
    #[serde(
        default,
        with = "humantime_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub snapshot_interval: Option<Duration>,
}

impl SchemaBackupOptions {
    /// [`Self::snapshot_interval`], or [`DEFAULT_SNAPSHOT_INTERVAL`].
    pub fn snapshot_interval_or_default(&self) -> Duration {
        self.snapshot_interval.unwrap_or(DEFAULT_SNAPSHOT_INTERVAL)
    }
}

impl Default for SchemaBackupOptions {
    fn default() -> Self {
        Self {
            subjects: default_subject_patterns(),
            exclude: Vec::new(),
            include_soft_deleted: true,
            include_versions: VersionSelection::default(),
            include_references: true,
            fail_on_partial: false,
            snapshot_interval: None,
        }
    }
}

/// `schema_registry.backup.include_versions`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum VersionSelection {
    #[default]
    All,
    Latest,
}

/// `schema_registry.restore`. The registry in `schema_registry.url` is the
/// target. `restore` and `three-phase-restore` restore it before the
/// records; `restore --schema-only` restores only the registry.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaRestoreOptions {
    /// `preserve_ids` (default) or `into_existing`.
    #[serde(default)]
    pub strategy: SchemaRestoreStrategy,

    /// `into_existing` only. When an original ID or version is held by a
    /// different schema, register the schema normally (new ID) and rewrite
    /// the schema IDs in the restored Confluent wire-format records
    /// (`restore.rewrite_schema_ids`, limited to the restored topics unless
    /// `restore.schema_id_rewrite_topics` is set). Off (strict): any such
    /// conflict fails the restore before the target is changed.
    #[serde(default)]
    pub rewrite_ids: bool,

    /// Register versions that were soft-deleted in the source with their
    /// original IDs and version numbers, then soft-delete them again
    /// (default true). Off: soft-deleted versions are not restored.
    #[serde(default = "default_true")]
    pub restore_soft_deleted: bool,

    /// Apply the backup's global (and context) compatibility and mode.
    /// Default: true for `preserve_ids`, false for `into_existing` (a shared
    /// registry keeps its own global settings).
    #[serde(default)]
    pub restore_global_config: Option<bool>,

    /// After the restore, check that every restored (subject, version)
    /// resolves to the expected ID, an identical schema, the same references
    /// and the same deleted state, and that subject compatibility and mode
    /// match the backup (default true).
    #[serde(default = "default_true")]
    pub verify: bool,

    /// The target's schema version limit, if it has one. Confluent Cloud
    /// Stream Governance packages count soft-deleted versions. The restore
    /// fails before any change when it would exceed the limit. The Schema
    /// Registry API does not expose the limit, so it cannot be detected.
    #[serde(default)]
    pub max_schema_versions: Option<u64>,

    /// Subject renames (source -> target). Applied to references too.
    #[serde(default)]
    pub subject_mapping: HashMap<String, String>,

    /// Plan only: report what would be registered, skipped, rewritten or in
    /// conflict, and change nothing. `restore.dry_run` implies it.
    #[serde(default)]
    pub dry_run: bool,
}

impl Default for SchemaRestoreOptions {
    fn default() -> Self {
        Self {
            strategy: SchemaRestoreStrategy::default(),
            rewrite_ids: false,
            restore_soft_deleted: true,
            restore_global_config: None,
            verify: true,
            max_schema_versions: None,
            subject_mapping: HashMap::new(),
            dry_run: false,
        }
    }
}

impl SchemaRestoreOptions {
    /// [`Self::restore_global_config`], or the strategy's default.
    pub fn applies_global_config(&self) -> bool {
        self.restore_global_config
            .unwrap_or(self.strategy == SchemaRestoreStrategy::PreserveIds)
    }
}

/// How the Schema Registry restore treats the target registry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaRestoreStrategy {
    /// The target must be empty (no subjects, soft-deleted included). The
    /// registry and each context are put in IMPORT mode; every version is
    /// registered with its original ID and version number; compatibility and
    /// modes are restored. A target that already holds exactly this backup
    /// (a re-run) is left unchanged. Resume a partly failed run with
    /// `into_existing`.
    #[default]
    PreserveIds,

    /// The target already has schemas (for example a shared Confluent Cloud
    /// environment). Per version: the same ID with an identical schema is
    /// skipped; a free ID and version is registered under subject-level
    /// IMPORT mode; an ID or version held by a different schema is a
    /// conflict (strict: the restore fails before any change; `rewrite_ids`:
    /// registered with a new ID). Never overwrites a version.
    IntoExisting,
}

impl SchemaRestoreStrategy {
    pub fn as_str(self) -> &'static str {
        match self {
            SchemaRestoreStrategy::PreserveIds => "preserve_ids",
            SchemaRestoreStrategy::IntoExisting => "into_existing",
        }
    }
}

/// `schema_registry.connection`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaConnectionConfig {
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

    /// Maximum requests per second (default 25).
    #[serde(default = "default_rate_limit_rps")]
    pub rate_limit_rps: u32,
}

impl Default for SchemaConnectionConfig {
    fn default() -> Self {
        Self {
            timeout_ms: default_timeout_ms(),
            max_retries: default_max_retries(),
            retry_backoff_ms: default_retry_backoff_ms(),
            rate_limit_rps: default_rate_limit_rps(),
        }
    }
}

/// `Option<Duration>` as a humantime string (`15m`, `1h 30m`).
mod humantime_opt {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(value: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
        match value {
            Some(d) => s.serialize_str(&humantime::format_duration(*d).to_string()),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
        let Some(raw) = Option::<String>::deserialize(d)? else {
            return Ok(None);
        };
        let duration = humantime::parse_duration(raw.trim()).map_err(|e| {
            serde::de::Error::custom(format!(
                "invalid duration {raw:?} ({e}); use a humantime duration such as 15m, 1h or 90s"
            ))
        })?;
        if duration.is_zero() {
            return Err(serde::de::Error::custom(
                "duration must be greater than zero",
            ));
        }
        Ok(Some(duration))
    }
}

fn default_true() -> bool {
    true
}

fn default_subject_patterns() -> Vec<String> {
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
    25
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> Result<SchemaRegistryConfig, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    #[test]
    fn the_proposed_shape_parses_with_its_defaults() {
        let cfg = parse(
            r#"
url: https://psrc-xxxxx.westeurope.azure.confluent.cloud
auth:
  type: basic
  username: key
  password: secret
backup:
  subjects: ["*"]
  exclude: ["*-test"]
  include_versions: all
  include_soft_deleted: true
  include_references: true
  fail_on_partial: false
restore:
  strategy: preserve_ids
  rewrite_ids: false
  restore_soft_deleted: true
  verify: true
  max_schema_versions: 20000
  subject_mapping:
    orders-value: orders-v2-value
  dry_run: false
"#,
        )
        .unwrap();
        assert!(cfg.enabled);
        cfg.validate().unwrap();
        assert!(matches!(cfg.auth, Some(SchemaRegistryAuth::Basic { .. })));
        let backup = cfg.backup_options();
        assert_eq!(backup.exclude, ["*-test"]);
        assert_eq!(
            backup.snapshot_interval_or_default(),
            DEFAULT_SNAPSHOT_INTERVAL
        );
        let restore = cfg.restore_options();
        assert_eq!(restore.strategy, SchemaRestoreStrategy::PreserveIds);
        assert_eq!(restore.max_schema_versions, Some(20000));
        assert_eq!(restore.subject_mapping["orders-value"], "orders-v2-value");
        assert!(restore.applies_global_config());
    }

    #[test]
    fn a_bare_url_gets_every_default() {
        let cfg = parse("url: http://sr:8081").unwrap();
        cfg.validate().unwrap();
        let backup = cfg.backup_options();
        assert_eq!(backup.subjects, ["*"]);
        assert!(backup.include_soft_deleted && backup.include_references);
        assert_eq!(backup.include_versions, VersionSelection::All);
        assert!(!backup.fail_on_partial);
        let restore = cfg.restore_options();
        assert!(restore.restore_soft_deleted && restore.verify && !restore.dry_run);
        assert!(!restore.rewrite_ids);
        let conn = SchemaConnectionConfig::default();
        assert_eq!(
            (
                conn.timeout_ms,
                conn.max_retries,
                conn.retry_backoff_ms,
                conn.rate_limit_rps
            ),
            (30_000, 3, 1_000, 25)
        );
    }

    #[test]
    fn into_existing_keeps_the_targets_global_settings_by_default() {
        let cfg = parse("url: http://sr\nrestore: { strategy: into_existing }").unwrap();
        assert!(!cfg.restore_options().applies_global_config());
        let cfg = parse(
            "url: http://sr\nrestore: { strategy: into_existing, restore_global_config: true }",
        )
        .unwrap();
        assert!(cfg.restore_options().applies_global_config());
    }

    #[test]
    fn unknown_keys_are_errors_at_every_level() {
        for yaml in [
            "url: http://sr\nurll: x",
            "url: http://sr\nbackup: { subjectz: [a] }",
            "url: http://sr\nrestore: { strategy: preserve_ids, rewrite_idz: true }",
            "url: http://sr\nconnection: { concurrent_requests: 4 }",
            "url: http://sr\ntls: { ca: x }",
            "url: http://sr\nauth: { type: basic, username: u, password: p, token: t }",
            "url: http://sr\nauth: { type: oauth, provider: idp, token_url: https://x }",
        ] {
            let err = parse(yaml).unwrap_err().to_string();
            assert!(err.contains("unknown field"), "{yaml}: {err}");
        }
        assert!(parse("url: http://sr\nrestore: { strategy: overwrite }").is_err());
        assert!(parse("url: http://sr\nauth: { type: kerberos }").is_err());
    }

    #[test]
    fn every_auth_type_parses() {
        let none = parse("url: http://sr\nauth: { type: none }").unwrap();
        assert!(matches!(none.auth, Some(SchemaRegistryAuth::None)));
        let mtls = parse(
            "url: https://sr\nauth: { type: mtls, client_cert: /c.pem, client_key: /k.pem, ca_cert: /ca.pem }",
        )
        .unwrap();
        mtls.validate().unwrap();
        assert!(matches!(
            mtls.auth,
            Some(SchemaRegistryAuth::MutualTls { .. })
        ));
        let oauth = parse(
            "url: https://sr\nauth: { type: oauth, provider: entra, target_sr_cluster: lsrc-1, identity_pool_id: pool-1 }",
        )
        .unwrap();
        oauth.validate().unwrap();
        match oauth.auth {
            Some(SchemaRegistryAuth::OAuth(o)) => {
                assert_eq!(o.provider, "entra");
                assert_eq!(o.target_sr_cluster.as_deref(), Some("lsrc-1"));
                assert!(o.token_source.is_none());
            }
            other => panic!("{other:?}"),
        }
        // mTLS without a key, OAuth without a provider.
        assert!(parse("url: https://sr\nauth: { type: mtls, client_cert: /c.pem }").is_err());
        assert!(parse("url: https://sr\nauth: { type: oauth }").is_err());
    }

    #[test]
    fn validation_catches_what_parsing_cannot() {
        let bad = |yaml: &str| parse(yaml).unwrap().validate().unwrap_err().to_string();
        assert!(bad("url: sr:8081").contains("http://"));
        assert!(
            bad("url: http://sr\nauth: { type: basic, username: u, password: '' }")
                .contains("non-empty")
        );
        assert!(bad("url: http://sr\nrestore: { rewrite_ids: true }").contains("into_existing"));
        assert!(bad("url: http://sr\nbackup: { subjects: ['[a'] }").contains("pattern"));
        assert!(bad("url: http://sr\nconnection: { rate_limit_rps: 0 }").contains("rate_limit"));
    }

    #[test]
    fn the_password_is_never_printed_or_serialized() {
        let cfg =
            parse("url: http://sr\nauth: { type: basic, username: u, password: hunter2-secret }")
                .unwrap();
        assert!(!format!("{cfg:?}").contains("hunter2"));
        assert!(!serde_yaml::to_string(&cfg).unwrap().contains("hunter2"));
    }

    #[test]
    fn snapshot_interval_parses_and_rejects_garbage_and_zero() {
        let cfg = parse("url: http://sr\nbackup: { snapshot_interval: 5m }").unwrap();
        let backup = cfg.backup_options();
        assert_eq!(backup.snapshot_interval, Some(Duration::from_secs(300)));
        let yaml = serde_yaml::to_string(&backup).unwrap();
        assert!(yaml.contains("snapshot_interval: 5m"), "{yaml}");
        for bad in ["soon", "0s", "-5m"] {
            assert!(
                parse(&format!(
                    "url: http://sr\nbackup: {{ snapshot_interval: {bad} }}"
                ))
                .is_err(),
                "{bad}"
            );
        }
    }
}
