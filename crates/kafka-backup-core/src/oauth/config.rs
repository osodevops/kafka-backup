//! OAuth configuration: the top-level `oauth` section and the per-cluster
//! `security.oauth` binding.
//!
//! ```yaml
//! oauth:
//!   providers:
//!     entra:
//!       type: oidc_client_credentials
//!       token_endpoint_url: https://login.microsoftonline.com/<tenant>/oauth2/v2.0/token
//!       client_id: <app id>
//!       client_secret: ${ENTRA_CLIENT_SECRET}
//!       scope: api://<app id>/.default
//!   cache: { refresh_fraction: 0.8, refresh_before_expiry: 5m, clock_skew: 30s, min_refresh_interval: 30s }
//!
//! source:
//!   security:
//!     security_protocol: SASL_SSL
//!     sasl_mechanism: OAUTHBEARER
//!     oauth:
//!       provider: entra
//!       extensions: { logicalCluster: lkc-abc12, identityPoolId: pool-Ab1 }
//! ```
//!
//! Every struct here rejects unknown keys: a misspelt credential setting is
//! an error, not a silently ignored line. Secrets are [`Secret`]s: zeroized
//! on drop, never printed and serialized as `***`.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use zeroize::Zeroizing;

use crate::oauth::cache::CacheSettings;
use crate::oauth::framing::validate_extension;
use crate::oauth::http::ClientAuth;

/// A secret from the configuration. `Debug` and `Serialize` never reveal it.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Secret(Zeroizing<String>);

impl Secret {
    pub fn new(value: impl Into<String>) -> Self {
        Self(Zeroizing::new(value.into()))
    }

    /// The secret value. Do not log it.
    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.trim().is_empty()
    }
}

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("\"***\"")
    }
}

impl Serialize for Secret {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str("***")
    }
}

impl<'de> Deserialize<'de> for Secret {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Secret::new(String::deserialize(d)?))
    }
}

/// The top-level `oauth` section.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OauthConfig {
    /// Named token providers. A cluster picks one with
    /// `<cluster>.security.oauth.provider`.
    #[serde(default)]
    pub providers: BTreeMap<String, ProviderConfig>,
    /// Token cache timing, shared by every provider.
    #[serde(default)]
    pub cache: CacheConfig,
}

/// `source.security.oauth` / `target.security.oauth`: which provider a
/// cluster authenticates with, and the SASL extensions sent with its tokens.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterBinding {
    /// A key of `oauth.providers`.
    pub provider: String,
    /// RFC 7628 SASL extensions sent with the token.
    #[serde(default)]
    pub extensions: SaslExtensions,
}

/// The SASL extensions Confluent Cloud understands. Both are optional.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SaslExtensions {
    /// The Kafka cluster ID (`lkc-...`).
    #[serde(
        rename = "logicalCluster",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub logical_cluster: Option<String>,
    /// The identity pool ID (`pool-...`), or a comma-separated list.
    #[serde(
        rename = "identityPoolId",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub identity_pool_id: Option<String>,
}

impl SaslExtensions {
    /// The extensions as sent: empty values dropped, a pool list normalised
    /// to `pool-a,pool-b`, every value checked against RFC 7628.
    pub fn to_map(&self) -> Result<BTreeMap<String, String>, String> {
        let mut map = BTreeMap::new();
        if let Some(cluster) = self.logical_cluster.as_deref().map(str::trim) {
            if !cluster.is_empty() {
                map.insert("logicalCluster".to_string(), cluster.to_string());
            }
        }
        if let Some(pools) = &self.identity_pool_id {
            let list = normalise_pool_list(pools);
            if !list.is_empty() {
                map.insert("identityPoolId".to_string(), list);
            }
        }
        for (k, v) in &map {
            validate_extension(k, v)?;
        }
        Ok(map)
    }
}

/// `" pool-a , pool-b ,"` → `"pool-a,pool-b"`.
pub fn normalise_pool_list(pools: &str) -> String {
    pools
        .split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join(",")
}

/// `oauth.providers.<name>`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderConfig {
    /// RFC 6749 client credentials (Entra ID, Okta, Keycloak, ...).
    OidcClientCredentials(OidcProviderConfig),
    /// Azure managed identity through IMDS (VMs, VMSS, AKS pod identity).
    AzureManagedIdentity(AzureManagedIdentityConfig),
    /// Azure workload identity: federated token exchange (AKS).
    AzureWorkloadIdentity(AzureWorkloadIdentityConfig),
    /// A token file kept fresh by something else.
    File(FileProviderConfig),
    /// A fixed token. Tests only.
    Static(StaticProviderConfig),
}

impl ProviderConfig {
    pub fn kind(&self) -> &'static str {
        match self {
            ProviderConfig::OidcClientCredentials(_) => "oidc_client_credentials",
            ProviderConfig::AzureManagedIdentity(_) => "azure_managed_identity",
            ProviderConfig::AzureWorkloadIdentity(_) => "azure_workload_identity",
            ProviderConfig::File(_) => "file",
            ProviderConfig::Static(_) => "static",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OidcProviderConfig {
    /// The token endpoint. Exactly one of this and `issuer`.
    #[serde(default)]
    pub token_endpoint_url: Option<String>,
    /// OIDC issuer; the token endpoint is discovered from
    /// `{issuer}/.well-known/openid-configuration`.
    #[serde(default)]
    pub issuer: Option<String>,
    pub client_id: String,
    pub client_secret: Secret,
    /// `basic` (`client_secret_basic`, default) or `post`.
    #[serde(default)]
    pub client_auth: ClientAuth,
    #[serde(default)]
    pub scope: Option<String>,
    /// Sent as the `audience` parameter (Okta, Auth0). Entra uses `scope`.
    #[serde(default)]
    pub audience: Option<String>,
    /// Extra form parameters for the token request.
    #[serde(default)]
    pub extra_params: BTreeMap<String, String>,
    /// Extra CA certificate (PEM) for a private identity provider.
    #[serde(default)]
    pub ca_cert: Option<PathBuf>,
    /// Allow `http://` to a non-loopback host. Client secrets then travel in
    /// clear text; for isolated test networks only.
    #[serde(default)]
    pub allow_insecure_http: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureManagedIdentityConfig {
    /// The resource (`api://<app id>`). Or give `scope`.
    #[serde(default)]
    pub resource: Option<String>,
    /// An Entra scope (`api://<app id>/.default`); `/.default` is dropped to
    /// form the resource.
    #[serde(default)]
    pub scope: Option<String>,
    /// Client ID of a user-assigned managed identity. Unset: the
    /// system-assigned identity.
    #[serde(default)]
    pub client_id: Option<String>,
    /// Override the IMDS token endpoint (tests, proxies).
    #[serde(default)]
    pub endpoint: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureWorkloadIdentityConfig {
    /// The Entra scope (`api://<app id>/.default`).
    pub scope: String,
    /// Default `AZURE_TENANT_ID`.
    #[serde(default)]
    pub tenant_id: Option<String>,
    /// Default `AZURE_CLIENT_ID`.
    #[serde(default)]
    pub client_id: Option<String>,
    /// Default `AZURE_FEDERATED_TOKEN_FILE`.
    #[serde(default)]
    pub token_file: Option<PathBuf>,
    /// Default `AZURE_AUTHORITY_HOST`, else `https://login.microsoftonline.com/`.
    #[serde(default)]
    pub authority_host: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileProviderConfig {
    /// Read on every refresh; surrounding whitespace is trimmed.
    pub path: PathBuf,
    /// Optional file holding the expiry (RFC 3339 or Unix seconds). Unset:
    /// the JWT `exp` claim, if any.
    #[serde(default)]
    pub expires_at_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticProviderConfig {
    pub token: Secret,
}

/// `oauth.cache`. Humantime durations (`30s`, `5m`).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_fraction: Option<f64>,
    #[serde(
        default,
        with = "duration_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub refresh_before_expiry: Option<Duration>,
    #[serde(
        default,
        with = "duration_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub clock_skew: Option<Duration>,
    #[serde(
        default,
        with = "duration_opt",
        skip_serializing_if = "Option::is_none"
    )]
    pub min_refresh_interval: Option<Duration>,
}

impl CacheConfig {
    pub fn to_settings(&self) -> Result<CacheSettings, String> {
        let d = CacheSettings::default();
        let settings = CacheSettings {
            refresh_fraction: self.refresh_fraction.unwrap_or(d.refresh_fraction),
            refresh_before_expiry: self
                .refresh_before_expiry
                .unwrap_or(d.refresh_before_expiry),
            clock_skew: self.clock_skew.unwrap_or(d.clock_skew),
            min_refresh_interval: self.min_refresh_interval.unwrap_or(d.min_refresh_interval),
        };
        if !(settings.refresh_fraction > 0.0 && settings.refresh_fraction < 1.0) {
            return Err(format!(
                "oauth.cache.refresh_fraction must be between 0 and 1 (exclusive), got {}",
                settings.refresh_fraction
            ));
        }
        Ok(settings)
    }
}

/// `Option<Duration>` as a humantime string; zero allowed.
mod duration_opt {
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
        humantime::parse_duration(raw.trim())
            .map(Some)
            .map_err(|e| {
                serde::de::Error::custom(format!(
                    "invalid duration {raw:?} ({e}); use e.g. 30s or 5m"
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_provider_type() {
        let yaml = r#"
providers:
  entra:
    type: oidc_client_credentials
    token_endpoint_url: https://login.microsoftonline.com/t/oauth2/v2.0/token
    client_id: app
    client_secret: s3cret-value
    client_auth: post
    scope: api://app/.default
  okta:
    type: oidc_client_credentials
    issuer: https://example.okta.com/oauth2/default
    client_id: app
    client_secret: x
    audience: api://kafka
    extra_params: { resource: r }
  mi:
    type: azure_managed_identity
    scope: api://app/.default
    client_id: uami
  wi:
    type: azure_workload_identity
    scope: api://app/.default
  vault:
    type: file
    path: /var/run/token
  test:
    type: static
    token: t
cache: { refresh_fraction: 0.75, clock_skew: 10s, min_refresh_interval: 0s }
"#;
        let cfg: OauthConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.providers.len(), 6);
        match &cfg.providers["entra"] {
            ProviderConfig::OidcClientCredentials(o) => {
                assert_eq!(o.client_auth, ClientAuth::Post);
                assert_eq!(o.client_secret.expose(), "s3cret-value");
            }
            other => panic!("{other:?}"),
        }
        let source: ClusterBinding = serde_yaml::from_str(
            r#"{ provider: entra, extensions: { logicalCluster: lkc-1, identityPoolId: "pool-a, pool-b" } }"#,
        )
        .unwrap();
        assert_eq!(source.provider, "entra");
        let map = source.extensions.to_map().unwrap();
        assert_eq!(map["identityPoolId"], "pool-a,pool-b");
        assert_eq!(map["logicalCluster"], "lkc-1");
        let settings = cfg.cache.to_settings().unwrap();
        assert_eq!(settings.refresh_fraction, 0.75);
        assert_eq!(settings.clock_skew, Duration::from_secs(10));
        assert_eq!(settings.min_refresh_interval, Duration::ZERO);
        assert_eq!(settings.refresh_before_expiry, Duration::from_secs(300));
    }

    #[test]
    fn secrets_never_print_or_serialize() {
        let yaml = r#"
providers:
  p: { type: oidc_client_credentials, token_endpoint_url: "https://idp/token", client_id: a, client_secret: very-secret-value }
  s: { type: static, token: static-secret-token }
"#;
        let cfg: OauthConfig = serde_yaml::from_str(yaml).unwrap();
        let debug = format!("{cfg:?}");
        let yaml_out = serde_yaml::to_string(&cfg).unwrap();
        for text in [&debug, &yaml_out] {
            assert!(!text.contains("very-secret-value"), "{text}");
            assert!(!text.contains("static-secret-token"), "{text}");
        }
    }

    #[test]
    fn unknown_keys_are_errors() {
        for yaml in [
            "providers: { p: { type: file, path: /t, pth: /x } }",
            "providers: { p: { type: oidc_client_credentials, token_endpoint_url: x, client_id: a, client_secret: b, scop: c } }",
            "cache: { skew: 1s }",
            "provider: {}",
            "kafka: { source: { provider: p } }",
            "providers: { p: { type: kerberos } }",
        ] {
            assert!(serde_yaml::from_str::<OauthConfig>(yaml).is_err(), "{yaml}");
        }
        for yaml in [
            "{ provider: p, extension: {} }",
            "{ provider: p, extensions: { logical_cluster: lkc-1 } }",
            "{ extensions: {} }",
        ] {
            assert!(
                serde_yaml::from_str::<ClusterBinding>(yaml).is_err(),
                "{yaml}"
            );
        }
    }

    #[test]
    fn extension_values_are_checked() {
        let ext = SaslExtensions {
            logical_cluster: Some("lkc\u{1}x".into()),
            identity_pool_id: None,
        };
        assert!(ext.to_map().is_err());
        let empty = SaslExtensions {
            logical_cluster: Some(" ".into()),
            identity_pool_id: Some(" , ".into()),
        };
        assert!(empty.to_map().unwrap().is_empty());
    }

    #[test]
    fn cache_fraction_bounds() {
        for bad in [0.0, 1.0, 1.5, -0.1] {
            let c = CacheConfig {
                refresh_fraction: Some(bad),
                ..Default::default()
            };
            assert!(c.to_settings().is_err(), "{bad}");
        }
    }
}
