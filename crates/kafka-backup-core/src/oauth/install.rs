//! [`install`]: validate the `oauth` section and the clusters' bindings,
//! build the shared token caches and install an OAUTHBEARER plugin factory
//! on each cluster that uses one.

use std::collections::BTreeMap;
use std::net::IpAddr;
use std::sync::Arc;

use tracing::{info, warn};

use crate::config::{Config, KafkaConfig, SaslMechanism, SecurityProtocol};
use crate::oauth::cache::{CacheSettings, TokenCache};
use crate::oauth::config::{ClusterBinding, OauthConfig, ProviderConfig};
use crate::oauth::http::RetryPolicy;
use crate::oauth::providers::azure_imds::{resource_from_scope, AzureManagedIdentityProvider};
use crate::oauth::providers::azure_workload::{
    AzureWorkloadIdentityProvider, DEFAULT_AUTHORITY_HOST,
};
use crate::oauth::providers::file::FileTokenProvider;
use crate::oauth::providers::oidc::{OidcClientCredentialsProvider, OidcSettings};
use crate::oauth::providers::static_token::StaticTokenProvider;
use crate::oauth::token::OauthTokenProvider;
use crate::oauth::{OauthBearerPlugin, OauthError, OauthPluginFactory};

/// Which Kafka cluster of a configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KafkaRole {
    Source,
    Target,
}

impl KafkaRole {
    /// The configuration key of the cluster: `source` or `target`.
    pub fn key(self) -> &'static str {
        match self {
            KafkaRole::Source => "source",
            KafkaRole::Target => "target",
        }
    }
}

/// What [`install`] set up.
#[derive(Debug, Clone, Default)]
pub struct OauthInstall {
    /// Every token cache in use (for metrics).
    pub caches: Vec<TokenCache>,
    /// What uses OAuth, for logs.
    pub uses: Vec<String>,
}

impl OauthInstall {
    /// True when something in this command authenticates with OAuth.
    pub fn is_active(&self) -> bool {
        !self.uses.is_empty()
    }
}

/// Wire the `oauth` section into `config`: an OAUTHBEARER plugin factory on
/// each of the `source` / `target` clusters that has a `security.oauth`
/// binding. Call after parsing and before building any client.
///
/// Clusters that share a provider share one token cache.
pub fn install(config: &mut Config) -> Result<OauthInstall, OauthError> {
    let oauth = config.oauth.clone();
    let oauth = oauth.as_ref();
    validate(oauth)?;

    let source = kafka_binding(KafkaRole::Source, config.source.as_ref())?;
    let target = kafka_binding(KafkaRole::Target, config.target.as_ref())?;

    let mut providers = ProviderRegistry::new(oauth)?;
    let mut uses = Vec::new();
    if let (Some(binding), Some(kafka)) = (&source, config.source.as_mut()) {
        install_binding(&mut providers, KafkaRole::Source, binding, kafka)?;
        uses.push(describe(KafkaRole::Source, binding));
    }
    if let (Some(binding), Some(kafka)) = (&target, config.target.as_mut()) {
        install_binding(&mut providers, KafkaRole::Target, binding, kafka)?;
        uses.push(describe(KafkaRole::Target, binding));
    }

    let installed = OauthInstall {
        caches: providers.into_caches(),
        uses,
    };
    if installed.is_active() {
        info!(uses = %installed.uses.join(", "), "OAuth enabled");
    }
    Ok(installed)
}

/// [`install`] for one cluster taken out of a configuration (the offset
/// commands' `--config --cluster`, `validation run`).
pub fn install_cluster(
    oauth: Option<&OauthConfig>,
    role: KafkaRole,
    kafka: &mut KafkaConfig,
) -> Result<OauthInstall, OauthError> {
    validate(oauth)?;
    let Some(binding) = kafka_binding(role, Some(kafka))? else {
        return Ok(OauthInstall::default());
    };
    let mut providers = ProviderRegistry::new(oauth)?;
    install_binding(&mut providers, role, &binding, kafka)?;
    let installed = OauthInstall {
        caches: providers.into_caches(),
        uses: vec![describe(role, &binding)],
    };
    info!(uses = %installed.uses.join(", "), "OAuth enabled");
    Ok(installed)
}

fn describe(role: KafkaRole, binding: &ClusterBinding) -> String {
    format!(
        "{} Kafka cluster (provider '{}')",
        role.key(),
        binding.provider
    )
}

/// Structural checks that need no network: provider names and cache
/// settings.
fn validate(oauth: Option<&OauthConfig>) -> Result<(), OauthError> {
    let Some(oauth) = oauth else {
        return Ok(());
    };
    for name in oauth.providers.keys() {
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        {
            return Err(OauthError::Config(format!(
                "provider name `{name}` must use letters, digits, `_`, `-` or `.`"
            )));
        }
    }
    oauth.cache.to_settings().map_err(OauthError::Config)?;
    Ok(())
}

fn provider_config<'a>(
    oauth: Option<&'a OauthConfig>,
    name: &str,
    user: &str,
) -> Result<&'a ProviderConfig, OauthError> {
    oauth.and_then(|o| o.providers.get(name)).ok_or_else(|| {
        let defined: Vec<&str> = oauth
            .map(|o| o.providers.keys().map(String::as_str).collect())
            .unwrap_or_default();
        OauthError::Config(format!(
            "{user} refers to OAuth provider '{name}', which is not defined under \
             oauth.providers (defined: {})",
            if defined.is_empty() {
                "none".to_string()
            } else {
                defined.join(", ")
            }
        ))
    })
}

/// The binding a cluster needs, or an error when its mechanism and its
/// `security.oauth` binding disagree.
fn kafka_binding(
    role: KafkaRole,
    kafka: Option<&KafkaConfig>,
) -> Result<Option<ClusterBinding>, OauthError> {
    let r = role.key();
    let Some(kafka) = kafka else {
        return Ok(None);
    };
    let security = &kafka.security;
    let wants_oauth = matches!(security.sasl_mechanism, Some(SaslMechanism::OAuthBearer));
    match (wants_oauth, security.oauth.clone()) {
        (false, None) => Ok(None),
        (false, Some(_)) => Err(OauthError::Config(format!(
            "{r}.security.oauth is set but {r}.security.sasl_mechanism is not OAUTHBEARER; \
             set {r}.security.sasl_mechanism: OAUTHBEARER (with security_protocol: SASL_SSL)"
        ))),
        // A factory installed in code (a library user's own
        // `SaslMechanismPlugin`) needs no binding.
        (true, None) if security.sasl_mechanism_plugin_factory.is_some() => Ok(None),
        (true, None) => Err(OauthError::Config(format!(
            "{r}.security.sasl_mechanism is OAUTHBEARER but {r}.security.oauth is not set; \
             add `{r}.security.oauth: {{ provider: <name> }}` naming one of oauth.providers"
        ))),
        (true, Some(binding)) => {
            match security.security_protocol {
                SecurityProtocol::SaslSsl => {}
                SecurityProtocol::SaslPlaintext => warn!(
                    "{r} uses OAUTHBEARER over SASL_PLAINTEXT: bearer tokens cross the network \
                     unencrypted. Use SASL_SSL outside local testing."
                ),
                SecurityProtocol::Plaintext | SecurityProtocol::Ssl => {
                    return Err(OauthError::Config(format!(
                        "{r}.security.sasl_mechanism is OAUTHBEARER but security_protocol is not \
                         SASL_SSL (or SASL_PLAINTEXT); set {r}.security.security_protocol: SASL_SSL"
                    )))
                }
            }
            Ok(Some(binding))
        }
    }
}

fn install_binding(
    providers: &mut ProviderRegistry<'_>,
    role: KafkaRole,
    binding: &ClusterBinding,
    kafka: &mut KafkaConfig,
) -> Result<(), OauthError> {
    let user = format!("{}.security.oauth", role.key());
    let extensions = binding
        .extensions
        .to_map()
        .map_err(|e| OauthError::Config(format!("{user}.extensions: {e}")))?;
    let cache = providers.cache(&binding.provider, &user)?;
    let plugin = OauthBearerPlugin::from_cache(cache, extensions);
    kafka.security.sasl_mechanism_plugin_factory =
        Some(OauthPluginFactory::new(plugin).into_handle());
    Ok(())
}

/// One [`TokenCache`] per provider name, built on first use, so that every
/// consumer of a provider (each Kafka cluster bound to it, a Schema Registry
/// client) shares one cache and one in-flight token request.
pub struct ProviderRegistry<'a> {
    oauth: Option<&'a OauthConfig>,
    settings: CacheSettings,
    caches: BTreeMap<String, TokenCache>,
}

impl<'a> ProviderRegistry<'a> {
    pub fn new(oauth: Option<&'a OauthConfig>) -> Result<Self, OauthError> {
        let settings = match oauth {
            Some(o) => o.cache.to_settings().map_err(OauthError::Config)?,
            None => CacheSettings::default(),
        };
        Ok(Self {
            oauth,
            settings,
            caches: BTreeMap::new(),
        })
    }

    /// The cache for provider `name`. `user` names the setting that refers
    /// to it, for the error when it is not defined.
    pub fn cache(&mut self, name: &str, user: &str) -> Result<TokenCache, OauthError> {
        if let Some(cache) = self.caches.get(name) {
            return Ok(cache.clone());
        }
        let config = provider_config(self.oauth, name, user)?;
        let provider = build_provider(name, config)?;
        let cache = TokenCache::new(name, provider, self.settings);
        self.caches.insert(name.to_string(), cache.clone());
        Ok(cache)
    }

    /// Every cache built so far.
    pub fn into_caches(self) -> Vec<TokenCache> {
        self.caches.into_values().collect()
    }
}

/// Build the provider for `oauth.providers.<name>`.
pub fn build_provider(
    name: &str,
    config: &ProviderConfig,
) -> Result<Arc<dyn OauthTokenProvider>, OauthError> {
    let wrap = |source| OauthError::Provider {
        provider: name.to_string(),
        source,
    };
    Ok(match config {
        ProviderConfig::OidcClientCredentials(o) => {
            for url in [&o.token_endpoint_url, &o.issuer].into_iter().flatten() {
                check_url(name, url, o.allow_insecure_http)?;
            }
            Arc::new(
                OidcClientCredentialsProvider::new(OidcSettings {
                    token_endpoint_url: o.token_endpoint_url.clone(),
                    issuer: o.issuer.clone(),
                    client_id: o.client_id.clone(),
                    client_secret: zeroize::Zeroizing::new(o.client_secret.expose().to_string()),
                    client_auth: o.client_auth,
                    scope: o.scope.clone(),
                    audience: o.audience.clone(),
                    extra_params: o.extra_params.clone(),
                    ca_cert: o.ca_cert.clone(),
                    retry: RetryPolicy::default(),
                })
                .map_err(wrap)?,
            )
        }
        ProviderConfig::AzureManagedIdentity(m) => {
            let resource = m
                .resource
                .clone()
                .filter(|r| !r.trim().is_empty())
                .or_else(|| m.scope.as_deref().map(resource_from_scope))
                .ok_or_else(|| {
                    OauthError::Config(format!(
                        "provider '{name}' (azure_managed_identity) needs `resource` or `scope`"
                    ))
                })?;
            Arc::new(
                AzureManagedIdentityProvider::new(
                    resource,
                    m.client_id.clone(),
                    m.endpoint.clone(),
                    None,
                    RetryPolicy::imds(),
                )
                .map_err(wrap)?,
            )
        }
        ProviderConfig::AzureWorkloadIdentity(w) => {
            let setting = |value: &Option<String>, var: &str, key: &str| {
                value
                    .clone()
                    .filter(|v| !v.trim().is_empty())
                    .or_else(|| std::env::var(var).ok().filter(|v| !v.trim().is_empty()))
                    .ok_or_else(|| {
                        OauthError::Config(format!(
                            "provider '{name}' (azure_workload_identity): set `{key}` or the {var} \
                             environment variable (the AKS workload identity webhook injects it)"
                        ))
                    })
            };
            let tenant_id = setting(&w.tenant_id, "AZURE_TENANT_ID", "tenant_id")?;
            let client_id = setting(&w.client_id, "AZURE_CLIENT_ID", "client_id")?;
            let token_file = setting(
                &w.token_file.as_ref().map(|p| p.display().to_string()),
                "AZURE_FEDERATED_TOKEN_FILE",
                "token_file",
            )?;
            let authority = w
                .authority_host
                .clone()
                .or_else(|| std::env::var("AZURE_AUTHORITY_HOST").ok())
                .filter(|a| !a.trim().is_empty())
                .unwrap_or_else(|| DEFAULT_AUTHORITY_HOST.to_string());
            check_url(name, &authority, false)?;
            Arc::new(
                AzureWorkloadIdentityProvider::new(
                    &tenant_id,
                    &client_id,
                    token_file.into(),
                    &authority,
                    &w.scope,
                    RetryPolicy::default(),
                )
                .map_err(wrap)?,
            )
        }
        ProviderConfig::File(f) => Arc::new(FileTokenProvider::new(
            f.path.clone(),
            f.expires_at_file.clone(),
        )),
        ProviderConfig::Static(s) => {
            warn!(
                provider = name,
                "OAuth provider type `static` is for tests: the token is never refreshed"
            );
            Arc::new(StaticTokenProvider::new(s.token.expose()).map_err(wrap)?)
        }
    })
}

/// `https`, or `http` to a loopback host (or with `allow_insecure_http`).
fn check_url(provider: &str, url: &str, allow_insecure_http: bool) -> Result<(), OauthError> {
    let parsed = reqwest::Url::parse(url).map_err(|e| {
        OauthError::Config(format!("provider '{provider}': invalid URL {url}: {e}"))
    })?;
    match parsed.scheme() {
        "https" => Ok(()),
        "http" if allow_insecure_http || is_loopback(parsed.host_str()) => Ok(()),
        "http" => Err(OauthError::Config(format!(
            "provider '{provider}': {url} is plain http, so credentials would cross the network \
             unencrypted; use https (or set allow_insecure_http: true on an isolated test network)"
        ))),
        other => Err(OauthError::Config(format!(
            "provider '{provider}': unsupported URL scheme `{other}` in {url}"
        ))),
    }
}

fn is_loopback(host: Option<&str>) -> bool {
    let Some(host) = host else {
        return false;
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
mode: restore
backup_id: b
source:
  bootstrap_servers: ["s:9092"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth: { provider: idp, extensions: { logicalCluster: lkc-src, identityPoolId: "pool-a, pool-b" } }
target:
  bootstrap_servers: ["t:9092"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth: { provider: idp, extensions: { logicalCluster: lkc-dst } }
storage: { backend: filesystem, path: /tmp/b }
"#;

    const OAUTH: &str = r#"
oauth:
  providers:
    idp:
      type: oidc_client_credentials
      token_endpoint_url: https://idp.example.com/token
      client_id: kafka-backup
      client_secret: s3cret-value
    other: { type: static, token: t }
"#;

    fn parse(yaml: &str) -> Config {
        Config::from_yaml_with_warnings(yaml).unwrap().0
    }

    /// The installed factory, as its Debug text (provider and extensions).
    fn factory_of(kafka: &Option<KafkaConfig>) -> String {
        let handle = kafka
            .as_ref()
            .unwrap()
            .security
            .sasl_mechanism_plugin_factory
            .clone()
            .expect("factory installed");
        assert_eq!(handle.mechanism_name(), "OAUTHBEARER");
        let text = format!("{handle:?}");
        assert!(text.contains("OauthPluginFactory"), "{text}");
        assert!(!text.contains("s3cret-value"), "{text}");
        text
    }

    #[test]
    fn installs_one_shared_cache_with_per_cluster_extensions() {
        let mut config = parse(&format!("{BASE}{OAUTH}"));
        let installed = install(&mut config).unwrap();
        assert_eq!(
            installed.caches.len(),
            1,
            "source and target share provider 'idp'"
        );
        assert_eq!(installed.caches[0].name(), "idp");
        assert_eq!(installed.uses.len(), 2);
        let source = factory_of(&config.source);
        assert!(
            source.contains("identityPoolId") && source.contains("pool-a,pool-b"),
            "{source}"
        );
        assert!(source.contains("lkc-src"), "{source}");
        let target = factory_of(&config.target);
        assert!(
            target.contains("lkc-dst") && !target.contains("pool-a"),
            "{target}"
        );
    }

    #[test]
    fn source_and_target_can_use_different_providers() {
        let yaml = format!("{BASE}{OAUTH}").replace(
            "oauth: { provider: idp, extensions: { logicalCluster: lkc-dst } }",
            "oauth: { provider: other }",
        );
        let mut config = parse(&yaml);
        let installed = install(&mut config).unwrap();
        let names: Vec<&str> = installed.caches.iter().map(|c| c.name()).collect();
        assert_eq!(names, vec!["idp", "other"]);
    }

    #[test]
    fn oauthbearer_without_binding_is_a_config_error() {
        let yaml = format!("{BASE}{OAUTH}").replace(
            "\n    oauth: { provider: idp, extensions: { logicalCluster: lkc-src, identityPoolId: \"pool-a, pool-b\" } }",
            "",
        );
        let mut config = parse(&yaml);
        let err = install(&mut config).unwrap_err().to_string();
        assert!(
            err.contains("source.security.sasl_mechanism is OAUTHBEARER"),
            "{err}"
        );
        assert!(err.contains("source.security.oauth"), "{err}");
        assert!(config
            .source
            .as_ref()
            .unwrap()
            .security
            .sasl_mechanism_plugin_factory
            .is_none());
    }

    #[test]
    fn binding_without_oauthbearer_is_a_config_error() {
        let yaml = format!("{BASE}{OAUTH}").replace(
            "    sasl_mechanism: OAUTHBEARER\n    oauth: { provider: idp, extensions: { logicalCluster: lkc-dst } }",
            "    sasl_mechanism: PLAIN\n    sasl_username: u\n    sasl_password: p\n    oauth: { provider: idp }",
        );
        let mut config = parse(&yaml);
        let err = install(&mut config).unwrap_err().to_string();
        assert!(err.contains("target.security.oauth is set"), "{err}");
    }

    #[test]
    fn unknown_provider_and_plaintext_protocol_are_errors() {
        let mut config = parse(&format!("{BASE}{OAUTH}").replace(
            "oauth: { provider: idp, extensions: { logicalCluster: lkc-dst } }",
            "oauth: { provider: nope }",
        ));
        let err = install(&mut config).unwrap_err().to_string();
        assert!(
            err.contains("target.security.oauth")
                && err.contains("'nope'")
                && err.contains("defined: idp, other"),
            "{err}"
        );

        // No `oauth` section at all.
        let mut config = parse(BASE);
        let err = install(&mut config).unwrap_err().to_string();
        assert!(err.contains("defined: none"), "{err}");

        let mut config = parse(&format!("{}{OAUTH}", BASE.replace("SASL_SSL", "SSL")));
        let err = install(&mut config).unwrap_err().to_string();
        assert!(err.contains("security_protocol"), "{err}");

        // SASL_PLAINTEXT works (with a warning).
        let mut config = parse(&format!(
            "{}{OAUTH}",
            BASE.replace("SASL_SSL", "SASL_PLAINTEXT")
        ));
        install(&mut config).unwrap();
    }

    #[test]
    fn invalid_extensions_and_provider_names_are_errors() {
        let mut config = parse(&format!("{BASE}{OAUTH}"));
        config
            .source
            .as_mut()
            .unwrap()
            .security
            .oauth
            .as_mut()
            .unwrap()
            .extensions
            .logical_cluster = Some("lkc\u{1}x".into());
        let err = install(&mut config).unwrap_err().to_string();
        assert!(err.contains("source.security.oauth.extensions"), "{err}");

        let mut config =
            parse(&format!("{BASE}{OAUTH}").replace("    other:", "    \"bad name\":"));
        let err = install(&mut config).unwrap_err().to_string();
        assert!(err.contains("provider name `bad name`"), "{err}");
    }

    #[test]
    fn nothing_to_do_without_oauthbearer() {
        let mut config = parse(
            "mode: backup\nbackup_id: b\nsource: { bootstrap_servers: [\"s:9092\"] }\nstorage: { backend: memory }\n",
        );
        let installed = install(&mut config).unwrap();
        assert!(!installed.is_active());
        assert!(installed.caches.is_empty());

        // Providers nobody uses are not built.
        let mut config = parse(&format!(
            "mode: backup\nbackup_id: b\nsource: {{ bootstrap_servers: [\"s:9092\"] }}\nstorage: {{ backend: memory }}\n{OAUTH}"
        ));
        assert!(!install(&mut config).unwrap().is_active());
    }

    #[test]
    fn a_factory_installed_in_code_needs_no_binding() {
        use crate::kafka::{
            SaslMechanismPluginFactory, SaslMechanismPluginHandle, SaslPluginError,
        };

        #[derive(Debug)]
        struct Custom;
        impl SaslMechanismPluginFactory for Custom {
            fn mechanism_name(&self) -> &str {
                "OAUTHBEARER"
            }
            fn build(&self, _: &str, _: u16) -> Result<SaslMechanismPluginHandle, SaslPluginError> {
                unreachable!("not connecting")
            }
        }

        let yaml = BASE.replace(
            "\n    oauth: { provider: idp, extensions: { logicalCluster: lkc-src, identityPoolId: \"pool-a, pool-b\" } }",
            "",
        );
        let config = parse(&yaml);
        let mut source = config.source.unwrap();
        source.security.sasl_mechanism_plugin_factory = Some(Arc::new(Custom));
        let installed = install_cluster(None, KafkaRole::Source, &mut source).unwrap();
        assert!(!installed.is_active());
        assert!(format!(
            "{:?}",
            source.security.sasl_mechanism_plugin_factory.unwrap()
        )
        .contains("Custom"));
    }

    #[test]
    fn insecure_urls() {
        assert!(check_url("p", "https://idp/token", false).is_ok());
        assert!(check_url("p", "http://localhost:18080/kafka/token", false).is_ok());
        assert!(check_url("p", "http://127.0.0.1:1/t", false).is_ok());
        assert!(check_url("p", "http://[::1]:1/t", false).is_ok());
        assert!(check_url("p", "http://idp.internal/token", false).is_err());
        assert!(check_url("p", "http://idp.internal/token", true).is_ok());
        assert!(check_url("p", "ftp://idp/token", true).is_err());
        assert!(check_url("p", "not a url", true).is_err());
    }

    #[test]
    fn install_cluster_for_offset_commands() {
        let config = parse(&format!("{BASE}{OAUTH}"));
        let mut target = config.target.clone().unwrap();
        let installed =
            install_cluster(config.oauth.as_ref(), KafkaRole::Target, &mut target).unwrap();
        assert!(installed.is_active());
        assert_eq!(
            installed.uses,
            vec!["target Kafka cluster (provider 'idp')"]
        );
        assert!(target.security.sasl_mechanism_plugin_factory.is_some());
        // The binding names a provider, but there is no `oauth` section.
        let mut target = config.target.clone().unwrap();
        assert!(install_cluster(None, KafkaRole::Target, &mut target).is_err());
    }

    #[test]
    fn workload_identity_with_explicit_settings_and_bad_authority() {
        let wi = ProviderConfig::AzureWorkloadIdentity(crate::oauth::AzureWorkloadIdentityConfig {
            scope: "api://app/.default".into(),
            tenant_id: Some("tenant".into()),
            client_id: Some("client".into()),
            token_file: Some("/var/run/secrets/azure/tokens/azure-identity-token".into()),
            authority_host: Some("https://login.microsoftonline.com/".into()),
        });
        let provider = build_provider("wi", &wi).unwrap();
        assert_eq!(provider.kind(), "azure_workload_identity");
        let bad =
            ProviderConfig::AzureWorkloadIdentity(crate::oauth::AzureWorkloadIdentityConfig {
                authority_host: Some("http://login.example.com/".into()),
                ..match wi {
                    ProviderConfig::AzureWorkloadIdentity(w) => w,
                    _ => unreachable!(),
                }
            });
        assert!(build_provider("wi", &bad).is_err());
    }

    #[test]
    fn managed_identity_needs_resource_or_scope() {
        let mi = ProviderConfig::AzureManagedIdentity(Default::default());
        assert!(build_provider("mi", &mi)
            .unwrap_err()
            .to_string()
            .contains("resource"));
    }
}
