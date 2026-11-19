//! Wire configuration-file `sasl_mechanism: GSSAPI` into a runtime
//! [`SaslMechanismPluginFactory`].
//!
//! Call [`populate_sasl_plugin`] immediately after parsing the YAML
//! config; it inspects `security.sasl_mechanism` and, for mechanisms
//! that require a plugin (currently just `GSSAPI`), constructs a
//! factory and sets `security.sasl_mechanism_plugin_factory`.
//!
//! `OAUTHBEARER` gets its factory from the configuration's `oauth`
//! section and the cluster's `security.oauth` binding ([`super::oauth`]),
//! which runs first; reaching here without one (for example
//! `--sasl-mechanism OAUTHBEARER` without `--config`) is an error that says
//! how to configure it.
//!
//! Binaries built without `--features gssapi` surface a clear runtime
//! error when the config requests `GSSAPI` — the default build stays
//! lean and the error message tells the operator exactly how to rebuild.
//!
//! The factory receives the broker endpoint at `KafkaClient::authenticate`
//! time, so each per-broker client authenticates against its own SPN
//! (`kafka/brokerN.fqdn@REALM`) even on multi-broker clusters.

use anyhow::{anyhow, Result};
use kafka_backup_core::config::{KafkaConfig, SaslMechanism, SecurityConfig};

/// Inspect the config's SASL mechanism and, if it needs a plugin,
/// construct a factory and wire it into
/// `security.sasl_mechanism_plugin_factory`.
pub fn populate_sasl_plugin(security: &mut SecurityConfig) -> Result<()> {
    match security.sasl_mechanism {
        Some(SaslMechanism::Gssapi) => install_gssapi_plugin(security),
        // `super::oauth` installs the OAUTHBEARER factory before this runs;
        // without one there is no token source.
        Some(SaslMechanism::OAuthBearer) if security.sasl_mechanism_plugin_factory.is_none() => {
            Err(anyhow!(OAUTHBEARER_NEEDS_PROVIDER))
        }
        _ => Ok(()),
    }
}

/// OAUTHBEARER reached a connection without a token provider (e.g.
/// `--sasl-mechanism OAUTHBEARER` without `--config`).
pub const OAUTHBEARER_NEEDS_PROVIDER: &str = "SASL OAUTHBEARER needs a token provider, which \
     flags cannot describe: pass --config <yaml> --cluster source|target, where the YAML defines \
     the provider under oauth.providers and binds the cluster with \
     <cluster>.security.oauth.provider";

/// Convenience wrapper for `Option<KafkaConfig>` config slots.
pub fn populate_sasl_plugin_opt(kafka: &mut Option<KafkaConfig>) -> Result<()> {
    if let Some(cfg) = kafka.as_mut() {
        populate_sasl_plugin(&mut cfg.security)?;
    }
    Ok(())
}

#[cfg(feature = "gssapi")]
fn install_gssapi_plugin(security: &mut SecurityConfig) -> Result<()> {
    use kafka_backup_core::kafka::GssapiPluginFactory;

    let service_name = security
        .sasl_kerberos_service_name
        .clone()
        .unwrap_or_else(|| "kafka".to_string());

    let factory = GssapiPluginFactory::new(
        service_name,
        security.sasl_keytab_path.clone(),
        security.sasl_krb5_config_path.clone(),
    )
    .map_err(|e| anyhow!("failed to construct GSSAPI plugin factory: {e}"))?;

    security.sasl_mechanism_plugin_factory = Some(factory.into_handle());
    Ok(())
}

#[cfg(not(feature = "gssapi"))]
fn install_gssapi_plugin(_security: &mut SecurityConfig) -> Result<()> {
    Err(anyhow!(
        "sasl_mechanism: GSSAPI requires the `gssapi` feature. \
         Rebuild with: cargo build --release --features gssapi -p kafka-backup-cli. \
         (Requires system krb5 dev headers: `apt-get install libkrb5-dev` / \
         `dnf install krb5-devel` / `brew install krb5`.)"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_gssapi_mechanism_is_noop() {
        let mut security = SecurityConfig {
            sasl_mechanism: Some(SaslMechanism::Plain),
            ..Default::default()
        };
        populate_sasl_plugin(&mut security).unwrap();
        assert!(security.sasl_mechanism_plugin_factory.is_none());
    }

    #[test]
    fn no_mechanism_is_noop() {
        let mut security = SecurityConfig::default();
        populate_sasl_plugin(&mut security).unwrap();
        assert!(security.sasl_mechanism_plugin_factory.is_none());
    }

    #[test]
    fn oauthbearer_without_plugin_explains_how_to_configure_a_provider() {
        let mut security = SecurityConfig {
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            ..Default::default()
        };
        let msg = format!("{}", populate_sasl_plugin(&mut security).unwrap_err());
        assert!(msg.contains("oauth.providers"), "{msg}");
        assert!(msg.contains("security.oauth.provider"), "{msg}");
        assert!(msg.contains("--config"), "{msg}");
    }

    #[test]
    fn oauthbearer_keeps_an_installed_plugin_factory() {
        use kafka_backup_core::kafka::{
            SaslMechanismPluginFactory, SaslMechanismPluginHandle, SaslPluginError,
        };

        // What `commands::oauth` installs before this runs.
        #[derive(Debug)]
        struct StubFactory;
        impl SaslMechanismPluginFactory for StubFactory {
            fn mechanism_name(&self) -> &str {
                "OAUTHBEARER"
            }
            fn build(&self, _: &str, _: u16) -> Result<SaslMechanismPluginHandle, SaslPluginError> {
                unreachable!("not connecting")
            }
        }

        let mut security = SecurityConfig {
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            sasl_mechanism_plugin_factory: Some(std::sync::Arc::new(StubFactory)),
            ..Default::default()
        };
        populate_sasl_plugin(&mut security).unwrap();
        assert_eq!(
            security
                .sasl_mechanism_plugin_factory
                .expect("kept")
                .mechanism_name(),
            "OAUTHBEARER"
        );
    }

    #[cfg(not(feature = "gssapi"))]
    #[test]
    fn gssapi_without_feature_returns_actionable_error() {
        let mut security = SecurityConfig {
            sasl_mechanism: Some(SaslMechanism::Gssapi),
            ..Default::default()
        };
        let err = populate_sasl_plugin(&mut security).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("--features gssapi"),
            "expected rebuild hint: {msg}"
        );
        assert!(
            msg.contains("libkrb5") || msg.contains("krb5-devel") || msg.contains("brew"),
            "expected platform install hints: {msg}"
        );
    }

    #[cfg(feature = "gssapi")]
    #[test]
    fn gssapi_with_feature_installs_factory() {
        let mut security = SecurityConfig {
            sasl_mechanism: Some(SaslMechanism::Gssapi),
            sasl_kerberos_service_name: Some("kafka".to_string()),
            ..Default::default()
        };
        populate_sasl_plugin(&mut security).unwrap();
        let factory = security
            .sasl_mechanism_plugin_factory
            .expect("factory installed");
        assert_eq!(factory.mechanism_name(), "GSSAPI");
    }
}
