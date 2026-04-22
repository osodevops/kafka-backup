//! Wire configuration-file `sasl_mechanism: GSSAPI` into a runtime
//! [`SaslMechanismPluginFactory`].
//!
//! Call [`populate_sasl_plugin`] immediately after parsing the YAML
//! config; it inspects `security.sasl_mechanism` and, for mechanisms
//! that require a plugin (currently just `GSSAPI`), constructs a
//! factory and sets `security.sasl_mechanism_plugin_factory`.
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
        _ => Ok(()),
    }
}

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
