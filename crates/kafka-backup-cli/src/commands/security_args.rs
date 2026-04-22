//! Shared SASL/SSL CLI args for offset-reset commands.
//!
//! The offset-reset commands (`offset-reset execute`, `offset-reset-bulk`,
//! `offset-rollback rollback` / `verify`) all need to construct a Kafka
//! client to apply offset changes on the target cluster. They share a
//! common set of security flags; keeping the struct here removes the
//! triplicated hand-coded `parse_security_config` helpers the three
//! commands used to carry.
//!
//! Consumers embed [`SecurityCliArgs`] via `#[command(flatten)]`:
//!
//! ```ignore
//! #[derive(Parser)]
//! struct Execute {
//!     /* ...other flags... */
//!     #[command(flatten)]
//!     security: SecurityCliArgs,
//! }
//! ```
//!
//! [`SecurityCliArgs::into_security_config`] produces a ready-to-use
//! `SecurityConfig`, including installing the SASL plugin for GSSAPI.

use anyhow::Result;
use clap::Args;
use kafka_backup_core::config::{SaslMechanism, SecurityConfig, SecurityProtocol};
use std::path::PathBuf;

/// SASL / SSL / Kerberos flags shared by all offset-reset-family
/// commands. Populate via `#[command(flatten)]` on a clap-derived
/// command struct.
#[derive(Debug, Clone, Args)]
pub struct SecurityCliArgs {
    /// Security protocol (PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT).
    #[arg(long)]
    pub security_protocol: Option<String>,

    /// SASL mechanism (PLAIN, SCRAM-SHA256, SCRAM-SHA512, GSSAPI).
    /// Required when security_protocol is SASL_*.
    #[arg(long, env = "KAFKA_SASL_MECHANISM")]
    pub sasl_mechanism: Option<String>,

    /// Path to Kerberos keytab file. Only used with GSSAPI.
    /// If unset, the OS credential cache (kinit) is used.
    #[arg(long, env = "KAFKA_SASL_KEYTAB")]
    pub sasl_keytab: Option<PathBuf>,

    /// Path to Kerberos `krb5.conf`. Only used with GSSAPI.
    /// If unset, the system default is used.
    #[arg(long, env = "KAFKA_KRB5_CONFIG")]
    pub sasl_krb5_config: Option<PathBuf>,

    /// Kerberos service name — must match the broker's
    /// `sasl.kerberos.service.name`. Defaults to `kafka`.
    #[arg(long, env = "KAFKA_SASL_KERBEROS_SERVICE_NAME")]
    pub sasl_kerberos_service_name: Option<String>,
}

impl SecurityCliArgs {
    /// Build a [`SecurityConfig`] from the parsed flags plus
    /// environment (`KAFKA_USERNAME` / `KAFKA_PASSWORD` /
    /// `KAFKA_SSL_CA_CERT` preserve the pre-existing behavior of the
    /// hand-coded helpers).
    ///
    /// For SASL/GSSAPI, installs the plugin factory via
    /// [`crate::commands::sasl_plugin::populate_sasl_plugin`].
    pub fn into_security_config(self) -> Result<SecurityConfig> {
        let security_protocol = parse_security_protocol(self.security_protocol.as_deref());
        let sasl_mechanism =
            parse_sasl_mechanism(self.sasl_mechanism.as_deref(), security_protocol)?;

        let needs_sasl_creds = matches!(
            sasl_mechanism,
            Some(SaslMechanism::Plain | SaslMechanism::ScramSha256 | SaslMechanism::ScramSha512)
        );
        let (sasl_username, sasl_password) = if needs_sasl_creds {
            (
                std::env::var("KAFKA_USERNAME").ok(),
                std::env::var("KAFKA_PASSWORD").ok(),
            )
        } else {
            (None, None)
        };

        let ssl_ca_location = if matches!(
            security_protocol,
            SecurityProtocol::Ssl | SecurityProtocol::SaslSsl
        ) {
            std::env::var("KAFKA_SSL_CA_CERT").ok().map(PathBuf::from)
        } else {
            None
        };

        let mut security = SecurityConfig {
            security_protocol,
            sasl_mechanism,
            sasl_username,
            sasl_password,
            ssl_ca_location,
            ssl_certificate_location: None,
            ssl_key_location: None,
            sasl_kerberos_service_name: self.sasl_kerberos_service_name,
            sasl_keytab_path: self.sasl_keytab,
            sasl_krb5_config_path: self.sasl_krb5_config,
            sasl_mechanism_plugin_factory: None,
        };

        super::sasl_plugin::populate_sasl_plugin(&mut security)?;

        Ok(security)
    }
}

fn parse_security_protocol(protocol: Option<&str>) -> SecurityProtocol {
    match protocol.map(|p| p.to_uppercase()).as_deref() {
        Some("SASL_SSL") => SecurityProtocol::SaslSsl,
        Some("SSL") => SecurityProtocol::Ssl,
        Some("SASL_PLAINTEXT") => SecurityProtocol::SaslPlaintext,
        _ => SecurityProtocol::Plaintext,
    }
}

fn parse_sasl_mechanism(
    mechanism: Option<&str>,
    protocol: SecurityProtocol,
) -> Result<Option<SaslMechanism>> {
    // Only meaningful under SASL_* protocols.
    if !matches!(
        protocol,
        SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
    ) {
        return Ok(None);
    }
    // Pre-GSSAPI behavior: if no mechanism is specified under a SASL
    // protocol, fall back to PLAIN. Keeps existing scripts working.
    let Some(m) = mechanism else {
        return Ok(Some(SaslMechanism::Plain));
    };
    match m.to_uppercase().as_str() {
        "PLAIN" => Ok(Some(SaslMechanism::Plain)),
        "SCRAM-SHA256" | "SCRAM-SHA-256" => Ok(Some(SaslMechanism::ScramSha256)),
        "SCRAM-SHA512" | "SCRAM-SHA-512" => Ok(Some(SaslMechanism::ScramSha512)),
        "GSSAPI" => Ok(Some(SaslMechanism::Gssapi)),
        other => Err(anyhow::anyhow!(
            "unsupported sasl_mechanism '{other}' — expected one of: \
             PLAIN, SCRAM-SHA256, SCRAM-SHA512, GSSAPI"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_args() -> SecurityCliArgs {
        SecurityCliArgs {
            security_protocol: None,
            sasl_mechanism: None,
            sasl_keytab: None,
            sasl_krb5_config: None,
            sasl_kerberos_service_name: None,
        }
    }

    #[test]
    fn plaintext_default_has_no_sasl() {
        let sec = default_args().into_security_config().unwrap();
        assert_eq!(sec.security_protocol, SecurityProtocol::Plaintext);
        assert!(sec.sasl_mechanism.is_none());
    }

    #[test]
    fn sasl_protocol_defaults_to_plain_when_mechanism_absent() {
        let mut args = default_args();
        args.security_protocol = Some("SASL_PLAINTEXT".to_string());
        let sec = args.into_security_config().unwrap();
        assert_eq!(sec.sasl_mechanism, Some(SaslMechanism::Plain));
    }

    #[test]
    fn scram_variants_parse_with_and_without_intra_hyphen() {
        for spelling in ["SCRAM-SHA256", "SCRAM-SHA-256", "scram-sha256"] {
            let mut args = default_args();
            args.security_protocol = Some("SASL_SSL".to_string());
            args.sasl_mechanism = Some(spelling.to_string());
            let sec = args.into_security_config().unwrap();
            assert_eq!(
                sec.sasl_mechanism,
                Some(SaslMechanism::ScramSha256),
                "{spelling}"
            );
        }
    }

    #[test]
    fn unknown_mechanism_is_rejected() {
        let mut args = default_args();
        args.security_protocol = Some("SASL_SSL".to_string());
        args.sasl_mechanism = Some("KERBEROS-V5".to_string());
        let err = args.into_security_config().unwrap_err();
        assert!(format!("{err}").contains("unsupported sasl_mechanism"));
    }

    #[cfg(feature = "gssapi")]
    #[test]
    fn gssapi_wires_all_kerberos_fields_and_installs_factory() {
        let mut args = default_args();
        args.security_protocol = Some("SASL_PLAINTEXT".to_string());
        args.sasl_mechanism = Some("GSSAPI".to_string());
        args.sasl_kerberos_service_name = Some("kafka".to_string());
        let sec = args.into_security_config().unwrap();
        assert_eq!(sec.sasl_mechanism, Some(SaslMechanism::Gssapi));
        assert_eq!(sec.sasl_kerberos_service_name.as_deref(), Some("kafka"));
        let factory = sec
            .sasl_mechanism_plugin_factory
            .expect("factory installed");
        assert_eq!(factory.mechanism_name(), "GSSAPI");
    }

    #[cfg(not(feature = "gssapi"))]
    #[test]
    fn gssapi_without_feature_produces_actionable_error() {
        let mut args = default_args();
        args.security_protocol = Some("SASL_PLAINTEXT".to_string());
        args.sasl_mechanism = Some("GSSAPI".to_string());
        let err = args.into_security_config().unwrap_err();
        assert!(format!("{err}").contains("--features gssapi"));
    }
}
