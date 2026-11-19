//! Connect the offset commands to a cluster described in a backup/restore
//! configuration file.
//!
//! The offset commands take `--bootstrap-servers` plus the flattened
//! [`SecurityCliArgs`] (credentials from `KAFKA_USERNAME` / `KAFKA_PASSWORD`).
//! `--config <yaml> --cluster source|target` sits next to them, so the same
//! `security:` block a backup or restore uses (TLS files, SASL credentials,
//! GSSAPI, OAuth) applies to `offset-reset execute`, `offset-reset-bulk`
//! and `offset-rollback`. A cluster with `sasl_mechanism: OAUTHBEARER` gets
//! its token provider from the file's `oauth` section, which the flags
//! cannot express.

use anyhow::{anyhow, Result};
use clap::{Args, ValueEnum};
use kafka_backup_core::config::SecurityConfig;
use kafka_backup_core::oauth::KafkaRole;
use tracing::warn;

use super::security_args::SecurityCliArgs;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ClusterRole {
    Source,
    Target,
}

impl ClusterRole {
    fn key(self) -> &'static str {
        match self {
            ClusterRole::Source => "source",
            ClusterRole::Target => "target",
        }
    }

    fn oauth_role(self) -> KafkaRole {
        match self {
            ClusterRole::Source => KafkaRole::Source,
            ClusterRole::Target => KafkaRole::Target,
        }
    }
}

#[derive(Debug, Clone, Args)]
pub struct ClusterConfigArgs {
    /// Take the cluster's bootstrap servers and security settings (TLS,
    /// SASL, OAuth) from this backup/restore configuration file. An explicit
    /// --bootstrap-servers still overrides the file's list
    #[arg(long = "config", value_name = "FILE")]
    pub config: Option<String>,

    /// Which cluster in --config to connect to (default: target, the
    /// cluster a restore writes to)
    #[arg(long, value_enum, requires = "config")]
    pub cluster: Option<ClusterRole>,
}

/// The bootstrap servers and security settings an offset command connects
/// with: from `--config` when given, otherwise from the flags.
pub async fn resolve_cluster(
    cluster: &ClusterConfigArgs,
    bootstrap_servers: &[String],
    security: SecurityCliArgs,
) -> Result<(Vec<String>, SecurityConfig)> {
    let Some(path) = cluster.config.as_deref() else {
        return Ok((bootstrap_servers.to_vec(), security.into_security_config()?));
    };
    let role = cluster.cluster.unwrap_or(ClusterRole::Target);

    if security.security_protocol.is_some()
        || security.sasl_mechanism.is_some()
        || security.sasl_keytab.is_some()
        || security.sasl_krb5_config.is_some()
        || security.sasl_kerberos_service_name.is_some()
    {
        warn!(
            "--security-protocol / --sasl-* (or their KAFKA_* environment variables) are ignored: \
             the {} cluster's security settings come from {path}",
            role.key()
        );
    }

    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| anyhow!("cannot read --config {path}: {e}"))?;
    let content = super::config::expand_env_vars(&content);
    let config = super::config::parse_config(&content)?;
    let oauth = config.oauth;
    let mut kafka = match role {
        ClusterRole::Source => config.source,
        ClusterRole::Target => config.target,
    }
    .ok_or_else(|| anyhow!("{path} has no `{}:` cluster section", role.key()))?;

    // OAUTHBEARER: the cluster's `security.oauth` binding and the file's
    // `oauth.providers`.
    super::oauth::apply_cluster(oauth.as_ref(), role.oauth_role(), &mut kafka)?;
    let mut security = kafka.security;
    super::sasl_plugin::populate_sasl_plugin(&mut security)?;

    let servers = if bootstrap_servers.is_empty() {
        kafka.bootstrap_servers
    } else {
        bootstrap_servers.to_vec()
    };
    if servers.is_empty() {
        anyhow::bail!(
            "no bootstrap servers: {path} `{}.bootstrap_servers` is empty and --bootstrap-servers was not given",
            role.key()
        );
    }
    Ok((servers, security))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_backup_core::config::{SaslMechanism, SecurityProtocol};

    fn no_flags() -> SecurityCliArgs {
        SecurityCliArgs {
            security_protocol: None,
            sasl_mechanism: None,
            sasl_keytab: None,
            sasl_krb5_config: None,
            sasl_kerberos_service_name: None,
        }
    }

    fn write_config(dir: &tempfile::TempDir) -> String {
        let path = dir.path().join("restore.yaml");
        std::fs::write(
            &path,
            r#"
mode: restore
backup_id: b
source:
  bootstrap_servers: ["source-1:9092"]
target:
  bootstrap_servers: ["target-1:9093", "target-2:9093"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA512
    sasl_username: svc
    sasl_password: secret
storage:
  backend: filesystem
  path: /tmp/b
"#,
        )
        .unwrap();
        path.display().to_string()
    }

    #[tokio::test]
    async fn without_config_the_flags_apply() {
        let args = ClusterConfigArgs {
            config: None,
            cluster: None,
        };
        let (servers, security) = resolve_cluster(&args, &["b:9092".to_string()], no_flags())
            .await
            .unwrap();
        assert_eq!(servers, vec!["b:9092"]);
        assert_eq!(security.security_protocol, SecurityProtocol::Plaintext);
    }

    #[tokio::test]
    async fn config_defaults_to_the_target_cluster_and_its_security() {
        let dir = tempfile::tempdir().unwrap();
        let args = ClusterConfigArgs {
            config: Some(write_config(&dir)),
            cluster: None,
        };
        let (servers, security) = resolve_cluster(&args, &[], no_flags()).await.unwrap();
        assert_eq!(servers, vec!["target-1:9093", "target-2:9093"]);
        assert_eq!(security.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(security.sasl_mechanism, Some(SaslMechanism::ScramSha512));
        assert_eq!(security.sasl_username.as_deref(), Some("svc"));
    }

    #[tokio::test]
    async fn source_cluster_and_bootstrap_override() {
        let dir = tempfile::tempdir().unwrap();
        let args = ClusterConfigArgs {
            config: Some(write_config(&dir)),
            cluster: Some(ClusterRole::Source),
        };
        let (servers, security) = resolve_cluster(&args, &[], no_flags()).await.unwrap();
        assert_eq!(servers, vec!["source-1:9092"]);
        assert_eq!(security.security_protocol, SecurityProtocol::Plaintext);

        let (servers, _) = resolve_cluster(&args, &["override:9092".to_string()], no_flags())
            .await
            .unwrap();
        assert_eq!(servers, vec!["override:9092"]);
    }

    #[tokio::test]
    async fn missing_cluster_section_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("backup.yaml");
        std::fs::write(
            &path,
            "mode: backup\nbackup_id: b\nsource:\n  bootstrap_servers: [\"s:9092\"]\nstorage:\n  backend: filesystem\n  path: /tmp/b\n",
        )
        .unwrap();
        let args = ClusterConfigArgs {
            config: Some(path.display().to_string()),
            cluster: Some(ClusterRole::Target),
        };
        let err = resolve_cluster(&args, &[], no_flags()).await.unwrap_err();
        assert!(
            format!("{err:#}").contains("no `target:` cluster section"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn oauthbearer_cluster_gets_its_provider_from_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("restore.yaml");
        std::fs::write(
            &path,
            r#"
mode: restore
backup_id: b
target:
  bootstrap_servers: ["target-1:9093"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth: { provider: idp, extensions: { logicalCluster: lkc-1 } }
storage:
  backend: filesystem
  path: /tmp/b
oauth:
  providers:
    idp: { type: file, path: /var/run/secrets/token }
"#,
        )
        .unwrap();
        let args = ClusterConfigArgs {
            config: Some(path.display().to_string()),
            cluster: None,
        };
        let (_, security) = resolve_cluster(&args, &[], no_flags()).await.unwrap();
        let factory = security
            .sasl_mechanism_plugin_factory
            .expect("OAUTHBEARER factory installed");
        assert_eq!(factory.mechanism_name(), "OAUTHBEARER");
        assert!(format!("{factory:?}").contains("lkc-1"));
    }
}
