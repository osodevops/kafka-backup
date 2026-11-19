//! Shared wiring for OAuth (SASL/OAUTHBEARER).
//!
//! Every command that connects to Kafka calls [`apply`] (or
//! [`apply_cluster`]) right after parsing its configuration, before any
//! engine, router or client is built, so the validation and the token
//! caches cannot drift between commands.

use anyhow::Result;
use kafka_backup_core::config::{Config, KafkaConfig};
use kafka_backup_core::oauth::{self, KafkaRole, OauthConfig, OauthInstall};

/// Install the configuration's `oauth` providers on its clusters
/// ([`oauth::install`]).
pub fn apply(config: &mut Config) -> Result<OauthInstall> {
    Ok(oauth::install(config)?)
}

/// Install `oauth` for one cluster ([`oauth::install_cluster`]).
pub fn apply_cluster(
    oauth: Option<&OauthConfig>,
    role: KafkaRole,
    kafka: &mut KafkaConfig,
) -> Result<OauthInstall> {
    Ok(oauth::install_cluster(oauth, role, kafka)?)
}
