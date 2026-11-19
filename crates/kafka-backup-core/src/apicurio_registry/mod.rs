//! Apicurio Registry backup (API v2 and v3).
//!
//! - [`config`]: the top-level `apicurio_registry` configuration section.
//! - [`client`]: the REST client (basic, mTLS and OIDC authentication, rate
//!   limit, retries of reads).
//! - [`backup`]: [`ApicurioRegistryBackup`], one snapshot pass: groups,
//!   artifacts, versions with their content, references, rules at the
//!   global, group and artifact level, and the registry's export archive.
//!
//! Restore is in [`crate::registry_restore`].

pub mod backup;
pub mod client;
pub mod config;
pub mod error;
pub mod models;

pub use backup::ApicurioRegistryBackup;
pub use client::ApicurioRegistryClient;
pub use config::{
    ApicurioApiVersion, ApicurioBackupOptions, ApicurioConnectionConfig, ApicurioRegistryAuth,
    ApicurioRegistryConfig, ApicurioRegistryTls,
};
pub use error::ApicurioRegistryError;

/// Take one snapshot of the registry in `config.apicurio_registry` into
/// `{backup_id}/apicurio-registry/` of `storage`.
pub async fn snapshot(
    config: &crate::Config,
    storage: std::sync::Arc<dyn crate::storage::StorageBackend>,
) -> Result<models::ApicurioRegistryManifest, ApicurioRegistryError> {
    let ap = config
        .apicurio_registry
        .as_ref()
        .filter(|ap| ap.enabled)
        .cloned()
        .ok_or_else(|| {
            ApicurioRegistryError::Config(
                "no enabled apicurio_registry section in the configuration".to_string(),
            )
        })?;
    ap.validate()
        .map_err(|e| ApicurioRegistryError::Config(e.to_string()))?;
    let client = ApicurioRegistryClient::new(&ap)?;
    ApicurioRegistryBackup::new(client, storage, config.backup_id.clone(), ap)
        .run()
        .await
}
