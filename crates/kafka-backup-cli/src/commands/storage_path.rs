use anyhow::{bail, Result};
use kafka_backup_core::storage::{
    create_backend, FilesystemBackend, StorageBackend, StorageBackendConfig,
};
use std::path::PathBuf;
use std::sync::Arc;

pub fn backend_from_path(path: &str) -> Result<Arc<dyn StorageBackend>> {
    if path.contains("://") {
        let config = StorageBackendConfig::from_url(path)?;
        return Ok(create_backend(&config)?);
    }

    Ok(Arc::new(FilesystemBackend::new(PathBuf::from(path))))
}

/// Resolve the storage backend and backup id from either `--config` (the
/// backup/restore config file — its storage `prefix` and `backup_id` apply) or
/// an explicit `--path` + `--backup-id` pair. Shared by prune / describe /
/// validate so the two conventions behave identically (issue #168).
pub async fn resolve_target(
    config: Option<&str>,
    path: Option<&str>,
    backup_id: Option<&str>,
) -> Result<(Arc<dyn StorageBackend>, String)> {
    match (config, path, backup_id) {
        (Some(config_path), None, None) => {
            let content = tokio::fs::read_to_string(config_path).await?;
            let content = super::config::expand_env_vars(&content);
            let cfg = super::config::parse_config(&content)?;
            Ok((create_backend(&cfg.storage)?, cfg.backup_id))
        }
        (None, Some(path), Some(id)) => Ok((backend_from_path(path)?, id.to_string())),
        _ => bail!("pass either --config, or --path together with --backup-id"),
    }
}
