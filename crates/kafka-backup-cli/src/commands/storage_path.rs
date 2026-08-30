use anyhow::{Context, Result};
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

/// Resolve a storage backend and backup ID from either CLI path arguments or a
/// full backup configuration. An explicit backup ID overrides the value from
/// the configuration, which is useful for scheduled jobs that reuse a config.
pub fn backend_and_backup_id(
    path: Option<&str>,
    config_path: Option<&str>,
    backup_id: Option<&str>,
) -> Result<(Arc<dyn StorageBackend>, String)> {
    if let Some(config_path) = config_path {
        let config_content = std::fs::read_to_string(config_path)
            .with_context(|| format!("Failed to read config file: {config_path}"))?;
        let config_content = super::config::expand_env_vars(&config_content);
        let config = super::config::parse_config(&config_content)
            .with_context(|| format!("Failed to parse config file: {config_path}"))?;
        let backup_id = backup_id.unwrap_or(&config.backup_id).to_string();

        return Ok((create_backend(&config.storage)?, backup_id));
    }

    let path = path.ok_or_else(|| anyhow::anyhow!("Either --config or --path is required"))?;
    let backup_id =
        backup_id.ok_or_else(|| anyhow::anyhow!("--backup-id is required when using --path"))?;

    Ok((backend_from_path(path)?, backup_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn config_file(contents: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("create temporary config");
        file.write_all(contents.as_bytes())
            .expect("write temporary config");
        file
    }

    #[test]
    fn config_expands_backup_id_environment_variable() {
        std::env::set_var("ISSUE_168_BACKUP_ID", "scheduled-backup");
        let file = config_file(
            "mode: backup\nbackup_id: ${ISSUE_168_BACKUP_ID}\nstorage:\n  backend: memory\n",
        );

        let (_, backup_id) =
            backend_and_backup_id(None, Some(file.path().to_str().expect("UTF-8 path")), None)
                .expect("resolve config storage");

        std::env::remove_var("ISSUE_168_BACKUP_ID");
        assert_eq!(backup_id, "scheduled-backup");
    }

    #[test]
    fn explicit_backup_id_overrides_config_value() {
        let file =
            config_file("mode: backup\nbackup_id: from-config\nstorage:\n  backend: memory\n");

        let (_, backup_id) = backend_and_backup_id(
            None,
            Some(file.path().to_str().expect("UTF-8 path")),
            Some("override-id"),
        )
        .expect("resolve config storage");

        assert_eq!(backup_id, "override-id");
    }
}
