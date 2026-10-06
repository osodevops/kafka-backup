use anyhow::{bail, Result};
use kafka_backup_core::storage::{
    create_backend, FilesystemBackend, StorageBackend, StorageBackendConfig,
};
use std::path::PathBuf;
use std::sync::Arc;

// The single sanctioned FilesystemBackend construction for user-supplied
// paths; clippy.toml disallows it everywhere else in this crate (#174).
#[allow(clippy::disallowed_methods)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn backend_kind(path: &str) -> String {
        backend_from_path(path)
            .unwrap_or_else(|e| panic!("{path:?} should resolve: {e:#}"))
            .backend_name()
            .to_string()
    }

    fn rejection(path: &str) -> String {
        match backend_from_path(path) {
            Ok(b) => panic!(
                "{path:?} should be rejected, got {} backend",
                b.backend_name()
            ),
            Err(e) => format!("{e:#}"),
        }
    }

    #[test]
    fn bare_absolute_path_uses_filesystem_backend() {
        assert_eq!(
            backend_kind("/var/lib/kafka-backup/snapshots"),
            "filesystem"
        );
    }

    #[test]
    fn bare_relative_path_uses_filesystem_backend() {
        assert_eq!(backend_kind("snapshots/dev"), "filesystem");
    }

    #[test]
    fn windows_drive_paths_are_not_parsed_as_url_schemes() {
        assert_eq!(backend_kind(r"C:\kafka-backup"), "filesystem");
        assert_eq!(backend_kind("C:/kafka-backup"), "filesystem");
    }

    #[tokio::test]
    async fn file_url_writes_under_the_url_path_not_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let backend = backend_from_path(&format!("file://{}", dir.path().display())).unwrap();
        assert_eq!(backend.backend_name(), "filesystem");

        backend
            .put(
                "offset-snapshots/x/metadata.json",
                Bytes::from_static(b"{}"),
            )
            .await
            .unwrap();
        assert!(dir.path().join("offset-snapshots/x/metadata.json").exists());
    }

    #[test]
    fn s3_url_builds_s3_backend_without_network() {
        assert_eq!(backend_kind("s3://bucket/prefix?region=us-east-1"), "s3");
    }

    #[test]
    fn uppercase_scheme_is_accepted() {
        assert_eq!(backend_kind("S3://bucket/prefix?region=us-east-1"), "s3");
    }

    #[test]
    fn unknown_scheme_is_rejected() {
        let err = rejection("bogus://x");
        assert!(err.contains("Unknown storage scheme: bogus"), "{err}");
    }

    #[test]
    fn path_with_scheme_separator_that_is_not_a_url_is_rejected() {
        let err = rejection("backups/s3://x");
        assert!(err.contains("Invalid storage URL"), "{err}");
    }

    #[test]
    fn s3_url_without_bucket_is_rejected() {
        let err = rejection("s3://");
        assert!(err.contains("bucket"), "{err}");
    }

    // A known scheme with a mangled separator would otherwise be taken as a
    // relative directory and written to locally - the #174 failure mode.
    #[test]
    fn known_scheme_missing_double_slash_is_rejected() {
        for path in [
            "s3:/bucket/prefix",
            "s3:bucket",
            "gcs:/bucket",
            "file:/tmp/x",
        ] {
            let err = rejection(path);
            assert!(err.contains("did you mean"), "{path}: {err}");
        }
    }

    // memory:// lives only for one process, so for a CLI it silently discards
    // whatever is written - the same data loss as #174.
    #[test]
    fn memory_url_is_rejected_for_cli() {
        let err = rejection("memory://");
        assert!(err.contains("memory://"), "{err}");
    }

    #[test]
    fn empty_path_is_rejected() {
        let err = rejection("");
        assert!(err.contains("empty"), "{err}");
    }
}
