use anyhow::Result;
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
