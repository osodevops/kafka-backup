//! Errors from the Schema Registry client, backup and restore.

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SchemaRegistryError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Schema Registry API error (status {status}): {message}")]
    Api {
        status: u16,
        error_code: i32,
        message: String,
    },

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("Subject not found: {0}")]
    SubjectNotFound(String),

    #[error("Rate limit exceeded after {attempts} retry attempts")]
    RateLimitExhausted { attempts: u32 },

    #[error("JSON serialization error: {0}")]
    Serialization(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Circular schema reference detected: {0}")]
    ReferenceCycle(String),

    #[error("Configuration error: {0}")]
    Config(String),

    /// The snapshot in storage is incomplete, corrupt or of another format.
    #[error("Schema Registry snapshot: {0}")]
    Snapshot(String),

    /// Restore refused before any change: the target already uses an
    /// original schema ID or version for a different schema.
    #[error("Schema Registry restore conflict: {0}")]
    RestoreConflict(String),

    /// The target registry's schema version limit would be (or was) exceeded.
    #[error("Schema Registry capacity exceeded: {0}")]
    CapacityExceeded(String),

    /// The restored registry does not match the backup.
    #[error("Schema Registry restore verification failed: {0}")]
    VerificationFailed(String),

    /// The restore was cancelled (SIGINT / SIGTERM); the target's modes were
    /// put back.
    #[error("Schema Registry restore cancelled: {0}")]
    Cancelled(String),

    #[error("{0}")]
    Other(String),
}

impl From<SchemaRegistryError> for crate::Error {
    fn from(e: SchemaRegistryError) -> Self {
        match e {
            SchemaRegistryError::Config(m) => crate::Error::Config(format!("schema_registry: {m}")),
            SchemaRegistryError::Auth(m) => {
                crate::Error::Authentication(format!("Schema Registry: {m}"))
            }
            other => crate::Error::SchemaRegistry(other.to_string()),
        }
    }
}
