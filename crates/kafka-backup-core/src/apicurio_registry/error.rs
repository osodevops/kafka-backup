//! Errors from the Apicurio Registry client and backup.

use thiserror::Error;

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum ApicurioRegistryError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Apicurio Registry API error (status {status}): {message}")]
    Api {
        status: u16,
        error_code: Option<i32>,
        message: String,
    },

    #[error("Authentication failed: {0}")]
    Auth(String),

    #[error("OIDC token exchange failed: {0}")]
    OidcTokenFailed(String),

    #[error("Group not found: {0}")]
    GroupNotFound(String),

    #[error("Artifact not found: group={group_id}, artifact={artifact_id}")]
    ArtifactNotFound {
        group_id: String,
        artifact_id: String,
    },

    #[error("Version not found: group={group_id}, artifact={artifact_id}, version={version}")]
    VersionNotFound {
        group_id: String,
        artifact_id: String,
        version: String,
    },

    #[error("Rate limit exceeded after {attempts} retry attempts")]
    RateLimitExhausted { attempts: u32 },

    #[error("Export API failed: {0}")]
    ExportFailed(String),

    #[error("JSON serialization error: {0}")]
    Serialization(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Circular artifact reference detected: {0}")]
    ReferenceCycle(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("{0}")]
    Other(String),
}

impl From<ApicurioRegistryError> for crate::Error {
    fn from(e: ApicurioRegistryError) -> Self {
        match e {
            ApicurioRegistryError::Config(m) => {
                crate::Error::Config(format!("apicurio_registry: {m}"))
            }
            ApicurioRegistryError::Auth(m) | ApicurioRegistryError::OidcTokenFailed(m) => {
                crate::Error::Authentication(format!("Apicurio Registry: {m}"))
            }
            other => crate::Error::SchemaRegistry(format!("Apicurio Registry: {other}")),
        }
    }
}
