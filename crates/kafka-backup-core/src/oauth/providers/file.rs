//! A token read from a file on every fetch: the Kubernetes projected-secret
//! / Vault Agent pattern, where something else keeps the file fresh.
//!
//! The file is re-read whenever the cache refreshes, surrounding whitespace
//! (the usual trailing newline) is trimmed, and an empty file is an error.
//! The expiry comes from an optional sidecar file (`expires_at_file`:
//! RFC 3339 or Unix seconds), else from the JWT `exp` claim.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use crate::oauth::token::{OauthProviderError, OauthToken, OauthTokenProvider};

#[derive(Debug, Clone)]
pub struct FileTokenProvider {
    path: PathBuf,
    expires_at_file: Option<PathBuf>,
}

impl FileTokenProvider {
    pub fn new(path: impl Into<PathBuf>, expires_at_file: Option<PathBuf>) -> Self {
        Self {
            path: path.into(),
            expires_at_file,
        }
    }
}

async fn read_trimmed(path: &Path, what: &str) -> Result<String, OauthProviderError> {
    let raw = tokio::fs::read_to_string(path).await.map_err(|e| {
        OauthProviderError::Config(format!("cannot read {what} {}: {e}", path.display()))
    })?;
    Ok(raw.trim().to_string())
}

/// RFC 3339 or (fractional) Unix seconds.
fn parse_expiry(text: &str) -> Option<SystemTime> {
    if let Ok(secs) = text.parse::<f64>() {
        return (secs.is_finite() && secs > 0.0)
            .then(|| SystemTime::UNIX_EPOCH + Duration::from_secs_f64(secs));
    }
    chrono::DateTime::parse_from_rfc3339(text)
        .ok()
        .and_then(|dt| u64::try_from(dt.timestamp()).ok())
        .map(|secs| SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
}

#[async_trait]
impl OauthTokenProvider for FileTokenProvider {
    async fn get_token(&self) -> Result<OauthToken, OauthProviderError> {
        let token = read_trimmed(&self.path, "token file").await?;
        if token.is_empty() {
            return Err(OauthProviderError::InvalidToken(format!(
                "token file {} is empty",
                self.path.display()
            )));
        }
        let expires_at = match &self.expires_at_file {
            Some(path) => {
                let text = read_trimmed(path, "expiry file").await?;
                Some(parse_expiry(&text).ok_or_else(|| {
                    OauthProviderError::Config(format!(
                        "expiry file {} must hold an RFC 3339 time or Unix seconds",
                        path.display()
                    ))
                })?)
            }
            None => None,
        };
        Ok(OauthToken::new(token, expires_at, None, HashMap::new()))
    }

    fn kind(&self) -> &'static str {
        "file"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rereads_and_trims() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        std::fs::write(&path, "first-token\n").unwrap();
        let p = FileTokenProvider::new(&path, None);
        assert_eq!(p.get_token().await.unwrap().value(), "first-token");
        std::fs::write(&path, "  second-token\r\n").unwrap();
        assert_eq!(p.get_token().await.unwrap().value(), "second-token");
    }

    #[tokio::test]
    async fn missing_and_empty_files_fail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token");
        let p = FileTokenProvider::new(&path, None);
        assert!(
            matches!(p.get_token().await, Err(OauthProviderError::Config(m)) if m.contains("cannot read"))
        );
        std::fs::write(&path, "\n").unwrap();
        assert!(matches!(
            p.get_token().await,
            Err(OauthProviderError::InvalidToken(_))
        ));
    }

    #[tokio::test]
    async fn expiry_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let token = dir.path().join("token");
        let exp = dir.path().join("token.expires_at");
        std::fs::write(&token, "t").unwrap();
        std::fs::write(&exp, "1900000000\n").unwrap();
        let p = FileTokenProvider::new(&token, Some(exp.clone()));
        assert_eq!(
            p.get_token().await.unwrap().expires_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_900_000_000))
        );
        std::fs::write(&exp, "2030-03-17T17:46:40Z").unwrap();
        assert_eq!(
            p.get_token().await.unwrap().expires_at,
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_900_000_000))
        );
        std::fs::write(&exp, "tomorrow").unwrap();
        assert!(p.get_token().await.is_err());
    }
}
