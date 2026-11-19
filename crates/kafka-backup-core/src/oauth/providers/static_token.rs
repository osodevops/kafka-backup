//! A fixed token. For tests and debugging only: the token is never
//! refreshed, so it stops working when it expires.

use std::collections::HashMap;

use async_trait::async_trait;
use zeroize::Zeroizing;

use crate::oauth::token::{OauthProviderError, OauthToken, OauthTokenProvider};

pub struct StaticTokenProvider {
    token: Zeroizing<String>,
}

impl StaticTokenProvider {
    pub fn new(token: impl Into<String>) -> Result<Self, OauthProviderError> {
        let token = Zeroizing::new(token.into().trim().to_string());
        if token.is_empty() {
            return Err(OauthProviderError::Config(
                "static provider: token is empty".into(),
            ));
        }
        Ok(Self { token })
    }
}

impl std::fmt::Debug for StaticTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StaticTokenProvider")
            .field("token", &"***REDACTED***")
            .finish()
    }
}

#[async_trait]
impl OauthTokenProvider for StaticTokenProvider {
    async fn get_token(&self) -> Result<OauthToken, OauthProviderError> {
        // Expiry comes from the JWT `exp` claim (read by the cache), if any.
        Ok(OauthToken::new(
            self.token.as_str(),
            None,
            None,
            HashMap::new(),
        ))
    }

    fn kind(&self) -> &'static str {
        "static"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn returns_the_token_and_redacts_debug() {
        let p = StaticTokenProvider::new("static-secret-token\n").unwrap();
        assert_eq!(p.get_token().await.unwrap().value(), "static-secret-token");
        assert!(!format!("{p:?}").contains("static-secret-token"));
        assert!(StaticTokenProvider::new("  ").is_err());
    }
}
