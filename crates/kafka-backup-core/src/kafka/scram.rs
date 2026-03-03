//! SCRAM-SHA-256/512 SASL authentication (RFC 5802).
//!
//! Implements the client side of the SCRAM protocol for Kafka SASL
//! authentication. Both SHA-256 and SHA-512 variants are supported.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use ring::{digest, hmac, pbkdf2, rand};
use std::num::NonZeroU32;

use crate::config::SaslMechanism;

/// Algorithm-specific constants for SCRAM variants.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ScramAlgorithm {
    Sha256,
    Sha512,
}

impl ScramAlgorithm {
    /// The Kafka SASL mechanism name string.
    pub fn mechanism_name(&self) -> &'static str {
        match self {
            ScramAlgorithm::Sha256 => "SCRAM-SHA-256",
            ScramAlgorithm::Sha512 => "SCRAM-SHA-512",
        }
    }

    /// The ring PBKDF2 algorithm for key derivation.
    fn pbkdf2_algorithm(&self) -> pbkdf2::Algorithm {
        match self {
            ScramAlgorithm::Sha256 => pbkdf2::PBKDF2_HMAC_SHA256,
            ScramAlgorithm::Sha512 => pbkdf2::PBKDF2_HMAC_SHA512,
        }
    }

    /// The ring HMAC algorithm for signing.
    fn hmac_algorithm(&self) -> hmac::Algorithm {
        match self {
            ScramAlgorithm::Sha256 => hmac::HMAC_SHA256,
            ScramAlgorithm::Sha512 => hmac::HMAC_SHA512,
        }
    }

    /// The ring digest algorithm for hashing.
    fn digest_algorithm(&self) -> &'static digest::Algorithm {
        match self {
            ScramAlgorithm::Sha256 => &digest::SHA256,
            ScramAlgorithm::Sha512 => &digest::SHA512,
        }
    }

    /// Output length of the digest in bytes.
    fn digest_len(&self) -> usize {
        match self {
            ScramAlgorithm::Sha256 => digest::SHA256_OUTPUT_LEN,
            ScramAlgorithm::Sha512 => digest::SHA512_OUTPUT_LEN,
        }
    }

    /// Convert from SaslMechanism config enum.
    pub fn from_mechanism(mechanism: SaslMechanism) -> Option<Self> {
        match mechanism {
            SaslMechanism::ScramSha256 => Some(ScramAlgorithm::Sha256),
            SaslMechanism::ScramSha512 => Some(ScramAlgorithm::Sha512),
            _ => None,
        }
    }
}

/// Parse server-first message: `r=<nonce>,s=<salt>,i=<iterations>`
fn parse_server_first(msg: &str) -> Result<(String, Vec<u8>, NonZeroU32), crate::Error> {
    let mut nonce = None;
    let mut salt_b64 = None;
    let mut iterations = None;

    for part in msg.split(',') {
        if let Some(value) = part.strip_prefix("r=") {
            nonce = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("s=") {
            salt_b64 = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("i=") {
            iterations = Some(value.parse::<u32>().map_err(|e| {
                crate::Error::Authentication(format!("Invalid iteration count '{}': {}", value, e))
            })?);
        }
    }

    let nonce = nonce.ok_or_else(|| {
        crate::Error::Authentication("Missing nonce (r=) in server-first message".to_string())
    })?;

    let salt = BASE64
        .decode(
            salt_b64.ok_or_else(|| {
                crate::Error::Authentication(
                    "Missing salt (s=) in server-first message".to_string(),
                )
            })?,
        )
        .map_err(|e| crate::Error::Authentication(format!("Invalid base64 salt: {}", e)))?;

    let iter_count = iterations.ok_or_else(|| {
        crate::Error::Authentication(
            "Missing iterations (i=) in server-first message".to_string(),
        )
    })?;

    let iter_count = NonZeroU32::new(iter_count).ok_or_else(|| {
        crate::Error::Authentication("Iteration count must be > 0".to_string())
    })?;

    Ok((nonce, salt, iter_count))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_mechanism_names() {
        assert_eq!(ScramAlgorithm::Sha256.mechanism_name(), "SCRAM-SHA-256");
        assert_eq!(ScramAlgorithm::Sha512.mechanism_name(), "SCRAM-SHA-512");
    }

    #[test]
    fn test_algorithm_from_mechanism() {
        assert!(matches!(
            ScramAlgorithm::from_mechanism(SaslMechanism::ScramSha256),
            Some(ScramAlgorithm::Sha256)
        ));
        assert!(matches!(
            ScramAlgorithm::from_mechanism(SaslMechanism::ScramSha512),
            Some(ScramAlgorithm::Sha512)
        ));
        assert!(ScramAlgorithm::from_mechanism(SaslMechanism::Plain).is_none());
    }

    #[test]
    fn test_parse_server_first_valid() {
        let msg = "r=abc123XYZ,s=QSXCR+Q6sek8bf92,i=4096";
        let (nonce, salt, iterations) = parse_server_first(msg).unwrap();
        assert_eq!(nonce, "abc123XYZ");
        assert!(!salt.is_empty());
        assert_eq!(iterations.get(), 4096);
    }

    #[test]
    fn test_parse_server_first_missing_nonce() {
        let msg = "s=QSXCR+Q6sek8bf92,i=4096";
        assert!(parse_server_first(msg).is_err());
    }

    #[test]
    fn test_parse_server_first_missing_salt() {
        let msg = "r=abc123,i=4096";
        assert!(parse_server_first(msg).is_err());
    }

    #[test]
    fn test_parse_server_first_missing_iterations() {
        let msg = "r=abc123,s=QSXCR+Q6sek8bf92";
        assert!(parse_server_first(msg).is_err());
    }

    #[test]
    fn test_parse_server_first_zero_iterations() {
        let msg = "r=abc,s=QSXCR+Q6sek8bf92,i=0";
        assert!(parse_server_first(msg).is_err());
    }

    #[test]
    fn test_parse_server_first_invalid_iterations() {
        let msg = "r=abc,s=QSXCR+Q6sek8bf92,i=notanumber";
        assert!(parse_server_first(msg).is_err());
    }
}
