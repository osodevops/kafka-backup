//! TLS configuration helpers for Kafka client connections.
//!
//! This module handles loading custom CA certificates and client certificates
//! for TLS and mTLS connections to Kafka brokers.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use tracing::debug;

use crate::config::SecurityConfig;
use crate::error::KafkaError;
use crate::Result;

/// Build a TLS ClientConfig from SecurityConfig.
///
/// This function:
/// 1. Loads custom CA certificates from `ssl_ca_location` if provided
/// 2. Falls back to webpki-roots if no custom CA is specified
/// 3. Configures client authentication (mTLS) if both cert and key are provided
pub fn build_tls_config(security: &SecurityConfig) -> Result<ClientConfig> {
    // Step 1: Build root certificate store
    let root_store = build_root_store(&security.ssl_ca_location)?;

    // Step 2: Build client config with or without client auth
    let config = match (
        &security.ssl_certificate_location,
        &security.ssl_key_location,
    ) {
        (Some(cert_path), Some(key_path)) => {
            // mTLS: Load client certificate and key
            debug!(
                "Configuring mTLS with cert={}, key={}",
                cert_path.display(),
                key_path.display()
            );

            let certs = load_certificates(cert_path)?;
            let key = load_private_key(key_path)?;

            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_client_auth_cert(certs, key)
                .map_err(|e| {
                    KafkaError::TlsConfig(format!("Failed to configure client authentication: {}", e))
                })?
        }
        (Some(cert_path), None) => {
            return Err(KafkaError::TlsConfig(format!(
                "ssl_certificate_location ({}) provided without ssl_key_location. \
                 Both must be provided for mTLS.",
                cert_path.display()
            ))
            .into());
        }
        (None, Some(key_path)) => {
            return Err(KafkaError::TlsConfig(format!(
                "ssl_key_location ({}) provided without ssl_certificate_location. \
                 Both must be provided for mTLS.",
                key_path.display()
            ))
            .into());
        }
        (None, None) => {
            // No client auth
            debug!("Configuring TLS without client authentication");
            ClientConfig::builder()
                .with_root_certificates(root_store)
                .with_no_client_auth()
        }
    };

    Ok(config)
}

/// Build a RootCertStore from a custom CA file or webpki-roots.
fn build_root_store(ca_path: &Option<std::path::PathBuf>) -> Result<RootCertStore> {
    match ca_path {
        Some(path) => {
            // Load custom CA certificates
            let certs = load_certificates(path)?;
            let mut root_store = RootCertStore::empty();

            for cert in certs {
                root_store.add(cert).map_err(|e| KafkaError::CertificateLoad {
                    path: path.display().to_string(),
                    message: format!("Failed to add certificate to root store: {}", e),
                })?;
            }

            if root_store.is_empty() {
                return Err(KafkaError::CertificateLoad {
                    path: path.display().to_string(),
                    message: "No valid certificates found in CA file".to_string(),
                }
                .into());
            }

            debug!(
                "Loaded {} CA certificate(s) from {}",
                root_store.len(),
                path.display()
            );

            Ok(root_store)
        }
        None => {
            // Fall back to webpki-roots (public CA certificates)
            debug!("Using webpki-roots for TLS verification (no custom CA specified)");
            Ok(RootCertStore {
                roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
            })
        }
    }
}

/// Load certificates from a PEM file.
fn load_certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path).map_err(|e| KafkaError::CertificateLoad {
        path: path.display().to_string(),
        message: format!("Failed to open file: {}", e),
    })?;

    let mut reader = BufReader::new(file);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| KafkaError::CertificateLoad {
            path: path.display().to_string(),
            message: format!("Failed to parse PEM certificates: {}", e),
        })?;

    if certs.is_empty() {
        return Err(KafkaError::CertificateLoad {
            path: path.display().to_string(),
            message: "No certificates found in file".to_string(),
        }
        .into());
    }

    debug!(
        "Loaded {} certificate(s) from {}",
        certs.len(),
        path.display()
    );

    Ok(certs)
}

/// Load a private key from a PEM file.
///
/// Supports PKCS#1 (RSA), PKCS#8, and SEC1 (EC) key formats.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path).map_err(|e| KafkaError::PrivateKeyLoad {
        path: path.display().to_string(),
        message: format!("Failed to open file: {}", e),
    })?;

    let mut reader = BufReader::new(file);

    // private_key() returns the first private key found (PKCS#1, PKCS#8, or SEC1)
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| KafkaError::PrivateKeyLoad {
            path: path.display().to_string(),
            message: format!("Failed to parse PEM private key: {}", e),
        })?
        .ok_or_else(|| {
            KafkaError::PrivateKeyLoad {
                path: path.display().to_string(),
                message: "No private key found in file".to_string(),
            }
            .into()
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_build_root_store_webpki_fallback() {
        let store = build_root_store(&None).unwrap();
        assert!(!store.is_empty(), "webpki-roots should not be empty");
    }

    #[test]
    fn test_build_root_store_missing_file() {
        let result = build_root_store(&Some(PathBuf::from("/nonexistent/ca.pem")));
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("Failed to open file") || err.contains("certificate"),
            "Error should mention file opening failure: {}",
            err
        );
    }

    #[test]
    fn test_mtls_requires_both_cert_and_key() {
        let security = SecurityConfig {
            ssl_certificate_location: Some(PathBuf::from("/path/to/cert.pem")),
            ssl_key_location: None,
            ..Default::default()
        };
        let result = build_tls_config(&security);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("ssl_key_location"),
            "Error should mention missing key: {}",
            err
        );
    }

    #[test]
    fn test_mtls_requires_both_key_and_cert() {
        let security = SecurityConfig {
            ssl_certificate_location: None,
            ssl_key_location: Some(PathBuf::from("/path/to/key.pem")),
            ..Default::default()
        };
        let result = build_tls_config(&security);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("ssl_certificate_location"),
            "Error should mention missing cert: {}",
            err
        );
    }

    #[test]
    fn test_no_client_auth_succeeds() {
        let security = SecurityConfig {
            ssl_ca_location: None,
            ssl_certificate_location: None,
            ssl_key_location: None,
            ..Default::default()
        };
        let result = build_tls_config(&security);
        assert!(
            result.is_ok(),
            "Should succeed with no client auth: {:?}",
            result
        );
    }

    /// Test loading actual PEM certificates from the test infrastructure.
    /// This test validates the fix for GitHub issue #3.
    #[test]
    fn test_load_real_ca_certificate() {
        // Path to test certificates (generated by tests/tls-test-infra/generate-certs.sh)
        let ca_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tests/tls-test-infra/certs/ca/ca.crt");

        if !ca_path.exists() {
            eprintln!(
                "Skipping test: CA cert not found at {}. Run tests/tls-test-infra/generate-certs.sh first.",
                ca_path.display()
            );
            return;
        }

        let security = SecurityConfig {
            ssl_ca_location: Some(ca_path.clone()),
            ssl_certificate_location: None,
            ssl_key_location: None,
            ..Default::default()
        };

        let result = build_tls_config(&security);
        assert!(
            result.is_ok(),
            "Should load CA certificate from {}: {:?}",
            ca_path.display(),
            result
        );
    }

    /// Test loading mTLS certificates (CA + client cert + client key).
    #[test]
    fn test_load_real_mtls_certificates() {
        let base_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("tests/tls-test-infra/certs");

        let ca_path = base_path.join("ca/ca.crt");
        let cert_path = base_path.join("client/client.crt");
        let key_path = base_path.join("client/client.key");

        if !ca_path.exists() || !cert_path.exists() || !key_path.exists() {
            eprintln!(
                "Skipping test: Certificates not found. Run tests/tls-test-infra/generate-certs.sh first."
            );
            return;
        }

        let security = SecurityConfig {
            ssl_ca_location: Some(ca_path),
            ssl_certificate_location: Some(cert_path),
            ssl_key_location: Some(key_path),
            ..Default::default()
        };

        let result = build_tls_config(&security);
        assert!(
            result.is_ok(),
            "Should load mTLS certificates: {:?}",
            result
        );
    }
}
