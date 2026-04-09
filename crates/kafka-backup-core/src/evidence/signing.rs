//! Cryptographic signing and verification of evidence reports using ECDSA-P256-SHA256.

use p256::ecdsa::signature::Signer;
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use p256::pkcs8::DecodePrivateKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::report::hex_encode;
use crate::Result;

/// A detached signature bundle for an evidence report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignatureBundle {
    /// Signing algorithm identifier.
    pub algorithm: String,

    /// Report ID this signature applies to.
    pub report_id: String,

    /// SHA-256 hex digest of the canonical JSON report.
    pub report_sha256: String,

    /// Base64-encoded ECDSA signature over the SHA-256 digest.
    pub signature: String,
}

impl SignatureBundle {
    /// Serialize to the detached `.sig` text format.
    pub fn to_sig_file(&self) -> String {
        let mut out = String::new();
        out.push_str("-----BEGIN KAFKA BACKUP EVIDENCE SIGNATURE-----\n");
        out.push_str(&format!("Algorithm: {}\n", self.algorithm));
        out.push_str(&format!("Report-ID: {}\n", self.report_id));
        out.push_str(&format!("Report-SHA256: {}\n", self.report_sha256));
        out.push_str(&format!("Signature: {}\n", self.signature));
        out.push_str("-----END KAFKA BACKUP EVIDENCE SIGNATURE-----\n");
        out
    }

    /// Parse from the detached `.sig` text format.
    pub fn from_sig_file(content: &str) -> Result<Self> {
        let mut algorithm = String::new();
        let mut report_id = String::new();
        let mut report_sha256 = String::new();
        let mut signature = String::new();

        for line in content.lines() {
            let line = line.trim();
            if let Some(val) = line.strip_prefix("Algorithm: ") {
                algorithm = val.to_string();
            } else if let Some(val) = line.strip_prefix("Report-ID: ") {
                report_id = val.to_string();
            } else if let Some(val) = line.strip_prefix("Report-SHA256: ") {
                report_sha256 = val.to_string();
            } else if let Some(val) = line.strip_prefix("Signature: ") {
                signature = val.to_string();
            }
        }

        if signature.is_empty() {
            return Err(crate::Error::Evidence(
                "Invalid signature file: no Signature field found".to_string(),
            ));
        }

        Ok(Self {
            algorithm,
            report_id,
            report_sha256,
            signature,
        })
    }
}

/// Sign a canonical JSON report with an ECDSA-P256 private key.
///
/// Returns a [`SignatureBundle`] containing the detached signature.
pub fn sign_report(
    canonical_json: &[u8],
    private_key_pem: &str,
    report_id: &str,
) -> Result<SignatureBundle> {
    let signing_key = SigningKey::from_pkcs8_pem(private_key_pem)
        .map_err(|e| crate::Error::Evidence(format!("Failed to load signing key: {e}")))?;

    let mut hasher = Sha256::new();
    hasher.update(canonical_json);
    let digest = hasher.finalize();
    let digest_hex = hex_encode(&digest);

    let signature: Signature = signing_key.sign(canonical_json);
    let sig_base64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        signature.to_bytes(),
    );

    Ok(SignatureBundle {
        algorithm: "ECDSA-P256-SHA256".to_string(),
        report_id: report_id.to_string(),
        report_sha256: digest_hex,
        signature: sig_base64,
    })
}

/// Verify a detached signature against a canonical JSON report.
///
/// The `public_key_pem` should be a PEM-encoded ECDSA P-256 public key.
pub fn verify_report(
    canonical_json: &[u8],
    signature_bundle: &SignatureBundle,
    public_key_pem: &str,
) -> Result<bool> {
    use p256::ecdsa::signature::Verifier;
    use p256::pkcs8::DecodePublicKey;

    let verifying_key = VerifyingKey::from_public_key_pem(public_key_pem)
        .map_err(|e| crate::Error::Evidence(format!("Failed to load public key: {e}")))?;

    let sig_bytes = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        &signature_bundle.signature,
    )
    .map_err(|e| crate::Error::Evidence(format!("Invalid base64 signature: {e}")))?;

    let signature = Signature::from_slice(&sig_bytes)
        .map_err(|e| crate::Error::Evidence(format!("Invalid ECDSA signature: {e}")))?;

    // Verify the SHA-256 digest matches
    let mut hasher = Sha256::new();
    hasher.update(canonical_json);
    let digest = hasher.finalize();
    let digest_hex = hex_encode(&digest);

    if digest_hex != signature_bundle.report_sha256 {
        return Ok(false);
    }

    // Verify the ECDSA signature over the original canonical JSON
    match verifying_key.verify(canonical_json, &signature) {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use p256::ecdsa::SigningKey;
    use p256::elliptic_curve::rand_core::OsRng;
    use p256::pkcs8::{EncodePrivateKey, EncodePublicKey};

    #[test]
    fn test_sign_and_verify_roundtrip() {
        // Generate a test key pair
        let signing_key = SigningKey::random(&mut OsRng);
        let verifying_key = VerifyingKey::from(&signing_key);

        let private_pem = signing_key
            .to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
            .unwrap();
        let public_pem = verifying_key
            .to_public_key_pem(p256::pkcs8::LineEnding::LF)
            .unwrap();

        let report_json = br#"{"test": "data", "value": 42}"#;

        let bundle = sign_report(report_json, private_pem.as_str(), "test-run-001").unwrap();
        assert_eq!(bundle.algorithm, "ECDSA-P256-SHA256");
        assert_eq!(bundle.report_id, "test-run-001");

        let valid = verify_report(report_json, &bundle, &public_pem).unwrap();
        assert!(valid, "Signature should verify");

        // Tampered data should fail
        let tampered = br#"{"test": "tampered", "value": 42}"#;
        let valid = verify_report(tampered, &bundle, &public_pem).unwrap();
        assert!(!valid, "Tampered data should not verify");
    }

    #[test]
    fn test_sig_file_roundtrip() {
        let bundle = SignatureBundle {
            algorithm: "ECDSA-P256-SHA256".to_string(),
            report_id: "test-001".to_string(),
            report_sha256: "abc123".to_string(),
            signature: "c2lnbmF0dXJl".to_string(),
        };

        let sig_text = bundle.to_sig_file();
        let parsed = SignatureBundle::from_sig_file(&sig_text).unwrap();

        assert_eq!(parsed.algorithm, bundle.algorithm);
        assert_eq!(parsed.report_id, bundle.report_id);
        assert_eq!(parsed.report_sha256, bundle.report_sha256);
        assert_eq!(parsed.signature, bundle.signature);
    }
}
