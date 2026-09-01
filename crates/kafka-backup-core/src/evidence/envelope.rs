//! Versioned detached evidence envelope (evidence contract v2).
//!
//! The v2 contract eliminates the self-referential `report_sha256` design of
//! contract v1: the digest and signature live in a detached envelope that
//! covers the exact, complete stored report bytes. No canonicalization is
//! performed at verification time — verification hashes the stored bytes as
//! they are, following DSSE's "sign bytes, not a parsed JSON AST" principle.
//!
//! Signature construction follows the DSSE v1 protocol exactly: the ECDSA
//! P-256 signature is computed over `PAE(payloadType, payload)` where PAE is
//! DSSE's Pre-Authentication Encoding. The envelope itself deliberately
//! deviates from the DSSE envelope format by NOT embedding the payload
//! (DSSE requires an embedded base64 `payload` field): the report must stay
//! directly readable in object storage for auditors, and the envelope
//! references it by digest instead. See `docs/evidence-contract.md` for the
//! byte-level contract, primary-source citations, and the deviation record.
//!
//! Legacy evidence (contract v1: the text `.sig` bundle emitted with report
//! schema 1.0) remains verifiable via [`verify_evidence`], which detects the
//! artifact version by content.

use p256::ecdsa::signature::{Signer, Verifier};
use p256::ecdsa::{Signature, SigningKey, VerifyingKey};
use p256::pkcs8::{DecodePrivateKey, DecodePublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::report::hex_encode;
use super::signing::SignatureBundle;
use crate::Result;

/// DSSE payload type identifying a kafka-backup evidence report.
pub const EVIDENCE_PAYLOAD_TYPE: &str = "application/vnd.kafka-backup.evidence-report+json";

/// Identifier of the v2 evidence envelope schema.
pub const ENVELOPE_SCHEMA_V2: &str = "kafka-backup/evidence-envelope/v2";

/// Signature algorithm used for all evidence signing.
pub const SIGNATURE_ALGORITHM: &str = "ECDSA-P256-SHA256";

/// Detached, versioned evidence envelope (contract v2).
///
/// `signature` is a DSSE v1 signature: ECDSA P-256 (SHA-256) over
/// `PAE(payload_type, payload)` of the exact stored report bytes.
/// `payload_sha256`/`payload_size_bytes` are unsigned hints used to give
/// precise diagnostics; verification independently enforces both the digest
/// and the signature against the stored bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceEnvelope {
    /// Envelope schema identifier ([`ENVELOPE_SCHEMA_V2`]).
    pub schema: String,
    /// DSSE payload type bound into the signature.
    pub payload_type: String,
    /// Lowercase hex SHA-256 of the exact stored report bytes.
    pub payload_sha256: String,
    /// Size of the stored report bytes.
    pub payload_size_bytes: u64,
    /// Report ID this envelope belongs to.
    pub report_id: String,
    /// Signature algorithm ([`SIGNATURE_ALGORITHM`]).
    pub algorithm: String,
    /// Base64 (standard) ECDSA signature over `PAE(payload_type, payload)`.
    pub signature: String,
    /// Informational signing timestamp (RFC 3339). Not covered by the
    /// signature.
    pub signed_at: String,
}

impl EvidenceEnvelope {
    /// Serialize to pretty JSON for storage. The envelope has no canonical
    /// byte form: nothing hashes or signs the envelope itself.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
            .map_err(|e| crate::Error::Evidence(format!("Failed to serialize envelope: {e}")))
    }

    /// Parse an envelope, enforcing the schema identifier.
    pub fn from_slice(data: &[u8]) -> Result<Self> {
        let envelope: Self = serde_json::from_slice(data)
            .map_err(|e| crate::Error::Evidence(format!("Invalid evidence envelope: {e}")))?;
        if envelope.schema != ENVELOPE_SCHEMA_V2 {
            return Err(crate::Error::Evidence(format!(
                "Unsupported evidence envelope schema '{}' (this build supports '{}' and the \
                 legacy v1 text signature format)",
                envelope.schema, ENVELOPE_SCHEMA_V2
            )));
        }
        Ok(envelope)
    }
}

/// DSSE v1 Pre-Authentication Encoding.
///
/// `PAE(type, body) = "DSSEv1" SP LEN(type) SP type SP LEN(body) SP body`
/// where `LEN` is the ASCII decimal byte length and SP a single 0x20. The
/// length prefixes remove concatenation ambiguity, so no canonicalization of
/// the payload is needed or performed.
pub fn pre_authentication_encoding(payload_type: &str, payload: &[u8]) -> Vec<u8> {
    let type_bytes = payload_type.as_bytes();
    let mut out = Vec::with_capacity(payload.len() + type_bytes.len() + 32);
    out.extend_from_slice(b"DSSEv1 ");
    out.extend_from_slice(type_bytes.len().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(type_bytes);
    out.push(b' ');
    out.extend_from_slice(payload.len().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload);
    out
}

/// Lowercase hex SHA-256 of a byte sequence.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(&hasher.finalize())
}

/// Sign stored report bytes, producing a v2 detached envelope.
pub fn sign_evidence(
    report_bytes: &[u8],
    private_key_pem: &str,
    report_id: &str,
) -> Result<EvidenceEnvelope> {
    let signing_key = SigningKey::from_pkcs8_pem(private_key_pem)
        .map_err(|e| crate::Error::Evidence(format!("Failed to load signing key: {e}")))?;

    let pae = pre_authentication_encoding(EVIDENCE_PAYLOAD_TYPE, report_bytes);
    let signature: Signature = signing_key.sign(&pae);
    let sig_base64 = base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        signature.to_bytes(),
    );

    Ok(EvidenceEnvelope {
        schema: ENVELOPE_SCHEMA_V2.to_string(),
        payload_type: EVIDENCE_PAYLOAD_TYPE.to_string(),
        payload_sha256: sha256_hex(report_bytes),
        payload_size_bytes: report_bytes.len() as u64,
        report_id: report_id.to_string(),
        algorithm: SIGNATURE_ALGORITHM.to_string(),
        signature: sig_base64,
        signed_at: chrono::Utc::now().to_rfc3339(),
    })
}

/// Verify an ECDSA P-256 signature (base64) over arbitrary bytes.
pub fn verify_ecdsa(data: &[u8], signature_base64: &str, public_key_pem: &str) -> Result<bool> {
    let verifying_key = VerifyingKey::from_public_key_pem(public_key_pem)
        .map_err(|e| crate::Error::Evidence(format!("Failed to load public key: {e}")))?;

    let sig_bytes =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, signature_base64)
            .map_err(|e| crate::Error::Evidence(format!("Invalid base64 signature: {e}")))?;

    let signature = Signature::from_slice(&sig_bytes)
        .map_err(|e| crate::Error::Evidence(format!("Invalid ECDSA signature: {e}")))?;

    Ok(verifying_key.verify(data, &signature).is_ok())
}

/// Evidence contract version of a signature artifact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceContractVersion {
    /// Text `.sig` bundle; signature covers the raw stored report bytes
    /// directly (no PAE). Emitted alongside report schema 1.0.
    V1Legacy,
    /// Detached JSON envelope; DSSE-style signature over
    /// `PAE(payload_type, payload)`. Emitted alongside report schema 1.1+.
    V2,
}

impl std::fmt::Display for EvidenceContractVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::V1Legacy => "v1 (legacy text signature)",
            Self::V2 => "v2 (detached envelope)",
        })
    }
}

/// A parsed signature artifact of either contract version.
#[derive(Debug, Clone)]
pub enum SignatureArtifact {
    /// Legacy v1 text bundle.
    V1(SignatureBundle),
    /// v2 detached envelope.
    V2(EvidenceEnvelope),
}

/// Detect and parse a signature artifact by content.
///
/// v2 envelopes are JSON objects with the v2 schema identifier; v1 bundles
/// are the `-----BEGIN KAFKA BACKUP EVIDENCE SIGNATURE-----` text format.
pub fn parse_signature_artifact(content: &str) -> Result<SignatureArtifact> {
    let trimmed = content.trim_start();
    if trimmed.starts_with('{') {
        Ok(SignatureArtifact::V2(EvidenceEnvelope::from_slice(
            trimmed.as_bytes(),
        )?))
    } else if trimmed.starts_with("-----BEGIN KAFKA BACKUP EVIDENCE SIGNATURE-----") {
        Ok(SignatureArtifact::V1(SignatureBundle::from_sig_file(
            content,
        )?))
    } else {
        Err(crate::Error::Evidence(
            "Unrecognized signature artifact: expected a v2 JSON evidence envelope or the \
             legacy v1 text signature format"
                .to_string(),
        ))
    }
}

/// Structured result of verifying evidence artifacts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationOutcome {
    /// Which contract version the signature artifact used.
    pub contract: EvidenceContractVersion,
    /// Report ID recorded in the signature artifact.
    pub report_id: String,
    /// Signature algorithm recorded in the artifact.
    pub algorithm: String,
    /// Digest recorded in the signature artifact.
    pub expected_sha256: String,
    /// Digest recomputed over the exact stored report bytes.
    pub actual_sha256: String,
    /// Whether recomputed and recorded digests match.
    pub digest_valid: bool,
    /// Whether a public key was supplied and the signature was checked.
    pub signature_checked: bool,
    /// Whether the cryptographic signature verified (meaningful only when
    /// `signature_checked`).
    pub signature_valid: bool,
    /// `schema_version` parsed from the report payload, when available.
    pub report_schema_version: Option<String>,
    /// Human-readable findings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
}

impl VerificationOutcome {
    /// Overall verdict: digest must match, and when a key was supplied the
    /// signature must verify.
    pub fn verified(&self) -> bool {
        self.digest_valid && (!self.signature_checked || self.signature_valid)
    }
}

/// Verify stored report bytes against a signature artifact of either
/// contract version.
///
/// The stored bytes are hashed and verified exactly as read — no JSON
/// parsing, re-serialization, or canonicalization influences the digest or
/// signature check. `public_key_pem` is optional; without it only the digest
/// binding is checked.
pub fn verify_evidence(
    report_bytes: &[u8],
    signature_content: &str,
    public_key_pem: Option<&str>,
) -> Result<VerificationOutcome> {
    let artifact = parse_signature_artifact(signature_content)?;
    let actual_sha256 = sha256_hex(report_bytes);
    let report_schema_version = serde_json::from_slice::<serde_json::Value>(report_bytes)
        .ok()
        .and_then(|v| {
            v.get("schema_version")
                .and_then(|s| s.as_str().map(str::to_string))
        });

    let mut messages = Vec::new();

    let (contract, report_id, algorithm, expected_sha256, signed_data) = match &artifact {
        SignatureArtifact::V1(bundle) => (
            EvidenceContractVersion::V1Legacy,
            bundle.report_id.clone(),
            bundle.algorithm.clone(),
            bundle.report_sha256.clone(),
            // v1 signatures cover the raw stored bytes directly.
            report_bytes.to_vec(),
        ),
        SignatureArtifact::V2(envelope) => {
            if envelope.payload_type != EVIDENCE_PAYLOAD_TYPE {
                messages.push(format!(
                    "envelope payload_type '{}' differs from '{}'",
                    envelope.payload_type, EVIDENCE_PAYLOAD_TYPE
                ));
            }
            if envelope.payload_size_bytes != report_bytes.len() as u64 {
                messages.push(format!(
                    "report size {} bytes does not match envelope payload_size_bytes {}",
                    report_bytes.len(),
                    envelope.payload_size_bytes
                ));
            }
            (
                EvidenceContractVersion::V2,
                envelope.report_id.clone(),
                envelope.algorithm.clone(),
                envelope.payload_sha256.clone(),
                // v2 signatures cover the DSSE PAE of the stored bytes.
                pre_authentication_encoding(&envelope.payload_type, report_bytes),
            )
        }
    };

    let digest_valid = expected_sha256 == actual_sha256;
    if !digest_valid {
        messages.push(
            "digest mismatch: the stored report bytes are not the bytes this signature \
             artifact covers (report or signature artifact was modified)"
                .to_string(),
        );
    }

    let (signature_checked, signature_valid) = match public_key_pem {
        Some(pem) => {
            let sig_base64 = match &artifact {
                SignatureArtifact::V1(bundle) => &bundle.signature,
                SignatureArtifact::V2(envelope) => &envelope.signature,
            };
            let valid = verify_ecdsa(&signed_data, sig_base64, pem)?;
            if !valid {
                messages.push("ECDSA signature verification failed".to_string());
            }
            (true, valid)
        }
        None => {
            messages.push(
                "no public key provided — signature not cryptographically checked".to_string(),
            );
            (false, false)
        }
    };

    Ok(VerificationOutcome {
        contract,
        report_id,
        algorithm,
        expected_sha256,
        actual_sha256,
        digest_valid,
        signature_checked,
        signature_valid,
        report_schema_version,
        messages,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use p256::ecdsa::SigningKey;
    use p256::elliptic_curve::rand_core::OsRng;
    use p256::pkcs8::{EncodePrivateKey, EncodePublicKey};

    fn test_keypair() -> (String, String) {
        let signing_key = SigningKey::random(&mut OsRng);
        let verifying_key = VerifyingKey::from(&signing_key);
        (
            signing_key
                .to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
                .unwrap()
                .to_string(),
            verifying_key
                .to_public_key_pem(p256::pkcs8::LineEnding::LF)
                .unwrap(),
        )
    }

    #[test]
    fn pae_matches_dsse_specification_vector() {
        // DSSE protocol.md test vector:
        // PAE("http://example.com/HelloWorld", "hello world") =
        // "DSSEv1 29 http://example.com/HelloWorld 11 hello world"
        let pae = pre_authentication_encoding("http://example.com/HelloWorld", b"hello world");
        assert_eq!(
            String::from_utf8(pae).unwrap(),
            "DSSEv1 29 http://example.com/HelloWorld 11 hello world"
        );
    }

    #[test]
    fn pae_is_unambiguous_for_empty_payload() {
        let pae = pre_authentication_encoding("t", b"");
        assert_eq!(pae, b"DSSEv1 1 t 0 ".to_vec());
    }

    #[test]
    fn v2_sign_verify_roundtrip_and_tamper() {
        let (private_pem, public_pem) = test_keypair();
        let payload = br#"{"schema_version":"1.1","x":1}"#;

        let envelope = sign_evidence(payload, &private_pem, "run-1").unwrap();
        assert_eq!(envelope.schema, ENVELOPE_SCHEMA_V2);
        assert_eq!(envelope.payload_sha256, sha256_hex(payload));
        assert_eq!(envelope.payload_size_bytes, payload.len() as u64);

        let sig_json = String::from_utf8(envelope.to_json().unwrap()).unwrap();
        let outcome = verify_evidence(payload, &sig_json, Some(&public_pem)).unwrap();
        assert!(outcome.verified());
        assert_eq!(outcome.contract, EvidenceContractVersion::V2);
        assert_eq!(outcome.report_schema_version.as_deref(), Some("1.1"));

        // Tampered payload: digest fails and signature fails.
        let tampered = br#"{"schema_version":"1.1","x":2}"#;
        let outcome = verify_evidence(tampered, &sig_json, Some(&public_pem)).unwrap();
        assert!(!outcome.verified());
        assert!(!outcome.digest_valid);
        assert!(!outcome.signature_valid);
    }

    #[test]
    fn v2_wrong_key_fails_signature_only() {
        let (private_pem, _) = test_keypair();
        let (_, wrong_public) = test_keypair();
        let payload = b"payload";
        let envelope = sign_evidence(payload, &private_pem, "run-2").unwrap();
        let sig_json = String::from_utf8(envelope.to_json().unwrap()).unwrap();

        let outcome = verify_evidence(payload, &sig_json, Some(&wrong_public)).unwrap();
        assert!(outcome.digest_valid, "digest still matches");
        assert!(!outcome.signature_valid, "wrong key must fail");
        assert!(!outcome.verified());
    }

    #[test]
    fn v2_tampered_envelope_signature_fails() {
        let (private_pem, public_pem) = test_keypair();
        let payload = b"payload";
        let mut envelope = sign_evidence(payload, &private_pem, "run-3").unwrap();
        // Corrupt the signature while keeping valid base64.
        envelope.signature =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, vec![0u8; 64]);
        let sig_json = String::from_utf8(envelope.to_json().unwrap()).unwrap();
        // An Err (invalid signature encoding) is also an acceptable failure.
        if let Ok(outcome) = verify_evidence(payload, &sig_json, Some(&public_pem)) {
            assert!(!outcome.verified());
        }
    }

    #[test]
    fn unknown_envelope_schema_is_rejected() {
        let json = r#"{
            "schema": "kafka-backup/evidence-envelope/v99",
            "payload_type": "t", "payload_sha256": "00", "payload_size_bytes": 0,
            "report_id": "r", "algorithm": "a", "signature": "", "signed_at": ""
        }"#;
        let err = parse_signature_artifact(json).unwrap_err();
        assert!(err
            .to_string()
            .contains("Unsupported evidence envelope schema"));
    }

    #[test]
    fn legacy_v1_artifact_verifies_and_detects_tampering() {
        let (private_pem, public_pem) = test_keypair();
        // Legacy contract: signature over raw stored bytes, text .sig format.
        let payload = br#"{"schema_version":"1.0","integrity":{"report_sha256":""}}"#;
        let bundle = super::super::signing::sign_report(payload, &private_pem, "legacy-1").unwrap();
        let sig_text = bundle.to_sig_file();

        let outcome = verify_evidence(payload, &sig_text, Some(&public_pem)).unwrap();
        assert!(outcome.verified());
        assert_eq!(outcome.contract, EvidenceContractVersion::V1Legacy);
        assert_eq!(outcome.report_schema_version.as_deref(), Some("1.0"));

        let outcome = verify_evidence(b"tampered", &sig_text, Some(&public_pem)).unwrap();
        assert!(!outcome.verified());
    }

    #[test]
    fn garbage_artifact_is_rejected() {
        assert!(parse_signature_artifact("not a signature").is_err());
    }
}
