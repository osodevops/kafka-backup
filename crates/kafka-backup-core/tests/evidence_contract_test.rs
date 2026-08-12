//! Issue #138: evidence contract v2 regression tests.
//!
//! Emits evidence through the shared core pipeline into a real storage
//! backend, reloads the stored JSON and signature artifacts, recomputes the
//! digest, verifies the ECDSA P-256 signature, checks schema versions, and
//! exercises tampered-JSON, tampered-envelope, wrong-key, and legacy-v1
//! evidence — including artifacts reproducing the original
//! blank-`report_sha256` bug that motivated the issue.

use p256::ecdsa::{SigningKey, VerifyingKey};
use p256::elliptic_curve::rand_core::OsRng;
use p256::pkcs8::{EncodePrivateKey, EncodePublicKey};

use kafka_backup_core::evidence::{
    self, build_evidence_report, emit_evidence, EvidenceContractVersion, EvidenceReportParams,
    ENVELOPE_SCHEMA_V2, EVIDENCE_PAYLOAD_TYPE, EVIDENCE_REPORT_SCHEMA_VERSION,
};
use kafka_backup_core::manifest::BackupManifest;
use kafka_backup_core::storage::{MemoryBackend, StorageBackend};
use kafka_backup_core::validation::config::{
    EvidenceConfig, EvidenceFormat, EvidenceStorageConfig, SigningConfig,
};
use kafka_backup_core::validation::{CheckOutcome, ValidationResult, ValidationSummary};

fn test_keypair_files(dir: &std::path::Path) -> (String, String, String) {
    let signing_key = SigningKey::random(&mut OsRng);
    let verifying_key = VerifyingKey::from(&signing_key);
    let private_pem = signing_key
        .to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
        .unwrap()
        .to_string();
    let public_pem = verifying_key
        .to_public_key_pem(p256::pkcs8::LineEnding::LF)
        .unwrap();
    let key_path = dir.join("signing.pem");
    std::fs::write(&key_path, &private_pem).unwrap();
    (
        key_path.to_string_lossy().to_string(),
        private_pem,
        public_pem,
    )
}

fn test_summary() -> ValidationSummary {
    ValidationSummary {
        overall_result: CheckOutcome::Passed,
        checks_total: 1,
        checks_passed: 1,
        checks_failed: 0,
        checks_skipped: 0,
        checks_warned: 0,
        total_duration_ms: 42,
        results: vec![ValidationResult {
            check_name: "MessageCountCheck".to_string(),
            outcome: CheckOutcome::Passed,
            detail: "1000/1000 records".to_string(),
            data: serde_json::json!({"expected": 1000, "actual": 1000}),
            duration_ms: 42,
        }],
    }
}

fn test_report(run_id: &str, signing_enabled: bool) -> kafka_backup_core::evidence::EvidenceReport {
    let manifest = BackupManifest::new("evidence-backup".to_string());
    build_evidence_report(EvidenceReportParams {
        run_id,
        tool_version: "0.0.0-test",
        backup_id: "evidence-backup",
        manifest: &manifest,
        manifest_sha256: "ab".repeat(32),
        storage_backend: "memory".to_string(),
        pitr_timestamp: None,
        target_bootstrap_servers: vec!["target:9092".to_string()],
        summary: test_summary(),
        retention_days: 2555,
        signing_enabled,
        triggered_by: Some("evidence-contract-test".to_string()),
    })
}

fn signed_config(key_path: &str) -> EvidenceConfig {
    EvidenceConfig {
        formats: vec![EvidenceFormat::Json, EvidenceFormat::Pdf],
        signing: SigningConfig {
            enabled: true,
            private_key_path: Some(key_path.to_string()),
            public_key_path: None,
        },
        storage: EvidenceStorageConfig::default(),
    }
}

async fn reload(storage: &dyn StorageBackend, key: &str) -> Vec<u8> {
    storage.get(key).await.expect("stored artifact").to_vec()
}

#[tokio::test]
async fn signed_emission_reload_recompute_and_verify() {
    let dir = tempfile::TempDir::new().unwrap();
    let (key_path, _, public_pem) = test_keypair_files(dir.path());
    let storage = MemoryBackend::new();

    let report = test_report("validation-run-1", true);
    let emission = emit_evidence(&storage, &report, &signed_config(&key_path))
        .await
        .unwrap();
    assert!(emission.signed);
    assert!(emission.warnings.is_empty(), "{:?}", emission.warnings);

    // Reload the exact artifacts from the backend.
    let stored_json = reload(&storage, emission.json_key.as_ref().unwrap()).await;
    let stored_sig = reload(&storage, emission.signature_key.as_ref().unwrap()).await;
    let stored_pdf = reload(&storage, emission.pdf_key.as_ref().unwrap()).await;

    // The stored JSON is byte-identical to what the emission hashed.
    assert_eq!(stored_json, emission.report_bytes);

    // Recompute the digest over the stored bytes.
    let recomputed = evidence::sha256_hex(&stored_json);
    assert_eq!(recomputed, emission.report_sha256);

    // The stored JSON never embeds its own digest.
    let value: serde_json::Value = serde_json::from_slice(&stored_json).unwrap();
    assert_eq!(
        value["schema_version"].as_str(),
        Some(EVIDENCE_REPORT_SCHEMA_VERSION)
    );
    assert!(
        value["integrity"].get("report_sha256").is_none(),
        "schema 1.1 stored JSON must not contain report_sha256, got: {}",
        value["integrity"]
    );

    // The envelope covers the stored bytes and carries the v2 schema.
    let envelope: serde_json::Value = serde_json::from_slice(&stored_sig).unwrap();
    assert_eq!(envelope["schema"].as_str(), Some(ENVELOPE_SCHEMA_V2));
    assert_eq!(
        envelope["payload_type"].as_str(),
        Some(EVIDENCE_PAYLOAD_TYPE)
    );
    assert_eq!(
        envelope["payload_sha256"].as_str(),
        Some(recomputed.as_str())
    );
    assert_eq!(
        envelope["payload_size_bytes"].as_u64(),
        Some(stored_json.len() as u64)
    );

    // Full cryptographic verification of the reloaded artifacts.
    let outcome = evidence::verify_evidence(
        &stored_json,
        std::str::from_utf8(&stored_sig).unwrap(),
        Some(&public_pem),
    )
    .unwrap();
    assert!(outcome.verified(), "{:?}", outcome.messages);
    assert_eq!(outcome.contract, EvidenceContractVersion::V2);
    assert!(outcome.digest_valid);
    assert!(outcome.signature_checked && outcome.signature_valid);
    assert_eq!(
        outcome.report_schema_version.as_deref(),
        Some(EVIDENCE_REPORT_SCHEMA_VERSION)
    );

    // PDF: generated with a valid header, and its integrity page renders the
    // same external digest the CLI prints (the PDF body streams are
    // compressed, so assert on the rendered source-of-truth lines).
    assert_eq!(&stored_pdf[0..5], b"%PDF-");
    let pdf_lines = kafka_backup_core::evidence::pdf::integrity_display_lines(
        &report,
        Some(&emission.report_sha256),
    );
    assert!(
        pdf_lines.iter().any(|l| l.contains(&emission.report_sha256)
            && l.contains("Report SHA-256 (stored JSON bytes)")),
        "PDF integrity page must display the digest of the stored JSON bytes: {pdf_lines:?}"
    );
    // And a legacy 1.0 report (embedded digest, no external digest) falls
    // back to the embedded value, clearly labelled.
    let mut legacy_report = test_report("legacy-display", false);
    legacy_report.schema_version = "1.0".to_string();
    legacy_report.integrity.report_sha256 = "fe".repeat(32);
    let legacy_lines =
        kafka_backup_core::evidence::pdf::integrity_display_lines(&legacy_report, None);
    assert!(legacy_lines
        .iter()
        .any(|l| l.contains("legacy, embedded") && l.contains(&"fe".repeat(32))));
}

#[tokio::test]
async fn tampered_stored_json_fails_verification() {
    let dir = tempfile::TempDir::new().unwrap();
    let (key_path, _, public_pem) = test_keypair_files(dir.path());
    let storage = MemoryBackend::new();

    let report = test_report("validation-run-2", true);
    let emission = emit_evidence(&storage, &report, &signed_config(&key_path))
        .await
        .unwrap();
    let stored_sig = reload(&storage, emission.signature_key.as_ref().unwrap()).await;

    // Semantically small tamper: flip a digit inside the stored JSON.
    let mut tampered = emission.report_bytes.clone();
    let pos = tampered
        .windows(4)
        .position(|w| w == b"2555")
        .expect("retention value present");
    tampered[pos] = b'9';

    let outcome = evidence::verify_evidence(
        &tampered,
        std::str::from_utf8(&stored_sig).unwrap(),
        Some(&public_pem),
    )
    .unwrap();
    assert!(!outcome.verified());
    assert!(!outcome.digest_valid, "digest must catch the tamper");
    assert!(!outcome.signature_valid);
}

#[tokio::test]
async fn tampered_envelope_fails_verification() {
    let dir = tempfile::TempDir::new().unwrap();
    let (key_path, _, public_pem) = test_keypair_files(dir.path());
    let storage = MemoryBackend::new();

    let report = test_report("validation-run-3", true);
    let emission = emit_evidence(&storage, &report, &signed_config(&key_path))
        .await
        .unwrap();
    let stored_sig = reload(&storage, emission.signature_key.as_ref().unwrap()).await;

    // Tamper the digest hint: verification must fail even though the
    // signature still covers the true payload bytes.
    let mut envelope: serde_json::Value = serde_json::from_slice(&stored_sig).unwrap();
    envelope["payload_sha256"] = serde_json::json!("00".repeat(32));
    let outcome = evidence::verify_evidence(
        &emission.report_bytes,
        &envelope.to_string(),
        Some(&public_pem),
    )
    .unwrap();
    assert!(!outcome.verified());
    assert!(!outcome.digest_valid);
    assert!(
        outcome.signature_valid,
        "signature still covers the true bytes — digest binding must fail the artifact"
    );

    // Tamper the signature itself.
    let mut envelope: serde_json::Value = serde_json::from_slice(&stored_sig).unwrap();
    let sig = envelope["signature"].as_str().unwrap().to_string();
    let mut sig_bytes =
        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, sig.as_bytes()).unwrap();
    sig_bytes[10] ^= 0xFF;
    envelope["signature"] = serde_json::json!(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD,
        sig_bytes
    ));
    let result = evidence::verify_evidence(
        &emission.report_bytes,
        &envelope.to_string(),
        Some(&public_pem),
    );
    match result {
        Ok(outcome) => {
            assert!(!outcome.verified());
            assert!(outcome.digest_valid);
            assert!(!outcome.signature_valid);
        }
        // A corrupted signature that no longer parses as ECDSA is also a
        // hard failure.
        Err(e) => assert!(e.to_string().contains("signature")),
    }
}

#[tokio::test]
async fn wrong_public_key_fails_verification() {
    let dir = tempfile::TempDir::new().unwrap();
    let (key_path, _, _) = test_keypair_files(dir.path());
    let dir2 = tempfile::TempDir::new().unwrap();
    let (_, _, wrong_public) = test_keypair_files(dir2.path());
    let storage = MemoryBackend::new();

    let report = test_report("validation-run-4", true);
    let emission = emit_evidence(&storage, &report, &signed_config(&key_path))
        .await
        .unwrap();
    let stored_sig = reload(&storage, emission.signature_key.as_ref().unwrap()).await;

    let outcome = evidence::verify_evidence(
        &emission.report_bytes,
        std::str::from_utf8(&stored_sig).unwrap(),
        Some(&wrong_public),
    )
    .unwrap();
    assert!(!outcome.verified());
    assert!(outcome.digest_valid, "digest is key-independent");
    assert!(
        !outcome.signature_valid,
        "wrong key must fail the signature"
    );
}

#[tokio::test]
async fn legacy_v1_evidence_still_verifies_including_blank_digest_bug() {
    let dir = tempfile::TempDir::new().unwrap();
    let (_, private_pem, public_pem) = test_keypair_files(dir.path());

    // Reproduce the original v1 pipeline: schema 1.0 report serialized while
    // integrity.report_sha256 was still blank, signed over those exact
    // bytes with the legacy text bundle.
    let mut report = test_report("legacy-run-1", true);
    report.schema_version = "1.0".to_string();
    report.integrity.report_sha256 = String::new();
    let legacy_bytes = report.to_deterministic_json().unwrap();
    let bundle = kafka_backup_core::evidence::signing::sign_report(
        &legacy_bytes,
        &private_pem,
        "legacy-run-1",
    )
    .unwrap();
    let sig_text = bundle.to_sig_file();

    let outcome = evidence::verify_evidence(&legacy_bytes, &sig_text, Some(&public_pem)).unwrap();
    assert!(outcome.verified(), "{:?}", outcome.messages);
    assert_eq!(outcome.contract, EvidenceContractVersion::V1Legacy);
    assert_eq!(outcome.report_schema_version.as_deref(), Some("1.0"));

    // Legacy evidence with a tampered payload must fail.
    let mut tampered = legacy_bytes.clone();
    let last = tampered.len() - 2;
    tampered[last] = b'X';
    let outcome = evidence::verify_evidence(&tampered, &sig_text, Some(&public_pem)).unwrap();
    assert!(!outcome.verified());

    // Legacy evidence without a public key still gets digest validation.
    let outcome = evidence::verify_evidence(&legacy_bytes, &sig_text, None).unwrap();
    assert!(outcome.digest_valid);
    assert!(!outcome.signature_checked);
    assert!(outcome.verified(), "digest-only verification passes");
}

#[tokio::test]
async fn unsigned_emission_digest_covers_stored_pretty_bytes() {
    let storage = MemoryBackend::new();
    let report = test_report("validation-run-5", false);
    let config = EvidenceConfig {
        formats: vec![EvidenceFormat::Json],
        signing: SigningConfig::default(),
        storage: EvidenceStorageConfig::default(),
    };

    let emission = emit_evidence(&storage, &report, &config).await.unwrap();
    assert!(!emission.signed);
    assert!(emission.signature_key.is_none());

    let stored_json = reload(&storage, emission.json_key.as_ref().unwrap()).await;
    // Pretty JSON for readability when unsigned…
    assert!(stored_json.windows(2).any(|w| w == b"\n "));
    // …and the displayed digest still covers exactly the stored bytes.
    assert_eq!(evidence::sha256_hex(&stored_json), emission.report_sha256);
}

#[tokio::test]
async fn signing_forces_json_storage_with_warning() {
    let dir = tempfile::TempDir::new().unwrap();
    let (key_path, _, public_pem) = test_keypair_files(dir.path());
    let storage = MemoryBackend::new();

    let report = test_report("validation-run-6", true);
    let mut config = signed_config(&key_path);
    config.formats = vec![EvidenceFormat::Pdf]; // JSON omitted on purpose

    let emission = emit_evidence(&storage, &report, &config).await.unwrap();
    assert!(
        emission.json_key.is_some(),
        "signed evidence must store its payload"
    );
    assert!(emission
        .warnings
        .iter()
        .any(|w| w.contains("storing the report JSON anyway")));

    let stored_json = reload(&storage, emission.json_key.as_ref().unwrap()).await;
    let stored_sig = reload(&storage, emission.signature_key.as_ref().unwrap()).await;
    let outcome = evidence::verify_evidence(
        &stored_json,
        std::str::from_utf8(&stored_sig).unwrap(),
        Some(&public_pem),
    )
    .unwrap();
    assert!(outcome.verified());
}

#[tokio::test]
async fn signing_failure_stores_nothing() {
    let storage = MemoryBackend::new();
    let report = test_report("validation-run-7", true);
    let config = EvidenceConfig {
        formats: vec![EvidenceFormat::Json],
        signing: SigningConfig {
            enabled: true,
            private_key_path: Some("/nonexistent/key.pem".to_string()),
            public_key_path: None,
        },
        storage: EvidenceStorageConfig::default(),
    };

    let err = emit_evidence(&storage, &report, &config).await.unwrap_err();
    assert!(err.to_string().contains("signing key"));
    let keys = storage.list("").await.unwrap();
    assert!(
        keys.is_empty(),
        "a signing failure must not leave partial artifacts: {keys:?}"
    );
}

#[tokio::test]
async fn emission_is_deterministic_for_identical_reports() {
    let dir = tempfile::TempDir::new().unwrap();
    let (key_path, _, _) = test_keypair_files(dir.path());

    let report = test_report("validation-run-8", true);
    let storage_a = MemoryBackend::new();
    let storage_b = MemoryBackend::new();
    let a = emit_evidence(&storage_a, &report, &signed_config(&key_path))
        .await
        .unwrap();
    let b = emit_evidence(&storage_b, &report, &signed_config(&key_path))
        .await
        .unwrap();

    assert_eq!(
        a.report_bytes, b.report_bytes,
        "serialization is deterministic"
    );
    assert_eq!(a.report_sha256, b.report_sha256);
}

#[tokio::test]
async fn report_with_embedded_digest_is_rejected() {
    let dir = tempfile::TempDir::new().unwrap();
    let (key_path, _, _) = test_keypair_files(dir.path());
    let storage = MemoryBackend::new();

    let mut report = test_report("validation-run-9", true);
    report.integrity.report_sha256 = "de".repeat(32); // illegal under contract v2

    let err = emit_evidence(&storage, &report, &signed_config(&key_path))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("must be empty"));
}
