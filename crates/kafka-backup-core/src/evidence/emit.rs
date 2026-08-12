//! Centralized evidence artifact creation and storage.
//!
//! Both the OSS and enterprise CLIs build, serialize, hash, sign, and upload
//! evidence through this module, so the two binaries cannot drift apart in
//! byte-level behaviour (issue #138).
//!
//! Contract v2 invariants enforced here:
//! - the report is serialized exactly once; the stored JSON, the digest, the
//!   envelope signature, and the digest displayed in the PDF/CLI all refer to
//!   that same byte sequence;
//! - the serialized report never embeds its own digest;
//! - when signing is enabled the JSON payload is always stored (a detached
//!   envelope without its payload would be unverifiable).

use chrono::Utc;

use crate::manifest::BackupManifest;
use crate::storage::StorageBackend;
use crate::validation::config::{EvidenceConfig, EvidenceFormat};
use crate::validation::ValidationSummary;
use crate::Result;

use super::envelope::{sha256_hex, sign_evidence, SIGNATURE_ALGORITHM};
use super::report::{
    BackupInfo, EvidenceReport, IntegrityInfo, RestoreInfo, ToolInfo,
    EVIDENCE_REPORT_SCHEMA_VERSION,
};
use super::{pdf, storage as evidence_storage};

/// Inputs for building a validation evidence report.
pub struct EvidenceReportParams<'a> {
    /// Unique run/report ID.
    pub run_id: &'a str,
    /// Version of the emitting binary (e.g. `env!("CARGO_PKG_VERSION")`).
    pub tool_version: &'a str,
    /// Backup that was validated.
    pub backup_id: &'a str,
    /// Loaded backup manifest.
    pub manifest: &'a BackupManifest,
    /// SHA-256 hex of the manifest bytes as loaded from storage.
    pub manifest_sha256: String,
    /// Human-readable storage backend description.
    pub storage_backend: String,
    /// PITR timestamp used during restore, if any.
    pub pitr_timestamp: Option<i64>,
    /// Bootstrap servers of the validated target cluster.
    pub target_bootstrap_servers: Vec<String>,
    /// Validation check results.
    pub summary: ValidationSummary,
    /// Evidence retention configured, in days (compliance mappings).
    pub retention_days: u32,
    /// Whether the report will be signed.
    pub signing_enabled: bool,
    /// Who or what triggered the run.
    pub triggered_by: Option<String>,
}

/// Build a schema 1.1 evidence report from validation results.
///
/// The report never embeds its own digest (`integrity.report_sha256` stays
/// empty and is skipped during serialization); the digest lives in the
/// detached envelope produced by [`emit_evidence`].
pub fn build_evidence_report(params: EvidenceReportParams<'_>) -> EvidenceReport {
    let check_names: Vec<String> = params
        .summary
        .results
        .iter()
        .map(|r| r.check_name.clone())
        .collect();
    let total_partitions: usize = params
        .manifest
        .topics
        .iter()
        .map(|t| t.partitions.len())
        .sum();
    let compliance =
        EvidenceReport::build_compliance_mappings(&check_names, params.retention_days, None);

    EvidenceReport {
        schema_version: EVIDENCE_REPORT_SCHEMA_VERSION.to_string(),
        report_id: params.run_id.to_string(),
        generated_at: Utc::now().to_rfc3339(),
        tool: ToolInfo {
            name: "kafka-backup".to_string(),
            version: params.tool_version.to_string(),
        },
        backup: BackupInfo {
            id: params.backup_id.to_string(),
            source_cluster_id: params.manifest.source_cluster_id.clone(),
            source_brokers: params.manifest.source_brokers.clone(),
            storage_backend: params.storage_backend,
            pitr_timestamp: params.pitr_timestamp,
            created_at: params.manifest.created_at,
            total_topics: params.manifest.topics.len(),
            total_partitions,
            total_segments: params.manifest.total_segments(),
            total_records: params.manifest.total_records(),
        },
        restore: Some(RestoreInfo {
            target_bootstrap_servers: params.target_bootstrap_servers,
            start_time: None,
            end_time: None,
            duration_seconds: None,
        }),
        validation: params.summary,
        integrity: IntegrityInfo {
            backup_manifest_sha256: params.manifest_sha256,
            report_sha256: String::new(),
            checksums_valid: true,
            signature_algorithm: if params.signing_enabled {
                SIGNATURE_ALGORITHM.to_string()
            } else {
                "none".to_string()
            },
            signed_by: None,
        },
        compliance_mappings: compliance,
        triggered_by: params.triggered_by,
    }
}

/// Artifacts written by [`emit_evidence`].
#[derive(Debug, Clone)]
pub struct EvidenceEmission {
    /// Storage key of the stored report JSON, when written.
    pub json_key: Option<String>,
    /// Storage key of the PDF, when written.
    pub pdf_key: Option<String>,
    /// Storage key of the detached signature envelope, when written.
    pub signature_key: Option<String>,
    /// Lowercase hex SHA-256 of the exact stored report bytes. This is the
    /// digest shown in the PDF and CLI and recorded in the envelope.
    pub report_sha256: String,
    /// The exact stored report bytes.
    pub report_bytes: Vec<u8>,
    /// Whether a signature envelope was produced.
    pub signed: bool,
    /// Non-fatal findings (e.g. PDF generation failure).
    pub warnings: Vec<String>,
}

/// Serialize, hash, sign, and upload evidence artifacts.
///
/// Byte-level behaviour:
/// - signing enabled: the report is stored as deterministic compact JSON and
///   the envelope's digest/signature cover exactly those stored bytes;
/// - signing disabled: the report is stored as pretty JSON and the reported
///   digest covers exactly those stored bytes;
/// - the PDF always displays the digest of the stored JSON bytes.
pub async fn emit_evidence(
    storage: &dyn StorageBackend,
    report: &EvidenceReport,
    config: &EvidenceConfig,
) -> Result<EvidenceEmission> {
    let signing_enabled = config.signing.enabled;
    let mut warnings = Vec::new();

    if !report.integrity.report_sha256.is_empty() {
        return Err(crate::Error::Evidence(
            "report.integrity.report_sha256 must be empty: schema 1.1 reports never embed \
             their own digest (it lives in the detached envelope)"
                .to_string(),
        ));
    }

    // Serialize exactly once. Everything downstream refers to these bytes.
    let report_bytes = if signing_enabled {
        report.to_deterministic_json()?
    } else {
        report.to_pretty_json()?
    };
    let report_sha256 = sha256_hex(&report_bytes);

    // Sign before uploading anything so a signing failure stores nothing.
    let envelope = if signing_enabled {
        let key_path = config.signing.private_key_path.as_ref().ok_or_else(|| {
            crate::Error::Evidence(
                "signing.enabled is true but signing.private_key_path is not configured"
                    .to_string(),
            )
        })?;
        let pem = std::fs::read_to_string(key_path).map_err(|e| {
            crate::Error::Evidence(format!("Failed to read signing key {key_path}: {e}"))
        })?;
        Some(sign_evidence(&report_bytes, &pem, &report.report_id)?)
    } else {
        None
    };

    // A single timestamp keeps the JSON/PDF/envelope under the same
    // year/month prefix even across a month boundary.
    let now = Utc::now();

    // Store the JSON payload. Signing forces JSON storage: the detached
    // envelope is unverifiable without its payload bytes.
    let store_json = config.formats.contains(&EvidenceFormat::Json) || signing_enabled;
    if signing_enabled && !config.formats.contains(&EvidenceFormat::Json) {
        warnings.push(
            "evidence.formats does not include 'json' but signing is enabled; storing the \
             report JSON anyway (the signature envelope covers those bytes)"
                .to_string(),
        );
    }
    let json_key = if store_json {
        Some(
            evidence_storage::upload_evidence_json(
                storage,
                &config.storage.prefix,
                &report.report_id,
                &report_bytes,
                now,
            )
            .await?,
        )
    } else {
        None
    };

    // PDF displays the digest of the stored JSON bytes.
    let pdf_key = if config.formats.contains(&EvidenceFormat::Pdf) {
        match pdf::generate_pdf(report, Some(&report_sha256)) {
            Ok(pdf_bytes) => Some(
                evidence_storage::upload_evidence_pdf(
                    storage,
                    &config.storage.prefix,
                    &report.report_id,
                    &pdf_bytes,
                    now,
                )
                .await?,
            ),
            Err(e) => {
                warnings.push(format!("PDF generation failed: {e}"));
                None
            }
        }
    } else {
        None
    };

    let signature_key = if let Some(envelope) = &envelope {
        Some(
            evidence_storage::upload_evidence_signature(
                storage,
                &config.storage.prefix,
                &report.report_id,
                &String::from_utf8(envelope.to_json()?).expect("envelope JSON is UTF-8"),
                now,
            )
            .await?,
        )
    } else {
        None
    };

    Ok(EvidenceEmission {
        json_key,
        pdf_key,
        signature_key,
        report_sha256,
        report_bytes,
        signed: envelope.is_some(),
        warnings,
    })
}
