//! Evidence report data structures and canonical JSON serialization.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::validation::ValidationSummary;
use crate::Result;

/// The canonical evidence report. All other outputs (PDF, signature) derive from this.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceReport {
    /// Schema version for forward compatibility.
    pub schema_version: String,

    /// Unique identifier for this validation run.
    pub report_id: String,

    /// ISO-8601 timestamp of report generation.
    pub generated_at: String,

    /// Information about the tool that produced this report.
    pub tool: ToolInfo,

    /// Backup that was validated.
    pub backup: BackupInfo,

    /// Restore details (if a restore was performed as part of validation).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub restore: Option<RestoreInfo>,

    /// Validation check results.
    pub validation: ValidationSummary,

    /// Integrity and signing information.
    pub integrity: IntegrityInfo,

    /// Compliance framework mappings.
    pub compliance_mappings: ComplianceMappings,

    /// Who or what triggered this validation run.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub triggered_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolInfo {
    pub name: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupInfo {
    pub id: String,
    pub source_cluster_id: Option<String>,
    pub source_brokers: Vec<String>,
    pub storage_backend: String,
    pub pitr_timestamp: Option<i64>,
    pub created_at: i64,
    pub total_topics: usize,
    pub total_partitions: usize,
    pub total_segments: usize,
    pub total_records: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreInfo {
    pub target_bootstrap_servers: Vec<String>,
    pub start_time: Option<String>,
    pub end_time: Option<String>,
    pub duration_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityInfo {
    /// SHA-256 of the backup manifest.
    pub backup_manifest_sha256: String,

    /// SHA-256 of the canonical JSON report (before signing).
    /// Populated after serialization.
    #[serde(default)]
    pub report_sha256: String,

    /// Whether checksums match expectations.
    pub checksums_valid: bool,

    /// Signature algorithm used (e.g. "ECDSA-P256-SHA256"), or "none".
    pub signature_algorithm: String,

    /// Identity that signed the report (from key metadata), if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signed_by: Option<String>,
}

/// Maps validation checks to compliance framework controls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComplianceMappings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sox_itgc: Option<SoxMapping>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub cmmc_l2: Option<CmmcMapping>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub gdpr_art32: Option<GdprMapping>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoxMapping {
    pub control: String,
    pub satisfied_by: Vec<String>,
    pub evidence_retention_required_years: u32,
    pub evidence_retention_configured_days: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CmmcMapping {
    pub control: String,
    pub description: String,
    pub satisfied_by: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GdprMapping {
    pub control: String,
    pub satisfied_by: Vec<String>,
    pub test_frequency: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rto_demonstrated_seconds: Option<u64>,
}

impl EvidenceReport {
    /// Serialize to canonical JSON (sorted keys, no extra whitespace).
    /// This is the byte sequence that gets hashed and signed.
    pub fn to_canonical_json(&self) -> Result<Vec<u8>> {
        // serde_json with sorted maps produces deterministic output.
        let value = serde_json::to_value(self)
            .map_err(|e| crate::Error::Evidence(format!("Failed to serialize report: {e}")))?;
        let canonical = serde_json::to_vec(&value).map_err(|e| {
            crate::Error::Evidence(format!("Failed to produce canonical JSON: {e}"))
        })?;
        Ok(canonical)
    }

    /// Serialize to pretty JSON for human readability.
    pub fn to_pretty_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
            .map_err(|e| crate::Error::Evidence(format!("Failed to serialize report: {e}")))
    }

    /// Compute the SHA-256 digest of the canonical JSON representation.
    pub fn sha256_digest(&self) -> Result<[u8; 32]> {
        let canonical = self.to_canonical_json()?;
        let mut hasher = Sha256::new();
        hasher.update(&canonical);
        Ok(hasher.finalize().into())
    }

    /// Build the standard compliance mappings based on which checks passed.
    pub fn build_compliance_mappings(
        check_names: &[String],
        retention_days: u32,
        rto_seconds: Option<u64>,
    ) -> ComplianceMappings {
        ComplianceMappings {
            sox_itgc: Some(SoxMapping {
                control: "IT General Controls - Backup and Recovery".to_string(),
                satisfied_by: check_names
                    .iter()
                    .filter(|n| *n == "MessageCountCheck" || *n == "OffsetRangeCheck")
                    .cloned()
                    .collect(),
                evidence_retention_required_years: 7,
                evidence_retention_configured_days: retention_days,
            }),
            cmmc_l2: Some(CmmcMapping {
                control: "RE.3.139".to_string(),
                description: "Regularly perform and test data back-ups".to_string(),
                satisfied_by: check_names.to_vec(),
            }),
            gdpr_art32: Some(GdprMapping {
                control: "Article 32 - Testing technical measures".to_string(),
                satisfied_by: check_names
                    .iter()
                    .filter(|n| *n == "MessageCountCheck" || *n == "OffsetRangeCheck")
                    .cloned()
                    .collect(),
                test_frequency: "on-demand".to_string(),
                rto_demonstrated_seconds: rto_seconds,
            }),
        }
    }
}

pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
