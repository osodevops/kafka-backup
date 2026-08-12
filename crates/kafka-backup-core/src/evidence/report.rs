//! Evidence report data structures and deterministic JSON serialization.

use serde::{Deserialize, Serialize};

use crate::validation::ValidationSummary;
use crate::Result;

/// Schema version written by this build (evidence contract v2).
///
/// - `1.0`: legacy contract — `integrity.report_sha256` was populated after
///   serialization, so signed/stored JSON carried an empty value while the
///   in-memory report carried the digest (issue #138).
/// - `1.1`: the serialized report no longer embeds its own digest; the digest
///   and signature live in the detached evidence envelope
///   (`kafka-backup/evidence-envelope/v2`).
pub const EVIDENCE_REPORT_SCHEMA_VERSION: &str = "1.1";

/// The evidence report payload. All other outputs (PDF, signature envelope)
/// derive from the exact stored serialization of this structure.
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

    /// Legacy (schema 1.0) self-referential report digest. Retained only so
    /// legacy evidence deserializes; schema 1.1 reports never serialize it —
    /// the report digest lives in the detached evidence envelope, which
    /// covers the exact stored report bytes. Leave empty when building new
    /// reports.
    #[serde(default, skip_serializing_if = "String::is_empty")]
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
    /// Serialize to deterministic, compact JSON.
    ///
    /// Determinism comes from `serde_json::Value` object maps being
    /// `BTreeMap`-backed (keys sorted by Rust `String` ordering, i.e. UTF-8
    /// byte order) and compact output with no whitespace, encoded as UTF-8.
    ///
    /// This is **deliberately not RFC 8785 (JCS)** and must not be described
    /// as such: RFC 8785 §3.2.3 sorts keys by UTF-16 code units (differs
    /// from UTF-8 byte order for supplementary-plane characters) and
    /// §3.2.2.3 requires ECMAScript number formatting (e.g. `1e+21`, `-0`
    /// serialized as `0`), which serde_json does not implement. The evidence
    /// contract does not depend on canonicalization: the digest and
    /// signature always cover the exact stored bytes, and verification never
    /// re-serializes. See `docs/evidence-contract.md`.
    pub fn to_deterministic_json(&self) -> Result<Vec<u8>> {
        let value = serde_json::to_value(self)
            .map_err(|e| crate::Error::Evidence(format!("Failed to serialize report: {e}")))?;
        let bytes = serde_json::to_vec(&value).map_err(|e| {
            crate::Error::Evidence(format!("Failed to produce deterministic JSON: {e}"))
        })?;
        Ok(bytes)
    }

    /// Serialize to pretty JSON for human readability.
    pub fn to_pretty_json(&self) -> Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
            .map_err(|e| crate::Error::Evidence(format!("Failed to serialize report: {e}")))
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

#[cfg(test)]
mod tests {
    /// Pins the known divergences between serde_json's deterministic output
    /// and RFC 8785 (JCS), so the evidence contract's "deterministic but NOT
    /// canonical" claim stays provably accurate. If this test ever fails,
    /// serde_json's formatting changed and `docs/evidence-contract.md` must
    /// be re-reviewed.
    #[test]
    fn deterministic_json_is_not_rfc8785() {
        // RFC 8785 §3.2.2.3 requires ECMAScript number formatting (ECMA-262
        // §7.1.12.1). serde_json's -0.0 serializes as "-0.0"; JCS requires
        // "0" (RFC 8785 Appendix B).
        let neg_zero = serde_json::to_string(&serde_json::json!(-0.0_f64)).unwrap();
        assert_eq!(neg_zero, "-0.0", "serde_json -0 diverges from JCS");

        // ECMAScript prints 1e-6 as "0.000001" (decimal down to 1e-6
        // inclusive; exponent form only below that). serde_json emits "1e-6".
        let micro = serde_json::to_string(&serde_json::json!(0.000001_f64)).unwrap();
        assert_eq!(
            micro, "1e-6",
            "serde_json small-number format diverges from JCS"
        );

        // Integers beyond 2^53 serialize at full u64 precision, which
        // ECMA-262 double formatting cannot produce.
        let big_int = serde_json::to_string(&serde_json::json!(9007199254740993_u64)).unwrap();
        assert_eq!(big_int, "9007199254740993");

        // RFC 8785 §3.2.3 sorts keys by UTF-16 code units. U+10000 (surrogate
        // pair D800 DC00 in UTF-16) sorts BEFORE U+E000 under JCS, but its
        // UTF-8 encoding (F0 90 80 80) sorts AFTER U+E000 (EE 80 80) under
        // serde_json's byte-wise BTreeMap ordering.
        let mut map = serde_json::Map::new();
        map.insert("\u{E000}".to_string(), serde_json::json!(1));
        map.insert("\u{10000}".to_string(), serde_json::json!(2));
        let out = serde_json::to_string(&serde_json::Value::Object(map)).unwrap();
        let e000_pos = out.find('\u{E000}').unwrap();
        let supp_pos = out.find('\u{10000}').unwrap();
        assert!(
            e000_pos < supp_pos,
            "serde_json (UTF-8 byte order) places U+E000 first; JCS (UTF-16 \
             order) would place U+10000 first"
        );
    }

    /// The deterministic serialization is byte-stable for a fixed value.
    #[test]
    fn deterministic_json_is_reproducible_and_key_sorted() {
        let value = serde_json::json!({
            "zebra": 1,
            "alpha": {"nested_z": true, "nested_a": [1, 2, 3]},
            "mid": "text with \"quotes\" and \n newline"
        });
        let a = serde_json::to_vec(&value).unwrap();
        let b = serde_json::to_vec(&value).unwrap();
        assert_eq!(a, b);
        assert_eq!(
            String::from_utf8(a).unwrap(),
            r#"{"alpha":{"nested_a":[1,2,3],"nested_z":true},"mid":"text with \"quotes\" and \n newline","zebra":1}"#
        );
    }
}
