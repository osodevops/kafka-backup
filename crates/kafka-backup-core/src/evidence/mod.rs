//! Evidence report generation, cryptographic signing, and storage.
//!
//! Produces machine-readable JSON and human-readable PDF compliance evidence
//! reports from validation results, optionally signed with ECDSA-P256-SHA256.
//!
//! The byte-level evidence contract (serialization, hashing, signing,
//! storage, verification, and the legacy v1 path) is documented in
//! `docs/evidence-contract.md`. Artifact creation and verification are
//! centralized in [`emit`] and [`envelope`] so the OSS and enterprise CLIs
//! cannot drift.

pub mod emit;
pub mod envelope;
pub mod pdf;
pub mod report;
pub mod signing;
pub mod storage;

pub use emit::{build_evidence_report, emit_evidence, EvidenceEmission, EvidenceReportParams};
pub use envelope::{
    parse_signature_artifact, sha256_hex, sign_evidence, verify_evidence, EvidenceContractVersion,
    EvidenceEnvelope, SignatureArtifact, VerificationOutcome, ENVELOPE_SCHEMA_V2,
    EVIDENCE_PAYLOAD_TYPE, SIGNATURE_ALGORITHM,
};
pub use report::{EvidenceReport, EVIDENCE_REPORT_SCHEMA_VERSION};
