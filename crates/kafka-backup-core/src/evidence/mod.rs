//! Evidence report generation, cryptographic signing, and storage.
//!
//! Produces machine-readable JSON and human-readable PDF compliance evidence
//! reports from validation results, optionally signed with ECDSA-P256-SHA256.

pub mod pdf;
pub mod report;
pub mod signing;
pub mod storage;

pub use report::EvidenceReport;
