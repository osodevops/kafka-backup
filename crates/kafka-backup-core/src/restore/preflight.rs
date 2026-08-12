//! Phase 1 header preflight: proves tracking-metadata coverage before restore.
//!
//! Three-phase restore relies on per-record tracking metadata written during
//! backup (`x-original-offset` / `x-original-timestamp` headers, plus the
//! consumer-groups snapshot for `auto_consumer_groups`). This module scans the
//! selected backup — every selected topic and partition, every segment that
//! the restore would process — and produces a structured coverage report.
//!
//! The scan is read-only: it never contacts the target cluster and never
//! mutates storage. When consumer-offset recovery is requested and required
//! metadata is missing, partial, corrupt, or unprovable, the preflight fails
//! so the caller can abort before creating topics or producing any record.
//!
//! Coverage semantics:
//! - zero records scanned is never a positive pass (`Empty`/`Indeterminate`
//!   are explicit states, distinct from `Full`);
//! - a segment that cannot be read is `Corrupt` (parse/CRC/decompress) or
//!   `DataMissing` (object absent from storage), never silently skipped;
//! - legacy backups (no tracking headers, or legacy JSON segment format) are
//!   reported explicitly and only pass when offset recovery is not requested.

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::{HeaderPreflightMode, RestoreOptions, TopicSelection};
use crate::error::StorageError;
use crate::manifest::{BackupManifest, BackupRecord, TopicBackup};
use crate::storage::StorageBackend;
use crate::Error;

/// Tracking header written by the backup engine for Phase 1.
pub const OFFSET_HEADER: &str = "x-original-offset";
/// Tracking header written by the backup engine for Phase 1.
pub const TIMESTAMP_HEADER: &str = "x-original-timestamp";
/// Optional source-cluster tracking header.
pub const SOURCE_CLUSTER_HEADER: &str = "x-source-cluster";

/// Maximum example artifacts (segment keys) retained per partition problem.
const MAX_EXAMPLES: usize = 3;

/// Header coverage state for one selected topic/partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PartitionCoverageState {
    /// Every scanned record carries the required offset and timestamp
    /// tracking headers, and at least one record was scanned.
    Full,
    /// Some scanned records carry the required tracking headers, others do
    /// not (for example a backup whose `include_offset_headers` setting
    /// changed between runs).
    Partial,
    /// Records were scanned and none carry the required tracking headers
    /// (typically a legacy or non-Phase-1 backup).
    Missing,
    /// The backup holds no records for this partition within the selected
    /// time window. Explicitly not a positive pass.
    Empty,
    /// The manifest references segment objects that are absent from storage.
    DataMissing,
    /// A segment exists but could not be decoded (CRC mismatch, truncated
    /// data, decompression or deserialization failure).
    Corrupt,
    /// Coverage was not determined: the scan was skipped, or storage returned
    /// a non-NotFound error. Never a positive pass.
    Indeterminate,
}

impl std::fmt::Display for PartitionCoverageState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Full => "full",
            Self::Partial => "partial",
            Self::Missing => "missing",
            Self::Empty => "empty",
            Self::DataMissing => "data_missing",
            Self::Corrupt => "corrupt",
            Self::Indeterminate => "indeterminate",
        };
        f.write_str(s)
    }
}

/// Per-topic/partition tracking-header coverage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionHeaderCoverage {
    /// Source topic name.
    pub topic: String,
    /// Source partition id.
    pub partition: i32,
    /// Derived coverage state.
    pub state: PartitionCoverageState,
    /// Segments the restore would process for this partition (after
    /// time-window filtering).
    pub segments_selected: usize,
    /// Segments successfully opened and decoded.
    pub segments_scanned: usize,
    /// Segments whose storage object was absent.
    pub segments_missing: usize,
    /// Segments that failed CRC/format/decode.
    pub segments_corrupt: usize,
    /// Segments that failed with a non-NotFound storage error.
    pub segments_unreadable: usize,
    /// Segments stored in the legacy JSON format (pre-binary).
    pub segments_legacy_format: usize,
    /// Record count the manifest claims for the selected segments.
    pub manifest_record_count: i64,
    /// Records actually decoded and inspected (after time-window filtering).
    pub records_scanned: u64,
    /// Records carrying a decodable `x-original-offset` header.
    pub records_with_offset_header: u64,
    /// Records carrying a decodable `x-original-timestamp` header.
    pub records_with_timestamp_header: u64,
    /// Records carrying an `x-source-cluster` header.
    pub records_with_source_cluster_header: u64,
    /// Records carrying both required tracking headers.
    pub records_with_required_headers: u64,
    /// Actionable per-partition problems (bounded examples).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub problems: Vec<String>,
}

/// Result of checking the consumer-groups snapshot required by
/// `auto_consumer_groups`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotCheck {
    /// `present`, `missing`, or `invalid`.
    pub state: String,
    /// Number of consumer groups in the snapshot (when present).
    pub groups: usize,
    /// Number of partition offsets in the snapshot (when present).
    pub offsets: usize,
    /// Human-readable detail (storage key, error text).
    pub detail: String,
}

/// Structured result of the Phase 1 header preflight.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderPreflightReport {
    /// Backup that was inspected.
    pub backup_id: String,
    /// RFC 3339 timestamp of the preflight run.
    pub generated_at: String,
    /// Preflight mode that was in effect (`auto`, `full`, `skip`).
    pub mode: String,
    /// Whether consumer-offset recovery was requested by the restore
    /// configuration (`reset_consumer_offsets` or `auto_consumer_groups`).
    pub offset_recovery_requested: bool,
    /// Whether segments were actually opened and records inspected. When
    /// false every partition is `Indeterminate` or `Empty`.
    pub scan_performed: bool,
    /// Per-topic/partition coverage, sorted by topic then partition.
    pub partitions: Vec<PartitionHeaderCoverage>,
    /// Consumer-groups snapshot check (present only when
    /// `auto_consumer_groups` is requested).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_group_snapshot: Option<SnapshotCheck>,
    /// Total records inspected across all partitions.
    pub records_scanned_total: u64,
    /// Final verdict given the restore configuration. Only meaningful for
    /// blocking decisions when `offset_recovery_requested` is true.
    pub passed: bool,
    /// Blocking, actionable errors (non-empty implies `passed == false`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
    /// Non-blocking findings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

impl HeaderPreflightReport {
    /// One-line summary suitable for logs and error messages.
    pub fn summary(&self) -> String {
        let mut counts = std::collections::BTreeMap::new();
        for p in &self.partitions {
            *counts.entry(p.state.to_string()).or_insert(0usize) += 1;
        }
        let states = counts
            .iter()
            .map(|(k, v)| format!("{v} {k}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "backup '{}': {} partition(s) [{}], {} record(s) scanned, verdict: {}",
            self.backup_id,
            self.partitions.len(),
            if states.is_empty() {
                "none selected".to_string()
            } else {
                states
            },
            self.records_scanned_total,
            if self.passed { "PASS" } else { "FAIL" }
        )
    }
}

/// Whether the restore configuration requests consumer-offset recovery.
///
/// `reset_consumer_offsets` is an explicit request. `auto_consumer_groups`
/// requests recovery driven by the backup's consumer-groups snapshot.
pub fn offset_recovery_requested(options: &RestoreOptions) -> bool {
    options.reset_consumer_offsets || options.auto_consumer_groups
}

/// Whether the preflight should open segments for the given mode/config.
pub fn scan_required(mode: HeaderPreflightMode, options: &RestoreOptions) -> bool {
    match mode {
        HeaderPreflightMode::Skip => false,
        HeaderPreflightMode::Full => true,
        HeaderPreflightMode::Auto => offset_recovery_requested(options),
    }
}

/// Select manifest topics using the same include/exclude semantics as the
/// restore engine.
pub fn select_topics<'a>(
    manifest: &'a BackupManifest,
    selection: &TopicSelection,
) -> Vec<&'a TopicBackup> {
    manifest
        .topics
        .iter()
        .filter(|topic| {
            let included = selection.include.is_empty()
                || selection
                    .include
                    .iter()
                    .any(|p| super::engine::pattern_match(p, &topic.name));
            let excluded = selection
                .exclude
                .iter()
                .any(|p| super::engine::pattern_match(p, &topic.name));
            included && !excluded
        })
        .collect()
}

/// Run the Phase 1 header preflight.
///
/// Read-only: inspects backup storage only. `mode` controls whether segments
/// are opened (`scan_required`); with scanning disabled, partitions are
/// classified `Empty` or `Indeterminate` from manifest metadata alone.
pub async fn run_header_preflight(
    storage: &dyn StorageBackend,
    manifest: &BackupManifest,
    selection: &TopicSelection,
    options: &RestoreOptions,
    mode: HeaderPreflightMode,
) -> HeaderPreflightReport {
    let recovery = offset_recovery_requested(options);
    let scan = scan_required(mode, options);

    let mut partitions = Vec::new();
    let mut records_total = 0u64;

    for topic in select_topics(manifest, selection) {
        for partition in &topic.partitions {
            if let Some(filter) = &options.source_partitions {
                if !filter.contains(&partition.partition_id) {
                    continue;
                }
            }

            let segments: Vec<_> = partition
                .segments
                .iter()
                .filter(|s| {
                    s.overlaps_time_window(options.time_window_start, options.time_window_end)
                })
                .collect();

            let mut cov = PartitionHeaderCoverage {
                topic: topic.name.clone(),
                partition: partition.partition_id,
                state: PartitionCoverageState::Indeterminate,
                segments_selected: segments.len(),
                segments_scanned: 0,
                segments_missing: 0,
                segments_corrupt: 0,
                segments_unreadable: 0,
                segments_legacy_format: 0,
                manifest_record_count: segments.iter().map(|s| s.record_count).sum(),
                records_scanned: 0,
                records_with_offset_header: 0,
                records_with_timestamp_header: 0,
                records_with_source_cluster_header: 0,
                records_with_required_headers: 0,
                problems: Vec::new(),
            };

            if segments.is_empty() {
                cov.state = PartitionCoverageState::Empty;
                partitions.push(cov);
                continue;
            }

            if !scan {
                cov.state = PartitionCoverageState::Indeterminate;
                cov.problems.push(
                    "header coverage not evaluated (preflight scan disabled for this mode)"
                        .to_string(),
                );
                partitions.push(cov);
                continue;
            }

            for segment in &segments {
                if is_legacy_segment_key(&segment.key) {
                    cov.segments_legacy_format += 1;
                }
                match super::helpers::read_segment(storage, segment).await {
                    Ok(records) => {
                        cov.segments_scanned += 1;
                        let records = super::helpers::filter_records_by_time(records, options);
                        for record in &records {
                            inspect_record(record, &mut cov);
                        }
                        cov.records_scanned += records.len() as u64;
                    }
                    Err(Error::Storage(StorageError::NotFound(_))) => {
                        cov.segments_missing += 1;
                        push_example(
                            &mut cov.problems,
                            format!("segment object missing from storage: {}", segment.key),
                        );
                    }
                    Err(Error::Storage(e)) => {
                        cov.segments_unreadable += 1;
                        push_example(
                            &mut cov.problems,
                            format!("segment {} unreadable (storage error): {e}", segment.key),
                        );
                    }
                    Err(e) => {
                        cov.segments_corrupt += 1;
                        push_example(
                            &mut cov.problems,
                            format!("segment {} corrupt: {e}", segment.key),
                        );
                    }
                }
            }

            cov.state = classify_partition(&cov);
            if cov.state == PartitionCoverageState::Partial {
                push_example(
                    &mut cov.problems,
                    format!(
                        "{} of {} scanned records lack required tracking headers",
                        cov.records_scanned - cov.records_with_required_headers,
                        cov.records_scanned
                    ),
                );
            } else if cov.state == PartitionCoverageState::Missing {
                push_example(
                    &mut cov.problems,
                    "no scanned record carries x-original-offset/x-original-timestamp \
                     tracking headers (legacy or non-Phase-1 backup)"
                        .to_string(),
                );
            }
            records_total += cov.records_scanned;
            partitions.push(cov);
        }
    }

    partitions.sort_by(|a, b| {
        a.topic
            .cmp(&b.topic)
            .then_with(|| a.partition.cmp(&b.partition))
    });

    // Consumer-groups snapshot check (required tracking metadata for
    // auto_consumer_groups).
    let snapshot = if options.auto_consumer_groups {
        Some(check_consumer_group_snapshot(storage, &manifest.backup_id).await)
    } else {
        None
    };

    let mut report = HeaderPreflightReport {
        backup_id: manifest.backup_id.clone(),
        generated_at: chrono::Utc::now().to_rfc3339(),
        mode: mode.to_string(),
        offset_recovery_requested: recovery,
        scan_performed: scan,
        partitions,
        consumer_group_snapshot: snapshot,
        records_scanned_total: records_total,
        passed: false,
        errors: Vec::new(),
        warnings: Vec::new(),
    };

    evaluate(&mut report, options, mode);

    if report.passed {
        info!("Header preflight passed: {}", report.summary());
    } else {
        warn!("Header preflight failed: {}", report.summary());
    }

    report
}

/// Load and validate `{backup_id}/consumer-groups-snapshot.json`.
async fn check_consumer_group_snapshot(
    storage: &dyn StorageBackend,
    backup_id: &str,
) -> SnapshotCheck {
    let key = format!("{backup_id}/consumer-groups-snapshot.json");
    match storage.get(&key).await {
        Ok(data) => match super::engine::parse_auto_consumer_group_snapshot(&data) {
            Ok(snapshot) => SnapshotCheck {
                state: "present".to_string(),
                groups: snapshot.group_count(),
                offsets: snapshot.offset_count(),
                detail: key,
            },
            Err(e) => SnapshotCheck {
                state: "invalid".to_string(),
                groups: 0,
                offsets: 0,
                detail: format!("{key}: {e}"),
            },
        },
        Err(e) => SnapshotCheck {
            state: "missing".to_string(),
            groups: 0,
            offsets: 0,
            detail: format!("{key}: {e}"),
        },
    }
}

/// Derive the final verdict and populate errors/warnings.
fn evaluate(
    report: &mut HeaderPreflightReport,
    options: &RestoreOptions,
    mode: HeaderPreflightMode,
) {
    let recovery = report.offset_recovery_requested;

    // Misconfiguration: an explicit reset request that can never act.
    if options.reset_consumer_offsets
        && options.consumer_groups.is_empty()
        && !options.auto_consumer_groups
    {
        report.errors.push(
            "reset_consumer_offsets is enabled but no consumer_groups are configured and \
             auto_consumer_groups is disabled; the offset reset would silently do nothing. \
             Configure restore.consumer_groups, enable restore.auto_consumer_groups, or \
             disable restore.reset_consumer_offsets."
                .to_string(),
        );
    }

    // Snapshot requirements for auto_consumer_groups.
    if let Some(snapshot) = &report.consumer_group_snapshot {
        match snapshot.state.as_str() {
            "present" => {
                if snapshot.groups == 0 {
                    report.warnings.push(
                        "consumer-groups snapshot is present but lists no groups; Phase 3 \
                         offset reset will have nothing to apply"
                            .to_string(),
                    );
                }
            }
            "missing" => report.errors.push(format!(
                "auto_consumer_groups requires the consumer-groups snapshot but it is absent \
                 ({}). Re-run the backup with consumer-group snapshotting, run the \
                 snapshot-groups command, or disable auto_consumer_groups.",
                snapshot.detail
            )),
            _ => report.errors.push(format!(
                "auto_consumer_groups requires the consumer-groups snapshot but it cannot be \
                 parsed ({}).",
                snapshot.detail
            )),
        }
    }

    if mode == HeaderPreflightMode::Skip {
        // Skip mode never blocks: the operator explicitly accepted the risk,
        // and behaviour degrades exactly as it did before the preflight
        // existed. Everything found so far is demoted to a warning.
        if recovery {
            report.warnings.push(
                "header preflight explicitly skipped (header_preflight: skip) while consumer-\
                 offset recovery is requested; tracking-metadata coverage is UNVERIFIED"
                    .to_string(),
            );
        }
        let demoted = std::mem::take(&mut report.errors);
        report
            .warnings
            .extend(demoted.into_iter().map(|e| format!("(skipped) {e}")));
        report.passed = true;
        return;
    }

    let mut full = 0usize;
    let mut empty = 0usize;
    for p in &report.partitions {
        match p.state {
            PartitionCoverageState::Full => full += 1,
            PartitionCoverageState::Empty => empty += 1,
            PartitionCoverageState::Partial
            | PartitionCoverageState::Missing
            | PartitionCoverageState::DataMissing
            | PartitionCoverageState::Corrupt
            | PartitionCoverageState::Indeterminate => {}
        }
    }

    for p in &report.partitions {
        let label = format!("{}/{}", p.topic, p.partition);
        let detail = if p.problems.is_empty() {
            String::new()
        } else {
            format!(" ({})", p.problems.join("; "))
        };
        match p.state {
            PartitionCoverageState::Full | PartitionCoverageState::Empty => {}
            PartitionCoverageState::Partial => {
                let msg = format!(
                    "{label}: partial tracking-header coverage — {}/{} records covered{detail}",
                    p.records_with_required_headers, p.records_scanned
                );
                if recovery {
                    report.errors.push(msg);
                } else {
                    report.warnings.push(msg);
                }
            }
            PartitionCoverageState::Missing => {
                let msg = format!(
                    "{label}: backup records carry no offset/timestamp tracking headers{detail}"
                );
                if recovery {
                    report.errors.push(format!(
                        "{msg}. Consumer-offset recovery requires a Phase 1 backup taken with \
                         include_offset_headers: true."
                    ));
                } else {
                    report.warnings.push(format!(
                        "{msg}. Offset recovery from this backup will not be possible."
                    ));
                }
            }
            PartitionCoverageState::DataMissing => {
                // Missing backup data blocks any restore of this partition;
                // always an error.
                report.errors.push(format!(
                    "{label}: {} segment object(s) referenced by the manifest are missing from \
                     storage{detail}",
                    p.segments_missing
                ));
            }
            PartitionCoverageState::Corrupt => {
                report.errors.push(format!(
                    "{label}: {} segment(s) are corrupt or undecodable{detail}",
                    p.segments_corrupt
                ));
            }
            PartitionCoverageState::Indeterminate => {
                let msg =
                    format!("{label}: tracking-header coverage could not be determined{detail}");
                if recovery {
                    report.errors.push(msg);
                } else {
                    report.warnings.push(msg);
                }
            }
        }
    }

    if recovery && report.scan_performed && full == 0 {
        if empty == report.partitions.len() {
            report.errors.push(
                "consumer-offset recovery was requested but the selected backup contains no \
                 records; there are no offsets to map (zero records is not a pass)"
                    .to_string(),
            );
        } else if report
            .partitions
            .iter()
            .all(|p| matches!(p.state, PartitionCoverageState::Empty))
        {
            // covered above
        } else if report.errors.is_empty() {
            // Defensive: recovery requested, nothing fully covered, but no
            // specific error was recorded. Never allow a silent pass.
            report.errors.push(
                "consumer-offset recovery was requested but no partition demonstrated full \
                 tracking-header coverage"
                    .to_string(),
            );
        }
    }

    if report.partitions.is_empty() {
        let msg =
            "no topics/partitions matched the restore selection; nothing was validated".to_string();
        if recovery {
            report.errors.push(msg);
        } else {
            report.warnings.push(msg);
        }
    }

    report.passed = report.errors.is_empty();
}

fn classify_partition(cov: &PartitionHeaderCoverage) -> PartitionCoverageState {
    if cov.segments_corrupt > 0 {
        return PartitionCoverageState::Corrupt;
    }
    if cov.segments_missing > 0 {
        return PartitionCoverageState::DataMissing;
    }
    if cov.segments_unreadable > 0 {
        return PartitionCoverageState::Indeterminate;
    }
    if cov.records_scanned == 0 {
        // Segments existed and were decoded but held no records in the
        // selected window.
        return PartitionCoverageState::Empty;
    }
    if cov.records_with_required_headers == cov.records_scanned {
        PartitionCoverageState::Full
    } else if cov.records_with_required_headers == 0 {
        PartitionCoverageState::Missing
    } else {
        PartitionCoverageState::Partial
    }
}

fn inspect_record(record: &BackupRecord, cov: &mut PartitionHeaderCoverage) {
    let mut offset = false;
    let mut timestamp = false;
    for header in &record.headers {
        match header.key.as_str() {
            OFFSET_HEADER => offset = offset || decodes_as_i64(&header.value),
            TIMESTAMP_HEADER => timestamp = timestamp || decodes_as_i64(&header.value),
            SOURCE_CLUSTER_HEADER => cov.records_with_source_cluster_header += 1,
            _ => {}
        }
    }
    if offset {
        cov.records_with_offset_header += 1;
    }
    if timestamp {
        cov.records_with_timestamp_header += 1;
    }
    if offset && timestamp {
        cov.records_with_required_headers += 1;
    }
}

/// Accept the same encodings as the restore engine's header extraction:
/// 8-byte little-endian i64 (preferred) or a UTF-8 decimal string.
fn decodes_as_i64(value: &[u8]) -> bool {
    if value.len() == 8 {
        return true;
    }
    std::str::from_utf8(value)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .is_some()
}

/// Legacy JSON segments are identified by extension; binary segments use
/// `.bin`. Mirrors the format detection in `helpers::read_segment`.
fn is_legacy_segment_key(key: &str) -> bool {
    let ext = key.rsplit('.').next().unwrap_or("");
    matches!(ext, "json" | "zst" | "lz4" | "gz")
}

fn push_example(problems: &mut Vec<String>, msg: String) {
    if problems.len() < MAX_EXAMPLES {
        problems.push(msg);
    } else if problems.len() == MAX_EXAMPLES {
        problems.push("(further problems truncated)".to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coverage(
        scanned: u64,
        required: u64,
        corrupt: usize,
        missing: usize,
    ) -> PartitionHeaderCoverage {
        PartitionHeaderCoverage {
            topic: "t".to_string(),
            partition: 0,
            state: PartitionCoverageState::Indeterminate,
            segments_selected: 1,
            segments_scanned: 1,
            segments_missing: missing,
            segments_corrupt: corrupt,
            segments_unreadable: 0,
            segments_legacy_format: 0,
            manifest_record_count: scanned as i64,
            records_scanned: scanned,
            records_with_offset_header: required,
            records_with_timestamp_header: required,
            records_with_source_cluster_header: 0,
            records_with_required_headers: required,
            problems: Vec::new(),
        }
    }

    #[test]
    fn classify_full_partial_missing_empty() {
        assert_eq!(
            classify_partition(&coverage(10, 10, 0, 0)),
            PartitionCoverageState::Full
        );
        assert_eq!(
            classify_partition(&coverage(10, 4, 0, 0)),
            PartitionCoverageState::Partial
        );
        assert_eq!(
            classify_partition(&coverage(10, 0, 0, 0)),
            PartitionCoverageState::Missing
        );
        assert_eq!(
            classify_partition(&coverage(0, 0, 0, 0)),
            PartitionCoverageState::Empty
        );
    }

    #[test]
    fn classify_corrupt_dominates_missing_data() {
        assert_eq!(
            classify_partition(&coverage(10, 10, 1, 1)),
            PartitionCoverageState::Corrupt
        );
        assert_eq!(
            classify_partition(&coverage(10, 10, 0, 1)),
            PartitionCoverageState::DataMissing
        );
    }

    #[test]
    fn i64_header_decoding() {
        assert!(decodes_as_i64(&42i64.to_le_bytes()));
        assert!(decodes_as_i64(b"12345"));
        assert!(!decodes_as_i64(b"not-a-number"));
        assert!(!decodes_as_i64(&[1, 2, 3]));
    }
}
