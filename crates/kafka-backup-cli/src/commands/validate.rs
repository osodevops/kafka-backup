use anyhow::Result;
use kafka_backup_core::segment::SegmentReader;
use kafka_backup_core::storage::StorageBackend;
use kafka_backup_core::BackupManifest;
use tracing::{error, info, warn};

use super::storage_path::resolve_target;

/// How much of each segment `validate` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Depth {
    /// Existence and size only.
    Shallow,
    /// Read the bytes and verify the manifest's SHA-256 (no decoding).
    Sha256,
    /// SHA-256, then decode and check record count and offsets.
    Deep,
}

#[derive(Debug, Default)]
pub(crate) struct ValidationReport {
    manifest_loaded: bool,
    segments_checked: usize,
    pub(crate) segments_valid: usize,
    pub(crate) segments_missing: usize,
    pub(crate) segments_corrupted: usize,
    records_validated: u64,
    pub(crate) issues: Vec<String>,
    /// Segments with no recorded SHA-256 (written before 0.21), so their
    /// bytes could not be verified by `--deep` / `--verify-sha256`.
    pub(crate) checksums_unverified: usize,
    /// Offsets inside a partition's range that nothing accounts for — no
    /// segment's covered range, no recorded gap, no pruned range — or
    /// segments whose covered ranges overlap. Integrity failures.
    pub(crate) contiguity_errors: Vec<String>,
    /// The same, next to a segment written before 0.24 (which records only
    /// its first and last record, so a compaction hole looks like a hole).
    /// Reported, not failed.
    pub(crate) contiguity_warnings: Vec<String>,
    /// Offset ranges the backup itself recorded as missing (retention deleted
    /// them before they could be fetched — see `OffsetGap`). These are not
    /// integrity failures: the stored data is intact, but the backup is
    /// knowingly incomplete for these ranges.
    data_gaps: Vec<String>,
    offsets_missing: i64,
    /// Ranges deliberately deleted by retention (`prune` /
    /// `backup.retention`). Informational, never an integrity failure.
    pruned_ranges: Vec<String>,
    /// Literal include topics absent from the cluster at the last backup run
    /// and skipped (`backup.on_missing_topic: warn`). Not an integrity failure.
    missing_topics: Vec<String>,
    depth: Option<Depth>,
}

impl ValidationReport {
    pub(crate) fn is_valid(&self) -> bool {
        self.manifest_loaded
            && self.segments_missing == 0
            && self.segments_corrupted == 0
            && self.contiguity_errors.is_empty()
    }

    fn print(&self) {
        println!("\n=== Validation Report ===\n");
        println!("Segments Checked:   {}", self.segments_checked);
        println!("Segments Valid:     {}", self.segments_valid);
        println!("Segments Missing:   {}", self.segments_missing);
        println!("Segments Corrupted: {}", self.segments_corrupted);
        println!("Records Validated:  {}", self.records_validated);
        if matches!(self.depth, Some(Depth::Sha256 | Depth::Deep)) {
            println!(
                "Checksums Verified: {}",
                self.segments_checked
                    .saturating_sub(self.segments_missing + self.checksums_unverified)
            );
            println!(
                "Contiguity:         {} holes/overlaps, {} legacy warnings",
                self.contiguity_errors.len(),
                self.contiguity_warnings.len()
            );
        }
        println!("Data Gaps:          {}", self.data_gaps.len());
        println!("Pruned Ranges:      {}", self.pruned_ranges.len());
        println!("Missing Topics:     {}", self.missing_topics.len());

        if !self.issues.is_empty() {
            println!("\nIssues Found:");
            for issue in &self.issues {
                println!("  - {}", issue);
            }
        }

        if !self.contiguity_errors.is_empty() {
            println!(
                "\nContiguity errors (offsets no segment, recorded gap or pruned range accounts for):"
            );
            for e in &self.contiguity_errors {
                println!("  - {}", e);
            }
        }

        if !self.contiguity_warnings.is_empty() {
            println!(
                "\nContiguity warnings (next to segments written before 0.24, which record only \
                 their first and last record — compaction holes look the same; not failed):"
            );
            for w in &self.contiguity_warnings {
                println!("  - {}", w);
            }
        }

        if self.checksums_unverified > 0 {
            println!(
                "\n{} segment(s) have no recorded SHA-256 (written before 0.21); their bytes \
                 were not verified.",
                self.checksums_unverified
            );
        }

        if !self.data_gaps.is_empty() {
            println!(
                "\nData Gaps (recorded during backup — {} source offsets could not be captured \
                 because the broker no longer had them):",
                self.offsets_missing
            );
            for gap in &self.data_gaps {
                println!("  - {}", gap);
            }
        }

        if !self.pruned_ranges.is_empty() {
            println!("\nPruned Ranges (deliberately deleted by retention — not data loss):");
            for range in &self.pruned_ranges {
                println!("  - {}", range);
            }
        }

        if !self.missing_topics.is_empty() {
            println!(
                "\nMissing Topics (configured with backup.on_missing_topic: warn and absent during \
                 the backup — not an integrity failure):"
            );
            for topic in &self.missing_topics {
                println!("  - {}", topic);
            }
        }

        println!();
        match (self.is_valid(), self.data_gaps.is_empty()) {
            (true, true) => println!("Result: VALID"),
            (true, false) => println!(
                "Result: VALID (with {} recorded data gaps — backup is intact but incomplete)",
                self.data_gaps.len()
            ),
            (false, _) => println!("Result: INVALID"),
        }
    }
}

pub async fn run(
    config: Option<&str>,
    path: Option<&str>,
    backup_id: Option<&str>,
    deep: bool,
    verify_sha256: bool,
) -> Result<()> {
    let (storage, backup_id) = resolve_target(config, path, backup_id).await?;
    let depth = match (deep, verify_sha256) {
        (true, _) => Depth::Deep,
        (false, true) => Depth::Sha256,
        (false, false) => Depth::Shallow,
    };
    info!("Validating backup: {} ({:?})", backup_id, depth);

    // Load manifest
    let manifest_key = format!("{}/manifest.json", backup_id);
    let manifest_data = match storage.get(&manifest_key).await {
        Ok(data) => data,
        Err(e) => {
            error!("Failed to load manifest: {}", e);
            let mut report = ValidationReport::default();
            report
                .issues
                .push(format!("Manifest not found: {}", manifest_key));
            report.print();
            std::process::exit(1);
        }
    };

    let manifest: BackupManifest = match serde_json::from_slice(&manifest_data) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse manifest: {}", e);
            let mut report = ValidationReport::default();
            report.issues.push(format!("Manifest parse error: {}", e));
            report.print();
            std::process::exit(1);
        }
    };

    println!("Validating backup: {}", manifest.backup_id);
    println!(
        "Created: {}",
        chrono::DateTime::from_timestamp_millis(manifest.created_at)
            .map(|dt| dt.to_string())
            .unwrap_or_else(|| "Unknown".to_string())
    );
    println!();

    let report = validate_backup(storage.as_ref(), &manifest, depth).await;
    report.print();

    if !report.is_valid() {
        std::process::exit(1);
    }

    Ok(())
}

/// Validate every segment the manifest lists at `depth`, and (for
/// `Sha256` / `Deep`) the contiguity of each partition's offsets.
pub(crate) async fn validate_backup(
    storage: &dyn StorageBackend,
    manifest: &BackupManifest,
    depth: Depth,
) -> ValidationReport {
    let mut report = ValidationReport {
        manifest_loaded: true,
        depth: Some(depth),
        ..Default::default()
    };

    // Surface any data gaps the backup recorded about itself (issue #144).
    for (topic, partition, gap) in manifest.gaps() {
        let detected = chrono::DateTime::from_timestamp_millis(gap.detected_at)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| gap.detected_at.to_string());
        report.data_gaps.push(format!(
            "{}:{} offsets {}..{} ({} offsets, {}, detected {})",
            topic,
            partition,
            gap.start_offset,
            gap.end_offset,
            gap.offset_span(),
            gap.reason,
            detected
        ));
        report.offsets_missing += gap.offset_span();
    }

    // Ranges deliberately removed by retention (issue #169).
    for (topic, partition, range) in manifest.pruned() {
        let when = chrono::DateTime::from_timestamp_millis(range.pruned_at)
            .map(|dt| dt.to_rfc3339())
            .unwrap_or_else(|| range.pruned_at.to_string());
        report.pruned_ranges.push(format!(
            "{}:{} offsets {}..{} ({} segments, {} bytes, {}, pruned {})",
            topic,
            partition,
            range.start_offset,
            range.end_offset,
            range.segments,
            range.bytes,
            range.reason,
            when
        ));
    }

    // Literal include topics skipped under on_missing_topic: warn (issue #167).
    report.missing_topics = manifest.missing_topics.clone();

    for topic in &manifest.topics {
        println!("Checking topic: {}", topic.name);

        for partition in &topic.partitions {
            for segment in &partition.segments {
                check_segment(storage, segment, depth, &mut report).await;
            }
            if depth != Depth::Shallow {
                check_contiguity(&topic.name, partition, &mut report);
            }
        }

        println!(
            "  {} partitions, {} segments checked",
            topic.partitions.len(),
            topic
                .partitions
                .iter()
                .map(|p| p.segments.len())
                .sum::<usize>()
        );
    }

    report
}

fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(data)
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect()
}

/// Check one segment. Any mismatch with the manifest (size, checksum,
/// record count, offsets) counts it as corrupted.
async fn check_segment(
    storage: &dyn StorageBackend,
    segment: &kafka_backup_core::manifest::SegmentMetadata,
    depth: Depth,
    report: &mut ValidationReport,
) {
    report.segments_checked += 1;
    let key = &segment.key;

    match storage.exists(key).await {
        Ok(true) => {}
        Ok(false) => {
            warn!("Missing segment: {}", key);
            report.segments_missing += 1;
            report.issues.push(format!("Missing segment: {}", key));
            return;
        }
        Err(e) => {
            warn!("Error checking segment {}: {}", key, e);
            report.segments_missing += 1;
            report
                .issues
                .push(format!("Error checking segment: {} ({})", key, e));
            return;
        }
    }

    let mut problems: Vec<String> = Vec::new();

    // Size: pre-0.21 manifests may not record it (0).
    match storage.size(key).await {
        Ok(size) if segment.compressed_size > 0 && size != segment.compressed_size => {
            problems.push(format!(
                "Size mismatch: {} (expected {}, got {})",
                key, segment.compressed_size, size
            ));
        }
        Ok(_) => {}
        Err(e) => problems.push(format!("Error getting segment size: {} ({})", key, e)),
    }

    let mut records = segment.record_count.max(0) as u64;
    if depth != Depth::Shallow {
        match storage.get(key).await {
            Ok(data) => {
                if segment.sha256.is_empty() {
                    report.checksums_unverified += 1;
                } else {
                    let actual = sha256_hex(&data);
                    if !actual.eq_ignore_ascii_case(&segment.sha256) {
                        problems.push(format!(
                            "SHA-256 mismatch: {} (manifest {}, stored bytes {})",
                            key, segment.sha256, actual
                        ));
                    }
                }

                if depth == Depth::Deep {
                    match SegmentReader::open(data) {
                        Ok(reader) => {
                            if reader.record_count() != segment.record_count as u64 {
                                problems.push(format!(
                                    "Record count mismatch: {} (expected {}, got {})",
                                    key,
                                    segment.record_count,
                                    reader.record_count()
                                ));
                            }
                            if reader.start_offset() != segment.start_offset {
                                problems.push(format!(
                                    "Start offset mismatch: {} (expected {}, got {})",
                                    key,
                                    segment.start_offset,
                                    reader.start_offset()
                                ));
                            }
                            if reader.end_offset() != segment.end_offset {
                                problems.push(format!(
                                    "End offset mismatch: {} (expected {}, got {})",
                                    key,
                                    segment.end_offset,
                                    reader.end_offset()
                                ));
                            }
                            records = reader.record_count();
                        }
                        Err(e) => {
                            problems.push(format!("Corrupted segment: {} ({})", key, e));
                        }
                    }
                }
            }
            Err(e) => problems.push(format!("Failed to read segment: {} ({})", key, e)),
        }
    }

    if problems.is_empty() {
        report.segments_valid += 1;
        report.records_validated += records;
    } else {
        for problem in &problems {
            error!("{}", problem);
        }
        report.segments_corrupted += 1;
        report.issues.extend(problems);
    }
}

/// One accounted-for offset range in a partition, inclusive.
struct Interval {
    start: i64,
    end: i64,
    what: String,
    /// A segment written before 0.24: only its first/last record are known.
    legacy: bool,
}

/// Every offset between the partition's first and last accounted offset
/// must be inside a segment's covered range, a recorded gap or a pruned
/// range, and segments must not overlap. Holes or overlaps touching a
/// pre-0.24 segment only warn.
fn check_contiguity(
    topic: &str,
    partition: &kafka_backup_core::manifest::PartitionBackup,
    report: &mut ValidationReport,
) {
    let mut intervals: Vec<Interval> = partition
        .segments
        .iter()
        .map(|s| Interval {
            start: s.covered_start(),
            end: s.covered_end(),
            what: format!("segment {}", s.key),
            legacy: !s.has_covered_range(),
        })
        .chain(partition.gaps.iter().map(|g| Interval {
            start: g.start_offset,
            end: g.end_offset - 1,
            what: format!("gap {}..{}", g.start_offset, g.end_offset),
            legacy: false,
        }))
        .chain(partition.pruned.iter().map(|p| Interval {
            start: p.start_offset,
            end: p.end_offset,
            what: format!("pruned range {}..={}", p.start_offset, p.end_offset),
            legacy: false,
        }))
        .filter(|i| i.end >= i.start)
        .collect();
    intervals.sort_by_key(|i| (i.start, i.end));

    let mut iter = intervals.into_iter();
    let Some(mut prev) = iter.next() else {
        return;
    };
    let where_ = format!("{}:{}", topic, partition.partition_id);
    for next in iter {
        let tolerated = prev.legacy || next.legacy;
        if next.start > prev.end + 1 {
            let msg = format!(
                "{} offsets {}..={} are not covered (between {} and {})",
                where_,
                prev.end + 1,
                next.start - 1,
                prev.what,
                next.what
            );
            if tolerated {
                report.contiguity_warnings.push(msg);
            } else {
                report.contiguity_errors.push(msg);
            }
        } else if next.start <= prev.end {
            let msg = format!(
                "{} {} and {} overlap at offsets {}..={}",
                where_,
                prev.what,
                next.what,
                next.start,
                prev.end.min(next.end)
            );
            if tolerated {
                report.contiguity_warnings.push(msg);
            } else {
                report.contiguity_errors.push(msg);
            }
        }
        if next.end >= prev.end {
            prev = next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use kafka_backup_core::manifest::{
        OffsetGap, OffsetGapReason, PartitionBackup, PruneReason, PrunedRange, SegmentMetadata,
    };
    use kafka_backup_core::segment::{BinaryRecord, SegmentWriter, SegmentWriterConfig};
    use kafka_backup_core::storage::{MemoryBackend, StorageBackend};
    use kafka_backup_core::PerformanceMetrics;
    use std::sync::Arc;

    /// Write a real segment holding `offsets`, covering `covered`.
    async fn write_segment(
        storage: &Arc<MemoryBackend>,
        offsets: std::ops::RangeInclusive<i64>,
        covered: Option<(i64, i64)>,
    ) -> SegmentMetadata {
        let mut writer = SegmentWriter::new(
            SegmentWriterConfig::default(),
            storage.clone(),
            Arc::new(PerformanceMetrics::new()),
        );
        if let Some((start, _)) = covered {
            writer.begin_coverage(start, start - 1);
        }
        let first = *offsets.start();
        for offset in offsets {
            writer
                .add_record(BinaryRecord {
                    timestamp: 1_000 + offset,
                    offset,
                    key: None,
                    value: Some(Bytes::from(format!("v{offset}"))),
                    headers: vec![],
                })
                .unwrap();
        }
        if let Some((_, end)) = covered {
            writer.extend_coverage(end);
        }
        writer
            .flush(&format!(
                "b/topics/t/partition=0/segment-{first:020}.bin.zst"
            ))
            .await
            .unwrap()
            .unwrap()
    }

    fn legacy(mut segment: SegmentMetadata) -> SegmentMetadata {
        segment.covered_start_offset = None;
        segment.covered_end_offset = None;
        segment.sha256 = String::new();
        segment
    }

    fn manifest_with(segments: Vec<SegmentMetadata>) -> BackupManifest {
        let mut manifest = BackupManifest::new("b".to_string());
        let partition = manifest.get_or_create_topic("t").get_or_create_partition(0);
        partition.segments = segments;
        manifest
    }

    fn partition(manifest: &mut BackupManifest) -> &mut PartitionBackup {
        manifest.get_or_create_topic("t").get_or_create_partition(0)
    }

    #[tokio::test]
    async fn a_clean_backup_is_valid_at_every_depth() {
        let storage = Arc::new(MemoryBackend::new());
        let manifest = manifest_with(vec![
            write_segment(&storage, 0..=9, Some((0, 12))).await,
            write_segment(&storage, 13..=20, Some((13, 20))).await,
        ]);
        for depth in [Depth::Shallow, Depth::Sha256, Depth::Deep] {
            let report = validate_backup(storage.as_ref(), &manifest, depth).await;
            assert!(report.is_valid(), "{depth:?}: {:?}", report.issues);
            assert_eq!(report.segments_valid, 2);
            assert!(report.contiguity_errors.is_empty());
        }
    }

    #[tokio::test]
    async fn sha256_mismatch_is_corruption() {
        let storage = Arc::new(MemoryBackend::new());
        let mut segment = write_segment(&storage, 0..=9, None).await;
        // Same size, different bytes: only the checksum can tell.
        segment.sha256 = "0".repeat(64);
        let manifest = manifest_with(vec![segment]);

        for depth in [Depth::Sha256, Depth::Deep] {
            let report = validate_backup(storage.as_ref(), &manifest, depth).await;
            assert_eq!(report.segments_corrupted, 1, "{depth:?}");
            assert!(!report.is_valid());
            assert!(
                report.issues.iter().any(|i| i.contains("SHA-256 mismatch")),
                "{:?}",
                report.issues
            );
        }
        // Shallow validation does not read segment bytes.
        let report = validate_backup(storage.as_ref(), &manifest, Depth::Shallow).await;
        assert!(report.is_valid());
    }

    #[tokio::test]
    async fn count_and_offset_mismatches_are_corruption() {
        let storage = Arc::new(MemoryBackend::new());
        let mut segment = write_segment(&storage, 0..=9, None).await;
        segment.record_count = 11;
        segment.end_offset = 10;
        let report =
            validate_backup(storage.as_ref(), &manifest_with(vec![segment]), Depth::Deep).await;
        assert_eq!(report.segments_corrupted, 1);
        assert!(!report.is_valid());
        assert!(report
            .issues
            .iter()
            .any(|i| i.contains("Record count mismatch")));
        assert!(report
            .issues
            .iter()
            .any(|i| i.contains("End offset mismatch")));
    }

    #[tokio::test]
    async fn size_mismatch_is_corruption_at_every_depth() {
        let storage = Arc::new(MemoryBackend::new());
        let mut segment = write_segment(&storage, 0..=9, None).await;
        segment.compressed_size += 1;
        let manifest = manifest_with(vec![segment]);
        for depth in [Depth::Shallow, Depth::Sha256, Depth::Deep] {
            let report = validate_backup(storage.as_ref(), &manifest, depth).await;
            assert_eq!(report.segments_corrupted, 1, "{depth:?}");
            assert!(!report.is_valid());
        }
    }

    #[tokio::test]
    async fn segments_without_a_recorded_checksum_or_size_still_validate() {
        let storage = Arc::new(MemoryBackend::new());
        let mut segment = legacy(write_segment(&storage, 0..=9, None).await);
        segment.compressed_size = 0; // pre-0.21 manifests may not carry it
        let report = validate_backup(
            storage.as_ref(),
            &manifest_with(vec![segment]),
            Depth::Sha256,
        )
        .await;
        assert!(report.is_valid(), "{:?}", report.issues);
        assert_eq!(report.checksums_unverified, 1);
    }

    #[tokio::test]
    async fn an_uncovered_hole_between_new_segments_is_an_error() {
        let storage = Arc::new(MemoryBackend::new());
        let manifest = manifest_with(vec![
            write_segment(&storage, 0..=9, Some((0, 9))).await,
            write_segment(&storage, 20..=29, Some((20, 29))).await,
        ]);
        let report = validate_backup(storage.as_ref(), &manifest, Depth::Deep).await;
        assert!(!report.is_valid());
        assert_eq!(
            report.contiguity_errors.len(),
            1,
            "{:?}",
            report.contiguity_errors
        );
        assert!(report.contiguity_errors[0].contains("10..=19"));
    }

    #[tokio::test]
    async fn recorded_gaps_and_pruned_ranges_fill_holes() {
        let storage = Arc::new(MemoryBackend::new());
        let mut manifest = manifest_with(vec![
            write_segment(&storage, 20..=29, Some((20, 29))).await,
            write_segment(&storage, 50..=59, Some((50, 59))).await,
        ]);
        partition(&mut manifest).add_pruned(PrunedRange {
            start_offset: 0,
            end_offset: 19,
            segments: 2,
            bytes: 10,
            pruned_at: 1,
            cutoff_timestamp: 1,
            reason: PruneReason::Retention,
        });
        partition(&mut manifest).add_gap(OffsetGap {
            start_offset: 30,
            end_offset: 50,
            reason: OffsetGapReason::OffsetOutOfRange,
            detected_at: 1,
        });
        let report = validate_backup(storage.as_ref(), &manifest, Depth::Deep).await;
        assert!(report.is_valid(), "{:?}", report.contiguity_errors);
        assert!(report.contiguity_warnings.is_empty());
    }

    #[tokio::test]
    async fn holes_next_to_legacy_segments_only_warn() {
        let storage = Arc::new(MemoryBackend::new());
        // Legacy segments only record their first/last record, so a
        // compaction hole between them is indistinguishable from loss.
        let manifest = manifest_with(vec![
            legacy(write_segment(&storage, 0..=9, None).await),
            legacy(write_segment(&storage, 15..=20, None).await),
        ]);
        let report = validate_backup(storage.as_ref(), &manifest, Depth::Deep).await;
        assert!(report.is_valid(), "{:?}", report.contiguity_errors);
        assert_eq!(report.contiguity_warnings.len(), 1);
    }

    #[tokio::test]
    async fn overlapping_new_segments_are_an_error_and_legacy_ones_warn() {
        let storage = Arc::new(MemoryBackend::new());
        let a = write_segment(&storage, 0..=9, Some((0, 9))).await;
        let b = write_segment(&storage, 5..=14, Some((5, 14))).await;
        let report = validate_backup(
            storage.as_ref(),
            &manifest_with(vec![a.clone(), b.clone()]),
            Depth::Deep,
        )
        .await;
        assert!(!report.is_valid());
        assert!(
            report.contiguity_errors[0].contains("overlap"),
            "{:?}",
            report.contiguity_errors
        );

        let report = validate_backup(
            storage.as_ref(),
            &manifest_with(vec![legacy(a), legacy(b)]),
            Depth::Deep,
        )
        .await;
        assert!(report.is_valid(), "{:?}", report.contiguity_errors);
        assert_eq!(report.contiguity_warnings.len(), 1);
    }

    #[tokio::test]
    async fn missing_segment_is_reported() {
        let storage = Arc::new(MemoryBackend::new());
        let segment = write_segment(&storage, 0..=9, None).await;
        storage.delete(&segment.key).await.unwrap();
        let report = validate_backup(
            storage.as_ref(),
            &manifest_with(vec![segment]),
            Depth::Shallow,
        )
        .await;
        assert_eq!(report.segments_missing, 1);
        assert!(!report.is_valid());
    }
}
