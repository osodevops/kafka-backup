use anyhow::Result;
use kafka_backup_core::segment::SegmentReader;
use kafka_backup_core::BackupManifest;
use tracing::{error, info, warn};

use super::storage_path::resolve_target;

#[derive(Debug, Default)]
struct ValidationReport {
    manifest_loaded: bool,
    segments_checked: usize,
    segments_valid: usize,
    segments_missing: usize,
    segments_corrupted: usize,
    records_validated: u64,
    issues: Vec<String>,
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
}

impl ValidationReport {
    fn is_valid(&self) -> bool {
        self.manifest_loaded && self.segments_missing == 0 && self.segments_corrupted == 0
    }

    fn print(&self) {
        println!("\n=== Validation Report ===\n");
        println!("Segments Checked:   {}", self.segments_checked);
        println!("Segments Valid:     {}", self.segments_valid);
        println!("Segments Missing:   {}", self.segments_missing);
        println!("Segments Corrupted: {}", self.segments_corrupted);
        println!("Records Validated:  {}", self.records_validated);
        println!("Data Gaps:          {}", self.data_gaps.len());
        println!("Pruned Ranges:      {}", self.pruned_ranges.len());
        println!("Missing Topics:     {}", self.missing_topics.len());

        if !self.issues.is_empty() {
            println!("\nIssues Found:");
            for issue in &self.issues {
                println!("  - {}", issue);
            }
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
) -> Result<()> {
    let (storage, backup_id) = resolve_target(config, path, backup_id).await?;
    info!("Validating backup: {} (deep={})", backup_id, deep);
    let mut report = ValidationReport::default();

    // Load manifest
    let manifest_key = format!("{}/manifest.json", backup_id);
    let manifest_data = match storage.get(&manifest_key).await {
        Ok(data) => data,
        Err(e) => {
            error!("Failed to load manifest: {}", e);
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
            report.issues.push(format!("Manifest parse error: {}", e));
            report.print();
            std::process::exit(1);
        }
    };
    report.manifest_loaded = true;

    println!("Validating backup: {}", manifest.backup_id);
    println!(
        "Created: {}",
        chrono::DateTime::from_timestamp_millis(manifest.created_at)
            .map(|dt| dt.to_string())
            .unwrap_or_else(|| "Unknown".to_string())
    );
    println!();

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

    // Validate each segment
    for topic in &manifest.topics {
        println!("Checking topic: {}", topic.name);

        for partition in &topic.partitions {
            for segment in &partition.segments {
                report.segments_checked += 1;

                // Check if segment exists
                let exists = match storage.exists(&segment.key).await {
                    Ok(e) => e,
                    Err(e) => {
                        warn!("Error checking segment {}: {}", segment.key, e);
                        report.segments_missing += 1;
                        report
                            .issues
                            .push(format!("Error checking segment: {}", segment.key));
                        continue;
                    }
                };

                if !exists {
                    warn!("Missing segment: {}", segment.key);
                    report.segments_missing += 1;
                    report
                        .issues
                        .push(format!("Missing segment: {}", segment.key));
                    continue;
                }

                // Check segment size
                let size = match storage.size(&segment.key).await {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Error getting segment size {}: {}", segment.key, e);
                        0
                    }
                };

                if size != segment.compressed_size {
                    warn!(
                        "Size mismatch for {}: expected {}, got {}",
                        segment.key, segment.compressed_size, size
                    );
                    report.issues.push(format!(
                        "Size mismatch: {} (expected {}, got {})",
                        segment.key, segment.compressed_size, size
                    ));
                }

                // Deep validation - read and parse segment
                if deep {
                    match storage.get(&segment.key).await {
                        Ok(data) => {
                            match SegmentReader::open(data) {
                                Ok(reader) => {
                                    // Verify record count
                                    if reader.record_count() != segment.record_count as u64 {
                                        warn!(
                                            "Record count mismatch for {}: expected {}, got {}",
                                            segment.key,
                                            segment.record_count,
                                            reader.record_count()
                                        );
                                        report.issues.push(format!(
                                            "Record count mismatch: {} (expected {}, got {})",
                                            segment.key,
                                            segment.record_count,
                                            reader.record_count()
                                        ));
                                    }

                                    // Verify offsets
                                    if reader.start_offset() != segment.start_offset {
                                        warn!(
                                            "Start offset mismatch for {}: expected {}, got {}",
                                            segment.key,
                                            segment.start_offset,
                                            reader.start_offset()
                                        );
                                        report.issues.push(format!(
                                            "Start offset mismatch: {}",
                                            segment.key
                                        ));
                                    }

                                    if reader.end_offset() != segment.end_offset {
                                        warn!(
                                            "End offset mismatch for {}: expected {}, got {}",
                                            segment.key,
                                            segment.end_offset,
                                            reader.end_offset()
                                        );
                                        report
                                            .issues
                                            .push(format!("End offset mismatch: {}", segment.key));
                                    }

                                    report.records_validated += reader.record_count();
                                    report.segments_valid += 1;
                                }
                                Err(e) => {
                                    error!("Failed to parse segment {}: {}", segment.key, e);
                                    report.segments_corrupted += 1;
                                    report.issues.push(format!(
                                        "Corrupted segment: {} ({})",
                                        segment.key, e
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to read segment {}: {}", segment.key, e);
                            report.segments_corrupted += 1;
                            report
                                .issues
                                .push(format!("Failed to read segment: {} ({})", segment.key, e));
                        }
                    }
                } else {
                    // Shallow validation - just check existence
                    report.segments_valid += 1;
                    report.records_validated += segment.record_count as u64;
                }
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

    report.print();

    if !report.is_valid() {
        std::process::exit(1);
    }

    Ok(())
}
