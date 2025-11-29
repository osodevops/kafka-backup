use anyhow::Result;
use kafka_backup_core::segment::SegmentReader;
use kafka_backup_core::storage::{FilesystemBackend, StorageBackend};
use kafka_backup_core::BackupManifest;
use std::path::PathBuf;
use tracing::{error, info, warn};

#[derive(Debug, Default)]
struct ValidationReport {
    segments_checked: usize,
    segments_valid: usize,
    segments_missing: usize,
    segments_corrupted: usize,
    records_validated: u64,
    issues: Vec<String>,
}

impl ValidationReport {
    fn is_valid(&self) -> bool {
        self.segments_missing == 0 && self.segments_corrupted == 0
    }

    fn print(&self) {
        println!("\n=== Validation Report ===\n");
        println!("Segments Checked:   {}", self.segments_checked);
        println!("Segments Valid:     {}", self.segments_valid);
        println!("Segments Missing:   {}", self.segments_missing);
        println!("Segments Corrupted: {}", self.segments_corrupted);
        println!("Records Validated:  {}", self.records_validated);

        if !self.issues.is_empty() {
            println!("\nIssues Found:");
            for issue in &self.issues {
                println!("  - {}", issue);
            }
        }

        println!();
        if self.is_valid() {
            println!("Result: VALID");
        } else {
            println!("Result: INVALID");
        }
    }
}

pub async fn run(path: &str, backup_id: &str, deep: bool) -> Result<()> {
    info!("Validating backup: {} (deep={})", backup_id, deep);

    let storage = FilesystemBackend::new(PathBuf::from(path));
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
            return Ok(());
        }
    };

    let manifest: BackupManifest = match serde_json::from_slice(&manifest_data) {
        Ok(m) => m,
        Err(e) => {
            error!("Failed to parse manifest: {}", e);
            report.issues.push(format!("Manifest parse error: {}", e));
            report.print();
            return Ok(());
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
