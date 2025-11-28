use anyhow::Result;
use kafka_backup_core::{Config, RestoreEngine};
use tracing::info;

pub async fn run(config_path: &str, format: &str) -> Result<()> {
    // Load configuration
    let config_content = std::fs::read_to_string(config_path)?;
    let mut config: Config = serde_yaml::from_str(&config_content)?;

    // Force dry-run mode
    if let Some(ref mut restore) = config.restore {
        restore.dry_run = true;
    } else {
        config.restore = Some(kafka_backup_core::config::RestoreOptions {
            dry_run: true,
            ..Default::default()
        });
    }

    info!("Validating restore configuration from: {}", config_path);

    // Create engine and run dry-run
    let engine = RestoreEngine::new(config)?;
    let report = engine.dry_run().await?;

    // Output based on format
    match format.to_lowercase().as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        "yaml" => {
            println!("{}", serde_yaml::to_string(&report)?);
        }
        _ => {
            print_validation_report(&report);
        }
    }

    // Exit with error code if validation failed
    if !report.valid || !report.errors.is_empty() {
        std::process::exit(1);
    }

    Ok(())
}

fn print_validation_report(report: &kafka_backup_core::manifest::DryRunReport) {
    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║                    RESTORE VALIDATION REPORT                         ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");

    let status = if report.valid && report.errors.is_empty() {
        "✓ VALID"
    } else {
        "✗ INVALID"
    };
    println!("║ Status:         {:55} ║", status);
    println!("║ Backup ID:      {:55} ║", report.backup_id);

    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║                         RESTORE SUMMARY                              ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!(
        "║ Topics to restore:    {:48} ║",
        report.topics_to_restore.len()
    );
    println!(
        "║ Segments to process:  {:48} ║",
        report.segments_to_process
    );
    println!(
        "║ Records to restore:   {:48} ║",
        report.records_to_restore
    );
    println!(
        "║ Bytes to restore:     {:48} ║",
        format_bytes(report.bytes_to_restore)
    );

    if let Some((start, end)) = report.time_range {
        let start_str = chrono::DateTime::from_timestamp_millis(start)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        let end_str = chrono::DateTime::from_timestamp_millis(end)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        println!("╠══════════════════════════════════════════════════════════════════════╣");
        println!("║                           TIME RANGE                                 ║");
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        println!("║ From: {:65} ║", start_str);
        println!("║ To:   {:65} ║", end_str);
    }

    // Topics
    if !report.topics_to_restore.is_empty() {
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        println!("║                        TOPICS TO RESTORE                             ║");
        println!("╠══════════════════════════════════════════════════════════════════════╣");

        for topic in &report.topics_to_restore {
            println!("║                                                                      ║");
            println!("║ {} -> {} ║", topic.source_topic, topic.target_topic);

            for partition in &topic.partitions {
                let offset_str = format!(
                    "offsets {}-{}",
                    partition.offset_range.0, partition.offset_range.1
                );
                println!(
                    "║   P{} -> P{}: {} records, {} ({} segments) ║",
                    partition.source_partition,
                    partition.target_partition,
                    partition.records,
                    offset_str,
                    partition.segments
                );
            }
        }
    }

    // Consumer offset actions
    if !report.consumer_offset_actions.is_empty() {
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        println!("║                    CONSUMER OFFSET ACTIONS                           ║");
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        for action in &report.consumer_offset_actions {
            println!("║ • {:68} ║", action);
        }
    }

    // Errors
    if !report.errors.is_empty() {
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        println!("║                            ERRORS                                    ║");
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        for error in &report.errors {
            println!("║ ✗ {:68} ║", error);
        }
    }

    // Warnings
    if !report.warnings.is_empty() {
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        println!("║                           WARNINGS                                   ║");
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        for warning in &report.warnings {
            println!("║ ⚠ {:68} ║", warning);
        }
    }

    println!("╚══════════════════════════════════════════════════════════════════════╝");
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
