//! CLI command for three-phase restore operations.
//!
//! This command runs the complete three-phase restore:
//! - Phase 2: Restore data and collect offset mapping
//! - Phase 3: Generate and apply consumer group offset resets

use anyhow::Result;
use kafka_backup_core::{Config, ThreePhaseRestore};
use tracing::info;

pub async fn run(config_path: &str) -> Result<()> {
    info!("Loading configuration from: {}", config_path);

    let config_content = tokio::fs::read_to_string(config_path).await?;
    let config: Config = serde_yaml::from_str(&config_content)?;

    info!(
        "Starting three-phase restore from backup: {}",
        config.backup_id
    );

    let orchestrator = ThreePhaseRestore::new(config)?;
    let report = orchestrator.run_all_phases().await?;

    // Print summary
    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                       THREE-PHASE RESTORE REPORT                             ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Backup ID: {:67} ║", report.backup_id);
    println!(
        "║ Status: {:70} ║",
        if report.success { "✓ SUCCESS" } else { "✗ FAILED" }
    );
    println!(
        "║ Duration: {:68} ║",
        format!("{}ms", report.total_duration_ms)
    );
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");

    // Phase 2 summary
    println!("║ PHASE 2: DATA RESTORE                                                        ║");
    println!("╟──────────────────────────────────────────────────────────────────────────────╢");
    println!(
        "║   Records restored: {:58} ║",
        report.restore_report.records_restored
    );
    println!(
        "║   Topics restored: {:59} ║",
        report.restore_report.topics_restored.len()
    );
    println!(
        "║   Offset mappings: {:59} ║",
        report.restore_report.offset_mapping.detailed_mapping_count()
    );

    if !report.restore_report.errors.is_empty() {
        println!("║   Errors:                                                                    ║");
        for error in &report.restore_report.errors {
            println!("║     - {:72} ║", truncate_string(error, 72));
        }
    }

    // Phase 3 summary
    if let Some(ref reset_plan) = report.offset_reset_plan {
        println!("╟──────────────────────────────────────────────────────────────────────────────╢");
        println!("║ PHASE 3: OFFSET RESET                                                        ║");
        println!("╟──────────────────────────────────────────────────────────────────────────────╢");
        println!(
            "║   Strategy: {:66} ║",
            format!("{:?}", reset_plan.strategy)
        );
        println!(
            "║   Consumer groups: {:59} ║",
            reset_plan.groups.len()
        );

        let total_partitions: usize = reset_plan.groups.iter().map(|g| g.partitions.len()).sum();
        println!("║   Partitions planned: {:56} ║", total_partitions);

        if let Some(ref reset_report) = report.offset_reset_report {
            println!(
                "║   Partitions reset: {:58} ║",
                reset_report.partitions_reset
            );
            println!(
                "║   Reset status: {:62} ║",
                if reset_report.success {
                    "✓ Success"
                } else {
                    "✗ Failed"
                }
            );

            if !reset_report.errors.is_empty() {
                println!("║   Errors:                                                                    ║");
                for error in &reset_report.errors {
                    println!("║     - {:72} ║", truncate_string(error, 72));
                }
            }
        } else if reset_plan.dry_run {
            println!("║   Note: Dry run mode - offsets not actually reset                            ║");
        }
    } else {
        println!("╟──────────────────────────────────────────────────────────────────────────────╢");
        println!("║ PHASE 3: OFFSET RESET (skipped)                                              ║");
        println!("║   No consumer groups configured or reset_consumer_offsets=false              ║");
    }

    // Warnings
    if !report.warnings.is_empty() {
        println!("╟──────────────────────────────────────────────────────────────────────────────╢");
        println!("║ WARNINGS                                                                     ║");
        for warning in &report.warnings {
            println!("║   ⚠️  {:72} ║", truncate_string(warning, 72));
        }
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════╝");

    if report.success {
        info!("Three-phase restore completed successfully");
    } else {
        anyhow::bail!("Three-phase restore completed with errors");
    }

    Ok(())
}

fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}
