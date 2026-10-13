//! CLI command for three-phase restore operations.
//!
//! This command runs the complete three-phase restore:
//! - Phase 2: Restore data and collect offset mapping
//! - Phase 3: Generate and apply consumer group offset resets

use std::sync::Arc;

use anyhow::Result;
use kafka_backup_core::{Config, PrometheusMetrics, ThreePhaseRestore};
use tokio::sync::broadcast;
use tracing::info;

pub async fn run(config_path: &str) -> Result<()> {
    info!("Loading configuration from: {}", config_path);

    let config_content = tokio::fs::read_to_string(config_path).await?;
    let config_content = super::config::expand_env_vars(&config_content);
    let mut config: Config = super::config::parse_config(&config_content)?;
    super::sasl_plugin::populate_sasl_plugin_opt(&mut config.source)?;
    super::sasl_plugin::populate_sasl_plugin_opt(&mut config.target)?;

    info!(
        "Starting three-phase restore from backup: {}",
        config.backup_id
    );

    // Phase 2 records the restore metrics, served like `restore` does.
    let metrics_config = config.metrics.clone().unwrap_or_default();
    let prometheus_metrics = Arc::new(PrometheusMetrics::with_max_labels(
        metrics_config.max_partition_labels,
    ));
    let metrics_server = super::metrics_runtime::RunningMetricsServer::start(
        &metrics_config,
        prometheus_metrics.clone(),
    );

    let orchestrator = ThreePhaseRestore::new(config)?.with_prometheus_metrics(prometheus_metrics);
    let result = orchestrator.run_all_phases().await;

    if let Some(metrics_server) = metrics_server {
        // The orchestrator has no shutdown handle: a signal only ends the
        // metrics keep-alive.
        let (operation_shutdown_tx, _) = broadcast::channel::<()>(1);
        let (lifecycle_shutdown_tx, mut lifecycle_shutdown_rx) = broadcast::channel::<()>(1);
        let signal_task = super::metrics_runtime::spawn_shutdown_signal_forwarder(
            operation_shutdown_tx,
            lifecycle_shutdown_tx,
        );
        metrics_server
            .shutdown_after_operation(&mut lifecycle_shutdown_rx)
            .await;
        signal_task.abort();
    }

    let report = result?;

    // Print summary
    println!();
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                       THREE-PHASE RESTORE REPORT                             ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Backup ID: {:67} ║", report.backup_id);
    println!(
        "║ Status: {:70} ║",
        if report.success {
            "✓ SUCCESS"
        } else {
            "✗ FAILED"
        }
    );
    println!(
        "║ Duration: {:68} ║",
        format!("{}ms", report.total_duration_ms)
    );
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");

    // Phase 1 preflight summary
    if let Some(ref preflight) = report.header_preflight {
        println!(
            "║ PHASE 1: HEADER PREFLIGHT                                                    ║"
        );
        println!(
            "╟──────────────────────────────────────────────────────────────────────────────╢"
        );
        println!(
            "║   Verdict: {:67} ║",
            if preflight.passed {
                "✓ PASSED"
            } else {
                "✗ FAILED"
            }
        );
        println!(
            "║   Mode: {:70} ║",
            format!(
                "{} (offset recovery requested: {}, scan performed: {})",
                preflight.mode, preflight.offset_recovery_requested, preflight.scan_performed
            )
        );
        println!(
            "║   Records scanned: {:59} ║",
            preflight.records_scanned_total
        );
        for p in &preflight.partitions {
            println!(
                "║   {:76} ║",
                truncate_string(
                    &format!(
                        "{}/{}: {} ({}/{} records covered, {} segments)",
                        p.topic,
                        p.partition,
                        p.state,
                        p.records_with_required_headers,
                        p.records_scanned,
                        p.segments_selected
                    ),
                    76
                )
            );
        }
        if let Some(ref snapshot) = preflight.consumer_group_snapshot {
            println!(
                "║   {:76} ║",
                truncate_string(
                    &format!(
                        "consumer-groups snapshot: {} ({} groups, {} offsets)",
                        snapshot.state, snapshot.groups, snapshot.offsets
                    ),
                    76
                )
            );
        }
        for error in &preflight.errors {
            println!("║     ✗ {:72} ║", truncate_string(error, 72));
        }
        println!(
            "╟──────────────────────────────────────────────────────────────────────────────╢"
        );
    }

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
    let filtered = report.restore_report.records_dropped_by_filter
        + report.restore_report.records_tombstoned_by_filter;
    if filtered > 0 {
        println!(
            "║   Records filtered: {:58} ║",
            format!(
                "{} ({} dropped, {} tombstoned)",
                filtered,
                report.restore_report.records_dropped_by_filter,
                report.restore_report.records_tombstoned_by_filter
            )
        );
    }
    println!(
        "║   Offset mappings: {:59} ║",
        report
            .restore_report
            .offset_mapping
            .detailed_mapping_count()
    );

    if !report.restore_report.errors.is_empty() {
        println!(
            "║   Errors:                                                                    ║"
        );
        for error in &report.restore_report.errors {
            println!("║     - {:72} ║", truncate_string(error, 72));
        }
    }

    // Phase 3 summary
    if let Some(ref reset_plan) = report.offset_reset_plan {
        println!(
            "╟──────────────────────────────────────────────────────────────────────────────╢"
        );
        println!(
            "║ PHASE 3: OFFSET RESET                                                        ║"
        );
        println!(
            "╟──────────────────────────────────────────────────────────────────────────────╢"
        );
        println!(
            "║   Strategy: {:66} ║",
            format!("{:?}", reset_plan.strategy)
        );
        println!("║   Consumer groups: {:59} ║", reset_plan.groups.len());

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
            println!(
                "║   Note: Dry run mode - offsets not actually reset                            ║"
            );
        }
    } else {
        println!(
            "╟──────────────────────────────────────────────────────────────────────────────╢"
        );
        println!(
            "║ PHASE 3: OFFSET RESET (skipped)                                              ║"
        );
        println!(
            "║   No consumer groups configured or reset_consumer_offsets=false              ║"
        );
    }

    // Warnings
    if !report.warnings.is_empty() {
        println!(
            "╟──────────────────────────────────────────────────────────────────────────────╢"
        );
        println!(
            "║ WARNINGS                                                                     ║"
        );
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
