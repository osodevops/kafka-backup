use anyhow::Result;
use kafka_backup_core::restore::{OffsetResetPhaseOutcome, ThreePhaseRestore};
use kafka_backup_core::{restore::RestoreEngine, Config, PrometheusMetrics};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{info, warn};

pub async fn run(config_path: &str) -> Result<()> {
    info!("Loading configuration from: {}", config_path);

    let config_content = tokio::fs::read_to_string(config_path).await?;
    let config_content = super::config::expand_env_vars(&config_content);
    let mut config: Config = super::config::parse_config(&config_content)?;
    super::sasl_plugin::populate_sasl_plugin_opt(&mut config.target)?;

    info!("Starting restore from backup: {}", config.backup_id);

    // Create Prometheus metrics registry
    let metrics_config = config.metrics.clone().unwrap_or_default();
    let prometheus_metrics = Arc::new(PrometheusMetrics::with_max_labels(
        metrics_config.max_partition_labels,
    ));

    let metrics_server =
        super::metrics_runtime::RunningMetricsServer::start(&metrics_config, prometheus_metrics);

    // Phase 3 (consumer group offset reset) is applied after the data restore
    // when the configuration asks for it. The engine itself never commits
    // offsets — see `ThreePhaseRestore::run_offset_reset_phase` (issue #148).
    let wants_offset_reset = config
        .restore
        .as_ref()
        .is_some_and(ThreePhaseRestore::wants_offset_reset);
    let orchestrator = if wants_offset_reset {
        Some(ThreePhaseRestore::new(config.clone())?)
    } else {
        None
    };

    // Run the restore engine
    let engine = RestoreEngine::new(config)?;

    // Spawn signal handler for graceful shutdown (SIGTERM + SIGINT/Ctrl-C)
    let shutdown_tx_signal = engine.shutdown_handle();
    let (lifecycle_shutdown_tx, mut lifecycle_shutdown_rx) = broadcast::channel::<()>(1);
    let signal_task = super::metrics_runtime::spawn_shutdown_signal_forwarder(
        shutdown_tx_signal,
        lifecycle_shutdown_tx,
    );

    let result = engine.run().await;

    if let Some(metrics_server) = metrics_server {
        metrics_server
            .shutdown_after_operation(&mut lifecycle_shutdown_rx)
            .await;
    }
    signal_task.abort(); // Clean up signal watcher if operation finished normally

    let report = result?;

    if let Some(orchestrator) = orchestrator {
        info!(
            "Applying consumer group offset reset (Phase 3) for {} group(s)",
            report.resolved_consumer_groups.len()
        );
        let outcome = orchestrator.run_offset_reset_phase(&report).await?;
        print_offset_reset_summary(&outcome);
        if !outcome.success() {
            anyhow::bail!(
                "Restore completed but consumer group offset reset failed — see the errors above \
                 (offsets on the target were not, or only partly, reset)"
            );
        }
    }

    info!("Restore completed successfully");
    Ok(())
}

/// Print a compact Phase 3 summary so an operator can see at a glance whether
/// consumer group offsets were actually applied.
fn print_offset_reset_summary(outcome: &OffsetResetPhaseOutcome) {
    println!();
    println!("=== Consumer Group Offset Reset (Phase 3) ===");
    match (&outcome.plan, &outcome.report) {
        (None, _) => {
            println!("No consumer groups in scope; nothing to reset.");
        }
        (Some(plan), None) => {
            println!(
                "Planned only (dry run): {} group(s), {} partition(s)",
                plan.groups.len(),
                plan.groups.iter().map(|g| g.partition_count).sum::<usize>()
            );
        }
        (Some(plan), Some(report)) => {
            println!(
                "Groups: {}   Partitions reset: {}   Errors: {}",
                plan.groups.len(),
                report.partitions_reset,
                report.errors.len()
            );
            for group in &plan.groups {
                println!(
                    "  - {} ({} partition(s){})",
                    group.group_id,
                    group.partition_count,
                    if group.complete {
                        ""
                    } else {
                        ", incomplete mapping"
                    }
                );
            }
            for err in &report.errors {
                println!("  ! {}", err);
            }
            println!(
                "Result: {}",
                if report.success {
                    "APPLIED"
                } else {
                    "FAILED (target offsets not, or only partly, reset)"
                }
            );
        }
    }
    for w in &outcome.warnings {
        warn!("{}", w);
    }
    println!();
}
