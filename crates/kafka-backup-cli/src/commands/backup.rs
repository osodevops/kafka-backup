//! `backup`: the Kafka data backup and, with a `schema_registry` section,
//! the registry snapshot (Confluent Schema Registry and/or Apicurio Registry).
//!
//! The snapshot does not depend on the data backup:
//!
//! - one-shot (`backup.continuous: false`): one snapshot after the data
//!   backup, also when the data backup failed;
//! - continuous: a snapshot at start and every
//!   `schema_registry.backup.snapshot_interval` (default 15m) alongside the
//!   engine, then a final one at shutdown;
//! - `--schema-only`: one snapshot, no Kafka connection.

use anyhow::Result;
use kafka_backup_core::{backup::BackupEngine, Config, PrometheusMetrics};
use std::sync::Arc;
use tokio::sync::{broadcast, watch};
use tracing::{error, info};

use super::schema_registry::{combine, print_pass, Snapshots};

pub async fn run(config_path: &str, schema_only: bool) -> Result<()> {
    info!("Loading configuration from: {}", config_path);

    let config_content = tokio::fs::read_to_string(config_path).await?;
    let config_content = super::config::expand_env_vars(&config_content);
    let mut config: Config = super::config::parse_config(&config_content)?;
    // OAuth before any client exists: the token caches are shared by every
    // connection the engine opens.
    let oauth = super::oauth::apply(&mut config)?;
    if schema_only {
        return super::schema_registry::backup_schema_only(&config).await;
    }
    super::sasl_plugin::populate_sasl_plugin_opt(&mut config.source)?;
    let snapshots = Snapshots::from_config(&config)?;
    let continuous = config.backup.as_ref().is_some_and(|b| b.continuous);
    let backup_id = config.backup_id.clone();

    info!("Starting backup: {}", config.backup_id);

    // Create Prometheus metrics registry
    let metrics_config = config.metrics.clone().unwrap_or_default();
    let prometheus_metrics = Arc::new(PrometheusMetrics::with_max_labels(
        metrics_config.max_partition_labels,
    ));

    // Initialize before exposing HTTP, so even the first scrape has error samples.
    prometheus_metrics.initialize_backup_errors(&config.backup_id);
    prometheus_metrics.track_oauth(&oauth.caches);

    let metrics_server = super::metrics_runtime::RunningMetricsServer::start(
        &metrics_config,
        prometheus_metrics.clone(),
    );

    let (lifecycle_shutdown_tx, mut lifecycle_shutdown_rx) = broadcast::channel::<()>(1);

    // Periodic registry snapshots run alongside a continuous engine.
    let (stop_tx, stop_rx) = watch::channel(false);
    let periodic = match (&snapshots, continuous) {
        (Some(snapshots), true) => Some(snapshots.spawn_periodic(stop_rx)),
        _ => None,
    };

    // An engine that cannot be built is a data-backup failure like any
    // other: the registry snapshot below is still taken.
    let (kafka_result, signal_task) =
        match BackupEngine::new_with_metrics(config, Some(prometheus_metrics)).await {
            Ok(engine) => {
                // Signal handler for graceful shutdown (SIGTERM + SIGINT/Ctrl-C)
                let signal_task = super::metrics_runtime::spawn_shutdown_signal_forwarder(
                    engine.shutdown_handle(),
                    lifecycle_shutdown_tx,
                );
                (engine.run().await, Some(signal_task))
            }
            Err(e) => (Err(e), None),
        };
    let kafka_result = kafka_result.map_err(anyhow::Error::from);

    match &kafka_result {
        Ok(()) => info!("Kafka data backup completed successfully"),
        Err(e) if snapshots.is_some() => {
            error!("Kafka data backup failed: {e:#}. Taking the Schema Registry snapshot anyway.")
        }
        Err(_) => {}
    }

    // Stop the periodic task (a pass in flight finishes first), then take
    // the final snapshot: the only one of a one-shot backup, the shutdown
    // one of a continuous backup.
    if let Some(handle) = periodic {
        let _ = stop_tx.send(true);
        let _ = handle.await;
    }
    let pass = match &snapshots {
        Some(snapshots) => {
            let pass = snapshots
                .pass(if continuous {
                    "shutdown"
                } else {
                    "post-backup"
                })
                .await;
            print_pass(&backup_id, &pass);
            Some(pass)
        }
        None => None,
    };

    if let Some(metrics_server) = metrics_server {
        metrics_server
            .shutdown_after_operation(&mut lifecycle_shutdown_rx)
            .await;
    }
    if let Some(signal_task) = signal_task {
        signal_task.abort(); // Clean up signal watcher if operation finished normally
    }

    combine(kafka_result, pass.as_ref())?;
    info!("Backup completed successfully");
    Ok(())
}
