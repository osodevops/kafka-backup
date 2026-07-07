use anyhow::Result;
use kafka_backup_core::{restore::RestoreEngine, Config, PrometheusMetrics};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::info;

pub async fn run(config_path: &str) -> Result<()> {
    info!("Loading configuration from: {}", config_path);

    let config_content = tokio::fs::read_to_string(config_path).await?;
    let config_content = super::config::expand_env_vars(&config_content);
    let mut config: Config = serde_yaml::from_str(&config_content)?;
    super::sasl_plugin::populate_sasl_plugin_opt(&mut config.target)?;

    info!("Starting restore from backup: {}", config.backup_id);

    // Create Prometheus metrics registry
    let metrics_config = config.metrics.clone().unwrap_or_default();
    let prometheus_metrics = Arc::new(PrometheusMetrics::with_max_labels(
        metrics_config.max_partition_labels,
    ));

    let metrics_server =
        super::metrics_runtime::RunningMetricsServer::start(&metrics_config, prometheus_metrics);

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

    result?;
    info!("Restore completed successfully");
    Ok(())
}
