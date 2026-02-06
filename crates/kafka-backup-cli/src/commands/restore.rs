use anyhow::Result;
use kafka_backup_core::{
    restore::RestoreEngine, Config, MetricsServer, MetricsServerConfig, PrometheusMetrics,
};
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::broadcast;
use tracing::info;

pub async fn run(config_path: &str) -> Result<()> {
    info!("Loading configuration from: {}", config_path);

    let config_content = tokio::fs::read_to_string(config_path).await?;
    let config: Config = serde_yaml::from_str(&config_content)?;

    info!("Starting restore from backup: {}", config.backup_id);

    // Create Prometheus metrics registry
    let metrics_config = config.metrics.clone().unwrap_or_default();
    let prometheus_metrics = Arc::new(PrometheusMetrics::with_max_labels(
        metrics_config.max_partition_labels,
    ));

    // Start metrics server if enabled
    let (shutdown_tx, _) = broadcast::channel::<()>(1);
    let metrics_server_handle = if metrics_config.enabled {
        let server_config = MetricsServerConfig::new(
            format!("{}:{}", metrics_config.bind_address, metrics_config.port)
                .parse()
                .unwrap_or_else(|_| "0.0.0.0:8080".parse().unwrap()),
            metrics_config.path.clone(),
        );

        let server = MetricsServer::new(server_config, prometheus_metrics.clone());
        let shutdown_rx = shutdown_tx.subscribe();

        Some(tokio::spawn(async move {
            if let Err(e) = server.run(shutdown_rx).await {
                tracing::error!("Metrics server error: {}", e);
            }
        }))
    } else {
        None
    };

    // Run the restore engine
    let engine = RestoreEngine::new(config)?;

    // Spawn signal handler for graceful shutdown (SIGTERM + SIGINT/Ctrl-C)
    let shutdown_tx_signal = engine.shutdown_handle();
    let signal_task = tokio::spawn(async move {
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
        info!("Received shutdown signal, initiating graceful shutdown...");
        let _ = shutdown_tx_signal.send(());
    });

    let result = engine.run().await;
    signal_task.abort(); // Clean up signal watcher if engine finished normally

    // Shutdown metrics server
    let _ = shutdown_tx.send(());
    if let Some(handle) = metrics_server_handle {
        let _ = handle.await;
    }

    result?;
    info!("Restore completed successfully");
    Ok(())
}
