use kafka_backup_core::{MetricsConfig, MetricsServer, MetricsServerConfig, PrometheusMetrics};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

pub struct RunningMetricsServer {
    shutdown_tx: broadcast::Sender<()>,
    handle: JoinHandle<()>,
    keep_alive_seconds: u64,
}

impl RunningMetricsServer {
    pub fn start(
        metrics_config: &MetricsConfig,
        prometheus_metrics: Arc<PrometheusMetrics>,
    ) -> Option<Self> {
        if !metrics_config.enabled {
            return None;
        }

        let (shutdown_tx, _) = broadcast::channel::<()>(1);
        let server_config = MetricsServerConfig::new(
            format!("{}:{}", metrics_config.bind_address, metrics_config.port)
                .parse()
                .unwrap_or_else(|_| "0.0.0.0:8080".parse().unwrap()),
            metrics_config.path.clone(),
        );

        let server = MetricsServer::new(server_config, prometheus_metrics);
        let shutdown_rx = shutdown_tx.subscribe();
        let handle = tokio::spawn(async move {
            if let Err(e) = server.run(shutdown_rx).await {
                tracing::error!("Metrics server error: {}", e);
            }
        });

        Some(Self {
            shutdown_tx,
            handle,
            keep_alive_seconds: metrics_config.keep_alive_seconds,
        })
    }

    pub async fn shutdown_after_operation(self, signal_rx: &mut broadcast::Receiver<()>) {
        if self.handle.is_finished() {
            warn!("Skipping metrics keep-alive because the metrics server is no longer running");
        } else if wait_for_keep_alive(self.keep_alive_seconds, signal_rx).await {
            info!("Shutdown signal received during metrics keep-alive");
        }

        let _ = self.shutdown_tx.send(());
        let _ = self.handle.await;
    }
}

pub fn spawn_shutdown_signal_forwarder(
    operation_shutdown_tx: broadcast::Sender<()>,
    lifecycle_shutdown_tx: broadcast::Sender<()>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!("Received shutdown signal, initiating graceful shutdown...");
        let _ = operation_shutdown_tx.send(());
        let _ = lifecycle_shutdown_tx.send(());
    })
}

async fn wait_for_keep_alive(
    keep_alive_seconds: u64,
    signal_rx: &mut broadcast::Receiver<()>,
) -> bool {
    if keep_alive_seconds == 0 {
        return false;
    }

    info!(
        "Keeping metrics server alive for {} seconds after operation completion",
        keep_alive_seconds
    );

    tokio::select! {
        _ = sleep(Duration::from_secs(keep_alive_seconds)) => false,
        result = signal_rx.recv() => {
            if let Err(e) = result {
                warn!("Metrics keep-alive signal channel closed: {}", e);
            }
            true
        }
    }
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = sigterm.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::timeout;

    #[tokio::test]
    async fn keep_alive_zero_returns_immediately() {
        let (_tx, mut rx) = broadcast::channel::<()>(1);

        timeout(Duration::from_millis(20), wait_for_keep_alive(0, &mut rx))
            .await
            .expect("zero keep-alive should not wait");
    }

    #[tokio::test]
    async fn keep_alive_is_interrupted_by_signal() {
        let (tx, mut rx) = broadcast::channel::<()>(1);
        let _ = tx.send(());

        let interrupted = timeout(Duration::from_millis(20), wait_for_keep_alive(60, &mut rx))
            .await
            .expect("signal should interrupt keep-alive promptly");

        assert!(interrupted);
    }

    #[tokio::test]
    async fn keep_alive_waits_without_signal() {
        let (_tx, mut rx) = broadcast::channel::<()>(1);

        let result = timeout(Duration::from_millis(20), wait_for_keep_alive(60, &mut rx)).await;

        assert!(result.is_err(), "keep-alive should wait without a signal");
    }
}
