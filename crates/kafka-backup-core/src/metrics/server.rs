//! HTTP server for exposing Prometheus metrics.
//!
//! This module provides an HTTP server using Hyper 1.x that exposes metrics in
//! Prometheus/OpenMetrics format at the `/metrics` endpoint. It also provides
//! a `/health` endpoint for health checks.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use super::registry::PrometheusMetrics;
use super::PerformanceMetrics;
use crate::health::HealthCheck;

/// Configuration for the metrics server.
#[derive(Debug, Clone)]
pub struct MetricsServerConfig {
    /// Address to bind the server to.
    pub bind_address: SocketAddr,
    /// Path for metrics endpoint (default: "/metrics").
    pub metrics_path: String,
}

impl Default for MetricsServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:8080".parse().unwrap(),
            metrics_path: "/metrics".to_string(),
        }
    }
}

impl MetricsServerConfig {
    /// Create a new config with a custom port.
    pub fn with_port(port: u16) -> Self {
        Self {
            bind_address: SocketAddr::from(([0, 0, 0, 0], port)),
            ..Default::default()
        }
    }

    /// Create a new config with a custom bind address.
    pub fn with_address(addr: impl Into<SocketAddr>) -> Self {
        Self {
            bind_address: addr.into(),
            ..Default::default()
        }
    }
}

/// HTTP server for exposing Prometheus metrics.
///
/// This server provides the following endpoints:
/// - `/metrics` - Prometheus metrics in text format
/// - `/health` - Health check endpoint (JSON)
/// - `/` - Simple HTML page with links
///
/// The server supports both the new `PrometheusMetrics` registry and the
/// legacy `PerformanceMetrics` for backward compatibility.
pub struct MetricsServer {
    config: MetricsServerConfig,
    prometheus_metrics: Option<Arc<PrometheusMetrics>>,
    legacy_metrics: Option<Arc<PerformanceMetrics>>,
    health: Option<Arc<HealthCheck>>,
}

impl MetricsServer {
    /// Create a new metrics server with Prometheus metrics.
    pub fn new(config: MetricsServerConfig, metrics: Arc<PrometheusMetrics>) -> Self {
        Self {
            config,
            prometheus_metrics: Some(metrics),
            legacy_metrics: None,
            health: None,
        }
    }

    /// Create a new metrics server with legacy PerformanceMetrics.
    #[deprecated(note = "Use new() with PrometheusMetrics instead")]
    pub fn with_legacy_metrics(
        config: MetricsServerConfig,
        metrics: Arc<PerformanceMetrics>,
    ) -> Self {
        Self {
            config,
            prometheus_metrics: None,
            legacy_metrics: Some(metrics),
            health: None,
        }
    }

    /// Set the health check instance.
    pub fn with_health(mut self, health: Arc<HealthCheck>) -> Self {
        self.health = Some(health);
        self
    }

    /// Run the metrics server.
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) -> crate::Result<()> {
        let listener = TcpListener::bind(self.config.bind_address)
            .await
            .map_err(|e| {
                crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    format!("Failed to bind metrics server: {}", e),
                ))
            })?;

        info!(
            "Metrics server listening on http://{}",
            self.config.bind_address
        );

        // Create shared state for request handling
        let state = Arc::new(ServerState {
            prometheus_metrics: self.prometheus_metrics.clone(),
            legacy_metrics: self.legacy_metrics.clone(),
            health: self.health.clone(),
            metrics_path: self.config.metrics_path.clone(),
        });

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            debug!("Connection from {}", addr);

                            let io = TokioIo::new(stream);
                            let state = Arc::clone(&state);

                            // Spawn a task to handle the connection
                            tokio::spawn(async move {
                                let service = service_fn(move |req| {
                                    let state = Arc::clone(&state);
                                    async move {
                                        Ok::<_, Infallible>(handle_request(req, &state))
                                    }
                                });

                                if let Err(err) = http1::Builder::new()
                                    .serve_connection(io, service)
                                    .await
                                {
                                    // Connection errors are expected when clients disconnect
                                    if !err.is_incomplete_message() {
                                        warn!("Error serving connection from {}: {}", addr, err);
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            warn!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = shutdown.recv() => {
                    info!("Metrics server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Run the metrics server until a signal is received.
    ///
    /// This is a convenience method that creates its own shutdown channel.
    pub async fn run_until_shutdown(self) -> crate::Result<()> {
        let (tx, rx) = broadcast::channel(1);

        // Set up signal handler
        let shutdown_tx = tx.clone();
        tokio::spawn(async move {
            if let Err(e) = tokio::signal::ctrl_c().await {
                error!("Failed to listen for ctrl-c: {}", e);
            }
            let _ = shutdown_tx.send(());
        });

        self.run(rx).await
    }
}

/// Shared state for request handling.
struct ServerState {
    prometheus_metrics: Option<Arc<PrometheusMetrics>>,
    legacy_metrics: Option<Arc<PerformanceMetrics>>,
    health: Option<Arc<HealthCheck>>,
    metrics_path: String,
}

/// Handle an incoming HTTP request.
fn handle_request(req: Request<Incoming>, state: &ServerState) -> Response<Full<Bytes>> {
    // Only allow GET requests
    if req.method() != Method::GET {
        return make_response(
            StatusCode::METHOD_NOT_ALLOWED,
            "text/plain",
            "Method Not Allowed",
        );
    }

    let path = req.uri().path();

    if path == state.metrics_path || path == "/metrics" {
        // Metrics endpoint
        let body = if let Some(ref metrics) = state.prometheus_metrics {
            metrics.encode()
        } else if let Some(ref metrics) = state.legacy_metrics {
            metrics.report().to_prometheus()
        } else {
            String::new()
        };

        make_response(
            StatusCode::OK,
            "text/plain; version=0.0.4; charset=utf-8",
            &body,
        )
    } else if path == "/health" || path == "/healthz" {
        // Health check endpoint
        handle_health_request(state)
    } else if path == "/" {
        // Root endpoint with links
        let body = format!(
            r#"<!DOCTYPE html>
<html>
<head><title>Kafka Backup Metrics</title></head>
<body>
<h1>Kafka Backup Metrics Server</h1>
<ul>
<li><a href="{}">{}</a> - Prometheus metrics</li>
<li><a href="/health">/health</a> - Health check</li>
</ul>
</body>
</html>"#,
            state.metrics_path, state.metrics_path
        );
        make_response(StatusCode::OK, "text/html; charset=utf-8", &body)
    } else {
        make_response(StatusCode::NOT_FOUND, "text/plain", "Not Found")
    }
}

/// Handle health check request.
fn handle_health_request(state: &ServerState) -> Response<Full<Bytes>> {
    if let Some(ref health) = state.health {
        let report = health.report();
        let status = match report.status {
            crate::health::HealthStatus::Healthy => "healthy",
            crate::health::HealthStatus::Degraded => "degraded",
            crate::health::HealthStatus::Unhealthy => "unhealthy",
        };

        // Build components JSON
        let components_json: Vec<String> = report
            .components
            .iter()
            .map(|comp| {
                let comp_status = match comp.status {
                    crate::health::HealthStatus::Healthy => "healthy",
                    crate::health::HealthStatus::Degraded => "degraded",
                    crate::health::HealthStatus::Unhealthy => "unhealthy",
                };
                format!(
                    r#""{}":{{"status":"{}","message":{}}}"#,
                    comp.name,
                    comp_status,
                    comp.message
                        .as_ref()
                        .map(|m| format!("\"{}\"", m.replace('"', "\\\"")))
                        .unwrap_or_else(|| "null".to_string())
                )
            })
            .collect();

        let body = format!(
            r#"{{"status":"{}","uptime_secs":{:.0},"components":{{{}}}}}"#,
            status,
            report.uptime_secs,
            components_json.join(",")
        );

        let code = match report.status {
            crate::health::HealthStatus::Healthy => StatusCode::OK,
            crate::health::HealthStatus::Degraded => StatusCode::OK,
            crate::health::HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
        };

        make_response(code, "application/json", &body)
    } else {
        make_response(
            StatusCode::OK,
            "application/json",
            r#"{"status":"healthy"}"#,
        )
    }
}

/// Create an HTTP response.
fn make_response(status: StatusCode, content_type: &str, body: &str) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", content_type)
        .header("Content-Length", body.len())
        .body(Full::new(Bytes::from(body.to_string())))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = MetricsServerConfig::default();
        assert_eq!(config.bind_address.port(), 8080);
        assert_eq!(config.metrics_path, "/metrics");
    }

    #[test]
    fn test_config_with_port() {
        let config = MetricsServerConfig::with_port(9090);
        assert_eq!(config.bind_address.port(), 9090);
    }

    #[test]
    fn test_make_response() {
        let response = make_response(StatusCode::OK, "text/plain", "Hello");
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_server_creation() {
        let config = MetricsServerConfig::with_port(0); // Use any available port
        let metrics = Arc::new(PrometheusMetrics::new());
        let _server = MetricsServer::new(config, metrics);
    }

    #[test]
    fn test_server_state() {
        // Verify ServerState can be created with the correct fields
        let _state = ServerState {
            prometheus_metrics: Some(Arc::new(PrometheusMetrics::new())),
            legacy_metrics: None,
            health: None,
            metrics_path: "/metrics".to_string(),
        };
    }
}
