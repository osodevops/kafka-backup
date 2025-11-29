//! HTTP server for exposing Prometheus metrics.
//!
//! This module provides a simple HTTP server that exposes metrics in Prometheus format
//! at the `/metrics` endpoint. It also provides a `/health` endpoint for health checks.

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use super::PerformanceMetrics;
use crate::health::HealthCheck;

/// Configuration for the metrics server
#[derive(Debug, Clone)]
pub struct MetricsServerConfig {
    /// Address to bind the server to
    pub bind_address: SocketAddr,
}

impl Default for MetricsServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0:9090".parse().unwrap(),
        }
    }
}

/// HTTP server for exposing metrics
pub struct MetricsServer {
    config: MetricsServerConfig,
    metrics: Arc<PerformanceMetrics>,
    health: Option<Arc<HealthCheck>>,
}

impl MetricsServer {
    /// Create a new metrics server
    pub fn new(config: MetricsServerConfig, metrics: Arc<PerformanceMetrics>) -> Self {
        Self {
            config,
            metrics,
            health: None,
        }
    }

    /// Set the health check instance
    pub fn with_health(mut self, health: Arc<HealthCheck>) -> Self {
        self.health = Some(health);
        self
    }

    /// Run the metrics server
    pub async fn run(&self, mut shutdown: broadcast::Receiver<()>) -> crate::Result<()> {
        let listener = TcpListener::bind(self.config.bind_address)
            .await
            .map_err(|e| {
                crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::AddrInUse,
                    format!("Failed to bind metrics server: {}", e),
                ))
            })?;

        info!("Metrics server listening on {}", self.config.bind_address);

        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((mut stream, addr)) => {
                            debug!("Connection from {}", addr);

                            // Read request
                            let mut buffer = [0u8; 1024];
                            let n = match stream.read(&mut buffer).await {
                                Ok(n) => n,
                                Err(e) => {
                                    warn!("Failed to read request: {}", e);
                                    continue;
                                }
                            };

                            let request = String::from_utf8_lossy(&buffer[..n]);
                            let response = self.handle_request(&request);

                            if let Err(e) = stream.write_all(response.as_bytes()).await {
                                warn!("Failed to write response: {}", e);
                            }
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

    fn handle_request(&self, request: &str) -> String {
        // Parse the request line
        let first_line = request.lines().next().unwrap_or("");
        let parts: Vec<&str> = first_line.split_whitespace().collect();

        if parts.len() < 2 {
            return self.http_response(400, "text/plain", "Bad Request");
        }

        let method = parts[0];
        let path = parts[1];

        if method != "GET" {
            return self.http_response(405, "text/plain", "Method Not Allowed");
        }

        match path {
            "/metrics" => {
                let report = self.metrics.report();
                let body = report.to_prometheus();
                self.http_response(200, "text/plain; version=0.0.4; charset=utf-8", &body)
            }
            "/health" => {
                if let Some(health) = &self.health {
                    let report = health.report();
                    let status = match report.status {
                        crate::health::HealthStatus::Healthy => "healthy",
                        crate::health::HealthStatus::Degraded => "degraded",
                        crate::health::HealthStatus::Unhealthy => "unhealthy",
                    };
                    let body = format!(
                        r#"{{"status":"{}","uptime_secs":{:.0},"components":{}}}"#,
                        status,
                        report.uptime_secs,
                        report.components.len()
                    );
                    let code = match report.status {
                        crate::health::HealthStatus::Healthy => 200,
                        crate::health::HealthStatus::Degraded => 200,
                        crate::health::HealthStatus::Unhealthy => 503,
                    };
                    self.http_response(code, "application/json", &body)
                } else {
                    self.http_response(200, "application/json", r#"{"status":"healthy"}"#)
                }
            }
            "/" => {
                let body = r#"<!DOCTYPE html>
<html>
<head><title>Kafka Backup Metrics</title></head>
<body>
<h1>Kafka Backup Metrics Server</h1>
<ul>
<li><a href="/metrics">/metrics</a> - Prometheus metrics</li>
<li><a href="/health">/health</a> - Health check</li>
</ul>
</body>
</html>"#;
                self.http_response(200, "text/html", body)
            }
            _ => self.http_response(404, "text/plain", "Not Found"),
        }
    }

    fn http_response(&self, status: u16, content_type: &str, body: &str) -> String {
        let status_text = match status {
            200 => "OK",
            400 => "Bad Request",
            404 => "Not Found",
            405 => "Method Not Allowed",
            503 => "Service Unavailable",
            _ => "Unknown",
        };

        format!(
            "HTTP/1.1 {} {}\r\n\
             Content-Type: {}\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\
             \r\n\
             {}",
            status,
            status_text,
            content_type,
            body.len(),
            body
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_format() {
        let metrics = PerformanceMetrics::new();
        metrics.record_records(1000);
        metrics.record_bytes(500, 2000);
        metrics.record_segment();

        let report = metrics.report();
        let prometheus = report.to_prometheus();

        assert!(prometheus.contains("kafka_backup_records_total 1000"));
        assert!(prometheus.contains("kafka_backup_bytes_written_total 500"));
        assert!(prometheus.contains("kafka_backup_segments_total 1"));
        assert!(prometheus.contains("kafka_backup_compression_ratio"));
    }
}
