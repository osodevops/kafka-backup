//! Live status monitoring for running backup jobs.
//!
//! This module provides real-time monitoring by polling the metrics server's
//! /health and /metrics endpoints.

use anyhow::{Context, Result};
use kafka_backup_core::{Config, HealthStatus, MetricsConfig};
use std::collections::HashMap;
use std::io::{self, Write};
use std::time::Duration;

/// HTTP client for fetching metrics from a running backup job.
pub struct MetricsClient {
    base_url: String,
    client: reqwest::Client,
}

/// Parsed health response from /health endpoint.
#[derive(Debug, Clone)]
pub struct HealthResponse {
    pub status: HealthStatus,
    pub uptime_secs: f64,
    pub components: HashMap<String, ComponentStatus>,
}

/// Component status from health endpoint.
#[derive(Debug, Clone)]
pub struct ComponentStatus {
    pub status: HealthStatus,
    pub message: Option<String>,
}

/// Parsed metrics from /metrics endpoint.
#[derive(Debug, Clone, Default)]
pub struct ParsedMetrics {
    pub bytes_total: u64,
    pub records_total: u64,
    pub lag_by_partition: HashMap<(String, i32), i64>,
    pub compression_ratio: f64,
    pub error_count: u64,
    pub segments_written: u64,
    pub throughput_records_per_sec: f64,
    pub throughput_bytes_per_sec: f64,
}

/// Combined live status from health and metrics.
#[derive(Debug, Clone)]
pub struct LiveStatus {
    pub status: HealthStatus,
    pub uptime_secs: f64,
    pub records_processed: u64,
    pub throughput_records_per_sec: f64,
    #[allow(dead_code)]
    pub active_jobs: usize,
    pub components: HashMap<String, ComponentStatus>,
    pub bytes_total: u64,
    pub lag_records_total: i64,
    pub lag_max_partition: Option<(String, i32, i64)>,
    pub compression_ratio: f64,
    pub error_count: u64,
    pub segments_written: u64,
    pub throughput_bytes_per_sec: f64,
}

impl MetricsClient {
    /// Create a new metrics client.
    pub fn new(host: &str, port: u16) -> Self {
        let base_url = format!("http://{}:{}", host, port);
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("Failed to build HTTP client");

        Self { base_url, client }
    }

    /// Fetch health status from /health endpoint.
    pub async fn fetch_health(&self) -> Result<HealthResponse> {
        let url = format!("{}/health", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to connect to metrics server")?;

        if !response.status().is_success() && response.status().as_u16() != 503 {
            anyhow::bail!(
                "Health endpoint returned unexpected status: {}",
                response.status()
            );
        }

        let json: serde_json::Value = response.json().await.context("Failed to parse health JSON")?;

        let status = match json["status"].as_str().unwrap_or("unknown") {
            "healthy" => HealthStatus::Healthy,
            "degraded" => HealthStatus::Degraded,
            _ => HealthStatus::Unhealthy,
        };

        let uptime_secs = json["uptime_secs"].as_f64().unwrap_or(0.0);

        let mut components = HashMap::new();
        if let Some(comps) = json["components"].as_object() {
            for (name, value) in comps {
                let comp_status = match value["status"].as_str().unwrap_or("unknown") {
                    "healthy" => HealthStatus::Healthy,
                    "degraded" => HealthStatus::Degraded,
                    _ => HealthStatus::Unhealthy,
                };
                let message = value["message"].as_str().map(|s| s.to_string());
                components.insert(
                    name.clone(),
                    ComponentStatus {
                        status: comp_status,
                        message,
                    },
                );
            }
        }

        Ok(HealthResponse {
            status,
            uptime_secs,
            components,
        })
    }

    /// Fetch and parse metrics from /metrics endpoint.
    pub async fn fetch_metrics(&self) -> Result<ParsedMetrics> {
        let url = format!("{}/metrics", self.base_url);
        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to connect to metrics server")?;

        if !response.status().is_success() {
            anyhow::bail!(
                "Metrics endpoint returned status: {}",
                response.status()
            );
        }

        let text = response.text().await.context("Failed to read metrics response")?;
        Ok(parse_prometheus_metrics(&text))
    }

    /// Fetch combined live status.
    pub async fn fetch_live_status(&self) -> Result<LiveStatus> {
        let health = self.fetch_health().await?;
        let metrics = self.fetch_metrics().await.unwrap_or_default();

        // Calculate total lag and find max lag partition
        let lag_records_total: i64 = metrics.lag_by_partition.values().sum();
        let lag_max_partition = metrics
            .lag_by_partition
            .iter()
            .max_by_key(|(_, lag)| *lag)
            .map(|((topic, partition), lag)| (topic.clone(), *partition, *lag));

        Ok(LiveStatus {
            status: health.status,
            uptime_secs: health.uptime_secs,
            records_processed: metrics.records_total,
            throughput_records_per_sec: metrics.throughput_records_per_sec,
            active_jobs: 1, // Assume 1 job if metrics server is responding
            components: health.components,
            bytes_total: metrics.bytes_total,
            lag_records_total,
            lag_max_partition,
            compression_ratio: metrics.compression_ratio,
            error_count: metrics.error_count,
            segments_written: metrics.segments_written,
            throughput_bytes_per_sec: metrics.throughput_bytes_per_sec,
        })
    }
}

/// Parse Prometheus text format metrics.
fn parse_prometheus_metrics(text: &str) -> ParsedMetrics {
    let mut metrics = ParsedMetrics::default();

    for line in text.lines() {
        // Skip comments and empty lines
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        // Parse metric lines
        if let Some((name_labels, value)) = line.rsplit_once(' ') {
            let value: f64 = value.parse().unwrap_or(0.0);

            // Extract metric name (before any labels)
            let name = name_labels
                .split('{')
                .next()
                .unwrap_or(name_labels);

            match name {
                // prometheus-client adds _total suffix to counters
                "kafka_backup_bytes_total" | "kafka_backup_bytes_total_total" | "kafka_backup_bytes_written_total" => {
                    metrics.bytes_total = value as u64;
                }
                "kafka_backup_records_total" | "kafka_backup_records_total_total" | "kafka_backup_records_written_total" => {
                    metrics.records_total = value as u64;
                }
                "kafka_backup_lag_records" => {
                    // Parse labels for topic and partition
                    if let Some((topic, partition)) = parse_topic_partition_labels(name_labels) {
                        metrics.lag_by_partition.insert((topic, partition), value as i64);
                    }
                }
                "kafka_backup_compression_ratio" => {
                    metrics.compression_ratio = value;
                }
                "kafka_backup_errors_total" | "kafka_backup_storage_errors_total" | "kafka_backup_storage_errors_total_total" => {
                    metrics.error_count += value as u64;
                }
                "kafka_backup_segments_total" | "kafka_backup_segments_total_total" | "kafka_backup_segments_written_total" => {
                    metrics.segments_written = value as u64;
                }
                "kafka_backup_storage_write_bytes_total_total" => {
                    // Use storage write bytes as backup bytes if not already set
                    if metrics.bytes_total == 0 {
                        metrics.bytes_total = value as u64;
                    }
                }
                "kafka_backup_throughput_records_per_second" => {
                    metrics.throughput_records_per_sec = value;
                }
                "kafka_backup_throughput_bytes_per_second" => {
                    metrics.throughput_bytes_per_sec = value;
                }
                _ => {}
            }
        }
    }

    metrics
}

/// Parse topic and partition from Prometheus labels.
fn parse_topic_partition_labels(metric_line: &str) -> Option<(String, i32)> {
    // Format: metric_name{topic="foo",partition="0"}
    let labels_start = metric_line.find('{')?;
    let labels_end = metric_line.find('}')?;
    let labels = &metric_line[labels_start + 1..labels_end];

    let mut topic = None;
    let mut partition = None;

    for label in labels.split(',') {
        let parts: Vec<&str> = label.split('=').collect();
        if parts.len() == 2 {
            let key = parts[0].trim();
            let value = parts[1].trim().trim_matches('"');
            match key {
                "topic" => topic = Some(value.to_string()),
                "partition" => partition = value.parse().ok(),
                _ => {}
            }
        }
    }

    match (topic, partition) {
        (Some(t), Some(p)) => Some((t, p)),
        _ => None,
    }
}

/// Load config and extract metrics server settings.
fn load_metrics_config(config_path: &str) -> Result<(MetricsConfig, String)> {
    let config_content = std::fs::read_to_string(config_path)
        .context(format!("Failed to read config file: {}", config_path))?;

    let config: Config = serde_yaml::from_str(&config_content)
        .context("Failed to parse config file")?;

    let metrics_config = config.metrics.unwrap_or_default();
    let backup_id = config.backup_id.clone();

    Ok((metrics_config, backup_id))
}

/// Clear terminal screen (cross-platform).
fn clear_screen() {
    // Use ANSI escape codes for clearing
    print!("\x1B[2J\x1B[H");
    let _ = io::stdout().flush();
}

/// Format uptime as HH:MM:SS.
fn format_uptime(secs: f64) -> String {
    let total_secs = secs as u64;
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

/// Format bytes as human-readable string.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format number with commas.
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

/// Get status indicator icon.
fn status_icon(status: &HealthStatus) -> &'static str {
    match status {
        HealthStatus::Healthy => "OK",
        HealthStatus::Degraded => "!",
        HealthStatus::Unhealthy => "X",
    }
}

/// Get status display text.
fn status_text(status: &HealthStatus) -> &'static str {
    match status {
        HealthStatus::Healthy => "RUNNING",
        HealthStatus::Degraded => "DEGRADED",
        HealthStatus::Unhealthy => "UNHEALTHY",
    }
}

/// Display the live status in a formatted box.
fn display_status(status: &LiveStatus, backup_id: &str, interval: u64) {
    let width = 64;
    let border = "=".repeat(width);

    println!("{}", border);
    println!("  OSO Kafka Backup - Live Status");
    println!("{}", border);
    println!(
        "  Backup ID: {:<36} Uptime: {}",
        backup_id,
        format_uptime(status.uptime_secs)
    );
    println!(
        "  Status: {:<44}",
        status_text(&status.status)
    );
    println!("{}", border);
    println!("  Progress");
    println!(
        "  |- Records: {}",
        format_number(status.records_processed)
    );
    println!(
        "  |- Bytes: {} (compressed)",
        format_bytes(status.bytes_total)
    );

    let throughput_str = if status.throughput_records_per_sec > 0.0 || status.throughput_bytes_per_sec > 0.0 {
        format!(
            "{:.0} rec/s | {}/s",
            status.throughput_records_per_sec,
            format_bytes(status.throughput_bytes_per_sec as u64)
        )
    } else {
        "calculating...".to_string()
    };
    println!("  |- Throughput: {}", throughput_str);

    if let Some((topic, partition, _lag)) = &status.lag_max_partition {
        println!(
            "  |- Lag: {} records ({}-{})",
            format_number(status.lag_records_total as u64),
            topic,
            partition
        );
    } else if status.lag_records_total > 0 {
        println!(
            "  |- Lag: {} records",
            format_number(status.lag_records_total as u64)
        );
    } else {
        println!("  |- Lag: 0 records");
    }

    if status.segments_written > 0 {
        println!("  |- Segments: {}", status.segments_written);
    }

    println!("{}", border);
    println!("  Components");

    // Sort components for consistent display
    let mut component_names: Vec<_> = status.components.keys().collect();
    component_names.sort();

    for name in component_names {
        if let Some(comp) = status.components.get(name) {
            let icon = status_icon(&comp.status);
            let msg = comp.message.as_deref().unwrap_or("ok");
            println!("  |- {:<16} [{}] {}", name, icon, msg);
        }
    }

    if status.components.is_empty() {
        println!("  |- (no components registered)");
    }

    println!("{}", border);

    let compression_str = if status.compression_ratio > 0.0 {
        format!("{:.1}x ratio", status.compression_ratio)
    } else {
        "n/a".to_string()
    };
    println!(
        "  Compression: {} | Errors: {}",
        compression_str,
        status.error_count
    );
    println!("{}", border);

    let now = chrono::Local::now();
    println!(
        "  Last updated: {} | Refresh: {}s | Ctrl+C to exit",
        now.format("%Y-%m-%d %H:%M:%S"),
        interval
    );
}

/// Run watch mode to continuously poll and display backup status.
pub async fn run_watch(config_path: &str, interval_secs: u64) -> Result<()> {
    let (metrics_config, backup_id) = load_metrics_config(config_path)?;

    if !metrics_config.enabled {
        anyhow::bail!(
            "Metrics server is disabled in config. Enable metrics to use watch mode:\n\
            metrics:\n  enabled: true\n  port: 8080"
        );
    }

    // Use localhost if bind_address is 0.0.0.0
    let host = if metrics_config.bind_address == "0.0.0.0" {
        "127.0.0.1"
    } else {
        &metrics_config.bind_address
    };

    let client = MetricsClient::new(host, metrics_config.port);

    println!("Connecting to metrics server at {}:{}...", host, metrics_config.port);

    // Initial connection test
    match client.fetch_health().await {
        Ok(_) => println!("Connected successfully. Starting watch mode...\n"),
        Err(e) => {
            anyhow::bail!(
                "Cannot connect to metrics server at {}:{}.\n\
                Is the backup running with metrics enabled?\n\n\
                Error: {}",
                host,
                metrics_config.port,
                e
            );
        }
    }

    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    let mut reconnect_delay = Duration::from_secs(1);
    let max_reconnect_delay = Duration::from_secs(30);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                match client.fetch_live_status().await {
                    Ok(status) => {
                        clear_screen();
                        display_status(&status, &backup_id, interval_secs);
                        reconnect_delay = Duration::from_secs(1); // Reset on success
                    }
                    Err(e) => {
                        clear_screen();
                        println!("Connection lost: {}", e);
                        println!("Reconnecting in {:?}...", reconnect_delay);
                        tokio::time::sleep(reconnect_delay).await;
                        reconnect_delay = std::cmp::min(reconnect_delay * 2, max_reconnect_delay);
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("\nStopping watch mode...");
                break;
            }
        }
    }

    Ok(())
}

/// Run one-shot status from config (without watch mode).
pub async fn run_from_config(config_path: &str) -> Result<()> {
    let (metrics_config, backup_id) = load_metrics_config(config_path)?;

    if !metrics_config.enabled {
        anyhow::bail!(
            "Metrics server is disabled in config. Enable metrics to query live status."
        );
    }

    let host = if metrics_config.bind_address == "0.0.0.0" {
        "127.0.0.1"
    } else {
        &metrics_config.bind_address
    };

    let client = MetricsClient::new(host, metrics_config.port);

    match client.fetch_live_status().await {
        Ok(status) => {
            display_status(&status, &backup_id, 0);
            Ok(())
        }
        Err(e) => {
            anyhow::bail!(
                "Cannot connect to metrics server at {}:{}.\n\
                Is the backup running with metrics enabled?\n\n\
                Error: {}",
                host,
                metrics_config.port,
                e
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_prometheus_metrics() {
        let input = r#"
# HELP kafka_backup_bytes_total Total bytes written
# TYPE kafka_backup_bytes_total counter
kafka_backup_bytes_total 123456789
kafka_backup_records_total 1000000
kafka_backup_compression_ratio 3.5
kafka_backup_errors_total 2
kafka_backup_lag_records{topic="orders",partition="0"} 100
kafka_backup_lag_records{topic="orders",partition="1"} 200
"#;

        let metrics = parse_prometheus_metrics(input);

        assert_eq!(metrics.bytes_total, 123456789);
        assert_eq!(metrics.records_total, 1000000);
        assert_eq!(metrics.compression_ratio, 3.5);
        assert_eq!(metrics.error_count, 2);
        assert_eq!(metrics.lag_by_partition.len(), 2);
        assert_eq!(
            metrics.lag_by_partition.get(&("orders".to_string(), 0)),
            Some(&100)
        );
        assert_eq!(
            metrics.lag_by_partition.get(&("orders".to_string(), 1)),
            Some(&200)
        );
    }

    #[test]
    fn test_parse_topic_partition_labels() {
        let line = r#"kafka_backup_lag_records{topic="my-topic",partition="5"}"#;
        let result = parse_topic_partition_labels(line);
        assert_eq!(result, Some(("my-topic".to_string(), 5)));

        let line_no_labels = "kafka_backup_bytes_total";
        assert_eq!(parse_topic_partition_labels(line_no_labels), None);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.0 KB");
        assert_eq!(format_bytes(1536), "1.5 KB");
        assert_eq!(format_bytes(1024 * 1024), "1.0 MB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1.0 GB");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(100), "100");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1234567), "1,234,567");
    }

    #[test]
    fn test_format_uptime() {
        assert_eq!(format_uptime(0.0), "00:00:00");
        assert_eq!(format_uptime(61.0), "00:01:01");
        assert_eq!(format_uptime(3661.0), "01:01:01");
        assert_eq!(format_uptime(86400.0), "24:00:00");
    }
}
