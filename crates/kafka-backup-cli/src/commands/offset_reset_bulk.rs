//! CLI command for bulk parallel offset reset operations.
//!
//! This command provides:
//! - Parallel offset reset with configurable concurrency (~50x speedup)
//! - Per-partition retry with exponential backoff
//! - Detailed performance metrics (p50/p99 latency, throughput)
//! - Progress reporting and observability

use anyhow::{Context, Result};
use kafka_backup_core::config::{KafkaConfig, SaslMechanism, SecurityConfig, SecurityProtocol};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::manifest::OffsetMapping;
use kafka_backup_core::restore::offset_automation::{
    BulkOffsetReset, BulkOffsetResetConfig, BulkOffsetResetReport, BulkResetStatus,
    OffsetMapping as BulkOffsetMapping,
};
use kafka_backup_core::storage::{FilesystemBackend, StorageBackend};
use std::path::PathBuf;
use tracing::{info, warn};

/// Output format for bulk offset reset reports
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OutputFormat {
    Text,
    Json,
}

impl From<&str> for OutputFormat {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => OutputFormat::Json,
            _ => OutputFormat::Text,
        }
    }
}

/// Execute bulk parallel offset reset
#[allow(clippy::too_many_arguments)]
pub async fn execute_bulk(
    path: &str,
    backup_id: &str,
    consumer_groups: &[String],
    bootstrap_servers: &[String],
    max_concurrent: usize,
    max_retries: u32,
    security_protocol: Option<&str>,
    format: OutputFormat,
) -> Result<()> {
    let storage = FilesystemBackend::new(path.into());

    // Load offset mapping from restore report
    let mapping = load_offset_mapping(&storage, backup_id)
        .await
        .context("Failed to load offset mapping")?;

    info!(
        "Loaded offset mapping with {} consumer groups, {} entries",
        mapping.consumer_groups.len(),
        mapping.entries.len()
    );

    // Build offset mappings for bulk reset
    let bulk_mappings = build_bulk_offset_mappings(&mapping, consumer_groups)?;

    if bulk_mappings.is_empty() {
        warn!("No offset mappings found for specified consumer groups");
        println!(
            "No offsets to reset. Check that the consumer groups exist in the offset mapping."
        );
        return Ok(());
    }

    info!(
        "Prepared {} offset mappings across {} groups",
        bulk_mappings.len(),
        consumer_groups.len()
    );

    // Create Kafka client
    let kafka_config = KafkaConfig {
        bootstrap_servers: bootstrap_servers.to_vec(),
        security: parse_security_config(security_protocol),
        topics: Default::default(),
        connection: Default::default(),
    };

    let client = KafkaClient::new(kafka_config);
    client
        .connect()
        .await
        .context("Failed to connect to Kafka")?;

    // Configure bulk reset
    let config = BulkOffsetResetConfig {
        max_concurrent_requests: max_concurrent,
        max_retry_attempts: max_retries,
        retry_base_delay_ms: 100,
        request_timeout_ms: 30000,
        continue_on_error: true,
    };

    println!("Starting bulk offset reset...");
    println!("  Groups: {}", consumer_groups.len());
    println!("  Total offsets: {}", bulk_mappings.len());
    println!("  Max concurrency: {}", max_concurrent);
    println!("  Max retries: {}", max_retries);
    println!();

    // Execute bulk reset
    let bulk_reset = BulkOffsetReset::new(client, config);
    let report = bulk_reset
        .reset_offsets_parallel(bulk_mappings)
        .await
        .context("Bulk offset reset failed")?;

    // Output results
    output_report(&report, format)?;

    // Return error if operation failed completely
    if report.status == BulkResetStatus::Failed {
        anyhow::bail!("All offset resets failed");
    }

    Ok(())
}

/// Build bulk offset mappings from the offset mapping data
fn build_bulk_offset_mappings(
    mapping: &OffsetMapping,
    consumer_groups: &[String],
) -> Result<Vec<BulkOffsetMapping>> {
    let mut bulk_mappings = Vec::new();

    for group_id in consumer_groups {
        if let Some(group_offsets) = mapping.consumer_groups.get(group_id) {
            for (topic, topic_offsets) in &group_offsets.offsets {
                for (partition, offset_info) in topic_offsets {
                    // Use target_offset if available, otherwise try to look up from detailed mapping
                    let target_offset = offset_info.target_offset.or_else(|| {
                        mapping.lookup_target_offset(topic, *partition, offset_info.source_offset)
                    });

                    if let Some(offset) = target_offset {
                        bulk_mappings.push(BulkOffsetMapping {
                            group_id: group_id.clone(),
                            topic: topic.clone(),
                            partition: *partition,
                            new_offset: offset,
                            metadata: offset_info.metadata.clone(),
                        });
                    } else {
                        warn!(
                            "No target offset for {}:{}:{}, skipping",
                            group_id, topic, partition
                        );
                    }
                }
            }
        } else {
            warn!("Consumer group {} not found in offset mapping", group_id);
        }
    }

    Ok(bulk_mappings)
}

/// Load offset mapping from storage
async fn load_offset_mapping(
    storage: &FilesystemBackend,
    backup_id: &str,
) -> Result<OffsetMapping> {
    // First try to load from restore report (has actual source->target mapping)
    let restore_report_key = format!("{}/restore-report.json", backup_id);
    if let Ok(data) = storage.get(&restore_report_key).await {
        let report: kafka_backup_core::manifest::RestoreReport = serde_json::from_slice(&data)?;
        info!("Loaded offset mapping from restore report");
        return Ok(report.offset_mapping);
    }

    // Fall back to offset-mapping.json
    let mapping_key = format!("{}/offset-mapping.json", backup_id);
    if let Ok(data) = storage.get(&mapping_key).await {
        let mapping: OffsetMapping = serde_json::from_slice(&data)?;
        info!("Loaded offset mapping from offset-mapping.json");
        return Ok(mapping);
    }

    anyhow::bail!(
        "No offset mapping found for backup {}. Run a restore first to generate the mapping.",
        backup_id
    )
}

/// Output the bulk reset report in the specified format
fn output_report(report: &BulkOffsetResetReport, format: OutputFormat) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(report)?);
        }
        OutputFormat::Text => {
            print_report_text(report);
        }
    }

    Ok(())
}

/// Print the bulk reset report in human-readable format
fn print_report_text(report: &BulkOffsetResetReport) {
    let status_icon = match report.status {
        BulkResetStatus::Success => "✓",
        BulkResetStatus::PartialSuccess => "⚠",
        BulkResetStatus::Failed => "✗",
    };

    let status_text = match report.status {
        BulkResetStatus::Success => "SUCCESS",
        BulkResetStatus::PartialSuccess => "PARTIAL SUCCESS",
        BulkResetStatus::Failed => "FAILED",
    };

    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                         BULK OFFSET RESET REPORT                             ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Status: {} {:67} ║", status_icon, status_text);
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Summary                                                                      ║");
    println!("╟──────────────────────────────────────────────────────────────────────────────╢");
    println!("║   Total Groups:     {:57} ║", report.total_groups);
    println!("║   Successful:       {:57} ║", report.successful_groups);
    println!("║   Failed:           {:57} ║", report.failed_groups);
    println!("║   Offsets Reset:    {:57} ║", report.total_offsets_reset);
    println!("║   Offsets Failed:   {:57} ║", report.total_offsets_failed);
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Performance                                                                  ║");
    println!("╟──────────────────────────────────────────────────────────────────────────────╢");
    println!("║   Duration:         {:54} ms ║", report.duration_ms);
    println!(
        "║   Avg Latency:      {:54.2} ms ║",
        report.performance.avg_latency_ms
    );
    println!(
        "║   P50 Latency:      {:54.2} ms ║",
        report.performance.p50_latency_ms
    );
    println!(
        "║   P99 Latency:      {:54.2} ms ║",
        report.performance.p99_latency_ms
    );
    println!(
        "║   Throughput:       {:51.2} ops/s ║",
        report.performance.offsets_per_second
    );
    println!(
        "║   Max Concurrency:  {:57} ║",
        report.performance.max_concurrency
    );
    println!(
        "║   Total Retries:    {:57} ║",
        report.performance.total_retries
    );
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");

    // Print any errors
    if !report.group_outcomes.is_empty() {
        let failed_groups: Vec<_> = report
            .group_outcomes
            .iter()
            .filter(|g| !g.errors.is_empty())
            .collect();

        if !failed_groups.is_empty() {
            println!();
            println!("Errors:");
            for group in failed_groups {
                println!("  Group: {}", group.group_id);
                for error in &group.errors {
                    println!(
                        "    - {}:{} - {} (code: {})",
                        error.topic, error.partition, error.message, error.error_code
                    );
                }
            }
        }
    }
}

/// Parse security configuration from protocol string
fn parse_security_config(protocol: Option<&str>) -> SecurityConfig {
    let security_protocol = match protocol.map(|p| p.to_uppercase()).as_deref() {
        Some("SASL_SSL") => SecurityProtocol::SaslSsl,
        Some("SSL") => SecurityProtocol::Ssl,
        Some("SASL_PLAINTEXT") => SecurityProtocol::SaslPlaintext,
        _ => SecurityProtocol::Plaintext,
    };

    let (sasl_mechanism, sasl_username, sasl_password) = if matches!(
        security_protocol,
        SecurityProtocol::SaslSsl | SecurityProtocol::SaslPlaintext
    ) {
        (
            Some(SaslMechanism::Plain),
            std::env::var("KAFKA_USERNAME").ok(),
            std::env::var("KAFKA_PASSWORD").ok(),
        )
    } else {
        (None, None, None)
    };

    let ssl_ca_location = if matches!(
        security_protocol,
        SecurityProtocol::Ssl | SecurityProtocol::SaslSsl
    ) {
        std::env::var("KAFKA_SSL_CA_CERT").ok().map(PathBuf::from)
    } else {
        None
    };

    SecurityConfig {
        security_protocol,
        sasl_mechanism,
        sasl_username,
        sasl_password,
        ssl_ca_location,
        ssl_certificate_location: None,
        ssl_key_location: None,
    }
}
