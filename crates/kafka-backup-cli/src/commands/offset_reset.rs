//! CLI command for Phase 3 offset reset operations.
//!
//! This command supports:
//! - Generating offset reset plans from restore offset mappings
//! - Previewing reset plans (dry-run)
//! - Executing offset resets
//! - Generating shell scripts for manual execution

use anyhow::Result;
use kafka_backup_core::config::{KafkaConfig, SaslMechanism, SecurityConfig, SecurityProtocol};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::manifest::OffsetMapping;
use kafka_backup_core::restore::offset_reset::{
    OffsetResetExecutor, OffsetResetPlan, OffsetResetStrategy,
};
use kafka_backup_core::storage::{FilesystemBackend, StorageBackend};
use std::path::PathBuf;
use tracing::{info, warn};

/// Output format for offset reset operations
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OutputFormat {
    Text,
    Json,
    Csv,
    ShellScript,
}

impl From<&str> for OutputFormat {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => OutputFormat::Json,
            "csv" => OutputFormat::Csv,
            "shell-script" | "shell" | "sh" | "script" => OutputFormat::ShellScript,
            _ => OutputFormat::Text,
        }
    }
}

/// Generate an offset reset plan from a backup's offset mapping
pub async fn generate_plan(
    path: &str,
    backup_id: &str,
    consumer_groups: &[String],
    bootstrap_servers: &[String],
    dry_run: bool,
    format: OutputFormat,
) -> Result<()> {
    let storage = FilesystemBackend::new(path.into());

    // Try to load offset mapping from restore report first
    let mapping = load_offset_mapping(&storage, backup_id).await?;

    info!(
        "Loaded offset mapping with {} entries",
        mapping.entries.len()
    );

    let strategy = if dry_run {
        OffsetResetStrategy::DryRun
    } else {
        OffsetResetStrategy::Manual
    };

    // Create executor (offline mode for plan generation only)
    let executor = OffsetResetExecutor::new_offline(bootstrap_servers.to_vec());

    let plan = executor
        .generate_plan(&mapping, consumer_groups, strategy)
        .await?;

    output_plan(&plan, format, bootstrap_servers)?;

    Ok(())
}

/// Execute an offset reset plan
pub async fn execute_plan(
    path: &str,
    backup_id: &str,
    consumer_groups: &[String],
    bootstrap_servers: &[String],
    security_protocol: Option<&str>,
) -> Result<()> {
    let storage = FilesystemBackend::new(path.into());
    let mapping = load_offset_mapping(&storage, backup_id).await?;

    info!(
        "Loaded offset mapping with {} entries",
        mapping.entries.len()
    );

    // Create Kafka client
    let kafka_config = KafkaConfig {
        bootstrap_servers: bootstrap_servers.to_vec(),
        security: parse_security_config(security_protocol),
        topics: Default::default(),
    };

    let client = KafkaClient::new(kafka_config);
    client.connect().await?;

    let executor = OffsetResetExecutor::new(client, bootstrap_servers.to_vec());

    // Generate plan with auto strategy
    let plan = executor
        .generate_plan(&mapping, consumer_groups, OffsetResetStrategy::Auto)
        .await?;

    info!("Generated reset plan for {} groups", plan.groups.len());

    // Execute the plan
    let report = executor.execute_plan(&plan).await?;

    if report.success {
        println!("✓ Offset reset completed successfully");
        println!("  Groups reset: {}", report.groups_reset.len());
        println!("  Partitions reset: {}", report.partitions_reset);
    } else {
        println!("✗ Offset reset completed with errors");
        for error in &report.errors {
            println!("  Error: {}", error);
        }
    }

    Ok(())
}

/// Generate a shell script for manual offset reset execution
pub async fn generate_script(
    path: &str,
    backup_id: &str,
    consumer_groups: &[String],
    bootstrap_servers: &[String],
    output_path: Option<&str>,
) -> Result<()> {
    let storage = FilesystemBackend::new(path.into());
    let mapping = load_offset_mapping(&storage, backup_id).await?;

    let executor = OffsetResetExecutor::new_offline(bootstrap_servers.to_vec());

    let plan = executor
        .generate_plan(&mapping, consumer_groups, OffsetResetStrategy::Manual)
        .await?;

    let script = executor.generate_shell_script(&plan);

    if let Some(out_path) = output_path {
        tokio::fs::write(out_path, &script).await?;
        println!("Script written to: {}", out_path);

        // Make executable on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = std::fs::metadata(out_path)?.permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(out_path, perms)?;
        }
    } else {
        println!("{}", script);
    }

    Ok(())
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

    // Fall back to offset-mapping.json if it exists
    let mapping_key = format!("{}/offset-mapping.json", backup_id);
    if let Ok(data) = storage.get(&mapping_key).await {
        let mapping: OffsetMapping = serde_json::from_slice(&data)?;
        info!("Loaded offset mapping from offset-mapping.json");
        return Ok(mapping);
    }

    // Build mapping from manifest as last resort (source offsets only, no target mapping)
    warn!("No restore report found, building offset mapping from manifest (source offsets only)");

    let manifest_key = format!("{}/manifest.json", backup_id);
    let data = storage.get(&manifest_key).await?;
    let manifest: kafka_backup_core::BackupManifest = serde_json::from_slice(&data)?;

    let mut mapping = OffsetMapping::new();

    for topic in &manifest.topics {
        for partition in &topic.partitions {
            let mut min_offset = i64::MAX;
            let mut max_offset = i64::MIN;
            let mut min_ts = i64::MAX;
            let mut max_ts = i64::MIN;

            for segment in &partition.segments {
                if segment.start_offset < min_offset {
                    min_offset = segment.start_offset;
                    min_ts = segment.start_timestamp;
                }
                if segment.end_offset > max_offset {
                    max_offset = segment.end_offset;
                    max_ts = segment.end_timestamp;
                }
            }

            if min_offset != i64::MAX {
                mapping.add(&topic.name, partition.partition_id, min_offset, None, min_ts);
                mapping.update_range(&topic.name, partition.partition_id, max_offset, None, max_ts);
            }
        }
    }

    Ok(mapping)
}

/// Output the plan in the specified format
fn output_plan(
    plan: &OffsetResetPlan,
    format: OutputFormat,
    bootstrap_servers: &[String],
) -> Result<()> {
    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(plan)?);
        }
        OutputFormat::Csv => {
            println!("group_id,topic,partition,source_offset,target_offset,strategy");
            for group in &plan.groups {
                for partition in &group.partitions {
                    println!(
                        "{},{},{},{},{},{}",
                        group.group_id,
                        partition.topic,
                        partition.partition,
                        partition.source_offset,
                        partition.target_offset,
                        format!("{:?}", plan.strategy)
                    );
                }
            }
        }
        OutputFormat::ShellScript => {
            let executor = OffsetResetExecutor::new_offline(bootstrap_servers.to_vec());
            println!("{}", executor.generate_shell_script(plan));
        }
        OutputFormat::Text => {
            print_plan_text(plan);
        }
    }

    Ok(())
}

/// Print the offset reset plan in human-readable format
fn print_plan_text(plan: &OffsetResetPlan) {
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                            OFFSET RESET PLAN                                 ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!(
        "║ Strategy: {:68} ║",
        format!("{:?}", plan.strategy)
    );
    println!(
        "║ Dry Run: {:69} ║",
        if plan.dry_run { "Yes" } else { "No" }
    );
    println!(
        "║ Groups: {:70} ║",
        plan.groups.len()
    );
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");

    for group in &plan.groups {
        println!("║ Group: {:71} ║", group.group_id);
        println!("╟──────────────────────────────────────────────────────────────────────────────╢");
        println!("║   Topic/Partition          │ Source Offset    │ Target Offset              ║");
        println!("╟────────────────────────────┼──────────────────┼────────────────────────────╢");

        for partition in &group.partitions {
            let tp = format!("{}/{}", partition.topic, partition.partition);
            println!(
                "║   {:24} │ {:16} │ {:26} ║",
                tp, partition.source_offset, partition.target_offset
            );
        }
        println!("╟──────────────────────────────────────────────────────────────────────────────╢");
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════╝");

    if plan.dry_run {
        println!();
        println!("⚠️  This is a DRY RUN. No changes were made.");
        println!("   Run with --execute to apply these changes.");
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

    let (sasl_mechanism, sasl_username, sasl_password) =
        if matches!(security_protocol, SecurityProtocol::SaslSsl | SecurityProtocol::SaslPlaintext)
        {
            (
                Some(SaslMechanism::Plain),
                std::env::var("KAFKA_USERNAME").ok(),
                std::env::var("KAFKA_PASSWORD").ok(),
            )
        } else {
            (None, None, None)
        };

    let ssl_ca_location =
        if matches!(security_protocol, SecurityProtocol::Ssl | SecurityProtocol::SaslSsl) {
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
