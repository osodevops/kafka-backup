//! CLI commands for offset snapshot and rollback operations.
//!
//! This module provides:
//! - Create offset snapshots before making changes
//! - List available snapshots
//! - Rollback to a previous snapshot
//! - Verify offsets match a snapshot

use anyhow::{Context, Result};
use kafka_backup_core::config::{KafkaConfig, SaslMechanism, SecurityConfig, SecurityProtocol};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::restore::offset_rollback::{
    rollback_offset_reset, snapshot_current_offsets, verify_rollback, OffsetSnapshot,
    OffsetSnapshotStorage, RollbackResult, RollbackStatus, StorageBackendSnapshotStore,
    VerificationResult,
};
use kafka_backup_core::storage::{FilesystemBackend, StorageBackend};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

/// Output format for rollback reports
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

/// Create a snapshot of current consumer group offsets
pub async fn create_snapshot(
    path: &str,
    consumer_groups: &[String],
    bootstrap_servers: &[String],
    description: Option<&str>,
    security_protocol: Option<&str>,
    format: OutputFormat,
) -> Result<()> {
    // Create storage backend and snapshot store
    let backend: Arc<dyn StorageBackend> = Arc::new(FilesystemBackend::new(path.into()));
    let snapshot_store = StorageBackendSnapshotStore::new(backend);

    // Create Kafka client
    let kafka_config = KafkaConfig {
        bootstrap_servers: bootstrap_servers.to_vec(),
        security: parse_security_config(security_protocol),
        topics: Default::default(),
    };

    let client = KafkaClient::new(kafka_config);
    client
        .connect()
        .await
        .context("Failed to connect to Kafka")?;

    println!(
        "Creating offset snapshot for {} consumer groups...",
        consumer_groups.len()
    );

    // Create snapshot
    let mut snapshot =
        snapshot_current_offsets(&client, consumer_groups, bootstrap_servers.to_vec())
            .await
            .context("Failed to create snapshot")?;

    // Add description if provided
    if let Some(desc) = description {
        snapshot.description = Some(desc.to_string());
    }

    // Save snapshot
    let snapshot_id = snapshot_store
        .save_snapshot(&snapshot)
        .await
        .context("Failed to save snapshot")?;

    // Output result
    match format {
        OutputFormat::Json => {
            let metadata = snapshot.to_metadata();
            println!("{}", serde_json::to_string_pretty(&metadata)?);
        }
        OutputFormat::Text => {
            println!();
            println!("Snapshot created successfully!");
            println!("  Snapshot ID: {}", snapshot_id);
            println!("  Created at:  {}", snapshot.created_at);
            println!("  Groups:      {}", snapshot.group_offsets.len());
            println!("  Offsets:     {}", snapshot.total_offsets());
            if let Some(desc) = &snapshot.description {
                println!("  Description: {}", desc);
            }
            println!();
            println!("To rollback to this snapshot, run:");
            println!(
                "  kafka-backup offset-rollback rollback --path {} --snapshot-id {}",
                path, snapshot_id
            );
        }
    }

    Ok(())
}

/// List all available offset snapshots
pub async fn list_snapshots(path: &str, format: OutputFormat) -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(FilesystemBackend::new(path.into()));
    let snapshot_store = StorageBackendSnapshotStore::new(backend);

    let snapshots = snapshot_store
        .list_snapshots()
        .await
        .context("Failed to list snapshots")?;

    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&snapshots)?);
        }
        OutputFormat::Text => {
            if snapshots.is_empty() {
                println!("No offset snapshots found.");
                return Ok(());
            }

            println!("Available offset snapshots:");
            println!();
            println!(
                "{:<40} {:<24} {:>8} {:>10}  DESCRIPTION",
                "SNAPSHOT ID", "CREATED AT", "GROUPS", "OFFSETS"
            );
            println!("{}", "-".repeat(100));

            for snap in &snapshots {
                let desc = snap.description.as_deref().unwrap_or("-");
                let desc_truncated = if desc.len() > 30 {
                    format!("{}...", &desc[..27])
                } else {
                    desc.to_string()
                };

                println!(
                    "{:<40} {:<24} {:>8} {:>10}  {}",
                    snap.snapshot_id,
                    snap.created_at.format("%Y-%m-%d %H:%M:%S UTC"),
                    snap.group_count,
                    snap.offset_count,
                    desc_truncated
                );
            }

            println!();
            println!("Total: {} snapshots", snapshots.len());
        }
    }

    Ok(())
}

/// Show details of a specific snapshot
pub async fn show_snapshot(path: &str, snapshot_id: &str, format: OutputFormat) -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(FilesystemBackend::new(path.into()));
    let snapshot_store = StorageBackendSnapshotStore::new(backend);

    let snapshot = snapshot_store
        .load_snapshot(snapshot_id)
        .await
        .context("Failed to load snapshot")?;

    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        OutputFormat::Text => {
            print_snapshot_details(&snapshot);
        }
    }

    Ok(())
}

/// Rollback offsets to a previous snapshot
pub async fn execute_rollback(
    path: &str,
    snapshot_id: &str,
    bootstrap_servers: &[String],
    security_protocol: Option<&str>,
    verify: bool,
    format: OutputFormat,
) -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(FilesystemBackend::new(path.into()));
    let snapshot_store = StorageBackendSnapshotStore::new(backend);

    // Load snapshot
    let snapshot = snapshot_store
        .load_snapshot(snapshot_id)
        .await
        .context("Failed to load snapshot")?;

    println!("Rolling back to snapshot: {}", snapshot_id);
    println!(
        "  Created: {}",
        snapshot.created_at.format("%Y-%m-%d %H:%M:%S UTC")
    );
    println!("  Groups: {}", snapshot.group_offsets.len());
    println!("  Offsets: {}", snapshot.total_offsets());
    println!();

    // Create Kafka client
    let kafka_config = KafkaConfig {
        bootstrap_servers: bootstrap_servers.to_vec(),
        security: parse_security_config(security_protocol),
        topics: Default::default(),
    };

    let client = KafkaClient::new(kafka_config);
    client
        .connect()
        .await
        .context("Failed to connect to Kafka")?;

    // Execute rollback
    let result = rollback_offset_reset(&client, &snapshot)
        .await
        .context("Rollback failed")?;

    // Optionally verify
    let verification = if verify {
        info!("Verifying rollback...");
        Some(
            verify_rollback(&client, &snapshot)
                .await
                .context("Verification failed")?,
        )
    } else {
        None
    };

    // Check status before outputting (since result may be moved in JSON case)
    let failed = result.status == RollbackStatus::Failed;

    // Output result
    match format {
        OutputFormat::Json => {
            #[derive(serde::Serialize)]
            struct RollbackOutput {
                result: RollbackResult,
                verification: Option<VerificationResult>,
            }
            let output = RollbackOutput {
                result,
                verification,
            };
            println!("{}", serde_json::to_string_pretty(&output)?);
        }
        OutputFormat::Text => {
            print_rollback_result(&result);
            if let Some(ref ver) = verification {
                println!();
                print_verification_result(ver);
            }
        }
    }

    if failed {
        anyhow::bail!("Rollback failed");
    }

    Ok(())
}

/// Verify current offsets match a snapshot
pub async fn verify_snapshot(
    path: &str,
    snapshot_id: &str,
    bootstrap_servers: &[String],
    security_protocol: Option<&str>,
    format: OutputFormat,
) -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(FilesystemBackend::new(path.into()));
    let snapshot_store = StorageBackendSnapshotStore::new(backend);

    // Load snapshot
    let snapshot = snapshot_store
        .load_snapshot(snapshot_id)
        .await
        .context("Failed to load snapshot")?;

    // Create Kafka client
    let kafka_config = KafkaConfig {
        bootstrap_servers: bootstrap_servers.to_vec(),
        security: parse_security_config(security_protocol),
        topics: Default::default(),
    };

    let client = KafkaClient::new(kafka_config);
    client
        .connect()
        .await
        .context("Failed to connect to Kafka")?;

    println!("Verifying offsets against snapshot: {}", snapshot_id);

    let result = verify_rollback(&client, &snapshot)
        .await
        .context("Verification failed")?;

    match format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
        OutputFormat::Text => {
            print_verification_result(&result);
        }
    }

    if !result.verified {
        anyhow::bail!("Verification failed - offsets do not match snapshot");
    }

    Ok(())
}

/// Delete a snapshot
pub async fn delete_snapshot(path: &str, snapshot_id: &str) -> Result<()> {
    let backend: Arc<dyn StorageBackend> = Arc::new(FilesystemBackend::new(path.into()));
    let snapshot_store = StorageBackendSnapshotStore::new(backend);

    // Verify snapshot exists
    if !snapshot_store.exists(snapshot_id).await? {
        anyhow::bail!("Snapshot {} not found", snapshot_id);
    }

    snapshot_store
        .delete_snapshot(snapshot_id)
        .await
        .context("Failed to delete snapshot")?;

    println!("Snapshot {} deleted successfully.", snapshot_id);
    Ok(())
}

// ============================================================================
// Helper Functions
// ============================================================================

fn print_snapshot_details(snapshot: &OffsetSnapshot) {
    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                           OFFSET SNAPSHOT DETAILS                            ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Snapshot ID: {:<64} ║", snapshot.snapshot_id);
    println!(
        "║ Created:     {:<64} ║",
        snapshot.created_at.format("%Y-%m-%d %H:%M:%S UTC")
    );
    println!("║ Groups:      {:<64} ║", snapshot.group_offsets.len());
    println!("║ Offsets:     {:<64} ║", snapshot.total_offsets());
    if let Some(ref desc) = snapshot.description {
        println!("║ Description: {:<64} ║", desc);
    }
    if let Some(ref restore_id) = snapshot.restore_id {
        println!("║ Restore ID:  {:<64} ║", restore_id);
    }
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Consumer Groups                                                              ║");
    println!("╟──────────────────────────────────────────────────────────────────────────────╢");

    for (group_id, state) in &snapshot.group_offsets {
        println!("║ Group: {:<71} ║", group_id);
        println!("║   Partitions: {:<63} ║", state.partition_count);

        for (topic, partitions) in &state.offsets {
            for (partition, offset_state) in partitions {
                println!(
                    "║     {}:{} -> offset {}{}",
                    topic,
                    partition,
                    offset_state.offset,
                    " ".repeat(
                        78 - 12 - topic.len() - 10 - format!("{}", offset_state.offset).len()
                    ) + "║"
                );
            }
        }
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════╝");
}

fn print_rollback_result(result: &RollbackResult) {
    let status_icon = match result.status {
        RollbackStatus::Success => "✓",
        RollbackStatus::PartialSuccess => "⚠",
        RollbackStatus::Failed => "✗",
    };

    let status_text = match result.status {
        RollbackStatus::Success => "SUCCESS",
        RollbackStatus::PartialSuccess => "PARTIAL SUCCESS",
        RollbackStatus::Failed => "FAILED",
    };

    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                             ROLLBACK RESULT                                  ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Status: {} {:<66} ║", status_icon, status_text);
    println!("╟──────────────────────────────────────────────────────────────────────────────╢");
    println!(
        "║   Groups Rolled Back: {:<55} ║",
        result.groups_rolled_back
    );
    println!("║   Groups Failed:      {:<55} ║", result.groups_failed);
    println!("║   Offsets Restored:   {:<55} ║", result.offsets_restored);
    println!("║   Duration:           {:<52} ms ║", result.duration_ms);
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");

    if !result.errors.is_empty() {
        println!();
        println!("Errors:");
        for error in &result.errors {
            println!("  - {}", error);
        }
    }
}

fn print_verification_result(result: &VerificationResult) {
    let status_icon = if result.verified { "✓" } else { "✗" };
    let status_text = if result.verified {
        "VERIFIED - All offsets match snapshot"
    } else {
        "MISMATCH - Some offsets differ from snapshot"
    };

    println!("╔══════════════════════════════════════════════════════════════════════════════╗");
    println!("║                           VERIFICATION RESULT                                ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════╣");
    println!("║ {} {:<74} ║", status_icon, status_text);
    println!("╟──────────────────────────────────────────────────────────────────────────────╢");
    println!(
        "║   Groups Verified:   {:<56} ║",
        result.groups_verified.len()
    );
    println!(
        "║   Groups Mismatched: {:<56} ║",
        result.groups_mismatched.len()
    );
    println!("╚══════════════════════════════════════════════════════════════════════════════╝");

    if !result.mismatches.is_empty() {
        println!();
        println!("Mismatches:");
        for m in &result.mismatches {
            println!(
                "  - {}:{}:{} expected {} but found {}",
                m.group_id, m.topic, m.partition, m.expected_offset, m.actual_offset
            );
        }
    }
}

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
