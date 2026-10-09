//! CLI commands for offset snapshot and rollback operations.
//!
//! This module provides:
//! - Create offset snapshots before making changes
//! - List available snapshots
//! - Rollback to a previous snapshot
//! - Verify offsets match a snapshot

use anyhow::{Context, Result};
use kafka_backup_core::config::{KafkaConfig, SecurityConfig};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::restore::offset_rollback::{
    rollback_offset_reset, snapshot_current_offsets, verify_rollback, OffsetSnapshot,
    OffsetSnapshotStorage, RollbackResult, RollbackStatus, StorageBackendSnapshotStore,
    VerificationResult,
};
use tracing::info;

use super::storage_path::backend_from_path;

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

/// Snapshot store at `--path`: a local directory or a storage URL
/// (`s3://`, `file://`, `azure://`, `gcs://`) (#174).
fn open_snapshot_store(path: &str) -> Result<StorageBackendSnapshotStore> {
    Ok(StorageBackendSnapshotStore::new(backend_from_path(path)?))
}

/// Create a snapshot of current consumer group offsets
pub async fn create_snapshot(
    path: &str,
    consumer_groups: &[String],
    bootstrap_servers: &[String],
    description: Option<&str>,
    security: SecurityConfig,
    format: OutputFormat,
) -> Result<()> {
    // Resolve storage before touching Kafka so a bad --path fails fast
    let snapshot_store = open_snapshot_store(path)?;

    // Create Kafka client
    let kafka_config = KafkaConfig {
        bootstrap_servers: bootstrap_servers.to_vec(),
        security,
        topics: Default::default(),
        connection: Default::default(),
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
                "  kafka-backup offset-rollback rollback --path '{}' --snapshot-id {}",
                path, snapshot_id
            );
        }
    }

    Ok(())
}

/// List all available offset snapshots
pub async fn list_snapshots(path: &str, format: OutputFormat) -> Result<()> {
    let snapshot_store = open_snapshot_store(path)?;

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
                println!("No offset snapshots found at {}.", path);
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
    let snapshot_store = open_snapshot_store(path)?;

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
    security: SecurityConfig,
    verify: bool,
    format: OutputFormat,
) -> Result<()> {
    let snapshot_store = open_snapshot_store(path)?;

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
        security,
        topics: Default::default(),
        connection: Default::default(),
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
    let mismatched = verification.as_ref().is_some_and(|v| !v.verified);

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
    if mismatched {
        anyhow::bail!("Verification failed - offsets do not match snapshot");
    }

    Ok(())
}

/// Verify current offsets match a snapshot
pub async fn verify_snapshot(
    path: &str,
    snapshot_id: &str,
    bootstrap_servers: &[String],
    security: SecurityConfig,
    format: OutputFormat,
) -> Result<()> {
    let snapshot_store = open_snapshot_store(path)?;

    // Load snapshot
    let snapshot = snapshot_store
        .load_snapshot(snapshot_id)
        .await
        .context("Failed to load snapshot")?;

    // Create Kafka client
    let kafka_config = KafkaConfig {
        bootstrap_servers: bootstrap_servers.to_vec(),
        security,
        topics: Default::default(),
        connection: Default::default(),
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
    let snapshot_store = open_snapshot_store(path)?;

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

/// Width between the `║` borders of the text boxes.
const BOX_INNER_WIDTH: usize = 78;

/// One box row: `content` padded to the box width. Content that doesn't fit
/// (long topic or group names) runs past the right border instead of being
/// cut off (#223).
fn box_row(content: &str) -> String {
    format!("║{content:<BOX_INNER_WIDTH$}║")
}

fn snapshot_details_lines(snapshot: &OffsetSnapshot) -> Vec<String> {
    let top = format!("╔{}╗", "═".repeat(BOX_INNER_WIDTH));
    let rule = format!("╠{}╣", "═".repeat(BOX_INNER_WIDTH));
    let thin = format!("╟{}╢", "─".repeat(BOX_INNER_WIDTH));
    let bottom = format!("╚{}╝", "═".repeat(BOX_INNER_WIDTH));

    let mut lines = vec![
        top,
        box_row(&format!("{:^BOX_INNER_WIDTH$}", "OFFSET SNAPSHOT DETAILS")),
        rule.clone(),
        box_row(&format!(" Snapshot ID: {}", snapshot.snapshot_id)),
        box_row(&format!(
            " Created:     {}",
            snapshot.created_at.format("%Y-%m-%d %H:%M:%S UTC")
        )),
        box_row(&format!(" Groups:      {}", snapshot.group_offsets.len())),
        box_row(&format!(" Offsets:     {}", snapshot.total_offsets())),
    ];
    if let Some(ref desc) = snapshot.description {
        lines.push(box_row(&format!(" Description: {desc}")));
    }
    if let Some(ref restore_id) = snapshot.restore_id {
        lines.push(box_row(&format!(" Restore ID:  {restore_id}")));
    }
    lines.push(rule);
    lines.push(box_row(" Consumer Groups"));
    lines.push(thin);

    let mut groups: Vec<_> = snapshot.group_offsets.iter().collect();
    groups.sort_by_key(|(group_id, _)| *group_id);
    for (group_id, state) in groups {
        lines.push(box_row(&format!(" Group: {group_id}")));
        lines.push(box_row(&format!(
            "   Partitions: {}",
            state.partition_count
        )));

        let mut topics: Vec<_> = state.offsets.iter().collect();
        topics.sort_by_key(|(topic, _)| *topic);
        for (topic, partitions) in topics {
            let mut partitions: Vec<_> = partitions.iter().collect();
            partitions.sort_by_key(|(partition, _)| **partition);
            for (partition, offset_state) in partitions {
                lines.push(box_row(&format!(
                    "     {topic}:{partition} -> offset {}",
                    offset_state.offset
                )));
            }
        }
    }

    lines.push(bottom);
    lines
}

fn print_snapshot_details(snapshot: &OffsetSnapshot) {
    for line in snapshot_details_lines(snapshot) {
        println!("{line}");
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_backup_core::restore::offset_rollback::{GroupOffsetState, PartitionOffsetState};
    use std::collections::HashMap;

    fn snapshot(topic: &str, offset: i64) -> OffsetSnapshot {
        let partition = PartitionOffsetState {
            offset,
            metadata: None,
            timestamp: None,
        };
        let group = GroupOffsetState {
            group_id: "g".to_string(),
            offsets: HashMap::from([(topic.to_string(), HashMap::from([(0, partition)]))]),
            partition_count: 1,
        };
        OffsetSnapshot {
            snapshot_id: "s".to_string(),
            created_at: chrono::Utc::now(),
            group_offsets: HashMap::from([("g".to_string(), group)]),
            restore_id: None,
            cluster_id: None,
            bootstrap_servers: vec![],
            description: None,
        }
    }

    /// #223: every topic length up to Kafka's 249-character limit, with
    /// negative (no committed offset), small and maximum offsets.
    #[test]
    fn snapshot_details_never_panic_and_fit_when_possible() {
        for len in 0..=249 {
            for offset in [-1, 0, 42, i64::MAX] {
                let topic = "t".repeat(len);
                let lines = snapshot_details_lines(&snapshot(&topic, offset));
                let row = lines
                    .iter()
                    .find(|l| l.contains(" -> offset "))
                    .expect("offset row");
                assert!(row.contains(&format!("{topic}:0 -> offset {offset}")));
                assert!(row.starts_with('║') && row.ends_with('║'), "{row}");
                let content = row.chars().count() - 2;
                if format!("     {topic}:0 -> offset {offset}").len() <= BOX_INNER_WIDTH {
                    assert_eq!(content, BOX_INNER_WIDTH, "short row not padded: {row}");
                }
            }
        }
    }
}
