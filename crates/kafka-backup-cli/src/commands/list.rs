use anyhow::Result;
use kafka_backup_core::storage::{FilesystemBackend, StorageBackend};
use kafka_backup_core::BackupManifest;
use tracing::info;

pub async fn run(path: &str, backup_id: Option<&str>) -> Result<()> {
    let storage = FilesystemBackend::new(path.into());

    match backup_id {
        Some(id) => {
            // Show details for a specific backup
            info!("Loading backup manifest: {}", id);
            let manifest_key = format!("{}/manifest.json", id);
            let data = storage.get(&manifest_key).await?;
            let manifest: BackupManifest = serde_json::from_slice(&data)?;

            println!("Backup ID: {}", manifest.backup_id);
            println!(
                "Created: {}",
                chrono::DateTime::from_timestamp_millis(manifest.created_at)
                    .map(|dt| dt.to_string())
                    .unwrap_or_else(|| "Unknown".to_string())
            );
            if let Some(cluster_id) = &manifest.source_cluster_id {
                println!("Source Cluster: {}", cluster_id);
            }
            println!("\nTopics:");
            for topic in &manifest.topics {
                let total_records: i64 = topic
                    .partitions
                    .iter()
                    .flat_map(|p| &p.segments)
                    .map(|s| s.record_count)
                    .sum();
                let total_segments: usize =
                    topic.partitions.iter().map(|p| p.segments.len()).sum();
                println!(
                    "  - {} ({} partitions, {} segments, {} records)",
                    topic.name,
                    topic.partitions.len(),
                    total_segments,
                    total_records
                );
            }
        }
        None => {
            // List all backups
            info!("Listing backups in: {}", path);
            let entries = storage.list("").await?;

            // Find all manifest.json files to identify backups
            let backup_ids: Vec<_> = entries
                .iter()
                .filter(|e| e.ends_with("/manifest.json"))
                .map(|e| e.trim_end_matches("/manifest.json"))
                .collect();

            if backup_ids.is_empty() {
                println!("No backups found in {}", path);
            } else {
                println!("Available backups:");
                for id in backup_ids {
                    println!("  - {}", id);
                }
            }
        }
    }

    Ok(())
}
