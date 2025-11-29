use anyhow::Result;
use kafka_backup_core::storage::{FilesystemBackend, StorageBackend};
use kafka_backup_core::{BackupManifest, OffsetStore, OffsetStoreConfig, SqliteOffsetStore};
use std::path::PathBuf;
use tracing::info;

pub async fn run(path: &str, backup_id: &str, db_path: Option<&str>) -> Result<()> {
    info!("Getting status for backup: {}", backup_id);

    let storage = FilesystemBackend::new(PathBuf::from(path));

    // Load manifest
    let manifest_key = format!("{}/manifest.json", backup_id);
    let manifest_data = storage.get(&manifest_key).await;

    let manifest: Option<BackupManifest> = match manifest_data {
        Ok(data) => serde_json::from_slice(&data).ok(),
        Err(_) => None,
    };

    // Load offset store if db_path provided
    let offset_store = if let Some(db) = db_path {
        let config = OffsetStoreConfig {
            db_path: PathBuf::from(db),
            ..Default::default()
        };
        SqliteOffsetStore::new(config).await.ok()
    } else {
        // Try default path
        let config = OffsetStoreConfig::default();
        SqliteOffsetStore::new(config).await.ok()
    };

    println!("=== Backup Status: {} ===\n", backup_id);

    // Show manifest info
    if let Some(m) = &manifest {
        println!("Manifest:");
        println!(
            "  Created: {}",
            chrono::DateTime::from_timestamp_millis(m.created_at)
                .map(|dt| dt.to_string())
                .unwrap_or_else(|| "Unknown".to_string())
        );
        if let Some(cluster_id) = &m.source_cluster_id {
            println!("  Source Cluster: {}", cluster_id);
        }

        let total_topics = m.topics.len();
        let total_partitions: usize = m.topics.iter().map(|t| t.partitions.len()).sum();
        let total_segments: usize = m
            .topics
            .iter()
            .flat_map(|t| &t.partitions)
            .map(|p| p.segments.len())
            .sum();
        let total_records: i64 = m
            .topics
            .iter()
            .flat_map(|t| &t.partitions)
            .flat_map(|p| &p.segments)
            .map(|s| s.record_count)
            .sum();
        let total_compressed: u64 = m
            .topics
            .iter()
            .flat_map(|t| &t.partitions)
            .flat_map(|p| &p.segments)
            .map(|s| s.compressed_size)
            .sum();
        let total_uncompressed: u64 = m
            .topics
            .iter()
            .flat_map(|t| &t.partitions)
            .flat_map(|p| &p.segments)
            .map(|s| s.uncompressed_size)
            .sum();

        println!("  Topics: {}", total_topics);
        println!("  Partitions: {}", total_partitions);
        println!("  Segments: {}", total_segments);
        println!("  Total Records: {}", total_records);
        println!(
            "  Size: {} MB compressed ({} MB uncompressed)",
            total_compressed / 1024 / 1024,
            total_uncompressed / 1024 / 1024
        );

        if total_uncompressed > 0 {
            let ratio = total_uncompressed as f64 / total_compressed as f64;
            println!("  Compression Ratio: {:.2}x", ratio);
        }
    } else {
        println!("Manifest: Not found");
    }

    println!();

    // Show offset tracking status
    if let Some(store) = &offset_store {
        println!("Offset Tracking:");

        if let Some(m) = &manifest {
            for topic in &m.topics {
                println!("  {}:", topic.name);
                for partition in &topic.partitions {
                    let tracked_offset = store
                        .get_offset(backup_id, &topic.name, partition.partition_id)
                        .await
                        .ok()
                        .flatten();

                    let last_segment_offset = partition
                        .segments
                        .last()
                        .map(|s| s.end_offset)
                        .unwrap_or(-1);

                    match tracked_offset {
                        Some(offset) => {
                            let status = if offset >= last_segment_offset {
                                "up-to-date"
                            } else {
                                "in-progress"
                            };
                            println!(
                                "    partition-{}: offset {} (segment end: {}) [{}]",
                                partition.partition_id, offset, last_segment_offset, status
                            );
                        }
                        None => {
                            println!(
                                "    partition-{}: no offset tracked (segment end: {})",
                                partition.partition_id, last_segment_offset
                            );
                        }
                    }
                }
            }
        } else {
            // No manifest, try to list all tracked offsets for this backup
            println!("  (No manifest - showing raw offset data if available)");
        }
    } else {
        println!("Offset Tracking: No offset database found");
        println!("  (Use --db-path to specify the offset database location)");
    }

    println!();

    // Show storage info
    println!("Storage:");
    println!("  Path: {}", path);

    let files = storage.list(backup_id).await?;
    let segment_count = files
        .iter()
        .filter(|f| f.ends_with(".bin") || f.ends_with(".zst"))
        .count();
    println!("  Segment Files: {}", segment_count);

    Ok(())
}
