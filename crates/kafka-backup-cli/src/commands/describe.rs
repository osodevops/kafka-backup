use anyhow::Result;
use kafka_backup_core::storage::{FilesystemBackend, StorageBackend};
use kafka_backup_core::BackupManifest;
use tracing::info;

/// Describe command output format
pub enum OutputFormat {
    Text,
    Json,
    Yaml,
}

impl OutputFormat {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "json" => Self::Json,
            "yaml" => Self::Yaml,
            _ => Self::Text,
        }
    }
}

pub async fn run(path: &str, backup_id: &str, format: &str) -> Result<()> {
    let storage = FilesystemBackend::new(path.into());
    let output_format = OutputFormat::from_str(format);

    info!("Loading backup manifest: {}", backup_id);
    let manifest_key = format!("{}/manifest.json", backup_id);
    let data = storage.get(&manifest_key).await?;
    let manifest: BackupManifest = serde_json::from_slice(&data)?;

    match output_format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&manifest)?);
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(&manifest)?);
        }
        OutputFormat::Text => {
            print_manifest_text(&manifest);
        }
    }

    Ok(())
}

fn print_manifest_text(manifest: &BackupManifest) {
    println!("╔══════════════════════════════════════════════════════════════════════╗");
    println!("║                         BACKUP MANIFEST                              ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║ Backup ID:      {:55} ║", manifest.backup_id);

    let created_str = chrono::DateTime::from_timestamp_millis(manifest.created_at)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "Unknown".to_string());
    println!("║ Created:        {:55} ║", created_str);

    if let Some(cluster_id) = &manifest.source_cluster_id {
        println!("║ Source Cluster: {:55} ║", cluster_id);
    }
    if !manifest.source_brokers.is_empty() {
        println!(
            "║ Source Brokers: {:55} ║",
            manifest.source_brokers.join(", ")
        );
    }
    println!("║ Compression:    {:55} ║", manifest.compression);
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║                              SUMMARY                                 ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║ Total Topics:   {:55} ║", manifest.topics.len());

    let total_partitions: usize = manifest.topics.iter().map(|t| t.partitions.len()).sum();
    println!("║ Total Partitions: {:53} ║", total_partitions);

    println!("║ Total Segments: {:55} ║", manifest.total_segments());
    println!("║ Total Records:  {:55} ║", manifest.total_records());

    // Calculate total size
    let total_compressed: u64 = manifest
        .topics
        .iter()
        .flat_map(|t| &t.partitions)
        .flat_map(|p| &p.segments)
        .map(|s| s.compressed_size)
        .sum();
    let total_uncompressed: u64 = manifest
        .topics
        .iter()
        .flat_map(|t| &t.partitions)
        .flat_map(|p| &p.segments)
        .map(|s| s.uncompressed_size)
        .sum();

    println!(
        "║ Compressed Size: {:54} ║",
        format_bytes(total_compressed)
    );
    println!(
        "║ Uncompressed Size: {:52} ║",
        format_bytes(total_uncompressed)
    );

    // Calculate compression ratio
    if total_uncompressed > 0 {
        let ratio = (total_compressed as f64 / total_uncompressed as f64) * 100.0;
        println!("║ Compression Ratio: {:51.1}% ║", ratio);
    }

    // Time range
    let min_ts: Option<i64> = manifest
        .topics
        .iter()
        .flat_map(|t| &t.partitions)
        .flat_map(|p| &p.segments)
        .map(|s| s.start_timestamp)
        .min();
    let max_ts: Option<i64> = manifest
        .topics
        .iter()
        .flat_map(|t| &t.partitions)
        .flat_map(|p| &p.segments)
        .map(|s| s.end_timestamp)
        .max();

    if let (Some(min), Some(max)) = (min_ts, max_ts) {
        let min_str = chrono::DateTime::from_timestamp_millis(min)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        let max_str = chrono::DateTime::from_timestamp_millis(max)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
            .unwrap_or_else(|| "Unknown".to_string());
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        println!("║                            TIME RANGE                                ║");
        println!("╠══════════════════════════════════════════════════════════════════════╣");
        println!("║ Earliest: {:61} ║", min_str);
        println!("║ Latest:   {:61} ║", max_str);
    }

    println!("╠══════════════════════════════════════════════════════════════════════╣");
    println!("║                              TOPICS                                  ║");
    println!("╠══════════════════════════════════════════════════════════════════════╣");

    for topic in &manifest.topics {
        let topic_records: i64 = topic
            .partitions
            .iter()
            .flat_map(|p| &p.segments)
            .map(|s| s.record_count)
            .sum();
        let topic_segments: usize = topic.partitions.iter().map(|p| p.segments.len()).sum();

        println!("║                                                                      ║");
        println!("║ Topic: {:64} ║", topic.name);
        println!("║   Partitions: {:57} ║", topic.partitions.len());
        println!("║   Segments:   {:57} ║", topic_segments);
        println!("║   Records:    {:57} ║", topic_records);

        // Per-partition details
        for partition in &topic.partitions {
            let p_records: i64 = partition.segments.iter().map(|s| s.record_count).sum();
            let offset_range = partition
                .segments
                .iter()
                .map(|s| (s.start_offset, s.end_offset))
                .fold((i64::MAX, i64::MIN), |(min, max), (s, e)| {
                    (min.min(s), max.max(e))
                });

            if p_records > 0 {
                println!(
                    "║     P{}: {} records, offsets {}-{} ({} segments) ║",
                    partition.partition_id,
                    p_records,
                    offset_range.0,
                    offset_range.1,
                    partition.segments.len()
                );
            }
        }
    }

    println!("╚══════════════════════════════════════════════════════════════════════╝");
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}
