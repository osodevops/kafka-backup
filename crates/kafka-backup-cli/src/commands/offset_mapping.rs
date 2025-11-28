use anyhow::Result;
use kafka_backup_core::manifest::OffsetMapping;
use kafka_backup_core::storage::{FilesystemBackend, StorageBackend};
use kafka_backup_core::BackupManifest;
use tracing::info;

pub async fn run(path: &str, backup_id: &str, format: &str) -> Result<()> {
    let storage = FilesystemBackend::new(path.into());

    info!("Loading backup manifest: {}", backup_id);
    let manifest_key = format!("{}/manifest.json", backup_id);
    let data = storage.get(&manifest_key).await?;
    let manifest: BackupManifest = serde_json::from_slice(&data)?;

    // Build offset mapping from manifest
    let mut mapping = OffsetMapping::new();

    for topic in &manifest.topics {
        for partition in &topic.partitions {
            // Find offset range from segments
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
                mapping.update_range(
                    &topic.name,
                    partition.partition_id,
                    max_offset,
                    None,
                    max_ts,
                );
            }
        }
    }

    // Output based on format
    match format.to_lowercase().as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&mapping)?);
        }
        "yaml" => {
            println!("{}", serde_yaml::to_string(&mapping)?);
        }
        "csv" => {
            println!("topic,partition,source_first_offset,source_last_offset,first_timestamp,last_timestamp");
            for entry in mapping.sorted_entries() {
                println!(
                    "{},{},{},{},{},{}",
                    entry.topic,
                    entry.partition,
                    entry.source_first_offset,
                    entry.source_last_offset,
                    entry.first_timestamp,
                    entry.last_timestamp
                );
            }
        }
        _ => {
            print_offset_mapping_text(&mapping, backup_id);
        }
    }

    Ok(())
}

fn print_offset_mapping_text(mapping: &OffsetMapping, backup_id: &str) {
    println!("╔══════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                              OFFSET MAPPING REPORT                                   ║");
    println!("╠══════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Backup ID: {:77} ║", backup_id);
    println!("╠══════════════════════════════════════════════════════════════════════════════════════╣");
    println!("║ Topic/Partition    │ Source Offset Range     │ First Timestamp        │ Last Timestamp          ║");
    println!("╠════════════════════╪═════════════════════════╪════════════════════════╪═════════════════════════╣");

    for entry in mapping.sorted_entries() {
        let tp = format!("{}/{}", entry.topic, entry.partition);
        let offset_range = format!("{} - {}", entry.source_first_offset, entry.source_last_offset);

        let first_ts = chrono::DateTime::from_timestamp_millis(entry.first_timestamp)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| entry.first_timestamp.to_string());

        let last_ts = chrono::DateTime::from_timestamp_millis(entry.last_timestamp)
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_else(|| entry.last_timestamp.to_string());

        println!(
            "║ {:18} │ {:23} │ {:22} │ {:23} ║",
            tp, offset_range, first_ts, last_ts
        );
    }

    println!("╚══════════════════════════════════════════════════════════════════════════════════════╝");

    // Print consumer group reset commands
    println!();
    println!("To reset consumer group offsets, use the following commands:");
    println!();

    for entry in mapping.sorted_entries() {
        println!(
            "# Reset to start of backup for {}/{}",
            entry.topic, entry.partition
        );
        println!(
            "kafka-consumer-groups --bootstrap-server <BROKER> \\",
        );
        println!("  --group <GROUP_NAME> \\");
        println!(
            "  --topic {}:{} \\",
            entry.topic, entry.partition
        );
        println!(
            "  --reset-offsets --to-offset {} --execute",
            entry.source_first_offset
        );
        println!();
    }

    println!("# Or reset by timestamp:");
    for entry in mapping.sorted_entries() {
        let ts = chrono::DateTime::from_timestamp_millis(entry.first_timestamp)
            .map(|dt| dt.format("%Y-%m-%dT%H:%M:%S.000").to_string())
            .unwrap_or_else(|| entry.first_timestamp.to_string());

        println!(
            "kafka-consumer-groups --bootstrap-server <BROKER> \\",
        );
        println!("  --group <GROUP_NAME> \\");
        println!(
            "  --topic {}:{} \\",
            entry.topic, entry.partition
        );
        println!(
            "  --reset-offsets --to-datetime {} --execute",
            ts
        );
        println!();
    }
}
