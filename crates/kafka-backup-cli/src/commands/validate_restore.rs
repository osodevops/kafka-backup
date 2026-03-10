use anyhow::Result;
use kafka_backup_core::{Config, RestoreEngine};
use tracing::info;

pub async fn run(config_path: &str, format: &str) -> Result<()> {
    // Load configuration
    let config_content = std::fs::read_to_string(config_path)?;
    let config_content = super::config::expand_env_vars(&config_content);
    let mut config: Config = serde_yaml::from_str(&config_content)?;

    // Force dry-run mode
    if let Some(ref mut restore) = config.restore {
        restore.dry_run = true;
    } else {
        config.restore = Some(kafka_backup_core::config::RestoreOptions {
            dry_run: true,
            ..Default::default()
        });
    }

    info!("Validating restore configuration from: {}", config_path);

    // Create engine and run dry-run
    let engine = RestoreEngine::new(config)?;
    let report = engine.dry_run().await?;

    // Output based on format
    match format.to_lowercase().as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        "yaml" => {
            println!("{}", serde_yaml::to_string(&report)?);
        }
        _ => {
            print_validation_report(&report);
        }
    }

    // Exit with error code if validation failed
    if !report.valid || !report.errors.is_empty() {
        std::process::exit(1);
    }

    Ok(())
}

/// A line in the box report, either left-aligned or centered
enum BoxLine {
    Left(String),
    Centered(String),
    Separator,
}

impl BoxLine {
    fn content_width(&self) -> usize {
        match self {
            BoxLine::Left(s) => s.chars().count(),
            BoxLine::Centered(s) => s.chars().count(),
            BoxLine::Separator => 0,
        }
    }
}

fn print_validation_report(report: &kafka_backup_core::manifest::DryRunReport) {
    // Collect all content lines first, then size the box to fit
    let status = if report.valid && report.errors.is_empty() {
        "✓ VALID"
    } else {
        "✗ INVALID"
    };

    let mut sections: Vec<Vec<BoxLine>> = Vec::new();

    // Header
    sections.push(vec![BoxLine::Centered("RESTORE VALIDATION REPORT".into())]);

    // Status
    sections.push(vec![
        BoxLine::Left(kv("Status", status)),
        BoxLine::Left(kv("Backup ID", &report.backup_id)),
    ]);

    // Summary
    {
        let mut lines = vec![BoxLine::Centered("RESTORE SUMMARY".into())];
        lines.push(BoxLine::Left(kv(
            "Topics to restore",
            &report.topics_to_restore.len().to_string(),
        )));
        lines.push(BoxLine::Left(kv(
            "Segments to process",
            &report.segments_to_process.to_string(),
        )));
        lines.push(BoxLine::Left(kv(
            "Records to restore",
            &report.records_to_restore.to_string(),
        )));
        lines.push(BoxLine::Left(kv(
            "Bytes to restore",
            &format_bytes(report.bytes_to_restore),
        )));
        sections.push(lines);
    }

    // Time range
    if let Some((start, end)) = report.time_range {
        let fmt = |ts: i64| {
            chrono::DateTime::from_timestamp_millis(ts)
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
                .unwrap_or_else(|| "Unknown".to_string())
        };
        sections.push(vec![
            BoxLine::Centered("TIME RANGE".into()),
            BoxLine::Left(format!("From: {}", fmt(start))),
            BoxLine::Left(format!("To:   {}", fmt(end))),
        ]);
    }

    // Topics
    if !report.topics_to_restore.is_empty() {
        let mut lines = vec![BoxLine::Centered("TOPICS TO RESTORE".into())];
        for (idx, topic) in report.topics_to_restore.iter().enumerate() {
            if idx > 0 {
                lines.push(BoxLine::Separator);
            }
            lines.push(BoxLine::Left(format!(
                "{} -> {}",
                topic.source_topic, topic.target_topic
            )));

            if let Some(ref repart) = topic.repartitioning {
                lines.push(BoxLine::Left(format!(
                    "  Repartitioning: {} partitions -> {} partitions ({} strategy)",
                    repart.source_partitions, repart.target_partitions, repart.strategy
                )));
            }

            for p in &topic.partitions {
                lines.push(BoxLine::Left(format!(
                    "  P{} -> P{}: {} records, offsets {}-{} ({} segments)",
                    p.source_partition,
                    p.target_partition,
                    p.records,
                    p.offset_range.0,
                    p.offset_range.1,
                    p.segments
                )));
            }
        }
        sections.push(lines);
    }

    // Consumer offset actions
    if !report.consumer_offset_actions.is_empty() {
        let mut lines = vec![BoxLine::Centered("CONSUMER OFFSET ACTIONS".into())];
        for action in &report.consumer_offset_actions {
            lines.push(BoxLine::Left(format!("* {}", action)));
        }
        sections.push(lines);
    }

    // Errors
    if !report.errors.is_empty() {
        let mut lines = vec![BoxLine::Centered("ERRORS".into())];
        for error in &report.errors {
            lines.push(BoxLine::Left(format!("✗ {}", error)));
        }
        sections.push(lines);
    }

    // Warnings
    if !report.warnings.is_empty() {
        let mut lines = vec![BoxLine::Centered("WARNINGS".into())];
        for warning in &report.warnings {
            lines.push(BoxLine::Left(format!("⚠ {}", warning)));
        }
        sections.push(lines);
    }

    // Determine box width from longest visible content line (min 60)
    let max_content = sections
        .iter()
        .flat_map(|s| s.iter())
        .map(|l| l.content_width())
        .max()
        .unwrap_or(0);
    let w = max_content.max(60) + 4; // 2 spaces padding on each side

    // Print
    println!("╔{}╗", "═".repeat(w));
    for (i, section) in sections.iter().enumerate() {
        if i > 0 {
            println!("╠{}╣", "═".repeat(w));
        }
        for line in section {
            match line {
                BoxLine::Centered(text) => {
                    let text_len = text.chars().count();
                    let left = (w - text_len) / 2;
                    let right = w - text_len - left;
                    println!("║{}{}{}║", " ".repeat(left), text, " ".repeat(right));
                }
                BoxLine::Left(text) => {
                    let chars = text.chars().count();
                    let pad = w - chars - 2;
                    println!("║ {}{}║", text, " ".repeat(pad + 1));
                }
                BoxLine::Separator => {
                    println!("║ {}║", "─".repeat(w - 2));
                }
            }
        }
    }
    println!("╚{}╝", "═".repeat(w));
}

/// Format a key-value line with consistent alignment
fn kv(key: &str, value: &str) -> String {
    format!("{:<22} {}", format!("{}:", key), value)
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
