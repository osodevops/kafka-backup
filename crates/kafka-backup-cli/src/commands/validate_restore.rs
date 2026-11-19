use anyhow::Result;
use kafka_backup_core::schema_registry::run::{self, RestoreScope};
use kafka_backup_core::{Config, RestoreEngine};
use tracing::info;

pub async fn run(config_path: &str, format: &str, schema_only: bool) -> Result<()> {
    // Load configuration
    let config_content = std::fs::read_to_string(config_path)?;
    let config_content = super::config::expand_env_vars(&config_content);
    let mut config: Config = super::config::parse_config(&config_content)?;
    // Same OAuth validation and wiring as the real restore, so a wrong
    // provider or binding fails validation instead of passing.
    super::oauth::apply(&mut config)?;

    // Force dry-run mode, for the records and for the Schema Registry.
    run::normalize_dry_run(&mut config, true);

    info!("Validating restore configuration from: {}", config_path);

    // The Schema Registry plan: what would be registered, skipped,
    // rewritten or in conflict. Nothing is changed and no report is written.
    let scope = if schema_only {
        RestoreScope::SchemaOnly
    } else {
        RestoreScope::BeforeRecords
    };
    let registry = super::schema_registry::preview(&config, scope).await?;
    let registry_error = registry
        .as_ref()
        .and_then(|outcome| outcome.error.as_ref())
        .map(|e| format!("Schema Registry: {e}"));

    if schema_only {
        let outcome = registry.expect("--schema-only always has a plan");
        let value = serde_json::json!({
            "valid": registry_error.is_none(),
            "backup_id": config.backup_id,
            "schema_registry": outcome.report,
        });
        match format.to_lowercase().as_str() {
            "json" => println!("{}", serde_json::to_string_pretty(&value)?),
            "yaml" => println!("{}", serde_yaml::to_string(&value)?),
            _ => {
                super::schema_registry::print_report(&outcome.report, "");
                println!(
                    "Validation: {}",
                    if registry_error.is_none() {
                        "PASSED"
                    } else {
                        "FAILED"
                    }
                );
            }
        }
        if let Some(e) = registry_error {
            anyhow::bail!("{e}");
        }
        return Ok(());
    }

    // Create engine and run dry-run
    let engine = RestoreEngine::new(config)?;
    let mut report = engine.dry_run().await?;
    if let Some(e) = &registry_error {
        report.errors.push(e.clone());
        report.valid = false;
    }

    // Output based on format
    match format.to_lowercase().as_str() {
        fmt @ ("json" | "yaml") => {
            // The dry-run report gains a `schema_registry` key only when a
            // registry is configured; otherwise the output is unchanged.
            let mut value = serde_json::to_value(&report)?;
            if let Some(outcome) = &registry {
                value["schema_registry"] = serde_json::to_value(&outcome.report)?;
            }
            if fmt == "json" {
                println!("{}", serde_json::to_string_pretty(&value)?);
            } else {
                println!("{}", serde_yaml::to_string(&value)?);
            }
        }
        _ => {
            if let Some(outcome) = &registry {
                super::schema_registry::print_report(&outcome.report, "");
            }
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

    // Phase 1 header preflight
    if let Some(ref preflight) = report.header_preflight {
        let mut lines = vec![BoxLine::Centered("HEADER PREFLIGHT (PHASE 1)".into())];
        lines.push(BoxLine::Left(kv(
            "Verdict",
            if preflight.passed {
                "✓ PASSED"
            } else {
                "✗ FAILED"
            },
        )));
        lines.push(BoxLine::Left(kv("Mode", &preflight.mode)));
        lines.push(BoxLine::Left(kv(
            "Offset recovery",
            &format!("requested: {}", preflight.offset_recovery_requested),
        )));
        lines.push(BoxLine::Left(kv(
            "Records scanned",
            &preflight.records_scanned_total.to_string(),
        )));
        for p in &preflight.partitions {
            lines.push(BoxLine::Left(format!(
                "{}/{}: {} ({}/{} records covered, {} segments)",
                p.topic,
                p.partition,
                p.state,
                p.records_with_required_headers,
                p.records_scanned,
                p.segments_selected
            )));
        }
        if let Some(ref snapshot) = preflight.consumer_group_snapshot {
            lines.push(BoxLine::Left(format!(
                "consumer-groups snapshot: {} ({} groups, {} offsets)",
                snapshot.state, snapshot.groups, snapshot.offsets
            )));
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
