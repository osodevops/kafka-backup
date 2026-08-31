//! `kafka-backup prune` — delete aged/oversized segments from a backup set,
//! safely (issue #169). Plan-only by default; `--execute` performs it.

use anyhow::{bail, Context, Result};
use kafka_backup_core::backup::prune::{self, PruneCriteria, PrunePlan};
use kafka_backup_core::manifest::{BackupManifest, PruneReason};
use kafka_backup_core::offset_store::{OffsetStore, OffsetStoreConfig, SqliteOffsetStore};
use kafka_backup_core::storage::{create_backend, StorageBackend};
use kafka_backup_core::util::parse_duration;
use std::collections::HashMap;
use std::sync::Arc;

use super::storage_path::backend_from_path;

#[allow(clippy::too_many_arguments)]
pub async fn run(
    config: Option<&str>,
    path: Option<&str>,
    backup_id: Option<&str>,
    older_than: Option<&str>,
    before: Option<&str>,
    keep_segments: usize,
    max_total_bytes: Option<u64>,
    execute: bool,
    force: bool,
    format: &str,
) -> Result<()> {
    let (storage, backup_id): (Arc<dyn StorageBackend>, String) = match (config, path, backup_id) {
        (Some(config_path), None, None) => {
            let content = tokio::fs::read_to_string(config_path).await?;
            let content = super::config::expand_env_vars(&content);
            let cfg = super::config::parse_config(&content)?;
            (create_backend(&cfg.storage)?, cfg.backup_id)
        }
        (None, Some(path), Some(id)) => (backend_from_path(path)?, id.to_string()),
        _ => bail!("pass either --config, or --path together with --backup-id"),
    };

    let cutoff = match (older_than, before) {
        (Some(raw), None) => {
            let d = parse_duration(raw).context("--older-than")?;
            Some(chrono::Utc::now().timestamp_millis() - d.as_millis() as i64)
        }
        (None, Some(raw)) => Some(parse_rfc3339_or_epoch_ms(raw).context("--before")?),
        (None, None) => None,
        (Some(_), Some(_)) => bail!("--older-than and --before are mutually exclusive"),
    };
    if cutoff.is_none() && max_total_bytes.is_none() {
        bail!("nothing to prune: pass --older-than/--before and/or --max-total-bytes");
    }

    // Load manifest.
    let manifest_key = format!("{}/manifest.json", backup_id);
    let manifest_bytes = storage
        .get(&manifest_key)
        .await
        .with_context(|| format!("loading {}", manifest_key))?;
    let manifest: BackupManifest = serde_json::from_slice(&manifest_bytes)
        .with_context(|| format!("parsing {}", manifest_key))?;

    // Resume positions + liveness from the remote offsets.db, if present.
    let offsets_key = format!("{}/offsets.db", backup_id);
    let mut resume: HashMap<(String, i32), i64> = HashMap::new();
    let mut newest_checkpoint_ms: Option<i64> = None;
    if storage.exists(&offsets_key).await.unwrap_or(false) {
        let tmp = std::env::temp_dir().join(format!(
            "kafka-backup-prune-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_millis()
        ));
        tokio::fs::create_dir_all(&tmp).await?;
        let store = SqliteOffsetStore::new(OffsetStoreConfig {
            db_path: tmp.join("offsets.db"),
            s3_key: Some(offsets_key.clone()),
            checkpoint_interval_secs: 5,
            sync_interval_secs: 30,
        })
        .await?;
        store
            .try_load_from_storage(storage.as_ref(), &offsets_key)
            .await?;
        let offsets = store.get_all_offsets(&backup_id).await?;
        newest_checkpoint_ms = offsets.iter().map(|o| o.checkpoint_ts).max();
        resume = offsets
            .into_iter()
            .map(|o| ((o.topic, o.partition), o.last_offset + 1))
            .collect();
        let _ = tokio::fs::remove_dir_all(&tmp).await;
    }

    // Refuse when a run looks live: a checkpoint younger than 2 minutes
    // means an active writer could resurrect the manifest we rewrite.
    if let Some(ts) = newest_checkpoint_ms {
        let age_ms = chrono::Utc::now().timestamp_millis() - ts;
        if age_ms < 120_000 && !force {
            bail!(
                "a backup run for '{}' looks live (offset checkpoint {}s old); \
                 re-run when it finishes, or pass --force",
                backup_id,
                age_ms / 1000
            );
        }
    }

    let criteria = PruneCriteria {
        older_than: cutoff,
        max_total_bytes,
        keep_segments: keep_segments.max(1),
    };
    let plan = prune::plan_prune(
        &manifest,
        &resume,
        &criteria,
        chrono::Utc::now().timestamp_millis(),
        PruneReason::Manual,
    );

    print_plan(&plan, execute, format)?;
    if plan.segments == 0 {
        return Ok(());
    }
    if !execute {
        println!("\nDry run — nothing deleted. Re-run with --execute to prune.");
        return Ok(());
    }

    prune::execute_prune(storage.as_ref(), manifest, &plan).await?;
    println!(
        "\nPruned {} segment(s), {} bytes. Manifest rewritten first; pruned ranges recorded.",
        plan.segments, plan.bytes
    );
    Ok(())
}

fn parse_rfc3339_or_epoch_ms(raw: &str) -> Result<i64> {
    if let Ok(ms) = raw.parse::<i64>() {
        return Ok(ms);
    }
    let dt = chrono::DateTime::parse_from_rfc3339(raw)
        .with_context(|| format!("expected epoch milliseconds or RFC 3339, got '{raw}'"))?;
    Ok(dt.timestamp_millis())
}

fn print_plan(plan: &PrunePlan, execute: bool, format: &str) -> Result<()> {
    if format == "json" {
        let value = serde_json::json!({
            "backup_id": plan.backup_id,
            "execute": execute,
            "segments": plan.segments,
            "bytes": plan.bytes,
            "partitions": plan.partitions.iter().map(|p| serde_json::json!({
                "topic": p.topic,
                "partition": p.partition,
                "segments": p.segments.len(),
                "bytes": p.range.bytes,
                "start_offset": p.range.start_offset,
                "end_offset": p.range.end_offset,
            })).collect::<Vec<_>>(),
            "protected_partitions": plan.protected_partitions,
        });
        println!("{}", serde_json::to_string_pretty(&value)?);
        return Ok(());
    }

    println!("Prune plan for '{}':", plan.backup_id);
    if plan.partitions.is_empty() {
        println!("  nothing to prune");
    }
    for p in &plan.partitions {
        println!(
            "  {}:{} — {} segment(s), {} bytes, offsets {}..{}",
            p.topic,
            p.partition,
            p.segments.len(),
            p.range.bytes,
            p.range.start_offset,
            p.range.end_offset
        );
    }
    for (topic, partition) in &plan.protected_partitions {
        println!(
            "  {}:{} — pruning stopped early to protect the resume position",
            topic, partition
        );
    }
    println!("Total: {} segment(s), {} bytes", plan.segments, plan.bytes);
    Ok(())
}
