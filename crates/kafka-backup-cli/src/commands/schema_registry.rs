//! Schema Registry wiring shared by `backup`, `restore`,
//! `three-phase-restore` and `validate-restore`.
//!
//! Backup: [`Snapshots`] takes the registry snapshot next to the topic data
//! (once for a one-shot backup, on an interval for a continuous one), and
//! [`combine`] turns the two results into the command's exit status.
//!
//! Restore: [`restore_first`] restores the registry before any record is
//! produced and stops the command when it fails, so records never land in a
//! cluster whose registry lacks their schema IDs.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use kafka_backup_core::schema_registry::models::{
    SchemaRegistryManifest, SchemaRestoreAction, SchemaRestoreReport,
};
use kafka_backup_core::schema_registry::run::{self, RegistryRestoreOutcome, RestoreScope};
use kafka_backup_core::storage::{create_backend, StorageBackend};
use kafka_backup_core::Config;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

// ---------------------------------------------------------------------------
// Backup
// ---------------------------------------------------------------------------

/// The registry snapshots of one backup command.
pub struct Snapshots {
    config: Config,
    storage: Arc<dyn StorageBackend>,
}

/// The result of one snapshot pass.
pub type Pass = std::result::Result<SchemaRegistryManifest, String>;

impl Snapshots {
    /// `None` when the configuration has no enabled `schema_registry`
    /// section.
    pub fn from_config(config: &Config) -> Result<Option<Arc<Self>>> {
        if run::enabled(config).is_none() {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            config: config.clone(),
            storage: create_backend(&config.storage)?,
        })))
    }

    fn options(&self) -> kafka_backup_core::schema_registry::SchemaBackupOptions {
        run::enabled(&self.config)
            .map(|sr| sr.backup_options())
            .unwrap_or_default()
    }

    /// With `fail_on_partial`, an incomplete snapshot fails the backup.
    pub fn fail_on_partial(&self) -> bool {
        self.options().fail_on_partial
    }

    pub fn interval(&self) -> Duration {
        self.options().snapshot_interval_or_default()
    }

    /// Take one snapshot and log its result.
    pub async fn pass(&self, label: &str) -> Pass {
        match run::snapshot(&self.config, self.storage.clone()).await {
            Ok(manifest) => {
                info!(
                    pass = label,
                    subjects = manifest.subjects_count,
                    versions = manifest.total_versions,
                    soft_deleted = manifest.soft_deleted_versions,
                    "Schema Registry backup complete"
                );
                Ok(manifest)
            }
            Err(e) => {
                error!(
                    pass = label,
                    "Schema Registry backup failed; the snapshot is marked incomplete and cannot \
                     be restored: {e}"
                );
                Err(e.to_string())
            }
        }
    }

    /// A pass at start, then one every [`Self::interval`] until `stop`
    /// flips. A pass that overruns the interval skips the missed ticks.
    pub fn spawn_periodic(self: &Arc<Self>, mut stop: watch::Receiver<bool>) -> JoinHandle<()> {
        let snapshots = self.clone();
        let interval = self.interval();
        info!(
            interval = %humantime::format_duration(interval),
            "Continuous backup: the Schema Registry is snapshotted at start, every interval and at shutdown"
        );
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    // Stop wins over a due tick: the shutdown pass follows.
                    biased;
                    _ = stop.changed() => break,
                    _ = ticker.tick() => {
                        let _ = snapshots.pass("periodic").await;
                    }
                }
            }
        })
    }
}

/// The exit status of `backup`: the Kafka data backup's result, and the last
/// registry snapshot's when one was taken. An incomplete snapshot fails the
/// command only with `fail_on_partial` (it is always logged as an error and
/// marked incomplete in storage).
pub fn combine(kafka: Result<()>, snapshot: Option<&Pass>, fail_on_partial: bool) -> Result<()> {
    let snapshot_error = snapshot.and_then(|pass| pass.as_ref().err());
    match (kafka, snapshot_error) {
        (Ok(()), None) => Ok(()),
        (Ok(()), Some(e)) if fail_on_partial => Err(anyhow!(
            "Schema Registry backup failed (schema_registry.backup.fail_on_partial): {e}"
        )),
        (Ok(()), Some(_)) => {
            warn!(
                "The Kafka data backup completed, but the Schema Registry snapshot is incomplete \
                 and cannot be restored. Set schema_registry.backup.fail_on_partial: true to fail \
                 the backup when this happens"
            );
            Ok(())
        }
        (Err(e), None) if snapshot.is_some() => {
            Err(e.context("Kafka data backup failed (the Schema Registry snapshot completed)"))
        }
        (Err(e), None) => Err(e),
        (Err(e), Some(sr)) => Err(e.context(format!(
            "Kafka data backup failed; the Schema Registry backup also failed: {sr}"
        ))),
    }
}

/// `backup --schema-only`: one snapshot, no Kafka connection.
pub async fn backup_schema_only(config: &Config) -> Result<()> {
    let Some(snapshots) = Snapshots::from_config(config)? else {
        bail!(
            "--schema-only backs up the Schema Registry, but the configuration has no enabled \
             schema_registry section"
        );
    };
    let manifest = snapshots
        .pass("schema-only")
        .await
        .map_err(|e| anyhow!("Schema Registry backup failed: {e}"))?;
    print_snapshot(&config.backup_id, &manifest);
    Ok(())
}

pub fn print_snapshot(backup_id: &str, manifest: &SchemaRegistryManifest) {
    println!(
        "Schema Registry backup: {} subject(s), {} version(s) ({} soft-deleted{}) -> {}/schema-registry/",
        manifest.subjects_count,
        manifest.total_versions,
        manifest.soft_deleted_versions,
        if manifest.referenced_subjects.is_empty() {
            String::new()
        } else {
            format!(
                ", {} subject(s) captured as references",
                manifest.referenced_subjects.len()
            )
        },
        backup_id
    );
}

// ---------------------------------------------------------------------------
// Restore
// ---------------------------------------------------------------------------

/// A token that fires on SIGINT / SIGTERM, and the task feeding it.
fn cancel_on_signal() -> (CancellationToken, JoinHandle<()>) {
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    let task = tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            match signal(SignalKind::terminate()) {
                Ok(mut term) => {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {}
                        _ = term.recv() => {}
                    }
                }
                Err(_) => {
                    let _ = tokio::signal::ctrl_c().await;
                }
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        warn!("Received shutdown signal: stopping the Schema Registry restore and putting the target's modes back");
        trigger.cancel();
    });
    (cancel, task)
}

/// Restore the Schema Registry before the records (or on its own for
/// `--schema-only`). Prints the report. An error means nothing further may
/// be restored. `Ok(None)`: a full restore without a `schema_registry`
/// section.
pub async fn restore_first(
    config: &mut Config,
    scope: RestoreScope,
) -> Result<Option<SchemaRestoreReport>> {
    if scope == RestoreScope::BeforeRecords && run::enabled(config).is_none() {
        return Ok(None);
    }
    let storage = create_backend(&config.storage)?;
    let (cancel, signals) = cancel_on_signal();
    let outcome = run::restore(config, storage, scope, cancel).await;
    signals.abort();
    let Some(outcome) = outcome? else {
        return Ok(None);
    };
    let key = outcome.report_key.clone().unwrap_or_default();
    print_report(&outcome.report, &key);

    if let Some(e) = &outcome.error {
        bail!(
            "Schema Registry restore failed: {e}. {}See {key} in the backup storage.",
            if scope == RestoreScope::BeforeRecords {
                "The Kafka records were not restored. "
            } else {
                ""
            },
        );
    }
    if let Some(rewrite) = &outcome.id_rewrite {
        println!(
            "Schema IDs: {} changed; the restored records' Confluent wire-format IDs are \
             rewritten ({}{})",
            rewrite.changed,
            if rewrite.topics.is_empty() {
                "every restored topic".to_string()
            } else {
                rewrite.topics.join(", ")
            },
            if rewrite.strict {
                "; a record with an ID the backup does not have fails the restore"
            } else {
                "; schema_id_rewrite_strict: false leaves unknown IDs unchanged"
            }
        );
    }
    Ok(Some(outcome.report))
}

/// The registry plan for `validate-restore`: nothing is changed and no
/// report is written.
pub async fn preview(
    config: &Config,
    scope: RestoreScope,
) -> Result<Option<RegistryRestoreOutcome>> {
    if scope == RestoreScope::BeforeRecords && run::enabled(config).is_none() {
        return Ok(None);
    }
    let storage = create_backend(&config.storage)?;
    Ok(run::preview(config, storage, scope).await?)
}

pub fn print_report(report: &SchemaRestoreReport, key: &str) {
    println!();
    println!(
        "=== Schema Registry restore{} ===",
        if report.dry_run { " (dry run)" } else { "" }
    );
    println!(
        "Target: {}   Strategy: {}{}",
        report.target_url,
        report.strategy,
        if report.rewrite_ids {
            " (rewrite_ids)"
        } else {
            ""
        }
    );
    if report.dry_run {
        for item in &report.items {
            let action = match item.action {
                SchemaRestoreAction::Register => "register",
                SchemaRestoreAction::Skip => "skip",
                SchemaRestoreAction::Rewrite => "rewrite",
                SchemaRestoreAction::MapExisting => "map",
                SchemaRestoreAction::Conflict => "CONFLICT",
            };
            println!(
                "  {action:<8} {} v{} (ID {}){}{}",
                item.target_subject,
                item.version,
                item.source_id,
                if item.soft_deleted {
                    " [soft-deleted]"
                } else {
                    ""
                },
                item.detail
                    .as_deref()
                    .map(|d| format!(" — {d}"))
                    .unwrap_or_default()
            );
        }
        let count = |a| report.items.iter().filter(|i| i.action == a).count();
        println!(
            "Plan: {} subject(s), {} version(s): {} to register, {} already present, {} to rewrite, {} mapped, {} in conflict",
            report.subjects_planned,
            report.versions_planned,
            count(SchemaRestoreAction::Register),
            count(SchemaRestoreAction::Skip),
            count(SchemaRestoreAction::Rewrite),
            count(SchemaRestoreAction::MapExisting),
            count(SchemaRestoreAction::Conflict),
        );
    } else {
        println!(
            "Versions: {} registered, {} already present, {} rewritten, {} soft-deleted again (of {} in {} subject(s))",
            report.versions_registered,
            report.versions_skipped,
            report.versions_rewritten,
            report.versions_soft_deleted,
            report.versions_planned,
            report.subjects_planned,
        );
        for (source, target) in &report.id_mapping {
            println!("  schema ID {source} -> {target}");
        }
        if let Some(v) = &report.verification {
            println!(
                "Verified: {} version(s), {} mismatch(es)",
                v.versions_checked,
                v.mismatches.len()
            );
        }
    }
    if let Some(c) = &report.capacity {
        println!(
            "Capacity: {} used + {} new of {} allowed",
            c.used, c.required, c.limit
        );
    }
    for conflict in &report.conflicts {
        println!("  ! {conflict}");
    }
    if report.cancelled {
        println!("The restore was cancelled.");
    }
    if report.modes_rolled_back {
        println!("Target modes were restored to their values before the restore.");
    }
    for e in &report.mode_restore_errors {
        println!("  ! {e}");
    }
    for change in &report.modes_changed_by_others {
        println!("  ! {change}");
    }
    if key.is_empty() {
        println!("Result: {}", if report.success { "OK" } else { "FAILED" });
    } else {
        println!(
            "Result: {}   Report: {key}",
            if report.success { "OK" } else { "FAILED" }
        );
    }
    println!();
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> SchemaRegistryManifest {
        serde_json::from_value(serde_json::json!({
            "schema_registry_url": "http://sr",
            "backup_timestamp": "2026-01-01T00:00:00Z",
            "global_compatibility": "BACKWARD",
            "global_mode": "READWRITE",
            "subjects": [],
            "subjects_count": 0,
            "total_versions": 0,
            "dependency_order": []
        }))
        .unwrap()
    }

    #[test]
    fn a_completed_backup_with_a_completed_snapshot_succeeds() {
        assert!(combine(Ok(()), Some(&Ok(manifest())), false).is_ok());
        assert!(combine(Ok(()), None, false).is_ok());
    }

    #[test]
    fn an_incomplete_snapshot_fails_the_backup_only_with_fail_on_partial() {
        let failed: Pass = Err("503 from /subjects".into());
        assert!(combine(Ok(()), Some(&failed), false).is_ok());
        let err = combine(Ok(()), Some(&failed), true).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("503 from /subjects"), "{msg}");
        assert!(msg.contains("fail_on_partial"), "{msg}");
    }

    #[test]
    fn a_kafka_failure_is_reported_with_the_snapshot_result() {
        let ok: Pass = Ok(manifest());
        let msg = format!(
            "{:#}",
            combine(Err(anyhow!("broker down")), Some(&ok), false).unwrap_err()
        );
        assert!(msg.contains("broker down"), "{msg}");
        assert!(msg.contains("snapshot completed"), "{msg}");

        let failed: Pass = Err("401".into());
        let msg = format!(
            "{:#}",
            combine(Err(anyhow!("broker down")), Some(&failed), false).unwrap_err()
        );
        assert!(msg.contains("broker down"), "{msg}");
        assert!(msg.contains("also failed: 401"), "{msg}");

        let msg = format!(
            "{:#}",
            combine(Err(anyhow!("broker down")), None, false).unwrap_err()
        );
        assert_eq!(msg, "broker down");
    }
}
