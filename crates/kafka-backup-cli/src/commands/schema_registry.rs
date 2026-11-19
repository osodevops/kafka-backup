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
use kafka_backup_core::schema_registry::models::{SchemaRestoreAction, SchemaRestoreReport};
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

/// A registry a backup snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Registry {
    Confluent,
    Apicurio,
}

impl Registry {
    pub fn name(self) -> &'static str {
        match self {
            Registry::Confluent => "Schema Registry",
            Registry::Apicurio => "Apicurio Registry",
        }
    }

    fn directory(self) -> &'static str {
        match self {
            Registry::Confluent => "schema-registry",
            Registry::Apicurio => "apicurio-registry",
        }
    }
}

/// The registry snapshots of one backup command.
pub struct Snapshots {
    config: Config,
    storage: Arc<dyn StorageBackend>,
    registries: Vec<Registry>,
}

/// The result of one registry in one pass: a summary line, or the error.
pub struct RegistryPass {
    pub registry: Registry,
    pub result: std::result::Result<String, String>,
    /// `fail_on_partial` of the registry's backup options.
    pub fail_on_partial: bool,
}

/// The result of one snapshot pass over every configured registry.
pub type Pass = Vec<RegistryPass>;

impl Snapshots {
    /// `None` when the configuration has no enabled `schema_registry` or
    /// `apicurio_registry` section.
    pub fn from_config(config: &Config) -> Result<Option<Arc<Self>>> {
        let mut registries = Vec::new();
        if run::enabled(config).is_some() {
            registries.push(Registry::Confluent);
        }
        if config
            .apicurio_registry
            .as_ref()
            .is_some_and(|ap| ap.enabled)
        {
            registries.push(Registry::Apicurio);
        }
        if registries.is_empty() {
            return Ok(None);
        }
        Ok(Some(Arc::new(Self {
            config: config.clone(),
            storage: create_backend(&config.storage)?,
            registries,
        })))
    }

    /// The shortest `snapshot_interval` of the configured registries.
    pub fn interval(&self) -> Duration {
        self.registries
            .iter()
            .filter_map(|registry| match registry {
                Registry::Confluent => run::enabled(&self.config)
                    .map(|sr| sr.backup_options().snapshot_interval_or_default()),
                Registry::Apicurio => self
                    .config
                    .apicurio_registry
                    .as_ref()
                    .map(|ap| ap.backup_options().snapshot_interval_or_default()),
            })
            .min()
            .unwrap_or(kafka_backup_core::schema_registry::config::DEFAULT_SNAPSHOT_INTERVAL)
    }

    async fn snapshot(&self, registry: Registry) -> std::result::Result<String, String> {
        match registry {
            Registry::Confluent => run::snapshot(&self.config, self.storage.clone())
                .await
                .map(|m| {
                    format!(
                        "{} subject(s), {} version(s) ({} soft-deleted{})",
                        m.subjects_count,
                        m.total_versions,
                        m.soft_deleted_versions,
                        if m.referenced_subjects.is_empty() {
                            String::new()
                        } else {
                            format!(
                                ", {} subject(s) captured as references",
                                m.referenced_subjects.len()
                            )
                        }
                    )
                })
                .map_err(|e| e.to_string()),
            Registry::Apicurio => {
                kafka_backup_core::apicurio_registry::snapshot(&self.config, self.storage.clone())
                    .await
                    .map(|m| {
                        format!(
                            "{} group(s), {} artifact(s), {} version(s){}",
                            m.groups_count,
                            m.artifacts_count,
                            m.total_versions,
                            if m.has_export_zip {
                                ", export archive"
                            } else {
                                ""
                            }
                        )
                    })
                    .map_err(|e| e.to_string())
            }
        }
    }

    fn fail_on_partial(&self, registry: Registry) -> bool {
        match registry {
            Registry::Confluent => {
                run::enabled(&self.config).is_some_and(|sr| sr.backup_options().fail_on_partial)
            }
            Registry::Apicurio => self
                .config
                .apicurio_registry
                .as_ref()
                .is_some_and(|ap| ap.backup_options().fail_on_partial),
        }
    }

    /// Snapshot every configured registry and log each result. One
    /// registry failing does not stop the other.
    pub async fn pass(&self, label: &str) -> Pass {
        let mut pass = Vec::new();
        for &registry in &self.registries {
            let result = self.snapshot(registry).await;
            match &result {
                Ok(summary) => info!(pass = label, "{} backup complete: {summary}", registry.name()),
                Err(e) => error!(
                    pass = label,
                    "{} backup failed; the snapshot is marked incomplete and cannot be restored: {e}",
                    registry.name()
                ),
            }
            pass.push(RegistryPass {
                registry,
                result,
                fail_on_partial: self.fail_on_partial(registry),
            });
        }
        pass
    }

    /// A pass at start, then one every [`Self::interval`] until `stop`
    /// flips. A pass that overruns the interval skips the missed ticks.
    pub fn spawn_periodic(self: &Arc<Self>, mut stop: watch::Receiver<bool>) -> JoinHandle<()> {
        let snapshots = self.clone();
        let interval = self.interval();
        info!(
            interval = %humantime::format_duration(interval),
            "Continuous backup: the registry is snapshotted at start, every interval and at shutdown"
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
/// registry snapshots' when any was taken. An incomplete snapshot fails the
/// command only with that registry's `fail_on_partial` (it is always logged
/// as an error and marked incomplete in storage).
pub fn combine(kafka: Result<()>, pass: Option<&Pass>) -> Result<()> {
    let failures: Vec<&RegistryPass> = pass
        .map(|p| p.iter().filter(|r| r.result.is_err()).collect())
        .unwrap_or_default();
    let describe = |failures: &[&RegistryPass]| {
        failures
            .iter()
            .map(|r| {
                format!(
                    "{}: {}",
                    r.registry.name(),
                    r.result.as_ref().err().map(String::as_str).unwrap_or("")
                )
            })
            .collect::<Vec<_>>()
            .join("; ")
    };
    match kafka {
        Ok(()) => {
            let fatal: Vec<&RegistryPass> = failures
                .iter()
                .copied()
                .filter(|r| r.fail_on_partial)
                .collect();
            if !fatal.is_empty() {
                return Err(anyhow!(
                    "registry backup failed (fail_on_partial): {}",
                    describe(&fatal)
                ));
            }
            for failure in &failures {
                warn!(
                    "The Kafka data backup completed, but the {} snapshot is incomplete and cannot \
                     be restored. Set its backup.fail_on_partial: true to fail the backup when \
                     this happens",
                    failure.registry.name()
                );
            }
            Ok(())
        }
        Err(e) if pass.is_none() => Err(e),
        Err(e) if failures.is_empty() => {
            Err(e.context("Kafka data backup failed (the Schema Registry snapshot completed)"))
        }
        Err(e) => Err(e.context(format!(
            "Kafka data backup failed; the registry backup also failed: {}",
            describe(&failures)
        ))),
    }
}

/// `backup --schema-only`: one snapshot of every configured registry, no
/// Kafka connection. Any failure fails the command.
pub async fn backup_schema_only(config: &Config) -> Result<()> {
    let Some(snapshots) = Snapshots::from_config(config)? else {
        bail!(
            "--schema-only backs up the registry, but the configuration has no enabled \
             schema_registry or apicurio_registry section"
        );
    };
    let pass = snapshots.pass("schema-only").await;
    print_pass(&config.backup_id, &pass);
    let failures: Vec<String> = pass
        .iter()
        .filter_map(|r| {
            r.result
                .as_ref()
                .err()
                .map(|e| format!("{} backup failed: {e}", r.registry.name()))
        })
        .collect();
    if !failures.is_empty() {
        bail!("{}", failures.join("; "));
    }
    Ok(())
}

/// One line per registry that was snapshotted.
pub fn print_pass(backup_id: &str, pass: &Pass) {
    for r in pass {
        if let Ok(summary) = &r.result {
            println!(
                "{} backup: {summary} -> {backup_id}/{}/",
                r.registry.name(),
                r.registry.directory()
            );
        }
    }
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

/// Restore the registries before the records (or on their own for
/// `--schema-only`). Prints the reports. An error means nothing further may
/// be restored. Nothing happens for a full restore without a registry
/// section.
///
/// Both engines plan before either changes a registry: the Confluent restore
/// (`schema_registry.restore`) and the provider-neutral one
/// (`registry_restore`, for Apicurio and cross-registry restores).
pub async fn restore_first(config: &mut Config, scope: RestoreScope) -> Result<()> {
    let schema_only = scope == RestoreScope::SchemaOnly;
    super::registry_restore::normalize_dry_run(config, false);
    let route = super::registry_restore::route(config, schema_only)?;
    if !route.engine.is_empty()
        && route.confluent
        && !schema_only
        && run::enabled(config)
            .and_then(|sr| sr.restore.as_ref())
            .is_some_and(|r| r.rewrite_ids)
        && config
            .registry_restore
            .as_ref()
            .is_some_and(|r| r.id_policy == kafka_backup_core::registry_restore::IdPolicy::Rewrite)
    {
        bail!(
            "schema_registry.restore.rewrite_ids and registry_restore.id_policy: rewrite would \
             both rewrite the restored records; restore the two registries in separate runs"
        );
    }
    // Plan the provider-neutral restore first: it is read-only, and a
    // conflict there must stop the run before the Confluent registry changes.
    let prepared = super::registry_restore::prepare(config, &route.engine, schema_only).await?;
    if let Some(conflicts) = prepared
        .iter()
        .map(|p| &p.plan.conflicts)
        .find(|c| !c.is_empty())
    {
        if route.confluent {
            println!(
                "{}",
                serde_json::to_string_pretty(
                    &prepared.iter().map(|p| &p.plan).collect::<Vec<_>>()
                )?
            );
            bail!(
                "Registry preflight failed: {}. Nothing was changed.",
                conflicts.join("; ")
            );
        }
    }
    if route.confluent {
        restore_confluent(config, scope).await?;
    }
    super::registry_restore::execute(config, prepared, schema_only).await
}

/// The Confluent-only restore (`schema_registry.restore`).
async fn restore_confluent(config: &mut Config, scope: RestoreScope) -> Result<()> {
    let storage = create_backend(&config.storage)?;
    let (cancel, signals) = cancel_on_signal();
    let outcome = run::restore(config, storage, scope, cancel).await;
    signals.abort();
    let Some(outcome) = outcome? else {
        return Ok(());
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
    Ok(())
}

/// The registry plans for `validate-restore`: nothing is changed and no
/// report is written.
pub struct Preview {
    /// The Confluent restore plan (`schema_registry.restore`).
    pub confluent: Option<RegistryRestoreOutcome>,
    /// The plans of the provider-neutral engine (`registry_restore`).
    pub engine: Vec<kafka_backup_core::registry_restore::engine::RegistryPlan>,
}

impl Preview {
    /// Everything that would make the restore fail.
    pub fn errors(&self) -> Vec<String> {
        let mut errors: Vec<String> = self
            .confluent
            .as_ref()
            .and_then(|o| o.error.as_ref())
            .map(|e| format!("Schema Registry: {e}"))
            .into_iter()
            .collect();
        errors.extend(
            self.engine
                .iter()
                .flat_map(|p| p.conflicts.iter().map(|c| format!("Registry: {c}"))),
        );
        errors
    }
}

pub async fn preview(config: &Config, scope: RestoreScope) -> Result<Preview> {
    let schema_only = scope == RestoreScope::SchemaOnly;
    let route = super::registry_restore::route(config, schema_only)?;
    let confluent = if route.confluent {
        let storage = create_backend(&config.storage)?;
        run::preview(config, storage, scope).await?
    } else {
        None
    };
    let engine = super::registry_restore::prepare(config, &route.engine, schema_only)
        .await?
        .into_iter()
        .map(|p| p.plan)
        .collect();
    Ok(Preview { confluent, engine })
}

/// A compact text rendering of the provider-neutral plans.
pub fn print_engine_plans(plans: &[kafka_backup_core::registry_restore::engine::RegistryPlan]) {
    for plan in plans {
        println!();
        println!(
            "=== Registry restore plan: {:?} -> {:?}, {:?} IDs, {} version(s) ===",
            plan.source,
            plan.destination,
            plan.id_policy,
            plan.registrations.len()
        );
        for registration in &plan.registrations {
            println!(
                "  {:<8} {} -> {}",
                registration.action,
                registration.source.path(),
                registration.destination.path()
            );
        }
        for (label, items) in [
            ("Capability", &plan.capabilities),
            ("Conversion", &plan.wire_conversions),
            ("Settings", &plan.settings_changes),
            ("Limitation", &plan.limitations),
            ("CONFLICT", &plan.conflicts),
        ] {
            for item in items {
                println!("  {label}: {item}");
            }
        }
    }
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

    fn ok(registry: Registry) -> RegistryPass {
        RegistryPass {
            registry,
            result: Ok("1 subject(s)".into()),
            fail_on_partial: false,
        }
    }

    fn failed(registry: Registry, error: &str, fail_on_partial: bool) -> RegistryPass {
        RegistryPass {
            registry,
            result: Err(error.into()),
            fail_on_partial,
        }
    }

    #[test]
    fn a_completed_backup_with_completed_snapshots_succeeds() {
        assert!(combine(
            Ok(()),
            Some(&vec![ok(Registry::Confluent), ok(Registry::Apicurio)])
        )
        .is_ok());
        assert!(combine(Ok(()), None).is_ok());
    }

    #[test]
    fn an_incomplete_snapshot_fails_the_backup_only_with_fail_on_partial() {
        let lenient = vec![failed(Registry::Confluent, "503 from /subjects", false)];
        assert!(combine(Ok(()), Some(&lenient)).is_ok());
        let strict = vec![failed(Registry::Confluent, "503 from /subjects", true)];
        let msg = format!("{:#}", combine(Ok(()), Some(&strict)).unwrap_err());
        assert!(msg.contains("503 from /subjects"), "{msg}");
        assert!(msg.contains("fail_on_partial"), "{msg}");
    }

    #[test]
    fn each_registry_has_its_own_fail_on_partial() {
        // Apicurio is strict and fails; Confluent is lenient and failed too.
        let pass = vec![
            failed(Registry::Confluent, "401", false),
            failed(Registry::Apicurio, "export refused", true),
        ];
        let msg = format!("{:#}", combine(Ok(()), Some(&pass)).unwrap_err());
        assert!(msg.contains("Apicurio Registry: export refused"), "{msg}");
        assert!(!msg.contains("401"), "{msg}");
        // The other way round, the strict registry succeeded.
        let pass = vec![
            failed(Registry::Confluent, "401", false),
            ok(Registry::Apicurio),
        ];
        assert!(combine(Ok(()), Some(&pass)).is_ok());
    }

    #[test]
    fn a_kafka_failure_is_reported_with_the_snapshot_result() {
        let msg = format!(
            "{:#}",
            combine(
                Err(anyhow!("broker down")),
                Some(&vec![ok(Registry::Confluent)])
            )
            .unwrap_err()
        );
        assert!(msg.contains("broker down"), "{msg}");
        assert!(msg.contains("snapshot completed"), "{msg}");

        let pass = vec![failed(Registry::Confluent, "401", false)];
        let msg = format!(
            "{:#}",
            combine(Err(anyhow!("broker down")), Some(&pass)).unwrap_err()
        );
        assert!(msg.contains("broker down"), "{msg}");
        assert!(msg.contains("also failed: Schema Registry: 401"), "{msg}");

        let msg = format!(
            "{:#}",
            combine(Err(anyhow!("broker down")), None).unwrap_err()
        );
        assert_eq!(msg, "broker down");
    }
}
