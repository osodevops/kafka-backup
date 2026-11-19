//! Schema Registry restore with original schema IDs.
//!
//! Runs before the Kafka records are produced, so records that carry schema
//! IDs in the Confluent wire format (magic byte + 4-byte ID) resolve in the
//! target registry.
//!
//! # Phases
//!
//! 1. **Load** the backup: every captured version, live and soft-deleted.
//! 2. **Order** versions topologically on their references, at version level,
//!    ties broken by original schema ID ([`super::ordering`]).
//! 3. **Plan** (read-only): classify each version against the target
//!    ([`SchemaRestoreAction`]). Conflicts in strict mode, a non-empty target
//!    for `preserve_ids`, an ambiguous ID rewrite or a capacity overrun fail
//!    here, before the target is changed. A dry run stops here.
//! 4. **Execute** under a [`ModeGuard`]: IMPORT mode (global and per context
//!    for `preserve_ids`, per subject for `into_existing`), registration with
//!    original IDs and versions, soft-deletes, compatibility, then the final
//!    modes. On any error, and on cancellation, the guard puts the target's
//!    prior global, context and subject modes back (explicit async cleanup:
//!    `Drop` cannot await). A mode that someone else changed while the
//!    restore ran is left as they set it, and reported.
//! 5. **Verify** every restored (subject, version) and the subject settings.
//!
//! The report is written to `{backup_id}/restore/schema-registry-report.json`
//! (`schema-registry-report.dry-run.json` for a dry run), on failure too.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use bytes::Bytes;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::storage::StorageBackend;

use super::config::{SchemaRegistryConfig, SchemaRestoreOptions, SchemaRestoreStrategy};
use super::error::SchemaRegistryError;

use super::backup::{global_config_key, manifest_key, subject_metadata_key, version_key};
use super::client::SchemaRegistryClient;
use super::context;
use super::models::{
    GlobalConfig, RegisterSchemaRequest, SchemaCapacityCheck, SchemaReference,
    SchemaRegistryManifest, SchemaRestoreAction, SchemaRestoreItem, SchemaRestoreReport,
    SchemaVerification, SchemaVersion, SubjectMetadata,
};
use super::ordering::{registration_order, VersionNode};
use super::snapshot::{self, SnapshotInfo};

const IMPORT: &str = "IMPORT";
const READWRITE: &str = "READWRITE";

/// Storage key of the restore report.
pub fn report_key(backup_id: &str, dry_run: bool) -> String {
    if dry_run {
        format!("{backup_id}/restore/schema-registry-report.dry-run.json")
    } else {
        format!("{backup_id}/restore/schema-registry-report.json")
    }
}

/// Restores a Schema Registry snapshot into the registry in
/// `config.url`, following `config.restore`.
pub struct SchemaRegistryRestore {
    client: SchemaRegistryClient,
    storage: Arc<dyn StorageBackend>,
    backup_id: String,
    config: SchemaRegistryConfig,
}

/// One backed-up schema version.
#[derive(Debug, Clone)]
struct SourceVersion {
    /// Source subject (canonical form).
    subject: String,
    /// Target subject after `subject_mapping` (canonical form).
    target_subject: String,
    schema: SchemaVersion,
    soft_deleted: bool,
}

/// The loaded backup, versions in registration order.
struct LoadedBackup {
    global: GlobalConfig,
    /// Source subject -> metadata.
    subjects: BTreeMap<String, SubjectMetadata>,
    versions: Vec<SourceVersion>,
    /// Target subject -> source subject.
    target_to_source: BTreeMap<String, String>,
}

/// The planned (and, after execution, actual) handling of one version.
#[derive(Debug, Clone)]
struct Step {
    action: SchemaRestoreAction,
    detail: Option<String>,
    /// Register with IMPORT over a target version that is soft-deleted (this
    /// revives it).
    revive: bool,
    /// Soft-delete the target version after registration.
    soft_delete_after: bool,
    target_version: Option<i32>,
    target_id: Option<i32>,
}

/// What the target holds for one subject.
#[derive(Debug, Clone, Default)]
struct TargetSubject {
    all: BTreeSet<i32>,
    live: BTreeSet<i32>,
}

impl SchemaRegistryRestore {
    pub fn new(
        client: SchemaRegistryClient,
        storage: Arc<dyn StorageBackend>,
        backup_id: String,
        config: SchemaRegistryConfig,
    ) -> Self {
        Self {
            client,
            storage,
            backup_id,
            config,
        }
    }

    fn options(&self) -> SchemaRestoreOptions {
        self.config.restore_options()
    }

    /// Run the restore. The report is written to storage either way; on
    /// failure the error is returned (and `report.error` holds it).
    pub async fn run(&self) -> Result<SchemaRestoreReport, SchemaRegistryError> {
        let (report, error) = self.run_detailed().await;
        match error {
            None => Ok(report),
            Some(e) => Err(e),
        }
    }

    /// Run the restore and return the report together with the error, if
    /// any, so a caller can print the report of a failed restore.
    pub async fn run_detailed(&self) -> (SchemaRestoreReport, Option<SchemaRegistryError>) {
        self.run_detailed_inner(false, CancellationToken::new())
            .await
    }

    /// [`Self::run_detailed`], stopping when `cancel` fires (SIGINT /
    /// SIGTERM). A cancelled restore puts the target's modes back, writes
    /// its report and returns [`SchemaRegistryError::Cancelled`]; rerun it
    /// with `strategy: into_existing` to finish.
    pub async fn run_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> (SchemaRestoreReport, Option<SchemaRegistryError>) {
        self.run_detailed_inner(false, cancel).await
    }

    /// Read-only validation: neither registry mutations nor report uploads.
    pub async fn preview(&self) -> (SchemaRestoreReport, Option<SchemaRegistryError>) {
        self.run_detailed_inner(true, CancellationToken::new())
            .await
    }

    async fn run_detailed_inner(
        &self,
        preview: bool,
        cancel: CancellationToken,
    ) -> (SchemaRestoreReport, Option<SchemaRegistryError>) {
        let mut opts = self.options();
        opts.dry_run |= preview;
        let mut report = SchemaRestoreReport {
            backup_id: self.backup_id.clone(),
            target_url: self.client.base_url().to_string(),
            strategy: opts.strategy.as_str().to_string(),
            dry_run: opts.dry_run,
            rewrite_ids: opts.rewrite_ids,
            started_at: chrono::Utc::now().to_rfc3339(),
            finished_at: String::new(),
            success: false,
            error: None,
            subjects_planned: 0,
            versions_planned: 0,
            versions_registered: 0,
            versions_skipped: 0,
            versions_rewritten: 0,
            versions_soft_deleted: 0,
            conflicts: Vec::new(),
            id_mapping: BTreeMap::new(),
            ids_preserved: BTreeSet::new(),
            capacity: None,
            modes_rolled_back: false,
            mode_restore_errors: Vec::new(),
            modes_changed_by_others: Vec::new(),
            cancelled: false,
            verification: None,
            warnings: Vec::new(),
            items: Vec::new(),
        };

        let result = self.restore(&opts, &mut report, &cancel).await;
        report.finished_at = chrono::Utc::now().to_rfc3339();
        report.success = result.is_ok();
        if let Err(e) = &result {
            report.error = Some(e.to_string());
        }
        for warning in &report.warnings {
            warn!("{warning}");
        }

        let key = report_key(&self.backup_id, opts.dry_run);
        if !preview {
            match serde_json::to_vec_pretty(&report) {
                Ok(json) => {
                    if let Err(e) = self.storage.put(&key, Bytes::from(json)).await {
                        warn!(key = %key, error = %e, "Cannot write the Schema Registry restore report");
                    }
                }
                Err(e) => warn!(error = %e, "Cannot serialise the Schema Registry restore report"),
            }
        }

        match result {
            Ok(()) => {
                info!(
                    registered = report.versions_registered,
                    skipped = report.versions_skipped,
                    rewritten = report.versions_rewritten,
                    soft_deleted = report.versions_soft_deleted,
                    dry_run = report.dry_run,
                    "Schema Registry restore complete"
                );
                (report, None)
            }
            Err(e) => {
                error!(error = %e, "Schema Registry restore failed");
                (report, Some(e))
            }
        }
    }

    async fn restore(
        &self,
        opts: &SchemaRestoreOptions,
        report: &mut SchemaRestoreReport,
        cancel: &CancellationToken,
    ) -> Result<(), SchemaRegistryError> {
        if opts.rewrite_ids && opts.strategy == SchemaRestoreStrategy::PreserveIds {
            return Err(SchemaRegistryError::Config(
                "rewrite_ids applies to strategy: into_existing only; preserve_ids always keeps the original IDs"
                    .to_string(),
            ));
        }

        // 1-2. Load and order.
        let backup = self.load_backup(opts).await?;
        report.versions_planned = backup.versions.len();
        report.subjects_planned = backup
            .versions
            .iter()
            .map(|v| v.target_subject.as_str())
            .collect::<BTreeSet<_>>()
            .len();
        info!(
            subjects = report.subjects_planned,
            versions = report.versions_planned,
            strategy = opts.strategy.as_str(),
            dry_run = opts.dry_run,
            target = %self.client.base_url(),
            "Restoring Schema Registry"
        );

        // 3. Plan (read-only).
        let mut targets: HashMap<String, TargetSubject> = HashMap::new();
        let existing = self.client.list_subjects(true).await?;
        let mut steps = match opts.strategy {
            SchemaRestoreStrategy::PreserveIds if existing.is_empty() => backup
                .versions
                .iter()
                .map(|v| Step::register(v.soft_deleted))
                .collect::<Vec<_>>(),
            _ => {
                let mut steps = Vec::with_capacity(backup.versions.len());
                for item in &backup.versions {
                    let request = self.request_for(item, &HashMap::new(), opts);
                    steps.push(
                        self.classify(item, &request, opts.rewrite_ids, &mut targets)
                            .await?,
                    );
                }
                steps
            }
        };
        claim_ids(&backup, &mut steps, opts.rewrite_ids);
        self.fill_report_items(&backup, &steps, report);

        if opts.strategy == SchemaRestoreStrategy::PreserveIds && !existing.is_empty() {
            let all_present = steps.iter().all(|s| s.action == SchemaRestoreAction::Skip);
            if !all_present {
                let mut sample: Vec<String> = existing.iter().take(5).cloned().collect();
                if existing.len() > 5 {
                    sample.push("…".to_string());
                }
                return Err(SchemaRegistryError::Config(format!(
                    "strategy preserve_ids needs an empty target registry, but it has {} subject(s) ({}) \
                     and does not already hold this backup. Use strategy: into_existing to restore into a \
                     populated registry (it skips what is already there, so it also resumes a partial restore)",
                    existing.len(),
                    sample.join(", ")
                )));
            }
            info!(
                "The target already holds every version of this backup; only settings are restored"
            );
        }

        // Conflicts (strict) fail before any change.
        if !report.conflicts.is_empty() {
            return Err(SchemaRegistryError::RestoreConflict(format!(
                "{} version(s) conflict with the target and rewrite_ids is off; nothing was changed: {}",
                report.conflicts.len(),
                report.conflicts.join("; ")
            )));
        }
        if opts.rewrite_ids {
            check_rewrite_is_unambiguous(&backup, &steps)?;
        }
        self.check_capacity(opts, &steps, report).await?;
        self.check_mode_cleanup(opts, &backup, &steps).await?;

        if opts.dry_run {
            return Ok(());
        }
        if cancel.is_cancelled() {
            return Err(SchemaRegistryError::Cancelled(
                "before any change to the target".to_string(),
            ));
        }

        // 4. Execute under the mode guard. Cancellation drops the execution
        // at its next await; a request already sent may still have been
        // applied, which is why the guard reads the target's actual modes
        // before putting them back.
        let mut guard = ModeGuard::new(&self.client);
        let executed = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(SchemaRegistryError::Cancelled(
                "the target's modes were put back; rerun with strategy: into_existing to finish \
                 (it skips what is already registered)"
                    .to_string(),
            )),
            result = self.execute(opts, &backup, &mut steps, &mut targets, &mut guard, report) => result,
        };
        self.fill_report_items(&backup, &steps, report);
        if let Err(e) = executed {
            report.cancelled = matches!(e, SchemaRegistryError::Cancelled(_));
            let errors = guard.rollback().await;
            report.modes_rolled_back = errors.is_empty();
            report.mode_restore_errors = errors;
            report.modes_changed_by_others = guard.take_foreign_changes();
            return Err(e);
        }
        guard.disarm();
        report.modes_changed_by_others = guard.take_foreign_changes();
        for change in &report.modes_changed_by_others {
            report.warnings.push(change.clone());
        }

        // 5. Verify.
        if opts.verify {
            let verification = self.verify(opts, &backup, &steps).await?;
            let mismatches = verification.mismatches.clone();
            report
                .warnings
                .extend(verification.warnings.iter().cloned());
            report.verification = Some(verification);
            if !mismatches.is_empty() {
                return Err(SchemaRegistryError::VerificationFailed(
                    mismatches.join("; "),
                ));
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Load
    // ------------------------------------------------------------------

    async fn read_json<T: serde::de::DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<T, SchemaRegistryError> {
        let bytes = self
            .storage
            .get(key)
            .await
            .map_err(|error| SchemaRegistryError::Storage(format!("read {key}: {error}")))?;
        serde_json::from_slice(&bytes)
            .map_err(|error| SchemaRegistryError::Serialization(format!("decode {key}: {error}")))
    }

    /// Every object the restore reads must be covered by a checksum.
    fn check_sealed(
        &self,
        sealed: Option<&SnapshotInfo>,
        key: &str,
    ) -> Result<(), SchemaRegistryError> {
        match sealed {
            Some(info) if !info.objects.contains_key(key) => Err(SchemaRegistryError::Snapshot(
                format!("{key} has no recorded checksum in the manifest"),
            )),
            _ => Ok(()),
        }
    }

    async fn load_backup(
        &self,
        opts: &SchemaRestoreOptions,
    ) -> Result<LoadedBackup, SchemaRegistryError> {
        let manifest: SchemaRegistryManifest =
            self.read_json(&manifest_key(&self.backup_id)).await?;
        // Refuse an incomplete or corrupt snapshot before touching the target.
        snapshot::verify(
            self.storage.as_ref(),
            &self.backup_id,
            manifest.snapshot.as_ref(),
        )
        .await?;
        let sealed = manifest.snapshot.as_ref();
        self.check_sealed(sealed, &global_config_key(&self.backup_id))?;
        let global: GlobalConfig = self.read_json(&global_config_key(&self.backup_id)).await?;
        if manifest.subjects_failed > 0 {
            warn!(
                failed = manifest.subjects_failed,
                "The Schema Registry backup is partial: {}",
                manifest.errors.join("; ")
            );
        }

        let mapping: HashMap<String, String> = opts
            .subject_mapping
            .iter()
            .map(|(k, v)| (context::normalize(k), context::normalize(v)))
            .collect();

        let mut subjects = BTreeMap::new();
        let mut target_to_source = BTreeMap::new();
        let mut versions = Vec::new();
        for summary in &manifest.subjects {
            let metadata_key = subject_metadata_key(&self.backup_id, &summary.name);
            self.check_sealed(sealed, &metadata_key)?;
            let metadata: SubjectMetadata = self.read_json(&metadata_key).await?;
            let subject = context::normalize(&summary.name);
            let target_subject = mapping
                .get(&subject)
                .cloned()
                .unwrap_or_else(|| subject.clone());
            if let Some(other) = target_to_source.insert(target_subject.clone(), subject.clone()) {
                return Err(SchemaRegistryError::Config(format!(
                    "subject_mapping maps both {other} and {subject} to {target_subject}"
                )));
            }
            for &version in &metadata.versions {
                let soft_deleted = metadata.is_version_soft_deleted(version);
                if soft_deleted && !opts.restore_soft_deleted {
                    continue;
                }
                let key = version_key(&self.backup_id, &summary.name, version);
                self.check_sealed(sealed, &key)?;
                let schema: SchemaVersion = self.read_json(&key).await?;
                if context::normalize(&schema.subject) != subject
                    || schema.version != version
                    || schema.id <= 0
                {
                    return Err(SchemaRegistryError::Snapshot(format!(
                        "{key} holds {} v{} (ID {}), not {subject} v{version}",
                        schema.subject, schema.version, schema.id
                    )));
                }
                versions.push(SourceVersion {
                    subject: subject.clone(),
                    target_subject: target_subject.clone(),
                    schema,
                    soft_deleted,
                });
            }
            subjects.insert(subject, metadata);
        }

        let nodes: Vec<VersionNode> = versions
            .iter()
            .map(|v| VersionNode {
                subject: v.subject.clone(),
                version: v.schema.version,
                id: v.schema.id,
                references: v
                    .schema
                    .references
                    .iter()
                    .map(|r| {
                        (
                            context::resolve_reference(&v.subject, &r.subject),
                            r.version,
                        )
                    })
                    .collect(),
            })
            .collect();
        let order = registration_order(&nodes)?;
        let mut slots: Vec<Option<SourceVersion>> = versions.into_iter().map(Some).collect();
        let versions = order
            .into_iter()
            .map(|i| slots[i].take().expect("each index once"))
            .collect();

        Ok(LoadedBackup {
            global,
            subjects,
            versions,
            target_to_source,
        })
    }

    // ------------------------------------------------------------------
    // Plan
    // ------------------------------------------------------------------

    /// The registration request for `item`, its references mapped to the
    /// target subjects and to any version numbers the restore changed.
    fn request_for(
        &self,
        item: &SourceVersion,
        version_map: &HashMap<(String, i32), i32>,
        opts: &SchemaRestoreOptions,
    ) -> RegisterSchemaRequest {
        let references = item
            .schema
            .references
            .iter()
            .map(|r| map_reference(item, r, version_map, opts))
            .collect();
        RegisterSchemaRequest {
            schema: item.schema.schema.clone(),
            schema_type: Some(item.schema.schema_type.clone()),
            references,
            id: Some(item.schema.id),
            version: Some(item.schema.version),
            metadata: item.schema.metadata.clone(),
            rule_set: item.schema.rule_set.clone(),
        }
    }

    async fn target_subject<'a>(
        &self,
        targets: &'a mut HashMap<String, TargetSubject>,
        subject: &str,
    ) -> Result<&'a TargetSubject, SchemaRegistryError> {
        if !targets.contains_key(subject) {
            let all = self.client.find_versions(subject, true).await?;
            let state = match all {
                None => TargetSubject::default(),
                Some(all) => TargetSubject {
                    all: all.into_iter().collect(),
                    live: self
                        .client
                        .find_versions(subject, false)
                        .await?
                        .unwrap_or_default()
                        .into_iter()
                        .collect(),
                },
            };
            targets.insert(subject.to_string(), state);
        }
        Ok(&targets[subject])
    }

    /// Classify one version against the target (read-only).
    async fn classify(
        &self,
        item: &SourceVersion,
        request: &RegisterSchemaRequest,
        rewrite: bool,
        targets: &mut HashMap<String, TargetSubject>,
    ) -> Result<Step, SchemaRegistryError> {
        let subject = &item.target_subject;
        let version = item.schema.version;
        let id = item.schema.id;
        let state = self.target_subject(targets, subject).await?.clone();

        if state.all.contains(&version) {
            let existing = self
                .client
                .find_schema_version(subject, version, true)
                .await?
                .ok_or_else(|| {
                    SchemaRegistryError::Other(format!(
                        "{subject} v{version} is listed by the target but cannot be read"
                    ))
                })?;
            let target_deleted = !state.live.contains(&version);
            let identical = same_schema(
                subject,
                &existing.schema,
                &existing.schema_type,
                &existing.references,
                request,
            );
            if identical && existing.id == id {
                if target_deleted && !item.soft_deleted {
                    // Re-registering with IMPORT revives a soft-deleted version.
                    return Ok(Step {
                        revive: true,
                        detail: Some("soft-deleted in the target; revived".to_string()),
                        ..Step::register(false)
                    });
                }
                return Ok(Step {
                    action: SchemaRestoreAction::Skip,
                    detail: Some("already in the target".to_string()),
                    revive: false,
                    soft_delete_after: item.soft_deleted && !target_deleted,
                    target_version: Some(version),
                    target_id: Some(id),
                });
            }
            if identical {
                let detail = format!(
                    "the target holds this schema at v{version} under ID {}",
                    existing.id
                );
                return Ok(if rewrite {
                    Step {
                        action: SchemaRestoreAction::MapExisting,
                        detail: Some(detail),
                        revive: false,
                        soft_delete_after: false,
                        target_version: Some(version),
                        target_id: Some(existing.id),
                    }
                } else {
                    Step::conflict(detail)
                });
            }
            let detail = format!(
                "v{version} in the target is a different schema (ID {})",
                existing.id
            );
            return Ok(if rewrite {
                Step::rewrite(detail, item.soft_deleted)
            } else {
                Step::conflict(detail)
            });
        }

        let ctx_subject = context::context_subject(context::context_of(subject));
        match self
            .client
            .find_schema_by_id(id, ctx_subject.as_deref())
            .await?
        {
            None => Ok(Step::register(item.soft_deleted)),
            Some(holder)
                if same_schema(
                    subject,
                    &holder.schema,
                    &holder.schema_type,
                    &holder.references,
                    request,
                ) =>
            {
                Ok(Step {
                    detail: Some(format!("ID {id} already holds this schema in the target")),
                    ..Step::register(item.soft_deleted)
                })
            }
            Some(_) => {
                let detail = format!("ID {id} is held by a different schema in the target");
                Ok(if rewrite {
                    Step::rewrite(detail, item.soft_deleted)
                } else {
                    Step::conflict(detail)
                })
            }
        }
    }

    fn fill_report_items(
        &self,
        backup: &LoadedBackup,
        steps: &[Step],
        report: &mut SchemaRestoreReport,
    ) {
        report.items = backup
            .versions
            .iter()
            .zip(steps)
            .map(|(v, s)| SchemaRestoreItem {
                subject: v.subject.clone(),
                target_subject: v.target_subject.clone(),
                version: v.schema.version,
                target_version: s.target_version.or_else(|| {
                    (s.action == SchemaRestoreAction::Register).then_some(v.schema.version)
                }),
                source_id: v.schema.id,
                target_id: s
                    .target_id
                    .or_else(|| (s.action == SchemaRestoreAction::Register).then_some(v.schema.id)),
                action: s.action,
                soft_deleted: v.soft_deleted,
                detail: s.detail.clone(),
            })
            .collect();
        report.conflicts = report
            .items
            .iter()
            .filter(|i| i.action == SchemaRestoreAction::Conflict)
            .map(|i| {
                format!(
                    "{} v{} (ID {}): {}",
                    i.target_subject,
                    i.version,
                    i.source_id,
                    i.detail.as_deref().unwrap_or("conflict")
                )
            })
            .collect();
    }

    async fn check_capacity(
        &self,
        opts: &SchemaRestoreOptions,
        steps: &[Step],
        report: &mut SchemaRestoreReport,
    ) -> Result<(), SchemaRegistryError> {
        let required = steps
            .iter()
            .filter(|s| {
                (s.action == SchemaRestoreAction::Register && !s.revive)
                    || s.action == SchemaRestoreAction::Rewrite
            })
            .count() as u64;
        let Some(limit) = opts.max_schema_versions else {
            if self.client.base_url().contains(".confluent.cloud") && required > 0 {
                report.warnings.push(format!(
                    "Confluent Cloud limits schema versions per Stream Governance package and counts \
                     soft-deleted versions; this restore adds {required}. Set \
                     schema_registry.restore.max_schema_versions to check the limit before \
                     the restore starts"
                ));
            }
            return Ok(());
        };
        let used = self.client.count_schema_versions().await?;
        report.capacity = Some(SchemaCapacityCheck {
            limit,
            used,
            required,
        });
        if used + required > limit {
            return Err(SchemaRegistryError::CapacityExceeded(format!(
                "the target holds {used} schema version(s) (soft-deleted included) and the restore adds \
                 {required}, over the limit of {limit}; nothing was changed. Hard-delete unused schemas \
                 or raise the limit"
            )));
        }
        Ok(())
    }

    /// The restore opens a subject (or context) for import with a temporary
    /// mode override and removes the override afterwards. A registry that
    /// cannot delete mode overrides (Confluent Platform 6.2) would be left
    /// with them, so a restore that needs one on a subject with an inherited
    /// mode is refused before any change.
    async fn check_mode_cleanup(
        &self,
        opts: &SchemaRestoreOptions,
        backup: &LoadedBackup,
        steps: &[Step],
    ) -> Result<(), SchemaRegistryError> {
        let mut candidates: BTreeSet<String> = BTreeSet::new();
        match opts.strategy {
            SchemaRestoreStrategy::IntoExisting => {
                for (item, step) in backup.versions.iter().zip(steps) {
                    let writes = matches!(
                        step.action,
                        SchemaRestoreAction::Register | SchemaRestoreAction::Rewrite
                    ) || step.soft_delete_after;
                    if writes {
                        candidates.insert(item.target_subject.clone());
                    }
                }
            }
            SchemaRestoreStrategy::PreserveIds => {
                if steps
                    .iter()
                    .any(|s| s.action == SchemaRestoreAction::Register)
                {
                    candidates.extend(
                        backup
                            .versions
                            .iter()
                            .map(|v| context::context_of(&v.target_subject))
                            .filter_map(context::context_subject),
                    );
                }
            }
        }
        let mut inherited = Vec::new();
        for subject in candidates {
            // A subject that gets its own mode from the backup keeps an
            // override, so nothing has to be deleted.
            let keeps_override = backup
                .target_to_source
                .get(&subject)
                .is_some_and(|source| backup.subjects[source].mode.is_some());
            if keeps_override {
                continue;
            }
            if self.client.get_subject_mode(&subject).await?.is_none() {
                inherited.push(subject);
            }
        }
        let Some(probe) = inherited.first() else {
            return Ok(());
        };
        if self.client.supports_mode_delete(probe).await? == Some(false) {
            let mut sample: Vec<String> = inherited.iter().take(5).cloned().collect();
            if inherited.len() > 5 {
                sample.push("…".to_string());
            }
            return Err(SchemaRegistryError::RestoreConflict(format!(
                "the target registry cannot delete mode overrides (Confluent Platform 6.2 and \
                 earlier), so the temporary IMPORT mode this restore needs on {} subject(s) with an \
                 inherited mode ({}) could not be removed afterwards; nothing was changed. Restore \
                 into Schema Registry 7.0 or later",
                inherited.len(),
                sample.join(", ")
            )));
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Execute
    // ------------------------------------------------------------------

    async fn execute(
        &self,
        opts: &SchemaRestoreOptions,
        backup: &LoadedBackup,
        steps: &mut [Step],
        targets: &mut HashMap<String, TargetSubject>,
        guard: &mut ModeGuard<'_>,
        report: &mut SchemaRestoreReport,
    ) -> Result<(), SchemaRegistryError> {
        let preserve = opts.strategy == SchemaRestoreStrategy::PreserveIds;
        let registers = steps
            .iter()
            .any(|s| s.action == SchemaRestoreAction::Register);

        if preserve && registers {
            // Global IMPORT does not reach non-default contexts: each context
            // in the restore gets context-level IMPORT too.
            guard.set_global(IMPORT).await?;
            let contexts: BTreeSet<&str> = backup
                .versions
                .iter()
                .map(|v| context::context_of(&v.target_subject))
                .filter(|c| *c != context::DEFAULT_CONTEXT)
                .collect();
            for ctx in contexts {
                let subject = context::context_subject(ctx).expect("non-default context");
                guard.set(&subject, IMPORT, false).await?;
            }
        }

        // Registration, in reference order.
        let mut version_map: HashMap<(String, i32), i32> = HashMap::new();
        let mut pending_deletes: Vec<usize> = Vec::new();
        for (index, item) in backup.versions.iter().enumerate() {
            let request = self.request_for(item, &version_map, opts);
            let subject = &item.target_subject;
            if opts.rewrite_ids {
                // Re-check against the target as it is now: a normal
                // registration earlier in this run may have taken a version
                // number or an ID (the registry assigns max ID + 1), or a
                // referenced version may have been renumbered.
                let planned = self.request_for(item, &HashMap::new(), opts);
                let refs_changed = planned.references != request.references;
                let fresh = matches!(
                    steps[index].action,
                    SchemaRestoreAction::Register | SchemaRestoreAction::Rewrite
                );
                if refs_changed || fresh {
                    targets.remove(subject);
                    steps[index] = self.classify(item, &request, true, targets).await?;
                }
            }
            match steps[index].action {
                SchemaRestoreAction::Register => {
                    self.make_writable(opts, guard, subject, IMPORT).await?;
                    let response = self
                        .client
                        .register_schema(subject, &request)
                        .await
                        .map_err(|e| registration_error(subject, item, e))?;
                    if response.id != item.schema.id {
                        return Err(SchemaRegistryError::Other(format!(
                            "{subject} v{}: the target assigned ID {} instead of {}",
                            item.schema.version, response.id, item.schema.id
                        )));
                    }
                    steps[index].target_id = Some(response.id);
                    steps[index].target_version = Some(item.schema.version);
                    report.versions_registered += 1;
                    debug!(subject = %subject, version = item.schema.version, id = response.id, "Registered");
                }
                SchemaRestoreAction::Rewrite => {
                    // Normal registration needs READWRITE.
                    guard.set(subject, READWRITE, false).await?;
                    let plain = RegisterSchemaRequest {
                        id: None,
                        version: None,
                        ..request.clone()
                    };
                    let response = self
                        .client
                        .register_schema(subject, &plain)
                        .await
                        .map_err(|e| registration_error(subject, item, e))?;
                    let version = match response.version {
                        Some(v) => v,
                        None => self
                            .client
                            .lookup_schema(subject, &plain)
                            .await?
                            .map(|s| s.version)
                            .ok_or_else(|| {
                                SchemaRegistryError::Other(format!(
                                    "{subject}: schema registered as ID {} but not found by lookup",
                                    response.id
                                ))
                            })?,
                    };
                    version_map.insert((subject.clone(), item.schema.version), version);
                    steps[index].target_id = Some(response.id);
                    steps[index].target_version = Some(version);
                    report.versions_rewritten += 1;
                    info!(
                        subject = %subject,
                        version = item.schema.version,
                        source_id = item.schema.id,
                        target_id = response.id,
                        target_version = version,
                        "Registered with a new ID (rewrite_ids)"
                    );
                }
                SchemaRestoreAction::MapExisting => {
                    report.versions_skipped += 1;
                    if item.soft_deleted {
                        report.warnings.push(format!(
                            "{subject} v{} was soft-deleted in the source; the target's own copy (ID {}) is left as it is",
                            item.schema.version,
                            steps[index].target_id.unwrap_or_default()
                        ));
                    }
                }
                SchemaRestoreAction::Skip => report.versions_skipped += 1,
                SchemaRestoreAction::Conflict => {
                    return Err(SchemaRegistryError::RestoreConflict(format!(
                        "{subject} v{} (ID {}): {}",
                        item.schema.version,
                        item.schema.id,
                        steps[index].detail.as_deref().unwrap_or("conflict")
                    )));
                }
            }
            // Register keeps the original ID (target_id unset).
            match steps[index].target_id {
                Some(target_id) if target_id != item.schema.id => {
                    report.id_mapping.insert(item.schema.id, target_id);
                }
                _ => {
                    report.ids_preserved.insert(item.schema.id);
                }
            }
            if steps[index].soft_delete_after {
                pending_deletes.push(index);
            }
        }

        // Soft-deletes, referrers before the versions they reference.
        for &index in pending_deletes.iter().rev() {
            let item = &backup.versions[index];
            let subject = &item.target_subject;
            let version = steps[index].target_version.unwrap_or(item.schema.version);
            self.make_writable(opts, guard, subject, IMPORT).await?;
            self.client
                .delete_schema_version(subject, version, false)
                .await
                .map_err(|e| {
                    SchemaRegistryError::Other(format!(
                        "cannot soft-delete {subject} v{version} again: {e}"
                    ))
                })?;
            report.versions_soft_deleted += 1;
        }

        // Compatibility: subjects, contexts, global.
        for (target_subject, source_subject) in &backup.target_to_source {
            let Some(compat) = backup.subjects[source_subject].compatibility.as_deref() else {
                continue;
            };
            let current = self
                .client
                .get_subject_config(target_subject)
                .await?
                .and_then(|c| c.compatibility_level);
            if current.as_deref() != Some(compat) {
                self.make_writable(opts, guard, target_subject, IMPORT)
                    .await?;
                self.client
                    .set_subject_config(target_subject, compat)
                    .await?;
            }
        }
        if opts.applies_global_config() {
            for (ctx, settings) in &backup.global.contexts {
                if let (Some(compat), Some(subject)) = (
                    settings.compatibility.as_deref(),
                    context::context_subject(ctx),
                ) {
                    self.client.set_subject_config(&subject, compat).await?;
                }
            }
            self.client
                .set_global_config(&backup.global.compatibility_level)
                .await?;
        }

        // Final modes: subjects, then contexts, then global.
        let mut subjects: BTreeSet<String> = guard.changed_subjects();
        subjects.extend(
            backup
                .target_to_source
                .iter()
                .filter(|(_, src)| backup.subjects[*src].mode.is_some())
                .map(|(target, _)| target.clone()),
        );
        let contexts: BTreeMap<String, Option<String>> = if opts.applies_global_config() {
            backup
                .global
                .contexts
                .iter()
                .filter_map(|(ctx, s)| context::context_subject(ctx).map(|c| (c, s.mode.clone())))
                .collect()
        } else {
            BTreeMap::new()
        };
        for subject in subjects {
            if is_context_subject(&subject) {
                continue;
            }
            let desired = match backup.target_to_source.get(&subject) {
                Some(src) => match &backup.subjects[src].mode {
                    Some(mode) => Some(mode.clone()),
                    None => guard.prior(&subject).await?,
                },
                None => guard.prior(&subject).await?,
            };
            guard.finish(&subject, desired).await?;
        }
        let mut context_subjects: BTreeSet<String> = guard
            .changed_subjects()
            .into_iter()
            .filter(|s| is_context_subject(s))
            .collect();
        context_subjects.extend(
            contexts
                .iter()
                .filter(|(_, m)| m.is_some())
                .map(|(c, _)| c.clone()),
        );
        for subject in context_subjects {
            let desired = match contexts.get(&subject).cloned().flatten() {
                Some(mode) => Some(mode),
                None => guard.prior(&subject).await?,
            };
            guard.finish(&subject, desired).await?;
        }
        let global_desired = if opts.applies_global_config() {
            backup.global.mode.clone()
        } else {
            None
        };
        guard.finish_global(global_desired).await?;
        if backup.global.mode.as_deref() == Some(IMPORT) && opts.applies_global_config() {
            report.warnings.push(
                "the backup's global mode is IMPORT, so the target is left in IMPORT mode as in the source"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Make `subject` accept `mode`-type writes. `into_existing` always uses a
    /// subject-level override; `preserve_ids` relies on the global/context
    /// IMPORT unless the subject has its own override.
    async fn make_writable(
        &self,
        opts: &SchemaRestoreOptions,
        guard: &mut ModeGuard<'_>,
        subject: &str,
        mode: &str,
    ) -> Result<(), SchemaRegistryError> {
        if guard.current(subject).await?.as_deref() == Some(mode) {
            return Ok(());
        }
        let needs_override = match opts.strategy {
            SchemaRestoreStrategy::IntoExisting => true,
            SchemaRestoreStrategy::PreserveIds => guard.current(subject).await?.is_some(),
        };
        if needs_override {
            guard.set(subject, mode, mode == IMPORT).await?;
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // Verify
    // ------------------------------------------------------------------

    async fn verify(
        &self,
        opts: &SchemaRestoreOptions,
        backup: &LoadedBackup,
        steps: &[Step],
    ) -> Result<SchemaVerification, SchemaRegistryError> {
        let mut verification = SchemaVerification::default();
        let mut live: HashMap<String, BTreeSet<i32>> = HashMap::new();
        let mut version_map: HashMap<(String, i32), i32> = HashMap::new();
        for (item, step) in backup.versions.iter().zip(steps) {
            if let Some(v) = step.target_version {
                version_map.insert((item.target_subject.clone(), item.schema.version), v);
            }
        }

        for (item, step) in backup.versions.iter().zip(steps) {
            let subject = &item.target_subject;
            let version = step.target_version.unwrap_or(item.schema.version);
            let expected_id = step.target_id.unwrap_or(item.schema.id);
            let label = format!("{subject} v{version}");
            verification.versions_checked += 1;
            let Some(actual) = self
                .client
                .find_schema_version(subject, version, true)
                .await?
            else {
                verification.mismatches.push(format!("{label}: missing"));
                continue;
            };
            if actual.id != expected_id {
                verification.mismatches.push(format!(
                    "{label}: ID {} (expected {expected_id})",
                    actual.id
                ));
            }
            let request = self.request_for(item, &version_map, opts);
            if !same_schema(
                subject,
                &actual.schema,
                &actual.schema_type,
                &actual.references,
                &request,
            ) {
                verification
                    .mismatches
                    .push(format!("{label}: schema differs from the backup"));
            } else if actual.schema != item.schema.schema {
                debug!(subject = %subject, version, "Schema text differs in formatting only");
            }
            for (field, expected, got) in [
                ("metadata", &item.schema.metadata, &actual.metadata),
                ("ruleSet", &item.schema.rule_set, &actual.rule_set),
            ] {
                if expected.is_some() && expected != got {
                    verification.warnings.push(format!(
                        "{label}: the target {} the {field} from the backup{}",
                        if got.is_none() { "dropped" } else { "changed" },
                        if field == "ruleSet" && got.is_none() {
                            " (data-contract rules need the rule-set extension in the target registry)"
                        } else {
                            ""
                        }
                    ));
                }
            }
            if step.action != SchemaRestoreAction::MapExisting {
                if !live.contains_key(subject) {
                    let versions = self
                        .client
                        .find_versions(subject, false)
                        .await?
                        .unwrap_or_default();
                    live.insert(subject.clone(), versions.into_iter().collect());
                }
                let is_live = live[subject].contains(&version);
                if is_live == item.soft_deleted {
                    verification.mismatches.push(format!(
                        "{label}: {} in the target, {} in the backup",
                        if is_live { "live" } else { "soft-deleted" },
                        if item.soft_deleted {
                            "soft-deleted"
                        } else {
                            "live"
                        }
                    ));
                }
            }
        }

        for (target, source) in &backup.target_to_source {
            let meta = &backup.subjects[source];
            if let Some(expected) = meta.compatibility.as_deref() {
                let actual = self
                    .client
                    .get_subject_config(target)
                    .await?
                    .and_then(|c| c.compatibility_level);
                if actual.as_deref() != Some(expected) {
                    verification.mismatches.push(format!(
                        "{target}: compatibility {} (expected {expected})",
                        actual.as_deref().unwrap_or("not set")
                    ));
                }
            }
            if let Some(expected) = meta.mode.as_deref() {
                let actual = self.client.get_subject_mode(target).await?.map(|m| m.mode);
                if actual.as_deref() != Some(expected) {
                    verification.mismatches.push(format!(
                        "{target}: mode {} (expected {expected})",
                        actual.as_deref().unwrap_or("not set")
                    ));
                }
            }
        }
        if opts.applies_global_config() {
            let compat = self.client.get_global_config().await?.compatibility_level;
            if compat.as_deref() != Some(backup.global.compatibility_level.as_str()) {
                verification.mismatches.push(format!(
                    "global compatibility {} (expected {})",
                    compat.as_deref().unwrap_or("not set"),
                    backup.global.compatibility_level
                ));
            }
            if let Some(expected) = backup.global.mode.as_deref() {
                let mode = self.client.get_global_mode().await?.map(|m| m.mode);
                if mode.as_deref() != Some(expected) {
                    verification.mismatches.push(format!(
                        "global mode {} (expected {expected})",
                        mode.as_deref().unwrap_or("not set")
                    ));
                }
            }
        }
        Ok(verification)
    }
}

impl Step {
    fn register(soft_deleted: bool) -> Self {
        Step {
            action: SchemaRestoreAction::Register,
            detail: None,
            revive: false,
            soft_delete_after: soft_deleted,
            target_version: None,
            target_id: None,
        }
    }

    fn rewrite(detail: String, soft_deleted: bool) -> Self {
        Step {
            action: SchemaRestoreAction::Rewrite,
            detail: Some(detail),
            revive: false,
            soft_delete_after: soft_deleted,
            target_version: None,
            target_id: None,
        }
    }

    fn conflict(detail: String) -> Self {
        Step {
            action: SchemaRestoreAction::Conflict,
            detail: Some(detail),
            revive: false,
            soft_delete_after: false,
            target_version: None,
            target_id: None,
        }
    }
}

/// Two backed-up schemas can claim the same original ID in one target
/// context when `subject_mapping` moves a subject between contexts (each
/// context has its own ID space). The first claim wins; a later, different
/// schema with that ID is a conflict (or, with `rewrite_ids`, a rewrite).
fn claim_ids(backup: &LoadedBackup, steps: &mut [Step], rewrite: bool) {
    let mut claimed: HashMap<(String, i32), usize> = HashMap::new();
    for (i, item) in backup.versions.iter().enumerate() {
        if steps[i].action != SchemaRestoreAction::Register {
            continue;
        }
        let key = (
            context::context_of(&item.target_subject).to_string(),
            item.schema.id,
        );
        let Some(&first) = claimed.get(&key) else {
            claimed.insert(key, i);
            continue;
        };
        let other = &backup.versions[first];
        let same = other.schema.schema == item.schema.schema
            && other.schema.schema_type == item.schema.schema_type
            && other.schema.references.len() == item.schema.references.len();
        if !same {
            let detail = format!(
                "ID {} is also used by {} v{} for a different schema in the same target context",
                item.schema.id, other.target_subject, other.schema.version
            );
            steps[i] = if rewrite {
                Step::rewrite(detail, item.soft_deleted)
            } else {
                Step::conflict(detail)
            };
        }
    }
}

fn is_context_subject(subject: &str) -> bool {
    subject.starts_with(":.") && subject.ends_with(':') && context::split(subject).1.is_empty()
}

/// Map one reference to the target: subject through `subject_mapping`,
/// version through the versions the restore renumbered. A reference the
/// mapping does not touch keeps its original (possibly relative) form.
fn map_reference(
    item: &SourceVersion,
    reference: &SchemaReference,
    version_map: &HashMap<(String, i32), i32>,
    opts: &SchemaRestoreOptions,
) -> SchemaReference {
    let resolved = context::resolve_reference(&item.subject, &reference.subject);
    let mapped = opts
        .subject_mapping
        .iter()
        .find(|(k, _)| context::normalize(k) == resolved)
        .map(|(_, v)| context::normalize(v))
        .unwrap_or_else(|| resolved.clone());
    let version = version_map
        .get(&(mapped.clone(), reference.version))
        .copied()
        .unwrap_or(reference.version);
    let same_context =
        context::context_of(&item.subject) == context::context_of(&item.target_subject);
    let subject = if mapped == resolved && same_context {
        reference.subject.clone()
    } else if context::context_of(&mapped) == context::context_of(&item.target_subject) {
        context::split(&mapped).1.to_string()
    } else {
        mapped
    };
    SchemaReference {
        name: reference.name.clone(),
        subject,
        version,
    }
}

/// Whether a registered schema (text, type, references) is the one in
/// `request`. References compare by name, resolved subject and version.
/// Avro and JSON Schema text also match when only the formatting differs.
fn same_schema(
    subject: &str,
    schema: &str,
    schema_type: &str,
    references: &[SchemaReference],
    request: &RegisterSchemaRequest,
) -> bool {
    let request_type = request.schema_type.as_deref().unwrap_or("AVRO");
    if !schema_type.eq_ignore_ascii_case(request_type) {
        return false;
    }
    let resolve = |refs: &[SchemaReference]| -> Vec<(String, String, i32)> {
        refs.iter()
            .map(|r| {
                (
                    r.name.clone(),
                    context::resolve_reference(subject, &r.subject),
                    r.version,
                )
            })
            .collect()
    };
    if resolve(references) != resolve(&request.references) {
        return false;
    }
    if schema == request.schema {
        return true;
    }
    if request_type.eq_ignore_ascii_case("PROTOBUF") {
        return false;
    }
    match (
        serde_json::from_str::<serde_json::Value>(schema),
        serde_json::from_str::<serde_json::Value>(&request.schema),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

/// The record rewrite (kafka-backup-core `schema_id_mapping`) is one global
/// map for every restored topic; it cannot tell contexts apart. Refuse a
/// rewrite when a rewritten ID is also used in another context.
fn check_rewrite_is_unambiguous(
    backup: &LoadedBackup,
    steps: &[Step],
) -> Result<(), SchemaRegistryError> {
    let mut contexts_by_id: BTreeMap<i32, BTreeSet<&str>> = BTreeMap::new();
    for v in &backup.versions {
        contexts_by_id
            .entry(v.schema.id)
            .or_default()
            .insert(context::context_of(&v.target_subject));
    }
    let mut ambiguous = BTreeSet::new();
    for (v, s) in backup.versions.iter().zip(steps) {
        let rewritten = matches!(
            s.action,
            SchemaRestoreAction::Rewrite | SchemaRestoreAction::MapExisting
        );
        if rewritten && contexts_by_id[&v.schema.id].len() > 1 {
            ambiguous.insert(v.schema.id);
        }
    }
    if ambiguous.is_empty() {
        return Ok(());
    }
    Err(SchemaRegistryError::RestoreConflict(format!(
        "rewrite_ids would rewrite schema ID(s) {:?}, which are also used in another context; the record \
         rewrite applies one ID map to every restored topic, so the result would be ambiguous. Restore \
         the contexts separately (subject filters / topic selection) or resolve the conflicts",
        ambiguous
    )))
}

fn registration_error(
    subject: &str,
    item: &SourceVersion,
    error: SchemaRegistryError,
) -> SchemaRegistryError {
    match error {
        SchemaRegistryError::Api {
            status: 422,
            error_code: 403,
            message,
        } => SchemaRegistryError::CapacityExceeded(format!(
            "{message} while registering {subject} v{}; the registry's schema limit counts soft-deleted \
             versions (set max_schema_versions to check before the restore starts)",
            item.schema.version
        )),
        SchemaRegistryError::Api {
            status,
            error_code,
            message,
        } => SchemaRegistryError::Api {
            status,
            error_code,
            message: format!(
                "registering {subject} v{} (ID {}): {message}",
                item.schema.version, item.schema.id
            ),
        },
        other => other,
    }
}

// ----------------------------------------------------------------------
// Mode guard
// ----------------------------------------------------------------------

/// Records the target's prior global, context and subject modes before the
/// restore changes them, and puts them back on failure or cancellation.
///
/// Before the guard puts a mode back (or sets its final value on success) it
/// reads the mode the target has now. If that is neither the prior value nor
/// a value the guard wrote, someone else changed it while the restore ran:
/// the guard leaves it alone and reports it ([`Self::take_foreign_changes`]).
///
/// Cleanup is explicit ([`ModeGuard::rollback`]) because it needs `await`;
/// a guard dropped while armed (a panic or a forgotten rollback) logs the
/// modes it could not restore.
pub(crate) struct ModeGuard<'a> {
    client: &'a SchemaRegistryClient,
    /// Subject (or `:.ctx:`) -> override before the restore; `None` = unset.
    priors: BTreeMap<String, Option<String>>,
    /// Subject -> override now, as far as the guard knows.
    current: BTreeMap<String, Option<String>>,
    /// Subject -> every value the guard wrote (or was writing when it was
    /// interrupted).
    written: BTreeMap<String, BTreeSet<Option<String>>>,
    /// Subjects whose override the guard changed.
    changed: BTreeSet<String>,
    /// Global mode before the restore, once the guard changed it.
    global_prior: Option<Option<String>>,
    global_current: Option<String>,
    global_written: BTreeSet<String>,
    /// Modes someone else changed during the restore, left as they are.
    foreign: Vec<String>,
    armed: bool,
}

fn show(mode: &Option<String>) -> &str {
    mode.as_deref().unwrap_or("(inherited)")
}

impl<'a> ModeGuard<'a> {
    pub(crate) fn new(client: &'a SchemaRegistryClient) -> Self {
        Self {
            client,
            priors: BTreeMap::new(),
            current: BTreeMap::new(),
            written: BTreeMap::new(),
            changed: BTreeSet::new(),
            global_prior: None,
            global_current: None,
            global_written: BTreeSet::new(),
            foreign: Vec::new(),
            armed: true,
        }
    }

    /// Modes that were changed by someone else while the restore ran and
    /// were therefore not put back.
    pub(crate) fn take_foreign_changes(&mut self) -> Vec<String> {
        std::mem::take(&mut self.foreign)
    }

    /// The subject's override before the restore (read once).
    pub(crate) async fn prior(
        &mut self,
        subject: &str,
    ) -> Result<Option<String>, SchemaRegistryError> {
        if let Some(prior) = self.priors.get(subject) {
            return Ok(prior.clone());
        }
        let mode = self.client.get_subject_mode(subject).await?.map(|m| m.mode);
        self.priors.insert(subject.to_string(), mode.clone());
        self.current.insert(subject.to_string(), mode.clone());
        Ok(mode)
    }

    /// The subject's override now.
    pub(crate) async fn current(
        &mut self,
        subject: &str,
    ) -> Result<Option<String>, SchemaRegistryError> {
        self.prior(subject).await?;
        Ok(self.current[subject].clone())
    }

    pub(crate) fn changed_subjects(&self) -> BTreeSet<String> {
        self.changed.clone()
    }

    pub(crate) async fn set(
        &mut self,
        subject: &str,
        mode: &str,
        force: bool,
    ) -> Result<(), SchemaRegistryError> {
        if self.current(subject).await?.as_deref() == Some(mode) {
            return Ok(());
        }
        // Recorded before the request: a cancelled or timed-out request may
        // still have been applied.
        self.changed.insert(subject.to_string());
        self.written
            .entry(subject.to_string())
            .or_default()
            .insert(Some(mode.to_string()));
        self.client.set_subject_mode(subject, mode, force).await?;
        self.current
            .insert(subject.to_string(), Some(mode.to_string()));
        debug!(subject = %subject, mode, "Mode set");
        Ok(())
    }

    async fn clear(&mut self, subject: &str) -> Result<(), SchemaRegistryError> {
        if self.current(subject).await?.is_none() {
            return Ok(());
        }
        self.changed.insert(subject.to_string());
        self.written
            .entry(subject.to_string())
            .or_default()
            .insert(None);
        self.client.delete_subject_mode(subject).await?;
        self.current.insert(subject.to_string(), None);
        debug!(subject = %subject, "Mode override removed");
        Ok(())
    }

    /// True when the subject's mode in the target is not one the guard knows
    /// about: someone else changed it. The change is recorded and adopted as
    /// the current value so nothing overwrites it.
    async fn changed_by_someone_else(
        &mut self,
        subject: &str,
    ) -> Result<bool, SchemaRegistryError> {
        if !self.changed.contains(subject) {
            return Ok(false);
        }
        let actual = self.client.get_subject_mode(subject).await?.map(|m| m.mode);
        let ours = self.priors.get(subject) == Some(&actual)
            || self.current.get(subject) == Some(&actual)
            || self
                .written
                .get(subject)
                .is_some_and(|values| values.contains(&actual));
        if ours {
            self.current.insert(subject.to_string(), actual);
            return Ok(false);
        }
        self.foreign.push(format!(
            "the mode of {subject} was changed to {} by someone else during the restore (it was {} \
             before); it was left as it is",
            show(&actual),
            show(&self.priors.get(subject).cloned().flatten()),
        ));
        self.current.insert(subject.to_string(), actual);
        Ok(true)
    }

    pub(crate) async fn set_global(&mut self, mode: &str) -> Result<(), SchemaRegistryError> {
        if self.global_prior.is_none() {
            let prior = self.client.get_global_mode().await?.map(|m| m.mode);
            self.global_current = prior.clone();
            self.global_prior = Some(prior);
        }
        if self.global_current.as_deref() == Some(mode) {
            return Ok(());
        }
        self.global_written.insert(mode.to_string());
        self.client.set_global_mode(mode).await?;
        self.global_current = Some(mode.to_string());
        debug!(mode, "Global mode set");
        Ok(())
    }

    /// [`Self::changed_by_someone_else`] for the global mode.
    async fn global_changed_by_someone_else(&mut self) -> Result<bool, SchemaRegistryError> {
        let Some(prior) = self.global_prior.clone() else {
            return Ok(false);
        };
        if self.global_written.is_empty() {
            return Ok(false);
        }
        let actual = self.client.get_global_mode().await?.map(|m| m.mode);
        let ours = actual == prior
            || actual == self.global_current
            || actual
                .as_ref()
                .is_some_and(|mode| self.global_written.contains(mode));
        if ours {
            self.global_current = actual;
            return Ok(false);
        }
        self.foreign.push(format!(
            "the global mode was changed to {} by someone else during the restore (it was {} \
             before); it was left as it is",
            show(&actual),
            show(&prior),
        ));
        self.global_current = actual;
        Ok(true)
    }

    /// Success path: leave `subject` at `desired` (`None` = no override).
    pub(crate) async fn finish(
        &mut self,
        subject: &str,
        desired: Option<String>,
    ) -> Result<(), SchemaRegistryError> {
        if self.changed_by_someone_else(subject).await? {
            return Ok(());
        }
        match desired {
            Some(mode) => {
                let force = mode == IMPORT;
                self.set(subject, &mode, force).await
            }
            None => self.clear(subject).await,
        }
    }

    /// Success path: leave the global mode at `desired`, or at its prior
    /// value when `desired` is `None` and the guard changed it.
    pub(crate) async fn finish_global(
        &mut self,
        desired: Option<String>,
    ) -> Result<(), SchemaRegistryError> {
        let target = match (desired, &self.global_prior) {
            (Some(mode), _) => mode,
            (None, Some(prior)) => prior.clone().unwrap_or_else(|| READWRITE.to_string()),
            (None, None) => return Ok(()),
        };
        if self.global_changed_by_someone_else().await? {
            return Ok(());
        }
        self.set_global(&target).await
    }

    /// Put every changed mode back to its prior value. Returns the modes
    /// that could not be restored.
    pub(crate) async fn rollback(&mut self) -> Vec<String> {
        let mut errors = Vec::new();
        // Subjects first, contexts next, global last (the reverse of the
        // order they were opened up in).
        let mut subjects: Vec<String> = self.changed.iter().cloned().collect();
        subjects.sort_by_key(|s| is_context_subject(s));
        for subject in subjects {
            let prior = self.priors.get(&subject).cloned().flatten();
            match self.changed_by_someone_else(&subject).await {
                Ok(true) => continue,
                Ok(false) => {}
                Err(e) => {
                    errors.push(format!(
                        "{subject}: cannot read the mode to restore {}: {e}",
                        show(&prior)
                    ));
                    continue;
                }
            }
            if self.current.get(&subject).cloned().flatten() == prior {
                continue;
            }
            let result = match &prior {
                Some(mode) => self
                    .client
                    .set_subject_mode(&subject, mode, mode == IMPORT)
                    .await
                    .map(|_| ()),
                None => self.client.delete_subject_mode(&subject).await,
            };
            match result {
                Ok(()) => {
                    self.current.insert(subject.clone(), prior.clone());
                    info!(subject = %subject, mode = ?prior, "Mode restored after failure");
                }
                Err(e) => errors.push(format!(
                    "{subject}: cannot restore mode {}: {e}",
                    show(&prior)
                )),
            }
        }
        if let Some(prior) = self.global_prior.clone() {
            let mode = prior.unwrap_or_else(|| READWRITE.to_string());
            match self.global_changed_by_someone_else().await {
                Ok(true) => {}
                Ok(false) if self.global_current.as_deref() == Some(mode.as_str()) => {}
                Ok(false) => match self.client.set_global_mode(&mode).await {
                    Ok(_) => {
                        self.global_current = Some(mode.clone());
                        info!(mode = %mode, "Global mode restored after failure");
                    }
                    Err(e) => errors.push(format!("global: cannot restore mode {mode}: {e}")),
                },
                Err(e) => errors.push(format!(
                    "global: cannot read the mode to restore {mode}: {e}"
                )),
            }
        }
        for e in &errors {
            error!("{e}");
        }
        for change in &self.foreign {
            warn!("{change}");
        }
        self.armed = false;
        errors
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ModeGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let pending: Vec<String> = self
            .changed
            .iter()
            .filter(|s| self.current.get(*s) != self.priors.get(*s))
            .map(|s| format!("{s} (was {})", show(&self.priors.get(s).cloned().flatten())))
            .collect();
        let global = self
            .global_prior
            .as_ref()
            .filter(|p| p.as_deref() != self.global_current.as_deref());
        if !pending.is_empty() || global.is_some() {
            error!(
                subjects = ?pending,
                global_was = ?global,
                "Schema Registry modes changed by the restore were not restored; set them back by hand"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(subject: &str, target: &str, refs: Vec<SchemaReference>) -> SourceVersion {
        SourceVersion {
            subject: subject.to_string(),
            target_subject: target.to_string(),
            schema: SchemaVersion {
                subject: subject.to_string(),
                version: 1,
                id: 1,
                schema_type: "AVRO".into(),
                schema: "{}".into(),
                references: refs,
                metadata: None,
                rule_set: None,
                extra: BTreeMap::new(),
            },
            soft_deleted: false,
        }
    }

    fn reference(subject: &str, version: i32) -> SchemaReference {
        SchemaReference {
            name: "n".into(),
            subject: subject.into(),
            version,
        }
    }

    #[test]
    fn unmapped_references_keep_their_form() {
        let i = item(":.ctx:orders", ":.ctx:orders", vec![]);
        let r = map_reference(
            &i,
            &reference("address", 1),
            &HashMap::new(),
            &Default::default(),
        );
        assert_eq!(r.subject, "address");
    }

    #[test]
    fn mapped_references_follow_the_mapping_and_renumbered_versions() {
        let opts = SchemaRestoreOptions {
            subject_mapping: [("address".to_string(), "addr-v2".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let i = item("orders", "orders", vec![]);
        let versions: HashMap<(String, i32), i32> =
            [(("addr-v2".to_string(), 1), 4)].into_iter().collect();
        let r = map_reference(&i, &reference("address", 1), &versions, &opts);
        assert_eq!((r.subject.as_str(), r.version), ("addr-v2", 4));
    }

    #[test]
    fn a_referrer_moved_out_of_its_context_qualifies_its_references() {
        let opts = SchemaRestoreOptions {
            subject_mapping: [(":.ctx:orders".to_string(), "orders".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let i = item(":.ctx:orders", "orders", vec![]);
        let r = map_reference(&i, &reference("address", 1), &HashMap::new(), &opts);
        assert_eq!(r.subject, ":.ctx:address");
    }

    #[test]
    fn same_schema_compares_type_references_and_formatting() {
        let req = RegisterSchemaRequest {
            schema: r#"{"type":"record","name":"A","fields":[]}"#.into(),
            schema_type: Some("AVRO".into()),
            references: vec![reference("b", 1)],
            id: None,
            version: None,
            metadata: None,
            rule_set: None,
        };
        let pretty = "{ \"type\": \"record\", \"name\": \"A\", \"fields\": [] }";
        assert!(same_schema("a", pretty, "AVRO", &[reference("b", 1)], &req));
        assert!(!same_schema(
            "a",
            pretty,
            "JSON",
            &[reference("b", 1)],
            &req
        ));
        assert!(!same_schema(
            "a",
            pretty,
            "AVRO",
            &[reference("b", 2)],
            &req
        ));
        assert!(!same_schema("a", pretty, "AVRO", &[], &req));
        // A qualified reference to the same context is the same reference.
        assert!(same_schema(
            ":.c:a",
            pretty,
            "AVRO",
            &[reference(":.c:b", 1)],
            &RegisterSchemaRequest {
                references: vec![reference("b", 1)],
                ..req.clone()
            }
        ));
        // Protobuf text must match exactly.
        let proto = RegisterSchemaRequest {
            schema: "syntax = \"proto3\";".into(),
            schema_type: Some("PROTOBUF".into()),
            references: vec![],
            ..req
        };
        assert!(!same_schema(
            "p",
            "syntax =  \"proto3\";",
            "PROTOBUF",
            &[],
            &proto
        ));
    }

    #[test]
    fn context_subjects_are_recognised() {
        assert!(is_context_subject(":.ctx-a:"));
        assert!(!is_context_subject(":.ctx-a:orders"));
        assert!(!is_context_subject("orders"));
    }

    #[test]
    fn report_keys() {
        assert_eq!(
            report_key("b1", false),
            "b1/restore/schema-registry-report.json"
        );
        assert_eq!(
            report_key("b1", true),
            "b1/restore/schema-registry-report.dry-run.json"
        );
    }
}
