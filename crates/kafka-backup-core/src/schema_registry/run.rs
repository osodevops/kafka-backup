//! What the commands call: one snapshot pass for a backup, and the registry
//! restore that runs before the records of a restore.
//!
//! Call [`crate::oauth::install`] on the configuration first when the
//! registry authenticates with OAuth: it attaches the token cache that the
//! Kafka connections of the same provider use.

use std::collections::HashMap;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::config::{Config, Mode};
use crate::storage::StorageBackend;

use super::backup::SchemaRegistryBackup;
use super::client::SchemaRegistryClient;
use super::config::SchemaRegistryConfig;
use super::error::SchemaRegistryError;
use super::models::{SchemaRegistryManifest, SchemaRestoreReport};
use super::restore::{report_key, SchemaRegistryRestore};

/// The `schema_registry` section when it is present and enabled.
pub fn enabled(config: &Config) -> Option<&SchemaRegistryConfig> {
    config.schema_registry.as_ref().filter(|sr| sr.enabled)
}

/// Take one snapshot of the registry in `config.schema_registry` into
/// `{backup_id}/schema-registry/` of `storage`.
pub async fn snapshot(
    config: &Config,
    storage: Arc<dyn StorageBackend>,
) -> Result<SchemaRegistryManifest, SchemaRegistryError> {
    let sr = enabled(config).cloned().ok_or_else(|| {
        SchemaRegistryError::Config(
            "no enabled schema_registry section in the configuration".to_string(),
        )
    })?;
    sr.validate()
        .map_err(|e| SchemaRegistryError::Config(e.to_string()))?;
    let client = SchemaRegistryClient::new(&sr)?;
    SchemaRegistryBackup::new(client, storage, config.backup_id.clone(), sr)
        .run()
        .await
}

/// What a restore-shaped command asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreScope {
    /// `restore --schema-only`: the registry only.
    SchemaOnly,
    /// A full restore: the registry (when configured), then the records.
    BeforeRecords,
}

/// The schema ID rewrite a registry restore handed to the record restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdRewrite {
    /// IDs that changed.
    pub changed: usize,
    /// `restore.schema_id_rewrite_topics` after the hand-over (empty: every
    /// restored topic).
    pub topics: Vec<String>,
    /// `restore.schema_id_rewrite_strict`.
    pub strict: bool,
}

/// The result of [`restore`] or [`preview`].
#[derive(Debug)]
pub struct RegistryRestoreOutcome {
    pub report: SchemaRestoreReport,
    /// Where the report was written in the backup storage (`None` for a
    /// preview, which writes nothing).
    pub report_key: Option<String>,
    /// Why the restore failed. The records must not be restored.
    pub error: Option<SchemaRegistryError>,
    /// Set when `rewrite_ids` changed IDs and `config.restore` was updated.
    pub id_rewrite: Option<IdRewrite>,
}

impl RegistryRestoreOutcome {
    pub fn success(&self) -> bool {
        self.error.is_none()
    }
}

/// The section to restore with, or `None` when a full restore has no
/// (enabled) registry configured.
fn selected(config: &Config, scope: RestoreScope) -> crate::Result<Option<SchemaRegistryConfig>> {
    match (enabled(config), scope) {
        (Some(sr), _) => {
            sr.validate()?;
            Ok(Some(sr.clone()))
        }
        (None, RestoreScope::BeforeRecords) => Ok(None),
        (None, RestoreScope::SchemaOnly) => Err(crate::Error::Config(
            if config.schema_registry.is_some() {
                "--schema-only restores the Schema Registry, but schema_registry.enabled is false"
            } else {
                "--schema-only restores the Schema Registry, but the configuration has no \
                 schema_registry section (its url is the target registry)"
            }
            .to_string(),
        )),
    }
}

/// A dry-run flag in either place makes the whole invocation a dry run:
/// `restore.dry_run` and `schema_registry.restore.dry_run`. Returns whether
/// it is one.
pub fn normalize_dry_run(config: &mut Config, force: bool) -> bool {
    let registry_dry = enabled(config)
        .and_then(|sr| sr.restore.as_ref())
        .is_some_and(|r| r.dry_run);
    let dry = force || registry_dry || config.restore.as_ref().is_some_and(|r| r.dry_run);
    if dry {
        config.restore.get_or_insert_with(Default::default).dry_run = true;
        if let Some(sr) = config.schema_registry.as_mut().filter(|sr| sr.enabled) {
            sr.restore.get_or_insert_with(Default::default).dry_run = true;
        }
    }
    dry
}

/// Restore the Schema Registry if the configuration asks for it.
///
/// Returns `Ok(None)` when a full restore has no enabled `schema_registry`
/// section. Otherwise the outcome carries the report; when
/// [`RegistryRestoreOutcome::error`] is set the records must not be
/// restored. A configuration problem is an `Err`, raised before anything
/// connects.
///
/// For a full restore this checks the record-restore configuration before
/// the registry is changed and, when `rewrite_ids` changed IDs, hands the
/// source-to-target ID map to `restore.rewrite_schema_ids` /
/// `restore.schema_id_mapping`, limited to the restored topics unless
/// `restore.schema_id_rewrite_topics` is already set.
pub async fn restore(
    config: &mut Config,
    storage: Arc<dyn StorageBackend>,
    scope: RestoreScope,
    cancel: CancellationToken,
) -> crate::Result<Option<RegistryRestoreOutcome>> {
    normalize_dry_run(config, false);
    let Some(sr) = selected(config, scope)? else {
        return Ok(None);
    };
    if scope == RestoreScope::BeforeRecords {
        // Reject a record-restore configuration the engine would refuse
        // before the registry is changed.
        if config.mode != Mode::Restore {
            return Err(crate::Error::Config(
                "Configuration mode must be 'restore'".to_string(),
            ));
        }
        config.validate()?;
    }

    let dry_run = sr.restore_options().dry_run;
    let backup_id = config.backup_id.clone();
    let client = SchemaRegistryClient::new(&sr)?;
    info!(
        target = %client.base_url(),
        strategy = sr.restore_options().strategy.as_str(),
        dry_run,
        "Restoring the Schema Registry{}",
        if scope == RestoreScope::BeforeRecords {
            " before the records"
        } else {
            ""
        }
    );
    let (report, error) = SchemaRegistryRestore::new(client, storage, backup_id.clone(), sr)
        .run_with_cancel(cancel)
        .await;

    let mut outcome = RegistryRestoreOutcome {
        report,
        report_key: Some(report_key(&backup_id, dry_run)),
        error,
        id_rewrite: None,
    };
    if outcome.success() && scope == RestoreScope::BeforeRecords && !dry_run {
        if let Some(mapping) = outcome.report.rewrite_mapping() {
            let changed = outcome.report.id_mapping.len();
            outcome.id_rewrite = Some(feed_id_mapping(config, mapping, changed)?);
        }
    }
    Ok(Some(outcome))
}

/// The plan of [`restore`], with no change to the registry and no report
/// written (`validate-restore`).
pub async fn preview(
    config: &Config,
    storage: Arc<dyn StorageBackend>,
    scope: RestoreScope,
) -> crate::Result<Option<RegistryRestoreOutcome>> {
    let Some(sr) = selected(config, scope)? else {
        return Ok(None);
    };
    let client = SchemaRegistryClient::new(&sr)?;
    let (report, error) = SchemaRegistryRestore::new(client, storage, config.backup_id.clone(), sr)
        .preview()
        .await;
    Ok(Some(RegistryRestoreOutcome {
        report,
        report_key: None,
        error,
        id_rewrite: None,
    }))
}

/// Give the record restore the source-to-target schema ID map.
///
/// `mapping` holds every restored ID (unchanged ones map to themselves), so
/// the strict rewrite (`schema_id_rewrite_strict`, on by default) fails only
/// on an ID the Schema Registry backup does not have, not on every record
/// that kept its ID. The rewrite is limited to the restored topics
/// (`schema_id_rewrite_topics` = `target.topics.include`) unless the
/// configuration sets its own scope.
fn feed_id_mapping(
    config: &mut Config,
    mapping: HashMap<i32, i32>,
    changed: usize,
) -> crate::Result<IdRewrite> {
    let restored_topics = config
        .target
        .as_ref()
        .map(|t| t.topics.include.clone())
        .unwrap_or_default();
    let restore = config.restore.get_or_insert_with(Default::default);
    for (source, target) in &mapping {
        if let Some(existing) = restore.schema_id_mapping.get(source) {
            if existing != target {
                return Err(crate::Error::Config(format!(
                    "restore.schema_id_mapping maps schema ID {source} to {existing}, but the \
                     Schema Registry restore registered it as {target}"
                )));
            }
        }
    }
    for (source, target) in mapping {
        restore.schema_id_mapping.entry(source).or_insert(target);
    }
    restore.rewrite_schema_ids = true;
    if restore.schema_id_rewrite_topics.is_empty() {
        restore.schema_id_rewrite_topics = restored_topics;
    }
    Ok(IdRewrite {
        changed,
        topics: restore.schema_id_rewrite_topics.clone(),
        strict: restore.schema_id_rewrite_strict,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(extra: &str) -> Config {
        serde_yaml::from_str(&format!(
            "mode: restore\nbackup_id: b\ntarget:\n  bootstrap_servers: [\"k:9092\"]\n  topics:\n    include: [orders]\nstorage:\n  backend: memory\n{extra}"
        ))
        .unwrap()
    }

    #[test]
    fn a_dry_run_flag_anywhere_applies_to_the_whole_restore() {
        let mut cfg = config("schema_registry:\n  url: http://sr\n  restore:\n    dry_run: true\n");
        assert!(normalize_dry_run(&mut cfg, false));
        assert!(cfg.restore.as_ref().unwrap().dry_run);

        let mut cfg = config("schema_registry:\n  url: http://sr\nrestore:\n  dry_run: true\n");
        assert!(normalize_dry_run(&mut cfg, false));
        assert!(cfg.schema_registry.unwrap().restore_options().dry_run);

        let mut cfg = config("schema_registry:\n  url: http://sr\n");
        assert!(!normalize_dry_run(&mut cfg, false));
        assert!(cfg.restore.is_none());
        assert!(normalize_dry_run(&mut cfg, true));
        assert!(cfg.schema_registry.unwrap().restore_options().dry_run);
    }

    #[test]
    fn schema_only_needs_an_enabled_section_and_a_full_restore_does_not() {
        let none = config("");
        assert!(selected(&none, RestoreScope::BeforeRecords)
            .unwrap()
            .is_none());
        let err = selected(&none, RestoreScope::SchemaOnly).unwrap_err();
        assert!(err.to_string().contains("no schema_registry section"));

        let disabled = config("schema_registry:\n  url: http://sr\n  enabled: false\n");
        assert!(selected(&disabled, RestoreScope::BeforeRecords)
            .unwrap()
            .is_none());
        let err = selected(&disabled, RestoreScope::SchemaOnly).unwrap_err();
        assert!(err.to_string().contains("enabled is false"));

        let on = config("schema_registry:\n  url: http://sr\n");
        assert!(selected(&on, RestoreScope::BeforeRecords)
            .unwrap()
            .is_some());

        let invalid = config("schema_registry:\n  url: sr:8081\n");
        assert!(selected(&invalid, RestoreScope::BeforeRecords).is_err());
    }

    #[test]
    fn the_id_map_is_limited_to_the_restored_topics_and_keeps_explicit_settings() {
        let mut cfg = config("");
        let fed = feed_id_mapping(&mut cfg, HashMap::from([(1, 1), (2, 102)]), 1).unwrap();
        assert_eq!(fed.changed, 1);
        assert_eq!(fed.topics, ["orders"]);
        assert!(fed.strict);
        let restore = cfg.restore.as_ref().unwrap();
        assert!(restore.rewrite_schema_ids);
        assert_eq!(restore.schema_id_mapping[&2], 102);
        assert_eq!(restore.schema_id_mapping[&1], 1);

        let mut cfg = config(
            "restore:\n  schema_id_rewrite_topics: [payments]\n  schema_id_mapping:\n    7: 70\n",
        );
        let fed = feed_id_mapping(&mut cfg, HashMap::from([(2, 102)]), 1).unwrap();
        assert_eq!(fed.topics, ["payments"]);
        assert_eq!(cfg.restore.as_ref().unwrap().schema_id_mapping[&7], 70);

        let mut cfg = config("restore:\n  schema_id_mapping:\n    2: 5\n");
        let err = feed_id_mapping(&mut cfg, HashMap::from([(2, 102)]), 1).unwrap_err();
        assert!(err.to_string().contains("maps schema ID 2 to 5"), "{err}");
    }
}
