//! Schema Registry snapshot: subject discovery, version enumeration,
//! reference resolution, storage write.
//!
//! The snapshot is written under `{backup_id}/schema-registry/`
//! ([`super::snapshot`] describes the completeness marker and the
//! checksums). Every pass replaces the previous snapshot, so the periodic
//! snapshots of a continuous backup always leave the latest state.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use bytes::Bytes;
use tracing::{info, warn};

use crate::storage::StorageBackend;

use super::client::SchemaRegistryClient;
use super::config::{SchemaRegistryConfig, VersionSelection};
use super::context;
use super::error::SchemaRegistryError;
use super::models::*;
use super::ordering::{registration_order, VersionNode};
use super::snapshot::{self, Coordinate, SnapshotInfo};

/// Schema Registry backup engine.
pub struct SchemaRegistryBackup {
    client: SchemaRegistryClient,
    storage: Arc<dyn StorageBackend>,
    backup_id: String,
    config: SchemaRegistryConfig,
}

impl SchemaRegistryBackup {
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

    /// Take one snapshot. The completeness marker is `false` from the start
    /// of the pass until the manifest is durable; a pass that fails leaves it
    /// `false` with the error, and a restore refuses the snapshot.
    pub async fn run(&self) -> Result<SchemaRegistryManifest, SchemaRegistryError> {
        snapshot::mark_status(self.storage.as_ref(), &self.backup_id, false, None).await?;
        match self.run_snapshot().await {
            Ok(manifest) => {
                snapshot::mark_status(self.storage.as_ref(), &self.backup_id, true, None).await?;
                info!(
                    subjects = manifest.subjects_count,
                    versions = manifest.total_versions,
                    soft_deleted = manifest.soft_deleted_versions,
                    "Schema Registry snapshot complete"
                );
                Ok(manifest)
            }
            Err(e) => {
                let reason = e.to_string();
                if let Err(mark) = snapshot::mark_status(
                    self.storage.as_ref(),
                    &self.backup_id,
                    false,
                    Some(&reason),
                )
                .await
                {
                    warn!(error = %mark, "Cannot record why the Schema Registry snapshot is incomplete");
                }
                Err(e)
            }
        }
    }

    async fn run_snapshot(&self) -> Result<SchemaRegistryManifest, SchemaRegistryError> {
        let opts = self.config.backup_options();
        let pattern = |p: &String| {
            glob::Pattern::new(p).map_err(|e| {
                SchemaRegistryError::Config(format!("invalid subject pattern `{p}`: {e}"))
            })
        };
        let include: Vec<_> = opts
            .subjects
            .iter()
            .map(pattern)
            .collect::<Result<_, _>>()?;
        let exclude: Vec<_> = opts.exclude.iter().map(pattern).collect::<Result<_, _>>()?;

        let subjects = self.client.list_subjects(opts.include_soft_deleted).await?;
        let selected: BTreeSet<_> = subjects
            .iter()
            .filter(|s| matches_subject_filters(s, &include, &exclude))
            .cloned()
            .collect();

        // Global and per-context compatibility and mode.
        let global_config = self.client.get_global_config().await?;
        let global_mode = self.client.get_global_mode().await?.map(|m| m.mode);
        let mut contexts = BTreeMap::new();
        for subject in &subjects {
            let ctx = context::context_of(subject);
            let Some(context_subject) = context::context_subject(ctx) else {
                continue;
            };
            if contexts.contains_key(ctx) {
                continue;
            }
            let compatibility = self
                .client
                .get_subject_config(&context_subject)
                .await?
                .and_then(|c| c.compatibility_level);
            let mode = self
                .client
                .get_subject_mode(&context_subject)
                .await?
                .map(|m| m.mode);
            contexts.insert(
                ctx.to_string(),
                ContextSettings {
                    compatibility,
                    mode,
                },
            );
        }
        let global = serde_json::json!({
            "compatibility_level": global_config.compatibility_level.clone().unwrap_or_default(),
            "mode": global_mode,
            "config": global_config,
            "contexts": contexts,
        });
        self.write(&global_config_key(&self.backup_id), &global)
            .await?;

        // Versions in scope, then (include_references) what they reference.
        let mut queue = VecDeque::new();
        for subject in &selected {
            let mut versions = self
                .client
                .list_versions(subject, opts.include_soft_deleted)
                .await?;
            versions.sort_unstable();
            if opts.include_versions == VersionSelection::Latest {
                versions = versions.last().copied().into_iter().collect();
            }
            for version in versions {
                queue.push_back((subject.clone(), version));
            }
        }
        let mut schemas: BTreeMap<(String, i32), SchemaVersion> = BTreeMap::new();
        while let Some((subject, version)) = queue.pop_front() {
            if schemas.contains_key(&(subject.clone(), version)) {
                continue;
            }
            let sv = self
                .client
                .get_schema_version_with(&subject, version, opts.include_soft_deleted)
                .await?;
            if sv.subject != subject || sv.version != version {
                return Err(SchemaRegistryError::Other(format!(
                    "the registry returned {} v{} for {subject} v{version}",
                    sv.subject, sv.version
                )));
            }
            if opts.include_references {
                for r in &sv.references {
                    queue.push_back((context::resolve_reference(&subject, &r.subject), r.version));
                }
            }
            self.write(&version_key(&self.backup_id, &subject, version), &sv)
                .await?;
            schemas.insert((subject, version), sv);
        }

        // Per-subject metadata: compatibility, mode, which versions are
        // soft-deleted.
        let names: BTreeSet<String> = schemas.keys().map(|(s, _)| s.clone()).collect();
        let mut summaries = Vec::new();
        for subject in &names {
            let versions: Vec<&SchemaVersion> = schemas
                .iter()
                .filter(|((s, _), _)| s == subject)
                .map(|(_, v)| v)
                .collect();
            let config = self.client.get_subject_config(subject).await?;
            let mode = self.client.get_subject_mode(subject).await?.map(|m| m.mode);
            let active: Option<Vec<i32>> = if opts.include_soft_deleted {
                Some(
                    self.client
                        .find_versions(subject, false)
                        .await?
                        .unwrap_or_default(),
                )
            } else {
                None
            };
            let deleted_versions: Vec<i32> = versions
                .iter()
                .filter(|v| active.as_ref().is_some_and(|a| !a.contains(&v.version)))
                .map(|v| v.version)
                .collect();
            let last = versions.last().expect("a captured subject has a version");
            self.write(
                &subject_metadata_key(&self.backup_id, subject),
                &SubjectMetadata {
                    subject: subject.clone(),
                    schema_type: last.schema_type.clone(),
                    versions: versions.iter().map(|v| v.version).collect(),
                    compatibility: config.as_ref().and_then(|c| c.compatibility_level.clone()),
                    mode: mode.clone(),
                    is_soft_deleted: active.as_ref().is_some_and(Vec::is_empty),
                    soft_deleted_versions: deleted_versions.clone(),
                    included_as_reference: !selected.contains(subject),
                    deleted_versions: deleted_versions.clone(),
                    config: config.clone(),
                },
            )
            .await?;
            summaries.push(SubjectSummary {
                name: subject.clone(),
                schema_type: last.schema_type.clone(),
                version_count: versions.len(),
                latest_version: last.version,
                latest_schema_id: last.id,
                compatibility: config.and_then(|c| c.compatibility_level),
                mode,
                has_references: versions.iter().any(|v| !v.references.is_empty()),
                soft_deleted_versions: deleted_versions.len(),
            });
        }

        // Registration order (version level) and references left unresolved.
        let entries: Vec<(&(String, i32), &SchemaVersion)> = schemas.iter().collect();
        let nodes: Vec<VersionNode> = entries
            .iter()
            .map(|((subject, version), sv)| VersionNode {
                subject: subject.clone(),
                version: *version,
                id: sv.id,
                references: sv
                    .references
                    .iter()
                    .map(|r| (context::resolve_reference(subject, &r.subject), r.version))
                    .collect(),
            })
            .collect();
        let captured: BTreeSet<(String, i32)> = nodes
            .iter()
            .map(|n| (context::normalize(&n.subject), n.version))
            .collect();
        let unresolved: BTreeSet<Coordinate> = nodes
            .iter()
            .flat_map(|n| n.references.iter())
            .filter(|(s, v)| !captured.contains(&(context::normalize(s), *v)))
            .map(|(s, v)| Coordinate::subject(s, *v))
            .collect();
        let mut dependency_order: Vec<String> = Vec::new();
        for index in registration_order(&nodes)? {
            let name = &entries[index].0 .0;
            if !dependency_order.contains(name) {
                dependency_order.push(name.clone());
            }
        }

        // Checksums, then the manifest.
        let mut keys = vec![global_config_key(&self.backup_id)];
        for name in &names {
            keys.push(subject_metadata_key(&self.backup_id, name));
        }
        for (name, version) in schemas.keys() {
            keys.push(version_key(&self.backup_id, name, *version));
        }
        let sealed = snapshot::seal(
            self.storage.as_ref(),
            keys,
            SnapshotInfo {
                provider: "confluent".into(),
                api_version: "confluent-v1".into(),
                server_version: self.client.server_version().await?,
                full_registry: opts.subjects == ["*"]
                    && opts.exclude.is_empty()
                    && opts.include_versions == VersionSelection::All
                    && opts.include_soft_deleted,
                unresolved_dependencies: unresolved.into_iter().collect(),
                ..Default::default()
            },
        )
        .await?;
        let manifest = SchemaRegistryManifest {
            schema_registry_url: self.client.base_url().to_string(),
            backup_timestamp: chrono::Utc::now().to_rfc3339(),
            global_compatibility: global_config.compatibility_level.unwrap_or_default(),
            global_mode,
            subjects_count: summaries.len(),
            total_versions: schemas.len(),
            subjects_discovered: selected.len(),
            subjects_failed: 0,
            errors: vec![],
            soft_deleted_versions: summaries.iter().map(|s| s.soft_deleted_versions).sum(),
            referenced_subjects: names
                .iter()
                .filter(|name| !selected.contains(*name))
                .cloned()
                .collect(),
            subjects: summaries,
            dependency_order,
            snapshot: Some(sealed),
        };
        self.write(&manifest_key(&self.backup_id), &manifest)
            .await?;
        Ok(manifest)
    }

    async fn write<T: serde::Serialize>(
        &self,
        key: &str,
        value: &T,
    ) -> Result<(), SchemaRegistryError> {
        let body = serde_json::to_vec_pretty(value)
            .map_err(|e| SchemaRegistryError::Serialization(format!("encode {key}: {e}")))?;
        self.storage
            .put(key, Bytes::from(body))
            .await
            .map_err(|e| SchemaRegistryError::Storage(format!("write {key}: {e}")))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

fn matches_subject_filters(
    subject: &str,
    include: &[glob::Pattern],
    exclude: &[glob::Pattern],
) -> bool {
    let included = include.is_empty() || include.iter().any(|p| p.matches(subject));
    let excluded = exclude.iter().any(|p| p.matches(subject));
    included && !excluded
}

// ============================================================================
// Storage Key Helpers
// ============================================================================

fn sr_prefix(backup_id: &str) -> String {
    snapshot::prefix(backup_id)
}

pub fn manifest_key(backup_id: &str) -> String {
    format!("{}/_manifest.json", sr_prefix(backup_id))
}

pub fn global_config_key(backup_id: &str) -> String {
    format!("{}/_global_config.json", sr_prefix(backup_id))
}

fn subject_prefix(backup_id: &str, subject: &str) -> String {
    format!(
        "{}/subjects/{}",
        sr_prefix(backup_id),
        urlencoding::encode(subject)
    )
}

pub fn subject_metadata_key(backup_id: &str, subject: &str) -> String {
    format!("{}/_metadata.json", subject_prefix(backup_id, subject))
}

pub fn version_key(backup_id: &str, subject: &str, version: i32) -> String {
    format!("{}/v{}.json", subject_prefix(backup_id, subject), version)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subject_filtering_include() {
        let include = vec![glob::Pattern::new("orders-*").unwrap()];
        let exclude = vec![];

        assert!(matches_subject_filters("orders-value", &include, &exclude));
        assert!(matches_subject_filters("orders-key", &include, &exclude));
        assert!(!matches_subject_filters(
            "payments-value",
            &include,
            &exclude
        ));
    }

    #[test]
    fn test_subject_filtering_exclude() {
        let include = vec![glob::Pattern::new("*").unwrap()];
        let exclude = vec![glob::Pattern::new("*-test").unwrap()];

        assert!(matches_subject_filters("orders-value", &include, &exclude));
        assert!(!matches_subject_filters("orders-test", &include, &exclude));
    }

    #[test]
    fn test_subject_filtering_include_and_exclude() {
        let include = vec![glob::Pattern::new("orders-*").unwrap()];
        let exclude = vec![glob::Pattern::new("*-internal").unwrap()];

        assert!(matches_subject_filters("orders-value", &include, &exclude));
        assert!(!matches_subject_filters(
            "orders-internal",
            &include,
            &exclude
        ));
        assert!(!matches_subject_filters(
            "payments-value",
            &include,
            &exclude
        ));
    }

    #[test]
    fn test_subject_filtering_empty_include_matches_all() {
        let include: Vec<glob::Pattern> = vec![];
        let exclude = vec![];

        assert!(matches_subject_filters("anything", &include, &exclude));
    }

    #[test]
    fn test_storage_key_generation() {
        assert_eq!(
            manifest_key("backup-001"),
            "backup-001/schema-registry/_manifest.json"
        );
        assert_eq!(
            global_config_key("backup-001"),
            "backup-001/schema-registry/_global_config.json"
        );
        assert_eq!(
            subject_metadata_key("backup-001", "orders-value"),
            "backup-001/schema-registry/subjects/orders-value/_metadata.json"
        );
        assert_eq!(
            version_key("backup-001", "orders-value", 3),
            "backup-001/schema-registry/subjects/orders-value/v3.json"
        );
    }

    #[test]
    fn test_storage_key_url_encodes_subject() {
        let key = subject_metadata_key("b", "ctx:.:/my-subject");
        assert!(key.contains("ctx%3A.%3A%2Fmy-subject"));
    }
}
