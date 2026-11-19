//! Shared preflight/execute/verify coordinator.
use super::{
    snapshot::{self, Coordinate, Schema, Snapshot},
    wire::{self, IdKey, IdKind, RegistryTransform, WireMapping},
    IdPolicy, Provider, RestoreConfig,
};
use crate::{
    apicurio_registry::{
        client::ApicurioRegistryClient,
        config::{ApicurioApiVersion, ApicurioRegistryConfig},
    },
    schema_registry::{client::SchemaRegistryClient, config::SchemaRegistryConfig},
};
use crate::{
    config::RestoreOptions,
    manifest::BackupManifest,
    restore::{filter::FilterAction, read_segment},
    storage::StorageBackend,
};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Cursor, Read},
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Registration {
    pub source: Coordinate,
    pub destination: Coordinate,
    pub source_id: i64,
    pub action: String,
    pub existing: Option<Schema>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryPlan {
    pub source: Provider,
    pub destination: Provider,
    pub id_policy: IdPolicy,
    pub snapshot_digest: String,
    pub target_identity: String,
    pub registrations: Vec<Registration>,
    pub conflicts: Vec<String>,
    pub limitations: Vec<String>,
    pub capabilities: Vec<String>,
    pub wire_conversions: Vec<String>,
    pub settings_changes: Vec<String>,
    pub archive_import: bool,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegistryReport {
    pub created: usize,
    pub reused: usize,
    pub mappings: Vec<Mapping>,
    pub verified: bool,
    pub cleanup_verified: bool,
    pub failures: Vec<String>,
    pub dry_run: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mapping {
    pub source: Coordinate,
    pub destination: Coordinate,
    pub source_id: i64,
    pub destination_id: i64,
    pub destination_content_id: Option<i64>,
    pub destination_guid: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Recovery {
    name: String,
    mode: Option<String>,
    config: Option<Value>,
    #[serde(default)]
    success_config: Option<Value>,
    #[serde(default)]
    success_mode: Option<String>,
    #[serde(default)]
    temporary_mode: Option<String>,
    #[serde(default)]
    global: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Journal {
    format_version: u32,
    snapshot_digest: String,
    target_identity: String,
    config_digest: String,
    mappings: Vec<Mapping>,
    recovery: Vec<Recovery>,
    pending: Option<Coordinate>,
    archive_started: bool,
    verified: bool,
}

pub enum Target {
    Confluent(SchemaRegistryClient),
    Apicurio(ApicurioRegistryClient),
}
impl Target {
    pub fn confluent(config: &SchemaRegistryConfig) -> Result<Self> {
        if confluent_cloud(&config.url) {
            bail!("Confluent Cloud on the common serializer-aware registry path is unqualified; use schema_registry.restore for Confluent-only recovery");
        }
        Ok(Self::Confluent(SchemaRegistryClient::new(config)?))
    }
    pub fn apicurio(config: &ApicurioRegistryConfig) -> Result<Self> {
        Ok(Self::Apicurio(ApicurioRegistryClient::new(config)?))
    }
    pub async fn identity(&self, url: &str) -> Result<String> {
        let mut identity = match self {
            Self::Confluent(c) => c.cluster_identity().await?,
            Self::Apicurio(_) => None,
        };
        if let Some(identity) = &mut identity {
            identity.sort_all_objects();
        }
        Ok(format!(
            "{url}|{}",
            identity
                .map(|v| v.to_string())
                .unwrap_or_else(|| "provider-has-no-instance-id".into())
        ))
    }
    fn provider(&self) -> Provider {
        match self {
            Self::Confluent(_) => Provider::Confluent,
            Self::Apicurio(_) => Provider::Apicurio,
        }
    }
    async fn get(&self, c: &Coordinate, deleted: bool) -> Result<Option<Schema>> {
        match self {
            Self::Confluent(client) => {
                let v = c.version.parse::<i32>()?;
                let result = if deleted {
                    client
                        .get_schema_version_including_deleted(&c.name, v)
                        .await
                } else {
                    client.get_schema_version(&c.name, v).await
                };
                let v = match result {
                    Ok(v) => v,
                    Err(e) if sr_absent(&e) => return Ok(None),
                    Err(e) => return Err(e.into()),
                };
                let envelope = v.envelope();
                Ok(Some(Schema {
                    coordinate: Coordinate::subject(&v.subject, v.version),
                    id: v.id.into(),
                    content_id: None,
                    schema_type: v.schema_type,
                    content: v.schema,
                    references: v
                        .references
                        .into_iter()
                        .map(|r| snapshot::Reference {
                            name: r.name,
                            coordinate: Coordinate::subject(&r.subject, r.version),
                        })
                        .collect(),
                    deleted: Some(false),
                    state: None,
                    envelope,
                }))
            }
            Self::Apicurio(client) => {
                let v = match client
                    .get_version_metadata(&c.group, &c.name, &c.version)
                    .await
                {
                    Ok(v) => v,
                    Err(e) if ap_absent(&e) => return Ok(None),
                    Err(e) => return Err(e.into()),
                };
                let content = if v.state.as_deref() == Some("DISABLED") {
                    client.get_content_by_id(v.content_id).await?
                } else {
                    client
                        .get_version_content(&c.group, &c.name, &c.version)
                        .await?
                };
                let refs = client
                    .get_version_references(&c.group, &c.name, &c.version)
                    .await?;
                let mut envelope = v.extra;
                envelope.insert("name".into(), json!(v.name));
                envelope.insert("description".into(), json!(v.description));
                envelope.insert("labels".into(), json!(v.labels));
                Ok(Some(Schema {
                    coordinate: Coordinate {
                        group: c.group.clone(),
                        name: c.name.clone(),
                        version: v.version,
                    },
                    id: v.global_id,
                    content_id: Some(v.content_id),
                    schema_type: v.artifact_type,
                    content: String::from_utf8(content.to_vec())?,
                    references: refs
                        .into_iter()
                        .map(|r| snapshot::Reference {
                            name: r.name,
                            coordinate: Coordinate {
                                group: r.group_id,
                                name: r.artifact_id,
                                version: r.version,
                            },
                        })
                        .collect(),
                    deleted: None,
                    state: v.state,
                    envelope,
                }))
            }
        }
    }
    async fn versions(&self, c: &Coordinate, deleted: bool) -> Result<Vec<String>> {
        match self {
            Self::Confluent(client) => match client.list_versions(&c.name, deleted).await {
                Ok(v) => Ok(v.into_iter().map(|v| v.to_string()).collect()),
                Err(e) if sr_absent(&e) => Ok(vec![]),
                Err(e) => Err(e.into()),
            },
            Self::Apicurio(client) => {
                let mut versions = vec![];
                let mut offset = 0;
                loop {
                    let page = match client.list_versions(&c.group, &c.name, 100, offset).await {
                        Ok(p) => p,
                        Err(e) if ap_absent(&e) => return Ok(versions),
                        Err(e) => return Err(e.into()),
                    };
                    let n = page.versions.len();
                    versions.extend(page.versions.into_iter().map(|v| v.version));
                    if versions.len() as i64 >= page.count || n == 0 {
                        break;
                    }
                    offset += 100;
                }
                Ok(versions)
            }
        }
    }
    async fn empty(&self) -> Result<bool> {
        match self {
            Self::Confluent(c) => Ok(c.list_subjects(true).await?.is_empty()),
            Self::Apicurio(c) => {
                let default = c.list_artifacts("default", 1, 0).await?;
                if default.count > 0 {
                    return Ok(false);
                }
                let mut offset = 0;
                loop {
                    let page = c.list_groups(100, offset).await?;
                    let n = page.groups.len();
                    if !page.groups.is_empty() {
                        return Ok(false);
                    }
                    if n == 0 || i64::from(offset + 100) >= page.count {
                        break;
                    }
                    offset += 100;
                }
                Ok(true)
            }
        }
    }
}
fn sr_absent(e: &crate::schema_registry::SchemaRegistryError) -> bool {
    matches!(
        e,
        crate::schema_registry::SchemaRegistryError::SubjectNotFound(_)
            | crate::schema_registry::SchemaRegistryError::Api { status: 404, .. }
    )
}
fn ap_absent(e: &crate::apicurio_registry::error::ApicurioRegistryError) -> bool {
    matches!(
        e,
        crate::apicurio_registry::error::ApicurioRegistryError::ArtifactNotFound { .. }
            | crate::apicurio_registry::error::ApicurioRegistryError::GroupNotFound(_)
            | crate::apicurio_registry::error::ApicurioRegistryError::Api { status: 404, .. }
    )
}

pub struct RegistryRestore {
    pub snapshot: Snapshot,
    pub config: RestoreConfig,
    target: Target,
    storage: Arc<dyn StorageBackend>,
    journal_path: PathBuf,
    target_identity: String,
}
impl RegistryRestore {
    pub fn new(
        snapshot: Snapshot,
        config: RestoreConfig,
        target: Target,
        storage: Arc<dyn StorageBackend>,
        target_identity: String,
        checkpoint: Option<&Path>,
    ) -> Result<Self> {
        let name = snapshot::hash(format!("{}:{target_identity}", snapshot.digest).as_bytes())
            .replace(':', "-");
        let journal_path = checkpoint
            .map(|p| {
                p.with_extension(
                    format!(
                        "registry-{:?}-{:?}.json",
                        snapshot.provider,
                        target.provider()
                    )
                    .to_lowercase(),
                )
            })
            .unwrap_or_else(|| {
                PathBuf::from(".kafka-backup/registry-restore").join(format!("{name}.json"))
            });
        Ok(Self {
            snapshot,
            config,
            target,
            storage,
            journal_path,
            target_identity,
        })
    }
    fn mapped(&self, c: &Coordinate) -> Result<Coordinate> {
        let matches: Vec<_> = self
            .config
            .coordinates
            .iter()
            .filter(|m| m.source_group == c.group && m.source_name == c.name)
            .collect();
        if matches.len() > 1 {
            bail!("Ambiguous coordinate mapping for {}", c.path());
        }
        let mut target = c.clone();
        if let Some(m) = matches.first() {
            target.group = m.target_group.clone();
            target.name = m.target_name.clone();
        } else if self.snapshot.provider != self.target.provider() {
            bail!(
                "Cross-provider migration requires explicit coordinate mapping for {}",
                c.path()
            );
        }
        if self.target.provider() == Provider::Confluent
            && (!target.group.is_empty() || target.version.parse::<i32>().is_err())
        {
            // Rewrite allocates actual destination versions; source Apicurio labels need not be numeric.
            if !target.group.is_empty() {
                bail!("Confluent destination group must be empty");
            }
            if self.config.id_policy == IdPolicy::Rewrite {
                target.version = "1".into();
            } else {
                bail!("Confluent version must be a positive integer");
            }
        }
        if target.name.is_empty() {
            bail!("Destination coordinate name cannot be empty");
        }
        if [&target.name, &target.group]
            .iter()
            .any(|name| matches!(name.as_str(), "." | ".."))
        {
            bail!(
                "Dot-only registry coordinates cannot be safely represented in registry REST paths"
            );
        }
        Ok(target)
    }
    pub async fn plan(&self) -> Result<RegistryPlan> {
        let mut plan = RegistryPlan {
            source: self.snapshot.provider,
            destination: self.target.provider(),
            id_policy: self.config.id_policy,
            snapshot_digest: self.snapshot.digest.clone(),
            target_identity: self.target_identity.clone(),
            registrations: vec![],
            conflicts: vec![],
            limitations: vec![],
            capabilities: vec![],
            wire_conversions: vec![],
            settings_changes: vec![],
            archive_import: false,
        };
        if self.snapshot.provider == Provider::Confluent
            && confluent_cloud(&self.snapshot.source_url)
        {
            plan.conflicts.push(
                "Confluent Cloud sources on the common serializer-aware registry path are unqualified; use schema_registry.restore for Confluent-only recovery".into(),
            );
        }
        if let Some(info) = &self.snapshot.info {
            plan.limitations.extend(info.limitations.clone());
            if !info.full_registry {
                plan.limitations.push("Snapshot covers selected coordinates/history; omitted entries are not reconstructed".into());
            }
            if !info.unresolved_dependencies.is_empty() {
                plan.conflicts.push(format!(
                    "Snapshot has unresolved reference versions: {:?}",
                    info.unresolved_dependencies
                ));
            }
        } else {
            plan.limitations.push("Legacy snapshot: metadata, settings, deletion/state fidelity and hashes were not fully recorded".into());
            plan.conflicts.push("Registry restore requires a version 2 snapshot with recorded metadata, settings and history/state; legacy snapshots can only be inspected".into());
        }
        let require_history = self.config.id_policy == IdPolicy::Rewrite
            || self.target.provider() != Provider::Confluent;
        let order =
            match snapshot::order_with_history(&self.snapshot.schemas, false, require_history) {
                Ok(o) => o,
                Err(e) => {
                    plan.conflicts.push(e.to_string());
                    return Ok(plan);
                }
            };
        let cross = self.snapshot.provider != self.target.provider();
        if cross && self.config.id_policy != IdPolicy::Rewrite {
            plan.conflicts
                .push("Cross-provider migration requires ID allocation and rewriting".into());
        }
        let journal = self.read_journal().await?;
        if let Some(j) = &journal {
            self.check_journal(j)?;
            if !j.recovery.is_empty() {
                plan.limitations.push(
                    "Pending temporary settings recovery will run before registrations".into(),
                );
            }
        }
        if self.target.provider() == Provider::Apicurio
            && self.config.id_policy == IdPolicy::Preserve
        {
            plan.archive_import = true;
            let info = self.snapshot.info.as_ref();
            if self.snapshot.provider != Provider::Apicurio
                || !info.is_some_and(|i| {
                    i.full_registry && i.archive_scope.as_deref() == Some("whole_registry")
                })
            {
                plan.conflicts.push(
                    "Apicurio ID preservation requires a complete unfiltered native archive".into(),
                );
            }
            if !self.config.coordinates.is_empty() {
                plan.conflicts
                    .push("Archive preservation does not support coordinate remapping".into());
            }
            if let Target::Apicurio(c) = &self.target {
                if self.snapshot.api_version != c.api_version().as_str() {
                    plan.conflicts
                        .push("ID-preserving archives require matching API generations".into());
                }
            }
            if journal.as_ref().is_none_or(|j| !j.archive_started) && !self.target.empty().await? {
                plan.conflicts
                    .push("Apicurio archive preservation requires an empty target".into());
            }
            if info.is_some() {
                if let Err(e) = self.validate_archive().await {
                    plan.conflicts.push(format!("Archive preflight: {e:#}"));
                }
            }
            plan.capabilities
                .push("native archive import with preserved global/content IDs".into());
        }
        if self.config.restore_global_settings && !self.target.empty().await? && journal.is_none() {
            plan.conflicts
                .push("Source global settings can only be restored into an empty target".into());
        }
        if self.config.restore_global_settings
            && self.snapshot.provider == Provider::Confluent
            && self.snapshot.global.get("config").is_none()
        {
            plan.conflicts
                .push("Source global settings were not fully recorded".into());
        }
        if cross {
            for (group, metadata) in &self.snapshot.groups {
                if metadata["rules"].as_array().is_some_and(|a| !a.is_empty())
                    || ["description", "labels", "properties"]
                        .iter()
                        .any(|f| meaningful(&metadata["original_metadata"][*f]).is_some())
                {
                    plan.conflicts.push(format!("Cross-provider group settings/metadata cannot be faithfully translated: {group}"));
                }
            }
            for (artifact, metadata) in &self.snapshot.artifacts {
                if [
                    "name",
                    "description",
                    "labels",
                    "properties",
                    "legacyLabels",
                ]
                .iter()
                .any(|f| {
                    meaningful(&metadata["original_metadata"][*f]).is_some()
                        && !self.snapshot.schemas.iter().any(|schema| {
                            format!("{}/{}", schema.coordinate.group, schema.coordinate.name)
                                == *artifact
                                && derived_display(schema, f, &metadata["original_metadata"][*f])
                        })
                }) {
                    plan.conflicts.push(format!("Cross-provider artifact metadata cannot be faithfully translated: {artifact}"));
                }
            }
        }
        if self.config.restore_global_settings && self.snapshot.global["mode"] == "IMPORT" {
            plan.conflicts.push("Source global IMPORT mode cannot be safely restored after registration without a forced transition".into());
        }
        let mut mapped_artifacts = BTreeMap::new();
        let mut known_targets = BTreeMap::new();
        for coordinate in order {
            let schema = self
                .snapshot
                .schemas
                .iter()
                .find(|s| s.coordinate == coordinate)
                .unwrap();
            let mut destination = match self.mapped(&coordinate) {
                Ok(c) => c,
                Err(e) => {
                    plan.conflicts.push(e.to_string());
                    continue;
                }
            };
            if let Some(source) =
                mapped_artifacts.insert(destination.artifact(), coordinate.artifact())
            {
                if source != coordinate.artifact() {
                    plan.conflicts.push(format!(
                        "Multiple source artifacts map to {:?}",
                        destination.artifact()
                    ));
                }
            }
            if cross {
                if !matches!(schema.schema_type.as_str(), "AVRO" | "JSON" | "PROTOBUF") {
                    plan.conflicts.push(format!(
                        "Unsupported cross-provider schema type {}",
                        schema.schema_type
                    ));
                }
                if schema.deleted == Some(true)
                    || schema.state.as_deref().is_some_and(|s| s != "ENABLED")
                    || schema.envelope.iter().any(|(k, v)| {
                        !v.is_null()
                            && !matches!(
                                k.as_str(),
                                "guid"
                                    | "ts"
                                    | "deleted"
                                    | "name"
                                    | "description"
                                    | "labels"
                                    | "owner"
                                    | "createdBy"
                                    | "createdOn"
                                    | "modifiedBy"
                                    | "modifiedOn"
                                    | "artifactId"
                                    | "groupId"
                            )
                            && (v != &json!({}) && v != &json!([]))
                    })
                {
                    plan.conflicts.push(format!("Schema {} has metadata/rules/state without a faithful destination representation",coordinate.path()));
                }
                if schema
                    .envelope
                    .get("name")
                    .is_some_and(|v| meaningful(v).is_some() && !derived_display(schema, "name", v))
                    || schema.envelope.get("description").is_some_and(|v| {
                        meaningful(v).is_some() && !derived_display(schema, "description", v)
                    })
                    || schema
                        .envelope
                        .get("labels")
                        .is_some_and(|v| v != &json!({}))
                {
                    plan.conflicts.push(format!(
                        "Cross-provider metadata translation unsupported for {}",
                        coordinate.path()
                    ));
                }
            }
            if schema.schema_type == "PROTOBUF" {
                if let Err(e) = message_indexes(&self.snapshot, schema) {
                    plan.conflicts.push(format!(
                        "Invalid Protobuf descriptors at {}: {e:#}",
                        coordinate.path()
                    ));
                }
            }
            if self.config.id_policy == IdPolicy::Rewrite && schema.deleted == Some(true) {
                plan.conflicts.push(format!(
                    "Rewriting deleted Confluent history is unsupported: {}",
                    coordinate.path()
                ));
            }
            let mut existing = if plan.archive_import {
                None
            } else {
                self.target.get(&destination, true).await?
            };
            let mut action = if self.config.id_policy == IdPolicy::Rewrite {
                "register_or_reuse"
            } else {
                "create"
            }
            .to_string();
            if self.config.id_policy == IdPolicy::Preserve && !plan.archive_import {
                let expected = self.with_references(schema, &BTreeMap::new(), false)?;
                if let Some(existing) = &existing {
                    if existing.id == schema.id && self.equivalent(&expected, existing, true) {
                        action = "reuse".into();
                        if schema.deleted == Some(false)
                            && self.target.get(&destination, false).await?.is_none()
                        {
                            plan.conflicts.push(format!("Active source version conflicts with soft-deleted target history at {}",destination.path()));
                        }
                    } else {
                        plan.conflicts.push(format!(
                            "Conflicting ID/content/version at {}",
                            destination.path()
                        ));
                    }
                } else {
                    let history = self.target.versions(&destination, true).await?;
                    let active = self.target.versions(&destination, false).await?;
                    if history != active {
                        plan.conflicts.push(format!(
                            "Subject {} contains soft-deleted history; IMPORT could purge it",
                            destination.name
                        ));
                    }
                    let ours = journal.as_ref().is_some_and(|j| {
                        j.mappings
                            .iter()
                            .any(|m| m.destination.artifact() == destination.artifact())
                            || j.pending
                                .as_ref()
                                .is_some_and(|c| c.artifact() == destination.artifact())
                            || j.recovery.iter().any(|r| r.name == destination.name)
                    });
                    if let Target::Confluent(c) = &self.target {
                        if c.get_subject_mode(&destination.name).await?.is_none()
                            && c.supports_mode_delete(&destination.name).await? == Some(false)
                        {
                            plan.conflicts.push(format!("Subject {} inherits its mode, but the target cannot DELETE mode overrides required for IMPORT cleanup",destination.name));
                        }
                    }
                    if !history.is_empty() && !ours {
                        plan.conflicts.push(format!(
                            "Populated subject {} cannot safely enter IMPORT",
                            destination.name
                        ));
                    }
                }
                if let Target::Confluent(c) = &self.target {
                    match c
                        .get_schema_by_id_for_subject(i32::try_from(schema.id)?, &destination.name)
                        .await
                    {
                        Ok(envelope) => {
                            let references:Vec<_>=expected.references.iter().map(|r|json!({"name":r.name,"subject":r.coordinate.name,"version":r.coordinate.version.parse::<i32>().unwrap_or(0)})).collect();
                            let reference_conflict = envelope
                                .get("references")
                                .is_some_and(|r| r != &json!(references));
                            let metadata_conflict = ["metadata", "ruleSet"].iter().any(|k| {
                                meaningful(&envelope[*k])
                                    != expected.envelope.get(*k).and_then(meaningful)
                            });
                            if reference_conflict
                                || metadata_conflict
                                || !schema_content_equal(
                                    &schema.content,
                                    envelope["schema"].as_str().unwrap_or(""),
                                )
                                || envelope["schemaType"].as_str().unwrap_or("AVRO")
                                    != schema.schema_type
                            {
                                plan.conflicts.push(format!(
                                    "Global schema ID {} conflicts with target content",
                                    schema.id
                                ));
                            }
                        }
                        Err(e) if sr_absent(&e) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            if self.config.id_policy == IdPolicy::Rewrite
                && schema
                    .references
                    .iter()
                    .all(|r| known_targets.contains_key(&r.coordinate))
            {
                let expected = self.with_references(schema, &known_targets, true)?;
                for version in self.target.versions(&destination, false).await? {
                    let mut candidate = destination.clone();
                    candidate.version = version;
                    if let Some(target) = self.target.get(&candidate, false).await? {
                        if self.equivalent(&expected, &target, false) {
                            destination = target.coordinate.clone();
                            existing = Some(target);
                            action = "reuse".into();
                            break;
                        }
                    }
                }
            }
            if action == "reuse" {
                known_targets.insert(coordinate.clone(), existing.as_ref().unwrap().clone());
            }
            plan.registrations.push(Registration {
                source: coordinate,
                destination,
                source_id: schema.id,
                action,
                existing,
            });
        }
        if let Target::Confluent(c) = &self.target {
            for setting in &self.snapshot.settings {
                let destination = self.mapped(&Coordinate {
                    group: setting.group.clone(),
                    name: setting.name.clone(),
                    version: "1".into(),
                })?;
                let current = c
                    .get_subject_config(&destination.name)
                    .await?
                    .map(serde_json::to_value)
                    .transpose()?;
                let mode = c.get_subject_mode(&destination.name).await?.map(|m| m.mode);
                let own_temporary_mode = journal.as_ref().is_some_and(|j| {
                    j.recovery.iter().any(|r| {
                        !r.global
                            && r.name == destination.name
                            && mode.as_ref() == r.temporary_mode.as_ref()
                    })
                });
                if !own_temporary_mode
                    && setting
                        .mode
                        .as_ref()
                        .zip(mode.as_ref())
                        .is_some_and(|(source, target)| source != target)
                {
                    plan.conflicts.push(format!(
                        "Existing subject mode conflicts at {}",
                        destination.name
                    ));
                }
                if self.config.id_policy == IdPolicy::Rewrite
                    && plan
                        .registrations
                        .iter()
                        .any(|r| r.destination.name == destination.name && r.action != "reuse")
                {
                    let effective = mode.or(c.get_global_mode().await?.map(|m| m.mode));
                    if effective.as_deref().is_some_and(|m| m != "READWRITE") {
                        plan.conflicts.push(format!(
                            "Normal registration requires READWRITE at {} (target mode {:?})",
                            destination.name, effective
                        ));
                    }
                }
                if let (Some(source), Some(current)) = (&setting.config, current) {
                    if config_put(source) != config_put(&current) {
                        plan.conflicts.push(format!(
                            "Existing subject configuration conflicts at {}",
                            destination.name
                        ));
                    }
                }
            }
        }
        if let Target::Confluent(c) = &self.target {
            let version = c.server_version().await?;
            let version_numbers: Vec<u32> = version
                .as_deref()
                .unwrap_or("")
                .split('.')
                .filter_map(|s| s.parse().ok())
                .collect();
            plan.capabilities.push(format!(
                "Confluent target version: {}",
                version.as_deref().unwrap_or("unknown")
            ));
            if ((self.config.id_policy == IdPolicy::Preserve
                && self
                    .snapshot
                    .schemas
                    .iter()
                    .any(|s| s.envelope.get("guid").is_some_and(|v| !v.is_null())))
                || self.config.serializers.values().any(|p| {
                    [&p.key, &p.value]
                        .iter()
                        .any(|p| matches!(p.target, wire::Serializer::ConfluentGuidHeader { .. }))
                }))
                && version_numbers.as_slice() < [8, 1].as_slice()
            {
                plan.conflicts.push("Original GUID preservation requires a target with verified GUID support (CP 8.1+)".into());
            }
            if self.snapshot.schemas.iter().any(|s| {
                ["metadata", "ruleSet"]
                    .iter()
                    .any(|k| s.envelope.get(*k).is_some_and(|v| !v.is_null()))
            }) && version_numbers.as_slice() < [7, 4].as_slice()
            {
                plan.conflicts.push("Target cannot verify support for Confluent metadata/ruleSet (requires CP 7.4+)".into());
            }
            let rules_need_registration = plan.registrations.iter().any(|r| {
                r.action != "reuse"
                    && self.snapshot.schemas.iter().any(|s| {
                        s.coordinate == r.source
                            && s.envelope.get("ruleSet").and_then(meaningful).is_some()
                    })
            });
            if rules_need_registration {
                plan.conflicts.push("Target rule support for new registrations has not been qualified against a real rule-enabled registry; ruleSet creation is unsupported in this release (verified exact reuse is supported)".into());
            }
            if c.get_global_mode().await?.is_none() {
                plan.conflicts
                    .push("Target has no mode API; IMPORT capability unavailable".into());
            }
            if self.config.id_policy == IdPolicy::Preserve {
                plan.capabilities
                    .push("subject IMPORT; explicit original IDs and versions; exact reuse".into());
            }
        }
        if cross {
            for s in &self.snapshot.settings {
                if s.config
                    .as_ref()
                    .is_some_and(|v| match self.snapshot.provider {
                        Provider::Confluent => meaningful(v).is_some(),
                        Provider::Apicurio => v.as_array().is_none_or(|a| !a.is_empty()),
                    })
                {
                    plan.conflicts.push(format!(
                        "Cross-provider artifact settings have no faithful translation: {}",
                        s.name
                    ));
                }
                if s.mode.is_some() {
                    plan.conflicts.push(format!(
                        "Cross-provider subject modes have no faithful translation: {}",
                        s.name
                    ));
                }
            }
            if self.config.restore_global_settings {
                plan.conflicts
                    .push("Cross-provider global settings translation is unsupported".into());
            }
        }
        if let Target::Apicurio(c) = &self.target {
            let version = c.server_version().await?;
            plan.capabilities.push(format!(
                "Apicurio target {} version: {}",
                c.api_version().as_str(),
                version.as_deref().unwrap_or("unknown")
            ));
            let numbers: Vec<u32> = version
                .as_deref()
                .unwrap_or("")
                .split('.')
                .filter_map(|p| p.parse().ok())
                .collect();
            if self
                .snapshot
                .schemas
                .iter()
                .any(|s| s.state.as_deref() == Some("SUNSET"))
                && numbers.as_slice() < [3, 3].as_slice()
            {
                plan.conflicts
                    .push("SUNSET state requires verified Apicurio 3.3+ support".into());
            }
            if !cross && !plan.archive_import {
                // v2 label arrays/properties and v3 label maps have different
                // semantics. Reject unavailable representations before POST,
                // rather than relying on a server ignoring unknown fields.
                let unsupported = match c.api_version() {
                    ApicurioApiVersion::V2 => vec!["labels"],
                    ApicurioApiVersion::V3 => vec!["properties", "legacyLabels"],
                };
                for (scope, metadata) in self
                    .snapshot
                    .groups
                    .iter()
                    .chain(self.snapshot.artifacts.iter())
                {
                    for field in &unsupported {
                        if meaningful(&metadata["original_metadata"][*field]).is_some() {
                            plan.conflicts.push(format!(
                                "Apicurio {} cannot faithfully restore {field} at {scope}",
                                c.api_version().as_str()
                            ));
                        }
                    }
                    if c.api_version() == ApicurioApiVersion::V2
                        && metadata["rules"].as_array().is_some_and(|r| !r.is_empty())
                        && self.snapshot.groups.contains_key(scope)
                    {
                        plan.conflicts.push(format!(
                            "Apicurio v2 has no group-rule representation at {scope}"
                        ));
                    }
                    if c.api_version() == ApicurioApiVersion::V2
                        && metadata["versions"].as_array().is_some_and(Vec::is_empty)
                    {
                        plan.conflicts.push(format!(
                            "Apicurio v2 cannot restore an artifact without versions at {scope}"
                        ));
                    }
                }
                for schema in &self.snapshot.schemas {
                    for field in &unsupported {
                        if schema
                            .envelope
                            .get(*field)
                            .is_some_and(|v| meaningful(v).is_some())
                        {
                            plan.conflicts.push(format!(
                                "Apicurio {} cannot faithfully restore {field} at {}",
                                c.api_version().as_str(),
                                schema.coordinate.path()
                            ));
                        }
                    }
                    if schema
                        .state
                        .as_deref()
                        .is_some_and(|s| !matches!(s, "ENABLED" | "DISABLED" | "DEPRECATED"))
                    {
                        plan.conflicts.push(format!(
                            "Apicurio state {:?} requires unsupported lifecycle transitions at {}",
                            schema.state,
                            schema.coordinate.path()
                        ));
                    }
                }
                for (group, metadata) in &self.snapshot.groups {
                    let target_group = self.mapped_group(group)?;
                    match c.get_group(&target_group).await {
                        Ok(old) => {
                            let old = serde_json::to_value(old)?;
                            for field in ["description", "labels", "properties"] {
                                let desired = &metadata["original_metadata"][field];
                                if meaningful(&old[field]).is_some()
                                    && meaningful(desired).is_some()
                                    && meaningful(&old[field]) != meaningful(desired)
                                {
                                    plan.conflicts.push(format!("Existing group metadata conflicts at {target_group}/{field}"));
                                }
                            }
                        }
                        Err(e) if ap_absent(&e) => {}
                        Err(e) => return Err(e.into()),
                    }
                    let rules = match c.list_group_rules(&target_group).await {
                        Ok(v) => v,
                        Err(e) if ap_absent(&e) => vec![],
                        Err(e) => return Err(e.into()),
                    };
                    for t in rules {
                        let current = c.get_group_rule(&target_group, &t).await?;
                        if metadata["rules"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .any(|r| r["ruleType"] == t && r["config"] != current.config)
                        {
                            plan.conflicts.push(format!(
                                "Existing group rule conflicts at {target_group}/{t}"
                            ));
                        }
                    }
                }
                for setting in &self.snapshot.settings {
                    let destination = self.mapped(&Coordinate {
                        group: setting.group.clone(),
                        name: setting.name.clone(),
                        version: "1".into(),
                    })?;
                    let metadata =
                        &self.snapshot.artifacts[&format!("{}/{}", setting.group, setting.name)];
                    match c
                        .get_artifact_metadata(&destination.group, &destination.name)
                        .await
                    {
                        Ok(old) => {
                            let old = serde_json::to_value(old)?;
                            for field in [
                                "name",
                                "description",
                                "labels",
                                "properties",
                                "legacyLabels",
                            ] {
                                let desired = &metadata["original_metadata"][field];
                                if meaningful(&old[field]).is_some()
                                    && meaningful(desired).is_some()
                                    && meaningful(&old[field]) != meaningful(desired)
                                {
                                    plan.conflicts.push(format!(
                                        "Existing artifact metadata conflicts at {}/{field}",
                                        destination.path()
                                    ));
                                }
                            }
                        }
                        Err(e) if ap_absent(&e) => {}
                        Err(e) => return Err(e.into()),
                    }
                    let rules = match c
                        .list_artifact_rules(&destination.group, &destination.name)
                        .await
                    {
                        Ok(v) => v,
                        Err(e) if ap_absent(&e) => vec![],
                        Err(e) => return Err(e.into()),
                    };
                    for t in rules {
                        let current = c
                            .get_artifact_rule(&destination.group, &destination.name, &t)
                            .await?;
                        if metadata["rules"]
                            .as_array()
                            .into_iter()
                            .flatten()
                            .any(|r| r["ruleType"] == t && r["config"] != current.config)
                        {
                            plan.conflicts.push(format!(
                                "Existing artifact rule conflicts at {}/{t}",
                                destination.path()
                            ));
                        }
                    }
                }
            }
        }
        let mut global_destinations: BTreeMap<IdKey, BTreeSet<Coordinate>> = BTreeMap::new();
        for profiles in self.config.serializers.values() {
            for p in [&profiles.key, &profiles.value] {
                let Ok(target_key) = p.target.id_key("0".into()) else {
                    continue;
                };
                if target_key.provider != "apicurio" || target_key.kind != IdKind::Global {
                    continue;
                }
                let Ok(source_kind) = p.source.id_key("0".into()) else {
                    continue;
                };
                for schema in &self.snapshot.schemas {
                    if source_kind.namespace != wire::namespace(self.snapshot.provider, schema) {
                        continue;
                    }
                    let id = match source_kind.kind {
                        IdKind::Content => schema.content_id.map(|i| i.to_string()),
                        IdKind::Guid => schema
                            .envelope
                            .get("guid")
                            .and_then(Value::as_str)
                            .map(guid)
                            .transpose()?,
                        _ => Some(schema.id.to_string()),
                    };
                    let Some(id) = id else {
                        continue;
                    };
                    let mut destination = self.mapped(&schema.coordinate)?;
                    if self.config.id_policy == IdPolicy::Rewrite {
                        destination.version.clear();
                    }
                    global_destinations
                        .entry(p.source.id_key(id)?)
                        .or_default()
                        .insert(destination);
                }
            }
        }
        for (source, destinations) in global_destinations {
            if destinations.len() > 1 {
                plan.conflicts.push(format!("Ambiguous identifier {source:?} maps to multiple Apicurio global-ID coordinates; use content-ID framing or restrict the snapshot"));
            }
        }
        for (topic, p) in &self.config.serializers {
            for (side, p) in [("key", &p.key), ("value", &p.value)] {
                plan.wire_conversions
                    .push(format!("{topic}/{side}: {:?} -> {:?}", p.source, p.target));
            }
        }
        if let Err(e) = self.identity_mappings().and_then(|m| self.transform(&m)) {
            plan.conflicts
                .push(format!("Serializer/mapping preflight: {e:#}"));
        }
        plan.settings_changes = self
            .snapshot
            .settings
            .iter()
            .map(|s| format!("subject/artifact settings for {}/{}", s.group, s.name))
            .collect();
        Ok(plan)
    }
    fn with_references(
        &self,
        s: &Schema,
        mappings: &BTreeMap<Coordinate, Schema>,
        require_actual: bool,
    ) -> Result<Schema> {
        let mut out = s.clone();
        out.coordinate = self.mapped(&s.coordinate)?;
        for r in &mut out.references {
            r.coordinate = if let Some(m) = mappings.get(&r.coordinate) {
                m.coordinate.clone()
            } else if require_actual {
                bail!(
                    "Missing destination version for reference {}",
                    r.coordinate.path()
                );
            } else {
                self.mapped(&r.coordinate)?
            };
        }
        if self.snapshot.provider != self.target.provider() {
            out.envelope.clear();
            out.state = None;
            out.deleted = Some(false);
        }
        Ok(out)
    }
    fn config_digest(&self) -> Result<String> {
        let mut value = serde_json::to_value(&self.config)?;
        value.as_object_mut().unwrap().remove("dry_run");
        Ok(snapshot::hash(&serde_json::to_vec(&value)?))
    }
    fn check_journal(&self, j: &Journal) -> Result<()> {
        if j.format_version != 1
            || j.snapshot_digest != self.snapshot.digest
            || j.target_identity != self.target_identity
            || j.config_digest != self.config_digest()?
        {
            bail!("Registry checkpoint snapshot, target identity or configuration changed; recover the previous operation first");
        }
        Ok(())
    }
    async fn read_journal(&self) -> Result<Option<Journal>> {
        match tokio::fs::read(&self.journal_path).await {
            Ok(b) => Ok(Some(serde_json::from_slice(&b)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    async fn save(&self, j: &Journal) -> Result<()> {
        if let Some(parent) = self.journal_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let temp = self.journal_path.with_extension("registry.tmp");
        let data = serde_json::to_vec_pretty(j)?;
        use tokio::io::AsyncWriteExt;
        let mut file = tokio::fs::File::create(&temp).await?;
        file.write_all(&data).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(temp, &self.journal_path).await?;
        Ok(())
    }
    /// Scan every selected surviving record before registry mutation. Unknown
    /// identifiers and malformed configured frames therefore cannot reach Kafka.
    pub async fn preflight_records(
        &self,
        backup_id: &str,
        options: &RestoreOptions,
        target_topics: &crate::config::TopicSelection,
    ) -> Result<u64> {
        let transform = self.transform(&self.identity_mappings()?)?;
        self.preflight_records_with_transform(backup_id, options, target_topics, &transform)
            .await
    }
    /// Recheck surviving records with allocated destination identifiers before
    /// Kafka replay, including destination-width overflow and actual mappings.
    pub async fn preflight_records_with_transform(
        &self,
        backup_id: &str,
        options: &RestoreOptions,
        target_topics: &crate::config::TopicSelection,
        transform: &RegistryTransform,
    ) -> Result<u64> {
        let manifest: BackupManifest = serde_json::from_slice(
            &self
                .storage
                .get(&format!("{backup_id}/manifest.json"))
                .await?,
        )?;
        let mut count = 0;
        for topic in &manifest.topics {
            if (!target_topics.include.is_empty()
                && !target_topics
                    .include
                    .iter()
                    .any(|p| kafka_topic_matches(p, &topic.name)))
                || target_topics
                    .exclude
                    .iter()
                    .any(|p| kafka_topic_matches(p, &topic.name))
            {
                continue;
            }
            for partition in &topic.partitions {
                if options
                    .source_partitions
                    .as_ref()
                    .is_some_and(|p| !p.contains(&partition.partition_id))
                {
                    continue;
                }
                for segment in &partition.segments {
                    for mut record in read_segment(self.storage.as_ref(), segment).await? {
                        if options
                            .time_window_start
                            .is_some_and(|t| record.timestamp < t)
                            || options
                                .time_window_end
                                .is_some_and(|t| record.timestamp > t)
                        {
                            continue;
                        }
                        if let Some(filter) = &options.record_filter {
                            if filter.applies_to(&topic.name) {
                                match filter.evaluate(&topic.name, &record) {
                                    FilterAction::Drop => continue,
                                    FilterAction::Tombstone => record.value = None,
                                    FilterAction::Keep => {}
                                }
                            }
                        }
                        transform
                            .check_record(&topic.name, &record)
                            .with_context(|| {
                                format!(
                                    "Record preflight: {}/{} offset {}",
                                    topic.name, partition.partition_id, record.offset
                                )
                            })?;
                        count += 1;
                    }
                }
            }
        }
        Ok(count)
    }
    fn identity_mappings(&self) -> Result<BTreeMap<Coordinate, Schema>> {
        // During preflight use valid stand-in target identifiers to validate
        // framing and schema identity before allocation; final IDs are checked
        // again after registry execution and before Kafka construction.
        self.snapshot
            .schemas
            .iter()
            .map(|s| {
                let mut t = self.with_references(s, &BTreeMap::new(), false)?;
                t.id = 1;
                t.content_id = Some(1);
                t.envelope
                    .insert("guid".into(), json!("00000000-0000-0000-0000-000000000001"));
                Ok((s.coordinate.clone(), t))
            })
            .collect()
    }
    pub fn transform(&self, mappings: &BTreeMap<Coordinate, Schema>) -> Result<RegistryTransform> {
        let mut wire_mappings = vec![];
        for s in &self.snapshot.schemas {
            let t = mappings
                .get(&s.coordinate)
                .context("Incomplete schema mappings")?;
            let source_namespace = wire::namespace(self.snapshot.provider, s);
            let target_namespace = wire::namespace(self.target.provider(), t);
            let kinds = match self.snapshot.provider {
                Provider::Confluent => vec![IdKind::Numeric],
                Provider::Apicurio => vec![IdKind::Global, IdKind::Content],
            };
            for kind in kinds {
                let source_id = if kind == IdKind::Content {
                    s.content_id.context("Missing source content ID")?
                } else {
                    s.id
                };
                // A source ID may be used with multiple destination profiles.
                // Each mapping must agree; conflicting profiles fail up front.
                let mut destination_kinds = BTreeSet::new();
                for p in self.config.serializers.values() {
                    for p in [&p.key, &p.value] {
                        if p.source.id_key(source_id.to_string()).is_ok_and(|k| {
                            k.provider
                                == wire::key(
                                    self.snapshot.provider,
                                    &source_namespace,
                                    kind,
                                    source_id,
                                )
                                .provider
                                && k.namespace == source_namespace
                                && k.kind == kind
                        }) {
                            if let Ok(k) = p.target.id_key("1".into()) {
                                destination_kinds.insert((k.kind, k.namespace));
                            }
                        }
                    }
                }
                if destination_kinds.is_empty() {
                    continue;
                }
                for (target_kind, profile_namespace) in destination_kinds {
                    if profile_namespace != target_namespace {
                        continue;
                    }
                    let target = match target_kind {
                        IdKind::Content => wire::key(
                            self.target.provider(),
                            &target_namespace,
                            target_kind,
                            t.content_id.context("Missing destination content ID")?,
                        ),
                        IdKind::Guid => IdKey {
                            provider: "confluent".into(),
                            namespace: target_namespace.clone(),
                            kind: IdKind::Guid,
                            id: guid(
                                t.envelope
                                    .get("guid")
                                    .and_then(Value::as_str)
                                    .context("Missing destination GUID")?,
                            )?,
                        },
                        _ => {
                            wire::key(self.target.provider(), &target_namespace, target_kind, t.id)
                        }
                    };
                    wire_mappings.push(WireMapping {
                        source: wire::key(
                            self.snapshot.provider,
                            &source_namespace,
                            kind,
                            source_id,
                        ),
                        target,
                        schema_type: s.schema_type.clone(),
                        top_level_bytes: wire::top_level_bytes(s),
                        blocked_reason: self.unavailable_record_reason(s, kind, target_kind),
                        requires_rule_executor: has_rules(s),
                        message_indexes: message_indexes(&self.snapshot, s)?,
                        target_message_indexes: self.destination_message_indexes(s, t)?,
                    });
                }
            }
            if let Some(source_guid) = s.envelope.get("guid").and_then(Value::as_str) {
                // GUID headers can be converted only with explicit destination framing.
                for p in self.config.serializers.values() {
                    for p in [&p.key, &p.value] {
                        let src = p.source.id_key(guid(source_guid)?);
                        if src.as_ref().is_ok_and(|k| {
                            k.kind == IdKind::Guid && k.namespace == source_namespace
                        }) {
                            let target_key =
                                p.target.id_key(match p.target.id_key("".into())?.kind {
                                    IdKind::Guid => guid(
                                        t.envelope
                                            .get("guid")
                                            .and_then(Value::as_str)
                                            .context("Missing destination GUID")?,
                                    )?,
                                    IdKind::Content => t
                                        .content_id
                                        .context("Missing destination content ID")?
                                        .to_string(),
                                    _ => t.id.to_string(),
                                })?;
                            wire_mappings.push(WireMapping {
                                source: src?,
                                blocked_reason: self.unavailable_record_reason(
                                    s,
                                    IdKind::Guid,
                                    target_key.kind,
                                ),
                                target: target_key,
                                schema_type: s.schema_type.clone(),
                                top_level_bytes: wire::top_level_bytes(s),
                                requires_rule_executor: has_rules(s),
                                message_indexes: message_indexes(&self.snapshot, s)?,
                                target_message_indexes: self.destination_message_indexes(s, t)?,
                            });
                        }
                    }
                }
            }
        }
        RegistryTransform::new(self.config.serializers.clone(), wire_mappings)
    }
    fn unavailable_record_reason(
        &self,
        root: &Schema,
        source_kind: IdKind,
        target_kind: IdKind,
    ) -> Option<String> {
        if self.snapshot.provider != Provider::Apicurio
            || self.target.provider() != Provider::Apicurio
        {
            return None;
        }
        if (target_kind == IdKind::Global
            && matches!(root.state.as_deref(), Some("DISABLED" | "SUNSET" | "DRAFT")))
            || (source_kind == IdKind::Global && root.state.as_deref() == Some("DRAFT"))
        {
            return Some(format!("Native global-ID replay cannot verify availability/immutability of {:?} version {}; use immutable content-ID framing or registry-only restore",root.state,root.coordinate.path()));
        }
        let mut pending: Vec<_> = root
            .references
            .iter()
            .map(|r| r.coordinate.clone())
            .collect();
        let mut seen = BTreeSet::new();
        while let Some(coordinate) = pending.pop() {
            if !seen.insert(coordinate.clone()) {
                continue;
            }
            if let Some(dependency) = self
                .snapshot
                .schemas
                .iter()
                .find(|s| s.coordinate == coordinate)
            {
                if matches!(
                    dependency.state.as_deref(),
                    Some("DISABLED" | "SUNSET" | "DRAFT")
                ) {
                    return Some(format!(
                        "Native replay cannot verify a referenced {:?} version {}",
                        dependency.state,
                        coordinate.path()
                    ));
                }
                pending.extend(dependency.references.iter().map(|r| r.coordinate.clone()));
            }
        }
        None
    }
    fn destination_message_indexes(
        &self,
        source: &Schema,
        target: &Schema,
    ) -> Result<BTreeMap<String, Vec<u32>>> {
        let mut target_root = source.clone();
        target_root.content = target.content.clone();
        message_indexes(&self.snapshot, &target_root)
    }
    fn equivalent(&self, expected: &Schema, actual: &Schema, preserve: bool) -> bool {
        let mut actual_envelope = actual.clone();
        // v2 derives display fields from schema content while v3 leaves them
        // absent. Only allocation across API generations may accept those
        // redundant additions; archive preservation still checks exact metadata.
        let cross_generation_rewrite = !preserve
            && self.snapshot.provider == Provider::Apicurio
            && matches!(&self.target, Target::Apicurio(c) if self.snapshot.api_version != c.api_version().as_str());
        if self.snapshot.provider != self.target.provider() || cross_generation_rewrite {
            for field in ["name", "description"] {
                if expected.envelope.get(field).and_then(meaningful).is_none()
                    && actual
                        .envelope
                        .get(field)
                        .is_some_and(|v| derived_display(actual, field, v))
                {
                    actual_envelope.envelope.remove(field);
                }
            }
        }
        if expected.schema_type != "PROTOBUF" {
            return equivalent(expected, &actual_envelope, preserve);
        }
        actual_envelope.content = expected.content.clone();
        if !equivalent(expected, &actual_envelope, preserve) {
            return false;
        }
        let source = self.snapshot.schemas.iter().find(|s| {
            s.id == expected.id
                && self
                    .mapped(&s.coordinate)
                    .is_ok_and(|c| c.artifact() == expected.coordinate.artifact())
                && s.content == expected.content
        });
        let Some(source) = source else {
            return false;
        };
        let mut target_root = source.clone();
        target_root.content = actual.content.clone();
        match (
            schema_descriptors(&self.snapshot, source),
            schema_descriptors(&self.snapshot, &target_root),
        ) {
            (Ok(a), Ok(b)) => canonical_descriptors(a) == canonical_descriptors(b),
            _ => false,
        }
    }
    pub async fn execute(
        &self,
        plan: &RegistryPlan,
    ) -> Result<(RegistryReport, RegistryTransform)> {
        self.execute_with_cancel(plan, tokio_util::sync::CancellationToken::new())
            .await
    }
    pub async fn execute_with_cancel(
        &self,
        plan: &RegistryPlan,
        cancel: tokio_util::sync::CancellationToken,
    ) -> Result<(RegistryReport, RegistryTransform)> {
        if !plan.conflicts.is_empty() {
            bail!(
                "Registry preflight conflicts: {}",
                plan.conflicts.join("; ")
            );
        }
        if plan.snapshot_digest != self.snapshot.digest
            || plan.target_identity != self.target_identity
        {
            bail!("Stale registry plan");
        }
        if self.config.dry_run {
            return Ok((
                RegistryReport {
                    dry_run: true,
                    ..Default::default()
                },
                self.transform(&self.identity_mappings()?)?,
            ));
        }
        if let Some(parent) = self.journal_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let lock_path = self.journal_path.with_extension("registry.lock");
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .context("Another registry restore is using this checkpoint")?;
        let mut j = self.read_journal().await?.unwrap_or(Journal {
            format_version: 1,
            snapshot_digest: self.snapshot.digest.clone(),
            target_identity: self.target_identity.clone(),
            config_digest: self.config_digest()?,
            mappings: vec![],
            recovery: vec![],
            pending: None,
            archive_started: false,
            verified: false,
        });
        self.check_journal(&j)?;
        self.cleanup(&mut j, false).await?;
        self.save(&j).await?;
        // Recheck after acquiring the checkpoint lock and recovering settings.
        let current = self.plan().await?;
        if !current.conflicts.is_empty() {
            bail!(
                "Registry changed during preflight: {}",
                current.conflicts.join("; ")
            );
        }
        let mut report = RegistryReport::default();
        let result = tokio::select! {
            r=self.execute_inner(&current,&mut j,&mut report)=>r,
            _=cancel.cancelled()=>Err(anyhow::anyhow!("Registry restore cancelled; pending writes will be reconciled on resume")),
        };
        let cleanup = self.cleanup(&mut j, result.is_ok()).await;
        if let Err(e) = cleanup {
            report
                .failures
                .push(format!("Temporary registry settings cleanup failed: {e:#}"));
        } else {
            report.cleanup_verified = true;
        }
        if cancel.is_cancelled() {
            report
                .failures
                .push("Registry restore cancelled; Kafka replay is blocked".into());
        }
        if let Err(e) = result {
            report
                .failures
                .push(format!("Registry execution/verification failed: {e:#}"));
        }
        report.mappings = j.mappings.clone();
        if !report.failures.is_empty() {
            self.write_report(&report).await?;
            bail!(
                "{}; report: {}",
                report.failures.join("; "),
                self.journal_path.with_extension("report.json").display()
            );
        }
        let mappings = self.reconcile(&j).await?;
        let transform = self.transform(&mappings)?;
        report.mappings = j.mappings.clone();
        report.verified = true;
        j.verified = true;
        self.save(&j).await?;
        self.write_report(&report).await?;
        Ok((report, transform))
    }
    async fn execute_inner(
        &self,
        plan: &RegistryPlan,
        j: &mut Journal,
        report: &mut RegistryReport,
    ) -> Result<()> {
        if plan.archive_import {
            if j.archive_started {
                // An uncertain import is reconciled in full, never retried.
                let mut mappings = BTreeMap::new();
                for s in &self.snapshot.schemas {
                    let target = self.target.get(&s.coordinate, true).await?.context(
                        "Previous archive import is partial/uncertain; refusing to repeat it",
                    )?;
                    if target.id != s.id
                        || target.content_id != s.content_id
                        || !self.equivalent(s, &target, false)
                    {
                        bail!("Previous archive import does not match source snapshot");
                    }
                    mappings.insert(s.coordinate.clone(), target);
                }
                self.record_mappings(j, &mappings).await?;
                report.reused = mappings.len();
                self.verify_scoped_settings().await?;
                return Ok(());
            }
            if !self.target.empty().await? {
                bail!("Target populated after archive preflight");
            }
            let c = match &self.target {
                Target::Apicurio(c) => c,
                _ => unreachable!(),
            };
            let global = self.ap_global_rules(c).await?;
            j.recovery.push(Recovery {
                name: "__global__".into(),
                mode: None,
                config: Some(json!(global)),
                success_config: Some(self.snapshot.global.clone()),
                success_mode: None,
                temporary_mode: None,
                global: false,
            });
            self.save(j).await?;
            j.archive_started = true;
            self.save(j).await?;
            let zip = self
                .storage
                .get(&format!("{}/_export.zip", self.snapshot.prefix))
                .await?;
            self.check_archive_hash(&zip)?;
            c.mutate(
                Method::POST,
                "/admin/import",
                "application/zip",
                zip,
                &[
                    ("X-Registry-Preserve-GlobalId".into(), "true".into()),
                    ("X-Registry-Preserve-ContentId".into(), "true".into()),
                ],
            )
            .await?;
            let mut mappings = BTreeMap::new();
            for s in &self.snapshot.schemas {
                let target = self
                    .target
                    .get(&s.coordinate, true)
                    .await?
                    .context("Imported archive is missing a version")?;
                if target.id != s.id
                    || target.content_id != s.content_id
                    || !self.equivalent(s, &target, false)
                {
                    bail!(
                        "Imported archive failed exact ID/content/reference/state verification: {}",
                        s.coordinate.path()
                    );
                }
                mappings.insert(s.coordinate.clone(), target);
            }
            self.record_mappings(j, &mappings).await?;
            report.created = mappings.len();
            if self.config.restore_global_settings {
                self.set_ap_global(c, self.snapshot.global.clone()).await?;
            }
            self.verify_scoped_settings().await?;
            return Ok(());
        }
        if let Target::Confluent(c) = &self.target {
            if self.config.restore_global_settings && !j.recovery.iter().any(|r| r.global) {
                j.recovery.push(Recovery {
                    name: "__global__".into(),
                    global: true,
                    mode: c.get_global_mode().await?.map(|m| m.mode),
                    config: Some(serde_json::to_value(c.get_global_config().await?)?),
                    success_config: None,
                    success_mode: None,
                    temporary_mode: None,
                });
                self.save(j).await?;
            }
        }
        if let Target::Confluent(c) = &self.target {
            let names: BTreeSet<_> = plan
                .registrations
                .iter()
                .map(|r| r.destination.name.clone())
                .collect();
            for name in names {
                if !j.recovery.iter().any(|r| r.name == name) {
                    j.recovery.push(Recovery {
                        name: name.clone(),
                        mode: c.get_subject_mode(&name).await?.map(|m| m.mode),
                        config: c
                            .get_subject_config(&name)
                            .await?
                            .map(serde_json::to_value)
                            .transpose()?,
                        success_config: None,
                        success_mode: None,
                        temporary_mode: None,
                        global: false,
                    });
                    self.save(j).await?;
                }
            }
        }
        let mut actual = self.reconcile(j).await?;
        let mut changed_modes = BTreeSet::new();
        for r in &plan.registrations {
            if actual.contains_key(&r.source) {
                report.reused += 1;
                continue;
            }
            let source = self
                .snapshot
                .schemas
                .iter()
                .find(|s| s.coordinate == r.source)
                .unwrap();
            let expected = self.with_references(source, &actual, true)?;
            let mut reused = None;
            for version in self.target.versions(&r.destination, true).await? {
                let mut c = r.destination.clone();
                c.version = version;
                if let Some(s) = self
                    .target
                    .get(&c, self.config.id_policy == IdPolicy::Preserve)
                    .await?
                {
                    if self.equivalent(&expected, &s, self.config.id_policy == IdPolicy::Preserve)
                        && (self.config.id_policy == IdPolicy::Rewrite
                            || (s.id == source.id && s.coordinate.version == r.destination.version))
                    {
                        reused = Some(s);
                        break;
                    }
                }
            }
            let target = if let Some(s) = reused {
                report.reused += 1;
                s
            } else {
                if let Target::Confluent(c) = &self.target {
                    if self.config.id_policy == IdPolicy::Preserve
                        && changed_modes.insert(r.destination.name.clone())
                    {
                        let history = self.target.versions(&r.destination, true).await?;
                        let ours = j
                            .mappings
                            .iter()
                            .any(|m| m.destination.artifact() == r.destination.artifact());
                        if !history.is_empty() && !ours {
                            bail!(
                                "Concurrent registration populated IMPORT subject {}",
                                r.destination.name
                            );
                        }
                        if history != self.target.versions(&r.destination, false).await? {
                            bail!("Subject contains soft-deleted versions; refusing IMPORT");
                        }
                        let recovery = Recovery {
                            name: r.destination.name.clone(),
                            mode: c
                                .get_subject_mode(&r.destination.name)
                                .await?
                                .map(|m| m.mode),
                            config: c
                                .get_subject_config(&r.destination.name)
                                .await?
                                .map(serde_json::to_value)
                                .transpose()?,
                            success_config: None,
                            success_mode: None,
                            temporary_mode: None,
                            global: false,
                        };
                        if !j.recovery.iter().any(|r| r.name == recovery.name) {
                            j.recovery.push(recovery);
                            self.save(j).await?;
                        }
                        if let Some(recovery) = j
                            .recovery
                            .iter_mut()
                            .find(|recovery| recovery.name == r.destination.name)
                        {
                            recovery.temporary_mode = Some("IMPORT".into());
                            self.save(j).await?;
                        }
                        c.mutate(
                            Method::PUT,
                            &format!("/mode/{}", urlencoding::encode(&r.destination.name)),
                            Some(&json!({"mode":"IMPORT"})),
                        )
                        .await?;
                    }
                }
                j.pending = Some(r.source.clone());
                self.save(j).await?;
                let written = self.register(&expected).await;
                let target = match written {
                    Ok(s) => s,
                    Err(e) => {
                        // HTTP timeout after commit: look for exactly the expected
                        // version/content before deciding this was a failed write.
                        let mut found = None;
                        for version in self.target.versions(&r.destination, true).await? {
                            let mut coord = r.destination.clone();
                            coord.version = version;
                            if let Some(s) = self
                                .target
                                .get(&coord, self.config.id_policy == IdPolicy::Preserve)
                                .await?
                            {
                                if self.equivalent(
                                    &expected,
                                    &s,
                                    self.config.id_policy == IdPolicy::Preserve,
                                ) && (self.config.id_policy == IdPolicy::Rewrite
                                    || s.id == source.id)
                                {
                                    found = Some(s);
                                    break;
                                }
                            }
                        }
                        found.with_context(|| {
                            format!("Registration failed and was not reconciled: {e:#}")
                        })?
                    }
                };
                if !self.equivalent(
                    &expected,
                    &target,
                    self.config.id_policy == IdPolicy::Preserve,
                ) || (self.config.id_policy == IdPolicy::Preserve
                    && (target.id != source.id
                        || target.coordinate.version != expected.coordinate.version))
                {
                    bail!(
                        "Registered schema failed envelope verification: {}",
                        r.source.path()
                    );
                }
                report.created += 1;
                target
            };
            actual.insert(r.source.clone(), target);
            self.record_mappings(j, &actual).await?;
            j.pending = None;
            self.save(j).await?;
        }
        // Remove source-deleted versions in reverse dependency order, preserving
        // their tombstoned history and identifiers; never permanently delete.
        if let Target::Confluent(c) = &self.target {
            for r in plan.registrations.iter().rev() {
                let source = self
                    .snapshot
                    .schemas
                    .iter()
                    .find(|s| s.coordinate == r.source)
                    .unwrap();
                if source.deleted == Some(true) {
                    let target = actual.get(&r.source).unwrap();
                    if self.target.get(&target.coordinate, false).await?.is_some() {
                        if let Some(recovery) = j
                            .recovery
                            .iter_mut()
                            .find(|r| r.name == target.coordinate.name && !r.global)
                        {
                            recovery.temporary_mode = Some("READWRITE".into());
                            self.save(j).await?;
                        }
                        c.mutate(
                            Method::PUT,
                            &format!("/mode/{}", urlencoding::encode(&target.coordinate.name)),
                            Some(&json!({"mode":"READWRITE"})),
                        )
                        .await?;
                        c.mutate(
                            Method::DELETE,
                            &format!(
                                "/subjects/{}/versions/{}",
                                urlencoding::encode(&target.coordinate.name),
                                target.coordinate.version
                            ),
                            None,
                        )
                        .await?;
                    }
                    if self.target.get(&target.coordinate, false).await?.is_some() {
                        bail!("Soft-deleted source version remains active on destination");
                    }
                }
            }
        }
        self.apply_settings(j).await?;
        self.reconcile(j).await?;
        self.verify_scoped_settings().await?;
        Ok(())
    }
    async fn register(&self, s: &Schema) -> Result<Schema> {
        match &self.target {
            Target::Confluent(c) => {
                let mut body = json!({"schema":s.content,"schemaType":s.schema_type,"references":s.references.iter().map(|r| json!({"name":r.name,"subject":r.coordinate.name,"version":r.coordinate.version.parse::<i32>().unwrap()})).collect::<Vec<_>>()});
                for k in ["metadata", "ruleSet"] {
                    if let Some(value) = s.envelope.get(k) {
                        body[k] = value.clone();
                    }
                }
                if self.config.id_policy == IdPolicy::Preserve {
                    body["id"] = json!(s.id);
                    body["version"] = json!(s.coordinate.version.parse::<i32>()?);
                }
                let normalize = self
                    .snapshot
                    .settings
                    .iter()
                    .find(|setting| {
                        self.mapped(&Coordinate {
                            group: setting.group.clone(),
                            name: setting.name.clone(),
                            version: s.coordinate.version.clone(),
                        })
                        .is_ok_and(|c| c.artifact() == s.coordinate.artifact())
                    })
                    .and_then(|s| s.config.as_ref())
                    .and_then(|c| c["normalize"].as_bool())
                    .or_else(|| self.snapshot.global["config"]["normalize"].as_bool())
                    .unwrap_or(false);
                let response = c
                    .mutate(
                        Method::POST,
                        &format!(
                            "/subjects/{}/versions?normalize={normalize}",
                            urlencoding::encode(&s.coordinate.name)
                        ),
                        Some(&body),
                    )
                    .await?;
                let id = response["id"]
                    .as_i64()
                    .context("Confluent registration returned no ID")?;
                for version in self.target.versions(&s.coordinate, true).await? {
                    let mut coord = s.coordinate.clone();
                    coord.version = version;
                    let target = self
                        .target
                        .get(&coord, self.config.id_policy == IdPolicy::Preserve)
                        .await?
                        .context("Registered version disappeared")?;
                    if target.id == id
                        && self.equivalent(s, &target, self.config.id_policy == IdPolicy::Preserve)
                    {
                        return Ok(target);
                    }
                }
                bail!("Confluent returned ID {id} without a matching subject/version envelope")
            }
            Target::Apicurio(c) => {
                let group = urlencoding::encode(&s.coordinate.group);
                let artifact = urlencoding::encode(&s.coordinate.name);
                let exists = !self.target.versions(&s.coordinate, true).await?.is_empty();
                let version_exists = self.target.get(&s.coordinate, true).await?.is_some();
                let references:Vec<_>=s.references.iter().map(|r| json!({"name":r.name,"groupId":r.coordinate.group,"artifactId":r.coordinate.name,"version":r.coordinate.version})).collect();
                let mut headers = vec![];
                let (path, content_type, body) = match c.api_version() {
                    ApicurioApiVersion::V3 => {
                        let mut version = json!({"content":{"content":s.content,"contentType":if s.schema_type=="PROTOBUF" {"application/x-protobuf"} else {"application/json"},"references":references}});
                        if !version_exists {
                            version["version"] = json!(s.coordinate.version);
                        }
                        for field in ["name", "description", "labels"] {
                            if let Some(value) = s.envelope.get(field).filter(|v| !v.is_null()) {
                                version[field] = value.clone();
                            }
                        }
                        if exists {
                            (
                                format!("/groups/{group}/artifacts/{artifact}/versions"),
                                "application/json",
                                version,
                            )
                        } else {
                            (
                                format!("/groups/{group}/artifacts"),
                                "application/json",
                                json!({"artifactId":s.coordinate.name,"artifactType":s.schema_type,"firstVersion":version}),
                            )
                        }
                    }
                    ApicurioApiVersion::V2 => {
                        if !version_exists {
                            headers
                                .push(("X-Registry-Version".into(), s.coordinate.version.clone()));
                        }
                        if !exists {
                            headers
                                .push(("X-Registry-ArtifactId".into(), s.coordinate.name.clone()));
                            headers.push(("X-Registry-ArtifactType".into(), s.schema_type.clone()));
                        }
                        (
                            if exists {
                                format!("/groups/{group}/artifacts/{artifact}/versions")
                            } else {
                                format!("/groups/{group}/artifacts")
                            },
                            "application/create.extended+json",
                            json!({"content":s.content,"references":references}),
                        )
                    }
                };
                let response = c
                    .mutate(
                        Method::POST,
                        &path,
                        content_type,
                        Bytes::from(serde_json::to_vec(&body)?),
                        &headers,
                    )
                    .await?;
                let response = response
                    .get("version")
                    .filter(|v| v.is_object())
                    .unwrap_or(&response);
                let version = response["version"]
                    .as_str()
                    .context("Apicurio registration returned no version")?;
                let mut coordinate = s.coordinate.clone();
                coordinate.version = version.into();
                if c.api_version() == ApicurioApiVersion::V2 {
                    let mut meta = json!({});
                    for field in ["name", "description", "properties", "legacyLabels"] {
                        if let Some(value) = s.envelope.get(field).filter(|v| !v.is_null()) {
                            meta[if field == "legacyLabels" {
                                "labels"
                            } else {
                                field
                            }] = value.clone();
                        }
                    }
                    if meta != json!({}) {
                        c.mutate(
                            Method::PUT,
                            &format!(
                                "/groups/{group}/artifacts/{artifact}/versions/{}/meta",
                                urlencoding::encode(version)
                            ),
                            "application/json",
                            Bytes::from(serde_json::to_vec(&meta)?),
                            &[],
                        )
                        .await?;
                    }
                }
                if let Some(state) = s.state.as_deref().filter(|s| *s != "ENABLED") {
                    let path = format!(
                        "/groups/{group}/artifacts/{artifact}/versions/{}/state",
                        urlencoding::encode(version)
                    );
                    c.mutate(
                        Method::PUT,
                        &path,
                        "application/json",
                        Bytes::from(serde_json::to_vec(&json!({"state":state}))?),
                        &[],
                    )
                    .await?;
                }
                self.target
                    .get(&coordinate, true)
                    .await?
                    .context("Apicurio registration was not visible")
            }
        }
    }
    async fn record_mappings(
        &self,
        j: &mut Journal,
        mappings: &BTreeMap<Coordinate, Schema>,
    ) -> Result<()> {
        j.mappings = mappings
            .iter()
            .map(|(source, t)| Mapping {
                source: source.clone(),
                destination: t.coordinate.clone(),
                source_id: self
                    .snapshot
                    .schemas
                    .iter()
                    .find(|s| &s.coordinate == source)
                    .unwrap()
                    .id,
                destination_id: t.id,
                destination_content_id: t.content_id,
                destination_guid: t
                    .envelope
                    .get("guid")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            })
            .collect();
        self.save(j).await
    }
    pub async fn verify(&self, report: &RegistryReport) -> Result<()> {
        let j = self
            .read_journal()
            .await?
            .context("Missing registry execution journal")?;
        self.check_journal(&j)?;
        if !report.cleanup_verified || !j.recovery.is_empty() {
            bail!("Temporary settings cleanup is unverified");
        }
        let actual = self.reconcile(&j).await?;
        if actual.len() != self.snapshot.schemas.len() {
            bail!("Registry verification has incomplete mappings");
        }
        if self.target.provider() == Provider::Confluent {
            for source in &self.snapshot.schemas {
                if let Some(deleted) = source.deleted {
                    let target = &actual[&source.coordinate];
                    let active = self.target.get(&target.coordinate, false).await?.is_some();
                    if active == deleted {
                        bail!(
                            "Destination deletion state differs from snapshot: {}",
                            target.coordinate.path()
                        );
                    }
                }
            }
        }
        self.verify_scoped_settings().await?;
        Ok(())
    }
    async fn reconcile(&self, j: &Journal) -> Result<BTreeMap<Coordinate, Schema>> {
        let mut actual = BTreeMap::new();
        for m in &j.mappings {
            let target = self
                .target
                .get(&m.destination, true)
                .await?
                .context(format!(
                    "Checkpoint schema missing: {}",
                    m.destination.path()
                ))?;
            if target.id != m.destination_id || target.content_id != m.destination_content_id {
                bail!("Destination schema IDs changed since checkpoint");
            }
            actual.insert(m.source.clone(), target);
        }
        for m in &j.mappings {
            let source = self
                .snapshot
                .schemas
                .iter()
                .find(|s| s.coordinate == m.source)
                .context("Unknown checkpoint source coordinate")?;
            let expected = self.with_references(source, &actual, true)?;
            if !self.equivalent(
                &expected,
                &actual[&m.source],
                self.config.id_policy == IdPolicy::Preserve,
            ) {
                bail!(
                    "Checkpoint schema content/envelope/references changed: {}",
                    m.destination.path()
                );
            }
        }
        Ok(actual)
    }
    async fn verify_scoped_settings(&self) -> Result<()> {
        let Target::Apicurio(c) = &self.target else {
            return Ok(());
        };
        if self.snapshot.provider != Provider::Apicurio {
            return Ok(());
        }
        for (group, metadata) in &self.snapshot.groups {
            let target_group = self.mapped_group(group)?;
            let target = serde_json::to_value(c.get_group(&target_group).await?)?;
            for field in ["description", "labels", "properties"] {
                if meaningful(&metadata["original_metadata"][field]).is_some()
                    && meaningful(&metadata["original_metadata"][field])
                        != meaningful(&target[field])
                {
                    bail!("Group metadata verification failed: {group}/{field}");
                }
            }
            let mut rules = vec![];
            for t in c.list_group_rules(&target_group).await? {
                rules.push(serde_json::to_value(
                    c.get_group_rule(&target_group, &t).await?,
                )?);
            }
            if !contains_rules(&metadata["rules"], &json!(rules)) {
                bail!("Group rules verification failed: {group}");
            }
        }
        for setting in &self.snapshot.settings {
            let source_key = format!("{}/{}", setting.group, setting.name);
            let Some(metadata) = self.snapshot.artifacts.get(&source_key) else {
                continue;
            };
            let destination = self.mapped(&Coordinate {
                group: setting.group.clone(),
                name: setting.name.clone(),
                version: "1".into(),
            })?;
            let target = serde_json::to_value(
                c.get_artifact_metadata(&destination.group, &destination.name)
                    .await?,
            )?;
            for field in [
                "name",
                "description",
                "labels",
                "properties",
                "legacyLabels",
            ] {
                if meaningful(&metadata["original_metadata"][field]).is_some()
                    && meaningful(&metadata["original_metadata"][field])
                        != meaningful(&target[field])
                {
                    bail!("Artifact metadata verification failed: {source_key}/{field}");
                }
            }
            let mut rules = vec![];
            for t in c
                .list_artifact_rules(&destination.group, &destination.name)
                .await?
            {
                rules.push(serde_json::to_value(
                    c.get_artifact_rule(&destination.group, &destination.name, &t)
                        .await?,
                )?);
            }
            if !contains_rules(&metadata["rules"], &json!(rules)) {
                bail!("Artifact rules verification failed: {source_key}");
            }
        }
        Ok(())
    }
    fn mapped_group(&self, group: &str) -> Result<String> {
        let mappings: BTreeSet<_> = self
            .config
            .coordinates
            .iter()
            .filter(|m| m.source_group == group)
            .map(|m| m.target_group.clone())
            .collect();
        if mappings.len() > 1 {
            bail!("One group maps to multiple destination groups; group settings cannot be faithfully restored");
        }
        Ok(mappings.into_iter().next().unwrap_or_else(|| group.into()))
    }
    async fn apply_settings(&self, j: &mut Journal) -> Result<()> {
        if self.snapshot.provider != self.target.provider() {
            return Ok(());
        }
        match &self.target {
            Target::Confluent(c) => {
                for setting in &self.snapshot.settings {
                    let coordinate = self.mapped(&Coordinate {
                        group: setting.group.clone(),
                        name: setting.name.clone(),
                        version: "1".into(),
                    })?;
                    if let Some(r) = j.recovery.iter_mut().find(|r| r.name == coordinate.name) {
                        r.success_mode = setting.mode.clone();
                        self.save(j).await?;
                    }
                    let original = j.recovery.iter().find(|r| r.name == coordinate.name);
                    // Existing explicit policy is preserved; changing it is not
                    // an implicit side effect of registering a schema.
                    if original.and_then(|r| r.config.as_ref()).is_none() {
                        if let Some(config) = &setting.config {
                            if let Some(current) = c.get_subject_config(&coordinate.name).await? {
                                if config_put(&serde_json::to_value(current)?) != config_put(config)
                                {
                                    bail!("Subject configuration changed concurrently; refusing overwrite");
                                }
                            }
                            if let Some(r) =
                                j.recovery.iter_mut().find(|r| r.name == coordinate.name)
                            {
                                r.success_config = Some(config.clone());
                                self.save(j).await?;
                            }
                            c.mutate(
                                Method::PUT,
                                &format!("/config/{}", urlencoding::encode(&coordinate.name)),
                                Some(&config_put(config)),
                            )
                            .await?;
                            let returned = c
                                .get_subject_config(&coordinate.name)
                                .await?
                                .context("Subject config was not applied")?;
                            if config_put(&serde_json::to_value(returned)?) != config_put(config) {
                                bail!("Subject configuration failed verification");
                            }
                            if let Some(r) =
                                j.recovery.iter_mut().find(|r| r.name == coordinate.name)
                            {
                                r.success_config = Some(config.clone());
                                r.success_mode = setting.mode.clone();
                                self.save(j).await?;
                            }
                        }
                    }
                }
                if self.config.restore_global_settings {
                    // Global source restoration is explicit and was gated on an
                    // empty target before any writes. Do not copy source IMPORT.
                    let config = self
                        .snapshot
                        .global
                        .get("config")
                        .context("Missing source global settings")?;
                    if let Some(r) = j.recovery.iter_mut().find(|r| r.global) {
                        r.success_config = Some(config.clone());
                        r.success_mode = self.snapshot.global["mode"].as_str().map(str::to_owned);
                        self.save(j).await?;
                    }
                    c.mutate(Method::PUT, "/config", Some(&config_put(config)))
                        .await?;
                }
            }
            Target::Apicurio(c) => {
                for (group, metadata) in &self.snapshot.groups {
                    let target_group = self.mapped_group(group)?;
                    let old = c.get_group(&target_group).await;
                    let mut body = json!({"groupId":target_group});
                    if c.api_version() == ApicurioApiVersion::V2 {
                        body = json!({"id":target_group});
                    }
                    for field in ["description", "labels", "properties"] {
                        let value = &metadata["original_metadata"][field];
                        if !value.is_null() {
                            body[field] = value.clone();
                        }
                    }
                    match old {
                        Err(e) if ap_absent(&e) => {
                            c.mutate(
                                Method::POST,
                                "/groups",
                                "application/json",
                                Bytes::from(serde_json::to_vec(&body)?),
                                &[],
                            )
                            .await?;
                        }
                        Ok(old) => {
                            let old = serde_json::to_value(old)?;
                            let mut update = json!({});
                            for field in ["description", "labels", "properties"] {
                                if meaningful(&body[field]).is_some()
                                    && meaningful(&old[field]) != meaningful(&body[field])
                                {
                                    if meaningful(&old[field]).is_some() {
                                        bail!("Existing group metadata differs; refusing implicit overwrite");
                                    }
                                    update[field] = body[field].clone();
                                }
                            }
                            if update != json!({}) {
                                c.mutate(
                                    Method::PUT,
                                    &format!("/groups/{}", urlencoding::encode(&target_group)),
                                    "application/json",
                                    Bytes::from(serde_json::to_vec(&update)?),
                                    &[],
                                )
                                .await?;
                            }
                        }
                        Err(e) => return Err(e.into()),
                    }
                    let base = format!("/groups/{}/rules", urlencoding::encode(&target_group));
                    let existing = c.list_group_rules(&target_group).await?;
                    for rule in metadata["rules"].as_array().into_iter().flatten() {
                        let typ = rule["ruleType"].as_str().context("Invalid group rule")?;
                        if existing.iter().any(|t| t == typ) {
                            let current = c.get_group_rule(&target_group, typ).await?;
                            if current.config != rule["config"].as_str().unwrap_or("") {
                                bail!(
                                    "Existing group rule differs; refusing to weaken target policy"
                                );
                            }
                        } else {
                            c.mutate(
                                Method::POST,
                                &base,
                                "application/json",
                                Bytes::from(serde_json::to_vec(rule)?),
                                &[],
                            )
                            .await?;
                        }
                    }
                }
                for setting in &self.snapshot.settings {
                    let destination = self.mapped(&Coordinate {
                        group: setting.group.clone(),
                        name: setting.name.clone(),
                        version: "1".into(),
                    })?;
                    let metadata =
                        &self.snapshot.artifacts[&format!("{}/{}", setting.group, setting.name)];
                    let mut body = json!({});
                    for field in [
                        "name",
                        "description",
                        "labels",
                        "properties",
                        "legacyLabels",
                    ] {
                        let value = &metadata["original_metadata"][field];
                        if meaningful(value).is_some() {
                            body[if field == "legacyLabels" {
                                "labels"
                            } else {
                                field
                            }] = value.clone();
                        }
                    }
                    let artifact_base = format!(
                        "/groups/{}/artifacts/{}",
                        urlencoding::encode(&destination.group),
                        urlencoding::encode(&destination.name)
                    );
                    let current = c
                        .get_artifact_metadata(&destination.group, &destination.name)
                        .await;
                    match current {
                        Ok(current) => {
                            if body != json!({}) {
                                let current = serde_json::to_value(current)?;
                                let changed = body
                                    .as_object()
                                    .unwrap()
                                    .iter()
                                    .any(|(k, v)| meaningful(v) != meaningful(&current[k]));
                                if changed {
                                    let path = if c.api_version() == ApicurioApiVersion::V2 {
                                        format!("{artifact_base}/meta")
                                    } else {
                                        artifact_base.clone()
                                    };
                                    c.mutate(
                                        Method::PUT,
                                        &path,
                                        "application/json",
                                        Bytes::from(serde_json::to_vec(&body)?),
                                        &[],
                                    )
                                    .await?;
                                }
                            }
                        }
                        Err(e) if ap_absent(&e) && c.api_version() == ApicurioApiVersion::V3 => {
                            body["artifactId"] = json!(destination.name);
                            body["artifactType"] = metadata["artifact_type"].clone();
                            c.mutate(
                                Method::POST,
                                &format!(
                                    "/groups/{}/artifacts",
                                    urlencoding::encode(&destination.group)
                                ),
                                "application/json",
                                Bytes::from(serde_json::to_vec(&body)?),
                                &[],
                            )
                            .await?;
                        }
                        Err(e) => return Err(e.into()),
                    }
                    let base = format!(
                        "/groups/{}/artifacts/{}/rules",
                        urlencoding::encode(&destination.group),
                        urlencoding::encode(&destination.name)
                    );
                    let existing: Vec<String> = c.get_json(&base).await?;
                    for rule in setting
                        .config
                        .as_ref()
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        let typ = rule["ruleType"]
                            .as_str()
                            .or_else(|| rule["rule_type"].as_str())
                            .context("Invalid artifact rule")?;
                        if existing.iter().any(|s| s == typ) {
                            let current: Value = c.get_json(&format!("{base}/{typ}")).await?;
                            if current["config"] != rule["config"] {
                                bail!("Existing destination artifact rule differs; refusing to weaken target policy");
                            }
                        } else {
                            c.mutate(
                                Method::POST,
                                &base,
                                "application/json",
                                Bytes::from(serde_json::to_vec(
                                    &json!({"ruleType":typ,"config":rule["config"]}),
                                )?),
                                &[],
                            )
                            .await?;
                        }
                    }
                }
                if self.config.restore_global_settings {
                    if !j.recovery.iter().any(|r| r.name == "__global__") {
                        j.recovery.push(Recovery {
                            name: "__global__".into(),
                            mode: None,
                            config: Some(json!(self.ap_global_rules(c).await?)),
                            success_config: Some(self.snapshot.global.clone()),
                            success_mode: None,
                            temporary_mode: None,
                            global: false,
                        });
                        self.save(j).await?;
                    }
                    self.set_ap_global(c, self.snapshot.global.clone()).await?;
                }
            }
        }
        Ok(())
    }
    async fn cleanup(&self, j: &mut Journal, success: bool) -> Result<()> {
        while let Some(recovery) = j.recovery.last().cloned() {
            match &self.target {
                Target::Confluent(c) => {
                    let path = if recovery.global {
                        "/mode".into()
                    } else {
                        format!("/mode/{}", urlencoding::encode(&recovery.name))
                    };
                    let desired_mode = if success {
                        recovery.success_mode.as_ref().or(recovery.mode.as_ref())
                    } else {
                        recovery.mode.as_ref()
                    };
                    let current = if recovery.global {
                        c.get_global_mode().await?.map(|m| m.mode)
                    } else {
                        c.get_subject_mode(&recovery.name).await?.map(|m| m.mode)
                    };
                    if current.as_ref() != desired_mode
                        && current != recovery.mode
                        && current != recovery.temporary_mode
                    {
                        bail!(
                            "Subject mode changed concurrently; refusing cleanup overwrite for {}",
                            recovery.name
                        );
                    }
                    if current.as_ref() != desired_mode {
                        match desired_mode {
                            Some(mode) => {
                                c.mutate(Method::PUT, &path, Some(&json!({"mode":mode})))
                                    .await?;
                            }
                            None => {
                                c.mutate(Method::DELETE, &path, None).await?;
                            }
                        }
                    }
                    let returned = if recovery.global {
                        c.get_global_mode().await?.map(|m| m.mode)
                    } else {
                        c.get_subject_mode(&recovery.name).await?.map(|m| m.mode)
                    };
                    if returned.as_ref() != desired_mode {
                        bail!("Mode cleanup verification failed for {}", recovery.name);
                    }
                    // Configuration captured before a temporary change is kept
                    // until the caller explicitly replaces it on success.
                    let desired_config = if success {
                        recovery
                            .success_config
                            .as_ref()
                            .or(recovery.config.as_ref())
                    } else {
                        recovery.config.as_ref()
                    };
                    let config_path = if recovery.global {
                        "/config".into()
                    } else {
                        format!("/config/{}", urlencoding::encode(&recovery.name))
                    };
                    let current = if recovery.global {
                        Some(serde_json::to_value(c.get_global_config().await?)?)
                    } else {
                        c.get_subject_config(&recovery.name)
                            .await?
                            .map(serde_json::to_value)
                            .transpose()?
                    };
                    let current_put = current.as_ref().map(config_put);
                    if current_put != recovery.config.as_ref().map(config_put)
                        && current_put != recovery.success_config.as_ref().map(config_put)
                    {
                        bail!(
                            "Configuration changed concurrently; refusing cleanup overwrite for {}",
                            recovery.name
                        );
                    }
                    if current_put == desired_config.map(config_put) {
                        // No temporary configuration change needs recovery.
                    } else if let Some(config) = desired_config {
                        c.mutate(Method::PUT, &config_path, Some(&config_put(config)))
                            .await?;
                        let actual = if recovery.global {
                            Some(serde_json::to_value(c.get_global_config().await?)?)
                        } else {
                            c.get_subject_config(&recovery.name)
                                .await?
                                .map(serde_json::to_value)
                                .transpose()?
                        };
                        if actual.as_ref().map(config_put) != Some(config_put(config)) {
                            bail!("Configuration cleanup verification failed");
                        }
                    } else {
                        c.mutate(Method::DELETE, &config_path, None).await?;
                        if c.get_subject_config(&recovery.name).await?.is_some() {
                            bail!("Inherited configuration cleanup failed");
                        }
                    }
                }
                Target::Apicurio(c) => {
                    if recovery.name == "__global__" {
                        let current = json!(self.ap_global_rules(c).await?);
                        if !rules_recovery_safe(
                            &current,
                            recovery.config.as_ref(),
                            recovery.success_config.as_ref(),
                        ) {
                            bail!("Global rules changed concurrently; refusing cleanup overwrite");
                        }
                        self.set_ap_global(
                            c,
                            (if success && self.config.restore_global_settings {
                                recovery
                                    .success_config
                                    .as_ref()
                                    .or(recovery.config.as_ref())
                            } else {
                                recovery.config.as_ref()
                            })
                            .cloned()
                            .context("Missing global settings recovery data")?,
                        )
                        .await?;
                    }
                }
            }
            j.recovery.pop();
            self.save(j).await?;
        }
        Ok(())
    }
    async fn ap_global_rules(&self, c: &ApicurioRegistryClient) -> Result<Vec<Value>> {
        let mut rules = vec![];
        for t in c.list_global_rules().await? {
            rules.push(serde_json::to_value(c.get_global_rule(&t).await?)?);
        }
        Ok(rules)
    }
    async fn set_ap_global(&self, c: &ApicurioRegistryClient, desired: Value) -> Result<()> {
        let rules = desired.as_array().context("Invalid global rule recovery")?;
        for existing in c.list_global_rules().await? {
            if !rules.iter().any(|r| r["ruleType"] == existing) {
                c.mutate(
                    Method::DELETE,
                    &format!("/admin/rules/{existing}"),
                    "application/json",
                    Bytes::new(),
                    &[],
                )
                .await?;
            }
        }
        for rule in rules {
            let name = rule["ruleType"].as_str().context("Invalid global rule")?;
            let types = c.list_global_rules().await?;
            let (method, path) = if types.iter().any(|t| t == name) {
                (Method::PUT, format!("/admin/rules/{name}"))
            } else {
                (Method::POST, "/admin/rules".into())
            };
            c.mutate(
                method,
                &path,
                "application/json",
                Bytes::from(serde_json::to_vec(rule)?),
                &[],
            )
            .await?;
        }
        let mut actual = self.ap_global_rules(c).await?;
        let mut expected = rules.clone();
        actual.sort_by_key(Value::to_string);
        expected.sort_by_key(Value::to_string);
        if actual != expected {
            bail!("Global settings recovery verification failed");
        }
        Ok(())
    }
    pub async fn write_report(&self, report: &RegistryReport) -> Result<()> {
        tokio::fs::write(
            self.journal_path.with_extension("report.json"),
            serde_json::to_vec_pretty(report)?,
        )
        .await?;
        Ok(())
    }
    async fn validate_archive(&self) -> Result<()> {
        let bytes = self
            .storage
            .get(&format!("{}/_export.zip", self.snapshot.prefix))
            .await?;
        self.check_archive_hash(&bytes)?;
        let mut zip = zip::ZipArchive::new(Cursor::new(bytes))?;
        let mut versions = BTreeMap::new();
        let mut contents = BTreeMap::new();
        let mut groups = BTreeSet::new();
        let mut artifacts = BTreeSet::new();
        let mut branches = vec![];
        let mut manifest = None;
        for i in 0..zip.len() {
            let mut f = zip.by_index(i)?;
            let name = f.name().to_owned();
            let mut data = vec![];
            f.read_to_end(&mut data)?;
            if name.ends_with(".Manifest.json") {
                manifest = Some(serde_json::from_slice::<Value>(&data)?);
            }
            if name.ends_with(".Group.json") {
                let group: Value = serde_json::from_slice(&data)?;
                let group_id = group["groupId"].as_str().unwrap_or("default");
                if !groups.insert(group_id.to_owned()) {
                    bail!("Duplicate archive group");
                }
            }
            if name.ends_with(".Artifact.json") {
                let artifact: Value = serde_json::from_slice(&data)?;
                let coordinate = (
                    artifact["groupId"].as_str().unwrap_or("default").to_owned(),
                    artifact["artifactId"]
                        .as_str()
                        .context("Archive artifact missing")?
                        .to_owned(),
                );
                if !artifacts.insert(coordinate) {
                    bail!("Duplicate archive artifact");
                }
            }
            if name.ends_with(".Branch.json") {
                branches.push(serde_json::from_slice::<Value>(&data)?);
            }
            if name.ends_with(".ArtifactVersion.json") {
                let v: Value = serde_json::from_slice(&data)?;
                let c = Coordinate {
                    group: v["groupId"].as_str().unwrap_or("default").into(),
                    name: v["artifactId"]
                        .as_str()
                        .context("Archive artifact missing")?
                        .into(),
                    version: v["version"]
                        .as_str()
                        .context("Archive version missing")?
                        .into(),
                };
                if versions.insert(c, v).is_some() {
                    bail!("Duplicate archive version");
                }
            }
            if name.ends_with(".Content.json") {
                let meta: Value = serde_json::from_slice(&data)?;
                if contents
                    .insert(
                        meta["contentId"]
                            .as_i64()
                            .context("Archive content ID missing")?,
                        (meta, name.trim_end_matches(".json").to_owned() + ".data"),
                    )
                    .is_some()
                {
                    bail!("Duplicate archive content ID");
                }
            }
        }
        let manifest = manifest.context("Archive manifest missing")?;
        let archive_version = manifest["exportVersion"]
            .as_str()
            .context("Missing archive export version")?;
        if (self.snapshot.api_version == "v2" && archive_version != "1.0")
            || (self.snapshot.api_version == "v3" && !matches!(archive_version, "2.0" | "3.0"))
        {
            bail!("Unsupported native archive generation {archive_version}");
        }
        if versions.len() != self.snapshot.schemas.len() {
            bail!("Archive scope differs from structured snapshot; refusing whole-registry import");
        }
        groups.insert("default".into());
        let mut expected_groups: BTreeSet<_> = self.snapshot.groups.keys().cloned().collect();
        expected_groups.insert("default".into());
        if groups != expected_groups {
            bail!("Archive group scope differs from structured snapshot");
        }
        if self.snapshot.api_version == "v3" {
            let expected: BTreeSet<_> = self
                .snapshot
                .settings
                .iter()
                .map(|s| (s.group.clone(), s.name.clone()))
                .collect();
            if artifacts != expected {
                bail!("Archive artifact scope differs from structured snapshot");
            }
        }
        for branch in branches {
            let group = branch["groupId"].as_str().unwrap_or("default").to_owned();
            let name = branch["artifactId"]
                .as_str()
                .context("Archive branch artifact missing")?
                .to_owned();
            if !artifacts.contains(&(group.clone(), name.clone())) {
                bail!("Archive branch outside artifact scope");
            }
            for version in branch["versions"]
                .as_array()
                .context("Archive branch versions missing")?
            {
                let coordinate = Coordinate {
                    group: group.clone(),
                    name: name.clone(),
                    version: version
                        .as_str()
                        .context("Invalid archive branch version")?
                        .into(),
                };
                if !versions.contains_key(&coordinate) {
                    bail!("Archive branch references a version outside structured scope");
                }
            }
        }
        for s in &self.snapshot.schemas {
            let v = versions
                .get(&s.coordinate)
                .context("Structured version missing from archive")?;
            if v["globalId"].as_i64() != Some(s.id)
                || v["contentId"].as_i64() != s.content_id
                || v["state"].as_str() != s.state.as_deref()
            {
                bail!("Archive IDs/state differ from structured snapshot");
            }
            let (meta, file) = contents
                .get(&s.content_id.context("Missing content ID")?)
                .context("Archive content missing")?;
            let mut content = String::new();
            zip.by_name(file)?.read_to_string(&mut content)?;
            if content != s.content {
                bail!("Archive content differs from structured snapshot");
            }
            let mut references = meta["serializedReferences"]
                .as_str()
                .map(serde_json::from_str::<Value>)
                .transpose()?
                .unwrap_or(json!([]));
            for reference in references
                .as_array_mut()
                .context("Invalid archive references")?
            {
                if reference["groupId"].is_null() {
                    reference["groupId"] = json!("default");
                }
            }
            let expected:Vec<_>=s.references.iter().map(|r| json!({"groupId":r.coordinate.group,"artifactId":r.coordinate.name,"version":r.coordinate.version,"name":r.name})).collect();
            if references != json!(expected) {
                bail!("Archive reference envelope differs from structured snapshot");
            }
        }
        Ok(())
    }
    fn check_archive_hash(&self, bytes: &[u8]) -> Result<()> {
        let key = format!("{}/_export.zip", self.snapshot.prefix);
        let expected = self
            .snapshot
            .info
            .as_ref()
            .and_then(|i| i.objects.get(&key))
            .context("Archive has no recorded integrity hash")?;
        if snapshot::hash(bytes) != *expected {
            bail!("Archive hash mismatch");
        }
        Ok(())
    }
}
fn config_put(value: &Value) -> Value {
    let mut value = value.clone();
    if let Some(object) = value.as_object_mut() {
        if let Some(v) = object.remove("compatibilityLevel") {
            object.insert("compatibility".into(), v);
        }
        object.retain(|_, v| !v.is_null());
    }
    value
}
fn schema_content_equal(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }
    match (
        serde_json::from_str::<Value>(a),
        serde_json::from_str::<Value>(b),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}
fn schema_envelope_content_equal(a: &Schema, b: &Schema) -> bool {
    if a.schema_type != "PROTOBUF" {
        return schema_content_equal(&a.content, &b.content);
    }
    let descriptor = |text: &str| -> Option<prost_types::FileDescriptorProto> {
        let mut descriptor = protox_parse::parse("root.proto", text).ok()?;
        descriptor.source_code_info = None;
        Some(descriptor)
    };
    match (descriptor(&a.content), descriptor(&b.content)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}
fn equivalent(expected: &Schema, actual: &Schema, preserve: bool) -> bool {
    if expected.schema_type != actual.schema_type
        || !schema_envelope_content_equal(expected, actual)
        || expected.references.len() != actual.references.len()
    {
        return false;
    }
    if !expected.references.iter().all(|r| {
        actual
            .references
            .iter()
            .any(|a| a.name == r.name && a.coordinate == r.coordinate)
    }) {
        return false;
    }
    if expected
        .state
        .as_ref()
        .is_some_and(|s| actual.state.as_ref() != Some(s))
    {
        return false;
    }
    for key in [
        "metadata",
        "ruleSet",
        "name",
        "description",
        "labels",
        "legacyLabels",
        "properties",
    ] {
        let a = expected
            .envelope
            .get(key)
            .filter(|v| !v.is_null() && **v != json!({}) && **v != json!([]));
        let b = actual
            .envelope
            .get(key)
            .filter(|v| !v.is_null() && **v != json!({}) && **v != json!([]));
        if a != b {
            return false;
        }
    }
    // With preservation, a GUID recorded in the snapshot must match.
    !(preserve
        && expected
            .envelope
            .get("guid")
            .is_some_and(|g| actual.envelope.get("guid") != Some(g)))
}
fn guid(value: &str) -> Result<String> {
    let value = value.replace('-', "").to_lowercase();
    if value.len() != 32 || !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        bail!("Invalid schema GUID");
    }
    Ok(value)
}
fn kafka_topic_matches(pattern: &str, topic: &str) -> bool {
    if let Some(pattern) = pattern.strip_prefix('~') {
        regex::Regex::new(pattern).is_ok_and(|p| p.is_match(topic))
    } else {
        glob::Pattern::new(pattern).is_ok_and(|p| p.matches(topic))
    }
}
fn schema_descriptors(
    snapshot: &Snapshot,
    schema: &Schema,
) -> Result<prost_types::FileDescriptorSet> {
    let temp = tempfile::tempdir()?;
    let root = temp.path().join("root.proto");
    std::fs::write(&root, &schema.content)?;
    let mut queue = vec![schema];
    let mut files = BTreeMap::new();
    while let Some(s) = queue.pop() {
        for r in &s.references {
            let path = Path::new(&r.name);
            if path.is_absolute()
                || path
                    .components()
                    .any(|c| !matches!(c, std::path::Component::Normal(_)))
            {
                bail!("Unsafe Protobuf import path {}", r.name);
            }
            let dep = snapshot
                .schemas
                .iter()
                .find(|s| s.coordinate == r.coordinate)
                .context("Missing Protobuf dependency")?;
            if let Some(old) = files.insert(r.name.clone(), dep.content.clone()) {
                if old != dep.content {
                    bail!("Conflicting Protobuf import filename");
                }
                continue;
            }
            let full = temp.path().join(path);
            if let Some(parent) = full.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(full, &dep.content)?;
            queue.push(dep);
        }
    }
    Ok(protox::compile([root], [temp.path()])?)
}
fn message_indexes(snapshot: &Snapshot, schema: &Schema) -> Result<BTreeMap<String, Vec<u32>>> {
    if schema.schema_type != "PROTOBUF" {
        return Ok(BTreeMap::new());
    }
    let descriptors = schema_descriptors(snapshot, schema)?;
    let file = descriptors
        .file
        .iter()
        .find(|f| f.name.as_deref() == Some("root.proto"))
        .context("Missing root Protobuf descriptor")?;
    fn visit(
        messages: &[prost_types::DescriptorProto],
        prefix: &str,
        parents: &[u32],
        out: &mut BTreeMap<String, Vec<u32>>,
    ) {
        for (i, message) in messages.iter().enumerate() {
            let name = message.name.as_deref().unwrap_or("");
            let name = if prefix.is_empty() {
                name.into()
            } else {
                format!("{prefix}.{name}")
            };
            let mut indexes = parents.to_vec();
            indexes.push(i as u32);
            out.insert(name.clone(), indexes.clone());
            visit(&message.nested_type, &name, &indexes, out);
        }
    }
    let mut out = BTreeMap::new();
    visit(
        &file.message_type,
        file.package.as_deref().unwrap_or(""),
        &[],
        &mut out,
    );
    Ok(out)
}

fn has_rules(schema: &Schema) -> bool {
    schema.envelope.get("ruleSet").is_some_and(|rule_set| {
        rule_set.as_object().map_or_else(
            || meaningful(rule_set).is_some(),
            |rules| rules.values().any(|rule| meaningful(rule).is_some()),
        )
    })
}

fn meaningful(value: &Value) -> Option<&Value> {
    (!value.is_null() && *value != json!({}) && *value != json!([])).then_some(value)
}

fn contains_rules(required: &Value, actual: &Value) -> bool {
    required
        .as_array()
        .into_iter()
        .flatten()
        .all(|r| actual.as_array().is_some_and(|a| a.contains(r)))
}

/// A multi-request global rules update may stop between writes. Only original
/// or intended values (including intended additions/removals) are recoverable.
fn rules_recovery_safe(
    current: &Value,
    original: Option<&Value>,
    intended: Option<&Value>,
) -> bool {
    let by_type = |v: Option<&Value>| -> BTreeMap<String, Value> {
        v.and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|r| r["ruleType"].as_str().map(|t| (t.to_owned(), r.clone())))
            .collect()
    };
    let current = by_type(Some(current));
    let original = by_type(original);
    let intended = by_type(intended);
    current
        .keys()
        .chain(original.keys())
        .chain(intended.keys())
        .all(|key| current.get(key) == original.get(key) || current.get(key) == intended.get(key))
}

fn confluent_cloud(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_owned))
        .is_some_and(|h| h == "confluent.cloud" || h.ends_with(".confluent.cloud"))
}

fn derived_display(schema: &Schema, field: &str, value: &Value) -> bool {
    let Some(value) = value.as_str() else {
        return false;
    };
    if value.is_empty() {
        return true;
    }
    if let Ok(json) = serde_json::from_str::<Value>(&schema.content) {
        if field == "name" {
            return json["name"] == value
                || json["title"] == value
                || (json["namespace"]
                    .as_str()
                    .zip(json["name"].as_str())
                    .is_some_and(|(namespace, name)| format!("{namespace}.{name}") == value));
        }
        if field == "description" {
            return json["doc"] == value || json["description"] == value;
        }
    }
    if field == "name" && schema.schema_type == "PROTOBUF" {
        return protox_parse::parse("root.proto", &schema.content).is_ok_and(|file| {
            file.message_type
                .iter()
                .any(|m| m.name.as_deref() == Some(value))
        });
    }
    false
}

fn canonical_descriptors(
    mut descriptors: prost_types::FileDescriptorSet,
) -> prost_types::FileDescriptorSet {
    fn messages(values: &mut [prost_types::DescriptorProto]) {
        for message in values.iter_mut() {
            message.field.sort_by_key(|f| f.number);
            messages(&mut message.nested_type);
            message.enum_type.sort_by(|a, b| a.name.cmp(&b.name));
        }
        values.sort_by(|a, b| a.name.cmp(&b.name));
    }
    for file in &mut descriptors.file {
        file.source_code_info = None;
        messages(&mut file.message_type);
        file.enum_type.sort_by(|a, b| a.name.cmp(&b.name));
        let old = file.dependency.clone();
        file.dependency.sort();
        for index in file
            .public_dependency
            .iter_mut()
            .chain(file.weak_dependency.iter_mut())
        {
            if let Some(name) = old.get(*index as usize) {
                if let Some(new) = file.dependency.iter().position(|n| n == name) {
                    *index = new as i32;
                }
            }
        }
    }
    descriptors.file.sort_by(|a, b| a.name.cmp(&b.name));
    descriptors
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;

    #[test]
    fn cross_generation_allocation_accepts_only_redundant_display_metadata() {
        let expected = Schema {
            coordinate: Coordinate {
                group: "default".into(),
                name: "oracle".into(),
                version: "1".into(),
            },
            id: 1,
            content_id: Some(1),
            schema_type: "AVRO".into(),
            content: json!({"type":"record","name":"OracleRecord","doc":"Schema documentation","fields":[]}).to_string(),
            references: vec![],
            deleted: None,
            state: Some("ENABLED".into()),
            envelope: BTreeMap::new(),
        };
        let mut engine = RegistryRestore {
            snapshot: Snapshot {
                provider: Provider::Apicurio,
                source_url: "http://source.invalid".into(),
                api_version: "v3".into(),
                schemas: vec![expected.clone()],
                settings: vec![],
                global: json!({}),
                groups: BTreeMap::new(),
                artifacts: BTreeMap::new(),
                info: None,
                digest: "test".into(),
                prefix: "test".into(),
            },
            config: RestoreConfig::default(),
            target: Target::apicurio(
                &serde_json::from_value(json!({"url":"http://target.invalid","api_version":"v2"}))
                    .unwrap(),
            )
            .unwrap(),
            storage: Arc::new(MemoryBackend::new()),
            journal_path: PathBuf::from("unused"),
            target_identity: "test".into(),
        };
        let mut actual = expected.clone();
        actual.envelope.insert("name".into(), json!("OracleRecord"));
        actual
            .envelope
            .insert("description".into(), json!("Schema documentation"));
        assert!(engine.equivalent(&expected, &actual, false));
        assert!(!engine.equivalent(&expected, &actual, true));
        engine.snapshot.api_version = "v2".into();
        assert!(!engine.equivalent(&expected, &actual, false));
        engine.snapshot.api_version = "v3".into();

        actual.envelope.insert("name".into(), json!("Custom name"));
        assert!(!engine.equivalent(&expected, &actual, false));
        actual.envelope.insert("name".into(), json!("OracleRecord"));
        let mut explicit = expected.clone();
        explicit
            .envelope
            .insert("name".into(), json!("Source custom name"));
        assert!(!engine.equivalent(&explicit, &actual, false));
        actual
            .envelope
            .insert("properties".into(), json!({"custom":"policy"}));
        assert!(!engine.equivalent(&expected, &actual, false));
    }
}
