use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::storage::StorageBackend;
use bytes::Bytes;
use tracing::info;

use super::config::ApicurioRegistryConfig;
use crate::apicurio_registry::error::ApicurioRegistryError;
use crate::schema_registry::config::VersionSelection;

use super::client::ApicurioRegistryClient;
use super::models::*;

const PAGE_SIZE: i32 = 100;

/// Apicurio Registry backup engine.
///
/// Orchestrates: export ZIP capture → group/artifact enumeration → reference
/// resolution → storage write → manifest generation.
pub struct ApicurioRegistryBackup {
    client: ApicurioRegistryClient,
    storage: Arc<dyn StorageBackend>,
    backup_id: String,
    config: ApicurioRegistryConfig,
}

impl ApicurioRegistryBackup {
    pub fn new(
        client: ApicurioRegistryClient,
        storage: Arc<dyn StorageBackend>,
        backup_id: String,
        config: ApicurioRegistryConfig,
    ) -> Self {
        Self {
            client,
            storage,
            backup_id,
            config,
        }
    }
    pub async fn run(&self) -> Result<ApicurioRegistryManifest, ApicurioRegistryError> {
        self.run_snapshot()
            .await
            .map_err(|e| ApicurioRegistryError::Other(e.to_string()))
    }
    async fn run_snapshot(&self) -> anyhow::Result<ApicurioRegistryManifest> {
        use crate::registry_restore::snapshot::{
            self, Coordinate, Reference, Schema, SnapshotInfo,
        };
        use std::collections::{BTreeMap, BTreeSet};
        let prefix = ar_prefix(&self.backup_id);
        snapshot::mark_status(self.storage.as_ref(), &prefix, false).await?;
        let opts = self.config.backup.clone().unwrap_or_default();
        let patterns = |p: &[String]| -> anyhow::Result<Vec<glob::Pattern>> {
            Ok(p.iter()
                .map(|p| glob::Pattern::new(p))
                .collect::<Result<_, _>>()?)
        };
        let groups_in = patterns(&opts.groups)?;
        let groups_out = patterns(&opts.exclude_groups)?;
        let artifacts_in = patterns(&opts.artifacts)?;
        let artifacts_out = patterns(&opts.exclude_artifacts)?;
        let mut groups = Vec::new();
        let mut offset = 0;
        loop {
            let page = self.client.list_groups(PAGE_SIZE, offset).await?;
            let n = page.groups.len();
            groups.extend(page.groups);
            if groups.len() as i64 >= page.count || n == 0 {
                break;
            }
            offset += PAGE_SIZE;
        }
        if !groups.iter().any(|g| g.group_id == "default")
            && self.client.list_artifacts("default", 1, 0).await?.count > 0
        {
            groups.push(self.client.get_group("default").await?);
        }
        let mut selected_artifacts = BTreeSet::new();
        let mut selected_groups = BTreeSet::new();
        let mut queue = VecDeque::new();
        for group in groups
            .iter()
            .filter(|g| matches_filter(&g.group_id, &groups_in, &groups_out))
        {
            if opts.artifacts == ["*"] && opts.exclude_artifacts.is_empty() {
                selected_groups.insert(group.group_id.clone());
            }
            let mut artifacts = Vec::new();
            let mut offset = 0;
            loop {
                let page = self
                    .client
                    .list_artifacts(&group.group_id, PAGE_SIZE, offset)
                    .await?;
                let n = page.artifacts.len();
                artifacts.extend(page.artifacts);
                if artifacts.len() as i64 >= page.count || n == 0 {
                    break;
                }
                offset += PAGE_SIZE;
            }
            for artifact in artifacts
                .iter()
                .filter(|a| matches_filter(&a.artifact_id, &artifacts_in, &artifacts_out))
            {
                selected_groups.insert(group.group_id.clone());
                selected_artifacts.insert((group.group_id.clone(), artifact.artifact_id.clone()));
                let mut versions = Vec::new();
                let mut offset = 0;
                loop {
                    let page = self
                        .client
                        .list_versions(&group.group_id, &artifact.artifact_id, PAGE_SIZE, offset)
                        .await?;
                    let n = page.versions.len();
                    versions.extend(page.versions);
                    if versions.len() as i64 >= page.count || n == 0 {
                        break;
                    }
                    offset += PAGE_SIZE;
                }
                versions.sort_by_key(|v| v.global_id);
                if matches!(opts.include_versions, VersionSelection::Latest) {
                    versions = versions.into_iter().last().into_iter().collect();
                }
                for version in versions {
                    queue.push_back((
                        group.group_id.clone(),
                        artifact.artifact_id.clone(),
                        version,
                    ));
                }
            }
        }
        let mut versions: BTreeMap<Coordinate, BackedUpVersion> = BTreeMap::new();
        let mut neutral = Vec::new();
        while let Some((group, name, v)) = queue.pop_front() {
            let coordinate = Coordinate {
                group: group.clone(),
                name: name.clone(),
                version: v.version.clone(),
            };
            selected_groups.insert(group.clone());
            selected_artifacts.insert((group.clone(), name.clone()));
            if versions.contains_key(&coordinate) {
                continue;
            }
            let content = if v.state.as_deref() == Some("DISABLED") {
                self.client.get_content_by_id(v.content_id).await?
            } else {
                self.client
                    .get_version_content(&group, &name, &v.version)
                    .await?
            };
            let references = self
                .client
                .get_version_references(&group, &name, &v.version)
                .await?;
            if opts.include_references {
                for r in &references {
                    let c = Coordinate {
                        group: r.group_id.clone(),
                        name: r.artifact_id.clone(),
                        version: r.version.clone(),
                    };
                    if !versions.contains_key(&c) {
                        queue.push_back((
                            r.group_id.clone(),
                            r.artifact_id.clone(),
                            self.client
                                .get_version_metadata(&r.group_id, &r.artifact_id, &r.version)
                                .await?,
                        ));
                    }
                }
            }
            let backed = BackedUpVersion {
                version: v.version.clone(),
                global_id: v.global_id,
                content_id: v.content_id,
                artifact_type: v.artifact_type.clone(),
                name: v.name.clone(),
                description: v.description.clone(),
                labels: v.labels.clone(),
                references: references.clone(),
                content_hash: snapshot::hash(&content),
                created_on: v.created_on.clone(),
                state: v.state.clone(),
                extra: v.extra.clone(),
            };
            self.write(
                &version_key(&self.backup_id, &group, &name, &v.version),
                &backed,
            )
            .await?;
            self.storage
                .put(
                    &version_content_key(&self.backup_id, &group, &name, &v.version),
                    content.clone(),
                )
                .await?;
            let mut envelope = v.extra;
            for (field, value) in [
                ("name", serde_json::to_value(v.name)?),
                ("description", serde_json::to_value(v.description)?),
                ("labels", serde_json::to_value(v.labels)?),
            ] {
                envelope.insert(field.into(), value);
            }
            neutral.push(Schema {
                coordinate: coordinate.clone(),
                id: v.global_id,
                content_id: Some(v.content_id),
                schema_type: v.artifact_type,
                content: String::from_utf8(content.to_vec())?,
                references: references
                    .into_iter()
                    .map(|r| Reference {
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
            });
            versions.insert(coordinate, backed);
        }
        let global_types = self.client.list_global_rules().await?;
        let mut global_rules = Vec::new();
        for t in global_types {
            global_rules.push(self.client.get_global_rule(&t).await?);
        }
        self.write(&global_rules_key(&self.backup_id), &global_rules)
            .await?;
        let mut group_summaries = Vec::new();
        let group_names = selected_groups;
        let mut artifacts_count = 0;
        for group in &group_names {
            let group_meta = self.client.get_group(group).await?;
            let mut group_rules = Vec::new();
            for t in self.client.list_group_rules(group).await? {
                group_rules.push(self.client.get_group_rule(group, &t).await?);
            }
            let artifacts: BTreeSet<_> = selected_artifacts
                .iter()
                .filter(|(g, _)| g == group)
                .map(|(_, a)| a.clone())
                .collect();
            self.write(&group_metadata_key(&self.backup_id,group),&serde_json::json!({"group_id":group,"description":group_meta.description,"labels":group_meta.labels,"artifact_count":artifacts.len(),"rules":group_rules,"original_metadata":group_meta})).await?;
            for name in &artifacts {
                let meta = self.client.get_artifact_metadata(group, name).await?;
                let mut rules = Vec::new();
                for t in self.client.list_artifact_rules(group, name).await? {
                    rules.push(self.client.get_artifact_rule(group, name, &t).await?);
                }
                let history: Vec<_> = versions
                    .keys()
                    .filter(|c| &c.group == group && &c.name == name)
                    .map(|c| c.version.clone())
                    .collect();
                self.write(&artifact_metadata_key(&self.backup_id,group,name),&serde_json::json!({"group_id":group,"artifact_id":name,"artifact_type":meta.artifact_type,"versions":history,"rules":rules,"labels":meta.labels,"original_metadata":meta})).await?;
            }
            let version_count = versions.keys().filter(|c| &c.group == group).count();
            artifacts_count += artifacts.len();
            group_summaries.push(GroupSummary {
                group_id: group.clone(),
                artifact_count: artifacts.len(),
                version_count,
            });
        }
        let all: BTreeSet<_> = versions.keys().cloned().collect();
        let unresolved: BTreeSet<_> = neutral
            .iter()
            .flat_map(|s| s.references.iter().map(|r| r.coordinate.clone()))
            .filter(|c| !all.contains(c))
            .collect();
        let mut dependency_order = Vec::new();
        for c in snapshot::order(&neutral, true)? {
            let a = ArtifactCoordinate {
                group_id: c.group,
                artifact_id: c.name,
            };
            if !dependency_order.contains(&a) {
                dependency_order.push(a);
            }
        }
        for (group_id, artifact_id) in selected_artifacts {
            let coordinate = ArtifactCoordinate {
                group_id,
                artifact_id,
            };
            if !dependency_order.contains(&coordinate) {
                dependency_order.push(coordinate);
            }
        }
        let has_export_zip = if opts.include_export {
            let zip = self.client.export_zip().await?;
            self.storage
                .put(&export_zip_key(&self.backup_id), zip)
                .await?;
            true
        } else {
            false
        };
        let mut keys = vec![global_rules_key(&self.backup_id)];
        for group in &group_names {
            keys.push(group_metadata_key(&self.backup_id, group));
        }
        for artifact in &dependency_order {
            keys.push(artifact_metadata_key(
                &self.backup_id,
                &artifact.group_id,
                &artifact.artifact_id,
            ));
        }
        for coordinate in versions.keys() {
            keys.push(version_key(
                &self.backup_id,
                &coordinate.group,
                &coordinate.name,
                &coordinate.version,
            ));
            keys.push(version_content_key(
                &self.backup_id,
                &coordinate.group,
                &coordinate.name,
                &coordinate.version,
            ));
        }
        if has_export_zip {
            keys.push(export_zip_key(&self.backup_id));
        }
        let snapshot = snapshot::seal(
            self.storage.as_ref(),
            keys,
            SnapshotInfo {
                provider: "apicurio".into(),
                api_version: self.config.api_version.as_str().into(),
                server_version: self.client.server_version().await?,
                full_registry: opts.groups == ["*"]
                    && opts.exclude_groups.is_empty()
                    && opts.artifacts == ["*"]
                    && opts.exclude_artifacts.is_empty()
                    && matches!(opts.include_versions, VersionSelection::All),
                archive_scope: has_export_zip.then(|| "whole_registry".into()),
                unresolved_dependencies: unresolved.into_iter().collect(),
                ..Default::default()
            },
        )
        .await?;
        let manifest = ApicurioRegistryManifest {
            registry_url: self.config.url.clone(),
            backup_timestamp: chrono::Utc::now().to_rfc3339(),
            api_version: self.config.api_version.as_str().into(),
            global_rules,
            groups_count: group_summaries.len(),
            groups: group_summaries,
            artifacts_count,
            total_versions: versions.len(),
            dependency_order,
            has_export_zip,
            snapshot: Some(snapshot),
        };
        self.write(&manifest_key(&self.backup_id), &manifest)
            .await?;
        snapshot::mark_status(self.storage.as_ref(), &prefix, true).await?;
        info!(
            versions = manifest.total_versions,
            "Apicurio Registry snapshot complete"
        );
        Ok(manifest)
    }
    async fn write<T: serde::Serialize>(&self, key: &str, value: &T) -> anyhow::Result<()> {
        self.storage
            .put(key, Bytes::from(serde_json::to_vec_pretty(value)?))
            .await?;
        Ok(())
    }
}

// ============================================================================
// Storage Key Helpers
// ============================================================================

fn ar_prefix(backup_id: &str) -> String {
    format!("{}/apicurio-registry", backup_id)
}

fn manifest_key(backup_id: &str) -> String {
    format!("{}/_manifest.json", ar_prefix(backup_id))
}

fn export_zip_key(backup_id: &str) -> String {
    format!("{}/_export.zip", ar_prefix(backup_id))
}

fn global_rules_key(backup_id: &str) -> String {
    format!("{}/_global_rules.json", ar_prefix(backup_id))
}

fn group_metadata_key(backup_id: &str, group_id: &str) -> String {
    format!(
        "{}/groups/{}/_metadata.json",
        ar_prefix(backup_id),
        urlencoding::encode(group_id)
    )
}

fn artifact_metadata_key(backup_id: &str, group_id: &str, artifact_id: &str) -> String {
    format!(
        "{}/groups/{}/artifacts/{}/_metadata.json",
        ar_prefix(backup_id),
        urlencoding::encode(group_id),
        urlencoding::encode(artifact_id)
    )
}

fn version_key(backup_id: &str, group_id: &str, artifact_id: &str, version: &str) -> String {
    format!(
        "{}/groups/{}/artifacts/{}/versions/{}.json",
        ar_prefix(backup_id),
        urlencoding::encode(group_id),
        urlencoding::encode(artifact_id),
        urlencoding::encode(version)
    )
}

fn version_content_key(
    backup_id: &str,
    group_id: &str,
    artifact_id: &str,
    version: &str,
) -> String {
    format!(
        "{}/groups/{}/artifacts/{}/versions/{}.content",
        ar_prefix(backup_id),
        urlencoding::encode(group_id),
        urlencoding::encode(artifact_id),
        urlencoding::encode(version)
    )
}

// ============================================================================
// Glob Filtering
// ============================================================================

/// Check if a name matches the include patterns and does not match any exclude pattern.
fn matches_filter(name: &str, include: &[glob::Pattern], exclude: &[glob::Pattern]) -> bool {
    let included = include.is_empty() || include.iter().any(|p| p.matches(name));
    let excluded = exclude.iter().any(|p| p.matches(name));
    included && !excluded
}

// ============================================================================
// Topological Sort (Kahn's Algorithm)
// ============================================================================

/// Resolve artifact reference dependencies into topological order.
///
/// Input: map of artifact coordinate → list of references (dependencies).
/// Output: artifacts sorted so that dependencies come before dependents.
pub fn topological_sort(
    artifact_refs: &HashMap<ArtifactCoordinate, Vec<ArtifactReference>>,
) -> Result<Vec<ArtifactCoordinate>, ApicurioRegistryError> {
    let mut graph: HashMap<ArtifactCoordinate, Vec<ArtifactCoordinate>> = HashMap::new();
    let mut in_degree: HashMap<ArtifactCoordinate, usize> = HashMap::new();

    // Ensure all known artifacts have an entry
    for coord in artifact_refs.keys() {
        in_degree.entry(coord.clone()).or_insert(0);
    }

    // Build adjacency list: dep → [dependents]
    for (coord, refs) in artifact_refs {
        for reference in refs {
            let dep = ArtifactCoordinate {
                group_id: reference.group_id.clone(),
                artifact_id: reference.artifact_id.clone(),
            };
            // Only count edges for artifacts we know about
            in_degree.entry(dep.clone()).or_insert(0);
            graph.entry(dep).or_default().push(coord.clone());
            *in_degree.entry(coord.clone()).or_insert(0) += 1;
        }
    }

    // Kahn's algorithm
    let mut queue: VecDeque<ArtifactCoordinate> = in_degree
        .iter()
        .filter(|(_, &deg)| deg == 0)
        .map(|(coord, _)| coord.clone())
        .collect();

    let mut ordered = Vec::new();

    while let Some(node) = queue.pop_front() {
        ordered.push(node.clone());
        if let Some(dependents) = graph.get(&node) {
            for dep in dependents {
                let deg = in_degree.get_mut(dep).unwrap();
                *deg -= 1;
                if *deg == 0 {
                    queue.push_back(dep.clone());
                }
            }
        }
    }

    if ordered.len() != in_degree.len() {
        return Err(ApicurioRegistryError::ReferenceCycle(
            "Circular reference detected in artifact dependencies".to_string(),
        ));
    }

    Ok(ordered)
}

// ============================================================================
// Simple Content Hash (no sha2 crate dependency)
// ============================================================================

/// Simple FNV-1a-like hash for content integrity verification.
/// Not cryptographic — just a fingerprint for change detection.
#[cfg(test)]
fn simple_hash(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_filter_wildcard() {
        let include = vec![glob::Pattern::new("*").unwrap()];
        let exclude: Vec<glob::Pattern> = vec![];
        assert!(matches_filter("payments", &include, &exclude));
        assert!(matches_filter("orders", &include, &exclude));
    }

    #[test]
    fn test_matches_filter_specific_pattern() {
        let include = vec![glob::Pattern::new("pay*").unwrap()];
        let exclude: Vec<glob::Pattern> = vec![];
        assert!(matches_filter("payments", &include, &exclude));
        assert!(!matches_filter("orders", &include, &exclude));
    }

    #[test]
    fn test_matches_filter_exclude() {
        let include = vec![glob::Pattern::new("*").unwrap()];
        let exclude = vec![glob::Pattern::new("test-*").unwrap()];
        assert!(matches_filter("payments", &include, &exclude));
        assert!(!matches_filter("test-group", &include, &exclude));
    }

    #[test]
    fn test_matches_filter_include_and_exclude() {
        let include = vec![
            glob::Pattern::new("pay*").unwrap(),
            glob::Pattern::new("order*").unwrap(),
        ];
        let exclude = vec![glob::Pattern::new("*-internal").unwrap()];
        assert!(matches_filter("payments", &include, &exclude));
        assert!(matches_filter("orders", &include, &exclude));
        assert!(!matches_filter("orders-internal", &include, &exclude));
        assert!(!matches_filter("shipping", &include, &exclude));
    }

    #[test]
    fn test_topological_sort_no_deps() {
        let refs: HashMap<ArtifactCoordinate, Vec<ArtifactReference>> = HashMap::from([
            (
                ArtifactCoordinate {
                    group_id: "g1".to_string(),
                    artifact_id: "a1".to_string(),
                },
                vec![],
            ),
            (
                ArtifactCoordinate {
                    group_id: "g2".to_string(),
                    artifact_id: "a2".to_string(),
                },
                vec![],
            ),
        ]);

        let order = topological_sort(&refs).unwrap();
        assert_eq!(order.len(), 2);
    }

    #[test]
    fn test_topological_sort_linear_chain() {
        // a1 → a2 → a3 (a3 depends on a2 which depends on a1)
        let refs = HashMap::from([
            (
                ArtifactCoordinate {
                    group_id: "g".to_string(),
                    artifact_id: "a1".to_string(),
                },
                vec![],
            ),
            (
                ArtifactCoordinate {
                    group_id: "g".to_string(),
                    artifact_id: "a2".to_string(),
                },
                vec![ArtifactReference {
                    group_id: "g".to_string(),
                    artifact_id: "a1".to_string(),
                    version: "1".to_string(),
                    name: "dep".to_string(),
                }],
            ),
            (
                ArtifactCoordinate {
                    group_id: "g".to_string(),
                    artifact_id: "a3".to_string(),
                },
                vec![ArtifactReference {
                    group_id: "g".to_string(),
                    artifact_id: "a2".to_string(),
                    version: "1".to_string(),
                    name: "dep".to_string(),
                }],
            ),
        ]);

        let order = topological_sort(&refs).unwrap();
        assert_eq!(order.len(), 3);

        // a1 must come before a2, a2 before a3
        let pos = |artifact_id: &str| {
            order
                .iter()
                .position(|c| c.artifact_id == artifact_id)
                .unwrap()
        };
        assert!(pos("a1") < pos("a2"));
        assert!(pos("a2") < pos("a3"));
    }

    #[test]
    fn test_topological_sort_diamond() {
        // a1 is referenced by both a2 and a3; a4 references both a2 and a3
        let refs = HashMap::from([
            (
                ArtifactCoordinate {
                    group_id: "g".to_string(),
                    artifact_id: "a1".to_string(),
                },
                vec![],
            ),
            (
                ArtifactCoordinate {
                    group_id: "g".to_string(),
                    artifact_id: "a2".to_string(),
                },
                vec![ArtifactReference {
                    group_id: "g".to_string(),
                    artifact_id: "a1".to_string(),
                    version: "1".to_string(),
                    name: "dep".to_string(),
                }],
            ),
            (
                ArtifactCoordinate {
                    group_id: "g".to_string(),
                    artifact_id: "a3".to_string(),
                },
                vec![ArtifactReference {
                    group_id: "g".to_string(),
                    artifact_id: "a1".to_string(),
                    version: "1".to_string(),
                    name: "dep".to_string(),
                }],
            ),
            (
                ArtifactCoordinate {
                    group_id: "g".to_string(),
                    artifact_id: "a4".to_string(),
                },
                vec![
                    ArtifactReference {
                        group_id: "g".to_string(),
                        artifact_id: "a2".to_string(),
                        version: "1".to_string(),
                        name: "dep1".to_string(),
                    },
                    ArtifactReference {
                        group_id: "g".to_string(),
                        artifact_id: "a3".to_string(),
                        version: "1".to_string(),
                        name: "dep2".to_string(),
                    },
                ],
            ),
        ]);

        let order = topological_sort(&refs).unwrap();
        assert_eq!(order.len(), 4);

        let pos = |id: &str| order.iter().position(|c| c.artifact_id == id).unwrap();
        assert!(pos("a1") < pos("a2"));
        assert!(pos("a1") < pos("a3"));
        assert!(pos("a2") < pos("a4"));
        assert!(pos("a3") < pos("a4"));
    }

    #[test]
    fn test_topological_sort_cycle_detected() {
        let refs = HashMap::from([
            (
                ArtifactCoordinate {
                    group_id: "g".to_string(),
                    artifact_id: "a1".to_string(),
                },
                vec![ArtifactReference {
                    group_id: "g".to_string(),
                    artifact_id: "a2".to_string(),
                    version: "1".to_string(),
                    name: "dep".to_string(),
                }],
            ),
            (
                ArtifactCoordinate {
                    group_id: "g".to_string(),
                    artifact_id: "a2".to_string(),
                },
                vec![ArtifactReference {
                    group_id: "g".to_string(),
                    artifact_id: "a1".to_string(),
                    version: "1".to_string(),
                    name: "dep".to_string(),
                }],
            ),
        ]);

        let result = topological_sort(&refs);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ApicurioRegistryError::ReferenceCycle(_)));
    }

    #[test]
    fn test_topological_sort_cross_group() {
        // Artifact in "orders" references artifact in "common"
        let refs = HashMap::from([
            (
                ArtifactCoordinate {
                    group_id: "common".to_string(),
                    artifact_id: "Address".to_string(),
                },
                vec![],
            ),
            (
                ArtifactCoordinate {
                    group_id: "orders".to_string(),
                    artifact_id: "OrderEvent".to_string(),
                },
                vec![ArtifactReference {
                    group_id: "common".to_string(),
                    artifact_id: "Address".to_string(),
                    version: "1".to_string(),
                    name: "common.Address".to_string(),
                }],
            ),
        ]);

        let order = topological_sort(&refs).unwrap();
        assert_eq!(order.len(), 2);

        let pos = |gid: &str, aid: &str| {
            order
                .iter()
                .position(|c| c.group_id == gid && c.artifact_id == aid)
                .unwrap()
        };
        assert!(pos("common", "Address") < pos("orders", "OrderEvent"));
    }

    #[test]
    fn test_storage_key_generation() {
        assert_eq!(
            manifest_key("backup-001"),
            "backup-001/apicurio-registry/_manifest.json"
        );
        assert_eq!(
            export_zip_key("backup-001"),
            "backup-001/apicurio-registry/_export.zip"
        );
        assert_eq!(
            global_rules_key("backup-001"),
            "backup-001/apicurio-registry/_global_rules.json"
        );
        assert_eq!(
            group_metadata_key("backup-001", "payments"),
            "backup-001/apicurio-registry/groups/payments/_metadata.json"
        );
        assert_eq!(
            artifact_metadata_key("backup-001", "payments", "PaymentEvent"),
            "backup-001/apicurio-registry/groups/payments/artifacts/PaymentEvent/_metadata.json"
        );
        assert_eq!(
            version_key("backup-001", "payments", "PaymentEvent", "2"),
            "backup-001/apicurio-registry/groups/payments/artifacts/PaymentEvent/versions/2.json"
        );
        assert_eq!(
            version_content_key("backup-001", "payments", "PaymentEvent", "2"),
            "backup-001/apicurio-registry/groups/payments/artifacts/PaymentEvent/versions/2.content"
        );
    }

    #[test]
    fn test_storage_key_url_encoding() {
        // Group with special chars
        assert_eq!(
            group_metadata_key("backup-001", "com.example/orders"),
            "backup-001/apicurio-registry/groups/com.example%2Forders/_metadata.json"
        );
    }

    #[test]
    fn test_simple_hash_deterministic() {
        let data = b"hello world";
        let h1 = simple_hash(data);
        let h2 = simple_hash(data);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_simple_hash_different_data() {
        let h1 = simple_hash(b"hello");
        let h2 = simple_hash(b"world");
        assert_ne!(h1, h2);
    }
}
