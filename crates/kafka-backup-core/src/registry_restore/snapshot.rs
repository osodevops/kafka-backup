//! Versioned snapshot integrity and provider-neutral schema coordinates.
use std::collections::{BTreeMap, BTreeSet};

use crate::storage::{get_if_exists, StorageBackend};
use anyhow::{bail, Context, Result};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::Provider;
pub use crate::schema_registry::snapshot::{hash, Coordinate, SnapshotInfo};

pub async fn mark_status(storage: &dyn StorageBackend, prefix: &str, complete: bool) -> Result<()> {
    storage
        .put(
            &format!("{prefix}/_status.json"),
            Bytes::from(serde_json::to_vec(
                &serde_json::json!({"complete":complete}),
            )?),
        )
        .await?;
    Ok(())
}

pub async fn seal(
    storage: &dyn StorageBackend,
    keys: Vec<String>,
    mut info: SnapshotInfo,
) -> Result<SnapshotInfo> {
    for key in keys {
        info.objects
            .insert(key.clone(), hash(&storage.get(&key).await?));
    }
    info.format_version = 2;
    info.complete = true;
    Ok(info)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reference {
    pub name: String,
    pub coordinate: Coordinate,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub coordinate: Coordinate,
    pub id: i64,
    pub content_id: Option<i64>,
    pub schema_type: String,
    pub content: String,
    pub references: Vec<Reference>,
    pub deleted: Option<bool>,
    pub state: Option<String>,
    pub envelope: BTreeMap<String, Value>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub group: String,
    pub name: String,
    pub config: Option<Value>,
    pub mode: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub provider: Provider,
    pub source_url: String,
    pub api_version: String,
    pub schemas: Vec<Schema>,
    pub settings: Vec<Settings>,
    pub global: Value,
    pub groups: BTreeMap<String, Value>,
    pub artifacts: BTreeMap<String, Value>,
    pub info: Option<SnapshotInfo>,
    pub digest: String,
    pub prefix: String,
}

/// Version-level DAG with same-artifact history edges. A subject-level cycle
/// may be satisfiable, e.g. A1 -> B1 -> A2.
pub fn order(schemas: &[Schema], allow_missing: bool) -> Result<Vec<Coordinate>> {
    order_with_history(schemas, allow_missing, true)
}

/// Explicit Confluent IMPORT versions need reference ordering but can be
/// registered out of numeric history order. Allocation still requires history.
pub fn order_with_history(
    schemas: &[Schema],
    allow_missing: bool,
    require_history: bool,
) -> Result<Vec<Coordinate>> {
    let mut dependencies: BTreeMap<Coordinate, BTreeSet<Coordinate>> = BTreeMap::new();
    let all: BTreeSet<_> = schemas.iter().map(|s| s.coordinate.clone()).collect();
    if all.len() != schemas.len() {
        bail!("Duplicate schema coordinates in snapshot");
    }
    let mut history: BTreeMap<_, Vec<&Schema>> = BTreeMap::new();
    for s in schemas {
        history.entry(s.coordinate.artifact()).or_default().push(s);
        let deps = dependencies.entry(s.coordinate.clone()).or_default();
        for reference in &s.references {
            if !all.contains(&reference.coordinate) {
                if !allow_missing {
                    bail!(
                        "Unresolved dependency {} required by {}",
                        reference.coordinate.path(),
                        s.coordinate.path()
                    );
                }
            } else {
                deps.insert(reference.coordinate.clone());
            }
        }
    }
    for versions in history.values_mut() {
        versions.sort_by_key(|s| {
            let sequence = if s.content_id.is_some() {
                // Apicurio version labels need not be increasing integers.
                s.envelope
                    .get("versionOrder")
                    .and_then(Value::as_i64)
                    .unwrap_or(s.id)
            } else {
                s.coordinate.version.parse::<i64>().unwrap_or(s.id)
            };
            (sequence, s.id)
        });
        for pair in versions.windows(2).filter(|_| require_history) {
            dependencies
                .get_mut(&pair[1].coordinate)
                .unwrap()
                .insert(pair[0].coordinate.clone());
        }
    }
    let mut result = Vec::new();
    while !dependencies.is_empty() {
        let ready: Vec<_> = dependencies
            .iter()
            .filter(|(_, deps)| deps.is_empty())
            .map(|(c, _)| c.clone())
            .collect();
        if ready.is_empty() {
            bail!(
                "Unsatisfiable schema version dependency cycle: {:?}",
                dependencies.keys().collect::<Vec<_>>()
            );
        }
        for c in &ready {
            dependencies.remove(c);
        }
        for deps in dependencies.values_mut() {
            for c in &ready {
                deps.remove(c);
            }
        }
        result.extend(ready);
    }
    Ok(result)
}

pub async fn load(
    storage: &dyn StorageBackend,
    backup_id: &str,
    provider: Provider,
) -> Result<Snapshot> {
    let component = match provider {
        Provider::Confluent => "schema-registry",
        Provider::Apicurio => "apicurio-registry",
    };
    let prefix = format!("{backup_id}/{component}");
    if let Some(status) = get_if_exists(storage, &format!("{prefix}/_status.json")).await? {
        if serde_json::from_slice::<Value>(&status)?["complete"] != true {
            bail!("Registry snapshot is incomplete: {prefix}");
        }
    }
    let manifest_bytes = storage.get(&format!("{prefix}/_manifest.json")).await?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes)?;
    let info: Option<SnapshotInfo> = manifest
        .get("snapshot")
        .filter(|v| !v.is_null())
        .map(|v| serde_json::from_value(v.clone()))
        .transpose()?;
    if let Some(info) = &info {
        if info.format_version != 2 || !info.complete {
            bail!("Unsupported or incomplete registry snapshot");
        }
        let expected_provider = if provider == Provider::Confluent {
            "confluent"
        } else {
            "apicurio"
        };
        if info.provider != expected_provider {
            bail!("Registry snapshot provider does not match manifest location");
        }
        for (key, expected) in &info.objects {
            if !key.starts_with(&format!("{prefix}/"))
                || key.split('/').any(|c| c == ".." || c == ".")
            {
                bail!("Snapshot object outside registry scope: {key}");
            }
            if hash(
                &storage
                    .get(key)
                    .await
                    .context(format!("Missing snapshot object {key}"))?,
            ) != *expected
            {
                bail!("Snapshot hash mismatch: {key}");
            }
        }
    }
    let mut snapshot = Snapshot {
        provider,
        source_url: manifest[if provider == Provider::Confluent {
            "schema_registry_url"
        } else {
            "registry_url"
        }]
        .as_str()
        .unwrap_or("")
        .into(),
        api_version: manifest["api_version"]
            .as_str()
            .unwrap_or("confluent-v1")
            .into(),
        schemas: vec![],
        settings: vec![],
        global: Value::Null,
        groups: BTreeMap::new(),
        artifacts: BTreeMap::new(),
        digest: String::new(),
        info,
        prefix: prefix.clone(),
    };
    let mut digest_bytes = manifest_bytes.to_vec();
    // Also hash legacy object bytes, so resume cannot silently change its input.
    async fn read(
        storage: &dyn StorageBackend,
        key: &str,
        digest: &mut Vec<u8>,
        info: Option<&SnapshotInfo>,
    ) -> Result<Value> {
        if info.is_some_and(|i| !i.objects.contains_key(key)) {
            bail!("Selected registry object has no recorded hash: {key}");
        }
        let bytes = storage
            .get(key)
            .await
            .context(format!("Missing registry object {key}"))?;
        digest.extend_from_slice(key.as_bytes());
        digest.extend_from_slice(&bytes);
        Ok(serde_json::from_slice(&bytes)?)
    }
    match provider {
        Provider::Confluent => {
            snapshot.global = read(
                storage,
                &format!("{prefix}/_global_config.json"),
                &mut digest_bytes,
                snapshot.info.as_ref(),
            )
            .await?;
            for subject in manifest["subjects"]
                .as_array()
                .context("Missing subjects")?
            {
                let name = subject["name"].as_str().context("Missing subject name")?;
                let base = format!("{prefix}/subjects/{}", urlencoding::encode(name));
                let metadata = read(
                    storage,
                    &format!("{base}/_metadata.json"),
                    &mut digest_bytes,
                    snapshot.info.as_ref(),
                )
                .await?;
                snapshot.settings.push(Settings {
                    group: String::new(),
                    name: name.into(),
                    config: metadata
                        .get("config")
                        .filter(|v| !v.is_null())
                        .cloned()
                        .or_else(|| {
                            metadata["compatibility"]
                                .as_str()
                                .map(|s| serde_json::json!({"compatibilityLevel":s}))
                        }),
                    mode: metadata["mode"].as_str().map(str::to_owned),
                });
                for version in metadata["versions"]
                    .as_array()
                    .context("Missing subject versions")?
                {
                    let number = i32::try_from(version.as_i64().context("Invalid version")?)?;
                    if number <= 0 {
                        bail!("Invalid Confluent version {number}");
                    }
                    let schema: crate::schema_registry::models::SchemaVersion =
                        serde_json::from_value(
                            read(
                                storage,
                                &format!("{base}/v{number}.json"),
                                &mut digest_bytes,
                                snapshot.info.as_ref(),
                            )
                            .await?,
                        )?;
                    if schema.subject != name || schema.version != number || schema.id <= 0 {
                        bail!("Invalid schema coordinates or ID for {name}/{number}");
                    }
                    let envelope = schema.envelope();
                    snapshot.schemas.push(Schema {
                        coordinate: Coordinate::subject(name, number),
                        id: schema.id.into(),
                        content_id: None,
                        schema_type: schema.schema_type,
                        content: schema.schema,
                        references: schema
                            .references
                            .into_iter()
                            .map(|r| Reference {
                                name: r.name,
                                coordinate: Coordinate::subject(&r.subject, r.version),
                            })
                            .collect(),
                        deleted: snapshot.info.as_ref().map(|_| {
                            metadata["deleted_versions"]
                                .as_array()
                                .is_some_and(|v| v.contains(&serde_json::json!(number)))
                        }),
                        state: None,
                        envelope,
                    });
                }
            }
        }
        Provider::Apicurio => {
            let global_key = format!("{prefix}/_global_rules.json");
            if snapshot.info.is_none() {
                snapshot.global = get_if_exists(storage, &global_key)
                    .await?
                    .map(|b| serde_json::from_slice(&b))
                    .transpose()?
                    .unwrap_or(Value::Null);
            } else {
                snapshot.global = read(
                    storage,
                    &global_key,
                    &mut digest_bytes,
                    snapshot.info.as_ref(),
                )
                .await?;
                if snapshot.global != manifest["global_rules"] {
                    bail!("Global rules differ between manifest and hashed object");
                }
            }
            // Old manifests omitted artifacts without references from their
            // dependency index. Discover their actual metadata objects for
            // inspection; no omitted settings or state is synthesized.
            let mut artifact_coordinates = manifest["dependency_order"]
                .as_array()
                .context("Missing artifact coordinates")?
                .clone();
            if snapshot.info.is_none() {
                artifact_coordinates.clear();
                for listed in storage.list(&format!("{prefix}/groups/")).await? {
                    let key = if storage.backend_name() == "filesystem" {
                        listed
                    } else {
                        urlencoding::decode(&listed)?.into_owned()
                    };
                    if !key.starts_with(&format!("{prefix}/groups/"))
                        || !key.ends_with("/_metadata.json")
                        || !key.contains("/artifacts/")
                        || key.split('/').any(|s| matches!(s, "." | ".."))
                    {
                        continue;
                    }
                    let metadata = read(storage, &key, &mut digest_bytes, None).await?;
                    artifact_coordinates.push(serde_json::json!({"group_id":metadata["group_id"],"artifact_id":metadata["artifact_id"]}));
                }
                artifact_coordinates.sort_by_key(Value::to_string);
                artifact_coordinates.dedup();
            }
            for group in manifest["groups"]
                .as_array()
                .context("Missing group coordinates")?
            {
                let name = group["group_id"].as_str().context("Missing group ID")?;
                let metadata = read(
                    storage,
                    &format!(
                        "{prefix}/groups/{}/_metadata.json",
                        urlencoding::encode(name)
                    ),
                    &mut digest_bytes,
                    snapshot.info.as_ref(),
                )
                .await?;
                snapshot.groups.insert(name.into(), metadata);
            }
            for artifact in &artifact_coordinates {
                let group = artifact["group_id"].as_str().context("Missing group")?;
                let name = artifact["artifact_id"]
                    .as_str()
                    .context("Missing artifact")?;
                let base = format!(
                    "{prefix}/groups/{}/artifacts/{}",
                    urlencoding::encode(group),
                    urlencoding::encode(name)
                );
                let metadata = read(
                    storage,
                    &format!("{base}/_metadata.json"),
                    &mut digest_bytes,
                    snapshot.info.as_ref(),
                )
                .await?;
                snapshot
                    .artifacts
                    .insert(format!("{group}/{name}"), metadata.clone());
                snapshot.settings.push(Settings {
                    group: group.into(),
                    name: name.into(),
                    config: Some(metadata["rules"].clone()),
                    mode: None,
                });
                for version in metadata["versions"]
                    .as_array()
                    .context("Missing artifact versions")?
                {
                    let version = version.as_str().context("Invalid artifact version")?;
                    let v: crate::apicurio_registry::models::BackedUpVersion =
                        serde_json::from_value(
                            read(
                                storage,
                                &format!("{base}/versions/{}.json", urlencoding::encode(version)),
                                &mut digest_bytes,
                                snapshot.info.as_ref(),
                            )
                            .await?,
                        )?;
                    let content_key =
                        format!("{base}/versions/{}.content", urlencoding::encode(version));
                    if snapshot
                        .info
                        .as_ref()
                        .is_some_and(|i| !i.objects.contains_key(&content_key))
                    {
                        bail!("Selected artifact content has no recorded hash");
                    }
                    let content = storage
                        .get(&format!(
                            "{base}/versions/{}.content",
                            urlencoding::encode(version)
                        ))
                        .await?;
                    digest_bytes.extend_from_slice(&content);
                    if snapshot.info.is_some() && hash(&content) != v.content_hash {
                        bail!("Artifact content hash mismatch");
                    }
                    if v.version != version || v.global_id <= 0 || v.content_id <= 0 {
                        bail!("Invalid artifact coordinates or IDs");
                    }
                    snapshot.schemas.push(Schema {
                        coordinate: Coordinate {
                            group: group.into(),
                            name: name.into(),
                            version: version.into(),
                        },
                        id: v.global_id,
                        content_id: Some(v.content_id),
                        schema_type: v.artifact_type,
                        content: String::from_utf8(content.to_vec())?,
                        references: v
                            .references
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
                        envelope: {
                            let mut envelope = v.extra;
                            envelope.insert("name".into(), serde_json::json!(v.name));
                            envelope.insert("description".into(), serde_json::json!(v.description));
                            envelope.insert("labels".into(), serde_json::json!(v.labels));
                            envelope
                        },
                    });
                }
            }
        }
    }
    if snapshot.schemas.len()
        != manifest["total_versions"]
            .as_u64()
            .context("Missing version count")? as usize
    {
        bail!("Registry snapshot version count does not match manifest");
    }
    snapshot.digest = hash(&digest_bytes);
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn schema(name: &str, version: i32, refs: &[(&str, i32)]) -> Schema {
        Schema {
            coordinate: Coordinate::subject(name, version),
            id: i64::from(version),
            content_id: None,
            schema_type: "AVRO".into(),
            content: "\"string\"".into(),
            references: refs
                .iter()
                .map(|(s, v)| Reference {
                    name: s.to_string(),
                    coordinate: Coordinate::subject(s, *v),
                })
                .collect(),
            deleted: Some(false),
            state: None,
            envelope: BTreeMap::new(),
        }
    }
    #[test]
    fn accepts_satisfiable_subject_cycle_at_version_level() {
        let a1 = schema("a", 1, &[]);
        let b1 = schema("b", 1, &[("a", 1)]);
        let a2 = schema("a", 2, &[("b", 1)]);
        assert_eq!(
            order(&[a2.clone(), a1.clone(), b1.clone()], false).unwrap(),
            vec![a1.coordinate, b1.coordinate, a2.coordinate]
        );
    }
    #[test]
    fn rejects_unsatisfiable_version_cycle() {
        assert!(order(
            &[schema("a", 1, &[("b", 1)]), schema("b", 1, &[("a", 1)])],
            false
        )
        .is_err());
    }
    #[test]
    fn explicit_import_versions_can_precede_earlier_history() {
        let a2 = schema("a", 2, &[]);
        let b1 = schema("b", 1, &[("a", 2)]);
        let a1 = schema("a", 1, &[("b", 1)]);
        let schemas = [a1.clone(), a2.clone(), b1.clone()];
        assert!(order(&schemas, false).is_err());
        assert_eq!(
            order_with_history(&schemas, false, false).unwrap(),
            vec![a2.coordinate, b1.coordinate, a1.coordinate]
        );
    }
    #[test]
    fn apicurio_version_labels_do_not_reverse_registration_history() {
        let mut first = schema("a", 10, &[]);
        first.id = 1;
        first.content_id = Some(1);
        let mut second = schema("a", 2, &[("a", 10)]);
        second.id = 2;
        second.content_id = Some(2);
        assert_eq!(
            order(&[second.clone(), first.clone()], false).unwrap(),
            vec![first.coordinate, second.coordinate]
        );
    }
    #[test]
    fn does_not_invent_missing_reference_versions() {
        assert!(order(&[schema("a", 3, &[("b", 2)])], false).is_err());
        assert!(order(&[schema("a", 3, &[("b", 2)])], true).is_ok());
    }
    #[test]
    fn sha256_uses_standard_known_digest() {
        assert_eq!(
            hash(b"abc"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
    #[tokio::test]
    async fn rejects_corrupted_objects_and_missing_integrity_entries() {
        use crate::storage::MemoryBackend;
        let storage = MemoryBackend::new();
        let objects = [
            (
                "backup/schema-registry/_global_config.json",
                serde_json::json!({"config":{"compatibilityLevel":"BACKWARD"}}),
            ),
            (
                "backup/schema-registry/subjects/event/_metadata.json",
                serde_json::json!({"versions":[1],"deleted_versions":[]}),
            ),
            (
                "backup/schema-registry/subjects/event/v1.json",
                serde_json::json!({"subject":"event","version":1,"id":1,"schema":"\"string\""}),
            ),
        ];
        for (key, value) in &objects {
            storage
                .put(key, Bytes::from(serde_json::to_vec(value).unwrap()))
                .await
                .unwrap();
        }
        let info = seal(
            &storage,
            objects.iter().map(|(k, _)| k.to_string()).collect(),
            SnapshotInfo {
                provider: "confluent".into(),
                api_version: "confluent-v1".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut manifest = serde_json::json!({"schema_registry_url":"http://source","subjects":[{"name":"event"}],"total_versions":1,"snapshot":info});
        let key = "backup/schema-registry/_manifest.json";
        storage
            .put(key, Bytes::from(serde_json::to_vec(&manifest).unwrap()))
            .await
            .unwrap();
        load(&storage, "backup", Provider::Confluent).await.unwrap();
        storage
            .put(objects[2].0, Bytes::from_static(b"corrupted"))
            .await
            .unwrap();
        assert!(load(&storage, "backup", Provider::Confluent)
            .await
            .unwrap_err()
            .to_string()
            .contains("hash mismatch"));
        storage
            .put(
                objects[2].0,
                Bytes::from(serde_json::to_vec(&objects[2].1).unwrap()),
            )
            .await
            .unwrap();
        manifest["snapshot"]["objects"]
            .as_object_mut()
            .unwrap()
            .remove(objects[2].0);
        storage
            .put(key, Bytes::from(serde_json::to_vec(&manifest).unwrap()))
            .await
            .unwrap();
        assert!(load(&storage, "backup", Provider::Confluent)
            .await
            .unwrap_err()
            .to_string()
            .contains("no recorded hash"));
    }
}
