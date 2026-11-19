//! Snapshot integrity: the completeness marker and the object checksums.
//!
//! A snapshot lives under `{backup_id}/schema-registry/`:
//!
//! - `_status.json`: `{"complete": false}` from the moment a pass starts
//!   until every object and the manifest are durable. A pass that fails or
//!   is interrupted leaves it `false` (with the error), so a snapshot that is
//!   half old and half new is never restored.
//! - `_manifest.json`: the summary, with a `snapshot` block holding the
//!   SHA-256 of every other object ([`SnapshotInfo::objects`]).
//!
//! [`verify`] is run before a restore touches the target.

use std::collections::BTreeMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::warn;

use crate::storage::{get_if_exists, StorageBackend};

use super::error::SchemaRegistryError;

/// The snapshot layout version written by this release.
pub const FORMAT_VERSION: u32 = 2;

/// The directory of a backup's snapshot.
pub fn prefix(backup_id: &str) -> String {
    format!("{backup_id}/schema-registry")
}

pub fn status_key(backup_id: &str) -> String {
    format!("{}/_status.json", prefix(backup_id))
}

/// `sha256:<hex>` of `data`.
pub fn hash(data: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(data))
}

/// A referenced (subject, version) that the snapshot does not contain.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Coordinate {
    /// Context-qualified subject.
    pub name: String,
    /// Always empty for Confluent Schema Registry.
    #[serde(default)]
    pub group: String,
    pub version: String,
}

impl Coordinate {
    pub fn subject(name: &str, version: i32) -> Self {
        Self {
            name: name.into(),
            group: String::new(),
            version: version.to_string(),
        }
    }
}

/// The `snapshot` block of the manifest.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SnapshotInfo {
    #[serde(default)]
    pub format_version: u32,
    #[serde(default)]
    pub provider: String,
    #[serde(default)]
    pub api_version: String,
    /// The registry's version, when it reports one.
    #[serde(default)]
    pub server_version: Option<String>,
    #[serde(default)]
    pub complete: bool,
    /// Every subject and version, soft-deleted included, was in scope.
    #[serde(default)]
    pub full_registry: bool,
    /// Storage key -> `sha256:<hex>` for every object of the snapshot except
    /// the manifest itself.
    #[serde(default)]
    pub objects: BTreeMap<String, String>,
    /// References to versions outside the snapshot (`include_references:
    /// false`); they must already exist in a restore target.
    #[serde(default)]
    pub unresolved_dependencies: Vec<Coordinate>,
    #[serde(default)]
    pub limitations: Vec<String>,
}

/// The `_status.json` marker.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotStatus {
    pub complete: bool,
    /// Why the last pass did not complete.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// When the marker was written (RFC 3339).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// Write the completeness marker.
pub async fn mark_status(
    storage: &dyn StorageBackend,
    backup_id: &str,
    complete: bool,
    error: Option<&str>,
) -> Result<(), SchemaRegistryError> {
    let status = SnapshotStatus {
        complete,
        error: error.map(str::to_string),
        updated_at: Some(chrono::Utc::now().to_rfc3339()),
    };
    let body = serde_json::to_vec(&status)
        .map_err(|e| SchemaRegistryError::Serialization(e.to_string()))?;
    storage
        .put(&status_key(backup_id), Bytes::from(body))
        .await
        .map_err(|e| SchemaRegistryError::Storage(format!("write snapshot status: {e}")))
}

/// Read the completeness marker, if the snapshot has one.
pub async fn read_status(
    storage: &dyn StorageBackend,
    backup_id: &str,
) -> Result<Option<SnapshotStatus>, SchemaRegistryError> {
    let key = status_key(backup_id);
    let Some(bytes) = get_if_exists(storage, &key)
        .await
        .map_err(|e| SchemaRegistryError::Storage(format!("read {key}: {e}")))?
    else {
        return Ok(None);
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| SchemaRegistryError::Snapshot(format!("{key} is not valid: {e}")))
}

/// Hash every object in `keys` as it is in storage.
pub async fn seal(
    storage: &dyn StorageBackend,
    keys: Vec<String>,
    mut info: SnapshotInfo,
) -> Result<SnapshotInfo, SchemaRegistryError> {
    for key in keys {
        let bytes = storage
            .get(&key)
            .await
            .map_err(|e| SchemaRegistryError::Storage(format!("read {key}: {e}")))?;
        info.objects.insert(key, hash(&bytes));
    }
    info.format_version = FORMAT_VERSION;
    info.complete = true;
    Ok(info)
}

/// Check a snapshot before it is restored: the marker says complete, the
/// format is known, and every recorded object exists with its recorded
/// checksum. `info` is the manifest's `snapshot` block.
pub async fn verify(
    storage: &dyn StorageBackend,
    backup_id: &str,
    info: Option<&SnapshotInfo>,
) -> Result<(), SchemaRegistryError> {
    let prefix = prefix(backup_id);
    if let Some(status) = read_status(storage, backup_id).await? {
        if !status.complete {
            return Err(SchemaRegistryError::Snapshot(format!(
                "the snapshot in {prefix} is incomplete{}; take a new Schema Registry backup \
                 (`backup --schema-only`) before restoring",
                status
                    .error
                    .map(|e| format!(" (last pass failed: {e})"))
                    .unwrap_or_default()
            )));
        }
    }
    // Kafka Backup Enterprise before 0.6.1 wrote no checksums. Such a
    // snapshot is still restorable; its integrity cannot be checked.
    let Some(info) = info else {
        warn!(
            "The Schema Registry snapshot in {prefix} has no checksums (written by an early \
             Kafka Backup Enterprise release); its integrity cannot be verified"
        );
        return Ok(());
    };
    if info.format_version != FORMAT_VERSION || !info.complete {
        return Err(SchemaRegistryError::Snapshot(format!(
            "unsupported or incomplete snapshot in {prefix} (format {}, complete: {})",
            info.format_version, info.complete
        )));
    }
    if info.provider != "confluent" {
        return Err(SchemaRegistryError::Snapshot(format!(
            "the snapshot in {prefix} is from provider `{}`, not Confluent Schema Registry",
            info.provider
        )));
    }
    for (key, expected) in &info.objects {
        if !key.starts_with(&format!("{prefix}/")) || key.split('/').any(|c| c == ".." || c == ".")
        {
            return Err(SchemaRegistryError::Snapshot(format!(
                "object outside the snapshot: {key}"
            )));
        }
        let bytes = storage
            .get(key)
            .await
            .map_err(|e| SchemaRegistryError::Snapshot(format!("missing object {key}: {e}")))?;
        if hash(&bytes) != *expected {
            return Err(SchemaRegistryError::Snapshot(format!(
                "checksum mismatch: {key}"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryBackend;

    #[test]
    fn sha256_uses_the_standard_digest() {
        assert_eq!(
            hash(b"abc"),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    async fn sealed(storage: &MemoryBackend) -> SnapshotInfo {
        for (key, body) in [
            ("b/schema-registry/_global_config.json", "{}"),
            ("b/schema-registry/subjects/event/v1.json", "{\"id\":1}"),
        ] {
            storage.put(key, Bytes::from(body)).await.unwrap();
        }
        seal(
            storage,
            vec![
                "b/schema-registry/_global_config.json".into(),
                "b/schema-registry/subjects/event/v1.json".into(),
            ],
            SnapshotInfo {
                provider: "confluent".into(),
                api_version: "confluent-v1".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_sealed_snapshot_verifies() {
        let storage = MemoryBackend::new();
        let info = sealed(&storage).await;
        assert_eq!(info.format_version, FORMAT_VERSION);
        assert!(info.complete);
        assert_eq!(info.objects.len(), 2);
        mark_status(&storage, "b", true, None).await.unwrap();
        verify(&storage, "b", Some(&info)).await.unwrap();
    }

    #[tokio::test]
    async fn corrupt_missing_incomplete_and_foreign_snapshots_are_refused() {
        let storage = MemoryBackend::new();
        let info = sealed(&storage).await;

        let key = "b/schema-registry/subjects/event/v1.json";
        storage.put(key, Bytes::from("tampered")).await.unwrap();
        let err = verify(&storage, "b", Some(&info)).await.unwrap_err();
        assert!(err.to_string().contains("checksum mismatch"), "{err}");

        storage.delete(key).await.unwrap();
        let err = verify(&storage, "b", Some(&info)).await.unwrap_err();
        assert!(err.to_string().contains("missing object"), "{err}");
        storage.put(key, Bytes::from("{\"id\":1}")).await.unwrap();
        verify(&storage, "b", Some(&info)).await.unwrap();

        mark_status(&storage, "b", false, Some("503 from /subjects"))
            .await
            .unwrap();
        let err = verify(&storage, "b", Some(&info)).await.unwrap_err();
        assert!(err.to_string().contains("incomplete"), "{err}");
        assert!(err.to_string().contains("503 from /subjects"), "{err}");
        mark_status(&storage, "b", true, None).await.unwrap();

        // A manifest without checksums (early Enterprise releases) is
        // accepted; the completeness marker still applies.
        verify(&storage, "b", None).await.unwrap();

        let mut other = info.clone();
        other.provider = "apicurio".into();
        assert!(verify(&storage, "b", Some(&other)).await.is_err());

        let mut escaped = info.clone();
        escaped
            .objects
            .insert("other-backup/schema-registry/x".into(), hash(b"x"));
        let err = verify(&storage, "b", Some(&escaped)).await.unwrap_err();
        assert!(err.to_string().contains("outside the snapshot"), "{err}");
    }
}
