use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

// ============================================================================
// Confluent Schema Registry API Response Types
// ============================================================================

/// Response from GET /subjects/{subject}/versions/{version}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaVersion {
    pub subject: String,
    pub version: i32,
    pub id: i32,
    #[serde(rename = "schemaType", default = "default_schema_type")]
    pub schema_type: String,
    pub schema: String,
    #[serde(default)]
    pub references: Vec<SchemaReference>,
    /// Confluent schema metadata, preserved verbatim for restore.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    /// Confluent Schema Registry rule set, preserved verbatim for restore.
    #[serde(rename = "ruleSet", default, skip_serializing_if = "Option::is_none")]
    pub rule_set: Option<serde_json::Value>,
    /// Preserve the complete envelope, including metadata, ruleSet and GUID.
    #[serde(flatten, default)]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

impl SchemaVersion {
    /// Complete provider metadata, including fields with typed accessors.
    pub fn envelope(&self) -> BTreeMap<String, serde_json::Value> {
        let mut envelope = self.extra.clone();
        if let Some(metadata) = &self.metadata {
            envelope.insert("metadata".into(), metadata.clone());
        }
        if let Some(rules) = &self.rule_set {
            envelope.insert("ruleSet".into(), rules.clone());
        }
        envelope
    }
}

/// Schema reference (dependency on another schema)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaReference {
    pub name: String,
    pub subject: String,
    pub version: i32,
}

/// Response from GET /config or GET /config/{subject}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityConfig {
    #[serde(rename = "compatibilityLevel", alias = "compatibility", default)]
    pub compatibility_level: Option<String>,
    #[serde(default)]
    pub normalize: Option<bool>,
    #[serde(flatten, default)]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Response from GET /mode or GET /mode/{subject}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeConfig {
    pub mode: String,
}

/// Request body for POST /subjects/{subject}/versions (used during restore)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSchemaRequest {
    pub schema: String,
    #[serde(rename = "schemaType", skip_serializing_if = "Option::is_none")]
    pub schema_type: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub references: Vec<SchemaReference>,
    /// Schema ID (used in IMPORT mode to force original ID)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<i32>,
    /// Version number (used in IMPORT mode)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    #[serde(rename = "ruleSet", skip_serializing_if = "Option::is_none")]
    pub rule_set: Option<serde_json::Value>,
}

/// Response from POST /subjects/{subject}/versions. Schema Registry 7.x
/// returns only the id; 8.x also returns the version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterSchemaResponse {
    pub id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<i32>,
}

/// Response from GET /schemas/ids/{id}. Older registries omit
/// `schemaType` for Avro and `references` when there are none.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaById {
    pub schema: String,
    #[serde(rename = "schemaType", default = "default_schema_type")]
    pub schema_type: String,
    #[serde(default)]
    pub references: Vec<SchemaReference>,
}

/// Request for PUT /config or PUT /config/{subject}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigUpdateRequest {
    pub compatibility: String,
}

/// Request for PUT /mode or PUT /mode/{subject}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModeUpdateRequest {
    pub mode: String,
}

/// Confluent SR API error response body
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SrApiError {
    pub error_code: i32,
    pub message: String,
}

// ============================================================================
// Storage Format Types (what we write to the backup storage backend)
// ============================================================================

/// Top-level schema registry backup manifest (`_manifest.json`)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRegistryManifest {
    /// Source Schema Registry URL
    pub schema_registry_url: String,
    /// Backup timestamp (ISO 8601)
    pub backup_timestamp: String,
    /// Global compatibility level
    pub global_compatibility: String,
    /// Global mode
    pub global_mode: Option<String>,
    /// Summary of each backed-up subject
    pub subjects: Vec<SubjectSummary>,
    /// Aggregate counts
    pub subjects_count: usize,
    pub total_versions: usize,
    /// Topologically sorted dependency order (for restore)
    pub dependency_order: Vec<String>,
    /// Subjects selected after include/exclude filtering.
    #[serde(default)]
    pub subjects_discovered: usize,
    /// Subjects that could not be captured completely.
    #[serde(default)]
    pub subjects_failed: usize,
    /// Per-subject failure details. Empty means the component is complete.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<String>,
    /// Versions captured that were soft-deleted in the source (they are part
    /// of `total_versions`).
    #[serde(default)]
    pub soft_deleted_versions: usize,
    /// Subjects outside the include/exclude filter that were captured only
    /// because a captured schema references them (`include_references`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub referenced_subjects: Vec<String>,
    #[serde(default)]
    pub snapshot: Option<super::snapshot::SnapshotInfo>,
}

/// Per-subject summary in the manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubjectSummary {
    pub name: String,
    pub schema_type: String,
    pub version_count: usize,
    pub latest_version: i32,
    pub latest_schema_id: i32,
    pub compatibility: Option<String>,
    pub mode: Option<String>,
    pub has_references: bool,
    /// How many of `version_count` were soft-deleted in the source.
    #[serde(default)]
    pub soft_deleted_versions: usize,
}

/// Per-subject metadata file (`_metadata.json`)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubjectMetadata {
    pub subject: String,
    pub schema_type: String,
    /// Every captured version, live and soft-deleted, ascending.
    pub versions: Vec<i32>,
    pub compatibility: Option<String>,
    pub mode: Option<String>,
    /// The whole subject was soft-deleted in the source (every version is).
    pub is_soft_deleted: bool,
    /// Captured versions that were soft-deleted in the source. Backups taken
    /// before 0.7.0 have none recorded (they never captured soft-deleted
    /// versions).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub soft_deleted_versions: Vec<i32>,
    /// The subject is outside the include/exclude filter and was captured
    /// only for the versions that captured schemas reference.
    #[serde(default, skip_serializing_if = "is_false")]
    pub included_as_reference: bool,
    #[serde(default)]
    pub deleted_versions: Vec<i32>,
    /// None means inherited; old backups did not capture all configuration.
    #[serde(default)]
    pub config: Option<CompatibilityConfig>,
}

impl SubjectMetadata {
    /// Whether `version` was soft-deleted in the source.
    pub fn is_version_soft_deleted(&self, version: i32) -> bool {
        self.is_soft_deleted
            || self.soft_deleted_versions.contains(&version)
            || self.deleted_versions.contains(&version)
    }
}

/// Global config file (`_global_config.json`)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub compatibility_level: String,
    pub mode: Option<String>,
    /// Context-level compatibility and mode overrides, keyed by context name
    /// (`.ctx-a`), for the non-default contexts of the captured subjects.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub contexts: BTreeMap<String, ContextSettings>,
}

/// Compatibility and mode set on a Schema Registry context (`/config/:.ctx:`,
/// `/mode/:.ctx:`). `None` means no context-level override.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSettings {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compatibility: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
}

// ============================================================================
// Restore report (`{backup_id}/restore/schema-registry-report.json`)
// ============================================================================

/// What the restore does (or, in a dry run, would do) with one backed-up
/// schema version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaRestoreAction {
    /// Registered with its original ID and version number (IMPORT mode).
    Register,
    /// The target already holds this version with the same ID and an
    /// identical schema; nothing to do.
    Skip,
    /// `rewrite_ids`: the original ID or version is taken by a different
    /// schema, so the schema is registered normally and gets a new ID.
    Rewrite,
    /// `rewrite_ids`: the target already holds this version with an identical
    /// schema under another ID; the ID is mapped, nothing is registered.
    MapExisting,
    /// Strict mode: the original ID or version is taken by a different schema.
    Conflict,
}

/// One backed-up schema version in the restore report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRestoreItem {
    /// Subject in the backup.
    pub subject: String,
    /// Subject in the target (after `subject_mapping`).
    pub target_subject: String,
    pub version: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_version: Option<i32>,
    pub source_id: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_id: Option<i32>,
    pub action: SchemaRestoreAction,
    /// The version was soft-deleted in the source and is soft-deleted again
    /// after registration.
    #[serde(default, skip_serializing_if = "is_false")]
    pub soft_deleted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Capacity pre-check against `max_schema_versions`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaCapacityCheck {
    pub limit: u64,
    /// Versions in the target before the restore, soft-deleted included.
    pub used: u64,
    /// Versions the restore adds.
    pub required: u64,
}

/// Post-restore verification of every restored version.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SchemaVerification {
    pub versions_checked: usize,
    #[serde(default)]
    pub mismatches: Vec<String>,
    /// Differences that do not fail the restore, such as a target that
    /// dropped a version's `ruleSet` (Schema Registry without the rule-set
    /// extension) or `metadata`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRestoreReport {
    pub backup_id: String,
    pub target_url: String,
    /// `preserve_ids` or `into_existing`.
    pub strategy: String,
    pub dry_run: bool,
    pub rewrite_ids: bool,
    pub started_at: String,
    #[serde(default)]
    pub finished_at: String,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub subjects_planned: usize,
    pub versions_planned: usize,
    pub versions_registered: usize,
    pub versions_skipped: usize,
    pub versions_rewritten: usize,
    pub versions_soft_deleted: usize,
    #[serde(default)]
    pub conflicts: Vec<String>,
    /// Source schema ID to target schema ID, for every ID that changed. This
    /// is what the record restore uses to rewrite Confluent wire-format
    /// payloads (`restore.schema_id_mapping`).
    #[serde(default)]
    pub id_mapping: BTreeMap<i32, i32>,
    /// Restored IDs the target holds unchanged (registered with their
    /// original ID, or already present under it).
    #[serde(default, skip_serializing_if = "std::collections::BTreeSet::is_empty")]
    pub ids_preserved: std::collections::BTreeSet<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<SchemaCapacityCheck>,
    /// The target's prior modes were put back after a failure or a
    /// cancellation.
    #[serde(default, skip_serializing_if = "is_false")]
    pub modes_rolled_back: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mode_restore_errors: Vec<String>,
    /// Modes someone else changed while the restore ran. They were left as
    /// they are instead of being put back.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub modes_changed_by_others: Vec<String>,
    /// The restore was cancelled (SIGINT / SIGTERM).
    #[serde(default, skip_serializing_if = "is_false")]
    pub cancelled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verification: Option<SchemaVerification>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub items: Vec<SchemaRestoreItem>,
}

impl SchemaRestoreReport {
    /// The source-to-target ID map for the record restore, or `None` when
    /// every ID was preserved. It covers every restored ID, the unchanged
    /// ones mapping to themselves: kafka-backup-core's rewrite is strict by
    /// default (`schema_id_rewrite_strict`), so an ID missing from the map
    /// fails the restore.
    pub fn rewrite_mapping(&self) -> Option<std::collections::HashMap<i32, i32>> {
        if self.id_mapping.is_empty() {
            return None;
        }
        let mut map: std::collections::HashMap<i32, i32> =
            self.ids_preserved.iter().map(|id| (*id, *id)).collect();
        // A changed ID wins (the same schema can be preserved under one
        // subject and found under another ID under another; both resolve).
        map.extend(self.id_mapping.iter().map(|(s, t)| (*s, *t)));
        Some(map)
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn default_schema_type() -> String {
    "AVRO".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_version_roundtrip() {
        let sv = SchemaVersion {
            subject: "orders-value".to_string(),
            version: 1,
            id: 42,
            schema_type: "AVRO".to_string(),
            schema: r#"{"type":"record","name":"Order","fields":[]}"#.to_string(),
            extra: Default::default(),
            references: vec![SchemaReference {
                name: "common.Address".to_string(),
                subject: "address-value".to_string(),
                version: 1,
            }],
            metadata: None,
            rule_set: None,
        };

        let json = serde_json::to_string(&sv).unwrap();
        let deserialized: SchemaVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.id, 42);
        assert_eq!(deserialized.references.len(), 1);
        assert_eq!(deserialized.references[0].subject, "address-value");
    }

    #[test]
    fn test_schema_version_default_type_is_avro() {
        let json = r#"{"subject":"test","version":1,"id":1,"schema":"{}"}"#;
        let sv: SchemaVersion = serde_json::from_str(json).unwrap();
        assert_eq!(sv.schema_type, "AVRO");
    }

    #[test]
    fn test_manifest_roundtrip() {
        let manifest = SchemaRegistryManifest {
            schema_registry_url: "https://sr:8081".to_string(),
            backup_timestamp: "2026-04-05T12:00:00Z".to_string(),
            global_compatibility: "BACKWARD".to_string(),
            global_mode: Some("READWRITE".to_string()),
            subjects: vec![SubjectSummary {
                name: "orders-value".to_string(),
                schema_type: "AVRO".to_string(),
                version_count: 2,
                latest_version: 2,
                latest_schema_id: 5,
                compatibility: Some("BACKWARD".to_string()),
                mode: None,
                has_references: false,
                soft_deleted_versions: 0,
            }],
            subjects_count: 1,
            total_versions: 2,
            dependency_order: vec!["orders-value".to_string()],
            subjects_discovered: 1,
            subjects_failed: 0,
            errors: vec![],
            soft_deleted_versions: 0,
            referenced_subjects: vec![],
            snapshot: None,
        };

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let deserialized: SchemaRegistryManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.subjects_count, 1);
        assert_eq!(deserialized.total_versions, 2);
        assert_eq!(deserialized.dependency_order, vec!["orders-value"]);
    }

    #[test]
    fn test_register_schema_request_import_mode() {
        let req = RegisterSchemaRequest {
            schema: "{}".to_string(),
            schema_type: Some("AVRO".to_string()),
            references: vec![],
            id: Some(42),
            version: Some(1),
            metadata: None,
            rule_set: None,
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"id\":42"));
        assert!(json.contains("\"version\":1"));
    }

    #[test]
    fn test_register_schema_request_normal_mode_omits_id() {
        let req = RegisterSchemaRequest {
            schema: "{}".to_string(),
            schema_type: None,
            references: vec![],
            id: None,
            version: None,
            metadata: None,
            rule_set: None,
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(!json.contains("\"id\""));
        assert!(!json.contains("\"version\""));
    }

    #[test]
    fn pre_0_7_subject_metadata_still_parses() {
        // Written by backups before soft-deleted versions were captured.
        let json = r#"{"subject":"s","schema_type":"AVRO","versions":[1,2],
            "compatibility":null,"mode":null,"is_soft_deleted":false}"#;
        let meta: SubjectMetadata = serde_json::from_str(json).unwrap();
        assert!(meta.soft_deleted_versions.is_empty());
        assert!(!meta.included_as_reference);
        assert!(!meta.is_version_soft_deleted(1));
    }

    #[test]
    fn soft_deleted_subject_marks_every_version() {
        let meta = SubjectMetadata {
            subject: "s".into(),
            schema_type: "AVRO".into(),
            versions: vec![1, 2],
            compatibility: None,
            mode: None,
            is_soft_deleted: true,
            soft_deleted_versions: vec![],
            included_as_reference: false,
            deleted_versions: vec![],
            config: None,
        };
        assert!(meta.is_version_soft_deleted(1) && meta.is_version_soft_deleted(2));
    }

    #[test]
    fn pre_0_7_global_config_still_parses() {
        let cfg: GlobalConfig =
            serde_json::from_str(r#"{"compatibility_level":"FULL","mode":null}"#).unwrap();
        assert!(cfg.contexts.is_empty());
    }

    #[test]
    fn schema_by_id_defaults_avro_without_references() {
        // Schema Registry 7.x omits schemaType for Avro.
        let s: SchemaById = serde_json::from_str(r#"{"schema":"\"string\""}"#).unwrap();
        assert_eq!(s.schema_type, "AVRO");
        assert!(s.references.is_empty());
    }

    #[test]
    fn register_response_version_is_optional() {
        let r: RegisterSchemaResponse = serde_json::from_str(r#"{"id":3}"#).unwrap();
        assert_eq!((r.id, r.version), (3, None));
        let r: RegisterSchemaResponse =
            serde_json::from_str(r#"{"id":3,"version":2,"guid":"x"}"#).unwrap();
        assert_eq!(r.version, Some(2));
    }
}
