use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ============================================================================
// Apicurio Registry Core API v3 Response Types
// ============================================================================

/// Paginated group list from GET /groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSearchResults {
    pub count: i64,
    #[serde(default)]
    pub groups: Vec<GroupInfo>,
}

/// Paginated artifact list from GET /groups/{groupId}/artifacts
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactSearchResults {
    pub count: i64,
    #[serde(default)]
    pub artifacts: Vec<ArtifactInfo>,
}

/// Paginated version list from GET /groups/{groupId}/artifacts/{artifactId}/versions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionSearchResults {
    pub count: i64,
    #[serde(default)]
    pub versions: Vec<VersionInfo>,
}

/// Group information from the Apicurio API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupInfo {
    #[serde(rename = "groupId", default = "default_group")]
    pub group_id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(rename = "createdOn", default)]
    pub created_on: Option<String>,
    #[serde(rename = "modifiedOn", default)]
    pub modified_on: Option<String>,
    #[serde(flatten, default)]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Artifact information from the Apicurio API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactInfo {
    #[serde(rename = "groupId", default = "default_group")]
    pub group_id: String,
    #[serde(rename = "artifactId")]
    pub artifact_id: String,
    #[serde(rename = "artifactType")]
    pub artifact_type: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(rename = "createdOn", default)]
    pub created_on: Option<String>,
    #[serde(rename = "modifiedOn", default)]
    pub modified_on: Option<String>,
    #[serde(flatten, default)]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Version information from the Apicurio API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionInfo {
    pub version: String,
    #[serde(rename = "globalId")]
    pub global_id: i64,
    #[serde(rename = "contentId")]
    pub content_id: i64,
    #[serde(rename = "artifactType")]
    pub artifact_type: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(rename = "createdOn", default)]
    pub created_on: Option<String>,
    #[serde(flatten, default)]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

/// Cross-artifact reference (dependency).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactReference {
    #[serde(rename = "groupId", default = "default_group")]
    pub group_id: String,
    #[serde(rename = "artifactId")]
    pub artifact_id: String,
    pub version: String,
    pub name: String,
}

/// Rule configuration (used at global, group, and artifact scope).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleConfig {
    #[serde(rename = "ruleType", alias = "type")]
    pub rule_type: String,
    pub config: String,
}

/// Apicurio API error response body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApicurioApiError {
    #[serde(default)]
    pub error_code: Option<i32>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub detail: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

// ============================================================================
// Backup Storage Types
// ============================================================================

/// Top-level Apicurio registry backup manifest.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApicurioRegistryManifest {
    pub registry_url: String,
    pub backup_timestamp: String,
    pub api_version: String,
    pub global_rules: Vec<RuleConfig>,
    pub groups: Vec<GroupSummary>,
    pub groups_count: usize,
    pub artifacts_count: usize,
    pub total_versions: usize,
    pub dependency_order: Vec<ArtifactCoordinate>,
    pub has_export_zip: bool,
    #[serde(default)]
    pub snapshot: Option<crate::registry_restore::snapshot::SnapshotInfo>,
}

/// Summary of a backed-up group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSummary {
    pub group_id: String,
    pub artifact_count: usize,
    pub version_count: usize,
}

/// Fully qualified artifact coordinate (group + artifact).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ArtifactCoordinate {
    pub group_id: String,
    pub artifact_id: String,
}

/// Per-group metadata stored in backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMetadata {
    pub group_id: String,
    pub description: Option<String>,
    pub labels: HashMap<String, String>,
    pub artifact_count: usize,
    pub rules: Vec<RuleConfig>,
}

/// Per-artifact metadata stored in backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactMetadata {
    pub group_id: String,
    pub artifact_id: String,
    pub artifact_type: String,
    pub versions: Vec<String>,
    pub rules: Vec<RuleConfig>,
    pub labels: HashMap<String, String>,
}

/// Per-version metadata stored in backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackedUpVersion {
    pub version: String,
    pub global_id: i64,
    pub content_id: i64,
    pub artifact_type: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub labels: HashMap<String, String>,
    pub references: Vec<ArtifactReference>,
    pub content_hash: String,
    #[serde(default)]
    pub state: Option<String>,
    pub created_on: Option<String>,
    #[serde(flatten, default)]
    pub extra: std::collections::BTreeMap<String, serde_json::Value>,
}

fn default_group() -> String {
    "default".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_info_deserialization() {
        let json = r#"{
            "groupId": "payments",
            "description": "Payment domain",
            "labels": {"team": "payments"},
            "createdOn": "2026-01-01T00:00:00Z"
        }"#;
        let info: GroupInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.group_id, "payments");
        assert_eq!(info.description.as_deref(), Some("Payment domain"));
        assert_eq!(info.labels.get("team").unwrap(), "payments");
    }

    #[test]
    fn test_artifact_info_deserialization() {
        let json = r#"{
            "groupId": "payments",
            "artifactId": "PaymentEvent",
            "artifactType": "AVRO",
            "name": "PaymentEvent",
            "labels": {}
        }"#;
        let info: ArtifactInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.artifact_id, "PaymentEvent");
        assert_eq!(info.artifact_type, "AVRO");
    }

    #[test]
    fn test_version_info_deserialization() {
        let json = r#"{
            "version": "2",
            "globalId": 42,
            "contentId": 15,
            "artifactType": "PROTOBUF",
            "state": "ENABLED",
            "labels": {}
        }"#;
        let info: VersionInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.version, "2");
        assert_eq!(info.global_id, 42);
        assert_eq!(info.content_id, 15);
        assert_eq!(info.artifact_type, "PROTOBUF");
    }

    #[test]
    fn test_artifact_reference_deserialization() {
        let json = r#"{
            "groupId": "common",
            "artifactId": "Address",
            "version": "1",
            "name": "common.Address"
        }"#;
        let reference: ArtifactReference = serde_json::from_str(json).unwrap();
        assert_eq!(reference.group_id, "common");
        assert_eq!(reference.artifact_id, "Address");
        assert_eq!(reference.version, "1");
        assert_eq!(reference.name, "common.Address");
    }

    #[test]
    fn test_rule_config_deserialization() {
        let json = r#"{"ruleType": "COMPATIBILITY", "config": "BACKWARD"}"#;
        let rule: RuleConfig = serde_json::from_str(json).unwrap();
        assert_eq!(rule.rule_type, "COMPATIBILITY");
        assert_eq!(rule.config, "BACKWARD");
    }

    #[test]
    fn test_group_search_results_deserialization() {
        let json = r#"{
            "count": 2,
            "groups": [
                {"groupId": "payments", "labels": {}},
                {"groupId": "orders", "labels": {}}
            ]
        }"#;
        let results: GroupSearchResults = serde_json::from_str(json).unwrap();
        assert_eq!(results.count, 2);
        assert_eq!(results.groups.len(), 2);
    }

    #[test]
    fn test_manifest_roundtrip() {
        let manifest = ApicurioRegistryManifest {
            registry_url: "https://apicurio:8080".to_string(),
            backup_timestamp: "2026-04-07T12:00:00Z".to_string(),
            api_version: "v3".to_string(),
            global_rules: vec![RuleConfig {
                rule_type: "COMPATIBILITY".to_string(),
                config: "BACKWARD".to_string(),
            }],
            groups: vec![GroupSummary {
                group_id: "payments".to_string(),
                artifact_count: 3,
                version_count: 7,
            }],
            groups_count: 1,
            artifacts_count: 3,
            total_versions: 7,
            dependency_order: vec![ArtifactCoordinate {
                group_id: "common".to_string(),
                artifact_id: "Address".to_string(),
            }],
            has_export_zip: true,
            snapshot: None,
        };

        let json = serde_json::to_string_pretty(&manifest).unwrap();
        let parsed: ApicurioRegistryManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.groups_count, 1);
        assert_eq!(parsed.artifacts_count, 3);
        assert_eq!(parsed.total_versions, 7);
        assert!(parsed.has_export_zip);
        assert_eq!(parsed.dependency_order.len(), 1);
    }

    #[test]
    fn test_artifact_coordinate_equality() {
        let a = ArtifactCoordinate {
            group_id: "payments".to_string(),
            artifact_id: "PaymentEvent".to_string(),
        };
        let b = ArtifactCoordinate {
            group_id: "payments".to_string(),
            artifact_id: "PaymentEvent".to_string(),
        };
        let c = ArtifactCoordinate {
            group_id: "orders".to_string(),
            artifact_id: "PaymentEvent".to_string(),
        };
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_version_info_defaults() {
        let json = r#"{
            "version": "1",
            "globalId": 1,
            "contentId": 1,
            "artifactType": "AVRO"
        }"#;
        let info: VersionInfo = serde_json::from_str(json).unwrap();
        assert!(info.name.is_none());
        assert!(info.description.is_none());
        assert!(info.state.is_none());
        assert!(info.labels.is_empty());
        assert!(info.created_on.is_none());
    }
}
