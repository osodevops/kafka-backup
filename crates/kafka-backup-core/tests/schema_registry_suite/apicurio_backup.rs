//! Integration tests for Apicurio Registry backup using a mock HTTP server.
//!
//! These tests verify the full backup pipeline against a mocked Apicurio Registry v3 API,
//! using an in-memory storage backend. No Docker or real registry required.

use kafka_backup_core::apicurio_registry::backup::ApicurioRegistryBackup;
use kafka_backup_core::apicurio_registry::client::ApicurioRegistryClient;
use kafka_backup_core::apicurio_registry::models::*;
use kafka_backup_core::config::{
    ApicurioBackupOptions, ApicurioConnectionConfig, ApicurioRegistryConfig,
};
use kafka_backup_core::storage::{MemoryBackend, StorageBackend};
use std::sync::Arc;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// `(artifact_id, type, [(version, globalId, contentId)])`
type ArtifactFixture<'a> = (&'a str, &'a str, &'a [(&'a str, i64, i64)]);

// ============================================================================
// Test Helpers
// ============================================================================

fn test_ar_config(url: &str) -> ApicurioRegistryConfig {
    ApicurioRegistryConfig {
        api_version: Default::default(),
        enabled: true,
        url: url.to_string(),
        auth: None,
        tls: None,
        backup: Some(ApicurioBackupOptions::default()),
        connection: Some(ApicurioConnectionConfig {
            timeout_ms: 5_000,
            max_retries: 1,
            retry_backoff_ms: 100,
            rate_limit_rps: 100,
        }),
    }
}

fn avro_schema(name: &str) -> String {
    format!(
        r#"{{"type":"record","name":"{}","namespace":"com.example","fields":[{{"name":"id","type":"string"}}]}}"#,
        name
    )
}

/// Mount mocks for a group with artifacts and versions.
async fn mount_group_mocks(server: &MockServer, group_id: &str, artifacts: &[ArtifactFixture<'_>]) {
    let api = "/apis/registry/v3";

    // Mock: list artifacts in group
    let artifact_infos: Vec<serde_json::Value> = artifacts
        .iter()
        .map(|(aid, atype, _)| {
            serde_json::json!({
                "groupId": group_id,
                "artifactId": aid,
                "artifactType": atype,
                "labels": {}
            })
        })
        .collect();

    Mock::given(method("GET"))
        .and(path(format!("{}/groups/{}/artifacts", api, group_id)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": artifacts.len(),
            "artifacts": artifact_infos
        })))
        .mount(server)
        .await;

    // Mock: get group metadata
    Mock::given(method("GET"))
        .and(path(format!("{}/groups/{}", api, group_id)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "groupId": group_id,
            "description": format!("{} group", group_id),
            "labels": {}
        })))
        .mount(server)
        .await;

    // Mock: group rules (empty)
    Mock::given(method("GET"))
        .and(path(format!("{}/groups/{}/rules", api, group_id)))
        .respond_with(ResponseTemplate::new(200).set_body_json::<Vec<String>>(vec![]))
        .mount(server)
        .await;

    for (artifact_id, artifact_type, versions) in artifacts {
        // Mock: list versions
        let version_infos: Vec<serde_json::Value> = versions
            .iter()
            .map(|(v, gid, cid)| {
                serde_json::json!({
                    "version": v,
                    "globalId": gid,
                    "contentId": cid,
                    "artifactType": artifact_type,
                    "labels": {}
                })
            })
            .collect();

        Mock::given(method("GET"))
            .and(path(format!(
                "{}/groups/{}/artifacts/{}/versions",
                api, group_id, artifact_id
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "count": versions.len(),
                "versions": version_infos
            })))
            .mount(server)
            .await;

        // Mock: get artifact metadata
        Mock::given(method("GET"))
            .and(path(format!(
                "{}/groups/{}/artifacts/{}",
                api, group_id, artifact_id
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "groupId": group_id,
                "artifactId": artifact_id,
                "artifactType": artifact_type,
                "labels": {}
            })))
            .mount(server)
            .await;

        // Mock: artifact rules (empty)
        Mock::given(method("GET"))
            .and(path(format!(
                "{}/groups/{}/artifacts/{}/rules",
                api, group_id, artifact_id
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json::<Vec<String>>(vec![]))
            .mount(server)
            .await;

        // Mock: each version's content and references
        for (version, _gid, _cid) in *versions {
            Mock::given(method("GET"))
                .and(path(format!(
                    "{}/groups/{}/artifacts/{}/versions/{}/content",
                    api, group_id, artifact_id, version
                )))
                .respond_with(ResponseTemplate::new(200).set_body_string(avro_schema(artifact_id)))
                .mount(server)
                .await;

            Mock::given(method("GET"))
                .and(path(format!(
                    "{}/groups/{}/artifacts/{}/versions/{}/references",
                    api, group_id, artifact_id, version
                )))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json::<Vec<serde_json::Value>>(vec![]),
                )
                .mount(server)
                .await;
        }
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[tokio::test]
async fn test_full_backup_pipeline() {
    let mock_server = MockServer::start().await;
    let api = "/apis/registry/v3";

    // Mock: export ZIP
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/export", api)))
        .respond_with(
            ResponseTemplate::new(200).set_body_bytes(b"PK\x03\x04fake-zip-data".to_vec()),
        )
        .mount(&mock_server)
        .await;

    // Mock: global rules
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/rules", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec!["COMPATIBILITY"]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/admin/rules/COMPATIBILITY", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ruleType": "COMPATIBILITY",
            "config": "BACKWARD"
        })))
        .mount(&mock_server)
        .await;

    // Mock: list groups
    Mock::given(method("GET"))
        .and(path(format!("{}/groups", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 2,
            "groups": [
                {"groupId": "payments", "labels": {}},
                {"groupId": "orders", "labels": {}}
            ]
        })))
        .mount(&mock_server)
        .await;

    // Mount group mocks
    mount_group_mocks(
        &mock_server,
        "payments",
        &[("PaymentEvent", "AVRO", &[("1", 1, 1), ("2", 5, 3)])],
    )
    .await;

    mount_group_mocks(
        &mock_server,
        "orders",
        &[
            ("OrderCreated", "AVRO", &[("1", 2, 2)]),
            ("OrderShipped", "PROTOBUF", &[("1", 3, 4), ("2", 6, 5)]),
        ],
    )
    .await;

    // Run backup
    let config = test_ar_config(&mock_server.uri());
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup = ApicurioRegistryBackup::new(
        client,
        storage.clone(),
        "test-backup-001".to_string(),
        config,
    );

    let manifest = backup.run().await.unwrap();

    // Verify manifest
    assert_eq!(manifest.groups_count, 2);
    assert_eq!(manifest.artifacts_count, 3);
    assert_eq!(manifest.total_versions, 5);
    assert!(manifest.has_export_zip);
    assert_eq!(manifest.global_rules.len(), 1);
    assert_eq!(manifest.global_rules[0].rule_type, "COMPATIBILITY");
    assert_eq!(manifest.global_rules[0].config, "BACKWARD");

    // Verify storage keys exist
    assert!(storage
        .exists("test-backup-001/apicurio-registry/_manifest.json")
        .await
        .unwrap());
    assert!(storage
        .exists("test-backup-001/apicurio-registry/_export.zip")
        .await
        .unwrap());
    assert!(storage
        .exists("test-backup-001/apicurio-registry/_global_rules.json")
        .await
        .unwrap());
    assert!(storage
        .exists("test-backup-001/apicurio-registry/groups/payments/_metadata.json")
        .await
        .unwrap());
    assert!(storage
        .exists("test-backup-001/apicurio-registry/groups/orders/_metadata.json")
        .await
        .unwrap());
    assert!(storage
        .exists("test-backup-001/apicurio-registry/groups/payments/artifacts/PaymentEvent/_metadata.json")
        .await
        .unwrap());
    assert!(storage
        .exists("test-backup-001/apicurio-registry/groups/payments/artifacts/PaymentEvent/versions/1.json")
        .await
        .unwrap());
    assert!(storage
        .exists("test-backup-001/apicurio-registry/groups/payments/artifacts/PaymentEvent/versions/1.content")
        .await
        .unwrap());
    assert!(storage
        .exists("test-backup-001/apicurio-registry/groups/payments/artifacts/PaymentEvent/versions/2.json")
        .await
        .unwrap());

    // Verify version content roundtrip
    let content = storage
        .get("test-backup-001/apicurio-registry/groups/payments/artifacts/PaymentEvent/versions/1.content")
        .await
        .unwrap();
    let content_str = String::from_utf8(content.to_vec()).unwrap();
    assert!(content_str.contains("PaymentEvent"));

    // Verify version metadata
    let version_meta = storage
        .get("test-backup-001/apicurio-registry/groups/payments/artifacts/PaymentEvent/versions/1.json")
        .await
        .unwrap();
    let backed_up: BackedUpVersion = serde_json::from_slice(&version_meta).unwrap();
    assert_eq!(backed_up.global_id, 1);
    assert_eq!(backed_up.content_id, 1);
    assert_eq!(backed_up.version, "1");
}

#[tokio::test]
async fn test_backup_with_group_filtering() {
    let mock_server = MockServer::start().await;
    let api = "/apis/registry/v3";

    // Mock: export ZIP (skip)
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/export", api)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"PK".to_vec()))
        .mount(&mock_server)
        .await;

    // Mock: global rules (empty)
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/rules", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json::<Vec<String>>(vec![]))
        .mount(&mock_server)
        .await;

    // Mock: 4 groups
    Mock::given(method("GET"))
        .and(path(format!("{}/groups", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 4,
            "groups": [
                {"groupId": "payments", "labels": {}},
                {"groupId": "orders", "labels": {}},
                {"groupId": "test-schemas", "labels": {}},
                {"groupId": "dev-internal", "labels": {}}
            ]
        })))
        .mount(&mock_server)
        .await;

    // Only mount mocks for groups we expect to be backed up
    mount_group_mocks(
        &mock_server,
        "payments",
        &[("PaymentEvent", "AVRO", &[("1", 1, 1)])],
    )
    .await;

    mount_group_mocks(
        &mock_server,
        "orders",
        &[("OrderCreated", "AVRO", &[("1", 2, 2)])],
    )
    .await;

    // Config: include pay* and order*, exclude test-*
    let mut config = test_ar_config(&mock_server.uri());
    config.backup = Some(ApicurioBackupOptions {
        groups: vec!["pay*".to_string(), "order*".to_string()],
        exclude_groups: vec!["test-*".to_string()],
        ..ApicurioBackupOptions::default()
    });

    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), "test-filter".to_string(), config);

    let manifest = backup.run().await.unwrap();

    // Only payments and orders should be backed up
    assert_eq!(manifest.groups_count, 2);
    assert_eq!(manifest.groups.len(), 2);
    let group_ids: Vec<&str> = manifest
        .groups
        .iter()
        .map(|g| g.group_id.as_str())
        .collect();
    assert!(group_ids.contains(&"payments"));
    assert!(group_ids.contains(&"orders"));
}

#[tokio::test]
async fn test_backup_with_artifact_references() {
    let mock_server = MockServer::start().await;
    let api = "/apis/registry/v3";

    // Mock: export ZIP
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/export", api)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"PK".to_vec()))
        .mount(&mock_server)
        .await;

    // Mock: global rules
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/rules", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json::<Vec<String>>(vec![]))
        .mount(&mock_server)
        .await;

    // Mock: 2 groups
    Mock::given(method("GET"))
        .and(path(format!("{}/groups", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 2,
            "groups": [
                {"groupId": "common", "labels": {}},
                {"groupId": "orders", "labels": {}}
            ]
        })))
        .mount(&mock_server)
        .await;

    // Mount "common" group with Address artifact (no refs)
    mount_group_mocks(
        &mock_server,
        "common",
        &[("Address", "AVRO", &[("1", 1, 1)])],
    )
    .await;

    // Mount "orders" group manually (not using helper) so we can set references
    Mock::given(method("GET"))
        .and(path(format!("{}/groups/{}/artifacts", api, "orders")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 1,
            "artifacts": [{"groupId": "orders", "artifactId": "OrderEvent", "artifactType": "PROTOBUF", "labels": {}}]
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/groups/orders", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "groupId": "orders", "labels": {}
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/groups/orders/rules", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json::<Vec<String>>(vec![]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/groups/orders/artifacts/OrderEvent", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "groupId": "orders", "artifactId": "OrderEvent", "artifactType": "PROTOBUF", "labels": {}
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "{}/groups/orders/artifacts/OrderEvent/rules",
            api
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json::<Vec<String>>(vec![]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/groups/orders/artifacts/OrderEvent/versions", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 1,
            "versions": [{"version": "1", "globalId": 2, "contentId": 2, "artifactType": "PROTOBUF", "labels": {}}]
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "{}/groups/orders/artifacts/OrderEvent/versions/1/content",
            api
        )))
        .respond_with(ResponseTemplate::new(200).set_body_string("syntax = \"proto3\";"))
        .mount(&mock_server)
        .await;

    // OrderEvent v1 references common/Address
    Mock::given(method("GET"))
        .and(path(format!(
            "{}/groups/orders/artifacts/OrderEvent/versions/1/references",
            api
        )))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(vec![serde_json::json!({
                "groupId": "common",
                "artifactId": "Address",
                "version": "1",
                "name": "common.Address"
            })]),
        )
        .mount(&mock_server)
        .await;

    let config = test_ar_config(&mock_server.uri());
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), "test-refs".to_string(), config);

    let manifest = backup.run().await.unwrap();

    assert_eq!(manifest.groups_count, 2);
    assert_eq!(manifest.artifacts_count, 2);

    // Verify dependency order: common/Address must come before orders/OrderEvent
    assert!(!manifest.dependency_order.is_empty());
    let address_pos = manifest
        .dependency_order
        .iter()
        .position(|c| c.group_id == "common" && c.artifact_id == "Address");
    let order_pos = manifest
        .dependency_order
        .iter()
        .position(|c| c.group_id == "orders" && c.artifact_id == "OrderEvent");

    if let (Some(a), Some(o)) = (address_pos, order_pos) {
        assert!(
            a < o,
            "Address should come before OrderEvent in dependency order"
        );
    }
}

#[tokio::test]
async fn test_backup_export_zip_stored() {
    let mock_server = MockServer::start().await;
    let api = "/apis/registry/v3";

    let zip_content = b"PK\x03\x04this-is-a-test-zip-archive-with-some-data";

    // Mock: export ZIP with specific content
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/export", api)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(zip_content.to_vec()))
        .mount(&mock_server)
        .await;

    // Mock: global rules (empty)
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/rules", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json::<Vec<String>>(vec![]))
        .mount(&mock_server)
        .await;

    // Mock: no groups
    Mock::given(method("GET"))
        .and(path(format!("{}/groups", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 0,
            "groups": []
        })))
        .mount(&mock_server)
        .await;

    let config = test_ar_config(&mock_server.uri());
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), "test-zip".to_string(), config);

    let manifest = backup.run().await.unwrap();
    assert!(manifest.has_export_zip);

    // Verify ZIP was stored with correct content
    let stored_zip = storage
        .get("test-zip/apicurio-registry/_export.zip")
        .await
        .unwrap();
    assert_eq!(stored_zip.as_ref(), zip_content);
}

#[tokio::test]
async fn test_backup_with_rules_at_three_scopes() {
    let mock_server = MockServer::start().await;
    let api = "/apis/registry/v3";

    // Mock: export ZIP
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/export", api)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"PK".to_vec()))
        .mount(&mock_server)
        .await;

    // Mock: global rules (COMPATIBILITY)
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/rules", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec!["COMPATIBILITY"]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/admin/rules/COMPATIBILITY", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ruleType": "COMPATIBILITY",
            "config": "BACKWARD"
        })))
        .mount(&mock_server)
        .await;

    // Mock: one group
    Mock::given(method("GET"))
        .and(path(format!("{}/groups", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 1,
            "groups": [{"groupId": "payments", "labels": {}}]
        })))
        .mount(&mock_server)
        .await;

    // Group metadata + group rules
    Mock::given(method("GET"))
        .and(path(format!("{}/groups/payments", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "groupId": "payments",
            "labels": {}
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/groups/payments/rules", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec!["VALIDITY"]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/groups/payments/rules/VALIDITY", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ruleType": "VALIDITY",
            "config": "FULL"
        })))
        .mount(&mock_server)
        .await;

    // Artifact with rules
    Mock::given(method("GET"))
        .and(path(format!("{}/groups/payments/artifacts", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 1,
            "artifacts": [{"groupId": "payments", "artifactId": "PaymentEvent", "artifactType": "AVRO", "labels": {}}]
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "{}/groups/payments/artifacts/PaymentEvent",
            api
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "groupId": "payments",
            "artifactId": "PaymentEvent",
            "artifactType": "AVRO",
            "labels": {}
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "{}/groups/payments/artifacts/PaymentEvent/rules",
            api
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec!["INTEGRITY"]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "{}/groups/payments/artifacts/PaymentEvent/rules/INTEGRITY",
            api
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "ruleType": "INTEGRITY",
            "config": "FULL"
        })))
        .mount(&mock_server)
        .await;

    // Version
    Mock::given(method("GET"))
        .and(path(format!("{}/groups/payments/artifacts/PaymentEvent/versions", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 1,
            "versions": [{"version": "1", "globalId": 1, "contentId": 1, "artifactType": "AVRO", "labels": {}}]
        })))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "{}/groups/payments/artifacts/PaymentEvent/versions/1/content",
            api
        )))
        .respond_with(ResponseTemplate::new(200).set_body_string(avro_schema("PaymentEvent")))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!(
            "{}/groups/payments/artifacts/PaymentEvent/versions/1/references",
            api
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json::<Vec<serde_json::Value>>(vec![]))
        .mount(&mock_server)
        .await;

    let config = test_ar_config(&mock_server.uri());
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), "test-rules".to_string(), config);

    let manifest = backup.run().await.unwrap();

    // Verify global rules in manifest
    assert_eq!(manifest.global_rules.len(), 1);
    assert_eq!(manifest.global_rules[0].config, "BACKWARD");

    // Verify group rules in stored metadata
    let group_meta_bytes = storage
        .get("test-rules/apicurio-registry/groups/payments/_metadata.json")
        .await
        .unwrap();
    let group_meta: GroupMetadata = serde_json::from_slice(&group_meta_bytes).unwrap();
    assert_eq!(group_meta.rules.len(), 1);
    assert_eq!(group_meta.rules[0].rule_type, "VALIDITY");

    // Verify artifact rules in stored metadata
    let artifact_meta_bytes = storage
        .get("test-rules/apicurio-registry/groups/payments/artifacts/PaymentEvent/_metadata.json")
        .await
        .unwrap();
    let artifact_meta: ArtifactMetadata = serde_json::from_slice(&artifact_meta_bytes).unwrap();
    assert_eq!(artifact_meta.rules.len(), 1);
    assert_eq!(artifact_meta.rules[0].rule_type, "INTEGRITY");
}

#[tokio::test]
async fn test_backup_empty_registry() {
    let mock_server = MockServer::start().await;
    let api = "/apis/registry/v3";

    Mock::given(method("GET"))
        .and(path(format!("{}/admin/export", api)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"PK".to_vec()))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/admin/rules", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json::<Vec<String>>(vec![]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/groups", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 0,
            "groups": []
        })))
        .mount(&mock_server)
        .await;

    let config = test_ar_config(&mock_server.uri());
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), "test-empty".to_string(), config);

    let manifest = backup.run().await.unwrap();
    assert_eq!(manifest.groups_count, 0);
    assert_eq!(manifest.artifacts_count, 0);
    assert_eq!(manifest.total_versions, 0);
    assert!(manifest.dependency_order.is_empty());
}

#[tokio::test]
async fn test_backup_handles_rate_limiting() {
    let mock_server = MockServer::start().await;
    let api = "/apis/registry/v3";

    // First request: 429, second: success
    Mock::given(method("GET"))
        .and(path(format!("{}/admin/export", api)))
        .respond_with(ResponseTemplate::new(429).set_body_string("Rate limited"))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/admin/export", api)))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"PK".to_vec()))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/admin/rules", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json::<Vec<String>>(vec![]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("{}/groups", api)))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "count": 0,
            "groups": []
        })))
        .mount(&mock_server)
        .await;

    let config = test_ar_config(&mock_server.uri());
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup = ApicurioRegistryBackup::new(
        client,
        storage.clone(),
        "test-rate-limit".to_string(),
        config,
    );

    // Should succeed after retry
    let manifest = backup.run().await.unwrap();
    assert!(manifest.has_export_zip);
}
