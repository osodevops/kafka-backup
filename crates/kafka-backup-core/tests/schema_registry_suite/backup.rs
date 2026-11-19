//! Integration tests for Schema Registry backup using a mock HTTP server.
//!
//! These tests verify the full backup pipeline against a mocked Schema Registry,
//! using an in-memory storage backend. No Docker or real SR required.

use kafka_backup_core::schema_registry::backup::SchemaRegistryBackup;
use kafka_backup_core::schema_registry::client::SchemaRegistryClient;
use kafka_backup_core::schema_registry::config::{
    SchemaBackupOptions, SchemaConnectionConfig, SchemaRegistryConfig,
};
use kafka_backup_core::schema_registry::models::*;
use kafka_backup_core::storage::{MemoryBackend, StorageBackend};
use std::sync::Arc;
use wiremock::matchers::{method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ============================================================================
// Test Helpers
// ============================================================================

fn test_sr_config(url: &str) -> SchemaRegistryConfig {
    SchemaRegistryConfig {
        enabled: true,
        url: url.to_string(),
        auth: None,
        tls: None,
        backup: Some(SchemaBackupOptions::default()),
        restore: None,
        connection: Some(SchemaConnectionConfig {
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

// ============================================================================
// Integration Tests (mocked HTTP)
// ============================================================================

#[tokio::test]
async fn test_full_backup_pipeline() {
    let mock_server = MockServer::start().await;

    // Mock: list subjects
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(vec!["orders-value", "payments-value"]),
        )
        .mount(&mock_server)
        .await;

    // Mock: global config
    Mock::given(method("GET"))
        .and(path("/config"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"compatibilityLevel": "BACKWARD"})),
        )
        .mount(&mock_server)
        .await;

    // Mock: global mode
    Mock::given(method("GET"))
        .and(path("/mode"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"mode": "READWRITE"})),
        )
        .mount(&mock_server)
        .await;

    // Mock: orders-value versions
    Mock::given(method("GET"))
        .and(path("/subjects/orders-value/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![1, 2]))
        .mount(&mock_server)
        .await;

    // Mock: orders-value v1
    Mock::given(method("GET"))
        .and(path("/subjects/orders-value/versions/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subject": "orders-value",
            "version": 1,
            "id": 1,
            "schemaType": "AVRO",
            "schema": avro_schema("Order"),
            "references": []
        })))
        .mount(&mock_server)
        .await;

    // Mock: orders-value v2
    Mock::given(method("GET"))
        .and(path("/subjects/orders-value/versions/2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subject": "orders-value",
            "version": 2,
            "id": 5,
            "schemaType": "AVRO",
            "schema": avro_schema("OrderV2"),
            "references": []
        })))
        .mount(&mock_server)
        .await;

    // Mock: payments-value versions
    Mock::given(method("GET"))
        .and(path("/subjects/payments-value/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![1]))
        .mount(&mock_server)
        .await;

    // Mock: payments-value v1
    Mock::given(method("GET"))
        .and(path("/subjects/payments-value/versions/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subject": "payments-value",
            "version": 1,
            "id": 2,
            "schemaType": "AVRO",
            "schema": avro_schema("Payment"),
            "references": []
        })))
        .mount(&mock_server)
        .await;

    // Mock: per-subject config (orders has override, payments does not)
    Mock::given(method("GET"))
        .and(path("/config/orders-value"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"compatibilityLevel": "FULL"})),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/config/payments-value"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error_code": 40401,
            "message": "Subject not found"
        })))
        .mount(&mock_server)
        .await;

    // Mock: per-subject mode (none have overrides)
    Mock::given(method("GET"))
        .and(path_regex("/mode/.+"))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error_code": 40409,
            "message": "Subject mode not found"
        })))
        .mount(&mock_server)
        .await;

    // Run backup
    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup_id = "test-backup-001";

    let backup = SchemaRegistryBackup::new(client, storage.clone(), backup_id.to_string(), config);

    let manifest = backup.run().await.unwrap();

    // Verify manifest
    assert_eq!(manifest.subjects_count, 2);
    assert_eq!(manifest.total_versions, 3);
    assert_eq!(manifest.global_compatibility, "BACKWARD");
    assert_eq!(manifest.global_mode, Some("READWRITE".to_string()));
    assert_eq!(manifest.dependency_order.len(), 2);

    // Verify orders-value subject summary
    let orders = manifest
        .subjects
        .iter()
        .find(|s| s.name == "orders-value")
        .unwrap();
    assert_eq!(orders.version_count, 2);
    assert_eq!(orders.latest_version, 2);
    assert_eq!(orders.latest_schema_id, 5);
    assert_eq!(orders.compatibility, Some("FULL".to_string()));
    assert_eq!(orders.schema_type, "AVRO");

    // Verify payments-value
    let payments = manifest
        .subjects
        .iter()
        .find(|s| s.name == "payments-value")
        .unwrap();
    assert_eq!(payments.version_count, 1);
    assert!(payments.compatibility.is_none());

    // Verify files exist in storage
    assert!(storage
        .exists(&format!("{}/schema-registry/_manifest.json", backup_id))
        .await
        .unwrap());
    assert!(storage
        .exists(&format!(
            "{}/schema-registry/_global_config.json",
            backup_id
        ))
        .await
        .unwrap());
    assert!(storage
        .exists(&format!(
            "{}/schema-registry/subjects/orders-value/_metadata.json",
            backup_id
        ))
        .await
        .unwrap());
    assert!(storage
        .exists(&format!(
            "{}/schema-registry/subjects/orders-value/v1.json",
            backup_id
        ))
        .await
        .unwrap());
    assert!(storage
        .exists(&format!(
            "{}/schema-registry/subjects/orders-value/v2.json",
            backup_id
        ))
        .await
        .unwrap());
    assert!(storage
        .exists(&format!(
            "{}/schema-registry/subjects/payments-value/_metadata.json",
            backup_id
        ))
        .await
        .unwrap());
    assert!(storage
        .exists(&format!(
            "{}/schema-registry/subjects/payments-value/v1.json",
            backup_id
        ))
        .await
        .unwrap());

    // Verify the stored schema content is valid JSON
    let v1_data = storage
        .get(&format!(
            "{}/schema-registry/subjects/orders-value/v1.json",
            backup_id
        ))
        .await
        .unwrap();
    let v1: SchemaVersion = serde_json::from_slice(&v1_data).unwrap();
    assert_eq!(v1.id, 1);
    assert_eq!(v1.version, 1);
    assert_eq!(v1.schema_type, "AVRO");

    // Verify the stored manifest is valid JSON
    let manifest_data = storage
        .get(&format!("{}/schema-registry/_manifest.json", backup_id))
        .await
        .unwrap();
    let stored_manifest: SchemaRegistryManifest = serde_json::from_slice(&manifest_data).unwrap();
    assert_eq!(stored_manifest.subjects_count, 2);
}

#[tokio::test]
async fn test_backup_with_subject_filtering() {
    let mock_server = MockServer::start().await;

    // Return 5 subjects, but we only want orders-*
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![
            "orders-value",
            "orders-key",
            "payments-value",
            "users-value",
            "internal-events",
        ]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/config"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"compatibilityLevel": "BACKWARD"})),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/mode"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    // Only orders-* subjects should be fetched
    for subject in ["orders-value", "orders-key"] {
        Mock::given(method("GET"))
            .and(path(format!("/subjects/{}/versions", subject)))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![1]))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/subjects/{}/versions/1", subject)))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": subject,
                "version": 1,
                "id": 1,
                "schemaType": "AVRO",
                "schema": avro_schema("Test"),
                "references": []
            })))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/config/{}", subject)))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/mode/{}", subject)))
            .respond_with(ResponseTemplate::new(404))
            .mount(&mock_server)
            .await;
    }

    let mut config = test_sr_config(&mock_server.uri());
    config.backup = Some(SchemaBackupOptions {
        fail_on_partial: false,
        snapshot_interval: None,
        subjects: vec!["orders-*".to_string()],
        exclude: vec![],
        include_soft_deleted: false,
        include_versions: Default::default(),
        include_references: true,
    });

    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup = SchemaRegistryBackup::new(
        client,
        storage.clone(),
        "filtered-backup".to_string(),
        config,
    );
    let manifest = backup.run().await.unwrap();

    // Only 2 subjects should have been backed up (orders-value, orders-key)
    assert_eq!(manifest.subjects_count, 2);
    assert!(manifest
        .subjects
        .iter()
        .all(|s| s.name.starts_with("orders-")));
}

#[tokio::test]
async fn test_backup_with_schema_references() {
    let mock_server = MockServer::start().await;

    // common-types has no refs; orders-value refs common-types
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(vec!["common-types-value", "orders-value"]),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/config"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"compatibilityLevel": "BACKWARD"})),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/mode"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    // common-types-value: no references
    Mock::given(method("GET"))
        .and(path("/subjects/common-types-value/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![1]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/subjects/common-types-value/versions/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subject": "common-types-value",
            "version": 1,
            "id": 10,
            "schemaType": "AVRO",
            "schema": avro_schema("CommonType"),
            "references": []
        })))
        .mount(&mock_server)
        .await;

    // orders-value: references common-types-value
    Mock::given(method("GET"))
        .and(path("/subjects/orders-value/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![1]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/subjects/orders-value/versions/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subject": "orders-value",
            "version": 1,
            "id": 11,
            "schemaType": "AVRO",
            "schema": avro_schema("Order"),
            "references": [{"name": "CommonType", "subject": "common-types-value", "version": 1}]
        })))
        .mount(&mock_server)
        .await;

    // Config/mode mocks
    Mock::given(method("GET"))
        .and(path_regex("/config/.+"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex("/mode/.+"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup =
        SchemaRegistryBackup::new(client, storage.clone(), "ref-backup".to_string(), config);
    let manifest = backup.run().await.unwrap();

    // Verify dependency order: common-types-value must come before orders-value
    let pos_common = manifest
        .dependency_order
        .iter()
        .position(|s| s == "common-types-value")
        .unwrap();
    let pos_orders = manifest
        .dependency_order
        .iter()
        .position(|s| s == "orders-value")
        .unwrap();
    assert!(
        pos_common < pos_orders,
        "common-types-value must be restored before orders-value"
    );

    // Verify the orders subject has_references flag
    let orders = manifest
        .subjects
        .iter()
        .find(|s| s.name == "orders-value")
        .unwrap();
    assert!(orders.has_references);
}

#[tokio::test]
async fn test_backup_handles_rate_limiting() {
    let mock_server = MockServer::start().await;

    // First request to /subjects returns 429, second succeeds
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(429).set_body_json(serde_json::json!({
            "error_code": 429,
            "message": "Rate limit exceeded"
        })))
        .up_to_n_times(1)
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(Vec::<String>::new()))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/config"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"compatibilityLevel": "BACKWARD"})),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/mode"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup = SchemaRegistryBackup::new(client, storage, "retry-backup".to_string(), config);
    let manifest = backup.run().await.unwrap();

    // Should succeed after retry
    assert_eq!(manifest.subjects_count, 0);
}

/// Test backup handles all Confluent schema types (Avro, JSON Schema, Protobuf).
/// Mocked test — no Docker needed.
#[tokio::test]
async fn test_backup_mixed_schema_types() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![
            "avro-subject",
            "json-subject",
            "proto-subject",
        ]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/config"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"compatibilityLevel": "BACKWARD"})),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/mode"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    // Avro schema
    Mock::given(method("GET"))
        .and(path("/subjects/avro-subject/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![1]))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/subjects/avro-subject/versions/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subject": "avro-subject",
            "version": 1,
            "id": 1,
            "schemaType": "AVRO",
            "schema": avro_schema("AvroRecord"),
            "references": []
        })))
        .mount(&mock_server)
        .await;

    // JSON Schema
    Mock::given(method("GET"))
        .and(path("/subjects/json-subject/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![1]))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/subjects/json-subject/versions/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subject": "json-subject",
            "version": 1,
            "id": 2,
            "schemaType": "JSON",
            "schema": r#"{"type":"object","properties":{"id":{"type":"string"}}}"#,
            "references": []
        })))
        .mount(&mock_server)
        .await;

    // Protobuf schema
    Mock::given(method("GET"))
        .and(path("/subjects/proto-subject/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![1]))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/subjects/proto-subject/versions/1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subject": "proto-subject",
            "version": 1,
            "id": 3,
            "schemaType": "PROTOBUF",
            "schema": "syntax = \"proto3\";\nmessage ProtoRecord { string id = 1; }",
            "references": []
        })))
        .mount(&mock_server)
        .await;

    // Config/mode mocks
    Mock::given(method("GET"))
        .and(path_regex("/config/.+"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/mode/.+"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup =
        SchemaRegistryBackup::new(client, storage.clone(), "mixed-types".to_string(), config);
    let manifest = backup.run().await.unwrap();

    assert_eq!(manifest.subjects_count, 3);

    // Verify each schema type is captured
    let types: Vec<&str> = manifest
        .subjects
        .iter()
        .map(|s| s.schema_type.as_str())
        .collect();
    assert!(types.contains(&"AVRO"));
    assert!(types.contains(&"JSON"));
    assert!(types.contains(&"PROTOBUF"));

    // Verify the stored schema content preserves the type
    let proto_data = storage
        .get("mixed-types/schema-registry/subjects/proto-subject/v1.json")
        .await
        .unwrap();
    let proto: SchemaVersion = serde_json::from_slice(&proto_data).unwrap();
    assert_eq!(proto.schema_type, "PROTOBUF");
    assert!(proto.schema.contains("proto3"));
}

/// Test backup with many schema versions (mimics schema evolution over time).
/// Mocked test — no Docker needed.
#[tokio::test]
async fn test_backup_many_versions() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec!["evolving-subject"]))
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/config"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"compatibilityLevel": "BACKWARD"})),
        )
        .mount(&mock_server)
        .await;

    Mock::given(method("GET"))
        .and(path("/mode"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    // 10 versions of the same subject
    let versions: Vec<i32> = (1..=10).collect();
    Mock::given(method("GET"))
        .and(path("/subjects/evolving-subject/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&versions))
        .mount(&mock_server)
        .await;

    for v in 1..=10 {
        Mock::given(method("GET"))
            .and(path(format!("/subjects/evolving-subject/versions/{}", v)))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": "evolving-subject",
                "version": v,
                "id": v * 10,
                "schemaType": "AVRO",
                "schema": avro_schema(&format!("RecordV{}", v)),
                "references": []
            })))
            .mount(&mock_server)
            .await;
    }

    Mock::given(method("GET"))
        .and(path("/config/evolving-subject"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/mode/evolving-subject"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup =
        SchemaRegistryBackup::new(client, storage.clone(), "many-versions".to_string(), config);
    let manifest = backup.run().await.unwrap();

    assert_eq!(manifest.subjects_count, 1);
    assert_eq!(manifest.total_versions, 10);

    let subject = &manifest.subjects[0];
    assert_eq!(subject.version_count, 10);
    assert_eq!(subject.latest_version, 10);
    assert_eq!(subject.latest_schema_id, 100); // 10 * 10

    // Verify all 10 version files exist
    for v in 1..=10 {
        assert!(storage
            .exists(&format!(
                "many-versions/schema-registry/subjects/evolving-subject/v{}.json",
                v
            ))
            .await
            .unwrap());
    }
}
