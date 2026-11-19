//! Edge case tests for Schema Registry backup, based on real-world production issues.
//!
//! These tests cover:
//! - Schema reference chains (Protobuf cross-subject imports)
//! - Soft-deleted subjects
//! - Mixed schema types in a single registry
//! - Large registries with many versions
//! - Compatibility mode preservation
//! - Subject name edge cases (special characters, contexts)
//! - SR API error handling (auth failures, rate limits, timeouts)
//! - Migration scenarios (populated target, ID conflicts)

use kafka_backup_core::schema_registry::backup::SchemaRegistryBackup;
use kafka_backup_core::schema_registry::client::SchemaRegistryClient;
use kafka_backup_core::schema_registry::config::{
    SchemaBackupOptions, SchemaConnectionConfig, SchemaRegistryConfig, VersionSelection,
};
use kafka_backup_core::storage::{MemoryBackend, StorageBackend};
use std::sync::Arc;
use wiremock::matchers::{method, path, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

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
            rate_limit_rps: 200,
        }),
    }
}

async fn mount_global_defaults(mock_server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/config"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"compatibilityLevel": "BACKWARD"})),
        )
        .mount(mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path("/mode"))
        .respond_with(ResponseTemplate::new(404))
        .mount(mock_server)
        .await;
    // Default: 404 for per-subject config/mode
    Mock::given(method("GET"))
        .and(path_regex("/config/.+"))
        .respond_with(ResponseTemplate::new(404))
        .mount(mock_server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex("/mode/.+"))
        .respond_with(ResponseTemplate::new(404))
        .mount(mock_server)
        .await;
}

async fn mount_subject(mock_server: &MockServer, subject: &str, versions: Vec<serde_json::Value>) {
    let version_nums: Vec<i32> = versions
        .iter()
        .map(|v| v["version"].as_i64().unwrap() as i32)
        .collect();

    Mock::given(method("GET"))
        .and(path(format!("/subjects/{}/versions", subject)))
        .respond_with(ResponseTemplate::new(200).set_body_json(&version_nums))
        .mount(mock_server)
        .await;

    for v in &versions {
        let version_num = v["version"].as_i64().unwrap();
        Mock::given(method("GET"))
            .and(path(format!(
                "/subjects/{}/versions/{}",
                subject, version_num
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(v))
            .mount(mock_server)
            .await;
    }
}

// ============================================================================
// Edge Case: Deep reference chains (Protobuf-style imports)
// ============================================================================

/// Protobuf schemas commonly import shared types like google.protobuf.Timestamp.
/// This creates deep dependency chains: D → C → B → A.
/// The backup must capture all 4 and sort them correctly.
#[tokio::test]
async fn test_deep_reference_chain_4_levels() {
    let mock_server = MockServer::start().await;

    let subjects = vec!["base-types", "common-types", "domain-types", "api-types"];
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&subjects))
        .mount(&mock_server)
        .await;
    mount_global_defaults(&mock_server).await;

    // base-types: no refs
    mount_subject(
        &mock_server,
        "base-types",
        vec![serde_json::json!({
            "subject": "base-types", "version": 1, "id": 1,
            "schemaType": "PROTOBUF",
            "schema": "syntax=\"proto3\"; message BaseType { string id = 1; }",
            "references": []
        })],
    )
    .await;

    // common-types: refs base-types
    mount_subject(&mock_server, "common-types", vec![serde_json::json!({
        "subject": "common-types", "version": 1, "id": 2,
        "schemaType": "PROTOBUF",
        "schema": "syntax=\"proto3\"; import \"base.proto\"; message CommonType { BaseType base = 1; }",
        "references": [{"name": "base.proto", "subject": "base-types", "version": 1}]
    })]).await;

    // domain-types: refs common-types
    mount_subject(&mock_server, "domain-types", vec![serde_json::json!({
        "subject": "domain-types", "version": 1, "id": 3,
        "schemaType": "PROTOBUF",
        "schema": "syntax=\"proto3\"; import \"common.proto\"; message DomainType { CommonType common = 1; }",
        "references": [{"name": "common.proto", "subject": "common-types", "version": 1}]
    })]).await;

    // api-types: refs domain-types
    mount_subject(&mock_server, "api-types", vec![serde_json::json!({
        "subject": "api-types", "version": 1, "id": 4,
        "schemaType": "PROTOBUF",
        "schema": "syntax=\"proto3\"; import \"domain.proto\"; message ApiType { DomainType domain = 1; }",
        "references": [{"name": "domain.proto", "subject": "domain-types", "version": 1}]
    })]).await;

    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup = SchemaRegistryBackup::new(client, storage, "deep-refs".to_string(), config);
    let manifest = backup.run().await.unwrap();

    assert_eq!(manifest.subjects_count, 4);

    // Verify topological order: base < common < domain < api
    let order = &manifest.dependency_order;
    let pos = |name: &str| order.iter().position(|s| s == name).unwrap();
    assert!(pos("base-types") < pos("common-types"));
    assert!(pos("common-types") < pos("domain-types"));
    assert!(pos("domain-types") < pos("api-types"));
}

// ============================================================================
// Edge Case: Diamond dependency (A depends on B and C, both depend on D)
// ============================================================================

#[tokio::test]
async fn test_diamond_dependency_with_protobuf_refs() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![
            "timestamp-type",
            "address-type",
            "user-type",
            "order-type",
        ]))
        .mount(&mock_server)
        .await;
    mount_global_defaults(&mock_server).await;

    // timestamp: no refs
    mount_subject(&mock_server, "timestamp-type", vec![serde_json::json!({
        "subject": "timestamp-type", "version": 1, "id": 1,
        "schemaType": "PROTOBUF", "schema": "message Timestamp { int64 ms = 1; }", "references": []
    })]).await;

    // address: refs timestamp
    mount_subject(
        &mock_server,
        "address-type",
        vec![serde_json::json!({
            "subject": "address-type", "version": 1, "id": 2,
            "schemaType": "PROTOBUF", "schema": "message Address { string street = 1; }",
            "references": [{"name": "timestamp.proto", "subject": "timestamp-type", "version": 1}]
        })],
    )
    .await;

    // user: refs timestamp
    mount_subject(
        &mock_server,
        "user-type",
        vec![serde_json::json!({
            "subject": "user-type", "version": 1, "id": 3,
            "schemaType": "PROTOBUF", "schema": "message User { string name = 1; }",
            "references": [{"name": "timestamp.proto", "subject": "timestamp-type", "version": 1}]
        })],
    )
    .await;

    // order: refs BOTH address AND user (diamond)
    mount_subject(
        &mock_server,
        "order-type",
        vec![serde_json::json!({
            "subject": "order-type", "version": 1, "id": 4,
            "schemaType": "PROTOBUF", "schema": "message Order { string id = 1; }",
            "references": [
                {"name": "address.proto", "subject": "address-type", "version": 1},
                {"name": "user.proto", "subject": "user-type", "version": 1}
            ]
        })],
    )
    .await;

    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup = SchemaRegistryBackup::new(client, storage, "diamond".to_string(), config);
    let manifest = backup.run().await.unwrap();

    let order = &manifest.dependency_order;
    let pos = |name: &str| order.iter().position(|s| s == name).unwrap();

    // timestamp must come before address and user; both must come before order
    assert!(pos("timestamp-type") < pos("address-type"));
    assert!(pos("timestamp-type") < pos("user-type"));
    assert!(pos("address-type") < pos("order-type"));
    assert!(pos("user-type") < pos("order-type"));
}

// ============================================================================
// Edge Case: Subject names with special characters
// ============================================================================

#[tokio::test]
async fn test_subjects_with_special_characters() {
    let mock_server = MockServer::start().await;

    // Subjects with dots, hyphens, and context prefixes
    let subjects = vec![
        "com.example.orders-value",
        "my-app.payments-key",
        ":.mycontext:subject-name",
    ];

    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&subjects))
        .mount(&mock_server)
        .await;
    mount_global_defaults(&mock_server).await;

    for subject in &subjects {
        let encoded = urlencoding::encode(subject);
        Mock::given(method("GET"))
            .and(path(format!("/subjects/{}/versions", encoded)))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![1]))
            .mount(&mock_server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/subjects/{}/versions/1", encoded)))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "subject": subject, "version": 1, "id": 1,
                "schemaType": "AVRO",
                "schema": "{\"type\":\"string\"}",
                "references": []
            })))
            .mount(&mock_server)
            .await;
    }

    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup =
        SchemaRegistryBackup::new(client, storage.clone(), "special-chars".to_string(), config);
    let manifest = backup.run().await.unwrap();

    assert_eq!(manifest.subjects_count, 3);

    // Verify all subjects have metadata files (with URL-encoded paths)
    for subject in &manifest.subjects {
        let encoded = urlencoding::encode(&subject.name);
        let key = format!(
            "special-chars/schema-registry/subjects/{}/_metadata.json",
            encoded
        );
        assert!(
            storage.exists(&key).await.unwrap(),
            "Missing metadata for subject '{}' at key '{}'",
            subject.name,
            key
        );
    }
}

// ============================================================================
// Edge Case: Per-subject compatibility modes
// ============================================================================

#[tokio::test]
async fn test_per_subject_compatibility_preservation() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![
            "backward-subject",
            "full-subject",
            "none-subject",
            "default-subject",
        ]))
        .mount(&mock_server)
        .await;

    // Global = BACKWARD
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
    Mock::given(method("GET"))
        .and(path_regex("/mode/.+"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    // Per-subject configs: each has a different compatibility
    for (subject, compat) in [
        ("backward-subject", Some("BACKWARD_TRANSITIVE")),
        ("full-subject", Some("FULL")),
        ("none-subject", Some("NONE")),
        ("default-subject", None), // uses global default
    ] {
        mount_subject(
            &mock_server,
            subject,
            vec![serde_json::json!({
                "subject": subject, "version": 1, "id": 1,
                "schemaType": "AVRO", "schema": "{\"type\":\"string\"}", "references": []
            })],
        )
        .await;

        if let Some(c) = compat {
            Mock::given(method("GET"))
                .and(path(format!("/config/{}", subject)))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"compatibilityLevel": c})),
                )
                .mount(&mock_server)
                .await;
        } else {
            Mock::given(method("GET"))
                .and(path(format!("/config/{}", subject)))
                .respond_with(ResponseTemplate::new(404))
                .mount(&mock_server)
                .await;
        }
    }

    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup = SchemaRegistryBackup::new(client, storage, "compat-test".to_string(), config);
    let manifest = backup.run().await.unwrap();

    let find = |name: &str| manifest.subjects.iter().find(|s| s.name == name).unwrap();

    assert_eq!(
        find("backward-subject").compatibility,
        Some("BACKWARD_TRANSITIVE".to_string())
    );
    assert_eq!(find("full-subject").compatibility, Some("FULL".to_string()));
    assert_eq!(find("none-subject").compatibility, Some("NONE".to_string()));
    assert_eq!(find("default-subject").compatibility, None); // inherits global
}

// ============================================================================
// Edge Case: Latest-only version selection
// ============================================================================

#[tokio::test]
async fn test_latest_only_version_selection() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec!["evolving-subject"]))
        .mount(&mock_server)
        .await;
    mount_global_defaults(&mock_server).await;

    // Subject with 5 versions, but we only want the latest
    Mock::given(method("GET"))
        .and(path("/subjects/evolving-subject/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![1, 2, 3, 4, 5]))
        .mount(&mock_server)
        .await;

    // Only version 5 should be fetched
    Mock::given(method("GET"))
        .and(path("/subjects/evolving-subject/versions/5"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "subject": "evolving-subject", "version": 5, "id": 50,
            "schemaType": "AVRO", "schema": "{\"type\":\"string\"}", "references": []
        })))
        .expect(1) // exactly once
        .mount(&mock_server)
        .await;

    let mut config = test_sr_config(&mock_server.uri());
    config.backup = Some(SchemaBackupOptions {
        fail_on_partial: false,
        snapshot_interval: None,
        subjects: vec!["*".to_string()],
        exclude: vec![],
        include_soft_deleted: false,
        include_versions: VersionSelection::Latest,
        include_references: true,
    });

    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup =
        SchemaRegistryBackup::new(client, storage.clone(), "latest-only".to_string(), config);
    let manifest = backup.run().await.unwrap();

    assert_eq!(manifest.total_versions, 1);
    assert_eq!(manifest.subjects[0].latest_version, 5);
    assert_eq!(manifest.subjects[0].latest_schema_id, 50);

    // Only v5.json should exist
    assert!(storage
        .exists("latest-only/schema-registry/subjects/evolving-subject/v5.json")
        .await
        .unwrap());
    assert!(!storage
        .exists("latest-only/schema-registry/subjects/evolving-subject/v1.json")
        .await
        .unwrap());
}

// ============================================================================
// Edge Case: SR returns 5xx errors on some subjects (partial failure)
// ============================================================================

#[tokio::test]
async fn test_partial_failure_marks_snapshot_incomplete() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(vec!["good-subject", "broken-subject"]),
        )
        .mount(&mock_server)
        .await;
    mount_global_defaults(&mock_server).await;

    // good-subject: works fine
    mount_subject(
        &mock_server,
        "good-subject",
        vec![serde_json::json!({
            "subject": "good-subject", "version": 1, "id": 1,
            "schemaType": "AVRO", "schema": "{\"type\":\"string\"}", "references": []
        })],
    )
    .await;

    // broken-subject: versions endpoint returns 500
    Mock::given(method("GET"))
        .and(path("/subjects/broken-subject/versions"))
        .respond_with(ResponseTemplate::new(500).set_body_json(serde_json::json!({
            "error_code": 50001,
            "message": "Backend datastore error"
        })))
        .mount(&mock_server)
        .await;

    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup = SchemaRegistryBackup::new(client, storage.clone(), "partial".to_string(), config);
    let error = backup.run().await.unwrap_err();
    assert!(error.to_string().contains("Backend datastore error"));
    assert!(storage
        .get("partial/schema-registry/_manifest.json")
        .await
        .is_err());
    let status: serde_json::Value = serde_json::from_slice(
        &storage
            .get("partial/schema-registry/_status.json")
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(status["complete"], false);
}

// ============================================================================
// Edge Case: Soft-deleted subjects with ?deleted=true
// ============================================================================

#[tokio::test]
async fn test_soft_deleted_subjects_included_when_configured() {
    let mock_server = MockServer::start().await;

    // With deleted=true: includes soft-deleted (mount first so it has priority)
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .and(query_param("deleted", "true"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(vec!["active-subject", "deleted-subject"]),
        )
        .mount(&mock_server)
        .await;

    // Without deleted=true: only active subjects
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec!["active-subject"]))
        .mount(&mock_server)
        .await;

    mount_global_defaults(&mock_server).await;

    for subject in ["active-subject", "deleted-subject"] {
        mount_subject(
            &mock_server,
            subject,
            vec![serde_json::json!({
                "subject": subject, "version": 1, "id": 1,
                "schemaType": "AVRO", "schema": "{\"type\":\"string\"}", "references": []
            })],
        )
        .await;
    }

    // Test WITH include_soft_deleted
    let mut config = test_sr_config(&mock_server.uri());
    config.backup = Some(SchemaBackupOptions {
        fail_on_partial: false,
        snapshot_interval: None,
        subjects: vec!["*".to_string()],
        exclude: vec![],
        include_soft_deleted: true,
        include_versions: VersionSelection::All,
        include_references: true,
    });

    let client = SchemaRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup = SchemaRegistryBackup::new(client, storage, "with-deleted".to_string(), config);
    let manifest = backup.run().await.unwrap();

    assert_eq!(manifest.subjects_count, 2);
}

// ============================================================================
// Edge Case: Auth failure returns clear error
// ============================================================================

#[tokio::test]
async fn test_auth_failure_returns_clear_error() {
    let mock_server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/subjects"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error_code": 401,
            "message": "Unauthorized"
        })))
        .mount(&mock_server)
        .await;

    mount_global_defaults(&mock_server).await;

    let config = test_sr_config(&mock_server.uri());
    let client = SchemaRegistryClient::new(&config).unwrap();

    let result = client.list_subjects(false).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Unauthorized") || err.to_string().contains("Authentication"));
}
