//! Schema Registry restore against a stateful fake registry (wiremock).
//!
//! Each test backs up a fake source registry into memory storage with the
//! real backup engine, then restores it into a fake target. The fake models
//! the registry rules the restore depends on (see
//! `support/fake_schema_registry.rs`), so wrong ordering, a missing IMPORT,
//! an overwrite or a mode left behind show up as real failures or state.

use std::sync::Arc;

use kafka_backup_core::schema_registry::backup::SchemaRegistryBackup;
use kafka_backup_core::schema_registry::client::SchemaRegistryClient;
use kafka_backup_core::schema_registry::config::{
    SchemaBackupOptions, SchemaConnectionConfig, SchemaRegistryConfig, SchemaRestoreOptions,
    SchemaRestoreStrategy, VersionSelection,
};
use kafka_backup_core::schema_registry::models::{
    SchemaRegistryManifest, SchemaRestoreAction, SchemaRestoreReport, SubjectMetadata,
};
use kafka_backup_core::schema_registry::restore::{report_key, SchemaRegistryRestore};
use kafka_backup_core::schema_registry::SchemaRegistryError;
use kafka_backup_core::storage::{MemoryBackend, StorageBackend};
use serde_json::json;
use wiremock::MockServer;

use super::fake_schema_registry::{FakeSchemaRegistry, Schema};

pub const BACKUP: &str = "sr-backup";

pub fn sr_config(
    url: &str,
    backup: Option<SchemaBackupOptions>,
    restore: Option<SchemaRestoreOptions>,
) -> SchemaRegistryConfig {
    SchemaRegistryConfig {
        enabled: true,
        url: url.to_string(),
        auth: None,
        tls: None,
        backup,
        restore,
        connection: Some(SchemaConnectionConfig {
            timeout_ms: 5_000,
            max_retries: 0,
            retry_backoff_ms: 1,
            rate_limit_rps: 10_000,
        }),
    }
}

pub async fn backup_with(
    source: &MockServer,
    storage: &Arc<MemoryBackend>,
    options: SchemaBackupOptions,
) -> SchemaRegistryManifest {
    let cfg = sr_config(&source.uri(), Some(options), None);
    SchemaRegistryBackup::new(
        SchemaRegistryClient::new(&cfg).unwrap(),
        storage.clone(),
        BACKUP.to_string(),
        cfg,
    )
    .run()
    .await
    .unwrap()
}

pub async fn backup(source: &MockServer, storage: &Arc<MemoryBackend>) -> SchemaRegistryManifest {
    backup_with(source, storage, SchemaBackupOptions::default()).await
}

pub async fn restore(
    target: &MockServer,
    storage: &Arc<MemoryBackend>,
    options: SchemaRestoreOptions,
) -> (SchemaRestoreReport, Option<SchemaRegistryError>) {
    let cfg = sr_config(&target.uri(), None, Some(options));
    SchemaRegistryRestore::new(
        SchemaRegistryClient::new(&cfg).unwrap(),
        storage.clone(),
        BACKUP.to_string(),
        cfg,
    )
    .run_detailed()
    .await
}

pub fn preserve_ids() -> SchemaRestoreOptions {
    SchemaRestoreOptions::default()
}

pub fn into_existing() -> SchemaRestoreOptions {
    SchemaRestoreOptions {
        strategy: SchemaRestoreStrategy::IntoExisting,
        ..Default::default()
    }
}

pub fn avro_record(name: &str, fields: &str) -> String {
    format!(r#"{{"type":"record","name":"{name}","namespace":"com.example","fields":[{fields}]}}"#)
}

pub const ADDRESS: &str = r#"{"type":"record","name":"Address","namespace":"com.example","fields":[{"name":"street","type":"string"}]}"#;

/// The source registry used by most tests.
pub fn seed_source(sr: &FakeSchemaRegistry) {
    // Avro with references, crossing several versions.
    sr.seed("address-value", 1, 1, Schema::avro(ADDRESS));
    sr.seed(
        "orders-value",
        1,
        2,
        Schema::avro(&avro_record(
            "Order",
            r#"{"name":"id","type":"string"},{"name":"address","type":"com.example.Address"}"#,
        ))
        .with_ref("com.example.Address", "address-value", 1),
    );
    let mut orders_v2 = Schema::avro(&avro_record(
        "Order",
        r#"{"name":"id","type":"string"},{"name":"address","type":"com.example.Address"},{"name":"note","type":"string","default":""}"#,
    ))
    .with_ref("com.example.Address", "address-value", 1);
    orders_v2.metadata = Some(json!({"properties": {"owner": "orders-team"}}));
    orders_v2.rule_set = Some(json!({"domainRules": [
        {"name": "id-present", "kind": "CONDITION", "mode": "WRITE", "type": "CEL", "expr": "message.id != ''"}
    ]}));
    sr.seed("orders-value", 2, 3, orders_v2);
    sr.set_config("orders-value", "FULL");

    // A soft-deleted version.
    sr.seed(
        "payments-value",
        1,
        4,
        Schema::avro(&avro_record("Payment", "")),
    );
    sr.seed(
        "payments-value",
        2,
        5,
        Schema::avro(&avro_record(
            "Payment",
            r#"{"name":"amount","type":"long","default":0}"#,
        )),
    );
    sr.soft_delete("payments-value", 1);

    // A soft-deleted subject.
    sr.seed(
        "legacy-value",
        1,
        6,
        Schema::avro(&avro_record("Legacy", "")),
    );
    sr.seed(
        "legacy-value",
        2,
        7,
        Schema::avro(&avro_record(
            "Legacy",
            r#"{"name":"x","type":"int","default":0}"#,
        )),
    );
    sr.soft_delete("legacy-value", 1);
    sr.soft_delete("legacy-value", 2);

    // A READONLY subject.
    sr.seed("readonly-value", 1, 8, Schema::avro(r#""string""#));
    sr.set_mode("readonly-value", "READONLY");

    // Protobuf with an import and nested messages.
    sr.seed(
        "common.proto",
        1,
        9,
        Schema::typed(
            "syntax = \"proto3\";\npackage common;\nmessage Money { int64 units = 1; string currency = 2; }\n",
            "PROTOBUF",
        ),
    );
    sr.seed(
        "events-value",
        1,
        10,
        Schema::typed(
            "syntax = \"proto3\";\npackage events;\nimport \"common.proto\";\nmessage Event { message Line { common.Money price = 1; } repeated Line lines = 1; }\n",
            "PROTOBUF",
        )
        .with_ref("common.proto", "common.proto", 1),
    );

    // JSON Schema with $ref.
    sr.seed(
        "json-defs",
        1,
        11,
        Schema::typed(
            r#"{"$id":"defs.json","type":"object","properties":{"name":{"type":"string"}}}"#,
            "JSON",
        ),
    );
    sr.seed(
        "json-value",
        1,
        12,
        Schema::typed(
            r#"{"type":"object","properties":{"customer":{"$ref":"defs.json"}}}"#,
            "JSON",
        )
        .with_ref("defs.json", "json-defs", 1),
    );

    // A context with its own ID space: ID 2 here is not orders-value v1.
    sr.seed(":.ctx-a:address-value", 1, 1, Schema::avro(ADDRESS));
    sr.seed(
        ":.ctx-a:orders-value",
        1,
        2,
        Schema::avro(&avro_record(
            "CtxOrder",
            r#"{"name":"address","type":"com.example.Address"}"#,
        ))
        // Unqualified: resolves in the referrer's context.
        .with_ref("com.example.Address", "address-value", 1),
    );
    sr.set_config(":.ctx-a:", "FORWARD");
}

pub async fn source() -> (MockServer, FakeSchemaRegistry) {
    let (server, sr) = FakeSchemaRegistry::start().await;
    seed_source(&sr);
    (server, sr)
}

pub fn assert_same_registry(source: &FakeSchemaRegistry, target: &FakeSchemaRegistry) {
    assert_eq!(
        source.snapshot(),
        target.snapshot(),
        "subjects/versions/IDs"
    );
    assert_eq!(source.modes(), target.modes(), "subject and context modes");
    assert_eq!(
        source.configs(),
        target.configs(),
        "subject and context compatibility"
    );
    assert_eq!(source.global_mode(), target.global_mode(), "global mode");
    assert_eq!(
        source.global_compat(),
        target.global_compat(),
        "global compat"
    );
}

// ===========================================================================
// Backup
// ===========================================================================

#[tokio::test]
async fn backup_captures_soft_deleted_versions_by_default() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    let manifest = backup(&src, &storage).await;

    // payments v1 and legacy v1, v2.
    assert_eq!(manifest.soft_deleted_versions, 3);
    let meta: SubjectMetadata = serde_json::from_slice(
        &storage
            .get("sr-backup/schema-registry/subjects/payments-value/_metadata.json")
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(meta.versions, vec![1, 2]);
    assert_eq!(meta.soft_deleted_versions, vec![1]);
    assert!(!meta.is_soft_deleted);

    let legacy: SubjectMetadata = serde_json::from_slice(
        &storage
            .get("sr-backup/schema-registry/subjects/legacy-value/_metadata.json")
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(legacy.is_soft_deleted);
    assert_eq!(legacy.soft_deleted_versions, vec![1, 2]);
}

#[tokio::test]
async fn backup_without_soft_deleted_skips_them() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    let manifest = backup_with(
        &src,
        &storage,
        SchemaBackupOptions {
            include_soft_deleted: false,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(manifest.soft_deleted_versions, 0);
    assert!(manifest.subjects.iter().all(|s| s.name != "legacy-value"));
    let payments = manifest
        .subjects
        .iter()
        .find(|s| s.name == "payments-value")
        .unwrap();
    assert_eq!(payments.version_count, 1);
}

#[tokio::test]
async fn backup_captures_contexts_and_their_settings() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    let manifest = backup(&src, &storage).await;
    assert!(manifest
        .subjects
        .iter()
        .any(|s| s.name == ":.ctx-a:orders-value"));
    let global: serde_json::Value = serde_json::from_slice(
        &storage
            .get("sr-backup/schema-registry/_global_config.json")
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(global["contexts"][".ctx-a"]["compatibility"], "FORWARD");
}

#[tokio::test]
async fn backup_follows_references_outside_the_filter() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    let manifest = backup_with(
        &src,
        &storage,
        SchemaBackupOptions {
            subjects: vec!["orders-value".to_string()],
            include_versions: VersionSelection::Latest,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(manifest.subjects_discovered, 1);
    assert_eq!(manifest.referenced_subjects, vec!["address-value"]);
    let address: SubjectMetadata = serde_json::from_slice(
        &storage
            .get("sr-backup/schema-registry/subjects/address-value/_metadata.json")
            .await
            .unwrap(),
    )
    .unwrap();
    assert!(address.included_as_reference);
    assert_eq!(address.versions, vec![1]);

    // And the restore resolves the reference.
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let (report, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(report.versions_registered, 2);
    assert_eq!(target.snapshot()["orders-value"][&2].0, 3);
}

#[tokio::test]
async fn backup_without_reference_following_leaves_them_out() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    let manifest = backup_with(
        &src,
        &storage,
        SchemaBackupOptions {
            subjects: vec!["orders-value".to_string()],
            include_references: false,
            ..Default::default()
        },
    )
    .await;
    assert_eq!(manifest.subjects_count, 1);
    assert!(manifest.referenced_subjects.is_empty());
}

#[tokio::test]
async fn crossing_references_back_up_and_restore() {
    // Subject level a <-> b is a cycle; version level is not. The old
    // subject-level sort failed the whole backup with ReferenceCycle.
    let (src, sr) = FakeSchemaRegistry::start().await;
    sr.seed("a", 1, 1, Schema::avro(&avro_record("A", "")));
    sr.seed("b", 1, 2, Schema::avro(&avro_record("B", "")));
    sr.seed(
        "a",
        2,
        3,
        Schema::avro(&avro_record("A2", r#"{"name":"b","type":"com.example.B"}"#)).with_ref(
            "com.example.B",
            "b",
            1,
        ),
    );
    sr.seed(
        "b",
        2,
        4,
        Schema::avro(&avro_record("B2", r#"{"name":"a","type":"com.example.A"}"#)).with_ref(
            "com.example.A",
            "a",
            1,
        ),
    );
    let storage = Arc::new(MemoryBackend::new());
    let manifest = backup(&src, &storage).await;
    assert_eq!(manifest.total_versions, 4);

    let (tgt, target) = FakeSchemaRegistry::start().await;
    let (_, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(sr.snapshot(), target.snapshot());
}

// ===========================================================================
// preserve_ids
// ===========================================================================

#[tokio::test]
async fn preserve_ids_into_an_empty_target_reproduces_the_source() {
    let (src, sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    let (report, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_none(), "{err:?}");
    assert!(report.success);
    assert_same_registry(&sr, &target);

    // Not left in IMPORT, globally or for the context.
    assert_eq!(target.global_mode(), "READWRITE");
    assert!(!target.modes().contains_key(":.ctx-a:"));

    // metadata and ruleSet travel with the schema.
    let orders_v2 = target.schema_of("orders-value", 2);
    assert_eq!(
        orders_v2.metadata.unwrap()["properties"]["owner"],
        "orders-team"
    );
    assert!(orders_v2.rule_set.unwrap()["domainRules"].is_array());

    assert_eq!(report.versions_registered, 14);
    assert_eq!(report.versions_soft_deleted, 3);
    assert!(report.id_mapping.is_empty());
    assert!(report.rewrite_mapping().is_none());
    let verification = report.verification.unwrap();
    assert_eq!(verification.versions_checked, 14);
    assert!(verification.mismatches.is_empty());

    // Subjects without a mode in the source never get a subject-level mode:
    // the restore uses global and context IMPORT.
    let subject_mode_puts: Vec<String> = target
        .log()
        .into_iter()
        .filter(|l| l.starts_with("PUT /mode/") && !l.starts_with("PUT /mode/:.ctx-a:"))
        .collect();
    assert_eq!(subject_mode_puts, vec!["PUT /mode/readonly-value"]);

    // The report is in storage.
    let stored: SchemaRestoreReport =
        serde_json::from_slice(&storage.get(&report_key(BACKUP, false)).await.unwrap()).unwrap();
    assert!(stored.success);
    assert_eq!(stored.items.len(), 14);
}

#[tokio::test]
async fn a_target_that_drops_the_rule_set_is_reported() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.with(|st| st.drops_rule_set = true);
    let (report, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_none(), "{err:?}");
    let warnings = report.warnings.join("\n");
    assert!(
        warnings.contains("orders-value v2: the target dropped the ruleSet"),
        "{warnings}"
    );
    assert!(!warnings.contains("metadata"), "{warnings}");
}

#[tokio::test]
async fn references_are_registered_before_their_referrers() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let (_, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_none(), "{err:?}");
    let registrations: Vec<String> = target
        .log()
        .into_iter()
        .filter(|l| l.starts_with("POST") && l.ends_with("/versions"))
        .collect();
    let pos = |s: &str| {
        registrations
            .iter()
            .position(|l| l == &format!("POST /subjects/{s}/versions"))
            .unwrap_or_else(|| panic!("{s} not registered: {registrations:?}"))
    };
    assert!(pos("address-value") < pos("orders-value"));
    assert!(pos("common.proto") < pos("events-value"));
    assert!(pos("json-defs") < pos("json-value"));
    assert!(pos(":.ctx-a:address-value") < pos(":.ctx-a:orders-value"));
}

#[tokio::test]
async fn preserve_ids_refuses_a_populated_target_before_changing_it() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.seed("unrelated-value", 1, 100, Schema::avro(r#""int""#));
    let (report, err) = restore(&tgt, &storage, preserve_ids()).await;
    let err = err.expect("must refuse");
    assert!(err.to_string().contains("into_existing"), "{err}");
    assert!(target.mutations().is_empty(), "{:?}", target.mutations());
    assert!(!report.success);
}

#[tokio::test]
async fn preserve_ids_rerun_is_a_noop() {
    let (src, sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let (_, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_none());
    target.clear_log();

    let (report, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(report.versions_registered, 0);
    assert_eq!(report.versions_skipped, 14);
    assert!(!target
        .log()
        .iter()
        .any(|l| l.starts_with("POST") && l.ends_with("/versions")));
    assert_same_registry(&sr, &target);
}

#[tokio::test]
async fn unknown_source_global_mode_puts_the_targets_prior_mode_back() {
    // Defect: with no global mode in the backup the target stayed in IMPORT.
    let (src, sr) = source().await;
    sr.with(|st| st.global_mode_available = false);
    let storage = Arc::new(MemoryBackend::new());
    let manifest = backup(&src, &storage).await;
    assert!(manifest.global_mode.is_none());

    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.with(|st| st.global_mode = "READONLY".to_string());
    let (_, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(target.global_mode(), "READONLY");
    assert!(target.log().contains(&"PUT /mode".to_string()));
}

#[tokio::test]
async fn a_failure_mid_restore_puts_every_mode_back() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.fail_once("POST", "/subjects/json-value/versions", 500, 50001);
    let (report, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_some());
    assert_eq!(target.global_mode(), "READWRITE");
    assert!(target.modes().is_empty(), "{:?}", target.modes());
    assert!(report.modes_rolled_back);
    assert!(report.mode_restore_errors.is_empty());
    // Some versions were registered before the failure.
    assert!(report.versions_registered > 0);

    let stored: SchemaRestoreReport =
        serde_json::from_slice(&storage.get(&report_key(BACKUP, false)).await.unwrap()).unwrap();
    assert!(!stored.success);
    assert!(stored.error.unwrap().contains("json-value"));

    // into_existing resumes the partial restore.
    let (report, err) = restore(&tgt, &storage, into_existing()).await;
    assert!(err.is_none(), "{err:?}");
    assert!(report.versions_skipped > 0);
}

#[tokio::test]
async fn soft_deleted_versions_can_be_left_out() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let (report, err) = restore(
        &tgt,
        &storage,
        SchemaRestoreOptions {
            restore_soft_deleted: false,
            ..preserve_ids()
        },
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(report.versions_soft_deleted, 0);
    let snapshot = target.snapshot();
    assert!(!snapshot.contains_key("legacy-value"));
    assert_eq!(
        snapshot["payments-value"]
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![2]
    );
}

#[tokio::test]
async fn rewrite_ids_is_rejected_with_preserve_ids() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let (_, err) = restore(
        &tgt,
        &storage,
        SchemaRestoreOptions {
            rewrite_ids: true,
            ..preserve_ids()
        },
    )
    .await;
    assert!(
        matches!(err, Some(SchemaRegistryError::Config(_))),
        "{err:?}"
    );
    assert!(target.mutations().is_empty());
}

// ===========================================================================
// into_existing
// ===========================================================================

#[tokio::test]
async fn into_existing_skips_identical_and_imports_free_ids_at_subject_level() {
    let (src, sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    // Already there: identical, same ID and version.
    target.seed("address-value", 1, 1, Schema::avro(ADDRESS));
    // An unrelated subject the restore must not touch.
    target.seed("unrelated-value", 1, 500, Schema::avro(r#""int""#));
    target.set_mode("unrelated-value", "READONLY");

    let (report, err) = restore(&tgt, &storage, into_existing()).await;
    assert!(err.is_none(), "{err:?}");
    let address = report
        .items
        .iter()
        .find(|i| i.target_subject == "address-value")
        .unwrap();
    assert_eq!(address.action, SchemaRestoreAction::Skip);
    assert_eq!(report.versions_registered, 13);

    let log = target.log();
    assert!(log.contains(&"PUT /mode/orders-value?force=true".to_string()));
    assert!(!log.contains(&"POST /subjects/address-value/versions".to_string()));
    // No global IMPORT on a shared registry.
    assert!(!log.contains(&"PUT /mode".to_string()));
    assert!(!log.iter().any(|l| l.contains("unrelated-value")));

    // Same versions and IDs as the source for every restored subject.
    let source = sr.snapshot();
    let restored = target.snapshot();
    for (subject, versions) in &source {
        assert_eq!(&restored[subject], versions, "{subject}");
    }
    assert_eq!(target.modes()["unrelated-value"], "READONLY");
}

#[tokio::test]
async fn a_subject_without_a_source_mode_is_cleared_not_forced_to_readwrite() {
    // Defect: the restore set every subject to READWRITE at subject level.
    let (src, sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let (_, err) = restore(&tgt, &storage, into_existing()).await;
    assert!(err.is_none(), "{err:?}");

    // Only the source's own subject-level mode is left behind.
    let modes = target.modes();
    assert_eq!(modes, sr.modes());
    assert!(!target
        .log()
        .iter()
        .any(|l| l.starts_with("PUT /mode/events-value") && !l.contains("force")));
    assert!(target
        .log()
        .contains(&"DELETE /mode/events-value".to_string()));
}

#[tokio::test]
async fn into_existing_restores_a_target_subjects_own_mode_after_a_failure() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.seed("address-value", 1, 1, Schema::avro(ADDRESS));
    target.seed(
        "orders-value",
        1,
        2,
        Schema::avro(&avro_record(
            "Order",
            r#"{"name":"id","type":"string"},{"name":"address","type":"com.example.Address"}"#,
        ))
        .with_ref("com.example.Address", "address-value", 1),
    );
    target.set_mode("orders-value", "READONLY");
    target.fail_once("POST", "/subjects/json-value/versions", 503, 50003);

    let (report, err) = restore(&tgt, &storage, into_existing()).await;
    assert!(err.is_some());
    // orders-value v2 was imported (IMPORT with force over READONLY), then
    // the failure put READONLY back; new subjects lost their IMPORT override.
    assert!(target.snapshot()["orders-value"].contains_key(&2));
    assert_eq!(
        target.modes(),
        [("orders-value".to_string(), "READONLY".to_string())]
            .into_iter()
            .collect()
    );
    assert!(report.modes_rolled_back);
}

#[tokio::test]
async fn an_id_holding_the_identical_schema_is_not_a_conflict() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    // ID 1 holds the Address schema under another subject.
    target.seed("address-copy", 1, 1, Schema::avro(ADDRESS));
    let (report, err) = restore(&tgt, &storage, into_existing()).await;
    assert!(err.is_none(), "{err:?}");
    let address = report
        .items
        .iter()
        .find(|i| i.target_subject == "address-value")
        .unwrap();
    assert_eq!(address.action, SchemaRestoreAction::Register);
    assert_eq!(target.snapshot()["address-value"][&1].0, 1);
}

/// A target where ID 4 (payments v1 in the source) holds another schema and
/// orders-value v1 is a different schema.
fn seed_colliding_target(target: &FakeSchemaRegistry) {
    target.seed("squatter-value", 1, 4, Schema::avro(r#""bytes""#));
    target.seed("orders-value", 1, 50, Schema::avro(r#""long""#));
}

#[tokio::test]
async fn strict_conflicts_fail_deterministically_before_any_change() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;

    let mut messages = Vec::new();
    for _ in 0..2 {
        let (tgt, target) = FakeSchemaRegistry::start().await;
        seed_colliding_target(&target);
        let before = target.snapshot();
        let (report, err) = restore(&tgt, &storage, into_existing()).await;
        let err = err.expect("strict conflicts must fail");
        assert!(
            matches!(err, SchemaRegistryError::RestoreConflict(_)),
            "{err}"
        );
        assert!(target.mutations().is_empty(), "{:?}", target.mutations());
        assert_eq!(
            target.snapshot(),
            before,
            "an occupied version was overwritten"
        );
        assert_eq!(report.conflicts.len(), 2, "{:?}", report.conflicts);
        messages.push(err.to_string());
    }
    assert_eq!(messages[0], messages[1]);
    assert!(
        messages[0].contains("orders-value v1 (ID 2)"),
        "{}",
        messages[0]
    );
    assert!(
        messages[0].contains("payments-value v1 (ID 4)"),
        "{}",
        messages[0]
    );
}

#[tokio::test]
async fn rewrite_ids_registers_new_ids_maps_them_and_follows_renumbered_references() {
    let (src, sr) = source().await;
    // invoice-value references orders-value v1, which the restore renumbers.
    sr.seed(
        "invoice-value",
        1,
        13,
        Schema::avro(&avro_record(
            "Invoice",
            r#"{"name":"order","type":"com.example.Order"}"#,
        ))
        .with_ref("com.example.Order", "orders-value", 1),
    );
    let storage = Arc::new(MemoryBackend::new());
    // Without the context: its ID 2 would make the record rewrite ambiguous
    // (covered by a_rewrite_that_would_be_ambiguous_across_contexts_is_refused).
    backup_with(
        &src,
        &storage,
        SchemaBackupOptions {
            exclude: vec![":.ctx-a:*".to_string()],
            ..Default::default()
        },
    )
    .await;

    for returns_version in [true, false] {
        let (tgt, target) = FakeSchemaRegistry::start().await;
        // Schema Registry 7.x answers a registration with the ID only.
        target.with(|st| st.register_returns_version = returns_version);
        seed_colliding_target(&target);
        let (report, err) = restore(
            &tgt,
            &storage,
            SchemaRestoreOptions {
                rewrite_ids: true,
                ..into_existing()
            },
        )
        .await;
        assert!(err.is_none(), "{err:?}");

        // orders v1 (ID 2): v1 is taken -> registered normally as v2.
        // orders v2 (ID 3): v2 is now taken by that -> registered as v3.
        // payments v1 (ID 4): the ID is taken -> new ID, soft-deleted again.
        let mapping = report.rewrite_mapping().expect("IDs changed");
        for source_id in [2, 3, 4] {
            assert!(mapping.contains_key(&source_id), "{mapping:?}");
        }
        // address-value v1 kept ID 1: the record rewrite (strict by default
        // since kafka-backup-core 0.25) needs it in the map, unchanged.
        assert!(
            !report.id_mapping.contains_key(&1),
            "{:?}",
            report.id_mapping
        );
        assert!(
            report.ids_preserved.contains(&1),
            "{:?}",
            report.ids_preserved
        );
        assert_eq!(mapping.get(&1), Some(&1), "{mapping:?}");
        assert!(
            mapping
                .iter()
                .all(|(s, t)| report.id_mapping.get(s).unwrap_or(s) == t),
            "{mapping:?}"
        );
        let orders = &target.snapshot()["orders-value"];
        assert_eq!(orders[&1].0, 50, "the target's own v1 is untouched");
        assert_eq!(orders[&2].0, mapping[&2]);
        assert_eq!(orders[&3].0, mapping[&3]);
        let payments = &target.snapshot()["payments-value"];
        let (_, (id, deleted, _)) = payments
            .iter()
            .find(|(_, (id, _, _))| *id == mapping[&4])
            .unwrap();
        assert_eq!(*id, mapping[&4]);
        assert!(deleted);

        // The referrer now points at the renumbered version.
        let invoice = target.schema_of("invoice-value", 1);
        assert_eq!(
            invoice.references,
            vec![(
                "com.example.Order".to_string(),
                "orders-value".to_string(),
                2
            )]
        );
        assert_eq!(report.versions_rewritten, 3);
        assert!(report.verification.unwrap().mismatches.is_empty());
    }
}

#[tokio::test]
async fn rewrite_ids_maps_an_identical_version_under_another_id() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    // readonly-value v1 is the same schema but under ID 77.
    target.seed("readonly-value", 1, 77, Schema::avro(r#""string""#));
    let (report, err) = restore(
        &tgt,
        &storage,
        SchemaRestoreOptions {
            rewrite_ids: true,
            ..into_existing()
        },
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    let item = report
        .items
        .iter()
        .find(|i| i.target_subject == "readonly-value")
        .unwrap();
    assert_eq!(item.action, SchemaRestoreAction::MapExisting);
    assert_eq!(report.id_mapping.get(&8), Some(&77));
}

#[tokio::test]
async fn a_rewrite_that_would_be_ambiguous_across_contexts_is_refused() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    // ID 2 in the default context holds another schema; ID 2 is also used
    // by :.ctx-a:orders-value, so one global record rewrite cannot be right.
    target.seed("squatter-value", 1, 2, Schema::avro(r#""bytes""#));
    let (_, err) = restore(
        &tgt,
        &storage,
        SchemaRestoreOptions {
            rewrite_ids: true,
            ..into_existing()
        },
    )
    .await;
    let err = err.expect("must refuse");
    assert!(err.to_string().contains("another context"), "{err}");
    assert!(target.mutations().is_empty());
}

#[tokio::test]
async fn subject_mapping_moves_subjects_and_their_references() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup_with(
        &src,
        &storage,
        SchemaBackupOptions {
            subjects: vec!["orders-value".into(), "address-value".into()],
            ..Default::default()
        },
    )
    .await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let (_, err) = restore(
        &tgt,
        &storage,
        SchemaRestoreOptions {
            subject_mapping: [("address-value".to_string(), "shared-address".to_string())]
                .into_iter()
                .collect(),
            ..preserve_ids()
        },
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    let orders = target.schema_of("orders-value", 1);
    assert_eq!(orders.references[0].1, "shared-address");
    assert_eq!(target.snapshot()["shared-address"][&1].0, 1);
}

// ===========================================================================
// Capacity, dry run
// ===========================================================================

#[tokio::test]
async fn capacity_pre_check_fails_before_any_change() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.seed("existing-value", 1, 900, Schema::avro(r#""int""#));
    let (report, err) = restore(
        &tgt,
        &storage,
        SchemaRestoreOptions {
            max_schema_versions: Some(14),
            ..into_existing()
        },
    )
    .await;
    let err = err.expect("over capacity");
    assert!(
        matches!(err, SchemaRegistryError::CapacityExceeded(_)),
        "{err}"
    );
    let capacity = report.capacity.unwrap();
    assert_eq!(
        (capacity.used, capacity.required, capacity.limit),
        (1, 14, 14)
    );
    assert!(target.mutations().is_empty());
}

#[tokio::test]
async fn a_registry_limit_hit_during_the_restore_is_reported_and_modes_restored() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.with(|st| st.max_versions = Some(4));
    let (report, err) = restore(&tgt, &storage, preserve_ids()).await;
    let err = err.expect("limit hit");
    assert!(
        matches!(err, SchemaRegistryError::CapacityExceeded(_)),
        "{err}"
    );
    assert!(err.to_string().contains("Schema Limit Exceeded"));
    assert_eq!(target.global_mode(), "READWRITE");
    assert!(target.modes().is_empty());
    assert!(report.modes_rolled_back);
}

#[tokio::test]
async fn dry_run_plans_without_changing_the_target() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.seed("address-value", 1, 1, Schema::avro(ADDRESS));
    let (report, err) = restore(
        &tgt,
        &storage,
        SchemaRestoreOptions {
            dry_run: true,
            ..into_existing()
        },
    )
    .await;
    assert!(err.is_none(), "{err:?}");
    assert!(report.dry_run);
    assert!(target.mutations().is_empty());
    let count = |a| report.items.iter().filter(|i| i.action == a).count();
    assert_eq!(count(SchemaRestoreAction::Skip), 1);
    assert_eq!(count(SchemaRestoreAction::Register), 13);
    // Written to the dry-run key, not over a real report.
    assert!(storage.get(&report_key(BACKUP, true)).await.is_ok());
    assert!(storage.get(&report_key(BACKUP, false)).await.is_err());
}

#[tokio::test]
async fn dry_run_reports_conflicts_and_fails() {
    let (src, _sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let (tgt, target) = FakeSchemaRegistry::start().await;
    seed_colliding_target(&target);
    let (report, err) = restore(
        &tgt,
        &storage,
        SchemaRestoreOptions {
            dry_run: true,
            ..into_existing()
        },
    )
    .await;
    assert!(matches!(err, Some(SchemaRegistryError::RestoreConflict(_))));
    assert_eq!(
        report
            .items
            .iter()
            .filter(|i| i.action == SchemaRestoreAction::Conflict)
            .count(),
        2
    );
    assert!(target.mutations().is_empty());
}

#[tokio::test]
async fn a_pre_0_7_backup_restores() {
    // Old backups have no soft_deleted_versions, contexts or new manifest
    // fields; they must still restore.
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let storage = Arc::new(MemoryBackend::new());
    let put = |key: &str, value: serde_json::Value| {
        let storage = storage.clone();
        let key = key.to_string();
        async move {
            storage
                .put(
                    &key,
                    bytes::Bytes::from(serde_json::to_vec(&value).unwrap()),
                )
                .await
                .unwrap()
        }
    };
    put(
        "sr-backup/schema-registry/_manifest.json",
        json!({
            "schema_registry_url": "http://old", "backup_timestamp": "2026-01-01T00:00:00Z",
            "global_compatibility": "BACKWARD", "global_mode": "READWRITE",
            "subjects": [{"name": "orders-value", "schema_type": "AVRO", "version_count": 1,
                "latest_version": 1, "latest_schema_id": 7, "compatibility": "FULL",
                "mode": null, "has_references": false}],
            "subjects_count": 1, "total_versions": 1, "dependency_order": ["orders-value"]
        }),
    )
    .await;
    put(
        "sr-backup/schema-registry/_global_config.json",
        json!({"compatibility_level": "BACKWARD", "mode": "READWRITE"}),
    )
    .await;
    put(
        "sr-backup/schema-registry/subjects/orders-value/_metadata.json",
        json!({"subject": "orders-value", "schema_type": "AVRO", "versions": [1],
               "compatibility": "FULL", "mode": null, "is_soft_deleted": false}),
    )
    .await;
    put(
        "sr-backup/schema-registry/subjects/orders-value/v1.json",
        json!({"subject": "orders-value", "version": 1, "id": 7, "schemaType": "AVRO",
               "schema": "\"string\"", "references": []}),
    )
    .await;
    let (_, err) = restore(&tgt, &storage, preserve_ids()).await;
    assert!(err.is_none(), "{err:?}");
    assert_eq!(target.snapshot()["orders-value"][&1].0, 7);
    assert_eq!(target.configs()["orders-value"], "FULL");
}

#[tokio::test]
async fn validation_preview_is_read_only_in_registry_and_backup_storage() {
    let (src, _) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup(&src, &storage).await;
    let before = storage.list("").await.unwrap();
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let cfg = sr_config(&tgt.uri(), None, Some(preserve_ids()));
    let (report, error) = SchemaRegistryRestore::new(
        SchemaRegistryClient::new(&cfg).unwrap(),
        storage.clone(),
        BACKUP.into(),
        cfg,
    )
    .preview()
    .await;
    assert!(error.is_none(), "{error:?}");
    assert!(report.success && report.dry_run);
    assert!(target.with(|state| state.subjects.is_empty()));
    assert_eq!(storage.list("").await.unwrap(), before);
}

#[tokio::test]
async fn incomplete_or_tampered_snapshot_cannot_mutate_the_target() {
    for incomplete in [false, true] {
        let (src, _) = source().await;
        let storage = Arc::new(MemoryBackend::new());
        backup(&src, &storage).await;
        if incomplete {
            storage
                .put(
                    "sr-backup/schema-registry/_status.json",
                    bytes::Bytes::from_static(br#"{"complete":false}"#),
                )
                .await
                .unwrap();
        } else {
            let key = "sr-backup/schema-registry/subjects/orders-value/v1.json";
            let mut value: serde_json::Value =
                serde_json::from_slice(&storage.get(key).await.unwrap()).unwrap();
            value["schema"] = json!("\"bytes\"");
            storage
                .put(key, bytes::Bytes::from(serde_json::to_vec(&value).unwrap()))
                .await
                .unwrap();
        }
        let (tgt, target) = FakeSchemaRegistry::start().await;
        let (_, error) = restore(&tgt, &storage, preserve_ids()).await;
        assert!(error.is_some(), "incomplete/corrupt snapshot was accepted");
        assert!(target.mutations().is_empty());
    }
}

/// Kafka Backup Enterprise 0.8 wrote the same layout with a few more fields
/// (`archive_scope` in the snapshot block, a bare `{"complete":true}`
/// marker, a `group` on unresolved coordinates). Such a snapshot restores.
#[tokio::test]
async fn a_snapshot_in_the_enterprise_0_8_format_restores() {
    let (src, source_sr) = source().await;
    let storage = Arc::new(MemoryBackend::new());
    backup_with(
        &src,
        &storage,
        SchemaBackupOptions {
            // Leaves unresolved references behind, as Enterprise recorded them.
            subjects: vec!["orders-value".to_string()],
            include_references: false,
            ..Default::default()
        },
    )
    .await;

    let manifest_key = format!("{BACKUP}/schema-registry/_manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&storage.get(&manifest_key).await.unwrap()).unwrap();
    let unresolved = manifest["snapshot"]["unresolved_dependencies"]
        .as_array()
        .unwrap()
        .clone();
    assert!(!unresolved.is_empty(), "the fixture needs a reference");
    assert!(unresolved.iter().all(|c| c["group"] == ""));
    manifest["snapshot"]["archive_scope"] = serde_json::Value::Null;
    storage
        .put(
            &manifest_key,
            bytes::Bytes::from(serde_json::to_vec(&manifest).unwrap()),
        )
        .await
        .unwrap();
    storage
        .put(
            &format!("{BACKUP}/schema-registry/_status.json"),
            bytes::Bytes::from_static(br#"{"complete":true}"#),
        )
        .await
        .unwrap();

    // The referenced subject is already in the target, as the missing
    // references require.
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let address = source_sr.snapshot()["address-value"][&1].clone();
    target.seed("address-value", 1, address.0, Schema::avro(ADDRESS));
    let (report, err) = restore(&tgt, &storage, into_existing()).await;
    assert!(err.is_none(), "{err:?}\n{report:#?}");
    assert_eq!(
        target.snapshot()["orders-value"],
        source_sr.snapshot()["orders-value"]
    );
}
