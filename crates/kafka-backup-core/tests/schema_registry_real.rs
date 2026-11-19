//! Schema Registry backup and restore against real Confluent Schema Registry
//! (issue #262, verification items 3 to 6). Needs the compose stack in
//! `tests/schema-registry-test-infra`:
//!
//! ```text
//! SR_VERSION=8.3.1 docker compose -f tests/schema-registry-test-infra/docker-compose.yml up -d --wait
//! SR_VERSION=8.3.1 cargo test -p kafka-backup-core --test schema_registry_real -- --ignored --test-threads=1
//! docker compose -f tests/schema-registry-test-infra/docker-compose.yml down -v
//! ```
//!
//! `tests/schema-registry-test-infra/matrix.sh` runs this for Confluent
//! Platform 6.2.0, 7.6.0, 7.9.2, 8.1.1 and 8.3.1. `SR_VERSION` tells the
//! tests what the registry can do: contexts and the deletion of mode
//! overrides came with 7.0, `metadata` with 7.4, and a stored `ruleSet`
//! needs the rule-set extension the stack configures for 8.x.
//!
//! Every test wipes the registries it uses (hard delete), so the stack can
//! be reused. The tests share the registries and run one at a time.

use std::collections::BTreeMap;
use std::sync::Arc;

use bytes::Bytes;
use kafka_backup_core::schema_registry::backup::SchemaRegistryBackup;
use kafka_backup_core::schema_registry::client::SchemaRegistryClient;
use kafka_backup_core::schema_registry::config::{
    SchemaBackupOptions, SchemaConnectionConfig, SchemaRegistryConfig, SchemaRestoreOptions,
    SchemaRestoreStrategy,
};
use kafka_backup_core::schema_registry::models::{SchemaRegistryManifest, SchemaRestoreReport};
use kafka_backup_core::schema_registry::restore::SchemaRegistryRestore;
use kafka_backup_core::schema_registry::SchemaRegistryError;
use kafka_backup_core::storage::{MemoryBackend, StorageBackend};
use reqwest::Method;
use serde_json::{json, Value};

fn source_url() -> String {
    std::env::var("SR_SOURCE_URL").unwrap_or_else(|_| "http://localhost:28081".to_string())
}

fn target_url() -> String {
    std::env::var("SR_TARGET_URL").unwrap_or_else(|_| "http://localhost:28082".to_string())
}

/// What the registry version under test supports.
#[derive(Debug, Clone, Copy)]
struct Caps {
    /// Schema contexts (`:.ctx:subject`) and `DELETE /mode/{subject}`: 7.0.
    contexts_and_mode_delete: bool,
    /// `metadata` on a schema version: 7.4.
    metadata: bool,
    /// A stored `ruleSet`: 8.x with the rule-set resource extension.
    rule_sets: bool,
}

fn caps() -> Caps {
    let version = std::env::var("SR_VERSION").unwrap_or_else(|_| "8.3.1".to_string());
    let mut parts = version.split('.').map(|p| p.parse::<u32>().unwrap_or(0));
    let (major, minor) = (parts.next().unwrap_or(0), parts.next().unwrap_or(0));
    Caps {
        contexts_and_mode_delete: major >= 7,
        metadata: (major, minor) >= (7, 4),
        rule_sets: major >= 8,
    }
}

static LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

// ---------------------------------------------------------------------------
// A small Schema Registry HTTP helper
// ---------------------------------------------------------------------------

struct Sr {
    url: String,
    http: reqwest::Client,
}

#[derive(Debug, Clone, PartialEq)]
struct VersionState {
    id: i64,
    schema_type: String,
    schema: String,
    references: Value,
    metadata: Value,
    rule_set: Value,
}

#[derive(Debug, Clone, PartialEq)]
struct SubjectState {
    all: Vec<i64>,
    live: Vec<i64>,
    versions: BTreeMap<i64, VersionState>,
    compatibility: Option<String>,
    mode: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
struct RegistryState {
    subjects: BTreeMap<String, SubjectState>,
    /// Context -> (compatibility, mode) overrides.
    contexts: BTreeMap<String, (Option<String>, Option<String>)>,
    global_compatibility: Value,
    global_mode: Value,
}

fn enc(s: &str) -> String {
    urlencoding::encode(s).into_owned()
}

impl Sr {
    fn new(url: &str) -> Self {
        Sr {
            url: url.to_string(),
            http: reqwest::Client::new(),
        }
    }

    async fn call(&self, method: Method, path: &str, body: Option<Value>) -> (u16, Value) {
        let mut req = self
            .http
            .request(method, format!("{}{}", self.url, path))
            .header("Content-Type", "application/vnd.schemaregistry.v1+json");
        if let Some(body) = body {
            req = req.json(&body);
        }
        let resp = req.send().await.unwrap_or_else(|e| {
            panic!(
                "{} unreachable ({e}); is the compose stack in tests/schema-registry-test-infra up?",
                self.url
            )
        });
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap();
        (
            status,
            serde_json::from_str(&text).unwrap_or(Value::String(text)),
        )
    }

    async fn ok(&self, method: Method, path: &str, body: Option<Value>) -> Value {
        let (status, value) = self.call(method.clone(), path, body).await;
        assert!(
            (200..300).contains(&status),
            "{method} {}{path}: {status} {value}",
            self.url
        );
        value
    }

    async fn get(&self, path: &str) -> Option<Value> {
        let (status, value) = self.call(Method::GET, path, None).await;
        match status {
            200 => Some(value),
            404 => None,
            other => panic!("GET {}{path}: {other} {value}", self.url),
        }
    }

    async fn register(&self, subject: &str, body: Value) -> i64 {
        self.ok(
            Method::POST,
            &format!("/subjects/{}/versions", enc(subject)),
            Some(body),
        )
        .await["id"]
            .as_i64()
            .unwrap()
    }

    async fn set_mode(&self, scope: Option<&str>, mode: &str) {
        let path = match scope {
            Some(s) => format!("/mode/{}?force=true", enc(s)),
            None => "/mode?force=true".to_string(),
        };
        self.ok(Method::PUT, &path, Some(json!({"mode": mode})))
            .await;
    }

    async fn set_compat(&self, scope: Option<&str>, compat: &str) {
        let path = match scope {
            Some(s) => format!("/config/{}", enc(s)),
            None => "/config".to_string(),
        };
        self.ok(Method::PUT, &path, Some(json!({"compatibility": compat})))
            .await;
    }

    async fn subjects(&self, deleted: bool) -> Vec<String> {
        let path = if deleted {
            "/subjects?deleted=true"
        } else {
            "/subjects"
        };
        serde_json::from_value(self.get(path).await.unwrap()).unwrap()
    }

    /// Hard-delete everything and reset modes and compatibility.
    async fn wipe(&self) {
        self.set_mode(None, "READWRITE").await;
        if let Some(contexts) = self.get("/contexts").await {
            for ctx in contexts.as_array().unwrap() {
                let ctx = ctx.as_str().unwrap();
                if ctx != "." {
                    let scope = enc(&format!(":{ctx}:"));
                    self.call(Method::DELETE, &format!("/mode/{scope}"), None)
                        .await;
                    self.call(Method::DELETE, &format!("/config/{scope}"), None)
                        .await;
                }
            }
        }
        // Referenced schemas cannot be deleted before their referrers, so
        // repeat until nothing is left.
        for _ in 0..10 {
            let subjects = self.subjects(true).await;
            if subjects.is_empty() {
                break;
            }
            self.delete_subjects(subjects).await;
        }
        self.set_compat(None, "BACKWARD").await;
        assert!(
            self.subjects(true).await.is_empty(),
            "{} not wiped",
            self.url
        );
    }

    async fn delete_subjects(&self, subjects: Vec<String>) {
        for subject in subjects {
            let s = enc(&subject);
            self.set_mode(Some(&subject), "READWRITE").await;
            self.call(Method::DELETE, &format!("/subjects/{s}"), None)
                .await;
            self.call(
                Method::DELETE,
                &format!("/subjects/{s}?permanent=true"),
                None,
            )
            .await;
            self.call(Method::DELETE, &format!("/mode/{s}"), None).await;
            self.call(Method::DELETE, &format!("/config/{s}"), None)
                .await;
        }
    }

    async fn state(&self) -> RegistryState {
        let mut subjects = BTreeMap::new();
        let mut contexts = BTreeMap::new();
        for subject in self.subjects(true).await {
            let s = enc(&subject);
            let all: Vec<i64> = serde_json::from_value(
                self.get(&format!("/subjects/{s}/versions?deleted=true"))
                    .await
                    .unwrap(),
            )
            .unwrap();
            let live: Vec<i64> = self
                .get(&format!("/subjects/{s}/versions"))
                .await
                .map(|v| serde_json::from_value(v).unwrap())
                .unwrap_or_default();
            let mut versions = BTreeMap::new();
            for v in &all {
                let sv = self
                    .get(&format!("/subjects/{s}/versions/{v}?deleted=true"))
                    .await
                    .unwrap();
                versions.insert(
                    *v,
                    VersionState {
                        id: sv["id"].as_i64().unwrap(),
                        schema_type: sv["schemaType"].as_str().unwrap_or("AVRO").to_string(),
                        schema: sv["schema"].as_str().unwrap().to_string(),
                        references: sv.get("references").cloned().unwrap_or_else(|| json!([])),
                        metadata: sv.get("metadata").cloned().unwrap_or(Value::Null),
                        rule_set: sv.get("ruleSet").cloned().unwrap_or(Value::Null),
                    },
                );
            }
            let compatibility = self
                .get(&format!("/config/{s}"))
                .await
                .and_then(|c| c["compatibilityLevel"].as_str().map(String::from));
            let mode = self
                .get(&format!("/mode/{s}"))
                .await
                .and_then(|m| m["mode"].as_str().map(String::from));
            if let Some(ctx) = context_of(&subject) {
                let scope = enc(&format!(":{ctx}:"));
                let compat = self
                    .get(&format!("/config/{scope}"))
                    .await
                    .and_then(|c| c["compatibilityLevel"].as_str().map(String::from));
                let mode = self
                    .get(&format!("/mode/{scope}"))
                    .await
                    .and_then(|m| m["mode"].as_str().map(String::from));
                contexts.insert(ctx, (compat, mode));
            }
            subjects.insert(
                subject,
                SubjectState {
                    all,
                    live,
                    versions,
                    compatibility,
                    mode,
                },
            );
        }
        RegistryState {
            subjects,
            contexts,
            global_compatibility: self.get("/config").await.unwrap()["compatibilityLevel"].clone(),
            global_mode: self.get("/mode").await.unwrap()["mode"].clone(),
        }
    }

    async fn schema_by_id(&self, id: i64) -> Value {
        self.get(&format!("/schemas/ids/{id}")).await.unwrap()
    }
}

fn context_of(subject: &str) -> Option<String> {
    let rest = subject.strip_prefix(":.")?;
    let end = rest.find(':')?;
    Some(subject[1..end + 2].to_string())
}

// ---------------------------------------------------------------------------
// Source data
// ---------------------------------------------------------------------------

const ADDRESS: &str = r#"{"type":"record","name":"Address","namespace":"com.example","fields":[{"name":"street","type":"string"}]}"#;
const ORDER_V1: &str = r#"{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"id","type":"string"},{"name":"address","type":"com.example.Address"}]}"#;
const ORDER_V2: &str = r#"{"type":"record","name":"Order","namespace":"com.example","fields":[{"name":"id","type":"string"},{"name":"address","type":"com.example.Address"},{"name":"note","type":"string","default":""}]}"#;

fn address_ref() -> Value {
    json!([{"name": "com.example.Address", "subject": "address-value", "version": 1}])
}

/// Avro with cross-subject references, Protobuf with an import and nested
/// messages, JSON Schema with $ref, a soft-deleted version, a soft-deleted
/// subject, a subject-level compatibility, a READONLY subject and, where
/// the registry version has them, a context with its own IDs and
/// compatibility and metadata/ruleSet on one version.
async fn seed_source(sr: &Sr) {
    sr.wipe().await;
    sr.register("address-value", json!({"schema": ADDRESS}))
        .await;
    sr.register(
        "orders-value",
        json!({"schema": ORDER_V1, "references": address_ref()}),
    )
    .await;
    let mut order_v2 = json!({"schema": ORDER_V2, "references": address_ref()});
    if caps().metadata {
        order_v2["metadata"] = json!({"properties": {"owner": "orders-team", "pii": "false"}});
    }
    if caps().rule_sets {
        order_v2["ruleSet"] = json!({"domainRules": [{
            "name": "id-present", "kind": "CONDITION", "mode": "WRITE",
            "type": "CEL", "expr": "message.id != ''"
        }]});
    }
    sr.register("orders-value", order_v2).await;
    sr.set_compat(Some("orders-value"), "FULL").await;

    sr.register(
        "payments-value",
        json!({"schema": r#"{"type":"record","name":"Payment","fields":[]}"#}),
    )
    .await;
    sr.register(
        "payments-value",
        json!({"schema": r#"{"type":"record","name":"Payment","fields":[{"name":"amount","type":"long","default":0}]}"#}),
    )
    .await;
    sr.ok(Method::DELETE, "/subjects/payments-value/versions/1", None)
        .await;

    sr.register(
        "legacy-value",
        json!({"schema": r#"{"type":"record","name":"Legacy","fields":[]}"#}),
    )
    .await;
    sr.ok(Method::DELETE, "/subjects/legacy-value", None).await;

    sr.register("readonly-value", json!({"schema": r#""string""#}))
        .await;
    sr.set_mode(Some("readonly-value"), "READONLY").await;

    sr.register(
        "common.proto",
        json!({"schemaType": "PROTOBUF", "schema": "syntax = \"proto3\";\npackage common;\n\nmessage Money {\n  int64 units = 1;\n  string currency = 2;\n}\n"}),
    )
    .await;
    sr.register(
        "events-value",
        json!({
            "schemaType": "PROTOBUF",
            "schema": "syntax = \"proto3\";\npackage events;\n\nimport \"common.proto\";\n\nmessage Event {\n  repeated Line lines = 1;\n  string id = 2;\n\n  message Line {\n    common.Money price = 1;\n    string sku = 2;\n  }\n}\n",
            "references": [{"name": "common.proto", "subject": "common.proto", "version": 1}]
        }),
    )
    .await;

    sr.register(
        "json-defs",
        json!({"schemaType": "JSON", "schema": r#"{"$id":"https://example.com/defs.json","type":"object","properties":{"name":{"type":"string"}}}"#}),
    )
    .await;
    sr.register(
        "json-value",
        json!({
            "schemaType": "JSON",
            "schema": r#"{"type":"object","properties":{"customer":{"$ref":"https://example.com/defs.json"}}}"#,
            "references": [{"name": "https://example.com/defs.json", "subject": "json-defs", "version": 1}]
        }),
    )
    .await;

    if caps().contexts_and_mode_delete {
        sr.register(":.ctx-a:address-value", json!({"schema": ADDRESS}))
            .await;
        sr.register(
            ":.ctx-a:orders-value",
            json!({
                "schema": r#"{"type":"record","name":"CtxOrder","namespace":"com.example","fields":[{"name":"address","type":"com.example.Address"}]}"#,
                // Unqualified: resolves in :.ctx-a:.
                "references": address_ref()
            }),
        )
        .await;
        sr.set_compat(Some(":.ctx-a:"), "FORWARD").await;
    }
}

fn sr_config(
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
            timeout_ms: 10_000,
            max_retries: 1,
            retry_backoff_ms: 50,
            rate_limit_rps: 500,
        }),
    }
}

async fn backup(
    storage: &Arc<MemoryBackend>,
    backup_id: &str,
    options: SchemaBackupOptions,
) -> SchemaRegistryManifest {
    let cfg = sr_config(&source_url(), Some(options), None);
    SchemaRegistryBackup::new(
        SchemaRegistryClient::new(&cfg).unwrap(),
        storage.clone(),
        backup_id.to_string(),
        cfg,
    )
    .run()
    .await
    .unwrap()
}

async fn restore(
    url: &str,
    storage: &Arc<MemoryBackend>,
    backup_id: &str,
    options: SchemaRestoreOptions,
) -> (SchemaRestoreReport, Option<SchemaRegistryError>) {
    let cfg = sr_config(url, None, Some(options));
    SchemaRegistryRestore::new(
        SchemaRegistryClient::new(&cfg).unwrap(),
        storage.clone(),
        backup_id.to_string(),
        cfg,
    )
    .run_detailed()
    .await
}

fn into_existing() -> SchemaRestoreOptions {
    SchemaRestoreOptions {
        strategy: SchemaRestoreStrategy::IntoExisting,
        ..Default::default()
    }
}

/// No mode left behind: global as expected, no context or subject override
/// other than `expected_subject_modes`.
async fn assert_modes(sr: &Sr, global: &str, expected_subject_modes: &[(&str, &str)]) {
    assert_eq!(sr.get("/mode").await.unwrap()["mode"], global);
    assert!(
        sr.get(&format!("/mode/{}", enc(":.ctx-a:")))
            .await
            .is_none(),
        "context mode left behind"
    );
    let expected: BTreeMap<&str, &str> = expected_subject_modes.iter().copied().collect();
    for subject in sr.subjects(true).await {
        let mode = sr
            .get(&format!("/mode/{}", enc(&subject)))
            .await
            .map(|m| m["mode"].as_str().unwrap().to_string());
        assert_eq!(
            mode.as_deref(),
            expected.get(subject.as_str()).copied(),
            "mode of {subject}"
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// The registry under test is older than 7.0: only the 6.2 test applies.
fn older_than_7(test: &str) -> bool {
    let old = !caps().contexts_and_mode_delete;
    if old {
        eprintln!("{test}: not applicable before Schema Registry 7.0 (see the 6.2 test)");
    }
    old
}

/// Verification item 3: every (subject, version) comes back with its
/// original ID, schema, references and deleted state; compatibility and
/// modes match; a second run changes nothing.
#[tokio::test]
#[ignore = "requires the compose stack in tests/schema-registry-test-infra"]
async fn preserve_ids_into_an_empty_target_matches_the_source_exactly() {
    if older_than_7("preserve_ids_into_an_empty_target_matches_the_source_exactly") {
        return;
    }
    let _lock = LOCK.lock().await;
    let (source, target) = (Sr::new(&source_url()), Sr::new(&target_url()));
    seed_source(&source).await;
    target.wipe().await;

    let storage = Arc::new(MemoryBackend::new());
    let manifest = backup(&storage, "e2e-preserve", SchemaBackupOptions::default()).await;
    assert_eq!(manifest.soft_deleted_versions, 2, "payments v1 + legacy v1");
    assert_eq!(manifest.subjects_count, 11);
    let sealed = manifest.snapshot.as_ref().expect("checksums");
    assert!(sealed.complete && sealed.full_registry);
    assert_eq!(
        sealed.objects.len(),
        1 + manifest.subjects_count + manifest.total_versions
    );

    let (report, err) = restore(&target_url(), &storage, "e2e-preserve", Default::default()).await;
    assert!(err.is_none(), "{err:?}\n{report:#?}");
    let verification = report.verification.as_ref().unwrap();
    assert!(verification.mismatches.is_empty(), "{verification:?}");
    assert_eq!(verification.versions_checked, manifest.total_versions);
    assert!(report.warnings.is_empty(), "{:?}", report.warnings);
    assert_eq!(report.versions_registered, manifest.total_versions);
    assert_eq!(report.versions_soft_deleted, 2);

    let expected = source.state().await;
    let actual = target.state().await;
    assert_eq!(expected, actual);
    // Spot checks on what the comparison covers.
    let orders = &actual.subjects["orders-value"];
    assert_eq!(orders.compatibility.as_deref(), Some("FULL"));
    assert_eq!(
        orders.versions[&2].references[0]["subject"],
        "address-value"
    );
    assert_eq!(
        orders.versions[&2].metadata["properties"]["owner"],
        "orders-team"
    );
    if caps().rule_sets {
        assert!(orders.versions[&2].rule_set["domainRules"].is_array());
    }
    assert_eq!(actual.subjects["payments-value"].live, vec![2]);
    assert_eq!(actual.subjects["payments-value"].all, vec![1, 2]);
    assert!(actual.subjects["legacy-value"].live.is_empty());
    assert_eq!(
        actual.subjects["readonly-value"].mode.as_deref(),
        Some("READONLY")
    );
    assert_eq!(
        actual.contexts[".ctx-a"],
        (Some("FORWARD".to_string()), None)
    );
    // The context has its own ID space: its IDs overlap the default one's.
    assert_eq!(
        actual.subjects[":.ctx-a:address-value"].versions[&1].id,
        expected.subjects[":.ctx-a:address-value"].versions[&1].id
    );
    // Not left in IMPORT anywhere.
    assert_modes(&target, "READWRITE", &[("readonly-value", "READONLY")]).await;

    // A second run changes nothing.
    let (report, err) = restore(&target_url(), &storage, "e2e-preserve", Default::default()).await;
    assert!(err.is_none(), "{err:?}\n{report:#?}");
    assert_eq!(report.versions_registered, 0, "{report:#?}");
    assert_eq!(report.versions_skipped, manifest.total_versions);
    assert_eq!(report.versions_soft_deleted, 0);
    assert_eq!(target.state().await, expected);
    assert_modes(&target, "READWRITE", &[("readonly-value", "READONLY")]).await;
}

/// preserve_ids refuses a target that has subjects and does not already
/// hold the backup, without changing it.
#[tokio::test]
#[ignore = "requires the compose stack in tests/schema-registry-test-infra"]
async fn preserve_ids_refuses_a_populated_target() {
    if older_than_7("preserve_ids_refuses_a_populated_target") {
        return;
    }
    let _lock = LOCK.lock().await;
    let (source, target) = (Sr::new(&source_url()), Sr::new(&target_url()));
    seed_source(&source).await;
    target.wipe().await;
    target
        .register("someone-elses-value", json!({"schema": r#""string""#}))
        .await;
    let before = target.state().await;

    let storage = Arc::new(MemoryBackend::new());
    backup(&storage, "e2e-populated", SchemaBackupOptions::default()).await;
    let (_, err) = restore(&target_url(), &storage, "e2e-populated", Default::default()).await;
    let err = err.expect("refused");
    assert!(err.to_string().contains("needs an empty target"), "{err}");
    assert_eq!(target.state().await, before, "the target changed");
}

/// Verification item 4, first half: identical entries are skipped and free
/// IDs are imported with their original IDs under subject-level IMPORT.
#[tokio::test]
#[ignore = "requires the compose stack in tests/schema-registry-test-infra"]
async fn into_existing_skips_identical_entries_and_imports_free_ids() {
    if older_than_7("into_existing_skips_identical_entries_and_imports_free_ids") {
        return;
    }
    let _lock = LOCK.lock().await;
    let (source, target) = (Sr::new(&source_url()), Sr::new(&target_url()));
    seed_source(&source).await;
    target.wipe().await;

    // Without the context: its IDs overlap the default context's, which
    // would make a record-level rewrite ambiguous (refused; unit-tested).
    let storage = Arc::new(MemoryBackend::new());
    let manifest = backup(
        &storage,
        "e2e-import",
        SchemaBackupOptions {
            exclude: vec![":.ctx-a:*".to_string()],
            ..Default::default()
        },
    )
    .await;
    let src = source.state().await;
    let address_v1 = src.subjects["address-value"].versions[&1].id;

    // The populated target: an unrelated subject, and one version that is
    // identical to the backup's (same ID, same schema).
    target.set_mode(None, "IMPORT").await;
    target
        .register(
            "address-value",
            json!({"schema": ADDRESS, "id": address_v1, "version": 1}),
        )
        .await;
    target
        .register(
            "unrelated-value",
            json!({"schema": r#""double""#, "id": 7001, "version": 1}),
        )
        .await;
    target.set_mode(None, "READWRITE").await;
    let unrelated = target.state().await.subjects["unrelated-value"].clone();

    // No conflicts: the identical version is skipped, every other ID is free
    // and imported with its original ID.
    let (report, err) = restore(&target_url(), &storage, "e2e-import", into_existing()).await;
    assert!(err.is_none(), "{err:?}\n{report:#?}");
    assert_eq!(report.versions_skipped, 1, "{report:#?}");
    assert_eq!(report.versions_registered, manifest.total_versions - 1);
    assert!(report.id_mapping.is_empty(), "{:?}", report.id_mapping);
    let after = target.state().await;
    for (subject, state) in &src.subjects {
        if subject.starts_with(":.") {
            continue;
        }
        assert_eq!(&after.subjects[subject], state, "{subject}");
    }
    assert_eq!(after.subjects["unrelated-value"], unrelated);
    // into_existing keeps the target's global settings and leaves no IMPORT.
    assert_modes(&target, "READWRITE", &[("readonly-value", "READONLY")]).await;
}

/// Verification item 4, second half: a conflicting ID fails in strict mode
/// with no changes, and `rewrite_ids` registers the conflicting schemas
/// under new IDs. (That the restored records then carry the new IDs is
/// checked end to end in `tests/schema-registry-test-infra/e2e`.)
#[tokio::test]
#[ignore = "requires the compose stack in tests/schema-registry-test-infra"]
async fn into_existing_conflicts_fail_strictly_and_rewrite_ids_succeeds() {
    if older_than_7("into_existing_conflicts_fail_strictly_and_rewrite_ids_succeeds") {
        return;
    }
    let _lock = LOCK.lock().await;
    let (source, target) = (Sr::new(&source_url()), Sr::new(&target_url()));
    seed_source(&source).await;
    target.wipe().await;

    // Without the context: its IDs overlap the default context's, which
    // would make a record-level rewrite ambiguous (refused; unit-tested).
    let storage = Arc::new(MemoryBackend::new());
    backup(
        &storage,
        "e2e-existing",
        SchemaBackupOptions {
            exclude: vec![":.ctx-a:*".to_string()],
            ..Default::default()
        },
    )
    .await;
    let src = source.state().await;
    let payments_v1 = src.subjects["payments-value"].versions[&1].id;
    let orders_v1 = src.subjects["orders-value"].versions[&1].id;
    let orders_v2 = src.subjects["orders-value"].versions[&2].id;

    // Collisions, made deterministic with explicit IDs: payments v1's ID
    // holds another schema, and orders-value v1 is a different schema.
    target.set_mode(None, "IMPORT").await;
    target
        .register(
            "squatter-value",
            json!({"schema": r#""bytes""#, "id": payments_v1, "version": 1}),
        )
        .await;
    target
        .register(
            "orders-value",
            json!({"schema": r#""long""#, "id": 9001, "version": 1}),
        )
        .await;
    target.set_mode(None, "READWRITE").await;
    // The target's own setting, so the rewritten Order versions can append.
    target.set_compat(Some("orders-value"), "NONE").await;
    let before = target.state().await;

    // Strict (default): refused before any change, the same way every time.
    let mut messages = Vec::new();
    for _ in 0..2 {
        let (report, err) = restore(&target_url(), &storage, "e2e-existing", into_existing()).await;
        let err = err.expect("strict conflicts");
        assert!(
            matches!(err, SchemaRegistryError::RestoreConflict(_)),
            "{err}"
        );
        assert_eq!(report.conflicts.len(), 2, "{:?}", report.conflicts);
        assert_eq!(target.state().await, before, "the target changed");
        messages.push(err.to_string());
    }
    assert_eq!(messages[0], messages[1]);
    assert!(messages[0].contains(&format!("orders-value v1 (ID {orders_v1})")));
    assert!(messages[0].contains(&format!("payments-value v1 (ID {payments_v1})")));

    // rewrite_ids: registered with new IDs, mapped.
    let (report, err) = restore(
        &target_url(),
        &storage,
        "e2e-existing",
        SchemaRestoreOptions {
            rewrite_ids: true,
            ..into_existing()
        },
    )
    .await;
    assert!(err.is_none(), "{err:?}\n{report:#?}");
    for id in [payments_v1, orders_v1, orders_v2] {
        assert!(
            report.id_mapping.contains_key(&(id as i32)),
            "{id} not mapped: {:?}",
            report.id_mapping
        );
    }
    // Every source ID resolves, in the target, to the source's schema:
    // through the map when it changed, as is otherwise.
    for (subject, state) in &src.subjects {
        if subject.starts_with(":.") {
            continue;
        }
        for version in state.versions.values() {
            let target_id = report
                .id_mapping
                .get(&(version.id as i32))
                .map(|t| *t as i64)
                .unwrap_or(version.id);
            assert_eq!(
                target.schema_by_id(target_id).await["schema"],
                source.schema_by_id(version.id).await["schema"],
                "{subject} ID {} -> {target_id}",
                version.id
            );
        }
    }
    // The target's own versions are untouched.
    let after = target.state().await;
    assert_eq!(after.subjects["orders-value"].versions[&1].id, 9001);
    assert_eq!(
        after.subjects["squatter-value"],
        before.subjects["squatter-value"]
    );
    assert_modes(&target, "READWRITE", &[("readonly-value", "READONLY")]).await;
}

/// Make one backed-up schema invalid so its registration fails mid-restore.
async fn break_json_value(storage: &Arc<MemoryBackend>, backup_id: &str) {
    let key = format!("{backup_id}/schema-registry/subjects/json-value/v1.json");
    let mut version: Value = serde_json::from_slice(&storage.get(&key).await.unwrap()).unwrap();
    version["schema"] = json!("{ this is not a schema");
    storage
        .put(&key, Bytes::from(serde_json::to_vec(&version).unwrap()))
        .await
        .unwrap();
    // Seal the invalid schema so the test reaches the registry's rejection
    // after the integrity check. Unsealed tampering fails before any change
    // (covered by the mocked suite).
    let manifest_key = format!("{backup_id}/schema-registry/_manifest.json");
    let mut manifest: SchemaRegistryManifest =
        serde_json::from_slice(&storage.get(&manifest_key).await.unwrap()).unwrap();
    let info = manifest.snapshot.take().unwrap();
    let keys = info.objects.keys().cloned().collect();
    manifest.snapshot = Some(
        kafka_backup_core::schema_registry::snapshot::seal(storage.as_ref(), keys, info)
            .await
            .unwrap(),
    );
    storage
        .put(
            &manifest_key,
            Bytes::from(serde_json::to_vec(&manifest).unwrap()),
        )
        .await
        .unwrap();
}

/// Verification item 5 at engine level: a restore that stops after some
/// registrations puts the target's modes back, and rerunning it with
/// `into_existing` completes with no duplicate versions.
#[tokio::test]
#[ignore = "requires the compose stack in tests/schema-registry-test-infra"]
async fn an_interrupted_restore_puts_the_modes_back_and_resumes_with_into_existing() {
    if older_than_7("an_interrupted_restore_puts_the_modes_back_and_resumes_with_into_existing") {
        return;
    }
    let _lock = LOCK.lock().await;
    let (source, target) = (Sr::new(&source_url()), Sr::new(&target_url()));
    seed_source(&source).await;
    let src = source.state().await;

    // preserve_ids: global and context IMPORT are put back.
    target.wipe().await;
    let storage = Arc::new(MemoryBackend::new());
    let good = Arc::new(MemoryBackend::new());
    backup(&storage, "e2e-fail", SchemaBackupOptions::default()).await;
    backup(&good, "e2e-fail", SchemaBackupOptions::default()).await;
    break_json_value(&storage, "e2e-fail").await;
    let (report, err) = restore(&target_url(), &storage, "e2e-fail", Default::default()).await;
    let err = err.expect("the broken schema must fail");
    assert!(err.to_string().contains("json-value"), "{err}");
    assert!(report.versions_registered > 0, "failed before registering");
    assert!(report.modes_rolled_back, "{report:#?}");
    assert_modes(&target, "READWRITE", &[]).await;

    // Cancelled part-way (what SIGINT / SIGTERM do): the same guarantee.
    target.wipe().await;
    let cancel = tokio_util::sync::CancellationToken::new();
    let cfg = sr_config(&target_url(), None, Some(SchemaRestoreOptions::default()));
    // Slow enough to be cancelled between registrations.
    let mut slow = cfg.clone();
    slow.connection.as_mut().unwrap().rate_limit_rps = 20;
    let engine = SchemaRegistryRestore::new(
        SchemaRegistryClient::new(&slow).unwrap(),
        good.clone(),
        "e2e-fail".to_string(),
        slow,
    );
    let trigger = cancel.clone();
    let watcher = Sr::new(&target_url());
    let canceller = tokio::spawn(async move {
        // Cancel once the target holds some, but not all, of the subjects.
        loop {
            if watcher.subjects(true).await.len() >= 3 {
                trigger.cancel();
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    });
    let (report, err) = engine.run_with_cancel(cancel).await;
    canceller.await.unwrap();
    assert!(
        matches!(err, Some(SchemaRegistryError::Cancelled(_))),
        "{err:?}"
    );
    assert!(report.cancelled && report.modes_rolled_back, "{report:#?}");
    let partial = target.state().await;
    assert!(
        (3..src.subjects.len()).contains(&partial.subjects.len()),
        "{} of {} subjects",
        partial.subjects.len(),
        src.subjects.len()
    );
    assert_modes(&target, "READWRITE", &[]).await;

    // Rerun with into_existing: completes, nothing registered twice, the
    // target equals the source and its modes are the backup's.
    let (report, err) = restore(&target_url(), &good, "e2e-fail", into_existing()).await;
    assert!(err.is_none(), "{err:?}\n{report:#?}");
    assert!(report.versions_skipped > 0, "{report:#?}");
    let resumed = target.state().await;
    assert_eq!(
        resumed.subjects, src.subjects,
        "no duplicates, nothing missing"
    );
    assert_modes(&target, "READWRITE", &[("readonly-value", "READONLY")]).await;

    // into_existing failing mid-way: a target subject's own READONLY comes
    // back, the IMPORT overrides on new subjects are removed.
    target.wipe().await;
    target.set_mode(None, "IMPORT").await;
    target.register("address-value", json!({"schema": ADDRESS, "id": src.subjects["address-value"].versions[&1].id, "version": 1})).await;
    target
        .register(
            "orders-value",
            json!({
                "schema": ORDER_V1, "references": address_ref(),
                "id": src.subjects["orders-value"].versions[&1].id, "version": 1
            }),
        )
        .await;
    target.set_mode(None, "READWRITE").await;
    target.set_mode(Some("orders-value"), "READONLY").await;
    let (report, err) = restore(&target_url(), &storage, "e2e-fail", into_existing()).await;
    assert!(err.is_some());
    assert!(report.modes_rolled_back, "{report:#?}");
    // orders-value v2 got in under IMPORT before the failure.
    assert!(target.state().await.subjects["orders-value"]
        .versions
        .contains_key(&2));
    assert_modes(&target, "READWRITE", &[("orders-value", "READONLY")]).await;
}

/// Verification item 6, Confluent Platform 6.2: mode overrides cannot be
/// deleted there, so a restore that needs a temporary subject-level IMPORT
/// on a subject with an inherited mode is refused before any change. A
/// `preserve_ids` restore into an empty registry needs only the global
/// IMPORT mode and works.
#[tokio::test]
#[ignore = "requires the compose stack in tests/schema-registry-test-infra"]
async fn schema_registry_6_2_refuses_what_it_could_not_clean_up() {
    if caps().contexts_and_mode_delete {
        eprintln!(
            "schema_registry_6_2_refuses_what_it_could_not_clean_up: only for SR_VERSION 6.x"
        );
        return;
    }
    let _lock = LOCK.lock().await;
    let (source, target) = (Sr::new(&source_url()), Sr::new(&target_url()));
    seed_source(&source).await;
    let src = source.state().await;
    let storage = Arc::new(MemoryBackend::new());
    let manifest = backup(&storage, "e2e-62", SchemaBackupOptions::default()).await;
    assert_eq!(manifest.subjects_count, 9);
    assert_eq!(manifest.soft_deleted_versions, 2);

    // into_existing into a populated registry (whose own schema uses an ID
    // the backup does not): refused, nothing changed.
    target.set_mode(None, "IMPORT").await;
    target
        .register(
            "someone-elses-value",
            json!({"schema": r#""double""#, "id": 7001, "version": 1}),
        )
        .await;
    target.set_mode(None, "READWRITE").await;
    let before = target.state().await;
    for dry_run in [true, false] {
        let (report, err) = restore(
            &target_url(),
            &storage,
            "e2e-62",
            SchemaRestoreOptions {
                dry_run,
                ..into_existing()
            },
        )
        .await;
        let err = err.expect("refused");
        assert!(
            matches!(err, SchemaRegistryError::RestoreConflict(_)),
            "{err}"
        );
        assert!(
            err.to_string().contains("cannot delete mode overrides"),
            "{err}"
        );
        assert!(!report.success);
        assert_eq!(target.state().await, before, "the target changed");
    }

    // preserve_ids into an empty registry: original IDs, versions, deleted
    // state, compatibility and modes; global mode back to READWRITE.
    target
        .ok(Method::DELETE, "/subjects/someone-elses-value", None)
        .await;
    target
        .ok(
            Method::DELETE,
            "/subjects/someone-elses-value?permanent=true",
            None,
        )
        .await;
    let (report, err) = restore(&target_url(), &storage, "e2e-62", Default::default()).await;
    assert!(err.is_none(), "{err:?}\n{report:#?}");
    assert_eq!(report.versions_registered, manifest.total_versions);
    assert!(report.verification.as_ref().unwrap().mismatches.is_empty());
    let actual = target.state().await;
    assert_eq!(actual.subjects, src.subjects);
    assert_eq!(actual.global_mode, json!("READWRITE"));
    assert_eq!(actual.global_compatibility, src.global_compatibility);
}
