//! Planner and record preflight contracts; compatibility claims live in the
//! separately required real-service/SDK suite.
use bytes::Bytes;
use kafka_backup_core::{
    config::SchemaRegistryConfig,
    registry_restore::{
        engine::{RegistryRestore, Target},
        snapshot::{Coordinate, Reference, Schema, Settings, Snapshot, SnapshotInfo},
        wire::{PayloadProfile, ProtobufFrame, Serializer, TopicProfile},
        IdPolicy, Provider, RestoreConfig,
    },
};
use kafka_backup_core::{
    config::{RestoreOptions, TopicSelection},
    manifest::{BackupManifest, BackupRecord, TopicBackup},
    storage::{MemoryBackend, StorageBackend},
};
use serde_json::json;
use std::{collections::BTreeMap, sync::Arc};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, ResponseTemplate,
};
fn schema() -> Schema {
    Schema {
        coordinate: Coordinate::subject("event-value", 1),
        id: 7,
        content_id: None,
        schema_type: "AVRO".into(),
        content: "\"string\"".into(),
        references: vec![],
        deleted: Some(false),
        state: None,
        envelope: BTreeMap::new(),
    }
}
fn snapshot() -> Snapshot {
    Snapshot {
        provider: Provider::Confluent,
        source_url: "http://source".into(),
        api_version: "confluent-v1".into(),
        schemas: vec![schema()],
        settings: vec![Settings {
            group: "".into(),
            name: "event-value".into(),
            config: None,
            mode: None,
        }],
        global: json!({"config":{"compatibilityLevel":"BACKWARD"},"mode":"READWRITE"}),
        groups: BTreeMap::new(),
        artifacts: BTreeMap::new(),
        info: Some(SnapshotInfo {
            format_version: 2,
            complete: true,
            provider: "confluent".into(),
            api_version: "confluent-v1".into(),
            ..Default::default()
        }),
        digest: "snapshot-test-digest".into(),
        prefix: "test/schema-registry".into(),
    }
}
fn config() -> RestoreConfig {
    let p = PayloadProfile {
        source: Serializer::Confluent {
            namespace: "".into(),
        },
        target: Serializer::Confluent {
            namespace: "".into(),
        },
        protobuf_source: ProtobufFrame::Indexes,
        protobuf_target: ProtobufFrame::Indexes,
        message: None,
        protobuf_source_type_header: None,
        protobuf_target_type_header: None,
    };
    RestoreConfig {
        serializers: BTreeMap::from([(
            "t".into(),
            TopicProfile {
                key: PayloadProfile {
                    source: Serializer::Raw,
                    target: Serializer::Raw,
                    ..p.clone()
                },
                value: p,
            },
        )]),
        ..Default::default()
    }
}
async fn server() -> MockServer {
    let s = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(json!({"error_code":40401,"message":"missing"})),
        )
        .with_priority(100)
        .mount(&s)
        .await;
    Mock::given(method("OPTIONS"))
        .respond_with(
            ResponseTemplate::new(200).insert_header("Allow", "GET, PUT, DELETE, OPTIONS"),
        )
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/mode"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"mode":"READWRITE"})))
        .mount(&s)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/metadata/version"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"version":"8.3.1"})))
        .mount(&s)
        .await;
    s
}
fn target(server: &MockServer) -> Target {
    let cfg: SchemaRegistryConfig = serde_json::from_value(
        json!({"url":server.uri(),"connection":{"max_retries":0,"rate_limit_rps":10000}}),
    )
    .unwrap();
    Target::confluent(&cfg).unwrap()
}
fn engine(
    snapshot: Snapshot,
    cfg: RestoreConfig,
    s: &MockServer,
    storage: Arc<dyn StorageBackend>,
    cp: &std::path::Path,
) -> RegistryRestore {
    RegistryRestore::new(snapshot, cfg, target(s), storage, s.uri(), Some(cp)).unwrap()
}
async fn no_mutations(s: &MockServer) {
    assert!(s
        .received_requests()
        .await
        .unwrap()
        .iter()
        .all(|r| matches!(r.method.as_str(), "GET" | "OPTIONS")));
}

#[tokio::test]
async fn legacy_rewrite_rejects_unavailable_semantics_before_mutation() {
    let s = server().await;
    let mut snapshot = snapshot();
    snapshot.info = None;
    let temp = tempfile::tempdir().unwrap();
    let mut config = config();
    config.id_policy = IdPolicy::Rewrite;
    let e = engine(
        snapshot,
        config,
        &s,
        Arc::new(MemoryBackend::new()),
        &temp.path().join("cp"),
    );
    let p = e.plan().await.unwrap();
    assert!(p.conflicts.iter().any(|c| c.contains("legacy")));
    assert!(e.execute(&p).await.is_err());
    no_mutations(&s).await;
}
#[tokio::test]
async fn missing_exact_reference_version_is_a_preflight_conflict() {
    let s = server().await;
    let mut snapshot = snapshot();
    snapshot.schemas[0].references.push(Reference {
        name: "missing".into(),
        coordinate: Coordinate::subject("dependency", 2),
    });
    let temp = tempfile::tempdir().unwrap();
    let e = engine(
        snapshot,
        config(),
        &s,
        Arc::new(MemoryBackend::new()),
        &temp.path().join("cp"),
    );
    let p = e.plan().await.unwrap();
    assert!(p
        .conflicts
        .iter()
        .any(|c| c.contains("Unresolved dependency")));
    assert!(e.execute(&p).await.is_err());
    no_mutations(&s).await;
}
#[tokio::test]
async fn populated_subject_cannot_be_forced_into_import() {
    let s = server().await;
    Mock::given(method("GET"))
        .and(path("/subjects/event-value/versions"))
        .respond_with(ResponseTemplate::new(200).set_body_json(vec![2]))
        .mount(&s)
        .await;
    let temp = tempfile::tempdir().unwrap();
    let e = engine(
        snapshot(),
        config(),
        &s,
        Arc::new(MemoryBackend::new()),
        &temp.path().join("cp"),
    );
    let p = e.plan().await.unwrap();
    assert!(p.conflicts.iter().any(|c| c.contains("Populated subject")));
    assert!(e.execute(&p).await.is_err());
    no_mutations(&s).await;
}
#[tokio::test]
async fn conflicting_global_id_fails_before_any_import_transition() {
    let s = server().await;
    Mock::given(method("GET"))
        .and(path("/schemas/ids/7"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"schema":"\"long\""})))
        .mount(&s)
        .await;
    let temp = tempfile::tempdir().unwrap();
    let e = engine(
        snapshot(),
        config(),
        &s,
        Arc::new(MemoryBackend::new()),
        &temp.path().join("cp"),
    );
    let p = e.plan().await.unwrap();
    assert!(p.conflicts.iter().any(|c| c.contains("Global schema ID")));
    assert!(e.execute(&p).await.is_err());
    no_mutations(&s).await;
}
#[tokio::test]
async fn explicitly_configured_serializer_rejects_unknown_records_before_mutation() {
    let s = server().await;
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let record = BackupRecord {
        key: None,
        value: Some(vec![0, 0, 0, 0, 8, 2, 65]),
        headers: vec![],
        timestamp: 123,
        offset: 42,
    };
    storage
        .put(
            "test/t/0/segment.json",
            Bytes::from(serde_json::to_vec(&vec![record]).unwrap()),
        )
        .await
        .unwrap();
    // Use serde to keep this fixture compatible with evolving OSS manifests.
    let mut manifest = BackupManifest::new("test".into());
    let topic:TopicBackup=serde_json::from_value(json!({"name":"t","partitions":[{"partition_id":0,"segments":[{"key":"test/t/0/segment.json","partition_id":0,"start_offset":42,"end_offset":42,"record_count":1,"size_bytes":100,"start_timestamp":123,"end_timestamp":123}]}]})).unwrap();
    manifest.topics.push(topic);
    storage
        .put(
            "test/manifest.json",
            Bytes::from(serde_json::to_vec(&manifest).unwrap()),
        )
        .await
        .unwrap();
    let temp = tempfile::tempdir().unwrap();
    let e = engine(snapshot(), config(), &s, storage, &temp.path().join("cp"));
    let error = e
        .preflight_records(
            "test",
            &RestoreOptions::default(),
            &TopicSelection::default(),
        )
        .await
        .unwrap_err();
    assert!(format!("{error:#}").contains("Unmapped registry identifier"));
    no_mutations(&s).await;
}

#[tokio::test]
async fn cleanup_failure_is_journaled_and_resume_never_registers_twice() {
    cleanup_scenario(false, false).await;
}

#[tokio::test]
async fn cleanup_preserves_concurrent_configuration_and_blocks_replay() {
    cleanup_scenario(true, false).await;
}

#[tokio::test]
async fn cancellation_recovers_import_mode_and_reconciles_a_committed_write() {
    cleanup_scenario(false, true).await;
}

async fn cleanup_scenario(concurrent_config: bool, cancellation: bool) {
    use std::sync::atomic::{AtomicBool, Ordering};
    let s = server().await;
    let registered = Arc::new(AtomicBool::new(false));
    let importing = Arc::new(AtomicBool::new(false));
    let fail_cleanup = Arc::new(AtomicBool::new(!concurrent_config && !cancellation));
    let cancel = tokio_util::sync::CancellationToken::new();
    let admin_config = Arc::new(AtomicBool::new(concurrent_config));
    let state = registered.clone();
    let admin = admin_config.clone();
    Mock::given(method("GET"))
        .and(path("/config/event-value"))
        .respond_with(move |_: &wiremock::Request| {
            if state.load(Ordering::SeqCst) && admin.load(Ordering::SeqCst) {
                ResponseTemplate::new(200).set_body_json(json!({"compatibilityLevel":"FULL"}))
            } else {
                ResponseTemplate::new(404)
                    .set_body_json(json!({"error_code":40401,"message":"inherited"}))
            }
        })
        .mount(&s)
        .await;
    let state = registered.clone();
    Mock::given(method("GET"))
        .and(path("/subjects/event-value/versions"))
        .respond_with(move |_: &wiremock::Request| {
            if state.load(Ordering::SeqCst) {
                ResponseTemplate::new(200).set_body_json(vec![1])
            } else {
                ResponseTemplate::new(404)
                    .set_body_json(json!({"error_code":40401,"message":"missing"}))
            }
        })
        .mount(&s)
        .await;
    let state = registered.clone();
    Mock::given(method("GET")).and(path("/subjects/event-value/versions/1")).respond_with(move |_:&wiremock::Request| if state.load(Ordering::SeqCst) {ResponseTemplate::new(200).set_body_json(json!({"subject":"event-value","version":1,"id":7,"schema":"\"string\"","references":[]}))} else {ResponseTemplate::new(404).set_body_json(json!({"error_code":40401,"message":"missing"}))}).mount(&s).await;
    let state = importing.clone();
    Mock::given(method("GET"))
        .and(path("/mode/event-value"))
        .respond_with(move |_: &wiremock::Request| {
            if state.load(Ordering::SeqCst) {
                ResponseTemplate::new(200).set_body_json(json!({"mode":"IMPORT"}))
            } else {
                ResponseTemplate::new(404)
                    .set_body_json(json!({"error_code":40401,"message":"inherited"}))
            }
        })
        .mount(&s)
        .await;
    let state = importing.clone();
    Mock::given(method("PUT"))
        .and(path("/mode/event-value"))
        .respond_with(move |_: &wiremock::Request| {
            state.store(true, Ordering::SeqCst);
            ResponseTemplate::new(200).set_body_json(json!({"mode":"IMPORT"}))
        })
        .mount(&s)
        .await;
    let state = importing.clone();
    let fail = fail_cleanup.clone();
    Mock::given(method("DELETE"))
        .and(path("/mode/event-value"))
        .respond_with(move |_: &wiremock::Request| {
            if fail.load(Ordering::SeqCst) {
                ResponseTemplate::new(500)
                    .set_body_json(json!({"error_code":50001,"message":"cleanup failed"}))
            } else {
                state.store(false, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_json(json!({"mode":"READWRITE"}))
            }
        })
        .mount(&s)
        .await;
    let state = registered.clone();
    let shutdown = cancel.clone();
    Mock::given(method("POST"))
        .and(path("/subjects/event-value/versions"))
        .respond_with(move |_: &wiremock::Request| {
            state.store(true, Ordering::SeqCst);
            if cancellation {
                shutdown.cancel();
            }
            let response = ResponseTemplate::new(200).set_body_json(json!({"id":7}));
            if cancellation {
                response.set_delay(std::time::Duration::from_millis(500))
            } else {
                response
            }
        })
        .mount(&s)
        .await;
    let temp = tempfile::tempdir().unwrap();
    let e = engine(
        snapshot(),
        config(),
        &s,
        Arc::new(MemoryBackend::new()),
        &temp.path().join("cp"),
    );
    let plan = e.plan().await.unwrap();
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
    let error = e.execute_with_cancel(&plan, cancel).await.unwrap_err();
    if cancellation {
        assert!(error.to_string().contains("cancelled"));
        assert!(!importing.load(Ordering::SeqCst));
    } else if concurrent_config {
        assert!(error
            .to_string()
            .contains("Configuration changed concurrently"));
        assert!(s
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| { r.url.path() != "/config/event-value" || r.method.as_str() == "GET" }));
        // An operator reconciles the new policy; resume must still reuse the
        // committed registration and never repeat the POST.
        admin_config.store(false, Ordering::SeqCst);
    } else {
        assert!(error.to_string().contains("cleanup failed"));
        assert!(importing.load(Ordering::SeqCst));
    }
    fail_cleanup.store(false, Ordering::SeqCst);
    let (report, _) = e.execute(&e.plan().await.unwrap()).await.unwrap();
    assert!(report.verified && report.cleanup_verified);
    assert_eq!(report.created, 0);
    assert_eq!(report.reused, 1);
    assert!(!importing.load(Ordering::SeqCst));
    let posts = s
        .received_requests()
        .await
        .unwrap()
        .iter()
        .filter(|r| r.method.as_str() == "POST")
        .count();
    assert_eq!(posts, 1);
}

#[tokio::test]
async fn apicurio_generation_metadata_is_rejected_before_registration() {
    use kafka_backup_core::config::ApicurioRegistryConfig;
    for (api, unavailable) in [("v3", "properties"), ("v2", "labels")] {
        let s = server().await;
        let mut snapshot = snapshot();
        snapshot.provider = Provider::Apicurio;
        snapshot.api_version = if api == "v3" { "v2" } else { "v3" }.into();
        snapshot.global = json!([]);
        snapshot.groups.clear();
        snapshot.settings.clear();
        let schema = &mut snapshot.schemas[0];
        schema.coordinate.group = "default".into();
        schema.content_id = Some(7);
        schema.deleted = None;
        schema.state = Some("ENABLED".into());
        schema
            .envelope
            .insert(unavailable.into(), json!({"team":"billing"}));
        let cfg:ApicurioRegistryConfig=serde_json::from_value(json!({"url":s.uri(),"api_version":api,"connection":{"max_retries":0,"rate_limit_rps":10000}})).unwrap();
        let temp = tempfile::tempdir().unwrap();
        let e = RegistryRestore::new(
            snapshot,
            RestoreConfig {
                id_policy: IdPolicy::Rewrite,
                ..Default::default()
            },
            Target::apicurio(&cfg).unwrap(),
            Arc::new(MemoryBackend::new()),
            s.uri(),
            Some(&temp.path().join("cp")),
        )
        .unwrap();
        let plan = e.plan().await.unwrap();
        assert!(
            plan.conflicts
                .iter()
                .any(|c| c.contains("cannot faithfully restore") && c.contains(unavailable)),
            "{:?}",
            plan.conflicts
        );
        assert!(e.execute(&plan).await.is_err());
        no_mutations(&s).await;
    }
}

#[tokio::test]
async fn unqualified_confluent_rule_registration_rejects_even_with_target_policy() {
    for policy in [
        json!({}),
        json!({"domainRules":[{"name":"existing-policy","type":"CEL","mode":"WRITEREAD","kind":"CONDITION","expr":"true"}]}),
    ] {
        let s = server().await;
        Mock::given(method("GET"))
            .and(path("/config"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    json!({"compatibilityLevel":"BACKWARD","defaultRuleSet":policy}),
                ),
            )
            .mount(&s)
            .await;
        let mut snapshot = snapshot();
        snapshot.schemas[0].envelope.insert("ruleSet".into(),json!({"domainRules":[{"name":"check","type":"CEL","mode":"WRITEREAD","kind":"CONDITION","expr":"true"}]}));
        let temp = tempfile::tempdir().unwrap();
        let e = engine(
            snapshot,
            config(),
            &s,
            Arc::new(MemoryBackend::new()),
            &temp.path().join("cp"),
        );
        let plan = e.plan().await.unwrap();
        assert!(
            plan.conflicts
                .iter()
                .any(|c| c.contains("Target rule support")),
            "{:?}",
            plan.conflicts
        );
        assert!(e.execute(&plan).await.is_err());
        no_mutations(&s).await;
    }
}

#[tokio::test]
async fn rule_bearing_exact_reuse_is_registry_only_and_blocks_record_preflight() {
    for category in ["encodingRules", "domainRules", "migrationRules"] {
        let s = server().await;
        let rules = json!({category:[{"name":"encrypted-field","type":"ENCRYPT","kind":"TRANSFORM","mode":"WRITEREAD","params":{"encrypt.kek.name":"source-key"}}]});
        let mut source = snapshot();
        source.schemas[0]
            .envelope
            .insert("ruleSet".into(), rules.clone());
        Mock::given(method("GET")).and(path("/subjects/event-value/versions/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"subject":"event-value","version":1,"id":7,"schema":"\"string\"","ruleSet":rules})))
            .mount(&s).await;
        Mock::given(method("GET"))
            .and(path("/subjects/event-value/versions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![1]))
            .mount(&s)
            .await;
        let record = BackupRecord {
            key: None,
            value: Some(vec![0, 0, 0, 0, 7, 2, 65]),
            headers: vec![],
            timestamp: 123,
            offset: 42,
        };
        let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
        storage
            .put(
                "test/t/0/segment.json",
                Bytes::from(serde_json::to_vec(&vec![record.clone()]).unwrap()),
            )
            .await
            .unwrap();
        let mut manifest = BackupManifest::new("test".into());
        manifest.topics.push(serde_json::from_value(json!({"name":"t","partitions":[{"partition_id":0,"segments":[{"key":"test/t/0/segment.json","partition_id":0,"start_offset":42,"end_offset":42,"record_count":1,"size_bytes":100,"start_timestamp":123,"end_timestamp":123}]}]})).unwrap());
        storage
            .put(
                "test/manifest.json",
                Bytes::from(serde_json::to_vec(&manifest).unwrap()),
            )
            .await
            .unwrap();
        let temp = tempfile::tempdir().unwrap();
        let e = engine(source, config(), &s, storage, &temp.path().join("cp"));
        let plan = e.plan().await.unwrap();
        assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
        assert!(plan.registrations.iter().all(|r| r.action == "reuse"));
        let topics = TopicSelection {
            include: vec!["t".into()],
            ..Default::default()
        };
        let error = e
            .preflight_records("test", &Default::default(), &topics)
            .await
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("rule executor"),
            "{category}: {error:#}"
        );
        no_mutations(&s).await;
        let (report, transform) = e.execute(&plan).await.unwrap();
        assert!(report.verified && report.cleanup_verified);
        assert_eq!(report.created, 0);
        assert_eq!(report.reused, 1);
        assert!(transform.check_record("t", &record).is_err());
        no_mutations(&s).await;
    }
}
