//! Real-service contracts. Run explicitly via scripts/test-registry-restore.sh.
//! Missing endpoints are failures, never implicit skips.
use kafka_backup_core::storage::{MemoryBackend, StorageBackend};
use kafka_backup_core::{
    apicurio_registry::{backup::ApicurioRegistryBackup, client::ApicurioRegistryClient},
    config::{ApicurioRegistryConfig, SchemaBackupOptions, SchemaRegistryConfig},
    registry_restore::{
        engine::{RegistryRestore, Target},
        snapshot,
        wire::*,
        CoordinateMapping, IdPolicy, Provider, RestoreConfig,
    },
    schema_registry::{backup::SchemaRegistryBackup, client::SchemaRegistryClient},
};
use reqwest::Method;
use std::{collections::BTreeMap, sync::Arc};
fn endpoint(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for real registry tests"))
}
fn sr(url: &str) -> SchemaRegistryConfig {
    serde_json::from_value(
        // Match production safe-read retries. Mutations are still one-shot.
        serde_json::json!({"url":url,"connection":{"rate_limit_rps":1000}}),
    )
    .unwrap()
}
fn ap(url: &str) -> ApicurioRegistryConfig {
    let side = if std::env::var("REGISTRY_APICURIO_TARGET").as_deref() == Ok(url) {
        "REGISTRY_APICURIO_TARGET_API"
    } else {
        "REGISTRY_APICURIO_SOURCE_API"
    };
    let api = std::env::var(side)
        .or_else(|_| std::env::var("REGISTRY_APICURIO_API"))
        .unwrap_or("v3".into());
    serde_json::from_value(
        serde_json::json!({"url":url,"api_version":api,"connection":{"rate_limit_rps":1000}}),
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated pinned real registry services"]
async fn confluent_preserve_real() {
    let source = endpoint("REGISTRY_CONFLUENT_SOURCE");
    let target = endpoint("REGISTRY_CONFLUENT_TARGET");
    let mut cfg = sr(&source);
    cfg.backup = Some(SchemaBackupOptions {
        subjects: vec!["restore-fixture-*".into()],
        include_soft_deleted: true,
        ..Default::default()
    });
    let c = SchemaRegistryClient::new(&cfg).unwrap();
    for schema in [
        r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"string"}]}"#,
        r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"string"},{"name":"new","type":"string","default":""}]}"#,
    ] {
        c.mutate(
            Method::POST,
            "/subjects/restore-fixture-event/versions",
            Some(&serde_json::json!({"schema":schema})),
        )
        .await
        .unwrap();
    }
    c.mutate(
        Method::POST,
        "/subjects/restore-fixture-bytes/versions",
        Some(&serde_json::json!({"schema":"\"bytes\""})),
    )
    .await
    .unwrap();
    c.mutate(Method::POST,"/subjects/restore-fixture-address/versions",Some(&serde_json::json!({"schema":"{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"common\",\"fields\":[{\"name\":\"city\",\"type\":\"string\"}]}"}))).await.unwrap();
    c.mutate(Method::POST,"/subjects/restore-fixture-reference/versions",Some(&serde_json::json!({"schema":"{\"type\":\"record\",\"name\":\"ReferencedEvent\",\"fields\":[{\"name\":\"address\",\"type\":\"common.Address\"}]}","references":[{"name":"common.Address","subject":"restore-fixture-address","version":1}]}))).await.unwrap();
    c.mutate(
        Method::POST,
        "/subjects/restore-fixture-deleted/versions",
        Some(&serde_json::json!({"schema":"\"long\""})),
    )
    .await
    .unwrap();
    c.mutate(
        Method::DELETE,
        "/subjects/restore-fixture-deleted/versions/latest",
        None,
    )
    .await
    .unwrap();
    let version = c.server_version().await.unwrap().unwrap_or_default();
    let version: Vec<u32> = version.split('.').filter_map(|p| p.parse().ok()).collect();
    if version.as_slice() >= [7, 4].as_slice() {
        c.mutate(
            Method::PUT,
            "/config/restore-fixture-event",
            Some(&serde_json::json!({"compatibility":"BACKWARD","normalize":true})),
        )
        .await
        .unwrap();
        c.mutate(Method::POST,"/subjects/restore-fixture-metadata/versions",Some(&serde_json::json!({"schema":"\"int\"","metadata":{"properties":{"team":"backup"},"tags":{"$field":["retained"]}},"ruleSet":{"domainRules":[{"name":"always-valid","kind":"CONDITION","mode":"WRITEREAD","type":"CEL","expr":"true"}]}}))).await.unwrap();
    }
    // IMPORT permits A2 -> B1 -> A1 even though numeric subject history would
    // impose A1 -> A2. Preserve explicit versions in reference order; rewriting
    // cannot allocate that history faithfully and must reject it instead.
    for subject in ["restore-fixture-reverse-a", "restore-fixture-reverse-b"] {
        c.mutate(
            Method::PUT,
            &format!("/config/{subject}"),
            Some(&serde_json::json!({"compatibility":"NONE"})),
        )
        .await
        .unwrap();
        c.mutate(
            Method::PUT,
            &format!("/mode/{subject}"),
            Some(&serde_json::json!({"mode":"IMPORT"})),
        )
        .await
        .unwrap();
    }
    for (subject, payload) in [
        (
            "restore-fixture-reverse-a",
            serde_json::json!({"id":1000,"version":2,"schema":"{\"type\":\"record\",\"name\":\"Later\",\"namespace\":\"reverse\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}),
        ),
        (
            "restore-fixture-reverse-b",
            serde_json::json!({"id":1001,"version":1,"schema":"{\"type\":\"record\",\"name\":\"Bridge\",\"namespace\":\"reverse\",\"fields\":[{\"name\":\"later\",\"type\":\"reverse.Later\"}]}","references":[{"name":"reverse.Later","subject":"restore-fixture-reverse-a","version":2}]}),
        ),
        (
            "restore-fixture-reverse-a",
            serde_json::json!({"id":1002,"version":1,"schema":"{\"type\":\"record\",\"name\":\"Earlier\",\"namespace\":\"reverse\",\"fields\":[{\"name\":\"bridge\",\"type\":\"reverse.Bridge\"}]}","references":[{"name":"reverse.Bridge","subject":"restore-fixture-reverse-b","version":1}]}),
        ),
    ] {
        c.mutate(
            Method::POST,
            &format!("/subjects/{subject}/versions"),
            Some(&payload),
        )
        .await
        .unwrap();
    }
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    SchemaRegistryBackup::new(c, storage.clone(), "real".into(), cfg)
        .run()
        .await
        .unwrap();
    let snapshot = snapshot::load(storage.as_ref(), "real", Provider::Confluent)
        .await
        .unwrap();
    assert!(snapshot.schemas.len() >= 6);
    assert!(snapshot.schemas.iter().any(|s| s.deleted == Some(true)));
    if version.as_slice() >= [7, 4].as_slice() {
        assert!(snapshot
            .schemas
            .iter()
            .any(|s| s.envelope.contains_key("metadata")));
        // These public images retain metadata but omit rules when their rule
        // extension is unavailable. Never turn an omitted rule set into a
        // successful rule-preservation claim.
        println!(
            "Confluent source ruleSet retained: {}",
            snapshot
                .schemas
                .iter()
                .any(|s| s.envelope.contains_key("ruleSet"))
        );
        assert!(snapshot
            .settings
            .iter()
            .any(|s| s.config.as_ref().is_some_and(|c| c["normalize"] == true)));
    }
    let schema_count = snapshot.schemas.len();
    let temp = tempfile::tempdir().unwrap();
    let checkpoint = temp.path().join("restore.json");
    let engine = RegistryRestore::new(
        snapshot,
        RestoreConfig::default(),
        Target::confluent(&sr(&target)).unwrap(),
        storage,
        target,
        Some(&checkpoint),
    )
    .unwrap();
    let plan = engine.plan().await.unwrap();
    if std::env::var("REGISTRY_EXPECT_MODE_REJECTION").is_ok() {
        assert!(
            plan.conflicts
                .iter()
                .any(|c| c.contains("DELETE mode overrides")),
            "{:?}",
            plan.conflicts
        );
        assert!(engine.execute(&plan).await.is_err());
        assert!(
            SchemaRegistryClient::new(&sr(&endpoint("REGISTRY_CONFLUENT_TARGET")))
                .unwrap()
                .list_subjects(true)
                .await
                .unwrap()
                .is_empty()
        );
        return;
    }
    if std::env::var("REGISTRY_EXPECT_GUID_REJECTION").is_ok() {
        assert!(
            plan.conflicts.iter().any(|c| c.contains("GUID support")),
            "{:?}",
            plan.conflicts
        );
        assert!(engine.execute(&plan).await.is_err());
        assert!(
            SchemaRegistryClient::new(&sr(&endpoint("REGISTRY_CONFLUENT_TARGET")))
                .unwrap()
                .list_subjects(true)
                .await
                .unwrap()
                .is_empty()
        );
        return;
    }
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
    let (report, _) = engine.execute(&plan).await.unwrap();
    assert!(report.verified && report.cleanup_verified);
    engine.verify(&report).await.unwrap();
    for m in &report.mappings {
        assert_eq!(m.source_id, m.destination_id);
        assert_eq!(m.source.version, m.destination.version);
    }
    let resume = engine.plan().await.unwrap();
    assert!(resume.conflicts.is_empty(), "{:?}", resume.conflicts);
    let (resumed, _) = engine.execute(&resume).await.unwrap();
    assert_eq!(resumed.created, 0);
    assert_eq!(resumed.reused, schema_count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated pinned real registry services"]
async fn apicurio_preserve_real() {
    let source = endpoint("REGISTRY_APICURIO_SOURCE");
    let target = endpoint("REGISTRY_APICURIO_TARGET");
    let cfg = ap(&source);
    let client = ApicurioRegistryClient::new(&cfg).unwrap();
    // Registry-only formats, metadata, empty coordinates and lifecycle state
    // must survive native preservation as well as the serializer fixtures.
    let v2 = cfg.api_version == kafka_backup_core::config::ApicurioApiVersion::V2;
    let group = if v2 {
        serde_json::json!({"id":"restore-group","description":"Registry-only fixtures","properties":{"team":"backup"}})
    } else {
        serde_json::json!({"groupId":"restore-group","description":"Registry-only fixtures","labels":{"team":"backup"}})
    };
    client
        .mutate(
            Method::POST,
            "/groups",
            "application/json",
            bytes::Bytes::from(serde_json::to_vec(&group).unwrap()),
            &[],
        )
        .await
        .unwrap();
    for (name, ty, content) in [
        ("discarded-gap", "AVRO", "\"float\""),
        (
            "api",
            "OPENAPI",
            r#"{"openapi":"3.0.0","info":{"title":"Oracle API","version":"1"},"paths":{}}"#,
        ),
        ("graph", "GRAPHQL", "type Query { greeting: String }"),
        ("inactive", "AVRO", "\"boolean\""),
        ("shared-content", "AVRO", "\"boolean\""),
    ] {
        let (body, ctype, headers) = if v2 {
            (
                serde_json::json!({"content":content,"references":[]}),
                "application/create.extended+json",
                vec![
                    ("X-Registry-ArtifactId".into(), name.into()),
                    ("X-Registry-ArtifactType".into(), ty.into()),
                    ("X-Registry-Version".into(), "1".into()),
                ],
            )
        } else {
            (
                serde_json::json!({"artifactId":name,"artifactType":ty,"name":"Fixture","labels":{"scope":"archive"},"firstVersion":{"version":"1","name":"Version one","labels":{"version":"first"},"content":{"content":content,"contentType":if ty=="GRAPHQL" {"application/graphql"} else {"application/json"}}}}),
                "application/json",
                vec![],
            )
        };
        client
            .mutate(
                Method::POST,
                "/groups/restore-group/artifacts",
                ctype,
                bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
                &headers,
            )
            .await
            .unwrap();
        if name == "discarded-gap" {
            client
                .mutate(
                    Method::DELETE,
                    "/groups/restore-group/artifacts/discarded-gap",
                    "application/json",
                    bytes::Bytes::new(),
                    &[],
                )
                .await
                .unwrap();
            continue;
        }
        if v2 {
            client.mutate(Method::PUT,&format!("/groups/restore-group/artifacts/{name}/versions/1/meta"),"application/json",bytes::Bytes::from_static(br#"{"name":"Version one","properties":{"version":"first"},"labels":["archive"]}"#),&[]).await.unwrap();
        }
    }
    client
        .mutate(
            Method::PUT,
            "/groups/restore-group/artifacts/inactive/versions/1/state",
            "application/json",
            bytes::Bytes::from_static(br#"{"state":"DISABLED"}"#),
            &[],
        )
        .await
        .unwrap();
    if !v2 {
        client.mutate(Method::POST,"/groups/restore-group/artifacts","application/json",bytes::Bytes::from_static(br#"{"artifactId":"empty","artifactType":"GRAPHQL","description":"No versions"}"#),&[]).await.unwrap();
        client
            .mutate(
                Method::POST,
                "/groups",
                "application/json",
                bytes::Bytes::from_static(
                    br#"{"groupId":"empty-group","description":"No artifacts"}"#,
                ),
                &[],
            )
            .await
            .unwrap();
    }
    // The script seeds native serializer artifacts, including the implicit
    // default group. A full backup must discover all of them.
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let manifest = ApicurioRegistryBackup::new(client, storage.clone(), "real".into(), cfg)
        .run()
        .await
        .unwrap();
    assert!(manifest.total_versions > 0);
    let snapshot = snapshot::load(storage.as_ref(), "real", Provider::Apicurio)
        .await
        .unwrap();
    assert!(snapshot
        .schemas
        .iter()
        .any(|s| s.state.as_deref() == Some("DISABLED")));
    assert!(snapshot.schemas.iter().any(|s| s.schema_type == "OPENAPI"));
    assert!(snapshot.schemas.iter().any(|s| s.schema_type == "GRAPHQL"));
    let temp = tempfile::tempdir().unwrap();
    let checkpoint = temp.path().join("restore.json");
    let engine = RegistryRestore::new(
        snapshot,
        RestoreConfig::default(),
        Target::apicurio(&ap(&target)).unwrap(),
        storage,
        target,
        Some(&checkpoint),
    )
    .unwrap();
    let plan = engine.plan().await.unwrap();
    if std::env::var("REGISTRY_EXPECT_ARCHIVE_REJECTION").is_ok() {
        assert!(
            plan.conflicts.iter().any(|c| c.contains("API generations")),
            "{:?}",
            plan.conflicts
        );
        assert!(engine.execute(&plan).await.is_err());
        assert_eq!(
            ApicurioRegistryClient::new(&ap(&endpoint("REGISTRY_APICURIO_TARGET")))
                .unwrap()
                .list_artifacts("default", 1, 0)
                .await
                .unwrap()
                .count,
            0
        );
        return;
    }
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
    let (report, _) = engine.execute(&plan).await.unwrap();
    assert!(report.verified && report.cleanup_verified);
    engine.verify(&report).await.unwrap();
    for m in &report.mappings {
        assert_eq!(m.source_id, m.destination_id);
        assert_eq!(m.source.version, m.destination.version);
        assert!(m.destination_content_id.is_some());
    }
    let (resumed, _) = engine.execute(&engine.plan().await.unwrap()).await.unwrap();
    assert_eq!(resumed.created, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires isolated pinned real registry services"]
async fn cross_provider_real() {
    let direction = endpoint("REGISTRY_CROSS_DIRECTION");
    let (source_provider, target_provider) = if direction == "confluent_to_apicurio" {
        (Provider::Confluent, Provider::Apicurio)
    } else {
        assert_eq!(direction, "apicurio_to_confluent");
        (Provider::Apicurio, Provider::Confluent)
    };
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    if source_provider == Provider::Confluent {
        let url = endpoint("REGISTRY_CONFLUENT_SOURCE");
        let mut cfg = sr(&url);
        cfg.backup = Some(SchemaBackupOptions {
            subjects: vec!["restore-fixture-bytes".into()],
            ..Default::default()
        });
        SchemaRegistryBackup::new(
            SchemaRegistryClient::new(&cfg).unwrap(),
            storage.clone(),
            "cross".into(),
            cfg,
        )
        .run()
        .await
        .unwrap();
    } else {
        let cfg = ap(&endpoint("REGISTRY_APICURIO_SOURCE"));
        ApicurioRegistryBackup::new(
            ApicurioRegistryClient::new(&cfg).unwrap(),
            storage.clone(),
            "cross".into(),
            cfg,
        )
        .run()
        .await
        .unwrap();
    }
    let snapshot = snapshot::load(storage.as_ref(), "cross", source_provider)
        .await
        .unwrap();
    let mut coordinates = vec![];
    for s in &snapshot.schemas {
        if !coordinates.iter().any(|c: &CoordinateMapping| {
            c.source_group == s.coordinate.group && c.source_name == s.coordinate.name
        }) {
            coordinates.push(CoordinateMapping {
                source_group: s.coordinate.group.clone(),
                source_name: s.coordinate.name.clone(),
                target_group: if target_provider == Provider::Apicurio {
                    "migration".into()
                } else {
                    "".into()
                },
                target_name: format!("cross-{}", s.coordinate.name),
            });
        }
    }
    let source = if source_provider == Provider::Confluent {
        Serializer::Confluent {
            namespace: "".into(),
        }
    } else {
        Serializer::Apicurio {
            namespace: "default".into(),
            id_kind: IdKind::Global,
            width: 8,
            header: None,
        }
    };
    let destination = if target_provider == Provider::Confluent {
        Serializer::Confluent {
            namespace: "".into(),
        }
    } else {
        Serializer::Apicurio {
            namespace: "migration".into(),
            id_kind: IdKind::Global,
            width: 8,
            header: None,
        }
    };
    let profile = PayloadProfile {
        source,
        target: destination,
        protobuf_source: ProtobufFrame::Indexes,
        protobuf_target: ProtobufFrame::Indexes,
        message: None,
        protobuf_source_type_header: None,
        protobuf_target_type_header: None,
    };
    let profiles = BTreeMap::from([(
        "fixture-topic".into(),
        TopicProfile {
            key: PayloadProfile {
                source: Serializer::Raw,
                target: Serializer::Raw,
                ..profile.clone()
            },
            value: profile,
        },
    )]);
    let config = RestoreConfig {
        source: Some(source_provider),
        destination: Some(target_provider),
        id_policy: IdPolicy::Rewrite,
        coordinates,
        serializers: profiles,
        ..Default::default()
    };
    let url = if target_provider == Provider::Confluent {
        endpoint("REGISTRY_CONFLUENT_TARGET")
    } else {
        endpoint("REGISTRY_APICURIO_TARGET")
    };
    let target = if target_provider == Provider::Confluent {
        Target::confluent(&sr(&url)).unwrap()
    } else {
        Target::apicurio(&ap(&url)).unwrap()
    };
    let temp = tempfile::tempdir().unwrap();
    let engine = RegistryRestore::new(
        snapshot,
        config,
        target,
        storage,
        url,
        Some(&temp.path().join("cross.json")),
    )
    .unwrap();
    let plan = engine.plan().await.unwrap();
    assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
    let (report, _) = engine.execute(&plan).await.unwrap();
    engine.verify(&report).await.unwrap();
    assert!(report.verified && report.cleanup_verified);
}

// Java SDK subprocesses block their caller. Match the production runtime so
// independent HTTP connection drivers continue running during oracle work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires pinned SDK oracle and isolated real registries"]
async fn independent_avro_sdk_cross_provider_real() {
    use kafka_backup_core::{manifest::BackupRecord, restore::transform::RecordTransform};
    use std::process::Command;
    let direction = endpoint("REGISTRY_CROSS_DIRECTION");
    let (source_provider, target_provider) =
        if let Ok(provider) = std::env::var("REGISTRY_SAME_PROVIDER") {
            let provider = if provider == "confluent" {
                Provider::Confluent
            } else {
                assert_eq!(provider, "apicurio");
                Provider::Apicurio
            };
            (provider, provider)
        } else if direction == "confluent_to_apicurio" {
            (Provider::Confluent, Provider::Apicurio)
        } else {
            assert_eq!(direction, "apicurio_to_confluent");
            (Provider::Apicurio, Provider::Confluent)
        };
    let source_name = if source_provider == Provider::Confluent {
        "confluent"
    } else {
        "apicurio"
    };
    let target_name = if target_provider == Provider::Confluent {
        "confluent"
    } else {
        "apicurio"
    };
    let source_url = if source_provider == Provider::Confluent {
        endpoint("REGISTRY_CONFLUENT_SOURCE")
    } else {
        endpoint("REGISTRY_APICURIO_SOURCE")
    };
    let target_url = if target_provider == Provider::Confluent {
        endpoint("REGISTRY_CONFLUENT_TARGET")
    } else {
        endpoint("REGISTRY_APICURIO_TARGET")
    };
    let oracle = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/registry-serdes-oracle");
    let classpath = std::env::var("REGISTRY_ORACLE_SOURCE_CLASSPATH").unwrap_or_else(|_| {
        format!(
            "{}:{}",
            oracle.join("target/classes").display(),
            std::fs::read_to_string(oracle.join("classpath.txt"))
                .expect("build released SDK oracle first")
                .trim()
        )
    });
    let source_classpath =
        std::env::var("REGISTRY_ORACLE_SOURCE_CLASSPATH").unwrap_or_else(|_| classpath.clone());
    let target_classpath =
        std::env::var("REGISTRY_ORACLE_TARGET_CLASSPATH").unwrap_or_else(|_| classpath.clone());
    let java = std::env::var("REGISTRY_JAVA").unwrap_or("java".into());
    let temp = tempfile::tempdir().unwrap();
    let source_framing =
        std::env::var("REGISTRY_ORACLE_SOURCE_FRAMING").unwrap_or("prefix4".into());
    let target_framing =
        std::env::var("REGISTRY_ORACLE_TARGET_FRAMING").unwrap_or("prefix4".into());
    let id_kind = if std::env::var("REGISTRY_ORACLE_ID_KIND").as_deref() == Ok("contentId") {
        IdKind::Content
    } else {
        IdKind::Global
    };
    let serialized_keys = std::env::var("REGISTRY_ORACLE_SERIALIZED_KEYS").as_deref() == Ok("true");
    let nested_rejection = std::env::var("REGISTRY_EXPECT_NATIVE_NESTED_REJECTION").is_ok();
    let nested = nested_rejection
        || ((source_provider == Provider::Confluent || source_framing == "compatible")
            && (target_provider == Provider::Confluent || target_framing == "compatible"));
    for data_type in ["bytes", "record", "json", "protobuf"] {
        if nested_rejection && data_type != "protobuf" {
            continue;
        }
        let broker = std::env::var("REGISTRY_KAFKA").ok();
        let run_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let topic = format!("sdk-{source_name}-{data_type}-{run_id}");
        let serialized = temp.path().join(format!("{data_type}.json"));
        let source_sdk_url = if source_provider == Provider::Apicurio {
            if source_framing == "compatible" {
                format!("{source_url}/apis/ccompat/v7")
            } else {
                format!(
                    "{source_url}/apis/registry/{}",
                    std::env::var("REGISTRY_APICURIO_SOURCE_API")
                        .or_else(|_| std::env::var("REGISTRY_APICURIO_API"))
                        .unwrap_or("v3".into())
                )
            }
        } else {
            source_url.clone()
        };
        let mut producer = Command::new(&java);
        producer.env(
            "REGISTRY_ORACLE_NESTED_PROTOBUF",
            if nested { "true" } else { "false" },
        );
        producer.env("REGISTRY_ORACLE_FRAMING", &source_framing);
        producer.args([
            "-cp",
            &source_classpath,
            "RegistryOracle",
            if broker.is_some() {
                "produce"
            } else {
                "serialize"
            },
            source_name,
            &source_sdk_url,
            &topic,
            data_type,
            serialized.to_str().unwrap(),
        ]);
        if let Some(broker) = &broker {
            producer.arg(broker);
        }
        let output = producer.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let serialized: serde_json::Value =
            serde_json::from_slice(&std::fs::read(serialized).unwrap()).unwrap();
        let storage_dir = temp.path().join(data_type);
        let storage_cfg = kafka_backup_core::storage::StorageBackendConfig::Filesystem {
            path: storage_dir.clone(),
        };
        let storage = kafka_backup_core::storage::create_backend(&storage_cfg).unwrap();
        if let Some(broker) = &broker {
            let cfg:kafka_backup_core::config::Config=serde_json::from_value(serde_json::json!({"mode":"backup","backup_id":"sdk","source":{"bootstrap_servers":[broker],"topics":{"include":[topic]}},"storage":{"backend":"filesystem","path":storage_dir},"backup":{"include_offset_headers":false}})).unwrap();
            kafka_backup_core::backup::BackupEngine::new(cfg)
                .await
                .unwrap()
                .run()
                .await
                .unwrap();
        }
        if source_provider == Provider::Confluent {
            let mut cfg = sr(&source_url);
            cfg.backup = Some(SchemaBackupOptions {
                subjects: if serialized_keys {
                    vec![format!("{topic}-*")]
                } else {
                    vec![format!("{topic}-value")]
                },
                ..Default::default()
            });
            SchemaRegistryBackup::new(
                SchemaRegistryClient::new(&cfg).unwrap(),
                storage.clone(),
                "sdk".into(),
                cfg,
            )
            .run()
            .await
            .unwrap();
        } else {
            let mut cfg = ap(&source_url);
            cfg.backup=Some(serde_json::from_value(serde_json::json!({"artifacts":[if serialized_keys {format!("{topic}-*")} else {format!("{topic}-value")}],"include_export":false})).unwrap());
            ApicurioRegistryBackup::new(
                ApicurioRegistryClient::new(&cfg).unwrap(),
                storage.clone(),
                "sdk".into(),
                cfg,
            )
            .run()
            .await
            .unwrap();
        }
        let snapshot = snapshot::load(storage.as_ref(), "sdk", source_provider)
            .await
            .unwrap();
        assert_eq!(snapshot.schemas.len(), if serialized_keys { 2 } else { 1 });
        let source_group = if source_provider == Provider::Apicurio {
            "default"
        } else {
            ""
        };
        let destination_group = if target_provider == Provider::Apicurio {
            "default"
        } else {
            ""
        };
        let mut coordinates = vec![CoordinateMapping {
            source_group: source_group.into(),
            source_name: format!("{topic}-value"),
            target_group: destination_group.into(),
            target_name: format!("{topic}-value"),
        }];
        if serialized_keys {
            coordinates.push(CoordinateMapping {
                source_group: source_group.into(),
                source_name: format!("{topic}-key"),
                target_group: destination_group.into(),
                target_name: format!("{topic}-key"),
            });
        }
        let serializer = |provider, framing: &str, id_kind| {
            if provider == Provider::Confluent {
                if framing == "headers" {
                    Serializer::ConfluentGuidHeader {
                        namespace: "".into(),
                        header: "__value_schema_id".into(),
                    }
                } else {
                    Serializer::Confluent {
                        namespace: "".into(),
                    }
                }
            } else {
                if framing == "compatible" {
                    return Serializer::ApicurioCompatible {
                        namespace: "default".into(),
                        id_kind,
                    };
                }
                Serializer::Apicurio {
                    namespace: "default".into(),
                    id_kind,
                    width: if framing == "prefix4" { 4 } else { 8 },
                    header: (framing == "headers").then(|| "oracle.value.registry.id".into()),
                }
            }
        };
        let src = serializer(source_provider, &source_framing, id_kind);
        let dst = serializer(target_provider, &target_framing, id_kind);
        let source_type_header = (source_provider == Provider::Apicurio
            && source_framing == "headers"
            && ap(&source_url).api_version == kafka_backup_core::config::ApicurioApiVersion::V2)
            .then(|| "apicurio.protobuf.type".to_string());
        let target_type_header = (target_provider == Provider::Apicurio
            && target_framing == "headers"
            && ap(&target_url).api_version == kafka_backup_core::config::ApicurioApiVersion::V2)
            .then(|| "apicurio.protobuf.type".to_string());
        let p = PayloadProfile {
            source: src,
            target: dst,
            protobuf_source: if source_type_header.is_some() {
                ProtobufFrame::Bare
            } else if source_provider == Provider::Confluent || source_framing == "compatible" {
                ProtobufFrame::Indexes
            } else {
                ProtobufFrame::TypeReference
            },
            protobuf_target: if target_type_header.is_some() {
                ProtobufFrame::Bare
            } else if target_provider == Provider::Confluent || target_framing == "compatible" {
                ProtobufFrame::Indexes
            } else {
                ProtobufFrame::TypeReference
            },
            message: Some(if nested {
                "oracle.Outer.EventMessage".into()
            } else {
                "oracle.EventMessage".into()
            }),
            protobuf_source_type_header: source_type_header,
            protobuf_target_type_header: target_type_header,
        };
        let config = RestoreConfig {
            source: Some(source_provider),
            destination: Some(target_provider),
            id_policy: IdPolicy::Rewrite,
            coordinates,
            serializers: BTreeMap::from([(
                topic.clone(),
                TopicProfile {
                    key: PayloadProfile {
                        protobuf_source_type_header: None,
                        protobuf_target_type_header: None,
                        source: if serialized_keys {
                            serializer(source_provider, "prefix4", IdKind::Global)
                        } else {
                            Serializer::Raw
                        },
                        target: if serialized_keys {
                            serializer(target_provider, "prefix4", IdKind::Global)
                        } else {
                            Serializer::Raw
                        },
                        ..p.clone()
                    },
                    value: p,
                },
            )]),
            ..Default::default()
        };
        let target = if target_provider == Provider::Confluent {
            Target::confluent(&sr(&target_url)).unwrap()
        } else {
            Target::apicurio(&ap(&target_url)).unwrap()
        };
        let engine = RegistryRestore::new(
            snapshot,
            config,
            target,
            storage,
            target_url.clone(),
            Some(&temp.path().join(format!("{data_type}.checkpoint"))),
        )
        .unwrap();
        let plan = engine.plan().await.unwrap();
        assert!(plan.conflicts.is_empty(), "{:?}", plan.conflicts);
        if nested_rejection {
            assert!(
                broker.is_some(),
                "Native nested rejection requires an archived Kafka record"
            );
            let topics = kafka_backup_core::config::TopicSelection {
                include: vec![topic.clone()],
                ..Default::default()
            };
            let error = engine
                .preflight_records("sdk", &Default::default(), &topics)
                .await
                .unwrap_err();
            assert!(
                format!("{error:#}").contains("nested messages"),
                "{error:#}"
            );
            assert!(ApicurioRegistryClient::new(&ap(&target_url))
                .unwrap()
                .get_artifact_metadata("default", &format!("{topic}-value"))
                .await
                .is_err());
            println!(
                "Expected native Apicurio nested Protobuf rejection before registry/Kafka writes"
            );
            return;
        }
        let (report, transform) = engine.execute(&plan).await.unwrap();
        engine.verify(&report).await.unwrap();
        if let Some(broker) = &broker {
            use kafka_backup_core::restore::transform::RecordTransformHandle;
            let destination_topic = format!("{topic}-restored");
            let repartition = std::env::var("REGISTRY_REPARTITION").is_ok();
            let mut cfg:kafka_backup_core::config::Config=serde_json::from_value(serde_json::json!({"mode":"restore","backup_id":"sdk","target":{"bootstrap_servers":[broker],"topics":{"include":[topic]}},"storage":{"backend":"filesystem","path":storage_dir},"restore":{"topic_mapping":{topic.clone():destination_topic},"strip_offset_headers":true,"create_topics":true}})).unwrap();
            engine
                .preflight_records_with_transform(
                    "sdk",
                    cfg.restore.as_ref().unwrap(),
                    &cfg.target.as_ref().unwrap().topics,
                    &transform,
                )
                .await
                .unwrap();
            cfg.restore.as_mut().unwrap().record_transform =
                Some(RecordTransformHandle::new(Arc::new(transform.clone())));
            cfg.restore.as_mut().unwrap().checkpoint_state =
                Some(temp.path().join(format!("kafka-{data_type}.checkpoint")));
            if repartition {
                cfg.restore.as_mut().unwrap().repartitioning.insert(
                    destination_topic.clone(),
                    kafka_backup_core::config::TopicRepartitioning {
                        strategy: kafka_backup_core::config::RepartitioningStrategy::Murmur2,
                        target_partitions: 2,
                    },
                );
            }
            let report = kafka_backup_core::RestoreEngine::new(cfg.clone())
                .unwrap()
                .run()
                .await
                .unwrap();
            assert_eq!(report.records_restored, 4);
            let (registry_resume, resumed_transform) =
                engine.execute(&engine.plan().await.unwrap()).await.unwrap();
            assert!(registry_resume.verified && registry_resume.cleanup_verified);
            assert_eq!(registry_resume.created, 0);
            assert_eq!(transform.fingerprint(), resumed_transform.fingerprint());
            cfg.restore.as_mut().unwrap().record_transform =
                Some(RecordTransformHandle::new(Arc::new(resumed_transform)));
            let resumed = kafka_backup_core::RestoreEngine::new(cfg)
                .unwrap()
                .run()
                .await
                .unwrap();
            assert_eq!(
                resumed.records_restored, 0,
                "resume produced duplicate records"
            );
            let decoded = temp.path().join(format!("{data_type}.consumed"));
            let destination_sdk_url = if target_provider == Provider::Apicurio {
                if target_framing == "compatible" {
                    format!("{target_url}/apis/ccompat/v7")
                } else {
                    format!(
                        "{target_url}/apis/registry/{}",
                        std::env::var("REGISTRY_APICURIO_TARGET_API")
                            .or_else(|_| std::env::var("REGISTRY_APICURIO_API"))
                            .unwrap_or("v3".into())
                    )
                }
            } else {
                target_url.clone()
            };
            let output = Command::new(&java)
                .env("REGISTRY_ORACLE_FRAMING", &target_framing)
                .args([
                    "-cp",
                    &target_classpath,
                    "RegistryOracle",
                    "consume",
                    target_name,
                    &destination_sdk_url,
                    &destination_topic,
                    data_type,
                    decoded.to_str().unwrap(),
                    broker,
                ])
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let values: Vec<String> =
                serde_json::from_slice(&std::fs::read(decoded).unwrap()).unwrap();
            assert_eq!(values.len(), 3);
            for value in values {
                if data_type == "bytes" {
                    assert_eq!(value, "0001027fff");
                } else {
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&value).unwrap(),
                        serde_json::from_str::<serde_json::Value>(
                            serialized["expected"].as_str().unwrap()
                        )
                        .unwrap()
                    );
                }
            }
            continue;
        }
        let hex = serialized["hex"].as_str().unwrap();
        let bytes: Vec<u8> = hex
            .as_bytes()
            .chunks(2)
            .map(|c| u8::from_str_radix(std::str::from_utf8(c).unwrap(), 16).unwrap())
            .collect();
        let mut record = BackupRecord {
            key: None,
            value: Some(bytes),
            headers: vec![],
            timestamp: 12345,
            offset: 42,
        };
        transform.transform(&topic, &mut record).unwrap();
        assert_eq!(record.timestamp, 12345);
        assert_eq!(record.offset, 42);
        let restored = temp.path().join(format!("{data_type}.hex"));
        std::fs::write(
            &restored,
            record
                .value
                .unwrap()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>(),
        )
        .unwrap();
        let decoded = temp.path().join(format!("{data_type}.decoded"));
        let destination_sdk_url = if target_provider == Provider::Apicurio {
            format!(
                "{target_url}/apis/registry/{}",
                std::env::var("REGISTRY_APICURIO_TARGET_API")
                    .or_else(|_| std::env::var("REGISTRY_APICURIO_API"))
                    .unwrap_or("v3".into())
            )
        } else {
            target_url.clone()
        };
        let output = Command::new(&java)
            .args([
                "-cp",
                &target_classpath,
                "RegistryOracle",
                "deserialize",
                target_name,
                &destination_sdk_url,
                &topic,
                restored.to_str().unwrap(),
                decoded.to_str().unwrap(),
                data_type,
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let value = std::fs::read_to_string(decoded).unwrap();
        if data_type == "bytes" {
            assert_eq!(value, "0001027fff");
        } else {
            let decoded: serde_json::Value = serde_json::from_str(&value).unwrap();
            assert_eq!(
                decoded,
                serde_json::from_str::<serde_json::Value>(serialized["expected"].as_str().unwrap())
                    .unwrap()
            );
        }
    }
}
