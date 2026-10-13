//! Azure Blob Storage backend against Azurite (the Azure Storage emulator).
//!
//! Covers the authentication paths that work without real Azure: the
//! well-known emulator account key, an explicit account key over a plain
//! HTTP endpoint, and a SAS token pasted as the portal prints it
//! (percent-encoded `sig` / `se`). The last test runs a full backup and
//! restore through the Azure backend.
//!
//! Azurite accepts a double-encoded SAS where Azure answers 403, so the
//! single-encoding fix is pinned by
//! `storage::azure::tests::portal_sas_reaches_the_wire_encoded_once`, not
//! here.
//!
//! Azurite runs from `mcr.microsoft.com/azure-storage/azurite:3.37.0` on a
//! free host port (the same port inside the container). Requires Docker.

use std::collections::HashMap;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use bytes::Bytes;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, RestoreOptions, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::storage::{
    create_backend, AzureBackend, AzureConfig, StorageBackend, StorageBackendConfig,
    EMULATOR_ACCOUNT, EMULATOR_ACCOUNT_KEY,
};

use super::common::KafkaTestCluster;

struct Azurite {
    _container: ContainerAsync<GenericImage>,
    /// `http://127.0.0.1:{port}/devstoreaccount1`
    endpoint: String,
}

/// A free TCP port on the host (racy, but only between this call and the
/// container start).
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("local_addr")
        .port()
}

/// Azurite listens on the same port number inside the container as on the
/// host. A copy whose source URL carries a SAS makes Azurite fetch that URL
/// itself, from inside the container; with a different host port it would
/// connect to nothing.
async fn start_azurite() -> Azurite {
    let port = free_port();
    let container = GenericImage::new("mcr.microsoft.com/azure-storage/azurite", "3.37.0")
        .with_exposed_port(port.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "Azurite Blob service successfully listens on",
        ))
        .with_cmd([
            "azurite-blob".to_string(),
            "--blobHost".to_string(),
            "0.0.0.0".to_string(),
            "--blobPort".to_string(),
            port.to_string(),
            "--skipApiVersionCheck".to_string(),
            "--loose".to_string(),
        ])
        .with_mapped_port(port, port.tcp())
        .start()
        .await
        .expect("start Azurite");
    Azurite {
        _container: container,
        endpoint: format!("http://127.0.0.1:{port}/{EMULATOR_ACCOUNT}"),
    }
}

/// Percent-encode the characters a SAS value can carry, the way the portal
/// does (`+ / = :`).
fn portal_encode(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace('+', "%2B")
        .replace('/', "%2F")
        .replace('=', "%3D")
        .replace(':', "%3A")
}

/// An account SAS for the emulator account (all blob permissions), in the
/// form the portal prints: leading `?`, `se` and `sig` percent-encoded.
fn portal_account_sas() -> String {
    let (sv, ss, srt, sp, se) = ("2020-08-04", "b", "sco", "rwdlacup", "2099-01-01T00:00:00Z");
    // Account SAS string-to-sign for versions before 2020-12-06:
    // account, sp, ss, srt, st, se, sip, spr, sv (each newline-terminated).
    let string_to_sign = format!("{EMULATOR_ACCOUNT}\n{sp}\n{ss}\n{srt}\n\n{se}\n\n\n{sv}\n");
    let key = ring::hmac::Key::new(
        ring::hmac::HMAC_SHA256,
        &BASE64.decode(EMULATOR_ACCOUNT_KEY).unwrap(),
    );
    let sig = BASE64.encode(ring::hmac::sign(&key, string_to_sign.as_bytes()).as_ref());
    format!(
        "?sv={sv}&ss={ss}&srt={srt}&sp={sp}&se={}&sig={}",
        portal_encode(se),
        portal_encode(&sig)
    )
}

/// Create `container` (object_store cannot), authorised by the account SAS.
async fn create_container(azurite: &Azurite, container: &str) {
    let sas = portal_account_sas();
    let url = format!(
        "{}/{container}?restype=container&{}",
        azurite.endpoint,
        sas.trim_start_matches('?')
    );
    let resp = reqwest::Client::new()
        .put(url)
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("create container request");
    assert!(
        resp.status().as_u16() == 201 || resp.status().as_u16() == 409,
        "create container {container}: {} {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );
}

fn emulator_config(azurite: &Azurite, container: &str) -> AzureConfig {
    AzureConfig {
        account_name: EMULATOR_ACCOUNT.to_string(),
        container_name: container.to_string(),
        account_key: None,
        prefix: Some("cluster-a".to_string()),
        endpoint: Some(azurite.endpoint.clone()),
        use_workload_identity: None,
        client_id: None,
        tenant_id: None,
        client_secret: None,
        sas_token: None,
        use_emulator: true,
        allow_http: false,
    }
}

/// How much of the API a credential can exercise against Azurite.
#[derive(Clone, Copy, PartialEq)]
enum Ops {
    All,
    /// object_store sends `delete` as a Blob Batch request, and Azurite
    /// 3.37 does not authorise Blob Batch with an account SAS
    /// ("OPERATION_ACCOUNT_SAS_PERMISSIONS doesn't have configuration for
    /// operation Container_SubmitBatch"). An emulator gap; real Azure is
    /// covered by the Azure compatibility run.
    AllButDelete,
}

/// Put, list, head, size, copy, get, delete and not-found mapping.
async fn exercise(backend: &dyn StorageBackend, tag: &str, ops: Ops) {
    let key = format!("{tag}/topics/orders/partition=0/segment-00000000000000000000.bin");
    let data = Bytes::from(format!("segment bytes for {tag}"));
    backend.put(&key, data.clone()).await.expect("put");
    assert!(backend.exists(&key).await.expect("exists"));
    assert_eq!(backend.get(&key).await.expect("get"), data);
    assert_eq!(backend.size(&key).await.expect("size"), data.len() as u64);
    assert_eq!(
        backend.head(&key).await.expect("head").size,
        data.len() as u64
    );
    let copy = format!("{tag}/copy");
    backend.copy(&key, &copy).await.expect("copy");
    assert_eq!(backend.get(&copy).await.expect("get copy"), data);
    let mut listed = backend.list(&format!("{tag}/")).await.expect("list");
    listed.sort();
    assert_eq!(listed, vec![copy.clone(), key.clone()]);
    let missing = format!("{tag}/never-written");
    assert!(!backend.exists(&missing).await.expect("exists missing"));
    let err = backend.get(&missing).await.expect_err("missing key");
    assert!(err.is_not_found(), "{err}");
    if ops == Ops::All {
        backend.delete(&key).await.expect("delete");
        backend.delete(&copy).await.expect("delete copy");
        assert!(!backend.exists(&key).await.expect("exists after delete"));
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn azurite_well_known_key_and_explicit_key_over_http() {
    let azurite = start_azurite().await;
    create_container(&azurite, "keys").await;

    // use_emulator with no credentials: the well-known emulator key.
    let backend = AzureBackend::new(emulator_config(&azurite, "keys")).expect("backend");
    exercise(&backend, "well-known-key", Ops::All).await;

    // An endpoint without the account path gets it appended.
    let mut config = emulator_config(&azurite, "keys");
    config.endpoint = Some(
        azurite
            .endpoint
            .trim_end_matches(&format!("/{EMULATOR_ACCOUNT}"))
            .to_string(),
    );
    exercise(
        &AzureBackend::new(config).expect("backend"),
        "bare-endpoint",
        Ops::All,
    )
    .await;

    // Not in emulator mode: explicit key, http:// endpoint implies allow_http.
    let mut config = emulator_config(&azurite, "keys");
    config.use_emulator = false;
    config.account_key = Some(EMULATOR_ACCOUNT_KEY.to_string());
    exercise(
        &AzureBackend::new(config).expect("backend"),
        "explicit-key",
        Ops::All,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn azurite_portal_style_sas_token() {
    let azurite = start_azurite().await;
    create_container(&azurite, "sas").await;

    let sas = portal_account_sas();
    assert!(
        sas.contains("%3D") && sas.contains("%3A"),
        "the token carries percent-encoded values: {sas}"
    );
    let mut config = emulator_config(&azurite, "sas");
    config.sas_token = Some(sas.clone());
    exercise(
        &AzureBackend::new(config).expect("backend"),
        "emulator-sas",
        Ops::AllButDelete,
    )
    .await;

    // Same token outside emulator mode (plain http endpoint).
    let mut config = emulator_config(&azurite, "sas");
    config.use_emulator = false;
    config.sas_token = Some(sas);
    exercise(
        &AzureBackend::new(config).expect("backend"),
        "endpoint-sas",
        Ops::AllButDelete,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn azurite_backup_and_restore_round_trip() {
    const SOURCE: &str = "azure-orders";
    const TARGET: &str = "azure-orders-restored";
    const RECORDS: usize = 30;
    const BACKUP_ID: &str = "azure-round-trip";

    let azurite = start_azurite().await;
    create_container(&azurite, "kafka-backups").await;
    let storage = StorageBackendConfig::Azure {
        account_name: EMULATOR_ACCOUNT.to_string(),
        container_name: "kafka-backups".to_string(),
        account_key: None,
        prefix: Some("dr".to_string()),
        endpoint: Some(azurite.endpoint.clone()),
        use_workload_identity: None,
        client_id: None,
        tenant_id: None,
        client_secret: None,
        sas_token: Some(portal_account_sas()),
        use_emulator: true,
        allow_http: false,
    };

    let cluster = KafkaTestCluster::start().await.expect("start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka ready");
    cluster
        .create_topic(SOURCE, RECORDS)
        .await
        .expect("seed topic");
    let kafka = |topics: Vec<String>| KafkaConfig {
        bootstrap_servers: vec![cluster.bootstrap_servers.clone()],
        security: SecurityConfig::default(),
        topics: TopicSelection {
            include: topics,
            exclude: vec![],
        },
        connection: Default::default(),
    };

    let backup = Config {
        mode: Mode::Backup,
        backup_id: BACKUP_ID.to_string(),
        source: Some(kafka(vec![SOURCE.to_string()])),
        target: None,
        storage: storage.clone(),
        backup: Some(BackupOptions {
            compression: CompressionType::Zstd,
            stop_at_current_offsets: true,
            continuous: false,
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
    };
    tokio::time::timeout(
        Duration::from_secs(90),
        BackupEngine::new(backup)
            .await
            .expect("backup engine")
            .run(),
    )
    .await
    .expect("backup timed out")
    .expect("backup");

    // The manifest and segments are in the Azure container.
    let backend = create_backend(&storage).expect("azure backend");
    let keys = backend.list(&format!("{BACKUP_ID}/")).await.expect("list");
    assert!(
        keys.contains(&format!("{BACKUP_ID}/manifest.json")),
        "{keys:?}"
    );
    assert!(keys.iter().any(|k| k.contains("/segment-")), "{keys:?}");

    let mut topic_mapping = HashMap::new();
    topic_mapping.insert(SOURCE.to_string(), TARGET.to_string());
    let restore = Config {
        mode: Mode::Restore,
        backup_id: BACKUP_ID.to_string(),
        source: None,
        target: Some(kafka(vec![])),
        storage,
        backup: None,
        restore: Some(RestoreOptions {
            topic_mapping,
            create_topics: true,
            ..RestoreOptions::default()
        }),
        offset_storage: None,
        metrics: None,
    };
    let report = tokio::time::timeout(
        Duration::from_secs(90),
        RestoreEngine::new(restore).expect("restore engine").run(),
    )
    .await
    .expect("restore timed out")
    .expect("restore");
    assert_eq!(report.records_restored, RECORDS as u64);

    let client = cluster.create_client();
    client.connect().await.expect("connect");
    let mut restored = 0;
    for partition in 0..3 {
        let (start, end) = client
            .get_offsets(
                TARGET,
                partition,
                kafka_backup_core::IsolationLevel::ReadUncommitted,
            )
            .await
            .expect("offsets");
        restored += end - start;
    }
    assert_eq!(restored, RECORDS as i64);
}
