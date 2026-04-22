//! `#[ignore]` E2E: GSSAPI (Kerberos) plugin against a real Kafka broker.
//!
//! Runs against the MIT-KDC-backed fixture at
//! `tests/sasl-gssapi-test-infra/`. The fixture creates a `TEST.LOCAL`
//! realm, exports a `client.keytab`, and advertises
//! `SASL_PLAINTEXT://kafka.test.local:9098` with `GSSAPI` enabled.
//!
//! ## Prerequisites
//!
//! 1. macOS: `brew install krb5` and export
//!    `PKG_CONFIG_PATH="$(brew --prefix krb5)/lib/pkgconfig:..."`.
//!    Linux: `apt-get install libkrb5-dev` (or equivalent).
//! 2. `/etc/hosts` contains `127.0.0.1 kafka.test.local kdc.test.local`.
//!    See `tests/sasl-gssapi-test-infra/README.md` for the one-liner.
//! 3. Start the fixture:
//!
//! ```bash
//! cd tests/sasl-gssapi-test-infra
//! docker compose -f docker-compose-gssapi.yml up -d --wait
//! ```
//!
//! ## Running
//!
//! ```bash
//! cargo test --features gssapi -p kafka-backup-core \
//!     --test integration_suite_tests -- --ignored sasl_gssapi_
//! ```

#![cfg(feature = "gssapi")]

use std::path::{Path, PathBuf};

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, RestoreOptions, SecurityConfig,
    SecurityProtocol, TopicSelection,
};
use kafka_backup_core::kafka::{
    GssapiPlugin, GssapiPluginFactory, KafkaClient, SaslMechanismPluginFactoryHandle, TopicToCreate,
};
use kafka_backup_core::manifest::BackupRecord;
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::storage::StorageBackendConfig;
use kafka_backup_core::BackupManifest;

const GSSAPI_BOOTSTRAP: &str = "kafka.test.local:9098";
const SERVICE_NAME: &str = "kafka";

/// Path to the keytab exported by the KDC container on `up`. Absolute
/// so the test works regardless of the cwd the harness chooses.
fn client_keytab_path() -> PathBuf {
    // CARGO_MANIFEST_DIR is set at compile time to the path of the
    // crate that owns this test (kafka-backup-core). Workspace root is
    // two levels up.
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../tests/sasl-gssapi-test-infra/keytabs/client.keytab")
}

fn krb5_config_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/sasl-gssapi-test-infra/krb5.conf")
}

/// Build a [`GssapiPluginFactory`] pointing at the fixture's keytab +
/// krb5.conf. The factory binds SPN host at `.build()` time, so the
/// same handle drops into every `KafkaConfig` the test constructs.
fn gssapi_factory() -> SaslMechanismPluginFactoryHandle {
    GssapiPluginFactory::new(
        SERVICE_NAME,
        Some(client_keytab_path()),
        Some(krb5_config_path()),
    )
    .expect("GssapiPluginFactory::new")
    .into_handle()
}

fn gssapi_config() -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![GSSAPI_BOOTSTRAP.to_string()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism_plugin_factory: Some(gssapi_factory()),
            ..Default::default()
        },
        topics: TopicSelection::default(),
        connection: Default::default(),
    }
}

/// E2E: happy-path keytab-based GSSAPI against a real broker.
///
/// Exercises Phase 1 context establishment, Phase 1→2 turnaround, and
/// Phase 2 wrap/unwrap. The post-auth Metadata RPC proves the session
/// survives the handshake, not just that the handshake completes.
#[tokio::test]
#[ignore = "requires Docker GSSAPI Kafka - run docker-compose-gssapi.yml first"]
async fn sasl_gssapi_keytab_e2e() {
    let client = KafkaClient::new(gssapi_config());

    let connect = client.connect().await;
    assert!(
        connect.is_ok(),
        "GSSAPI connect should succeed. Error: {:?}",
        connect.err()
    );

    let metadata = client.fetch_metadata(None).await;
    assert!(
        metadata.is_ok(),
        "GSSAPI metadata fetch should succeed. Error: {:?}",
        metadata.err()
    );
}

/// E2E: keytab missing at construction time surfaces a clear error.
///
/// `GssapiPluginFactory::new` validates up front (before we touch
/// GSSAPI), so the operator sees "keytab not found" with the bad path
/// rather than a cryptic GSS minor-status buried three layers deep.
/// This used to live on `GssapiPlugin::new`; the factory kept the
/// same eager-validation contract.
#[tokio::test]
#[ignore = "requires Docker GSSAPI Kafka - run docker-compose-gssapi.yml first"]
async fn sasl_gssapi_missing_keytab_surfaces_clear_error() {
    let bad_path = PathBuf::from("/nonexistent/path/client.keytab");

    let result = GssapiPluginFactory::new(
        SERVICE_NAME,
        Some(bad_path.clone()),
        Some(krb5_config_path()),
    );

    let err = result.expect_err("should reject missing keytab");
    let msg = format!("{err}");
    assert!(msg.contains("keytab"), "error should mention keytab: {msg}");
    assert!(
        msg.contains("nonexistent"),
        "error should surface the bad path: {msg}"
    );

    // And for completeness: the direct plugin path also rejects.
    let plugin_result = GssapiPlugin::new(
        SERVICE_NAME,
        "kafka.test.local",
        Some(bad_path),
        Some(krb5_config_path()),
    );
    plugin_result.expect_err("GssapiPlugin::new also rejects missing keytab");
}

/// E2E: GSSAPI sessions survive the broker's advertised reauth window
/// via the drain-and-reconnect path, not live KIP-368 re-authentication.
///
/// `GssapiPlugin::supports_reauth()` returns `false` — Apache Kafka
/// rejects in-place `SaslAuthenticate` for Kerberos connections, so the
/// client does not schedule a reauth task. Instead, once the broker
/// expires the session (fixture sets `KAFKA_CONNECTIONS_MAX_REAUTH_MS=
/// 60000`), the next RPC observes a broken pipe, the client reconnects
/// through the normal auth path, and the fetch succeeds.
///
/// We hold the client for 90s with metadata probes every 15s — crossing
/// the 60s window at least once. Every probe must succeed; the point
/// of the test is that the session-continuity guarantee survives
/// without any live-reauth scheduler firing.
#[tokio::test]
#[ignore = "requires Docker GSSAPI Kafka + 90s runtime - run docker-compose-gssapi.yml first"]
async fn sasl_gssapi_session_expires_without_live_reauth() {
    let client = KafkaClient::new(gssapi_config());
    client.connect().await.expect("initial GSSAPI connect");

    // Probe every 15s for 90s — crosses the broker's 60s window at
    // least once. Every probe must succeed: either because the session
    // is still live, or because the reconnect-on-next-RPC path
    // transparently re-authenticates after the broker expires it.
    for tick in 0..6 {
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        let md = client.fetch_metadata(None).await;
        assert!(
            md.is_ok(),
            "metadata fetch should succeed after {}s — \
             drain-and-reconnect must have carried the session across \
             the broker's reauth window: {:?}",
            (tick + 1) * 15,
            md.err()
        );
    }
}

/// E2E: full backup → restore roundtrip against a live Kerberized broker.
///
/// The three tests above prove the handshake works and survives reauth,
/// but only via metadata-only probes. This one exercises the engines
/// we actually ship (`BackupEngine` + `RestoreEngine`) end-to-end: it
/// produces records over GSSAPI, backs them up to local disk, restores
/// them to a remapped topic, and consumes from the remap to verify
/// record count + payload. If GSSAPI silently drops mid-produce or
/// mid-fetch under a producer/consumer workload, this test catches it
/// where the metadata-only tests would not.
#[tokio::test]
#[ignore = "requires Docker GSSAPI Kafka - run docker-compose-gssapi.yml first"]
async fn sasl_gssapi_backup_restore_roundtrip() {
    use chrono::Utc;
    use tempfile::TempDir;

    // `try_init` so reruns under the same process don't panic.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_test_writer()
        .try_init();

    // One factory, shared across producer + backup + restore configs.
    // `GssapiPluginFactory::build` hands every `KafkaClient` its own
    // fresh `GssapiPlugin` so per-connection GSS context state does not
    // get clobbered by sibling handshakes or KIP-368 reauth firing on
    // another connection in the pool.
    let factory = gssapi_factory();

    // Per-run topic name so reruns do not collide on the same fixture.
    let run_id = Utc::now().timestamp_millis();
    let source_topic = format!("gssapi-roundtrip-{run_id}");
    let restored_topic = format!("{source_topic}-restored");
    let backup_id = format!("gssapi-roundtrip-{run_id}");
    let record_count: usize = 50;

    // --- Produce records ----------------------------------------------
    {
        let producer_config = KafkaConfig {
            bootstrap_servers: vec![GSSAPI_BOOTSTRAP.to_string()],
            security: SecurityConfig {
                security_protocol: SecurityProtocol::SaslPlaintext,
                sasl_mechanism_plugin_factory: Some(factory.clone()),
                ..Default::default()
            },
            topics: TopicSelection::default(),
            connection: Default::default(),
        };
        let producer_client = KafkaClient::new(producer_config);
        producer_client
            .connect()
            .await
            .expect("producer: GSSAPI connect");

        // Explicitly create the topic before producing. Auto-create is
        // enabled on the broker, but the first `produce` RPC races
        // against topic auto-create and can see
        // `UNKNOWN_TOPIC_OR_PARTITION` (code 3) before the metadata
        // update propagates.
        // Create both topics up front. The restored topic has to exist
        // before RestoreEngine's first produce — auto-create works, but
        // the first produce RPC can race metadata propagation and see
        // `Partition 0 not available` on an auto-created topic.
        let create_results = producer_client
            .create_topics(
                vec![
                    TopicToCreate {
                        name: source_topic.clone(),
                        num_partitions: 1,
                        replication_factor: 1,
                    },
                    TopicToCreate {
                        name: restored_topic.clone(),
                        num_partitions: 1,
                        replication_factor: 1,
                    },
                ],
                30_000,
            )
            .await
            .expect("create_topics over GSSAPI");
        assert!(
            create_results.iter().all(|r| r.is_success_or_exists()),
            "topic create failed: {create_results:?}"
        );

        let records: Vec<BackupRecord> = (0..record_count)
            .map(|i| BackupRecord {
                key: Some(format!("key-{i}").into_bytes()),
                value: Some(format!("value-{i}-{source_topic}").into_bytes()),
                headers: vec![],
                timestamp: Utc::now().timestamp_millis() + i as i64,
                offset: i as i64,
            })
            .collect();

        for record in &records {
            producer_client
                .produce(&source_topic, 0, vec![record.clone()], -1, 30_000)
                .await
                .expect("produce over GSSAPI");
        }

        // Drop before the backup's KafkaClients connect — cancels the
        // producer's KIP-368 reauth task so its wake-up cannot land in
        // the middle of the backup handshake on a shared env lock.
    }

    // --- Backup -------------------------------------------------------
    let temp_dir = TempDir::new().expect("tempdir");
    let storage_path = temp_dir.path().to_path_buf();

    let backup_config = Config {
        mode: Mode::Backup,
        backup_id: backup_id.clone(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec![GSSAPI_BOOTSTRAP.to_string()],
            security: SecurityConfig {
                security_protocol: SecurityProtocol::SaslPlaintext,
                sasl_mechanism_plugin_factory: Some(factory.clone()),
                ..Default::default()
            },
            topics: TopicSelection {
                include: vec![source_topic.clone()],
                exclude: vec![],
            },
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem {
            path: storage_path.clone(),
        },
        backup: Some(BackupOptions {
            segment_max_bytes: 1024 * 1024,
            segment_max_interval_ms: 10_000,
            compression: CompressionType::Zstd,
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
    };

    let backup_engine = BackupEngine::new(backup_config)
        .await
        .expect("BackupEngine::new");
    backup_engine.run().await.expect("backup run");

    // Manifest sanity — proves the engine emitted backup state.
    let manifest_path = storage_path.join(&backup_id).join("manifest.json");
    assert!(
        manifest_path.exists(),
        "manifest.json missing at {manifest_path:?}"
    );
    let manifest_bytes = std::fs::read(&manifest_path).expect("read manifest");
    let manifest: BackupManifest = serde_json::from_slice(&manifest_bytes).expect("parse manifest");
    assert_eq!(manifest.backup_id, backup_id);
    assert!(!manifest.topics.is_empty(), "manifest has no topics");

    // --- Restore with topic remap ------------------------------------
    let mut restore_opts = RestoreOptions::default();
    restore_opts
        .topic_mapping
        .insert(source_topic.clone(), restored_topic.clone());

    let restore_config = Config {
        mode: Mode::Restore,
        backup_id: backup_id.clone(),
        source: None,
        target: Some(KafkaConfig {
            bootstrap_servers: vec![GSSAPI_BOOTSTRAP.to_string()],
            security: SecurityConfig {
                security_protocol: SecurityProtocol::SaslPlaintext,
                sasl_mechanism_plugin_factory: Some(factory.clone()),
                ..Default::default()
            },
            topics: TopicSelection {
                include: vec![source_topic.clone()],
                exclude: vec![],
            },
            connection: Default::default(),
        }),
        storage: StorageBackendConfig::Filesystem {
            path: storage_path.clone(),
        },
        backup: None,
        restore: Some(restore_opts),
        offset_storage: None,
        metrics: None,
    };

    let restore_engine = RestoreEngine::new(restore_config).expect("RestoreEngine::new");
    restore_engine.run().await.expect("restore run");

    // --- Consume restored topic and verify ---------------------------
    let consumer_config = KafkaConfig {
        bootstrap_servers: vec![GSSAPI_BOOTSTRAP.to_string()],
        security: SecurityConfig {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism_plugin_factory: Some(factory.clone()),
            ..Default::default()
        },
        topics: TopicSelection::default(),
        connection: Default::default(),
    };
    let consumer_client = KafkaClient::new(consumer_config);
    consumer_client
        .connect()
        .await
        .expect("consumer: GSSAPI connect");

    // AUTO_CREATE_TOPICS is on, so a fetch against the restored topic
    // will succeed even before the producer's batch is visible, but
    // the high-watermark will be 0 until the restore has actually
    // appended. Pull with a small retry loop.
    let mut fetched: Vec<BackupRecord> = Vec::new();
    for _ in 0..20 {
        let resp = consumer_client
            .fetch(&restored_topic, 0, 0, 1_048_576)
            .await
            .expect("fetch restored topic");
        if resp.records.len() >= record_count {
            fetched = resp.records;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    assert_eq!(
        fetched.len(),
        record_count,
        "restored record count mismatch: got {}, expected {record_count}",
        fetched.len()
    );

    // Payload assertion — order in a single partition is guaranteed.
    for (i, got) in fetched.iter().enumerate() {
        let expected_value = format!("value-{i}-{source_topic}");
        assert_eq!(
            got.value.as_deref(),
            Some(expected_value.as_bytes()),
            "record {i} value mismatch"
        );
    }
}
