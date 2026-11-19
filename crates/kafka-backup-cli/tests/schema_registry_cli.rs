//! Schema Registry backup and restore through the CLI (issue #262),
//! black-box: the registries are the stateful fake from the core tests and
//! Kafka is a listener that only counts connections, so "the registry comes
//! first" and "nothing is produced after a registry failure" are observable.

#[path = "../../kafka-backup-core/tests/schema_registry_suite/fake_schema_registry.rs"]
mod fake_schema_registry;

use std::fs;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use fake_schema_registry::{FakeSchemaRegistry, Schema};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const BACKUP_ID: &str = "sr-cli";

/// A "Kafka broker" that accepts connections, counts them and hangs up.
struct CountingKafka {
    address: String,
    connections: Arc<AtomicUsize>,
}

impl CountingKafka {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let connections = Arc::new(AtomicUsize::new(0));
        let counter = connections.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                counter.fetch_add(1, Ordering::SeqCst);
                drop(stream);
            }
        });
        Self {
            address,
            connections,
        }
    }

    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    storage: PathBuf,
    config: PathBuf,
    kafka: CountingKafka,
}

impl Fixture {
    fn config(&self) -> &str {
        self.config.to_str().unwrap()
    }

    fn has(&self, key: &str) -> bool {
        self.storage.join(key).exists()
    }

    fn json(&self, key: &str) -> serde_json::Value {
        let path = self.storage.join(key);
        serde_json::from_slice(&fs::read(&path).unwrap_or_else(|e| panic!("{path:?}: {e}")))
            .unwrap()
    }

    fn report(&self, name: &str) -> serde_json::Value {
        self.json(&format!("{BACKUP_ID}/restore/{name}"))
    }
}

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
        .args(args)
        .env("RUST_LOG", "info")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .output()
        .unwrap()
}

fn text(output: &std::process::Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The source registry: a reference, a soft-deleted version and a subject
/// with its own mode.
async fn source() -> (MockServer, FakeSchemaRegistry) {
    let (server, sr) = FakeSchemaRegistry::start().await;
    sr.seed("address-value", 1, 1, Schema::avro(r#""string""#));
    sr.seed(
        "orders-value",
        1,
        2,
        Schema::avro(r#"{"type":"record","name":"O","fields":[{"name":"a","type":"string"}]}"#)
            .with_ref("a", "address-value", 1),
    );
    sr.seed("payments-value", 1, 3, Schema::avro(r#""long""#));
    sr.soft_delete("payments-value", 1);
    sr.set_mode("orders-value", "READONLY");
    (server, sr)
}

fn registry_yaml(url: &str, extra: &str) -> String {
    format!(
        "schema_registry:\n  url: {url}\n  connection:\n    max_retries: 0\n    retry_backoff_ms: 1\n{extra}"
    )
}

/// A backup configuration: `registry` is the whole `schema_registry`
/// section (or nothing).
fn backup_fixture(registry: &str, backup_extra: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let storage = dir.path().join("storage");
    fs::create_dir_all(&storage).unwrap();
    let kafka = CountingKafka::start();
    let config = dir.path().join("backup.yaml");
    fs::write(
        &config,
        format!(
            r#"
mode: backup
backup_id: {BACKUP_ID}
source:
  bootstrap_servers: ["{kafka}"]
  topics:
    include: ["orders"]
storage:
  backend: filesystem
  path: {storage}
metrics:
  enabled: false
backup:
  stop_at_current_offsets: false
{backup_extra}
{registry}
"#,
            kafka = kafka.address,
            storage = storage.display(),
        ),
    )
    .unwrap();
    Fixture {
        _dir: dir,
        storage,
        config,
        kafka,
    }
}

/// A restore configuration over a backup set made with the CLI itself:
/// `backup --schema-only` of [`source`], plus a Kafka data manifest with one
/// empty topic so the record restore has something to read.
async fn restore_fixture(target_url: &str, registry_extra: &str, extra_yaml: &str) -> Fixture {
    let (src, _source) = source().await;
    let backup = backup_fixture(&registry_yaml(&src.uri(), ""), "");
    let out = run(&["backup", "--schema-only", "--config", backup.config()]);
    assert!(out.status.success(), "{}", text(&out));

    let now = chrono::Utc::now().timestamp_millis();
    let manifest = serde_json::json!({
        "backup_id": BACKUP_ID,
        "created_at": now - 60_000,
        "compression": "zstd",
        "topics": [{"name": "orders", "original_partition_count": 1,
                    "partitions": [{"partition_id": 0, "segments": []}]}]
    });
    fs::write(
        backup.storage.join(BACKUP_ID).join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();

    let kafka = CountingKafka::start();
    let config = backup._dir.path().join("restore.yaml");
    fs::write(
        &config,
        format!(
            r#"
mode: restore
backup_id: {BACKUP_ID}
target:
  bootstrap_servers: ["{kafka}"]
storage:
  backend: filesystem
  path: {storage}
metrics:
  enabled: false
{extra_yaml}
{registry}
"#,
            kafka = kafka.address,
            storage = backup.storage.display(),
            registry = if target_url.is_empty() {
                String::new()
            } else {
                registry_yaml(target_url, registry_extra)
            },
        ),
    )
    .unwrap();
    Fixture {
        storage: backup.storage.clone(),
        config,
        kafka,
        _dir: backup._dir,
    }
}

const MANIFEST: &str = "sr-cli/schema-registry/_manifest.json";
const STATUS: &str = "sr-cli/schema-registry/_status.json";

// ===========================================================================
// backup
// ===========================================================================

#[tokio::test]
async fn backup_schema_only_snapshots_the_registry_without_connecting_to_kafka() {
    let (src, source) = source().await;
    let fx = backup_fixture(&registry_yaml(&src.uri(), ""), "");
    let out = run(&["backup", "--schema-only", "--config", fx.config()]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(
        all.contains("Schema Registry backup: 3 subject(s), 3 version(s) (1 soft-deleted)"),
        "{all}"
    );
    assert_eq!(fx.kafka.connections(), 0, "{all}");

    let manifest = fx.json(MANIFEST);
    assert_eq!(manifest["total_versions"], 3);
    assert_eq!(manifest["soft_deleted_versions"], 1);
    // The manifest carries a checksum for every object of the snapshot.
    let objects = manifest["snapshot"]["objects"].as_object().unwrap();
    assert_eq!(objects.len(), 1 + 3 + 3, "{objects:?}");
    for (key, checksum) in objects {
        assert!(fx.has(key), "{key}");
        assert!(checksum.as_str().unwrap().starts_with("sha256:"));
    }
    assert_eq!(fx.json(STATUS)["complete"], true);
    // A backup only reads.
    assert!(source.mutations().is_empty(), "{:?}", source.mutations());
}

#[tokio::test]
async fn backup_schema_only_fails_and_marks_the_snapshot_incomplete_when_the_registry_fails() {
    let (src, source) = source().await;
    source.fail_once("GET", "/subjects/orders-value/versions/1", 500, 50001);
    // fail_on_partial: false is the default: --schema-only fails anyway,
    // there is nothing else the command could have succeeded at.
    let fx = backup_fixture(&registry_yaml(&src.uri(), ""), "");
    let out = run(&["backup", "--schema-only", "--config", fx.config()]);
    let all = text(&out);
    assert!(!out.status.success(), "{all}");
    assert!(all.contains("Schema Registry backup failed"), "{all}");
    assert!(all.contains("marked incomplete"), "{all}");
    let status = fx.json(STATUS);
    assert_eq!(status["complete"], false);
    assert!(
        status["error"]
            .as_str()
            .unwrap()
            .contains("injected failure"),
        "{status}"
    );
    assert!(!fx.has(MANIFEST), "no manifest for an incomplete snapshot");

    // A restore refuses the incomplete snapshot before touching the target.
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let restore = fx._dir.path().join("restore.yaml");
    fs::write(
        &restore,
        format!(
            "mode: restore\nbackup_id: {BACKUP_ID}\nstorage:\n  backend: filesystem\n  path: {}\n{}",
            fx.storage.display(),
            registry_yaml(&tgt.uri(), "")
        ),
    )
    .unwrap();
    let out = run(&[
        "restore",
        "--schema-only",
        "--config",
        restore.to_str().unwrap(),
    ]);
    assert!(!out.status.success(), "{}", text(&out));
    assert!(target.mutations().is_empty());
}

#[tokio::test]
async fn schema_only_needs_an_enabled_schema_registry_section() {
    let fx = backup_fixture("", "");
    let out = run(&["backup", "--schema-only", "--config", fx.config()]);
    let all = text(&out);
    assert!(!out.status.success(), "{all}");
    assert!(all.contains("no enabled schema_registry section"), "{all}");

    let fx = restore_fixture("", "", "").await;
    for command in ["restore", "validate-restore"] {
        let out = run(&[command, "--schema-only", "--config", fx.config()]);
        let all = text(&out);
        assert!(!out.status.success(), "{command}: {all}");
        assert!(
            all.contains("no schema_registry section"),
            "{command}: {all}"
        );
    }
    assert_eq!(fx.kafka.connections(), 0);
}

#[tokio::test]
async fn a_kafka_failure_still_takes_the_registry_snapshot_and_fails_the_backup() {
    let (src, _source) = source().await;
    let fx = backup_fixture(&registry_yaml(&src.uri(), ""), "");
    let out = run(&["backup", "--config", fx.config()]);
    let all = text(&out);
    assert!(
        !out.status.success(),
        "a failed data backup must fail the run\n{all}"
    );
    assert!(all.contains("Kafka data backup failed"), "{all}");
    assert!(
        all.contains("the Schema Registry snapshot completed"),
        "{all}"
    );
    assert!(fx.has(MANIFEST), "{all}");
    assert_eq!(fx.json(STATUS)["complete"], true);
}

#[tokio::test]
async fn a_continuous_backup_snapshots_at_start_and_at_shutdown() {
    let (src, source) = source().await;
    let fx = backup_fixture(
        &registry_yaml(&src.uri(), "  backup:\n    snapshot_interval: 1h\n"),
        "  continuous: true\n",
    );
    // Kafka hangs up, so the engine stops at once: the start pass and the
    // shutdown pass both ran, the hourly tick did not.
    let out = run(&["backup", "--config", fx.config()]);
    let all = text(&out);
    assert!(!out.status.success(), "{all}");
    assert!(
        all.contains("snapshotted at start, every interval and at shutdown"),
        "{all}"
    );
    assert!(all.contains("interval=1h"), "{all}");
    assert!(all.contains("pass=\"shutdown\""), "{all}");
    assert!(fx.has(MANIFEST), "{all}");
    let listings = source
        .log()
        .iter()
        .filter(|l| l.starts_with("GET /subjects?deleted=true"))
        .count();
    assert!((1..=2).contains(&listings), "{listings} passes\n{all}");
}

#[test]
fn unknown_keys_in_the_schema_registry_section_are_errors() {
    for (registry, key) in [
        ("schema_registry:\n  url: http://sr\n  backup:\n    subjectz: [a]\n", "subjectz"),
        ("schema_registry:\n  url: http://sr\n  retore: {}\n", "retore"),
        (
            "schema_registry:\n  url: http://sr\n  auth: { type: basic, username: u, password: p, token: t }\n",
            "token",
        ),
    ] {
        let fx = backup_fixture(registry, "");
        let out = run(&["backup", "--schema-only", "--config", fx.config()]);
        let all = text(&out);
        assert!(!out.status.success(), "{key}: {all}");
        assert!(all.contains("unknown field"), "{key}: {all}");
        assert!(all.contains(key), "{key}: {all}");
    }
    // A misspelt section name is reported like any other unknown key.
    let fx = backup_fixture("schema_registy:\n  url: http://sr\n", "");
    let out = run(&["describe", "--config", fx.config()]);
    assert!(
        text(&out).contains("Ignoring unknown config key `schema_registy`"),
        "{}",
        text(&out)
    );
}

// ===========================================================================
// restore
// ===========================================================================

#[tokio::test]
async fn restore_schema_only_restores_the_registry_and_writes_the_report() {
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let fx = restore_fixture(&tgt.uri(), "", "").await;
    let out = run(&["restore", "--schema-only", "--config", fx.config()]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(all.contains("=== Schema Registry restore ==="), "{all}");
    assert!(all.contains("3 registered"), "{all}");
    assert!(
        all.contains("Verified: 3 version(s), 0 mismatch(es)"),
        "{all}"
    );
    assert_eq!(fx.kafka.connections(), 0, "{all}");

    let snapshot = target.snapshot();
    assert_eq!(snapshot["address-value"][&1].0, 1);
    assert_eq!(snapshot["orders-value"][&1].0, 2, "original schema ID");
    assert!(snapshot["payments-value"][&1].1, "soft-deleted again");
    assert_eq!(target.modes()["orders-value"], "READONLY");
    assert_eq!(target.global_mode(), "READWRITE");

    let r = fx.report("schema-registry-report.json");
    assert_eq!(r["success"], true);
    assert_eq!(r["strategy"], "preserve_ids");
    assert_eq!(r["verification"]["versions_checked"], 3);

    // A second run changes nothing.
    target.clear_log();
    let out = run(&["restore", "--schema-only", "--config", fx.config()]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(all.contains("0 registered, 3 already present"), "{all}");
    assert!(
        !target.mutations().iter().any(|m| m.starts_with("POST")),
        "{:?}",
        target.mutations()
    );
    assert_eq!(target.snapshot(), snapshot);
}

#[tokio::test]
async fn restore_and_three_phase_restore_do_the_registry_first_then_the_records() {
    for command in ["restore", "three-phase-restore"] {
        let (tgt, target) = FakeSchemaRegistry::start().await;
        let fx = restore_fixture(&tgt.uri(), "", "").await;
        let out = run(&[command, "--config", fx.config()]);
        let all = text(&out);
        // "Kafka" hangs up, so the record restore fails, after the registry
        // was restored.
        assert!(!out.status.success(), "{command}: {all}");
        assert!(
            all.contains("=== Schema Registry restore ==="),
            "{command}: {all}"
        );
        assert!(
            target.snapshot().contains_key("orders-value"),
            "{command}: {all}"
        );
        assert_eq!(
            fx.report("schema-registry-report.json")["success"],
            true,
            "{command}"
        );
        assert!(
            fx.kafka.connections() > 0,
            "{command}: the record restore must have started\n{all}"
        );
    }
}

#[tokio::test]
async fn a_registry_failure_stops_the_restore_before_any_kafka_connection() {
    for command in ["restore", "three-phase-restore"] {
        let (tgt, target) = FakeSchemaRegistry::start().await;
        // ID 2 holds another schema: a strict into_existing conflict.
        target.seed("squatter", 1, 2, Schema::avro(r#""bytes""#));
        let fx = restore_fixture(&tgt.uri(), "  restore:\n    strategy: into_existing\n", "").await;
        let out = run(&[command, "--config", fx.config()]);
        let all = text(&out);
        assert!(!out.status.success(), "{command}: {all}");
        assert!(
            all.contains("Schema Registry restore failed"),
            "{command}: {all}"
        );
        assert!(
            all.contains("The Kafka records were not restored"),
            "{command}: {all}"
        );
        assert!(all.contains("orders-value v1 (ID 2)"), "{command}: {all}");
        assert!(target.mutations().is_empty(), "{command}");
        assert_eq!(fx.kafka.connections(), 0, "{command}: {all}");
        // The report is written on failure too.
        assert_eq!(
            fx.report("schema-registry-report.json")["success"],
            false,
            "{command}"
        );
    }
}

#[tokio::test]
async fn schema_only_dry_run_lists_the_plan_and_changes_nothing() {
    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.seed("address-value", 1, 1, Schema::avro(r#""string""#));
    let fx = restore_fixture(
        &tgt.uri(),
        "  restore:\n    strategy: into_existing\n    dry_run: true\n",
        "",
    )
    .await;
    let out = run(&["restore", "--schema-only", "--config", fx.config()]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(all.contains("(dry run)"), "{all}");
    assert!(all.contains("skip     address-value v1 (ID 1)"), "{all}");
    assert!(all.contains("register orders-value v1 (ID 2)"), "{all}");
    assert!(
        all.contains("register payments-value v1 (ID 3) [soft-deleted]"),
        "{all}"
    );
    assert!(target.mutations().is_empty());
    assert_eq!(
        fx.report("schema-registry-report.dry-run.json")["dry_run"],
        true
    );
    assert!(!fx.has("sr-cli/restore/schema-registry-report.json"));
}

#[tokio::test]
async fn a_dry_run_flag_in_either_section_makes_the_whole_restore_read_only() {
    for (registry_extra, extra) in [
        ("  restore:\n    dry_run: true\n", ""),
        ("", "restore:\n  dry_run: true\n"),
    ] {
        let (tgt, target) = FakeSchemaRegistry::start().await;
        let fx = restore_fixture(&tgt.uri(), registry_extra, extra).await;
        let out = run(&["restore", "--config", fx.config()]);
        let all = text(&out);
        assert!(out.status.success(), "{all}");
        assert!(all.contains("(dry run)"), "{all}");
        assert!(target.mutations().is_empty(), "{all}");
        assert_eq!(
            fx.report("schema-registry-report.dry-run.json")["dry_run"],
            true
        );
    }
}

/// `rewrite_ids` hands the complete ID map to the record restore, on the
/// repartitioning path too.
#[tokio::test]
async fn rewrite_ids_restores_the_registry_and_feeds_the_record_rewrite() {
    let (tgt, target) = FakeSchemaRegistry::start().await;
    // ID 2 (orders-value v1 in the backup) is held by another schema.
    target.seed("squatter-value", 1, 2, Schema::avro(r#""bytes""#));
    let fx = restore_fixture(
        &tgt.uri(),
        "  restore:\n    strategy: into_existing\n    rewrite_ids: true\n",
        "restore:\n  repartitioning:\n    orders:\n      target_partitions: 3\n",
    )
    .await;
    let out = run(&["restore", "--config", fx.config()]);
    let all = text(&out);
    let r = fx.report("schema-registry-report.json");
    assert_eq!(r["success"], true, "{r}\n{all}");
    let changed = r["id_mapping"].as_object().expect("id_mapping");
    assert!(
        all.contains(&format!(
            "Schema IDs: {} changed; the restored records' Confluent wire-format IDs are \
             rewritten (every restored topic",
            changed.len()
        )),
        "{all}"
    );
    let new_id = r["id_mapping"]["2"].as_i64().expect("ID 2 rewritten");
    assert_ne!(new_id, 2);
    assert_eq!(target.snapshot()["orders-value"][&1].0 as i64, new_id);
    // The squatter kept its ID and its schema.
    assert_eq!(target.snapshot()["squatter-value"][&1].0, 2);
    // The unchanged IDs are recorded for the strict record rewrite.
    let preserved: Vec<i64> = r["ids_preserved"]
        .as_array()
        .expect("ids_preserved")
        .iter()
        .map(|v| v.as_i64().unwrap())
        .collect();
    assert!(preserved.contains(&1), "{r}");
    assert!(
        preserved
            .iter()
            .all(|id| !changed.contains_key(&id.to_string())),
        "{r}"
    );
    // Then the record restore ran (and failed on the fake broker).
    assert!(!out.status.success(), "{all}");
    assert!(fx.kafka.connections() > 0, "{all}");
}

#[tokio::test]
async fn a_disabled_section_leaves_the_restore_as_it_was() {
    let (tgt, target) = FakeSchemaRegistry::start().await;
    let fx = restore_fixture(&tgt.uri(), "  enabled: false\n", "").await;
    let out = run(&["restore", "--config", fx.config()]);
    let all = text(&out);
    assert!(!all.contains("Schema Registry restore"), "{all}");
    assert!(target.log().is_empty());
}

// ===========================================================================
// validate-restore
// ===========================================================================

#[tokio::test]
async fn validate_restore_schema_only_shows_the_plan_and_writes_nothing() {
    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.seed("address-value", 1, 1, Schema::avro(r#""string""#));
    let fx = restore_fixture(&tgt.uri(), "  restore:\n    strategy: into_existing\n", "").await;

    let out = run(&["validate-restore", "--schema-only", "--config", fx.config()]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(all.contains("skip     address-value v1 (ID 1)"), "{all}");
    assert!(all.contains("register orders-value v1 (ID 2)"), "{all}");
    assert!(all.contains("Validation: PASSED"), "{all}");

    let out = run(&[
        "validate-restore",
        "--schema-only",
        "--format",
        "json",
        "--config",
        fx.config(),
    ]);
    assert!(out.status.success(), "{}", text(&out));
    let json: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("stdout is the JSON report only");
    assert_eq!(json["valid"], true);
    assert_eq!(json["schema_registry"]["dry_run"], true);
    let actions: Vec<&str> = json["schema_registry"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["action"].as_str().unwrap())
        .collect();
    assert_eq!(actions, ["skip", "register", "register"]);

    // Read-only everywhere: the registry, the storage and Kafka.
    assert!(target.mutations().is_empty());
    assert!(!fx.has("sr-cli/restore/schema-registry-report.dry-run.json"));
    assert!(!fx.has("sr-cli/restore/schema-registry-report.json"));
    assert_eq!(fx.kafka.connections(), 0);
}

#[tokio::test]
async fn validate_restore_reports_a_registry_conflict_and_fails() {
    let (tgt, target) = FakeSchemaRegistry::start().await;
    target.seed("squatter", 1, 2, Schema::avro(r#""bytes""#));
    let fx = restore_fixture(&tgt.uri(), "  restore:\n    strategy: into_existing\n", "").await;

    let out = run(&[
        "validate-restore",
        "--schema-only",
        "--format",
        "json",
        "--config",
        fx.config(),
    ]);
    assert!(!out.status.success(), "{}", text(&out));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["valid"], false);
    assert_eq!(
        json["schema_registry"]["conflicts"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // The full validation carries the registry plan and fails on it too.
    let out = run(&[
        "validate-restore",
        "--format",
        "json",
        "--config",
        fx.config(),
    ]);
    assert!(!out.status.success(), "{}", text(&out));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["valid"], false);
    assert!(json["errors"]
        .as_array()
        .unwrap()
        .iter()
        .any(|e| e.as_str().unwrap().contains("Schema Registry")));
    assert_eq!(json["schema_registry"]["success"], false);
    assert!(target.mutations().is_empty());
}

#[tokio::test]
async fn validate_restore_without_a_registry_keeps_its_output_shape() {
    let fx = restore_fixture("", "", "").await;
    let out = run(&[
        "validate-restore",
        "--format",
        "json",
        "--config",
        fx.config(),
    ]);
    assert!(out.status.success(), "{}", text(&out));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(json["backup_id"], BACKUP_ID);
    assert!(json.get("schema_registry").is_none(), "{json}");
}

// ===========================================================================
// OAuth
// ===========================================================================

fn oauth_registry_yaml(registry: &str, idp: &str, provider: &str) -> String {
    format!(
        r#"schema_registry:
  url: {registry}
  auth:
    type: oauth
    provider: {provider}
    target_sr_cluster: lsrc-abc123
    identity_pool_id: pool-Ab1
  connection:
    max_retries: 0
oauth:
  providers:
    idp:
      type: oidc_client_credentials
      token_endpoint_url: {idp}/token
      client_id: kafka-backup
      client_secret: cli-client-secret
      allow_insecure_http: true
"#
    )
}

/// Every `--schema-only` command authenticates to the registry with a token
/// from the configured provider and sends the Confluent Cloud headers.
#[tokio::test]
async fn schema_only_commands_authenticate_to_the_registry_with_the_oauth_provider() {
    for command in ["backup", "restore", "validate-restore"] {
        let idp = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "cli-registry-token", "token_type": "Bearer", "expires_in": 3600
            })))
            .expect(1)
            .mount(&idp)
            .await;
        // Records what each request carried; answers like an empty registry.
        let registry = MockServer::start().await;
        Mock::given(wiremock::matchers::any())
            .respond_with(
                ResponseTemplate::new(404).set_body_json(
                    serde_json::json!({"error_code": 40401, "message": "not found"}),
                ),
            )
            .mount(&registry)
            .await;

        let fx = if command == "backup" {
            backup_fixture(&oauth_registry_yaml(&registry.uri(), &idp.uri(), "idp"), "")
        } else {
            let fx = restore_fixture("", "", "").await;
            let mut config = fs::read_to_string(&fx.config).unwrap();
            config.push_str(&oauth_registry_yaml(&registry.uri(), &idp.uri(), "idp"));
            fs::write(&fx.config, config).unwrap();
            fx
        };
        let out = run(&[command, "--schema-only", "--config", fx.config()]);
        let all = text(&out);
        let requests = registry.received_requests().await.unwrap();
        assert!(!requests.is_empty(), "{command}: {all}");
        for request in &requests {
            let header = |name: &str| {
                request
                    .headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string()
            };
            assert_eq!(
                header("authorization"),
                "Bearer cli-registry-token",
                "{command}"
            );
            assert_eq!(header("target-sr-cluster"), "lsrc-abc123", "{command}");
            assert_eq!(
                header("confluent-identity-pool-id"),
                "pool-Ab1",
                "{command}"
            );
        }
        assert!(!all.contains("cli-registry-token"), "{command}: {all}");
        assert!(!all.contains("cli-client-secret"), "{command}: {all}");
        assert_eq!(fx.kafka.connections(), 0, "{command}");
    }
}

/// A registry bound to a provider that is not defined fails before any
/// request reaches the registry (or Kafka), for every command that uses it.
#[tokio::test]
async fn a_wrong_registry_oauth_provider_fails_every_command_before_connecting() {
    let commands: &[&[&str]] = &[
        &["backup", "--schema-only"],
        &["backup"],
        &["restore", "--schema-only"],
        &["restore"],
        &["three-phase-restore"],
        &["validate-restore", "--schema-only"],
        &["validate-restore"],
    ];
    for command in commands {
        let registry = MockServer::start().await;
        let yaml = oauth_registry_yaml(&registry.uri(), "http://127.0.0.1:1", "nope");
        let fx = if command[0] == "backup" {
            backup_fixture(&yaml, "")
        } else {
            let fx = restore_fixture("", "", "").await;
            let mut config = fs::read_to_string(&fx.config).unwrap();
            config.push_str(&yaml);
            fs::write(&fx.config, config).unwrap();
            fx
        };
        let mut args: Vec<&str> = command.to_vec();
        args.extend(["--config", fx.config()]);
        let out = run(&args);
        let all = text(&out);
        assert!(!out.status.success(), "{command:?}: {all}");
        assert!(
            all.contains("schema_registry.auth refers to OAuth provider 'nope'"),
            "{command:?}: {all}"
        );
        assert!(
            registry.received_requests().await.unwrap().is_empty(),
            "{command:?}"
        );
        assert_eq!(fx.kafka.connections(), 0, "{command:?}: {all}");
    }
}
