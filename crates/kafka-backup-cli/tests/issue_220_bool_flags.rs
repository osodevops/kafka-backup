//! Issue #220 — `offset-rollback rollback --verify` and `offset-reset plan
//! --dry-run` default to true but must still be switchable off. Both accept
//! an explicit value (`--verify false`, `--dry-run=false`), and the bare flag
//! keeps working as before.

use std::path::Path;
use std::process::Output;

use serde_json::Value;

#[path = "../../kafka-backup-core/tests/integration_suite/group_coordinator_mock.rs"]
mod group_coordinator_mock;

use group_coordinator_mock::{MockCluster, TOPIC};

const BACKUP_ID: &str = "issue-220";
const GROUP: &str = "payments";

async fn kafka_backup(args: &[&str]) -> Output {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
        .args(args)
        .output()
        .await
        .unwrap()
}

fn stdout_json(out: &Output, what: &str) -> Value {
    assert!(
        out.status.success(),
        "{what} failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    // `rollback` prints a short text preamble before the JSON document.
    let start = stdout.find('{').expect("JSON object on stdout");
    serde_json::from_str(&stdout[start..]).expect("valid JSON on stdout")
}

/// A manifest-only backup, enough for `offset-reset plan` to build a plan
/// offline.
fn write_manifest(root: &Path) {
    let backup_dir = root.join(BACKUP_ID);
    std::fs::create_dir_all(&backup_dir).unwrap();
    let manifest = serde_json::json!({
        "backup_id": BACKUP_ID,
        "created_at": 1_700_000_000_000i64,
        "compression": "zstd",
        "topics": [{"name": TOPIC, "partitions": [{"partition_id": 0, "segments": [{
            "key": format!("{BACKUP_ID}/topics/{TOPIC}/partition=0/segment-0.bin.zst"),
            "start_offset": 0,
            "end_offset": 9,
            "start_timestamp": 1_700_000_000_000i64,
            "end_timestamp": 1_700_000_001_000i64,
            "record_count": 10,
            "uncompressed_size": 40,
            "compressed_size": 10,
            "uploaded_at": 1_700_000_002_000i64,
        }]}]}],
    });
    std::fs::write(
        backup_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
}

async fn plan_dry_run(root: &Path, flag: &[&str]) -> bool {
    let path = root.to_str().unwrap();
    let mut args = vec![
        "offset-reset",
        "plan",
        "--path",
        path,
        "--backup-id",
        BACKUP_ID,
        "--groups",
        GROUP,
        "--bootstrap-servers",
        "localhost:9092",
        "--format",
        "json",
    ];
    args.extend_from_slice(flag);
    let plan = stdout_json(&kafka_backup(&args).await, &format!("plan {flag:?}"));
    plan["dry_run"].as_bool().expect("plan has a dry_run field")
}

#[tokio::test]
async fn offset_reset_plan_dry_run_can_be_turned_off() {
    let dir = tempfile::tempdir().unwrap();
    write_manifest(dir.path());

    // Default and bare flag: unchanged, still a dry run.
    assert!(plan_dry_run(dir.path(), &[]).await);
    assert!(plan_dry_run(dir.path(), &["--dry-run"]).await);
    assert!(plan_dry_run(dir.path(), &["--dry-run", "true"]).await);
    assert!(plan_dry_run(dir.path(), &["--dry-run=true"]).await);

    // Explicitly off, in both spellings.
    assert!(!plan_dry_run(dir.path(), &["--dry-run", "false"]).await);
    assert!(!plan_dry_run(dir.path(), &["--dry-run=false"]).await);
}

#[tokio::test]
async fn offset_reset_plan_dry_run_rejects_non_boolean_values() {
    let dir = tempfile::tempdir().unwrap();
    write_manifest(dir.path());
    let path = dir.path().to_str().unwrap();

    let out = kafka_backup(&[
        "offset-reset",
        "plan",
        "--path",
        path,
        "--backup-id",
        BACKUP_ID,
        "--dry-run",
        "maybe",
    ])
    .await;
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("invalid value 'maybe'"),
        "unexpected error: {stderr}"
    );
}

async fn create_snapshot(path: &str, bootstrap: &str) -> String {
    let out = kafka_backup(&[
        "offset-rollback",
        "snapshot",
        "--path",
        path,
        "--groups",
        GROUP,
        "--bootstrap-servers",
        bootstrap,
        "--format",
        "json",
    ])
    .await;
    let metadata = stdout_json(&out, "snapshot");
    metadata["snapshot_id"]
        .as_str()
        .expect("snapshot metadata has an id")
        .to_string()
}

/// Runs `rollback --format json` with `flag` and returns the `verification`
/// section of the output (`null` when verification was skipped).
async fn rollback_verification(path: &str, bootstrap: &str, id: &str, flag: &[&str]) -> Value {
    let mut args = vec![
        "offset-rollback",
        "rollback",
        "--path",
        path,
        "--snapshot-id",
        id,
        "--bootstrap-servers",
        bootstrap,
        "--format",
        "json",
    ];
    args.extend_from_slice(flag);
    let out = stdout_json(&kafka_backup(&args).await, &format!("rollback {flag:?}"));
    out["verification"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_rollback_verify_can_be_turned_off() {
    let cluster = MockCluster::start(1).await;
    cluster.add_group(GROUP, 1, &[(TOPIC, 0, 42)]);
    let bootstrap = cluster.addr(1);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_str().unwrap();

    let id = create_snapshot(path, &bootstrap).await;
    // Move the group forward so the rollback has something to undo.
    cluster.set_offset(GROUP, TOPIC, 0, 100);

    // Default and bare flag: unchanged, the rollback is verified.
    for flag in [
        &[][..],
        &["--verify"],
        &["--verify", "true"],
        &["--verify=true"],
    ] {
        let ver = rollback_verification(path, &bootstrap, &id, flag).await;
        assert!(ver.is_object(), "{flag:?} should verify, got {ver}");
        assert_eq!(cluster.committed(GROUP)[&(TOPIC.to_string(), 0)], 42);
        cluster.set_offset(GROUP, TOPIC, 0, 100);
    }

    // Explicitly off: the rollback still happens, verification is skipped.
    for flag in [&["--verify", "false"][..], &["--verify=false"]] {
        let ver = rollback_verification(path, &bootstrap, &id, flag).await;
        assert!(
            ver.is_null(),
            "{flag:?} should skip verification, got {ver}"
        );
        assert_eq!(cluster.committed(GROUP)[&(TOPIC.to_string(), 0)], 42);
        cluster.set_offset(GROUP, TOPIC, 0, 100);
    }
}
