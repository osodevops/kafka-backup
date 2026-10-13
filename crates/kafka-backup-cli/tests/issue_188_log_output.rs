//! Issue #188 — the CLI wrote its logs to stdout, so `--format json` output
//! was preceded by `INFO ... Loading backup manifest` lines and did not
//! parse. Logs now go to stderr (text by default, or one JSON object per
//! line with `--log-format json`); stdout carries only the command output.

use std::path::Path;
use std::process::{Command, Output};
use std::sync::Arc;

use bytes::Bytes;
use kafka_backup_core::config::CompressionType;
use kafka_backup_core::manifest::BackupManifest;
use kafka_backup_core::metrics::PerformanceMetrics;
use kafka_backup_core::segment::{BinaryRecord, SegmentWriter, SegmentWriterConfig};
use kafka_backup_core::storage::{create_backend, StorageBackendConfig};

const BACKUP_ID: &str = "issue-188";
const TOPIC: &str = "orders";

/// A one-segment filesystem backup of `orders` partition 0.
async fn write_backup(root: &Path) {
    let storage = create_backend(&StorageBackendConfig::Filesystem {
        path: root.to_path_buf(),
    })
    .unwrap();
    let mut manifest = BackupManifest::new(BACKUP_ID.to_string());
    manifest.compression = "none".to_string();
    let config = SegmentWriterConfig {
        compression: CompressionType::None,
        max_segment_bytes: 1024 * 1024,
        ..Default::default()
    };
    let mut writer =
        SegmentWriter::new(config, storage.clone(), Arc::new(PerformanceMetrics::new()));
    for offset in 0..10i64 {
        writer
            .add_record(BinaryRecord {
                timestamp: 1_700_000_000_000 + offset,
                offset,
                key: Some(Bytes::from(format!("k{offset}"))),
                value: Some(Bytes::from(format!("v{offset}"))),
                headers: vec![],
            })
            .unwrap();
    }
    let key = format!(
        "{BACKUP_ID}/topics/{TOPIC}/partition=0/segment-{:020}.bin",
        0
    );
    let meta = writer.flush(&key).await.unwrap().expect("segment written");
    manifest
        .get_or_create_topic(TOPIC)
        .get_or_create_partition(0)
        .add_segment(meta);
    storage
        .put(
            &format!("{BACKUP_ID}/manifest.json"),
            Bytes::from(serde_json::to_vec_pretty(&manifest).unwrap()),
        )
        .await
        .unwrap();
}

fn restore_config(root: &Path) -> std::path::PathBuf {
    let path = root.join("restore.yaml");
    std::fs::write(
        &path,
        format!(
            "mode: restore\nbackup_id: {BACKUP_ID}\ntarget:\n  bootstrap_servers: [\"127.0.0.1:1\"]\n\
             storage:\n  backend: filesystem\n  path: {}\nmetrics:\n  enabled: false\n",
            root.display()
        ),
    )
    .unwrap();
    path
}

fn kafka_backup(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
        .args(args)
        .env_remove("RUST_LOG")
        .env_remove("KAFKA_BACKUP_LOG_FORMAT")
        .output()
        .unwrap()
}

fn assert_json_stdout(out: &Output, what: &str) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(out.status.success(), "{what} failed:\n{stdout}\n{stderr}");
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!("{what}: stdout is not JSON ({e}):\n{stdout}\n--- stderr ---\n{stderr}")
    })
}

#[tokio::test]
async fn json_output_parses_even_with_verbose_logging() {
    let dir = tempfile::tempdir().unwrap();
    write_backup(dir.path()).await;
    let root = dir.path().to_str().unwrap();
    let config = restore_config(dir.path());
    let config = config.to_str().unwrap();

    for verbosity in [None, Some("-v"), Some("-vv")] {
        let mut describe = vec![
            "describe",
            "--path",
            root,
            "--backup-id",
            BACKUP_ID,
            "--format",
            "json",
        ];
        let mut validate_restore = vec!["validate-restore", "--config", config, "--format", "json"];
        if let Some(v) = verbosity {
            describe.push(v);
            validate_restore.push(v);
        }

        let out = kafka_backup(&describe);
        let manifest = assert_json_stdout(&out, &format!("describe {verbosity:?}"));
        assert_eq!(manifest["backup_id"], BACKUP_ID);
        // The log lines are still written, to stderr.
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("Loading backup manifest"),
            "describe logs on stderr: {stderr}"
        );

        let out = kafka_backup(&validate_restore);
        let report = assert_json_stdout(&out, &format!("validate-restore {verbosity:?}"));
        assert_eq!(report["backup_id"], BACKUP_ID);
        assert_eq!(report["valid"], true, "{report}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("Validating restore configuration"),
            "validate-restore logs on stderr: {stderr}"
        );
    }
}

#[tokio::test]
async fn log_format_json_writes_one_json_object_per_stderr_line() {
    let dir = tempfile::tempdir().unwrap();
    write_backup(dir.path()).await;
    let root = dir.path().to_str().unwrap();

    let out = kafka_backup(&[
        "--log-format",
        "json",
        "describe",
        "--path",
        root,
        "--backup-id",
        BACKUP_ID,
        "--format",
        "json",
        "-v",
    ]);
    assert_json_stdout(&out, "describe --log-format json");

    let stderr = String::from_utf8_lossy(&out.stderr);
    let lines: Vec<&str> = stderr.lines().filter(|l| !l.trim().is_empty()).collect();
    assert!(!lines.is_empty(), "expected log lines on stderr");
    for line in &lines {
        let event: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("log line is not JSON ({e}): {line}"));
        assert!(event["level"].is_string(), "{line}");
        assert!(event["timestamp"].is_string(), "{line}");
    }
    assert!(
        lines
            .iter()
            .any(|l| l.contains("\"message\":\"Loading backup manifest")),
        "{stderr}"
    );

    // The environment variable selects the format too (Kubernetes).
    let out = Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
        .args(["describe", "--path", root, "--backup-id", BACKUP_ID])
        .env_remove("RUST_LOG")
        .env("KAFKA_BACKUP_LOG_FORMAT", "json")
        .output()
        .unwrap();
    assert!(out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    let first = stderr.lines().next().expect("a log line");
    serde_json::from_str::<serde_json::Value>(first)
        .unwrap_or_else(|e| panic!("KAFKA_BACKUP_LOG_FORMAT=json line is not JSON ({e}): {first}"));
}

#[tokio::test]
async fn text_logs_go_to_stderr_without_ansi_codes_on_a_pipe() {
    let dir = tempfile::tempdir().unwrap();
    write_backup(dir.path()).await;
    let root = dir.path().to_str().unwrap();

    let out = kafka_backup(&["list", "--path", root, "--backup-id", BACKUP_ID]);
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stdout.contains(&format!("Backup ID: {BACKUP_ID}")),
        "{stdout}"
    );
    assert!(
        !stdout.contains("Loading backup manifest"),
        "log on stdout: {stdout}"
    );
    assert!(stderr.contains("Loading backup manifest"), "{stderr}");
    assert!(stderr.contains("INFO"), "{stderr}");
    assert!(
        !stderr.contains('\u{1b}'),
        "ANSI escape in piped logs: {stderr:?}"
    );
}

#[test]
fn unknown_log_format_is_rejected() {
    let out = kafka_backup(&["--log-format", "xml", "list", "--path", "/tmp"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("--log-format"), "{stderr}");
}
