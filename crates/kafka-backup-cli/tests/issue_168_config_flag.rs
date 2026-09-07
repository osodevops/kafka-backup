//! Issue #168 — `describe` and `validate` accept `--config` (parity with
//! `status` / `prune`), resolving storage (including `prefix`) and the backup
//! id from the backup configuration file.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "kb-issue168-{}-{}-{}",
        name,
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// A minimal backup set (manifest + segment files) under `root/<backup_id>/`.
fn create_backup_set(root: &Path, backup_id: &str) {
    let now = chrono::Utc::now().timestamp_millis();
    let seg_dir = root.join(backup_id).join("topics/orders/partition=0");
    fs::create_dir_all(&seg_dir).unwrap();
    let name = "segment-00000000000000000000.bin.zst";
    let payload = "fake-segment";
    fs::write(seg_dir.join(name), payload).unwrap();
    let manifest = serde_json::json!({
        "backup_id": backup_id,
        "created_at": now - 60_000,
        "compression": "zstd",
        "topics": [{
            "name": "orders",
            "original_partition_count": 1,
            "partitions": [{"partition_id": 0, "segments": [{
                "key": format!("{backup_id}/topics/orders/partition=0/{name}"),
                "start_offset": 0,
                "end_offset": 9,
                "start_timestamp": now - 60_000,
                "end_timestamp": now - 1_000,
                "record_count": 10,
                "uncompressed_size": payload.len() * 4,
                "compressed_size": payload.len(),
                "uploaded_at": now,
            }]}]
        }]
    });
    fs::write(
        root.join(backup_id).join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
}

fn write_config(dir: &Path, storage_root: &Path, backup_id: &str) -> PathBuf {
    let path = dir.join("backup.yaml");
    fs::write(
        &path,
        format!(
            r#"
mode: backup
backup_id: {backup_id}
source:
  bootstrap_servers: ["127.0.0.1:1"]
storage:
  backend: filesystem
  path: {root}
"#,
            root = storage_root.display()
        ),
    )
    .unwrap();
    path
}

fn run(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
        .args(args)
        .env("RUST_LOG", "warn")
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

#[test]
fn describe_accepts_config() {
    let dir = scratch_dir("describe");
    let storage = dir.join("storage");
    create_backup_set(&storage, "daily");
    let config = write_config(&dir, &storage, "daily");

    let out = run(&[
        "describe",
        "--config",
        config.to_str().unwrap(),
        "--format",
        "json",
    ]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json manifest");
    assert_eq!(value["backup_id"], serde_json::json!("daily"), "{all}");

    // Same result as the explicit --path/--backup-id form.
    let out_path = run(&[
        "describe",
        "--path",
        storage.to_str().unwrap(),
        "--backup-id",
        "daily",
        "--format",
        "json",
    ]);
    assert!(out_path.status.success(), "{}", text(&out_path));
    assert_eq!(
        out.stdout, out_path.stdout,
        "--config and --path must agree"
    );
}

#[test]
fn validate_accepts_config() {
    let dir = scratch_dir("validate");
    let storage = dir.join("storage");
    create_backup_set(&storage, "daily");
    let config = write_config(&dir, &storage, "daily");

    let out = run(&["validate", "--config", config.to_str().unwrap()]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(all.contains("VALID"), "{all}");
}

#[test]
fn config_and_path_are_mutually_exclusive() {
    let dir = scratch_dir("conflict");
    let storage = dir.join("storage");
    create_backup_set(&storage, "daily");
    let config = write_config(&dir, &storage, "daily");

    for cmd in ["describe", "validate"] {
        let out = run(&[
            cmd,
            "--config",
            config.to_str().unwrap(),
            "--path",
            storage.to_str().unwrap(),
            "--backup-id",
            "daily",
        ]);
        assert_eq!(out.status.code(), Some(2), "{cmd}: {}", text(&out));

        // --path without --backup-id (and vice versa) is a usage error too.
        let out = run(&[cmd, "--path", storage.to_str().unwrap()]);
        assert_eq!(out.status.code(), Some(2), "{cmd}: {}", text(&out));
        let out = run(&[cmd, "--backup-id", "daily"]);
        assert_eq!(out.status.code(), Some(2), "{cmd}: {}", text(&out));
    }
}

#[test]
fn prune_still_resolves_from_config() {
    let dir = scratch_dir("prune");
    let storage = dir.join("storage");
    create_backup_set(&storage, "daily");
    let config = write_config(&dir, &storage, "daily");

    // Plan-only prune through the shared resolver (regression for #169).
    let out = run(&[
        "prune",
        "--config",
        config.to_str().unwrap(),
        "--older-than",
        "30d",
    ]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(all.contains("Prune plan for 'daily'"), "{all}");
}
