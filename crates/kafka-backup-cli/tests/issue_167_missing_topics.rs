//! Issue #167 — a manifest that records literal include topics skipped under
//! `backup.on_missing_topic: warn` is surfaced by `describe` and `validate`
//! without being treated as an integrity failure.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "kb-issue167-{}-{}-{}",
        name,
        std::process::id(),
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn create_backup_set(root: &Path, backup_id: &str, missing: &[&str]) {
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
        "missing_topics": missing,
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
fn describe_reports_missing_topics() {
    let dir = scratch_dir("describe");
    create_backup_set(&dir, "daily", &["payments", "ghost"]);
    let root = dir.to_str().unwrap();

    let out = run(&["describe", "--path", root, "--backup-id", "daily"]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(all.contains("Missing Topics: 2"), "{all}");
    assert!(all.contains("payments, ghost"), "{all}");

    let out = run(&[
        "describe",
        "--path",
        root,
        "--backup-id",
        "daily",
        "--format",
        "json",
    ]);
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).expect("json");
    assert_eq!(
        value["missing_topics"],
        serde_json::json!(["payments", "ghost"])
    );
}

#[test]
fn validate_lists_missing_topics_and_stays_valid() {
    let dir = scratch_dir("validate");
    create_backup_set(&dir, "daily", &["ghost"]);

    let out = run(&[
        "validate",
        "--path",
        dir.to_str().unwrap(),
        "--backup-id",
        "daily",
    ]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(all.contains("Missing Topics:     1"), "{all}");
    assert!(all.contains("not an integrity failure"), "{all}");
    assert!(all.contains("- ghost"), "{all}");
    assert!(all.contains("Result: VALID"), "{all}");
}

#[test]
fn manifests_without_the_field_are_unaffected() {
    let dir = scratch_dir("legacy");
    create_backup_set(&dir, "daily", &[]);

    let out = run(&[
        "describe",
        "--path",
        dir.to_str().unwrap(),
        "--backup-id",
        "daily",
    ]);
    let all = text(&out);
    assert!(out.status.success(), "{all}");
    assert!(!all.contains("Missing Topics"), "{all}");
}
