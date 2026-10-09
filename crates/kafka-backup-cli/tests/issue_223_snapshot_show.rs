//! Issue #223 — `offset-rollback show` (text format) padded each offset row
//! with `usize` arithmetic, so a topic name longer than ~50 characters
//! panicked ("attempt to subtract with overflow" in debug builds, "capacity
//! overflow" in release builds).

use std::path::Path;
use std::process::{Command, Output};

/// Kafka's maximum topic name length.
const MAX_TOPIC_LEN: usize = 249;

fn write_snapshot(root: &Path, snapshot_id: &str, group: &str, topic: &str, offset: i64) {
    let dir = root.join("offset-snapshots").join(snapshot_id);
    std::fs::create_dir_all(&dir).unwrap();
    let snapshot = serde_json::json!({
        "snapshot_id": snapshot_id,
        "created_at": "2026-10-09T12:00:00Z",
        "bootstrap_servers": ["localhost:9092"],
        "description": "x".repeat(120),
        "group_offsets": {group: {
            "group_id": group,
            "partition_count": 2,
            "offsets": {topic: {"0": {"offset": offset}, "11": {"offset": 7}}},
        }},
    });
    std::fs::write(
        dir.join("snapshot.json"),
        serde_json::to_vec_pretty(&snapshot).unwrap(),
    )
    .unwrap();
}

fn show(root: &Path, snapshot_id: &str, format: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
        .args(["offset-rollback", "show", "--path"])
        .arg(root)
        .args(["--snapshot-id", snapshot_id, "--format", format])
        .output()
        .unwrap()
}

#[test]
fn show_prints_long_topic_names_without_panicking() {
    let dir = tempfile::tempdir().unwrap();
    // A Kafka Streams changelog topic, and the longest legal topic name with
    // the largest possible offset.
    let streams = "my-streams-app-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog";
    let longest = "t".repeat(MAX_TOPIC_LEN);
    write_snapshot(dir.path(), "streams", "app", streams, 1_234_567);
    write_snapshot(dir.path(), "longest", &"g".repeat(200), &longest, i64::MAX);

    for (id, topic, offset) in [
        ("streams", streams.to_string(), 1_234_567),
        ("longest", longest, i64::MAX),
    ] {
        let out = show(dir.path(), id, "text");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(out.status.success(), "{id}: show failed:\n{stdout}{stderr}");
        assert!(!stderr.contains("panicked"), "{id}: {stderr}");
        assert!(
            stdout.contains(&format!("{topic}:0 -> offset {offset}")),
            "{id}: offset row missing:\n{stdout}"
        );
        assert!(stdout.contains(&format!("{topic}:11 -> offset 7")), "{id}");
    }
}

#[test]
fn show_box_lines_up_for_short_names() {
    let dir = tempfile::tempdir().unwrap();
    write_snapshot(dir.path(), "short", "app", "orders", 42);

    let out = show(dir.path(), "short", "text");
    assert!(out.status.success());
    let stdout = String::from_utf8_lossy(&out.stdout);
    let boxed: Vec<&str> = stdout
        .lines()
        .filter(|l| l.starts_with(['╔', '║', '╠', '╟', '╚']))
        .collect();
    assert!(boxed.len() > 8, "no box in:\n{stdout}");
    let width = boxed[0].chars().count();
    for line in &boxed {
        // The description is deliberately too long for the box; it may
        // overflow, everything else must line up with the border.
        if line.contains("Description:") {
            continue;
        }
        assert_eq!(line.chars().count(), width, "misaligned row: {line:?}");
    }
    // Rows are sorted, so the output is stable between runs.
    let p0 = stdout.find("orders:0 ").unwrap();
    let p11 = stdout.find("orders:11 ").unwrap();
    assert!(p0 < p11, "partitions not in order:\n{stdout}");
}
