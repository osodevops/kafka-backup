//! End-to-end CLI tests for `kafka-backup prune` (issue #169) against a
//! fabricated backup set on the filesystem backend.

use std::fs;
use std::path::Path;
use std::process::Command;

/// Three segments per the manifest, with real files on disk. Segment ages
/// (uploaded_at) are 90, 60 and 1 day(s) old; offsets 0-9, 10-19, 20-29.
fn create_backup_set(root: &Path, backup_id: &str) {
    let now = chrono::Utc::now().timestamp_millis();
    let day = 86_400_000i64;
    let seg_dir = root.join(backup_id).join("topics/orders/partition=0");
    fs::create_dir_all(&seg_dir).unwrap();

    let mut segments = Vec::new();
    for (idx, (start, end, age_days)) in [(0i64, 9i64, 90i64), (10, 19, 60), (20, 29, 1)]
        .iter()
        .enumerate()
    {
        let name = format!("segment-{:020}.bin.zst", start);
        let payload = format!("fake-segment-{idx}");
        fs::write(seg_dir.join(&name), &payload).unwrap();
        segments.push(serde_json::json!({
            "key": format!("{backup_id}/topics/orders/partition=0/{name}"),
            "start_offset": start,
            "end_offset": end,
            "start_timestamp": now - age_days * day,
            "end_timestamp": now - age_days * day,
            "record_count": end - start + 1,
            "uncompressed_size": payload.len() * 4,
            "compressed_size": payload.len(),
            "uploaded_at": now - age_days * day,
        }));
    }

    let manifest = serde_json::json!({
        "backup_id": backup_id,
        "created_at": now - 90 * day,
        "compression": "zstd",
        "topics": [{
            "name": "orders",
            "original_partition_count": 1,
            "partitions": [{"partition_id": 0, "segments": segments}]
        }]
    });
    fs::write(
        root.join(backup_id).join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
}

fn run_kafka_backup(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
        .args(args)
        .output()
        .unwrap()
}

fn output_text(output: &std::process::Output) -> String {
    format!(
        "status: {}\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn segment_files(root: &Path, backup_id: &str) -> Vec<String> {
    let mut names: Vec<String> =
        fs::read_dir(root.join(backup_id).join("topics/orders/partition=0"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
    names.sort();
    names
}

#[test]
fn prune_is_a_plan_only_dry_run_by_default() {
    let temp = tempfile::tempdir().unwrap();
    create_backup_set(temp.path(), "daily");
    let path = temp.path().to_str().unwrap().to_string();

    let output = run_kafka_backup(&[
        "prune",
        "--path",
        &path,
        "--backup-id",
        "daily",
        "--older-than",
        "30d",
    ]);
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Dry run — nothing deleted"), "{text}");
    assert!(text.contains("2 segment(s)"), "{text}");
    assert_eq!(segment_files(temp.path(), "daily").len(), 3, "{text}");
}

#[test]
fn prune_execute_deletes_segments_and_records_the_range() {
    let temp = tempfile::tempdir().unwrap();
    create_backup_set(temp.path(), "daily");
    let path = temp.path().to_str().unwrap().to_string();

    let output = run_kafka_backup(&[
        "prune",
        "--path",
        &path,
        "--backup-id",
        "daily",
        "--older-than",
        "30d",
        "--execute",
    ]);
    let text = output_text(&output);
    assert!(output.status.success(), "{text}");
    assert!(text.contains("Pruned 2 segment(s)"), "{text}");

    // The two old segment files are gone; the newest remains.
    let remaining = segment_files(temp.path(), "daily");
    assert_eq!(
        remaining,
        vec!["segment-00000000000000000020.bin.zst"],
        "{text}"
    );

    // Manifest: one segment left, pruned range 0..19 recorded.
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(temp.path().join("daily/manifest.json")).unwrap())
            .unwrap();
    let partition = &manifest["topics"][0]["partitions"][0];
    assert_eq!(partition["segments"].as_array().unwrap().len(), 1);
    let pruned = &partition["pruned"][0];
    assert_eq!(pruned["start_offset"], 0);
    assert_eq!(pruned["end_offset"], 19);
    assert_eq!(pruned["reason"], "manual");

    // A second run is a no-op.
    let output2 = run_kafka_backup(&[
        "prune",
        "--path",
        &path,
        "--backup-id",
        "daily",
        "--older-than",
        "30d",
        "--execute",
    ]);
    let text2 = output_text(&output2);
    assert!(output2.status.success(), "{text2}");
    assert!(text2.contains("nothing to prune"), "{text2}");

    // validate still passes (pruned is informational) and mentions the range.
    let validate = run_kafka_backup(&["validate", "--path", &path, "--backup-id", "daily"]);
    let vtext = output_text(&validate);
    assert!(validate.status.success(), "{vtext}");
    assert!(
        vtext.contains("Pruned Ranges (deliberately deleted"),
        "{vtext}"
    );

    // describe shows it too.
    let describe = run_kafka_backup(&["describe", "--path", &path, "--backup-id", "daily"]);
    let dtext = output_text(&describe);
    assert!(describe.status.success(), "{dtext}");
    assert!(dtext.contains("PRUNED offsets 0..19"), "{dtext}");
}

#[test]
fn prune_requires_a_criterion_and_exclusive_arguments() {
    let temp = tempfile::tempdir().unwrap();
    create_backup_set(temp.path(), "daily");
    let path = temp.path().to_str().unwrap().to_string();

    let output = run_kafka_backup(&["prune", "--path", &path, "--backup-id", "daily"]);
    let text = output_text(&output);
    assert!(!output.status.success(), "{text}");
    assert!(text.contains("nothing to prune"), "{text}");
}
