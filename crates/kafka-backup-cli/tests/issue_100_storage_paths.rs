use std::fs;
use std::path::Path;
use std::process::Command;

fn create_manifest(root: &Path, backup_id: &str) {
    let backup_dir = root.join(backup_id);
    fs::create_dir_all(&backup_dir).unwrap();

    let manifest = serde_json::json!({
        "backup_id": backup_id,
        "created_at": 1_700_000_000_000_i64,
        "source_cluster_id": "test-cluster",
        "source_brokers": ["localhost:9092"],
        "compression": "zstd",
        "topics": []
    });

    fs::write(
        backup_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
}

fn file_url(path: &Path) -> String {
    format!("file://{}", path.display())
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

#[test]
fn list_all_accepts_file_url_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    create_manifest(temp_dir.path(), "issue-100");
    let path = file_url(temp_dir.path());

    let output = run_kafka_backup(&["list", "--path", &path]);
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Available backups:"), "{text}");
    assert!(text.contains("  - issue-100"), "{text}");
}

#[test]
fn list_specific_backup_accepts_file_url_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    create_manifest(temp_dir.path(), "issue-100");
    let path = file_url(temp_dir.path());

    let output = run_kafka_backup(&["list", "--path", &path, "--backup-id", "issue-100"]);
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Backup ID: issue-100"), "{text}");
}

#[test]
fn describe_accepts_file_url_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    create_manifest(temp_dir.path(), "issue-100");
    let path = file_url(temp_dir.path());

    let output = run_kafka_backup(&[
        "describe",
        "--path",
        &path,
        "--backup-id",
        "issue-100",
        "--format",
        "json",
    ]);
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("\"backup_id\": \"issue-100\""), "{text}");
}

#[test]
fn validate_accepts_file_url_path() {
    let temp_dir = tempfile::tempdir().unwrap();
    create_manifest(temp_dir.path(), "issue-100");
    let path = file_url(temp_dir.path());

    let output = run_kafka_backup(&["validate", "--path", &path, "--backup-id", "issue-100"]);
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Validating backup: issue-100"), "{text}");
    assert!(text.contains("Created:"), "{text}");
    assert!(!text.contains("Manifest not found"), "{text}");
    assert!(text.contains("Result: VALID"), "{text}");
}

#[test]
fn validate_missing_manifest_returns_invalid_and_nonzero() {
    let temp_dir = tempfile::tempdir().unwrap();
    let path = file_url(temp_dir.path());

    let output = run_kafka_backup(&["validate", "--path", &path, "--backup-id", "missing"]);
    let text = output_text(&output);

    assert!(!output.status.success(), "{text}");
    assert!(
        text.contains("Manifest not found: missing/manifest.json"),
        "{text}"
    );
    assert!(text.contains("Result: INVALID"), "{text}");
}
