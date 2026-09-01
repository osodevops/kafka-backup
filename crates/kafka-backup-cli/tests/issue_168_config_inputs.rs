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

fn create_config(root: &Path, backup_id: &str) -> std::path::PathBuf {
    let config_path = root.join("backup.yaml");
    let storage_path = root.to_string_lossy().replace('\\', "/");
    fs::write(
        &config_path,
        format!(
            "mode: backup\nbackup_id: {backup_id}\nstorage:\n  backend: filesystem\n  path: '{storage_path}'\n"
        ),
    )
    .unwrap();
    config_path
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
fn describe_uses_storage_and_backup_id_from_config() {
    let temp_dir = tempfile::tempdir().unwrap();
    create_manifest(temp_dir.path(), "from-config");
    let config = create_config(temp_dir.path(), "from-config");

    let output = run_kafka_backup(&[
        "describe",
        "--config",
        config.to_str().unwrap(),
        "--format",
        "json",
    ]);
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("\"backup_id\": \"from-config\""), "{text}");
}

#[test]
fn validate_allows_backup_id_override_with_config() {
    let temp_dir = tempfile::tempdir().unwrap();
    create_manifest(temp_dir.path(), "override-id");
    let config = create_config(temp_dir.path(), "default-id");

    let output = run_kafka_backup(&[
        "validate",
        "--config",
        config.to_str().unwrap(),
        "--backup-id",
        "override-id",
    ]);
    let text = output_text(&output);

    assert!(output.status.success(), "{text}");
    assert!(text.contains("Validating backup: override-id"), "{text}");
    assert!(text.contains("Result: VALID"), "{text}");
}
