//! Issue #221 — `--path file://…` is percent-decoded and must name a local
//! path: `file:///tmp/my%20backups` is the directory `/tmp/my backups`, and a
//! host (`file://tmp/backups`) is an error instead of silently becoming
//! `/backups`.

use std::path::Path;
use std::process::{Command, Output};

fn write_backup(root: &Path, backup_id: &str) {
    let dir = root.join(backup_id);
    std::fs::create_dir_all(&dir).unwrap();
    let manifest = serde_json::json!({
        "backup_id": backup_id,
        "created_at": 1_700_000_000_000i64,
        "compression": "zstd",
        "topics": [],
    });
    std::fs::write(dir.join("manifest.json"), manifest.to_string()).unwrap();
}

fn kb(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
        .args(args)
        .output()
        .unwrap()
}

fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// Percent-encode the characters a file URL can't carry literally.
fn file_url(path: &Path) -> String {
    let p = path.to_str().unwrap();
    format!(
        "file://{}",
        p.replace('%', "%25")
            .replace(' ', "%20")
            .replace('#', "%23")
    )
}

#[test]
fn percent_encoded_file_url_reaches_the_real_directory() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("my backups #1");
    write_backup(&root, "bk1");
    let url = file_url(&root);

    let out = kb(&["list", "--path", &url]);
    assert!(out.status.success(), "{}", text(&out));
    assert!(
        text(&out).contains("bk1"),
        "{url}: backup not listed:\n{}",
        text(&out)
    );

    let out = kb(&[
        "describe",
        "--path",
        &url,
        "--backup-id",
        "bk1",
        "--format",
        "json",
    ]);
    assert!(out.status.success(), "{url}: {}", text(&out));

    // Nothing was created under the literal, still-encoded name.
    let literal = dir.path().join("my%20backups%20%231");
    assert!(!literal.exists());
}

#[test]
fn file_url_with_a_host_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    write_backup(dir.path(), "bk2");
    let host_url = format!("file://some-nfs-server{}", dir.path().to_str().unwrap());

    // Used to list the local directory as if the host weren't there.
    let out = kb(&["list", "--path", &host_url]);
    assert!(
        !out.status.success(),
        "{host_url} should fail:\n{}",
        text(&out)
    );
    assert!(
        text(&out).contains("host \"some-nfs-server\""),
        "{}",
        text(&out)
    );

    let out = kb(&["list", "--path", "file://tmp/backups"]);
    assert!(!out.status.success());
    assert!(text(&out).contains("file:///tmp/backups"), "{}", text(&out));
}

#[test]
fn file_url_without_a_directory_is_rejected() {
    for url in ["file://", "file:///", "file://localhost"] {
        let out = kb(&["list", "--path", url]);
        assert!(!out.status.success(), "{url} should fail:\n{}", text(&out));
        assert!(text(&out).contains("no directory"), "{url}: {}", text(&out));
    }
}
