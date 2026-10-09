//! Issue #219 — `validation evidence-list` / `evidence-get` resolve `--path`
//! like every other command (`storage_path::backend_from_path`): bare local
//! paths work, and the #216 fail-fast checks apply.

use std::path::Path;
use std::process::{Command, Output};

const REPORT_ID: &str = "val-2026-10-09-abc";

/// An evidence report laid out the way `validation run` uploads it.
fn write_report(root: &Path) -> String {
    let dir = root
        .join("evidence-reports")
        .join(REPORT_ID)
        .join("2026")
        .join("10");
    std::fs::create_dir_all(&dir).unwrap();
    let body = format!(r#"{{"report_id":"{REPORT_ID}"}}"#);
    std::fs::write(dir.join(format!("{REPORT_ID}.json")), &body).unwrap();
    std::fs::write(dir.join(format!("{REPORT_ID}.sig")), "sig").unwrap();
    body
}

fn kb(cwd: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
        .current_dir(cwd)
        .args(args)
        // Never reach for real cloud credentials / instance metadata.
        .env("AWS_EC2_METADATA_DISABLED", "true")
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

#[test]
fn evidence_list_accepts_bare_and_file_url_paths() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("evidence store");
    write_report(&root);
    let abs = root.to_str().unwrap().to_string();

    // Absolute, relative (resolved against the cwd), and file:// URL.
    for path in [abs.as_str(), "evidence store", "./evidence store"] {
        let out = kb(dir.path(), &["validation", "evidence-list", "--path", path]);
        assert!(out.status.success(), "{path}: {}", text(&out));
        assert!(
            text(&out).contains(&format!("{REPORT_ID}.json")),
            "{path}: report not listed:\n{}",
            text(&out)
        );
    }
    let plain = dir.path().join("plain");
    write_report(&plain);
    let url = format!("file://{}", plain.to_str().unwrap());
    let out = kb(dir.path(), &["validation", "evidence-list", "--path", &url]);
    assert!(text(&out).contains(REPORT_ID), "{url}: {}", text(&out));
}

#[test]
fn evidence_get_accepts_a_bare_path() {
    let dir = tempfile::tempdir().unwrap();
    let body = write_report(&dir.path().join("store"));
    let output = dir.path().join("report.json");

    let out = kb(
        dir.path(),
        &[
            "validation",
            "evidence-get",
            "--path",
            "store",
            "--report-id",
            REPORT_ID,
            "--output",
            output.to_str().unwrap(),
        ],
    );
    assert!(out.status.success(), "{}", text(&out));
    assert_eq!(std::fs::read_to_string(&output).unwrap(), body);
}

/// The #216 checks: these used to list an empty in-memory store ("No
/// evidence reports found.") or spend seconds on AWS instance metadata.
#[test]
fn evidence_commands_reject_unusable_paths_up_front() {
    let dir = tempfile::tempdir().unwrap();
    let cases = [
        ("memory://x", "memory:// is not supported"),
        ("s3:/bucket/evidence", "did you mean s3://bucket/evidence"),
        ("", "--path is empty"),
        ("s3://", "has no bucket name"),
    ];
    for (path, expected) in cases {
        for cmd in [
            vec!["validation", "evidence-list", "--path", path],
            vec![
                "validation",
                "evidence-get",
                "--path",
                path,
                "--report-id",
                REPORT_ID,
                "--output",
                "out.json",
            ],
        ] {
            let started = std::time::Instant::now();
            let out = kb(dir.path(), &cmd);
            assert!(!out.status.success(), "{cmd:?} should fail");
            assert!(text(&out).contains(expected), "{cmd:?}: {}", text(&out));
            assert!(
                started.elapsed() < std::time::Duration::from_secs(5),
                "{cmd:?} should fail before any network I/O"
            );
        }
    }
}

#[test]
fn evidence_help_lists_accepted_path_forms() {
    let dir = tempfile::tempdir().unwrap();
    for sub in ["evidence-list", "evidence-get"] {
        let out = kb(dir.path(), &["validation", sub, "--help"]);
        let help = text(&out);
        assert!(
            help.contains("s3://bucket/prefix") && help.contains("file:///"),
            "{sub} --help:\n{help}"
        );
    }
}
