//! Issue #218 — only a real "not found" reads as missing. A storage error
//! such as S3 403 AccessDenied must surface with its cause instead of being
//! reported as "Manifest: Not found", silently falling through to the next
//! offset-mapping source, or skipping prune's liveness check. And the S3
//! backend reports a missing key as not-found, like the other backends.

mod support;

use support::stub_s3::{StubS3, CREDS};
use support::*;

const BUCKET_PREFIX: &str = "kb-test/team-b";
const BACKUP_ID: &str = "bk-218";

/// object_store's message for S3 403 AccessDenied.
fn is_access_denied(text: &str) -> bool {
    text.contains("lacked the necessary privileges") || text.contains("AccessDenied")
}

fn run(s3: &StubS3, args: &[&str]) -> Run {
    let sb = Sandbox::new();
    let url = s3.url(BUCKET_PREFIX);
    let args: Vec<&str> = args
        .iter()
        .map(|a| if *a == "URL" { url.as_str() } else { a })
        .collect();
    kb_env(&sb.fresh_cwd(), &args, &CREDS)
}

fn seeded(objects: &[Objects]) -> StubS3 {
    let s3 = StubS3::start();
    for o in objects {
        s3.seed(BUCKET_PREFIX, o);
    }
    s3
}

mod status {
    use super::*;

    #[test]
    fn unreadable_manifest_is_an_error_not_missing() {
        let s3 = seeded(&[backup_fixture(BACKUP_ID)]);
        s3.deny_get("manifest.json");

        let out = run(&s3, &["status", "--path", "URL", "--backup-id", BACKUP_ID]);
        assert!(!out.success(), "{}", out.text());
        assert!(
            !out.stdout.contains("Manifest: Not found"),
            "{}",
            out.text()
        );
        assert!(out.stderr.contains("manifest.json"), "{}", out.text());
        assert!(is_access_denied(&out.text()), "{}", out.text());
    }

    #[test]
    fn missing_manifest_is_still_not_found() {
        let s3 = StubS3::start();
        let out = run(&s3, &["status", "--path", "URL", "--backup-id", BACKUP_ID]);
        assert!(out.stdout.contains("Manifest: Not found"), "{}", out.text());
    }
}

mod describe {
    use super::*;

    /// S3 used to report a missing key as "Backend error: S3 GET failed".
    #[test]
    fn missing_backup_on_s3_is_reported_as_not_found() {
        let s3 = StubS3::start();
        let out = run(&s3, &["describe", "--path", "URL", "--backup-id", "nope"]);
        assert!(!out.success());
        assert!(
            out.stderr.contains("Object not found: nope/manifest.json"),
            "{}",
            out.text()
        );
        assert!(!out.stderr.contains("Backend error"), "{}", out.text());
    }
}

mod offset_reset {
    use super::*;

    fn plan(s3: &StubS3) -> Run {
        run(
            s3,
            &[
                "offset-reset",
                "plan",
                "--path",
                "URL",
                "--backup-id",
                BACKUP_ID,
                "--groups",
                "g1",
                "--format",
                "json",
            ],
        )
    }

    fn bulk(s3: &StubS3) -> Run {
        run(
            s3,
            &[
                "offset-reset-bulk",
                "--path",
                "URL",
                "--backup-id",
                BACKUP_ID,
                "--groups",
                "g1",
                "--bootstrap-servers",
                "127.0.0.1:1",
            ],
        )
    }

    /// An unreadable restore report must not silently be replaced by the
    /// next source (offset-mapping.json, or the manifest's source offsets).
    #[test]
    fn unreadable_restore_report_is_an_error() {
        let s3 = seeded(&[
            backup_fixture(BACKUP_ID),
            offset_mapping_fixture(BACKUP_ID, "g1", 5),
            vec![(format!("{BACKUP_ID}/restore-report.json"), b"{}".to_vec())],
        ]);
        s3.deny_get("restore-report.json");

        for (name, out) in [("plan", plan(&s3)), ("bulk", bulk(&s3))] {
            assert!(!out.success(), "{name}: {}", out.text());
            assert!(
                out.stderr.contains("restore-report.json"),
                "{name}: {}",
                out.text()
            );
            assert!(is_access_denied(&out.text()), "{name}: {}", out.text());
        }
    }

    #[test]
    fn unreadable_offset_mapping_is_an_error() {
        let s3 = seeded(&[
            backup_fixture(BACKUP_ID),
            offset_mapping_fixture(BACKUP_ID, "g1", 5),
        ]);
        s3.deny_get("offset-mapping.json");

        for (name, out) in [("plan", plan(&s3)), ("bulk", bulk(&s3))] {
            assert!(!out.success(), "{name}: {}", out.text());
            assert!(
                out.stderr.contains("offset-mapping.json"),
                "{name}: {}",
                out.text()
            );
        }
    }

    /// A missing restore report still falls through to offset-mapping.json.
    #[test]
    fn missing_restore_report_falls_through() {
        let s3 = seeded(&[
            backup_fixture(BACKUP_ID),
            offset_mapping_fixture(BACKUP_ID, "g1", 5),
        ]);
        let out = plan(&s3);
        assert!(out.success(), "{}", out.text());
        assert!(out.stdout.contains("\"g1\""), "{}", out.text());
    }
}

mod prune {
    use super::*;

    /// prune reads offsets.db for resume positions and to tell whether a
    /// backup is live. An unreadable offsets.db must not be taken as "no
    /// offsets.db" (= not live, nothing to protect).
    #[test]
    fn unreadable_offsets_db_aborts_prune() {
        let s3 = seeded(&[
            backup_fixture(BACKUP_ID),
            vec![(format!("{BACKUP_ID}/offsets.db"), b"sqlite".to_vec())],
        ]);
        s3.deny_get("offsets.db");

        let out = run(
            &s3,
            &[
                "prune",
                "--path",
                "URL",
                "--backup-id",
                BACKUP_ID,
                "--older-than",
                "1d",
            ],
        );
        assert!(!out.success(), "{}", out.text());
        assert!(out.stderr.contains("offsets.db"), "{}", out.text());
    }
}
