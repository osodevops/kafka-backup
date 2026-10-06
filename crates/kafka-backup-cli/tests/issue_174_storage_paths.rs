//! Issue #174 — `--path` storage URLs (`s3://`, `file://`, ...) are honoured by
//! every subcommand instead of being treated as a relative local directory.
//!
//! Before the fix, `offset-rollback`, `show-offset-mapping`, `offset-reset`,
//! `offset-reset-bulk` and static `status` built a `FilesystemBackend` straight
//! from `--path`, so `--path s3://bucket/prefix` wrote to `./s3:/bucket/prefix`
//! (lost with the container) and reads reported "No offset snapshots found" /
//! "Manifest: Not found" with exit 0. Each invocation below runs in a fresh,
//! empty working directory and asserts it stays empty, so a local write can
//! never be read back by the next command and hide the bug.

mod support;

use support::stub_s3::{StubS3, CREDS};
use support::*;

const SNAP: &str = "snap-1790000000000-0000174a";
const BACKUP: &str = "b1";

fn json_stdout(run: &Run) -> serde_json::Value {
    serde_json::from_str(&run.stdout)
        .unwrap_or_else(|e| panic!("stdout is not JSON ({e})\n{}", run.text()))
}

mod file_url {
    use super::*;

    #[test]
    fn rollback_list_reads_snapshots_via_file_url() {
        let sb = Sandbox::new();
        sb.seed(&snapshot_fixture(SNAP, "issue-174"));

        let cwd = sb.fresh_cwd();
        let run = kb(&cwd, &["offset-rollback", "list", "--path", &sb.store_url()]);
        assert!(run.success(), "{}", run.text());
        assert!(run.stdout.contains(SNAP), "{}", run.text());
        assert!(run.stdout.contains("Total: 1 snapshots"), "{}", run.text());
        assert_cwd_untouched(&cwd, "offset-rollback list");

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-rollback",
                "list",
                "--path",
                &sb.store_url(),
                "--format",
                "json",
            ],
        );
        assert!(run.success(), "{}", run.text());
        let listed = json_stdout(&run);
        assert_eq!(listed.as_array().map(Vec::len), Some(1), "{}", run.text());
        assert_eq!(listed[0]["snapshot_id"], SNAP);
        assert_cwd_untouched(&cwd, "offset-rollback list --format json");
    }

    #[test]
    fn rollback_show_reads_snapshot_via_file_url() {
        let sb = Sandbox::new();
        sb.seed(&snapshot_fixture(SNAP, "issue-174"));

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-rollback",
                "show",
                "--path",
                &sb.store_url(),
                "--snapshot-id",
                SNAP,
                "--format",
                "json",
            ],
        );
        assert!(run.success(), "{}", run.text());
        let shown = json_stdout(&run);
        assert_eq!(shown["snapshot_id"], SNAP);
        assert_eq!(shown["description"], "issue-174");
        assert_cwd_untouched(&cwd, "offset-rollback show");
    }

    #[test]
    fn rollback_rollback_loads_snapshot_via_file_url_before_kafka() {
        let sb = Sandbox::new();
        sb.seed(&snapshot_fixture(SNAP, "issue-174"));

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-rollback",
                "rollback",
                "--path",
                &sb.store_url(),
                "--snapshot-id",
                SNAP,
                "--bootstrap-servers",
                DEAD_KAFKA,
            ],
        );
        assert!(!run.success(), "{}", run.text());
        assert!(
            run.stdout
                .contains(&format!("Rolling back to snapshot: {SNAP}")),
            "{}",
            run.text()
        );
        assert!(
            run.stderr.contains("Failed to connect to Kafka"),
            "{}",
            run.text()
        );
        assert!(
            !run.stderr.contains("Failed to load snapshot"),
            "{}",
            run.text()
        );
        assert_cwd_untouched(&cwd, "offset-rollback rollback");
    }

    #[test]
    fn rollback_verify_loads_snapshot_via_file_url_before_kafka() {
        let sb = Sandbox::new();
        sb.seed(&snapshot_fixture(SNAP, "issue-174"));

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-rollback",
                "verify",
                "--path",
                &sb.store_url(),
                "--snapshot-id",
                SNAP,
                "--bootstrap-servers",
                DEAD_KAFKA,
            ],
        );
        assert!(!run.success(), "{}", run.text());
        assert!(
            run.stderr.contains("Failed to connect to Kafka"),
            "{}",
            run.text()
        );
        assert!(
            !run.stderr.contains("Failed to load snapshot"),
            "{}",
            run.text()
        );
        assert_cwd_untouched(&cwd, "offset-rollback verify");
    }

    #[test]
    fn rollback_delete_removes_snapshot_via_file_url() {
        let sb = Sandbox::new();
        sb.seed(&snapshot_fixture(SNAP, "issue-174"));

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-rollback",
                "delete",
                "--path",
                &sb.store_url(),
                "--snapshot-id",
                SNAP,
            ],
        );
        assert!(run.success(), "{}", run.text());
        assert!(run.stdout.contains("deleted successfully"), "{}", run.text());
        assert!(!sb.store_has(&format!("offset-snapshots/{SNAP}/snapshot.json")));
        assert!(!sb.store_has(&format!("offset-snapshots/{SNAP}/metadata.json")));
        assert_cwd_untouched(&cwd, "offset-rollback delete");
    }

    #[test]
    fn rollback_snapshot_with_empty_group_set_writes_to_file_url_not_cwd() {
        let sb = Sandbox::new();
        let sink = tcp_sink();

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-rollback",
                "snapshot",
                "--path",
                &sb.store_url(),
                "--bootstrap-servers",
                &sink.addr,
                "--description",
                "issue-174",
            ],
        );
        assert!(run.success(), "{}", run.text());
        assert!(sink.connections() >= 1, "Kafka sink was never dialled");
        let id = snapshot_id_from(&run);
        assert!(
            sb.store_has(&format!("offset-snapshots/{id}/snapshot.json")),
            "{:?}",
            entries_under(&sb.store)
        );
        assert!(sb.store_has(&format!("offset-snapshots/{id}/metadata.json")));
        assert_cwd_untouched(&cwd, "offset-rollback snapshot");
    }

    #[test]
    fn show_offset_mapping_reads_manifest_via_file_url() {
        let sb = Sandbox::new();
        sb.seed(&backup_fixture(BACKUP));

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "show-offset-mapping",
                "--path",
                &sb.store_url(),
                "--backup-id",
                BACKUP,
                "--format",
                "json",
            ],
        );
        assert!(run.success(), "{}", run.text());
        assert!(
            json_stdout(&run)["entries"]["orders/0"].is_object(),
            "{}",
            run.text()
        );
        assert_cwd_untouched(&cwd, "show-offset-mapping");
    }

    #[test]
    fn offset_reset_plan_reads_manifest_via_file_url() {
        let sb = Sandbox::new();
        sb.seed(&backup_fixture(BACKUP));

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-reset",
                "plan",
                "--path",
                &sb.store_url(),
                "--backup-id",
                BACKUP,
                "--groups",
                "g1",
                "--bootstrap-servers",
                DEAD_KAFKA,
                "--format",
                "json",
            ],
        );
        assert!(run.success(), "{}", run.text());
        assert_eq!(
            json_stdout(&run)["groups"][0]["group_id"],
            "g1",
            "{}",
            run.text()
        );
        assert_cwd_untouched(&cwd, "offset-reset plan");
    }

    #[test]
    fn offset_reset_script_reads_manifest_via_file_url() {
        let sb = Sandbox::new();
        sb.seed(&backup_fixture(BACKUP));
        let script = sb.root.join("reset.sh");

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-reset",
                "script",
                "--path",
                &sb.store_url(),
                "--backup-id",
                BACKUP,
                "--groups",
                "g1",
                "--bootstrap-servers",
                DEAD_KAFKA,
                "--output",
                script.to_str().unwrap(),
            ],
        );
        assert!(run.success(), "{}", run.text());
        let body = std::fs::read_to_string(&script).unwrap();
        assert!(body.starts_with("#!/bin/bash"), "{body}");
        assert_cwd_untouched(&cwd, "offset-reset script");
    }

    #[test]
    fn offset_reset_execute_loads_mapping_via_file_url_before_kafka() {
        let sb = Sandbox::new();
        sb.seed(&backup_fixture(BACKUP));

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-reset",
                "execute",
                "--path",
                &sb.store_url(),
                "--backup-id",
                BACKUP,
                "--groups",
                "g1",
                "--bootstrap-servers",
                DEAD_KAFKA,
            ],
        );
        assert!(!run.success(), "{}", run.text());
        assert!(run.stderr.contains("No available brokers"), "{}", run.text());
        assert!(!run.stderr.contains("Object not found"), "{}", run.text());
        assert_cwd_untouched(&cwd, "offset-reset execute");
    }

    #[test]
    fn offset_reset_bulk_reads_offset_mapping_via_file_url() {
        let sb = Sandbox::new();
        sb.seed(&offset_mapping_fixture(BACKUP, "g1", 5));

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-reset-bulk",
                "--path",
                &sb.store_url(),
                "--backup-id",
                BACKUP,
                "--groups",
                "g-not-in-mapping",
                "--bootstrap-servers",
                DEAD_KAFKA,
            ],
        );
        assert!(run.success(), "{}", run.text());
        assert!(run.stdout.contains("No offsets to reset"), "{}", run.text());
        assert_cwd_untouched(&cwd, "offset-reset-bulk");
    }

    #[test]
    fn offset_reset_bulk_with_targets_reaches_kafka() {
        let sb = Sandbox::new();
        sb.seed(&offset_mapping_fixture(BACKUP, "g1", 5));

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-reset-bulk",
                "--path",
                &sb.store_url(),
                "--backup-id",
                BACKUP,
                "--groups",
                "g1",
                "--bootstrap-servers",
                DEAD_KAFKA,
            ],
        );
        assert!(!run.success(), "{}", run.text());
        assert!(
            run.stderr.contains("Failed to connect to Kafka"),
            "{}",
            run.text()
        );
        assert!(
            !run.stderr.contains("Failed to load offset mapping"),
            "{}",
            run.text()
        );
        assert_cwd_untouched(&cwd, "offset-reset-bulk with targets");
    }

    #[test]
    fn status_reads_manifest_via_file_url() {
        let sb = Sandbox::new();
        sb.seed(&backup_fixture(BACKUP));
        let db = sb.root.join("offsets.db");

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "status",
                "--path",
                &sb.store_url(),
                "--backup-id",
                BACKUP,
                "--db-path",
                db.to_str().unwrap(),
            ],
        );
        assert!(run.success(), "{}", run.text());
        assert!(!run.stdout.contains("Manifest: Not found"), "{}", run.text());
        assert!(run.stdout.contains("Topics: 1"), "{}", run.text());
        assert!(run.stdout.contains("Segment Files: 1"), "{}", run.text());
        assert_cwd_untouched(&cwd, "status");
    }
}

mod fail_fast {
    use super::*;

    /// Every subcommand that takes `--path`, with the other arguments it needs.
    fn path_commands(path: &str, root: &std::path::Path) -> Vec<Vec<String>> {
        let db = root.join("offsets.db").display().to_string();
        let script = root.join("reset.sh").display().to_string();
        let report = root.join("report.json").display().to_string();
        let rows: Vec<Vec<&str>> = vec![
            vec!["offset-rollback", "snapshot", "--path", path, "--bootstrap-servers", DEAD_KAFKA],
            vec!["offset-rollback", "list", "--path", path],
            vec!["offset-rollback", "show", "--path", path, "--snapshot-id", SNAP],
            vec!["offset-rollback", "rollback", "--path", path, "--snapshot-id", SNAP, "--bootstrap-servers", DEAD_KAFKA],
            vec!["offset-rollback", "verify", "--path", path, "--snapshot-id", SNAP, "--bootstrap-servers", DEAD_KAFKA],
            vec!["offset-rollback", "delete", "--path", path, "--snapshot-id", SNAP],
            vec!["show-offset-mapping", "--path", path, "--backup-id", BACKUP],
            vec!["offset-reset", "plan", "--path", path, "--backup-id", BACKUP, "--groups", "g1", "--bootstrap-servers", DEAD_KAFKA],
            vec!["offset-reset", "execute", "--path", path, "--backup-id", BACKUP, "--groups", "g1", "--bootstrap-servers", DEAD_KAFKA],
            vec!["offset-reset", "script", "--path", path, "--backup-id", BACKUP, "--groups", "g1", "--bootstrap-servers", DEAD_KAFKA, "--output", &script],
            vec!["offset-reset-bulk", "--path", path, "--backup-id", BACKUP, "--groups", "g1", "--bootstrap-servers", DEAD_KAFKA],
            vec!["status", "--path", path, "--backup-id", BACKUP, "--db-path", &db],
            vec!["list", "--path", path],
            vec!["describe", "--path", path, "--backup-id", BACKUP],
            vec!["validate", "--path", path, "--backup-id", BACKUP],
            vec!["prune", "--path", path, "--backup-id", BACKUP, "--older-than", "30d"],
            vec!["validation", "evidence-list", "--path", path],
            vec!["validation", "evidence-get", "--path", path, "--report-id", "r1", "--output", &report],
        ];
        rows.into_iter()
            .map(|r| r.into_iter().map(String::from).collect())
            .collect()
    }

    #[test]
    fn every_path_subcommand_rejects_unknown_scheme() {
        let sb = Sandbox::new();
        let mut failures = Vec::new();
        for args in path_commands("bogus://x", &sb.root) {
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let cwd = sb.fresh_cwd();
            let run = kb(&cwd, &args);
            let local = entries_under(&cwd);
            if run.success()
                || !run.stderr.contains("Unknown storage scheme: bogus")
                || run.stderr.contains("Failed to connect to Kafka")
                || run.stderr.contains("No available brokers")
                || !local.is_empty()
            {
                failures.push(format!(
                    "`{}`: {}\nlocal writes: {local:?}",
                    args[..2].join(" "),
                    run.text()
                ));
            }
        }
        assert!(
            failures.is_empty(),
            "{} subcommand(s) did not fail fast on an unknown scheme:\n\n{}",
            failures.len(),
            failures.join("\n\n")
        );
    }

    #[test]
    fn rollback_snapshot_with_unknown_scheme_writes_nothing_locally() {
        let sb = Sandbox::new();
        let sink = tcp_sink();

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-rollback",
                "snapshot",
                "--path",
                "bogus://x",
                "--bootstrap-servers",
                &sink.addr,
            ],
        );
        assert!(!run.success(), "{}", run.text());
        assert!(
            run.stderr.contains("Unknown storage scheme: bogus"),
            "{}",
            run.text()
        );
        assert_eq!(
            sink.connections(),
            0,
            "storage must be resolved before connecting to Kafka"
        );
        assert_cwd_untouched(&cwd, "offset-rollback snapshot bogus://");
    }
}

mod back_compat {
    use super::*;

    #[test]
    fn bare_absolute_paths_still_work() {
        let sb = Sandbox::new();
        sb.seed(&snapshot_fixture(SNAP, "issue-174"));
        sb.seed(&backup_fixture(BACKUP));
        let path = sb.store.display().to_string();
        let db = sb.root.join("offsets.db").display().to_string();

        let commands: Vec<(Vec<&str>, &str)> = vec![
            (vec!["offset-rollback", "list", "--path", &path], SNAP),
            (
                vec!["offset-rollback", "show", "--path", &path, "--snapshot-id", SNAP],
                SNAP,
            ),
            (
                vec!["status", "--path", &path, "--backup-id", BACKUP, "--db-path", &db],
                "Topics: 1",
            ),
            (
                vec!["show-offset-mapping", "--path", &path, "--backup-id", BACKUP],
                "orders",
            ),
            (
                vec![
                    "offset-reset", "plan", "--path", &path, "--backup-id", BACKUP,
                    "--groups", "g1", "--bootstrap-servers", DEAD_KAFKA,
                ],
                "g1",
            ),
        ];
        for (args, marker) in commands {
            let cwd = sb.fresh_cwd();
            let run = kb(&cwd, &args);
            assert!(run.success(), "{args:?}\n{}", run.text());
            assert!(run.stdout.contains(marker), "{args:?}\n{}", run.text());
            assert_cwd_untouched(&cwd, &args[..2].join(" "));
        }
    }

    #[test]
    fn bare_relative_path_resolves_against_cwd() {
        let sb = Sandbox::new();
        let sink = tcp_sink();

        let cwd = sb.fresh_cwd();
        let run = kb(
            &cwd,
            &[
                "offset-rollback",
                "snapshot",
                "--path",
                "snapshots",
                "--bootstrap-servers",
                &sink.addr,
            ],
        );
        assert!(run.success(), "{}", run.text());
        let id = snapshot_id_from(&run);
        assert!(cwd
            .join(format!("snapshots/offset-snapshots/{id}/snapshot.json"))
            .exists());

        let run = kb(&cwd, &["offset-rollback", "list", "--path", "snapshots"]);
        assert!(run.stdout.contains(&id), "{}", run.text());
        let top_level: Vec<_> = std::fs::read_dir(&cwd)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(top_level, vec!["snapshots".to_string()]);
    }
}

mod help {
    use super::*;

    #[test]
    fn path_help_documents_storage_url_forms() {
        let sb = Sandbox::new();
        let subcommands: Vec<Vec<&str>> = vec![
            vec!["status"],
            vec!["show-offset-mapping"],
            vec!["offset-reset-bulk"],
            vec!["offset-rollback", "snapshot"],
            vec!["offset-rollback", "list"],
            vec!["offset-rollback", "show"],
            vec!["offset-rollback", "rollback"],
            vec!["offset-rollback", "verify"],
            vec!["offset-rollback", "delete"],
            vec!["offset-reset", "plan"],
            vec!["offset-reset", "execute"],
            vec!["offset-reset", "script"],
        ];
        let mut missing = Vec::new();
        for sub in subcommands {
            let mut args = sub.clone();
            args.push("--help");
            let run = kb(&sb.fresh_cwd(), &args);
            if !(run.stdout.contains("s3://bucket/prefix") && run.stdout.contains("file:///")) {
                missing.push(sub.join(" "));
            }
        }
        assert!(
            missing.is_empty(),
            "--path help does not document storage URL forms for: {missing:?}"
        );
    }
}

mod s3_stub {
    use super::*;

    const BUCKET_PREFIX: &str = "kb-test/team-a/snaps";

    #[test]
    fn rollback_snapshot_puts_objects_to_s3_bucket_prefix() {
        let sb = Sandbox::new();
        let s3 = StubS3::start();
        let sink = tcp_sink();

        let cwd = sb.fresh_cwd();
        let run = kb_env(
            &cwd,
            &[
                "offset-rollback",
                "snapshot",
                "--path",
                &s3.url(BUCKET_PREFIX),
                "--bootstrap-servers",
                &sink.addr,
                "--description",
                "issue-174",
            ],
            &CREDS,
        );
        assert!(run.success(), "{}", run.text());
        let id = snapshot_id_from(&run);
        let lines = s3.request_lines();
        for file in ["snapshot.json", "metadata.json"] {
            let want = format!("PUT /{BUCKET_PREFIX}/offset-snapshots/{id}/{file}");
            assert!(lines.contains(&want), "missing `{want}`; requests: {lines:#?}");
        }
        assert!(
            s3.requests().iter().all(|r| r.sigv4),
            "unsigned request: {:#?}",
            s3.requests()
        );
        assert_cwd_untouched(&cwd, "offset-rollback snapshot to s3://");
    }

    #[test]
    fn rollback_list_show_delete_target_s3_prefix() {
        let sb = Sandbox::new();
        let s3 = StubS3::start();
        s3.seed(BUCKET_PREFIX, &snapshot_fixture(SNAP, "issue-174"));
        let url = s3.url(BUCKET_PREFIX);

        let cwd = sb.fresh_cwd();
        let run = kb_env(&cwd, &["offset-rollback", "list", "--path", &url], &CREDS);
        assert!(run.success(), "{}", run.text());
        assert!(run.stdout.contains(SNAP), "{}", run.text());
        let list = s3
            .requests()
            .into_iter()
            .find(|r| r.method == "GET" && r.query.contains("list-type=2"))
            .unwrap_or_else(|| panic!("no ListObjectsV2: {:#?}", s3.requests()));
        assert_eq!(list.path, "/kb-test");
        assert!(
            list.query.contains("prefix=team-a/snaps/offset-snapshots"),
            "{list:?}"
        );
        assert_cwd_untouched(&cwd, "offset-rollback list from s3://");

        s3.clear_requests();
        let cwd = sb.fresh_cwd();
        let run = kb_env(
            &cwd,
            &[
                "offset-rollback",
                "show",
                "--path",
                &url,
                "--snapshot-id",
                SNAP,
                "--format",
                "json",
            ],
            &CREDS,
        );
        assert!(run.success(), "{}", run.text());
        assert_eq!(json_stdout(&run)["snapshot_id"], SNAP);
        let want = format!("GET /{BUCKET_PREFIX}/offset-snapshots/{SNAP}/snapshot.json");
        assert!(
            s3.request_lines().contains(&want),
            "{:#?}",
            s3.request_lines()
        );

        s3.clear_requests();
        let cwd = sb.fresh_cwd();
        let run = kb_env(
            &cwd,
            &[
                "offset-rollback",
                "delete",
                "--path",
                &url,
                "--snapshot-id",
                SNAP,
            ],
            &CREDS,
        );
        assert!(run.success(), "{}", run.text());
        let lines = s3.request_lines();
        for want in [
            format!("HEAD /{BUCKET_PREFIX}/offset-snapshots/{SNAP}/snapshot.json"),
            format!("DELETE /{BUCKET_PREFIX}/offset-snapshots/{SNAP}/snapshot.json"),
            format!("DELETE /{BUCKET_PREFIX}/offset-snapshots/{SNAP}/metadata.json"),
        ] {
            assert!(lines.contains(&want), "missing `{want}`; requests: {lines:#?}");
        }
        assert!(s3.keys().is_empty(), "{:?}", s3.keys());
        assert_cwd_untouched(&cwd, "offset-rollback delete from s3://");
    }

    #[test]
    fn s3_bucket_root_without_prefix_routes_to_bucket_root() {
        let sb = Sandbox::new();
        let s3 = StubS3::start();
        let sink = tcp_sink();
        let url = s3.url("kb-test");

        let cwd = sb.fresh_cwd();
        let run = kb_env(
            &cwd,
            &[
                "offset-rollback",
                "snapshot",
                "--path",
                &url,
                "--bootstrap-servers",
                &sink.addr,
            ],
            &CREDS,
        );
        assert!(run.success(), "{}", run.text());
        let id = snapshot_id_from(&run);
        let want = format!("PUT /kb-test/offset-snapshots/{id}/snapshot.json");
        assert!(
            s3.request_lines().contains(&want),
            "{:#?}",
            s3.request_lines()
        );

        let cwd = sb.fresh_cwd();
        let run = kb_env(&cwd, &["offset-rollback", "list", "--path", &url], &CREDS);
        assert!(run.stdout.contains(&id), "{}", run.text());
    }

    #[test]
    fn offset_reset_family_and_status_read_from_s3() {
        let sb = Sandbox::new();
        let s3 = StubS3::start();
        s3.seed(BUCKET_PREFIX, &backup_fixture(BACKUP));
        s3.seed(BUCKET_PREFIX, &offset_mapping_fixture(BACKUP, "g1", 5));
        let url = s3.url(BUCKET_PREFIX);
        let key = |file: &str| format!("GET /{BUCKET_PREFIX}/{BACKUP}/{file}");

        let run = kb_env(
            &sb.fresh_cwd(),
            &["show-offset-mapping", "--path", &url, "--backup-id", BACKUP],
            &CREDS,
        );
        assert!(run.success(), "{}", run.text());
        assert!(s3.request_lines().contains(&key("manifest.json")));

        s3.clear_requests();
        let run = kb_env(
            &sb.fresh_cwd(),
            &[
                "offset-reset", "plan", "--path", &url, "--backup-id", BACKUP,
                "--groups", "g1", "--bootstrap-servers", DEAD_KAFKA,
            ],
            &CREDS,
        );
        assert!(run.success(), "{}", run.text());
        let gets: Vec<String> = s3
            .request_lines()
            .into_iter()
            .filter(|l| l.starts_with("GET "))
            .collect();
        assert_eq!(
            gets,
            vec![key("restore-report.json"), key("offset-mapping.json")],
            "mapping must be looked up on S3, restore report first"
        );

        let run = kb_env(
            &sb.fresh_cwd(),
            &[
                "offset-reset-bulk", "--path", &url, "--backup-id", BACKUP,
                "--groups", "g-not-in-mapping", "--bootstrap-servers", DEAD_KAFKA,
            ],
            &CREDS,
        );
        assert!(run.stdout.contains("No offsets to reset"), "{}", run.text());

        let db = sb.root.join("offsets.db").display().to_string();
        let run = kb_env(
            &sb.fresh_cwd(),
            &["status", "--path", &url, "--backup-id", BACKUP, "--db-path", &db],
            &CREDS,
        );
        assert!(run.stdout.contains("Topics: 1"), "{}", run.text());
        assert!(run.stdout.contains("Segment Files: 1"), "{}", run.text());
    }

    #[test]
    fn rollback_list_surfaces_s3_access_denied_instead_of_no_snapshots() {
        let sb = Sandbox::new();
        let s3 = StubS3::start();
        s3.deny_list();

        let run = kb_env(
            &sb.fresh_cwd(),
            &["offset-rollback", "list", "--path", &s3.url(BUCKET_PREFIX)],
            &CREDS,
        );
        assert!(!run.success(), "{}", run.text());
        assert!(
            run.stderr.contains("Failed to list snapshots"),
            "{}",
            run.text()
        );
        assert!(
            !run.stdout.contains("No offset snapshots found"),
            "{}",
            run.text()
        );
    }

    /// The demos pipe `show --format json` into files at the default log
    /// level; the S3 backend's creation log must not land in stdout.
    #[test]
    fn json_output_over_s3_is_parseable_at_default_log_level() {
        let sb = Sandbox::new();
        let s3 = StubS3::start();
        s3.seed(BUCKET_PREFIX, &snapshot_fixture(SNAP, "issue-174"));

        let mut env = CREDS.to_vec();
        env.push(("RUST_LOG", "info"));
        let run = kb_env(
            &sb.fresh_cwd(),
            &[
                "offset-rollback",
                "show",
                "--path",
                &s3.url(BUCKET_PREFIX),
                "--snapshot-id",
                SNAP,
                "--format",
                "json",
            ],
            &env,
        );
        assert!(run.success(), "{}", run.text());
        assert_eq!(json_stdout(&run)["snapshot_id"], SNAP);
    }
}

/// Real S3 round trip. Run against MinIO (or any S3-compatible store):
///
/// ```text
/// KB_E2E_S3_URL='s3://kafka-backups/ci?endpoint=http://127.0.0.1:9000&region=us-east-1' \
/// AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
///   cargo test -p kafka-backup-cli --test issue_174_storage_paths -- --ignored minio_
/// ```
mod minio {
    use super::*;
    use kafka_backup_core::storage::{create_backend, StorageBackendConfig};

    fn required(name: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| {
            panic!("{name} must be set to run this test (see the module docs)")
        })
    }

    /// Keys under `offset-snapshots/`, read independently of the CLI.
    fn remote_snapshot_keys(url: &str) -> Vec<String> {
        let backend = create_backend(&StorageBackendConfig::from_url(url).unwrap()).unwrap();
        tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(backend.list("offset-snapshots/"))
            .unwrap()
    }

    #[test]
    #[ignore = "needs MinIO: KB_E2E_S3_URL, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY"]
    fn minio_rollback_lifecycle() {
        let base = required("KB_E2E_S3_URL");
        let key_id = required("AWS_ACCESS_KEY_ID");
        let secret = required("AWS_SECRET_ACCESS_KEY");
        let creds = [
            ("AWS_ACCESS_KEY_ID", key_id.as_str()),
            ("AWS_SECRET_ACCESS_KEY", secret.as_str()),
        ];
        let (location, query) = base.split_once('?').unwrap_or((&base, ""));
        let run_id = format!(
            "issue-174-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let url = format!("{}/{run_id}?{query}", location.trim_end_matches('/'));

        let sb = Sandbox::new();
        let sink = tcp_sink();
        let step = |args: &[&str]| {
            let cwd = sb.fresh_cwd();
            let run = kb_env(&cwd, args, &creds);
            assert_cwd_untouched(&cwd, &args[..2].join(" "));
            run
        };

        let run = step(&[
            "offset-rollback", "snapshot", "--path", &url,
            "--bootstrap-servers", &sink.addr, "--description", "issue-174 e2e",
        ]);
        assert!(run.success(), "{}", run.text());
        let id = snapshot_id_from(&run);
        let keys = remote_snapshot_keys(&url);
        for file in ["snapshot.json", "metadata.json"] {
            let want = format!("offset-snapshots/{id}/{file}");
            assert!(keys.contains(&want), "missing {want} in {keys:?}");
        }

        let run = step(&["offset-rollback", "list", "--path", &url]);
        assert!(run.stdout.contains(&id), "{}", run.text());

        let run = step(&[
            "offset-rollback", "show", "--path", &url, "--snapshot-id", &id, "--format", "json",
        ]);
        assert!(run.success(), "{}", run.text());
        assert_eq!(json_stdout(&run)["description"], "issue-174 e2e");

        let run = step(&[
            "offset-rollback", "verify", "--path", &url, "--snapshot-id", &id,
            "--bootstrap-servers", &sink.addr,
        ]);
        assert!(run.success(), "{}", run.text());
        assert!(run.stdout.contains("VERIFIED"), "{}", run.text());

        let run = step(&[
            "offset-rollback", "rollback", "--path", &url, "--snapshot-id", &id,
            "--bootstrap-servers", &sink.addr,
        ]);
        assert!(run.success(), "{}", run.text());
        assert!(
            run.stdout.contains(&format!("Rolling back to snapshot: {id}")),
            "{}",
            run.text()
        );

        let run = step(&["offset-rollback", "delete", "--path", &url, "--snapshot-id", &id]);
        assert!(run.success(), "{}", run.text());
        let run = step(&["offset-rollback", "delete", "--path", &url, "--snapshot-id", &id]);
        assert!(!run.success(), "{}", run.text());
        assert!(run.stderr.contains("not found"), "{}", run.text());

        let run = step(&["offset-rollback", "list", "--path", &url]);
        assert!(
            run.stdout.contains("No offset snapshots found"),
            "{}",
            run.text()
        );
        assert!(remote_snapshot_keys(&url).is_empty());
    }
}
