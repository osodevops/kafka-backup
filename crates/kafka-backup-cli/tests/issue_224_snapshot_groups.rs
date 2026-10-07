//! Issue #224 — `snapshot-groups` must save every consumer group, not just
//! the ones coordinated by the first bootstrap broker.

use std::path::Path;

use serde_json::Value;

#[path = "../../kafka-backup-core/tests/integration_suite/group_coordinator_mock.rs"]
mod group_coordinator_mock;

use group_coordinator_mock::{MockCluster, GROUP_AUTHORIZATION_FAILED, TOPIC};

/// The 12 groups from the issue's reproduction, spread over 3 coordinators.
const GROUPS: [&str; 12] = [
    "alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa",
    "lambda", "mu",
];
const BACKUP_ID: &str = "issue-224";

fn write_backup(root: &Path, bootstrap: &str) -> std::path::PathBuf {
    let backup_dir = root.join(BACKUP_ID);
    std::fs::create_dir_all(&backup_dir).unwrap();
    let manifest = serde_json::json!({
        "backup_id": BACKUP_ID,
        "created_at": 1_700_000_000_000i64,
        "compression": "zstd",
        "topics": [{"name": TOPIC, "partitions": []}],
    });
    std::fs::write(
        backup_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();

    let config = root.join("backup.yaml");
    std::fs::write(
        &config,
        format!(
            "mode: backup\n\
             backup_id: {BACKUP_ID}\n\
             source:\n  bootstrap_servers: [\"{bootstrap}\"]\n  topics:\n    include: [\"{TOPIC}\"]\n\
             storage:\n  backend: filesystem\n  path: {}\n",
            root.display()
        ),
    )
    .unwrap();
    config
}

async fn snapshot_groups(config: &Path) -> std::process::Output {
    tokio::process::Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
        .args(["snapshot-groups", "--config"])
        .arg(config)
        .env("RUST_LOG", "info")
        .output()
        .await
        .unwrap()
}

fn snapshot_path(root: &Path) -> std::path::PathBuf {
    root.join(BACKUP_ID).join("consumer-groups-snapshot.json")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_groups_saves_groups_from_every_coordinator() {
    let cluster = MockCluster::start(3).await;
    for (i, group) in GROUPS.iter().enumerate() {
        cluster.add_group(group, (i % 3) as i32 + 1, &[(TOPIC, 0, i as i64 + 1)]);
    }
    cluster.add_group("other-topic-only", 2, &[("unrelated", 0, 5)]);
    cluster.add_group("no-offsets", 3, &[]);
    let dir = tempfile::tempdir().unwrap();
    let config = write_backup(dir.path(), &cluster.addr(1));

    let out = snapshot_groups(&config).await;
    assert!(
        out.status.success(),
        "snapshot-groups failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let snapshot: Value =
        serde_json::from_slice(&std::fs::read(snapshot_path(dir.path())).unwrap()).unwrap();
    let mut saved: Vec<(String, i64)> = snapshot["groups"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| {
            (
                g["group_id"].as_str().unwrap().to_string(),
                g["offsets"][TOPIC]["0"].as_i64().unwrap(),
            )
        })
        .collect();
    saved.sort();
    let mut expected: Vec<(String, i64)> = GROUPS
        .iter()
        .enumerate()
        .map(|(i, g)| (g.to_string(), i as i64 + 1))
        .collect();
    expected.sort();
    assert_eq!(saved, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_groups_fails_and_keeps_the_old_snapshot_when_a_group_cannot_be_read() {
    let cluster = MockCluster::start(3).await;
    for (i, group) in GROUPS.iter().enumerate() {
        cluster.add_group(group, (i % 3) as i32 + 1, &[(TOPIC, 0, 1)]);
    }
    cluster.fail_fetches("beta", &[GROUP_AUTHORIZATION_FAILED]);
    let dir = tempfile::tempdir().unwrap();
    let config = write_backup(dir.path(), &cluster.addr(1));
    std::fs::write(snapshot_path(dir.path()), "previous snapshot").unwrap();

    let out = snapshot_groups(&config).await;

    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!out.status.success(), "expected failure:\n{output}");
    assert!(
        output.contains("beta"),
        "failure should name the group:\n{output}"
    );
    assert_eq!(
        std::fs::read_to_string(snapshot_path(dir.path())).unwrap(),
        "previous snapshot"
    );
}
