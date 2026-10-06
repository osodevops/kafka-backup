//! Shared helpers for black-box CLI tests that exercise `--path` storage
//! resolution (issue #174).
#![allow(dead_code)]

pub mod stub_s3;

use std::fs;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

/// Nothing listens on port 1, so Kafka connects fail immediately.
pub const DEAD_KAFKA: &str = "127.0.0.1:1";

/// Storage objects as `(key, bytes)`, seedable into a directory or the stub.
pub type Objects = Vec<(String, Vec<u8>)>;

/// A temp root holding a `store/` directory plus one fresh, empty working
/// directory per CLI invocation - like `docker compose run --rm`, a local
/// write in one invocation can never be read back by the next.
pub struct Sandbox {
    _tmp: tempfile::TempDir,
    pub root: PathBuf,
    pub store: PathBuf,
    cwds: AtomicUsize,
}

impl Sandbox {
    pub fn new() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        let store = root.join("store");
        fs::create_dir_all(&store).unwrap();
        Self {
            _tmp: tmp,
            root,
            store,
            cwds: AtomicUsize::new(0),
        }
    }

    pub fn store_url(&self) -> String {
        format!("file://{}", self.store.display())
    }

    pub fn fresh_cwd(&self) -> PathBuf {
        let n = self.cwds.fetch_add(1, Ordering::SeqCst);
        let dir = self.root.join(format!("cwd-{n}"));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    pub fn seed(&self, objects: &Objects) {
        for (key, bytes) in objects {
            let path = self.store.join(key);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
    }

    pub fn store_has(&self, key: &str) -> bool {
        self.store.join(key).exists()
    }
}

/// Every file or directory under `dir`, relative to it.
pub fn entries_under(dir: &Path) -> Vec<String> {
    fn walk(base: &Path, dir: &Path, out: &mut Vec<String>) {
        for entry in fs::read_dir(dir).unwrap().flatten() {
            let path = entry.path();
            out.push(path.strip_prefix(base).unwrap().display().to_string());
            if path.is_dir() {
                walk(base, &path, out);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, dir, &mut out);
    out.sort();
    out
}

/// The #174 failure mode: a storage URL treated as a relative directory
/// shows up as `./s3:/bucket/...` (or `./file:/...`) under the cwd.
pub fn assert_cwd_untouched(cwd: &Path, context: &str) {
    let entries = entries_under(cwd);
    assert!(
        entries.is_empty(),
        "{context}: command wrote under its working directory: {entries:?}"
    );
}

pub struct Run {
    pub status: ExitStatus,
    pub stdout: String,
    pub stderr: String,
}

impl Run {
    pub fn success(&self) -> bool {
        self.status.success()
    }

    pub fn text(&self) -> String {
        format!(
            "status: {}\nstdout:\n{}\nstderr:\n{}",
            self.status, self.stdout, self.stderr
        )
    }
}

pub fn kb(cwd: &Path, args: &[&str]) -> Run {
    kb_env(cwd, args, &[])
}

/// Run the CLI in `cwd` with a scrubbed environment, so a developer's or CI
/// runner's AWS/Kafka/proxy settings can't leak in, and `RUST_LOG=error` so
/// stdout carries only command output. A watchdog bounds object_store's retry
/// back-off if a test endpoint misbehaves.
pub fn kb_env(cwd: &Path, args: &[&str], env: &[(&str, &str)]) -> Run {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kafka-backup"));
    cmd.args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (key, _) in std::env::vars_os() {
        let Some(key) = key.to_str() else { continue };
        let upper = key.to_ascii_uppercase();
        if ["AWS_", "AZURE_", "GOOGLE_", "KAFKA_"]
            .iter()
            .any(|p| upper.starts_with(p))
            || upper.ends_with("_PROXY")
            || upper == "RUST_LOG"
        {
            cmd.env_remove(key);
        }
    }
    cmd.env("RUST_LOG", "error")
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost");
    for (key, value) in env {
        cmd.env(key, value);
    }

    let mut child = cmd.spawn().unwrap();
    let mut out = child.stdout.take().unwrap();
    let mut err = child.stderr.take().unwrap();
    let out = thread::spawn(move || {
        let mut s = String::new();
        let _ = out.read_to_string(&mut s);
        s
    });
    let err = thread::spawn(move || {
        let mut s = String::new();
        let _ = err.read_to_string(&mut s);
        s
    });

    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!(
                "kafka-backup {args:?} did not finish within 60s\nstdout:\n{}\nstderr:\n{}",
                out.join().unwrap(),
                err.join().unwrap()
            );
        }
        thread::sleep(Duration::from_millis(20));
    };
    Run {
        status,
        stdout: out.join().unwrap(),
        stderr: err.join().unwrap(),
    }
}

/// A TCP listener that accepts and holds connections without speaking Kafka.
///
/// For PLAINTEXT, `KafkaClient::connect` only opens a socket (no ApiVersions
/// or Metadata request), and with an empty consumer-group set snapshot /
/// rollback / verify send no Kafka requests at all. So these commands run to
/// completion against the sink, which lets the storage write - the step #174
/// loses - be tested without a broker. This proves nothing about Kafka
/// compatibility; `connections()` proves the command really dialled it.
pub struct TcpSink {
    pub addr: String,
    connections: Arc<AtomicUsize>,
}

impl TcpSink {
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
}

pub fn tcp_sink() -> TcpSink {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let connections = Arc::new(AtomicUsize::new(0));
    let counter = connections.clone();
    thread::spawn(move || {
        let mut held = Vec::new();
        for stream in listener.incoming().flatten() {
            counter.fetch_add(1, Ordering::SeqCst);
            held.push(stream);
        }
    });
    TcpSink { addr, connections }
}

/// Snapshot ID from `offset-rollback snapshot` text output.
pub fn snapshot_id_from(run: &Run) -> String {
    run.stdout
        .lines()
        .find_map(|l| l.trim().strip_prefix("Snapshot ID:"))
        .map(|id| id.trim().to_string())
        .unwrap_or_else(|| panic!("no 'Snapshot ID:' line\n{}", run.text()))
}

fn json(value: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec_pretty(&value).unwrap()
}

// Fixtures are written at explicit keys rather than through kafka-backup-core
// so they pin the storage layout users already have in their buckets.

/// An offset snapshot with no consumer groups.
pub fn snapshot_fixture(id: &str, description: &str) -> Objects {
    let created_at = "2026-10-01T12:00:00Z";
    vec![
        (
            format!("offset-snapshots/{id}/snapshot.json"),
            json(serde_json::json!({
                "snapshot_id": id,
                "created_at": created_at,
                "group_offsets": {},
                "bootstrap_servers": ["kafka:9092"],
                "description": description,
            })),
        ),
        (
            format!("offset-snapshots/{id}/metadata.json"),
            json(serde_json::json!({
                "snapshot_id": id,
                "created_at": created_at,
                "group_count": 0,
                "offset_count": 0,
                "description": description,
            })),
        ),
    ]
}

/// A backup set: manifest plus one segment for `orders/0`, offsets 0-9.
pub fn backup_fixture(backup_id: &str) -> Objects {
    let segment = "segment-00000000000000000000.bin.zst";
    let segment_key = format!("{backup_id}/topics/orders/partition=0/{segment}");
    let payload = b"fake-segment".to_vec();
    let manifest = serde_json::json!({
        "backup_id": backup_id,
        "created_at": 1_790_000_000_000_i64,
        "compression": "zstd",
        "topics": [{
            "name": "orders",
            "original_partition_count": 1,
            "partitions": [{"partition_id": 0, "segments": [{
                "key": segment_key,
                "start_offset": 0,
                "end_offset": 9,
                "start_timestamp": 1_790_000_000_000_i64,
                "end_timestamp": 1_790_000_060_000_i64,
                "record_count": 10,
                "uncompressed_size": payload.len() * 4,
                "compressed_size": payload.len(),
                "uploaded_at": 1_790_000_060_000_i64,
            }]}]
        }]
    });
    vec![
        (format!("{backup_id}/manifest.json"), json(manifest)),
        (segment_key, payload),
    ]
}

/// `{backup_id}/offset-mapping.json` with one committed offset for
/// `group` on `orders/0`, mapped to `target_offset` on the restored cluster.
pub fn offset_mapping_fixture(backup_id: &str, group: &str, target_offset: i64) -> Objects {
    let mapping = serde_json::json!({
        "entries": {"orders/0": {
            "topic": "orders",
            "partition": 0,
            "source_first_offset": 0,
            "source_last_offset": 9,
            "target_first_offset": 0,
            "target_last_offset": 9,
            "first_timestamp": 1_790_000_000_000_i64,
            "last_timestamp": 1_790_000_060_000_i64,
        }},
        "consumer_groups": {group: {
            "group_id": group,
            "offsets": {"orders": {"0": {
                "source_offset": target_offset,
                "target_offset": target_offset,
                "timestamp": 1_790_000_060_000_i64,
            }}}
        }},
        "created_at": 1_790_000_060_000_i64,
    });
    vec![(format!("{backup_id}/offset-mapping.json"), json(mapping))]
}
