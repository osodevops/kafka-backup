//! Issue #261 — OAuth against a real broker and a real identity provider.
//!
//! Needs the stack in `tests/sasl-oauth-test-infra/`:
//!
//! ```text
//! docker compose -f tests/sasl-oauth-test-infra/docker-compose-oauth-idp.yml up -d --wait
//! cargo test -p kafka-backup-cli --test oauth_real_broker -- --ignored --nocapture --test-threads=1
//! ```
//!
//! - broker OAUTHBEARER listener on `localhost:19093` (SASL_PLAINTEXT, tokens
//!   validated against the identity provider's JWKS, expected issuer and
//!   audience, `connections.max.reauth.ms=60000`), PLAINTEXT on 19092;
//! - mock identity provider on `localhost:18080`: issuer `kafka` (trusted),
//!   `other` (another issuer) and `wrongaud` (another audience), 120 s
//!   tokens, `sub=<client_id>`.
//!
//! `KB_OAUTH_SOAK_SECS` (default 330) sets the length of the re-auth soak;
//! the issue's acceptance run is `KB_OAUTH_SOAK_SECS=1800`.
//!
//! The tests drive the real binary, produce data with the broker's own tools
//! through `docker exec`, and check the backups' manifests and the restored
//! topics.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

const BIN: &str = env!("CARGO_BIN_EXE_kafka-backup");
const BROKER: &str = "kb-oauth-kafka-1";
const IDP_CONTAINER: &str = "kb-oauth-idp-1";
const INTERNAL_BOOTSTRAP: &str = "kafka:29092";
const OAUTH_BOOTSTRAP: &str = "localhost:19093";
const IDP: &str = "http://localhost:18080";
const SECRET: &str = "kafka-backup-secret";
const METRICS_PORT: u16 = 19190;
const RESTORE_METRICS_PORT: u16 = 19191;
/// Token lifetime of every issuer in `idp/mock-oauth2-config.json`.
const TOKEN_SECS: f64 = 120.0;

/// The tests share the broker, the IdP container and the metrics ports.
static SERIAL: Mutex<()> = Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL.lock().unwrap_or_else(|p| p.into_inner())
}

fn token_url(issuer: &str) -> String {
    format!("{IDP}/{issuer}/token")
}

// ── Stack helpers ───────────────────────────────────────────────────────

fn sh(program: &str, args: &[&str]) -> String {
    let out = Command::new(program).args(args).output().unwrap();
    assert!(
        out.status.success(),
        "{program} {args:?} failed:\n{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn docker_kafka(args: &[&str]) -> String {
    let mut all = vec!["exec", BROKER];
    all.extend_from_slice(args);
    sh("docker", &all)
}

fn require_stack() {
    let running = sh(
        "docker",
        &[
            "ps",
            "--filter",
            "label=com.docker.compose.project=kb-oauth",
            "--format",
            "{{.Names}}",
        ],
    );
    for name in [BROKER, IDP_CONTAINER] {
        assert!(
            running.lines().any(|l| l == name),
            "{name} is not running; start the stack with `docker compose -f \
             tests/sasl-oauth-test-infra/docker-compose-oauth-idp.yml up -d --wait`"
        );
    }
    wait_for_idp(Duration::from_secs(60));
    std::env::set_var("KB_OAUTH_SECRET", SECRET);
}

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    )
}

fn create_topic(topic: &str, partitions: u32) {
    docker_kafka(&[
        "kafka-topics",
        "--bootstrap-server",
        INTERNAL_BOOTSTRAP,
        "--create",
        "--topic",
        topic,
        "--partitions",
        &partitions.to_string(),
        "--replication-factor",
        "1",
    ]);
}

/// Produce `records` at `per_sec` (-1: as fast as possible) over PLAINTEXT.
fn spawn_producer(topic: &str, records: u64, per_sec: i64) -> Child {
    Command::new("docker")
        .args([
            "exec",
            BROKER,
            "kafka-producer-perf-test",
            "--topic",
            topic,
            "--num-records",
            &records.to_string(),
            "--throughput",
            &per_sec.to_string(),
            "--record-size",
            "64",
            "--producer-props",
            &format!("bootstrap.servers={INTERNAL_BOOTSTRAP}"),
            "acks=all",
            "partitioner.class=org.apache.kafka.clients.producer.RoundRobinPartitioner",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap()
}

fn produce(topic: &str, records: u64) {
    assert!(spawn_producer(topic, records, -1).wait().unwrap().success());
}

/// Partition → log-end offset.
fn end_offsets(topic: &str) -> BTreeMap<i32, i64> {
    docker_kafka(&[
        "kafka-get-offsets",
        "--bootstrap-server",
        INTERNAL_BOOTSTRAP,
        "--topic",
        topic,
    ])
    .lines()
    .filter_map(|line| {
        let mut parts = line.rsplitn(3, ':');
        let offset = parts.next()?.trim().parse().ok()?;
        let partition = parts.next()?.trim().parse().ok()?;
        Some((partition, offset))
    })
    .collect()
}

fn total(offsets: &BTreeMap<i32, i64>) -> i64 {
    offsets.values().sum()
}

/// SHA-256 over every record of `topic` as `partition, offset, value`, in
/// partition and offset order: two topics with the same checksum hold the
/// same values at the same positions.
fn topic_checksum(topic: &str, records: i64) -> (usize, String) {
    let out = docker_kafka(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        INTERNAL_BOOTSTRAP,
        "--topic",
        topic,
        "--from-beginning",
        "--max-messages",
        &records.to_string(),
        "--timeout-ms",
        "30000",
        "--property",
        "print.partition=true",
        "--property",
        "print.offset=true",
    ]);
    let mut lines: Vec<(i32, i64, String)> = out
        .lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, '\t');
            let partition = fields.next()?.strip_prefix("Partition:")?.parse().ok()?;
            let offset = fields.next()?.strip_prefix("Offset:")?.parse().ok()?;
            Some((partition, offset, fields.next()?.to_string()))
        })
        .collect();
    lines.sort();
    let mut hash = Sha256::new();
    for (partition, offset, value) in &lines {
        hash.update(format!("{partition}\t{offset}\t{value}\n"));
    }
    let digest = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
    (lines.len(), digest)
}

fn idp_up() -> bool {
    Command::new("curl")
        .args([
            "-sf",
            "-o",
            "/dev/null",
            "-u",
            "probe:x",
            "-X",
            "POST",
            &token_url("kafka"),
            "-d",
            "grant_type=client_credentials",
        ])
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn wait_for_idp(limit: Duration) {
    let start = Instant::now();
    while !idp_up() {
        assert!(start.elapsed() < limit, "the mock IdP did not come up");
        thread::sleep(Duration::from_millis(500));
    }
}

/// A real token from the mock IdP for `client_id` (`sub` = client_id).
fn mint(client_id: &str) -> serde_json::Value {
    let out = sh(
        "curl",
        &[
            "-sf",
            "-u",
            &format!("{client_id}:unused"),
            "-X",
            "POST",
            &token_url("kafka"),
            "-d",
            "grant_type=client_credentials",
            "-d",
            "scope=kafka",
        ],
    );
    serde_json::from_str(&out).unwrap()
}

fn docker_logs_since(container: &str, since: &str) -> String {
    let out = Command::new("docker")
        .args(["logs", "--since", since, container])
        .output()
        .unwrap();
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

// ── kafka-backup helpers ────────────────────────────────────────────────

struct Run {
    dir: tempfile::TempDir,
}

impl Run {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("storage")).unwrap();
        Self { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn storage(&self) -> String {
        self.path("storage").display().to_string()
    }

    fn write(&self, name: &str, content: &str) -> PathBuf {
        let path = self.path(name);
        std::fs::write(&path, content).unwrap();
        path
    }

    fn command(&self, args: &[&str], log: &str) -> Command {
        let mut cmd = Command::new(BIN);
        let log_file = std::fs::File::create(self.path(log)).unwrap();
        cmd.args(args)
            .current_dir(self.dir.path())
            .env("NO_COLOR", "1")
            .env(
                "RUST_LOG",
                "info,kafka_backup_core::oauth=debug,kafka_backup_core::kafka::client=debug,\
                 kafka_backup_core::kafka::sasl=debug",
            )
            .stdout(log_file.try_clone().unwrap())
            .stderr(log_file);
        cmd
    }

    /// Run to completion within `limit`; returns (exit code, log).
    fn run(&self, args: &[&str], log: &str, limit: Duration) -> (Option<i32>, String) {
        let mut child = self.command(args, log).spawn().unwrap();
        let status = wait_for(&mut child, limit).unwrap_or_else(|| {
            let _ = child.kill();
            panic!(
                "kafka-backup {args:?} did not finish within {limit:?}:\n{}",
                self.log(log)
            )
        });
        (status, self.log(log))
    }

    fn spawn(&self, args: &[&str], log: &str) -> Child {
        self.command(args, log).spawn().unwrap()
    }

    fn log(&self, name: &str) -> String {
        strip_ansi(&std::fs::read_to_string(self.path(name)).unwrap_or_default())
    }

    fn manifest(&self, backup_id: &str) -> serde_json::Value {
        serde_json::from_slice(
            &std::fs::read(self.path("storage").join(backup_id).join("manifest.json")).unwrap(),
        )
        .unwrap()
    }

    /// Partition → records in the backup's manifest.
    fn manifest_records(&self, backup_id: &str) -> BTreeMap<i32, i64> {
        let manifest = self.manifest(backup_id);
        let mut out = BTreeMap::new();
        for topic in manifest["topics"].as_array().unwrap() {
            for partition in topic["partitions"].as_array().unwrap() {
                let records: i64 = partition["segments"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|s| s["record_count"].as_i64().unwrap())
                    .sum();
                *out.entry(partition["partition_id"].as_i64().unwrap() as i32)
                    .or_default() += records;
            }
        }
        out
    }

    /// Check that every partition's segments cover offsets `0..end` exactly
    /// once: no gap and no offset archived twice.
    fn assert_no_gaps_or_duplicates(&self, backup_id: &str, ends: &BTreeMap<i32, i64>) {
        let manifest = self.manifest(backup_id);
        for topic in manifest["topics"].as_array().unwrap() {
            for partition in topic["partitions"].as_array().unwrap() {
                let id = partition["partition_id"].as_i64().unwrap() as i32;
                let mut segments: Vec<(i64, i64, i64)> = partition["segments"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|s| {
                        (
                            s["start_offset"].as_i64().unwrap(),
                            s["end_offset"].as_i64().unwrap(),
                            s["record_count"].as_i64().unwrap(),
                        )
                    })
                    .collect();
                segments.sort();
                let mut next = 0;
                for (start, end, records) in &segments {
                    assert_eq!(
                        *start, next,
                        "partition {id}: expected a segment starting at offset {next}, found \
                         {start} (a gap if larger, a duplicate if smaller): {segments:?}"
                    );
                    assert_eq!(
                        *records,
                        end - start + 1,
                        "partition {id}: segment {start}-{end} holds {records} records"
                    );
                    next = end + 1;
                }
                assert_eq!(
                    next, ends[&id],
                    "partition {id}: archived up to offset {next}, log end is {}",
                    ends[&id]
                );
            }
        }
    }
}

/// Remove ANSI colour sequences (`ESC [ ... m`).
fn strip_ansi(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\x1b' && chars.peek() == Some(&'[') {
            for d in chars.by_ref() {
                if d == 'm' {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn wait_for(child: &mut Child, limit: Duration) -> Option<Option<i32>> {
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status.code());
        }
        if start.elapsed() > limit {
            return None;
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn sigterm(child: &Child) {
    sh("kill", &["-TERM", &child.id().to_string()]);
}

/// Stop a continuous backup gracefully; returns its exit code.
fn stop(mut child: Child, limit: Duration) -> Option<i32> {
    sigterm(&child);
    wait_for(&mut child, limit).unwrap_or_else(|| {
        let _ = child.kill();
        panic!("kafka-backup did not stop within {limit:?} after SIGTERM")
    })
}

fn scrape(port: u16) -> String {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return String::new();
    };
    let _ = write!(stream, "GET /metrics HTTP/1.0\r\nHost: localhost\r\n\r\n");
    let mut body = String::new();
    let _ = stream.read_to_string(&mut body);
    body
}

fn metric_value(metrics: &str, prefix: &str) -> Option<f64> {
    metrics
        .lines()
        .find(|l| l.starts_with(prefix))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

/// Seconds since the first log line, for every line containing `needle`.
fn event_times(log: &str, needle: &str) -> Vec<f64> {
    let stamp = |line: &str| {
        line.split_whitespace()
            .next()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
    };
    let Some(first) = log.lines().find_map(stamp) else {
        return Vec::new();
    };
    log.lines()
        .filter(|l| l.contains(needle))
        .filter_map(|l| stamp(l).map(|t| (t - first).num_milliseconds() as f64 / 1000.0))
        .collect()
}

const HANDSHAKE: &str = "SASL OAUTHBEARER plugin authentication successful";
const REAUTH_OK: &str = "SASL reauth succeeded";
const REAUTH_FAILED: &str = "SASL reauth failed";

/// An `oidc_client_credentials` provider named `idp` on `issuer`.
fn oidc_provider(issuer: &str, client_auth: &str) -> String {
    format!(
        r#"    idp:
      type: oidc_client_credentials
      token_endpoint_url: {url}
      client_id: kafka-backup
      client_secret: ${{KB_OAUTH_SECRET}}
      client_auth: {client_auth}
      scope: kafka
"#,
        url = token_url(issuer)
    )
}

fn backup_yaml(
    run: &Run,
    backup_id: &str,
    topic: &str,
    continuous: bool,
    providers: &str,
    provider: &str,
) -> String {
    format!(
        r#"mode: backup
backup_id: {backup_id}
source:
  bootstrap_servers: ["{OAUTH_BOOTSTRAP}"]
  security:
    security_protocol: SASL_PLAINTEXT
    sasl_mechanism: OAUTHBEARER
    oauth: {{ provider: {provider} }}
  topics: {{ include: ["{topic}"] }}
storage: {{ backend: filesystem, path: "{storage}" }}
backup:
  start_offset: earliest
  continuous: {continuous}
  stop_at_current_offsets: {stop}
  segment_max_interval_ms: 5000
metrics: {{ enabled: true, bind_address: 127.0.0.1, port: {METRICS_PORT}, keep_alive_seconds: 0 }}
oauth:
  providers:
{providers}"#,
        storage = run.storage(),
        stop = !continuous,
    )
}

fn restore_yaml(
    run: &Run,
    backup_id: &str,
    topic: &str,
    restored: &str,
    providers: &str,
) -> String {
    format!(
        r#"mode: restore
backup_id: {backup_id}
target:
  bootstrap_servers: ["{OAUTH_BOOTSTRAP}"]
  security:
    security_protocol: SASL_PLAINTEXT
    sasl_mechanism: OAUTHBEARER
    oauth: {{ provider: idp }}
  topics: {{ include: ["{topic}"] }}
storage: {{ backend: filesystem, path: "{storage}" }}
restore:
  create_topics: true
  topic_mapping: {{ "{topic}": "{restored}" }}
  produce_acks: -1
metrics: {{ enabled: true, bind_address: 127.0.0.1, port: {RESTORE_METRICS_PORT}, keep_alive_seconds: 0 }}
oauth:
  providers:
{providers}"#,
        storage = run.storage(),
    )
}

// ── Tests ───────────────────────────────────────────────────────────────

/// Back up and restore over the OAuth listener with client credentials,
/// `client_secret_basic` and `client_secret_post`: record counts and
/// checksums match.
#[test]
#[ignore = "requires the tests/sasl-oauth-test-infra identity-provider stack"]
fn backup_and_restore_over_oauth_with_basic_and_post() {
    let _serial = serial();
    require_stack();
    for auth in ["basic", "post"] {
        let run = Run::new();
        let topic = unique(&format!("roundtrip-{auth}"));
        create_topic(&topic, 3);
        produce(&topic, 900);
        let source = end_offsets(&topic);
        assert_eq!(total(&source), 900);

        let backup_id = unique("roundtrip");
        let providers = oidc_provider("kafka", auth);
        let config = run.write(
            "backup.yaml",
            &backup_yaml(&run, &backup_id, &topic, false, &providers, "idp"),
        );
        let (code, log) = run.run(
            &["backup", "--config", config.to_str().unwrap()],
            "backup.log",
            Duration::from_secs(120),
        );
        assert_eq!(code, Some(0), "backup ({auth}):\n{log}");
        assert!(log.contains("OAuth enabled"), "{log}");
        let handshakes = event_times(&log, HANDSHAKE).len();
        let fetches = event_times(&log, "OAuth token fetched").len();
        assert!(handshakes >= 2, "{log}");
        assert_eq!(fetches, 1, "every connection shares one token:\n{log}");
        assert_eq!(run.manifest_records(&backup_id), source);
        run.assert_no_gaps_or_duplicates(&backup_id, &source);
        assert!(!log.contains(SECRET), "secret in the backup log");

        let restored = format!("{topic}-restored");
        let config = run.write(
            "restore.yaml",
            &restore_yaml(&run, &backup_id, &topic, &restored, &providers),
        );
        let (code, log) = run.run(
            &["restore", "--config", config.to_str().unwrap()],
            "restore.log",
            Duration::from_secs(180),
        );
        assert_eq!(code, Some(0), "restore ({auth}):\n{log}");
        assert!(log.contains(HANDSHAKE), "{log}");
        assert!(!log.contains(SECRET), "secret in the restore log");
        assert_eq!(
            end_offsets(&restored),
            source,
            "restored topic matches the source"
        );
        let (source_count, source_sum) = topic_checksum(&topic, 900);
        let (restored_count, restored_sum) = topic_checksum(&restored, 900);
        assert_eq!(source_count, 900, "read every source record");
        assert_eq!(restored_count, 900, "read every restored record");
        assert_eq!(
            restored_sum, source_sum,
            "restored records differ from the source"
        );
        println!(
            "{auth}: 900 records backed up and restored over OAUTHBEARER ({handshakes} backup \
             handshakes, {fetches} token fetch); sha256 {source_sum}"
        );
    }
}

/// A validly signed token the cluster does not accept (another issuer,
/// another audience) fails the backup with an error that says what to check,
/// and nothing is archived.
#[test]
#[ignore = "requires the tests/sasl-oauth-test-infra identity-provider stack"]
fn wrong_issuer_and_wrong_audience_are_rejected_with_an_actionable_error() {
    let _serial = serial();
    require_stack();
    for (issuer, what) in [("other", "issuer"), ("wrongaud", "audience")] {
        let run = Run::new();
        let topic = unique(&format!("rejected-{issuer}"));
        create_topic(&topic, 1);
        produce(&topic, 10);
        let backup_id = unique("rejected");
        let config = run.write(
            "backup.yaml",
            &backup_yaml(
                &run,
                &backup_id,
                &topic,
                false,
                &oidc_provider(issuer, "basic"),
                "idp",
            ),
        );
        let (code, log) = run.run(
            &["backup", "--config", config.to_str().unwrap()],
            "backup.log",
            Duration::from_secs(180),
        );
        assert_ne!(code, Some(0), "a wrong {what} must fail the backup:\n{log}");
        assert!(
            log.contains("OAuth token fetched"),
            "the identity provider issued a token:\n{log}"
        );
        assert!(
            log.contains("SASL OAUTHBEARER rejected by broker"),
            "the broker's refusal is reported:\n{log}"
        );
        assert!(log.contains("OAuth provider 'idp'"), "{log}");
        for cause in ["issuer", "audience", "identity pool"] {
            assert!(log.contains(cause), "the error names {cause}:\n{log}");
        }
        assert!(!log.contains("panicked"), "{log}");
        assert!(!log.contains(SECRET), "secret in the log");
        assert!(
            !run.path("storage")
                .join(&backup_id)
                .join("manifest.json")
                .exists(),
            "nothing is archived when authentication fails"
        );
        let last = log
            .lines()
            .rev()
            .find(|l| l.contains("rejected by broker"))
            .unwrap_or("");
        println!("wrong {what}: exit {code:?}; {last}");
    }
}

/// A continuous backup against a broker that forces re-authentication every
/// 60 s with 120 s tokens: the connections re-authenticate in place
/// (KIP-368), no new handshakes after start-up, token requests track the
/// refresh schedule, and the archive has no gaps or duplicate offsets.
#[test]
#[ignore = "requires the tests/sasl-oauth-test-infra identity-provider stack"]
fn soak_reauthenticates_without_reconnecting() {
    let _serial = serial();
    require_stack();
    let soak: u64 = std::env::var("KB_OAUTH_SOAK_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(330);
    let run = Run::new();
    let topic = unique("soak");
    create_topic(&topic, 3);
    let backup_id = unique("soak");
    // Refresh 20 s before expiry instead of the default 5 m, which is longer
    // than these tokens live.
    let providers = format!(
        "{}  cache:\n    refresh_before_expiry: 20s\n",
        oidc_provider("kafka", "basic")
    );
    let config = run.write(
        "backup.yaml",
        &backup_yaml(&run, &backup_id, &topic, true, &providers, "idp"),
    );
    let broker_since = chrono::Utc::now().to_rfc3339();

    let backup = run.spawn(
        &["backup", "--config", config.to_str().unwrap()],
        "soak.log",
    );
    let records = soak * 5;
    let mut producer = spawn_producer(&topic, records, 5);
    let started = Instant::now();
    assert!(producer.wait().unwrap().success());
    thread::sleep(Duration::from_secs(20)); // let the backup catch up
    let elapsed = started.elapsed().as_secs_f64();
    let metrics = scrape(METRICS_PORT);
    let code = stop(backup, Duration::from_secs(60));
    let log = run.log("soak.log");
    assert_eq!(code, Some(0), "{log}");

    let handshakes = event_times(&log, HANDSHAKE);
    let reauths = event_times(&log, REAUTH_OK);
    let late_handshakes = handshakes.iter().filter(|t| **t > 60.0).count();
    let token_requests = metric_value(
        &metrics,
        "kafka_backup_oauth_token_requests_total{provider=\"idp\",kind=\"oidc_client_credentials\",outcome=\"success\"}",
    )
    .unwrap_or_else(|| panic!("token request counter missing:\n{metrics}"));
    let token_failures = metric_value(
        &metrics,
        "kafka_backup_oauth_token_requests_total{provider=\"idp\",kind=\"oidc_client_credentials\",outcome=\"failure\"}",
    )
    .unwrap_or_else(|| panic!("token failure counter missing:\n{metrics}"));
    let broker_log = docker_logs_since(BROKER, &broker_since);
    let broker_reauths = broker_log.matches("re-authenticat").count();
    println!(
        "soak {elapsed:.0}s: {} full handshakes ({late_handshakes} after the first minute), {} \
         re-authentications, {token_requests} token requests ({token_failures} failed), broker \
         lines mentioning re-authentication: {broker_reauths}",
        handshakes.len(),
        reauths.len()
    );

    assert!(!log.contains(REAUTH_FAILED), "{log}");
    assert!(!log.contains("Cannot change principals"), "{log}");
    assert_eq!(
        late_handshakes, 0,
        "connections were re-established instead of re-authenticated:\n{log}"
    );
    // Each connection re-authenticates at 80 % of the 60 s session.
    let per_connection = elapsed / 60.0;
    assert!(
        reauths.len() as f64 >= per_connection.floor() - 1.0,
        "expected at least ~{per_connection:.0} re-authentications, got {}",
        reauths.len()
    );
    assert_eq!(
        token_failures, 0.0,
        "the identity provider was up throughout"
    );
    // Handshakes ask for 60 s of remaining lifetime and the cache refreshes
    // at 80 % of a token's 120 s, so a token serves 60-96 s: at least one
    // request per token lifetime, never more than one per 30 s.
    assert!(
        token_requests >= (elapsed / TOKEN_SECS).floor(),
        "too few token requests for {elapsed:.0}s: {token_requests}"
    );
    assert!(
        token_requests <= elapsed / 30.0 + 2.0,
        "too many token requests for {elapsed:.0}s: {token_requests}"
    );
    let ends = end_offsets(&topic);
    assert_eq!(total(&ends), records as i64);
    assert_eq!(
        run.manifest_records(&backup_id),
        ends,
        "every record backed up"
    );
    run.assert_no_gaps_or_duplicates(&backup_id, &ends);
    assert!(!log.contains(SECRET), "secret in the log");
}

/// The IdP goes away: briefly (tokens still valid: the backup carries on),
/// then for longer than a token lives (connections cannot re-authenticate:
/// a clean failure, no panic, no secret in the logs), then comes back (the
/// backup recovers, in-process or on the next run).
#[test]
#[ignore = "requires the tests/sasl-oauth-test-infra identity-provider stack"]
fn idp_outages_short_and_beyond_token_lifetime() {
    let _serial = serial();
    require_stack();
    let run = Run::new();
    let topic = unique("outage");
    create_topic(&topic, 3);
    let backup_id = unique("outage");
    let providers = oidc_provider("kafka", "basic");
    let config = run.write(
        "backup.yaml",
        &backup_yaml(&run, &backup_id, &topic, true, &providers, "idp"),
    );

    let mut backup = run.spawn(
        &["backup", "--config", config.to_str().unwrap()],
        "outage.log",
    );
    let mut producer = spawn_producer(&topic, 5 * 420, 5);
    thread::sleep(Duration::from_secs(30));

    // Short outage: well inside the token lifetime.
    sh("docker", &["stop", "-t", "1", IDP_CONTAINER]);
    thread::sleep(Duration::from_secs(20));
    sh("docker", &["start", IDP_CONTAINER]);
    wait_for_idp(Duration::from_secs(60));
    assert!(
        backup.try_wait().unwrap().is_none(),
        "backup stopped during a short IdP outage:\n{}",
        run.log("outage.log")
    );
    let short_ok = run.manifest_records(&backup_id);
    println!(
        "after the short outage: {} records backed up",
        total(&short_ok)
    );
    assert!(total(&short_ok) > 0, "the backup carried on");
    thread::sleep(Duration::from_secs(30));

    // Long outage: beyond the 120 s token lifetime.
    let long_from = Instant::now();
    sh("docker", &["stop", "-t", "1", IDP_CONTAINER]);
    thread::sleep(Duration::from_secs(190));
    let during = run.log("outage.log");
    sh("docker", &["start", IDP_CONTAINER]);
    wait_for_idp(Duration::from_secs(60));
    println!("IdP back after {:.0}s", long_from.elapsed().as_secs_f64());

    let token_failures = during.matches("token request failed").count()
        + during.matches("OAuth token provider 'idp'").count();
    assert!(
        token_failures > 0,
        "the outage should surface as token errors:\n{during}"
    );
    assert!(!during.contains("panicked"), "{during}");
    assert!(!during.contains(SECRET), "secret in logs");

    // Recovery: either the running backup recovers, or it failed cleanly
    // and the next run completes.
    assert!(producer.wait().unwrap().success());
    let deadline = Instant::now() + Duration::from_secs(150);
    let expected = end_offsets(&topic);
    let mut recovered_in_process = false;
    while Instant::now() < deadline {
        if backup.try_wait().unwrap().is_some() {
            break;
        }
        if std::fs::metadata(run.path("storage").join(&backup_id).join("manifest.json")).is_ok()
            && run.manifest_records(&backup_id) == expected
        {
            recovered_in_process = true;
            break;
        }
        thread::sleep(Duration::from_secs(2));
    }
    if recovered_in_process {
        let code = stop(backup, Duration::from_secs(60));
        let log = run.log("outage.log");
        assert_eq!(code, Some(0), "{log}");
        println!("long outage: the running backup recovered by itself once the IdP returned");
    } else {
        let code = match backup.try_wait().unwrap() {
            Some(status) => status.code(),
            None => stop(backup, Duration::from_secs(60)),
        };
        let log = run.log("outage.log");
        let last_error = log
            .lines()
            .rev()
            .find(|l| l.contains("ERROR") || l.starts_with("Error"))
            .unwrap_or("");
        println!("long outage: backup ended with {code:?}; last error: {last_error}");
        assert!(!log.contains("panicked"), "{log}");
        // Re-run once the IdP is back: it resumes and completes.
        let rerun_yaml = backup_yaml(&run, &backup_id, &topic, false, &providers, "idp");
        let rerun = run.write("rerun.yaml", &rerun_yaml);
        let (code, log) = run.run(
            &["backup", "--config", rerun.to_str().unwrap()],
            "rerun.log",
            Duration::from_secs(180),
        );
        assert_eq!(code, Some(0), "{log}");
        println!("long outage: the rerun after the IdP returned completed the backup");
    }
    assert_eq!(
        run.manifest_records(&backup_id),
        expected,
        "every record backed up"
    );
    run.assert_no_gaps_or_duplicates(&backup_id, &expected);
}

/// File provider whose token changes subject mid-run: the broker refuses the
/// re-authentication ("Cannot change principals"), the connection is
/// re-established with a full handshake, and the backup completes.
#[test]
#[ignore = "requires the tests/sasl-oauth-test-infra identity-provider stack"]
fn file_provider_subject_change_reconnects_and_completes() {
    let _serial = serial();
    require_stack();
    let run = Run::new();
    let token_file = run.path("token");
    let subject = Arc::new(Mutex::new("kafka-backup".to_string()));
    let stop_writer = Arc::new(AtomicBool::new(false));
    let write_token = {
        let token_file = token_file.clone();
        move |client: &str| {
            let token = mint(client)["access_token"].as_str().unwrap().to_string();
            let tmp = token_file.with_extension("tmp");
            std::fs::write(&tmp, format!("{token}\n")).unwrap();
            std::fs::rename(&tmp, &token_file).unwrap();
        }
    };
    write_token("kafka-backup");
    let writer = {
        let (subject, stop_writer, write_token) =
            (subject.clone(), stop_writer.clone(), write_token.clone());
        thread::spawn(move || {
            while !stop_writer.load(Ordering::SeqCst) {
                let client = subject.lock().unwrap().clone();
                write_token(&client);
                for _ in 0..30 {
                    if stop_writer.load(Ordering::SeqCst) {
                        return;
                    }
                    thread::sleep(Duration::from_millis(500));
                }
            }
        })
    };

    let topic = unique("subject");
    create_topic(&topic, 3);
    let backup_id = unique("subject");
    let providers = format!(
        "    vault:\n      type: file\n      path: \"{}\"\n",
        token_file.display()
    );
    let config = run.write(
        "backup.yaml",
        &backup_yaml(&run, &backup_id, &topic, true, &providers, "vault"),
    );
    let backup = run.spawn(
        &["backup", "--config", config.to_str().unwrap()],
        "subject.log",
    );
    let mut producer = spawn_producer(&topic, 5 * 200, 5);

    thread::sleep(Duration::from_secs(70));
    *subject.lock().unwrap() = "kafka-backup-rotated".to_string();
    write_token("kafka-backup-rotated");
    let switched_at = 70.0;

    assert!(producer.wait().unwrap().success());
    thread::sleep(Duration::from_secs(20));
    let code = stop(backup, Duration::from_secs(60));
    stop_writer.store(true, Ordering::SeqCst);
    writer.join().unwrap();
    let log = run.log("subject.log");
    assert_eq!(code, Some(0), "{log}");

    let refused = log
        .lines()
        .filter(|l| l.contains(REAUTH_FAILED) && l.contains("principal"))
        .count();
    let rehandshakes = event_times(&log, HANDSHAKE)
        .into_iter()
        .filter(|t| *t > switched_at)
        .count();
    println!("subject change: {refused} re-authentications refused for a principal change, {rehandshakes} full handshakes after it");
    assert!(
        refused > 0,
        "expected the broker to refuse the principal change:\n{log}"
    );
    assert!(
        rehandshakes > 0,
        "expected reconnects with the new principal:\n{log}"
    );
    let ends = end_offsets(&topic);
    assert_eq!(
        run.manifest_records(&backup_id),
        ends,
        "every record backed up"
    );
    run.assert_no_gaps_or_duplicates(&backup_id, &ends);
}

// ── Azure IMDS and Entra workload identity, through stub servers ────────

/// A tiny HTTP server; `handle(method, path_and_query, headers, body)`
/// returns (status, json).
fn stub_server<F>(handle: F) -> (u16, Arc<AtomicUsize>)
where
    F: Fn(&str, &str, &BTreeMap<String, String>, &str) -> (u16, serde_json::Value) + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(AtomicUsize::new(0));
    let counter = hits.clone();
    thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            counter.fetch_add(1, Ordering::SeqCst);
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            let mut headers = BTreeMap::new();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                let line = line.trim_end();
                if line.is_empty() {
                    break;
                }
                if let Some((k, v)) = line.split_once(':') {
                    headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
                }
            }
            let length: usize = headers
                .get("content-length")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let mut parts = request_line.split_whitespace();
            let (method, target) = (parts.next().unwrap_or(""), parts.next().unwrap_or(""));
            let (status, json) = handle(method, target, &headers, &String::from_utf8_lossy(&body));
            let body = json.to_string();
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    });
    (port, hits)
}

/// Decode `application/x-www-form-urlencoded` text.
fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(byte) => {
                        out.push(byte);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            other => out.push(other),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn form(body: &str) -> BTreeMap<String, String> {
    body.split('&')
        .filter_map(|kv| kv.split_once('='))
        .map(|(k, v)| (percent_decode(k), percent_decode(v)))
        .collect()
}

/// Back up 200 records from a fresh topic with the given provider block.
fn backup_with(run: &Run, providers: &str, provider: &str, label: &str) -> String {
    let topic = unique(label);
    create_topic(&topic, 2);
    produce(&topic, 200);
    let backup_id = unique(label);
    let config = run.write(
        &format!("{label}.yaml"),
        &backup_yaml(run, &backup_id, &topic, false, providers, provider),
    );
    let (code, log) = run.run(
        &["backup", "--config", config.to_str().unwrap()],
        &format!("{label}.log"),
        Duration::from_secs(120),
    );
    assert_eq!(code, Some(0), "{label}:\n{log}");
    let ends = end_offsets(&topic);
    assert_eq!(run.manifest_records(&backup_id), ends);
    run.assert_no_gaps_or_duplicates(&backup_id, &ends);
    log
}

#[test]
#[ignore = "requires the tests/sasl-oauth-test-infra identity-provider stack"]
fn azure_managed_identity_through_an_imds_stub() {
    let _serial = serial();
    require_stack();
    let (port, hits) = stub_server(|method, target, headers, _| {
        let query: BTreeMap<String, String> = target
            .split_once('?')
            .map(|(_, q)| form(q))
            .unwrap_or_default();
        if method != "GET"
            || !target.starts_with("/metadata/identity/oauth2/token")
            || headers.get("metadata").map(String::as_str) != Some("true")
            || query.get("api-version").map(String::as_str) != Some("2018-02-01")
            || query.get("resource").map(String::as_str) != Some("api://kafka-backup-local")
        {
            return (
                400,
                serde_json::json!({"error": "invalid_request", "error_description": format!("bad request {target} {headers:?}")}),
            );
        }
        // The user-assigned identity's client ID becomes the token subject.
        let client = query
            .get("client_id")
            .cloned()
            .unwrap_or_else(|| "system-assigned".into());
        let token = mint(&client);
        let expires_in = token["expires_in"].as_u64().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        (
            200,
            serde_json::json!({
                "access_token": token["access_token"],
                "client_id": client,
                "expires_in": expires_in.to_string(),
                "expires_on": (now + expires_in).to_string(),
                "not_before": now.to_string(),
                "resource": "api://kafka-backup-local",
                "token_type": "Bearer",
            }),
        )
    });
    let run = Run::new();
    let providers = format!(
        "    mi:\n      type: azure_managed_identity\n      scope: api://kafka-backup-local/.default\n      client_id: kafka-backup-uami\n      endpoint: http://127.0.0.1:{port}/metadata/identity/oauth2/token\n"
    );
    let log = backup_with(&run, &providers, "mi", "imds");
    assert!(hits.load(Ordering::SeqCst) >= 1);
    assert!(log.contains("azure_managed_identity"), "{log}");
    println!(
        "IMDS: backup over OAUTHBEARER with a user-assigned managed identity token ({} IMDS calls)",
        hits.load(Ordering::SeqCst)
    );
}

#[test]
#[ignore = "requires the tests/sasl-oauth-test-infra identity-provider stack"]
fn azure_workload_identity_through_an_entra_stub() {
    let _serial = serial();
    require_stack();
    let run = Run::new();
    let federated = run.write("azure-identity-token", "federated-token-from-the-kubelet\n");
    let (port, hits) = stub_server(move |method, target, headers, body| {
        let f = form(body);
        let ok = method == "POST"
            && target == "/tenant-kb/oauth2/v2.0/token"
            && !headers.contains_key("authorization")
            && f.get("grant_type").map(String::as_str) == Some("client_credentials")
            && f.get("client_id").map(String::as_str) == Some("kafka-backup-wi")
            && f.get("scope").map(String::as_str) == Some("api://kafka-backup-local/.default")
            && f.get("client_assertion_type").map(String::as_str)
                == Some("urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
            && f.get("client_assertion").map(String::as_str)
                == Some("federated-token-from-the-kubelet")
            && !f.contains_key("client_secret");
        if !ok {
            return (
                400,
                serde_json::json!({"error": "invalid_request", "error_description": format!("unexpected {method} {target} {f:?}")}),
            );
        }
        let token = mint("kafka-backup-wi");
        (
            200,
            serde_json::json!({
                "token_type": "Bearer",
                "expires_in": token["expires_in"],
                "ext_expires_in": token["expires_in"],
                "access_token": token["access_token"],
            }),
        )
    });
    let providers = format!(
        "    wi:\n      type: azure_workload_identity\n      scope: api://kafka-backup-local/.default\n      tenant_id: tenant-kb\n      client_id: kafka-backup-wi\n      token_file: \"{}\"\n      authority_host: http://127.0.0.1:{port}/\n",
        federated.display()
    );
    let log = backup_with(&run, &providers, "wi", "workload");
    assert!(hits.load(Ordering::SeqCst) >= 1);
    assert!(
        !log.contains("federated-token-from-the-kubelet"),
        "assertion leaked into the log"
    );
    println!(
        "Workload identity: backup over OAUTHBEARER with a client-assertion token ({} token calls)",
        hits.load(Ordering::SeqCst)
    );
}

#[test]
#[ignore = "requires the tests/sasl-oauth-test-infra identity-provider stack"]
fn oidc_discovery_from_the_mock_issuer() {
    let _serial = serial();
    require_stack();
    let run = Run::new();
    let providers = format!(
        "    idp:\n      type: oidc_client_credentials\n      issuer: {IDP}/kafka\n      client_id: kafka-backup\n      client_secret: ${{KB_OAUTH_SECRET}}\n      scope: kafka\n"
    );
    let log = backup_with(&run, &providers, "idp", "discovery");
    assert!(log.contains("OIDC token endpoint discovered"), "{log}");
}

/// The offset commands reach an OAUTHBEARER cluster through
/// `--config --cluster`: snapshot a group's offsets, then verify them.
#[test]
#[ignore = "requires the tests/sasl-oauth-test-infra identity-provider stack"]
fn offset_commands_use_the_config_files_provider() {
    let _serial = serial();
    require_stack();
    let run = Run::new();
    let topic = unique("offsets");
    create_topic(&topic, 1);
    produce(&topic, 50);
    let group = unique("group");
    // Commit an offset for the group with the broker's own consumer.
    docker_kafka(&[
        "kafka-console-consumer",
        "--bootstrap-server",
        INTERNAL_BOOTSTRAP,
        "--topic",
        &topic,
        "--group",
        &group,
        "--from-beginning",
        "--max-messages",
        "50",
        "--timeout-ms",
        "30000",
    ]);

    let config = run.write(
        "restore.yaml",
        &restore_yaml(
            &run,
            "unused",
            &topic,
            "unused",
            &oidc_provider("kafka", "basic"),
        ),
    );
    let (code, log) = run.run(
        &[
            "offset-rollback",
            "snapshot",
            "--path",
            &run.storage(),
            "--groups",
            &group,
            "--config",
            config.to_str().unwrap(),
            "--cluster",
            "target",
            "--format",
            "json",
        ],
        "snapshot.log",
        Duration::from_secs(120),
    );
    assert_eq!(code, Some(0), "{log}");
    assert!(log.contains(HANDSHAKE), "{log}");
    let snapshots: Vec<_> = std::fs::read_dir(run.path("storage").join("offset-snapshots"))
        .unwrap()
        .flatten()
        .collect();
    assert_eq!(snapshots.len(), 1, "one snapshot saved");
    let id = snapshots[0].file_name().to_string_lossy().into_owned();
    let snapshot: serde_json::Value =
        serde_json::from_slice(&std::fs::read(snapshots[0].path().join("snapshot.json")).unwrap())
            .unwrap();
    assert_eq!(
        snapshot["group_offsets"][&group]["offsets"][&topic]["0"]["offset"], 50,
        "{snapshot}"
    );

    let (code, log) = run.run(
        &[
            "offset-rollback",
            "verify",
            "--path",
            &run.storage(),
            "--snapshot-id",
            &id,
            "--config",
            config.to_str().unwrap(),
        ],
        "verify.log",
        Duration::from_secs(120),
    );
    assert_eq!(code, Some(0), "{log}");
    assert!(log.contains(HANDSHAKE), "{log}");
    println!("offset-rollback snapshot + verify over OAUTHBEARER through --config: group {group} at offset 50");
}
