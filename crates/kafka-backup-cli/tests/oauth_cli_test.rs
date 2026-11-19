//! Issue #261 — OAuth (SASL/OAUTHBEARER) at the CLI, with no Docker.
//!
//! - Every command that connects to Kafka applies the configuration's
//!   `oauth` providers: run against an in-process mock broker and a mock
//!   identity provider, each one sends an RFC 7628 payload carrying the
//!   provider's token and the cluster's SASL extensions.
//! - A wrong OAuth configuration (unknown provider, missing field, invalid
//!   extension, misplaced key, plain-http endpoint) fails with a clear
//!   message before any connection is made.
//! - `--sasl-mechanism OAUTHBEARER` on a flag-driven command says how to
//!   configure a provider.
//! - No client secret or token appears in any output.

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

mod support;

#[allow(dead_code)]
#[path = "../../kafka-backup-core/tests/integration_suite/sasl_mock_broker.rs"]
mod sasl_mock_broker;

use sasl_mock_broker::{Exchange, MockKafkaBroker};
use support::{backup_fixture, kb_env, offset_mapping_fixture, tcp_sink, Run, Sandbox};

const BACKUP_ID: &str = "oauth-cli";
const SECRET: &str = "cli-secret-value-7c1e";
const SECRET_ENV: (&str, &str) = ("OAUTH_CLI_TEST_SECRET", SECRET);

/// A backup or restore configuration whose only cluster (`role`) uses
/// OAUTHBEARER through provider `idp`.
fn cluster_config(
    mode: &str,
    role: &str,
    bootstrap: &str,
    store: &Path,
    token_url: &str,
) -> String {
    format!(
        r#"mode: {mode}
backup_id: {BACKUP_ID}
{role}:
  bootstrap_servers: ["{bootstrap}"]
  topics:
    include: ["orders"]
  security:
    security_protocol: SASL_PLAINTEXT
    sasl_mechanism: OAUTHBEARER
    oauth:
      provider: idp
      extensions: {{ logicalCluster: lkc-cli01, identityPoolId: pool-cli }}
storage:
  backend: filesystem
  path: {store}
metrics:
  enabled: false
oauth:
  providers:
    idp:
      type: oidc_client_credentials
      token_endpoint_url: {token_url}
      client_id: kafka-backup
      client_secret: ${{OAUTH_CLI_TEST_SECRET}}
      scope: kafka
"#,
        store = store.display()
    )
}

/// The `validation run` configuration: same provider, `target` cluster.
fn validation_config(bootstrap: &str, store: &Path, token_url: &str) -> String {
    format!(
        r#"backup_id: {BACKUP_ID}
storage:
  backend: filesystem
  path: {store}
target:
  bootstrap_servers: ["{bootstrap}"]
  security:
    security_protocol: SASL_PLAINTEXT
    sasl_mechanism: OAUTHBEARER
    oauth:
      provider: idp
      extensions: {{ logicalCluster: lkc-cli01, identityPoolId: pool-cli }}
oauth:
  providers:
    idp:
      type: oidc_client_credentials
      token_endpoint_url: {token_url}
      client_id: kafka-backup
      client_secret: ${{OAUTH_CLI_TEST_SECRET}}
      scope: kafka
"#,
        store = store.display()
    )
}

fn write(sandbox: &Sandbox, name: &str, yaml: &str) -> String {
    let path = sandbox.root.join(name);
    std::fs::write(&path, yaml).unwrap();
    path.display().to_string()
}

/// A sandbox seeded with a backup and an offset mapping for group `g1`.
fn seeded() -> Sandbox {
    let sandbox = Sandbox::new();
    sandbox.seed(&backup_fixture(BACKUP_ID));
    sandbox.seed(&offset_mapping_fixture(BACKUP_ID, "g1", 5));
    sandbox
}

/// Which configuration file a command takes.
#[derive(Clone, Copy)]
enum Cfg {
    /// `mode: backup`, cluster under `source`.
    Backup,
    /// `mode: restore`, cluster under `target`.
    Restore,
    /// A `validation run` configuration.
    Validation,
}

/// Every command that opens a Kafka connection, with the arguments that
/// follow its configuration file (`CONFIG`) and storage location (`STORE`).
const CONNECTING_COMMANDS: &[(&str, Cfg, &[&str])] = &[
    ("backup", Cfg::Backup, &["backup", "--config", "CONFIG"]),
    (
        "snapshot-groups",
        Cfg::Backup,
        &["snapshot-groups", "--config", "CONFIG"],
    ),
    ("restore", Cfg::Restore, &["restore", "--config", "CONFIG"]),
    (
        "three-phase-restore",
        Cfg::Restore,
        &["three-phase-restore", "--config", "CONFIG"],
    ),
    (
        "validation run",
        Cfg::Validation,
        &["validation", "run", "--config", "CONFIG"],
    ),
    (
        "offset-reset execute",
        Cfg::Restore,
        &[
            "offset-reset",
            "execute",
            "--path",
            "STORE",
            "--backup-id",
            BACKUP_ID,
            "--groups",
            "g1",
            "--config",
            "CONFIG",
        ],
    ),
    (
        "offset-reset-bulk",
        Cfg::Restore,
        &[
            "offset-reset-bulk",
            "--path",
            "STORE",
            "--backup-id",
            BACKUP_ID,
            "--groups",
            "g1",
            "--config",
            "CONFIG",
            "--cluster",
            "target",
        ],
    ),
    (
        "offset-rollback snapshot",
        Cfg::Restore,
        &[
            "offset-rollback",
            "snapshot",
            "--path",
            "STORE",
            "--groups",
            "g1",
            "--config",
            "CONFIG",
        ],
    ),
    (
        "offset-rollback rollback",
        Cfg::Restore,
        &[
            "offset-rollback",
            "rollback",
            "--path",
            "STORE",
            "--snapshot-id",
            "snap-oauth",
            "--config",
            "CONFIG",
        ],
    ),
    (
        "offset-rollback verify",
        Cfg::Restore,
        &[
            "offset-rollback",
            "verify",
            "--path",
            "STORE",
            "--snapshot-id",
            "snap-oauth",
            "--config",
            "CONFIG",
        ],
    ),
];

/// An offset snapshot holding one committed offset, so that rollback and
/// verify have something to send to the cluster.
fn seed_offset_snapshot(sandbox: &Sandbox) {
    let created_at = "2026-10-01T12:00:00Z";
    let json = |v: serde_json::Value| serde_json::to_vec_pretty(&v).unwrap();
    sandbox.seed(&vec![
        (
            "offset-snapshots/snap-oauth/snapshot.json".to_string(),
            json(serde_json::json!({
                "snapshot_id": "snap-oauth",
                "created_at": created_at,
                "group_offsets": {"g1": {
                    "group_id": "g1",
                    "offsets": {"orders": {"0": {
                        "offset": 5, "metadata": null, "timestamp": 1_790_000_060_000_i64
                    }}},
                    "partition_count": 1
                }},
                "bootstrap_servers": ["kafka:9092"],
                "description": "oauth",
            })),
        ),
        (
            "offset-snapshots/snap-oauth/metadata.json".to_string(),
            json(serde_json::json!({
                "snapshot_id": "snap-oauth",
                "created_at": created_at,
                "group_count": 1,
                "offset_count": 1,
                "description": "oauth",
            })),
        ),
    ]);
}

fn config_for(cfg: Cfg, sandbox: &Sandbox, bootstrap: &str, token_url: &str) -> String {
    let yaml = match cfg {
        Cfg::Backup => cluster_config("backup", "source", bootstrap, &sandbox.store, token_url),
        Cfg::Restore => cluster_config("restore", "target", bootstrap, &sandbox.store, token_url),
        Cfg::Validation => validation_config(bootstrap, &sandbox.store, token_url),
    };
    write(sandbox, "config.yaml", &yaml)
}

fn args_for(template: &[&str], config: &str, sandbox: &Sandbox) -> Vec<String> {
    template
        .iter()
        .map(|a| match *a {
            "CONFIG" => config.to_string(),
            "STORE" => sandbox.store_url(),
            other => other.to_string(),
        })
        .collect()
}

/// Each connecting command fetches a token from the configured provider and
/// authenticates with it, sending the cluster's SASL extensions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_connecting_command_authenticates_with_the_configured_provider() {
    for (index, (name, cfg, template)) in CONNECTING_COMMANDS.iter().enumerate() {
        let token = format!("cli-access-token-{index}");
        let idp = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("scope=kafka"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": token, "token_type": "Bearer", "expires_in": 3600
            })))
            .mount(&idp)
            .await;
        // The mock authenticates one connection and then hangs up, which
        // fails whatever the command does next; the payload is the point.
        let broker = MockKafkaBroker::start(vec![
            Exchange::HandshakeSuccess,
            Exchange::AuthenticateSuccess {
                auth_bytes: vec![],
                session_lifetime_ms: 0,
            },
        ])
        .await;

        let sandbox = seeded();
        seed_offset_snapshot(&sandbox);
        let config = config_for(
            *cfg,
            &sandbox,
            &broker.bootstrap(),
            &format!("{}/token", idp.uri()),
        );
        let args = args_for(template, &config, &sandbox);

        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_kafka-backup"))
            .args(&args)
            .current_dir(sandbox.fresh_cwd())
            .env(SECRET_ENV.0, SECRET_ENV.1)
            .env("RUST_LOG", "debug")
            .env("NO_PROXY", "127.0.0.1,localhost")
            .env_remove("KAFKA_SASL_MECHANISM")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(45);
        let payload = loop {
            let captured = broker.captured().await;
            if let Some(payload) = captured.authenticate_payloads.first() {
                assert_eq!(
                    captured.handshake_mechanism.as_deref(),
                    Some("OAUTHBEARER"),
                    "{name}"
                );
                break payload.clone();
            }
            let exited = child.try_wait().unwrap().is_some();
            if exited || Instant::now() > deadline {
                let _ = child.start_kill();
                let out = child.wait_with_output().await.unwrap();
                panic!(
                    "`{name}` {} without authenticating (args {args:?})\nstdout:\n{}\nstderr:\n{}",
                    if exited { "exited" } else { "ran for 45s" },
                    String::from_utf8_lossy(&out.stdout),
                    String::from_utf8_lossy(&out.stderr)
                );
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(
            String::from_utf8_lossy(&payload),
            format!(
                "n,,\x01identityPoolId=pool-cli\x01logicalCluster=lkc-cli01\x01auth=Bearer {token}\x01\x01"
            ),
            "{name}"
        );

        // Let the command fail on the closed connection, then check that
        // neither the secret nor the token reached its output.
        let _ = tokio::time::timeout(Duration::from_secs(5), child.wait()).await;
        let _ = child.start_kill();
        let out = child.wait_with_output().await.unwrap();
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            !output.contains(SECRET),
            "`{name}` printed the secret:\n{output}"
        );
        assert!(
            !output.contains(&token),
            "`{name}` printed the token:\n{output}"
        );
        broker.shutdown().await;
    }
}

/// One way a configuration can be wrong, as an edit to the valid YAML, and
/// the text the error must contain.
struct Broken {
    what: &'static str,
    edit: fn(&str) -> String,
    expect: &'static [&'static str],
}

const BROKEN: &[Broken] = &[
    Broken {
        what: "unknown provider name",
        edit: |y| y.replace("      provider: idp", "      provider: entra"),
        expect: &["security.oauth", "'entra'", "defined: idp"],
    },
    Broken {
        what: "missing provider field",
        edit: |y| y.replace("      client_id: kafka-backup\n", ""),
        expect: &["client_id"],
    },
    Broken {
        what: "invalid extension value",
        edit: |y| {
            y.replace(
                "logicalCluster: lkc-cli01",
                "logicalCluster: \"lkc-\\u0001\"",
            )
        },
        expect: &["security.oauth.extensions", "logicalCluster"],
    },
    Broken {
        what: "unknown extension",
        edit: |y| y.replace("identityPoolId: pool-cli", "identity_pool_id: pool-cli"),
        expect: &["identity_pool_id"],
    },
    Broken {
        what: "misspelt provider key",
        edit: |y| y.replace("      scope: kafka", "      scop: kafka"),
        expect: &["scop"],
    },
    Broken {
        what: "provider setting in the binding",
        edit: |y| {
            y.replace(
                "      provider: idp\n",
                "      provider: idp\n      client_id: kafka-backup\n",
            )
        },
        expect: &["client_id"],
    },
    Broken {
        what: "OAUTHBEARER without a binding",
        edit: |y| {
            y.replace(
                "    oauth:\n      provider: idp\n      extensions: { logicalCluster: lkc-cli01, identityPoolId: pool-cli }\n",
                "",
            )
        },
        expect: &["sasl_mechanism is OAUTHBEARER", "security.oauth is not set"],
    },
    Broken {
        what: "a binding without OAUTHBEARER",
        edit: |y| {
            y.replace(
                "    security_protocol: SASL_PLAINTEXT\n    sasl_mechanism: OAUTHBEARER\n",
                "    security_protocol: PLAINTEXT\n",
            )
        },
        expect: &["security.oauth is set", "is not OAUTHBEARER"],
    },
    Broken {
        what: "OAUTHBEARER without SASL",
        edit: |y| {
            y.replace(
                "security_protocol: SASL_PLAINTEXT",
                "security_protocol: SSL",
            )
        },
        expect: &["security_protocol", "SASL_SSL"],
    },
    Broken {
        what: "plain-http token endpoint",
        edit: |y| y.replace("http://127.0.0.1:9/token", "http://idp.example.com/token"),
        expect: &["plain http", "allow_insecure_http"],
    },
    Broken {
        what: "token endpoint and issuer together",
        edit: |y| {
            y.replace(
                "      client_id: kafka-backup\n",
                "      client_id: kafka-backup\n      issuer: https://idp.example.com\n",
            )
        },
        expect: &["token_endpoint_url", "issuer"],
    },
    Broken {
        what: "invalid cache setting",
        edit: |y| format!("{y}  cache:\n    refresh_fraction: 1.5\n"),
        expect: &["refresh_fraction"],
    },
];

fn failed_clearly(run: &Run, sink_connections: usize, context: &str, expect: &[&str]) {
    let text = run.text();
    assert!(!run.success(), "{context} must fail\n{text}");
    for needle in expect {
        assert!(text.contains(needle), "{context}: no `{needle}` in\n{text}");
    }
    assert_eq!(
        sink_connections, 0,
        "{context}: failed only after connecting to Kafka\n{text}"
    );
    assert!(
        !text.contains(SECRET),
        "{context} printed the secret\n{text}"
    );
    assert!(
        !text.contains("panicked"),
        "{context}: a panic is not a clear message\n{text}"
    );
}

/// A wrong OAuth configuration stops every connecting command before it
/// opens a connection, with a message that names the problem.
#[test]
fn wrong_oauth_configuration_fails_every_command_before_connecting() {
    let sandbox = seeded();
    seed_offset_snapshot(&sandbox);
    for (name, cfg, template) in CONNECTING_COMMANDS {
        for broken in BROKEN {
            let sink = tcp_sink();
            // Port 9 (discard) is never dialled: these fail before a token
            // is requested.
            let valid = match cfg {
                Cfg::Backup => cluster_config(
                    "backup",
                    "source",
                    &sink.addr,
                    &sandbox.store,
                    "http://127.0.0.1:9/token",
                ),
                Cfg::Restore => cluster_config(
                    "restore",
                    "target",
                    &sink.addr,
                    &sandbox.store,
                    "http://127.0.0.1:9/token",
                ),
                Cfg::Validation => {
                    validation_config(&sink.addr, &sandbox.store, "http://127.0.0.1:9/token")
                }
            };
            let yaml = (broken.edit)(&valid);
            assert_ne!(yaml, valid, "{}: the edit applies", broken.what);
            let config = write(&sandbox, "broken.yaml", &yaml);
            let args = args_for(template, &config, &sandbox);
            let args: Vec<&str> = args.iter().map(String::as_str).collect();
            let run = kb_env(&sandbox.fresh_cwd(), &args, &[SECRET_ENV]);
            failed_clearly(
                &run,
                sink.connections(),
                &format!("`{name}` with {}", broken.what),
                broken.expect,
            );
        }
    }
}

/// `validate-restore` opens no Kafka connection, but applies the same OAuth
/// validation as the restore it previews.
#[test]
fn validate_restore_accepts_oauth_and_rejects_a_wrong_provider() {
    let sandbox = seeded();
    let sink = tcp_sink();
    let valid = cluster_config(
        "restore",
        "target",
        &sink.addr,
        &sandbox.store,
        "https://idp.example.com/token",
    );
    let config = write(&sandbox, "restore.yaml", &valid);
    let run = kb_env(
        &sandbox.fresh_cwd(),
        &["validate-restore", "--config", &config],
        &[SECRET_ENV],
    );
    let text = run.text();
    assert!(
        !text.contains("OAuth configuration error") && !text.contains("OAUTHBEARER"),
        "a valid OAuth configuration is accepted\n{text}"
    );
    assert!(!text.contains(SECRET), "{text}");

    let config = write(
        &sandbox,
        "restore-broken.yaml",
        &valid.replace("      provider: idp", "      provider: entra"),
    );
    let run = kb_env(
        &sandbox.fresh_cwd(),
        &["validate-restore", "--config", &config],
        &[SECRET_ENV],
    );
    failed_clearly(
        &run,
        sink.connections(),
        "`validate-restore` with an unknown provider",
        &["'entra'", "defined: idp"],
    );
}

/// The flags cannot describe a token provider: `--sasl-mechanism
/// OAUTHBEARER` says to use `--config` instead of failing at the broker.
#[test]
fn oauthbearer_flag_without_config_explains_how_to_configure_a_provider() {
    let sandbox = seeded();
    seed_offset_snapshot(&sandbox);
    let store = sandbox.store_url();
    let flag_driven: [&[&str]; 5] = [
        &[
            "offset-reset",
            "execute",
            "--path",
            &store,
            "--backup-id",
            BACKUP_ID,
            "--groups",
            "g1",
        ],
        &[
            "offset-reset-bulk",
            "--path",
            &store,
            "--backup-id",
            BACKUP_ID,
            "--groups",
            "g1",
        ],
        &[
            "offset-rollback",
            "snapshot",
            "--path",
            &store,
            "--groups",
            "g1",
        ],
        &[
            "offset-rollback",
            "rollback",
            "--path",
            &store,
            "--snapshot-id",
            "snap-oauth",
        ],
        &[
            "offset-rollback",
            "verify",
            "--path",
            &store,
            "--snapshot-id",
            "snap-oauth",
        ],
    ];
    for command in flag_driven {
        let sink = tcp_sink();
        let mut args = command.to_vec();
        args.extend([
            "--bootstrap-servers",
            &sink.addr,
            "--security-protocol",
            "SASL_SSL",
            "--sasl-mechanism",
            "OAUTHBEARER",
        ]);
        let run = kb_env(&sandbox.fresh_cwd(), &args, &[]);
        failed_clearly(
            &run,
            sink.connections(),
            &format!("{command:?} with --sasl-mechanism OAUTHBEARER"),
            &["--config", "oauth.providers", "security.oauth.provider"],
        );
    }
}

/// `--cluster` picks the cluster of `--config`; `source` on a restore
/// configuration without one is an error that says so.
#[test]
fn cluster_flag_needs_config_and_an_existing_cluster_section() {
    let sandbox = seeded();
    let sink = tcp_sink();
    let config = write(
        &sandbox,
        "restore.yaml",
        &cluster_config(
            "restore",
            "target",
            &sink.addr,
            &sandbox.store,
            "https://idp.example.com/token",
        ),
    );
    let store = sandbox.store_url();
    let base = [
        "offset-rollback",
        "snapshot",
        "--path",
        &store,
        "--groups",
        "g1",
    ];

    let mut args = base.to_vec();
    args.extend(["--config", &config, "--cluster", "source"]);
    let run = kb_env(&sandbox.fresh_cwd(), &args, &[SECRET_ENV]);
    failed_clearly(
        &run,
        sink.connections(),
        "--cluster source on a restore configuration",
        &["no `source:` cluster section"],
    );

    let mut args = base.to_vec();
    args.extend(["--cluster", "target"]);
    let run = kb_env(&sandbox.fresh_cwd(), &args, &[]);
    assert!(!run.success(), "{}", run.text());
    assert!(run.stderr.contains("--config"), "{}", run.text());
}
