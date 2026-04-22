# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.15.0] - 2026-04-21

### Added
- **SASL/GSSAPI (Kerberos) authentication** via the `gssapi` Cargo feature.
  Default builds remain Kerberos-free; opt in with
  `cargo build --features gssapi -p kafka-backup-cli`. State machine
  and credential hints adapted from @kthimjo's PR #95 — thank you.
  - New `SaslMechanism::Gssapi` enum variant.
  - New optional `SecurityConfig` fields: `sasl_kerberos_service_name`,
    `sasl_keytab_path`, `sasl_krb5_config_path`.
  - `GssapiPlugin` implements RFC 4752 Phase 1 multi-round `gss_init_sec_context`,
    Phase 1→2 turnaround, Phase 2 `layer = 0x01` (no security layer, no size)
    wrap/unwrap, and KIP-368 re-authentication via fresh-context rebuild.
  - `GssapiPluginFactory` — constructed from the operator-provided keytab +
    krb5.conf + service name, validated eagerly at config time. The factory
    binds the SPN hostname at `.build()` time (see **Factory extension point**
    below), so each per-broker `KafkaClient` authenticates against the correct
    per-broker SPN (`kafka/brokerN.fqdn@REALM`) on multi-broker clusters.
  - Process-wide `KRB5_ENV_LOCK: tokio::sync::Mutex<()>` serialises
    `KRB5_CLIENT_KTNAME` / `KRB5_CONFIG` / `KRB5CCNAME` mutation during
    credential acquisition — eliminates the multi-client env-var race
    inherent to `libgssapi 0.9`.
  - When a keytab is configured, `GssapiPlugin` isolates its credential
    cache via `KRB5CCNAME=MEMORY:<ptr>`. This prevents stale tickets in
    the OS default ccache (common on macOS `API:<uuid>` caches) from
    being preferred over a fresh TGT from the keytab — a failure mode
    that surfaces as a cryptic broker-side
    `Authentication failed due to invalid credentials`.
- **Factory extension point** — `SecurityConfig.sasl_mechanism_plugin_factory:
  Option<SaslMechanismPluginFactoryHandle>` replaces the prior
  `sasl_mechanism_plugin: Option<SaslMechanismPluginHandle>` (both introduced
  on this branch; neither has shipped). `KafkaClient::authenticate` calls
  `factory.build(broker_host, broker_port)` once per connection, receiving the
  endpoint from `bootstrap_servers[0]` — which `PartitionLeaderRouter` has
  already rewritten to the advertised per-broker `host:port` before spawning
  pooled clients. This fixes one correctness bug and removes a latent one:
  1. **Multi-broker GSSAPI SPN (fixed).** Non-bootstrap brokers now
     authenticate against their own SPN
     (`kafka/brokerN.fqdn@REALM`) rather than the bootstrap host's — the
     standard librdkafka / JVM-client behaviour.
  2. **Per-connection GSSAPI state (removed as a latent risk).** Each
     pooled `KafkaClient` now owns its own `GssapiPlugin` and its own
     `ClientCtx`. A shared plugin across the pool would have been a
     concurrency hazard even if it has not produced a visible failure in
     the current test matrix.
  - `SharedPluginFactory` — convenience wrapper for stateless mechanisms
    (PLAIN, OAUTHBEARER with a shared token provider); returns the same Arc
    from every `build` call.
  - New `SaslPluginError::FactoryFailed { mechanism, source }` variant for
    clean error surfaces at build time.
- **`SaslMechanismPlugin::supports_reauth()` capability flag** — default
  `true` (PLAIN, SCRAM, OAUTHBEARER continue to schedule KIP-368 live
  re-auth); `GssapiPlugin` overrides to `false`. Apache Kafka does not
  support live re-authentication for GSSAPI — Kerberos GSS-API contexts
  are bound to the wire connection, and the broker rejects in-place
  `SaslAuthenticate` after the initial handshake. Matches librdkafka:
  treat the broker-advertised `session_lifetime_ms` as a
  drain-and-reconnect timer rather than firing a reauth the broker will
  reject. With the plugin opting out, `KafkaClient::authenticate` skips
  `spawn_reauth_task` entirely; the session expires naturally and the
  next RPC reconnects through the normal auth path.
- CLI plumbing: new flags `--sasl-mechanism`, `--sasl-keytab`,
  `--sasl-krb5-config`, `--sasl-kerberos-service-name` on `offset-reset`,
  `offset-reset-bulk`, and `offset-rollback` commands. YAML configs auto-wire
  a `GssapiPluginFactory` when `sasl_mechanism: GSSAPI` is set. A runtime
  error surfaces if the CLI was built without `--features gssapi`.
- Deduplicated CLI security-args parsing via `commands/security_args.rs`
  (`#[derive(clap::Args)] SecurityCliArgs`) — removes three copies of the prior
  `parse_security_config` helper.
- Docker test fixture at `tests/sasl-gssapi-test-infra/` — self-hosted MIT KDC
  (`Dockerfile.kdc`), Apache Kafka 7.7.0 configured for
  `SASL_PLAINTEXT://kafka.test.local:9098` with `GSSAPI` enabled, realm
  `TEST.LOCAL`, keytab auto-generation with healthcheck gate.
- Three `#[ignore]` E2E tests: keytab happy-path, missing-keytab clear error,
  KIP-368 reauth fires within broker's 60 s window
  (`crates/kafka-backup-core/tests/integration_suite/sasl_gssapi_tests.rs`).
- Full backup → restore roundtrip E2E test over GSSAPI
  (`sasl_gssapi_backup_restore_roundtrip`): produces records, drives
  `BackupEngine` + `RestoreEngine` with topic remap, consumes from the
  restored topic and asserts record count + payload. Runs at the default
  `connections_per_broker: 4` now that each pooled connection owns its own
  `GssapiPlugin` via the factory.
- Factory-dispatch regression test
  (`sasl_plugin_mock_tests::factory_receives_per_broker_endpoint`): a
  `CapturingFactory` asserts `build(host, port)` is called exactly once per
  `KafkaClient` with the endpoint from `bootstrap_servers[0]`. No Docker —
  uses the in-process `MockKafkaBroker` fixture.
- Pool-isolation regression test
  (`sasl_plugin_mock_tests::pool_produces_distinct_plugin_per_kafkaclient`):
  N=3 separate `MockKafkaBroker` instances, N `KafkaClient`s sharing one
  `SaslMechanismPluginFactory`; asserts the factory is invoked once per
  client with the correct endpoint and returns a pointer-distinct plugin
  Arc each time. Turns item 2 above ("removed as a latent risk") into a
  tested guarantee.
- Scheduler-opt-out regression test
  (`sasl_plugin_mock_tests::reauth_scheduler_not_spawned_when_plugin_opts_out`):
  a plugin returning `supports_reauth() = false` connects against a mock
  that advertises `session_lifetime_ms: 60_000`; virtual time is advanced
  past the 80 % reauth deadline; the test asserts `reauth_payload` is
  never called and the mock sees exactly one `SaslAuthenticate` frame.
- Example YAML configs for operators: `config/gssapi-backup.yaml` and
  `config/gssapi-restore.yaml`, driving the release binary end-to-end
  against the fixture.
- Release-binary CLI smoke script at
  `tests/sasl-gssapi-test-infra/run-cli-smoke.sh` — builds
  `--release --features gssapi` and exercises `kafka-backup backup`
  and `kafka-backup restore` against the fixture, asserting exit codes,
  manifest existence, and restored record count.

### Build requirements
- `gssapi` feature links against MIT krb5 at build time. Install:
  - macOS: `brew install krb5` + export
    `PKG_CONFIG_PATH="$(brew --prefix krb5)/lib/pkgconfig:…"` (Apple's bundled
    Heimdal does not expose the symbols `libgssapi 0.9` requires).
  - Debian/Ubuntu: `apt-get install libkrb5-dev`.
  - Fedora/RHEL: `dnf install krb5-devel`.

### Notes on GSSAPI re-authentication
- Apache Kafka does not support live KIP-368 re-authentication for the
  GSSAPI mechanism — Kerberos GSS-API contexts are bound to the wire
  connection and the broker rejects in-place `SaslAuthenticate` after
  the initial handshake. `GssapiPlugin::supports_reauth()` returns
  `false`, so the client no longer schedules a reauth task for GSSAPI
  connections; the broker-advertised `session_lifetime_ms` is treated
  as a drain-and-reconnect window, matching librdkafka and the JVM
  client behaviour. The connection lives out its session and the next
  RPC transparently reconnects through the normal auth path.

### Limitations
- The mock-broker test proves the factory contract (`build` is called with the
  correct endpoint per `KafkaClient`). A multi-broker Docker GSSAPI fixture
  that exercises distinct per-broker SPNs end-to-end is a planned follow-up.
- Release binaries and the default Docker image do not include GSSAPI. Build
  your own image with `--build-arg FEATURES=gssapi` once the downstream image
  ships that arg.

## [0.14.0] - 2026-04-21

### Added
- **Pluggable SASL mechanism extension point** (`SaslMechanismPlugin` trait)
  — lets downstream crates implement OAUTHBEARER, MSK IAM, or custom
  SASL mechanisms without forking `kafka-backup-core`.
  - Handshake + single- or multi-round `SaslAuthenticate` dispatch.
  - KIP-368 re-authentication scheduler: spawns a task post-handshake
    when the broker advertises `session_lifetime_ms > 0`; fires
    `reauth_payload` at 80 % of the advertised lifetime with a 30 s
    minimum floor and ±5 s jitter.
  - Default `interpret_server_error` handles both RFC 7628 JSON and
    Apache Kafka 3.5+ free-form `error_message` bytes.
  - New field `SecurityConfig.sasl_mechanism_plugin: Option<Arc<dyn SaslMechanismPlugin>>`
    (marked `#[serde(skip)]` — programmatic wiring only, no YAML surface).
- 14 unit tests + 4 integration tests exercising single-round,
  multi-round, server-error, and scheduler paths against an
  in-process Kafka-wire mock (no Docker required).
- `#[ignore]` E2E test against Confluent cp-kafka 7.7.0 configured for
  SASL_PLAINTEXT + OAUTHBEARER with the bundled unsecured-JWS validator.
  Fixture: `tests/sasl-oauth-test-infra/`.
- Example: `examples/custom_sasl_plugin.rs` — minimal static-token
  OAUTHBEARER plugin (reference implementation).

### Changed
- SASL dispatch in `KafkaClient` unified: the four duplicated
  `sasl_{plain,scram}_auth{,_raw}` methods collapse into a single
  dispatch function called by both initial-connect and reconnect.
  Behaviour for existing `PLAIN` / `SCRAM-SHA-256` / `SCRAM-SHA-512`
  configurations is unchanged.

## [0.13.5] - 2026-04-16

### Fixed
- **Incremental one-shot backups now work** — offset tracking was previously gated on `continuous: true`,
  making one-shot and snapshot backups always start from `earliest`. Now, adding `offset_storage` to the
  config enables resume-from-last-offset in any backup mode.

### Added
- Unit tests for `merge_manifests()` function (previously untested)
- Integration test for incremental one-shot backup resume behavior

## [0.5.0] - 2026-01-17

### Added
- **Prometheus/OpenMetrics metrics support** ([#9](https://github.com/osodevops/kafka-backup/issues/9))
  - Consumer lag tracking per topic/partition (`kafka_backup_lag_records`)
  - Records and bytes throughput counters (`kafka_backup_records_total`, `kafka_backup_bytes_total`)
  - Compression ratio gauge (`kafka_backup_compression_ratio`)
  - Storage write latency histogram (`kafka_backup_storage_write_latency_seconds`)
  - Storage I/O bytes counter (`kafka_backup_storage_write_bytes_total`)
  - Error counting by type (`kafka_backup_errors_total`)
- HTTP metrics server with `/metrics` endpoint (default port 8080)
- `/health` endpoint for liveness checks
- New `metrics` configuration section in config file
- `MetricsServerConfig::new()` constructor for programmatic configuration

### Changed
- **Breaking:** Added `metrics: Option<MetricsConfig>` field to `Config` struct
  - Existing code constructing `Config` with struct literals must add `metrics: None`
  - YAML configs are unaffected (field is optional with serde default)
- Marked `MetricsConfig` as `#[non_exhaustive]` to prevent future breaking changes

### Documentation
- Added Metrics & Monitoring section to README
- Full metrics reference available at [kafka-backup-docs](https://osodevops.github.io/kafka-backup-docs/docs/reference/metrics)
- Monitoring stack (Prometheus + Grafana) available in [kafka-backup-demos](https://github.com/osodevops/kafka-backup-demos/tree/main/monitoring-stack)

## [0.4.0] - 2026-01-09

### Added
- TLS/SSL support for custom CA certificates (`ssl_ca_location`)
- Mutual TLS (mTLS) authentication with client certificates (`ssl_certificate_location`, `ssl_key_location`)
- TLS test infrastructure with Docker Compose for integration testing
- Comprehensive TLS documentation in configuration guide

### Fixed
- **Breaking:** Fixed TLS certificate configuration being ignored ([#3](https://github.com/osodevops/kafka-backup/issues/3))
  - Previously, `ssl_ca_location`, `ssl_certificate_location`, and `ssl_key_location` were parsed but never used
  - Connections to Kafka with self-signed or internal CA certificates now work correctly
  - Added new error variants to `KafkaError`: `TlsConfig`, `CertificateLoad`, `PrivateKeyLoad`

### Changed
- **Breaking:** Added new variants to `KafkaError` enum. Code that exhaustively matches on this enum without a wildcard will need updating.

## [0.1.4] - 2025-12-03

### Added
- crates.io publishing for `kafka-backup-core` library
- Semantic version checking workflow for breaking change detection
- Dependabot configuration for operator repository
- Crate-specific README for kafka-backup-core

### Changed
- Updated kafka-backup-core package metadata for crates.io compatibility

## [0.1.3] - 2025-12-01

### Added
- Try It Yourself section linking to demos repository
- Suggest Features link to Contributing section
- GitHub issue templates for bugs and feature requests
- Contributing section in README

### Changed
- Improved issue templates structure

## [0.1.2] - 2025-11-30

### Added
- Scoop package manager support for Windows installation
- Docker Hub automated publishing on releases
- Comprehensive installation guide in README

### Changed
- Simplified Homebrew install to one-liner (`brew install osodevops/tap/kafka-backup`)
- Renamed Homebrew formula to `kafka-backup`
- Updated README installation instructions
- Fixed Docker image naming to use semantic versions

## [0.1.0] - 2025-11-30

### Added
- Initial release of kafka-backup
- `BackupEngine` for backing up Kafka topics to cloud storage
- `RestoreEngine` with point-in-time recovery (PITR) support
- Multi-cloud storage support:
  - Amazon S3
  - Azure Blob Storage
  - Google Cloud Storage
  - Local filesystem
  - In-memory (for testing)
- Consumer group offset recovery with multiple strategies:
  - `skip` - restore data only
  - `header-based` - extract offset from message headers
  - `timestamp-based` - query target by timestamp
  - `cluster-scan` - scan target `__consumer_offsets`
  - `manual` - operator-driven reset
- Three-phase restore orchestration for exact offset recovery
- Offset snapshot and rollback functionality
- Compression support: zstd, lz4, gzip, snappy
- Prometheus metrics integration
- Circuit breaker pattern for fault tolerance
- SQLite-based offset tracking with cloud sync
- CLI with commands: backup, restore, list, describe, validate, offset-reset
- cargo-dist release workflow with cross-platform binaries
- Homebrew tap for macOS/Linux installation

[Unreleased]: https://github.com/osodevops/kafka-backup/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/osodevops/kafka-backup/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/osodevops/kafka-backup/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/osodevops/kafka-backup/compare/v0.1.4...v0.3.0
[0.1.4]: https://github.com/osodevops/kafka-backup/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/osodevops/kafka-backup/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/osodevops/kafka-backup/compare/v0.1.0...v0.1.2
[0.1.0]: https://github.com/osodevops/kafka-backup/releases/tag/v0.1.0
