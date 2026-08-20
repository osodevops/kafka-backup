# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.17.4] - 2026-08-20

### Changed
- `kafka-protocol` is now the crates.io **0.18.0** release and the temporary
  `[patch.crates-io]` git pin from 0.17.3 has been removed. 0.18.0 contains
  both fixes the pin carried — the record `timestampDelta` varlong decode fix
  (kafka-protocol-rs#159, our [#150](https://github.com/osodevops/kafka-backup/issues/150))
  and the snappy-decompression fix (kafka-protocol-rs#149) — plus the KIP-534
  `delete_horizon` batch attribute (kafka-protocol-rs#157), which this
  codebase already supported. Requested upstream in
  [kafka-protocol-rs#162](https://github.com/kafka-protocol-rs/kafka-protocol-rs/issues/162).
- **Publishing `kafka-backup-core` to crates.io resumes with this release**
  (it was skipped for 0.17.3 while the source patch was active). Library
  consumers no longer need a `[patch.crates-io]` entry in their workspace.

## [0.17.3] - 2026-08-18

### Fixed
- Backups no longer fail — and restores no longer refuse — on record batches
  whose records' timestamps span more than ~24.8 days (`i32::MAX` ms), e.g.
  1970-01-01 placeholder timestamps mixed with current ones. `kafka-protocol`
  0.17.0 decodes each record's `timestampDelta` as a 32-bit varint (the wire
  format is a varlong), so from the first such record on every field was
  misread and the fetch failed with errors like `Failed to decode records:
  Unexpected negative record value length (-54 bytes)` or `invalid utf-8
  sequence`, permanently failing the partition; its encoder also refused to
  produce such batches. `kafka-protocol` is now pinned via `[patch.crates-io]`
  to upstream `main` (`946ceb77`), which carries the fix
  (kafka-protocol-rs#159) and a snappy-decompression corruption fix
  (kafka-protocol-rs#149). Fixes
  [#150](https://github.com/osodevops/kafka-backup/issues/150).

### Changed
- All artifacts built from this repository (CLI binaries, Docker image,
  Homebrew formula) carry the pinned kafka-protocol. The published
  `kafka-backup-core` crate cannot be built against the crates.io
  kafka-protocol 0.17.0 it declares, so **publishing kafka-backup-core to
  crates.io is skipped while the patch is active** (the release workflow
  detects the patch and skips the job; it resumes automatically once a fixed
  kafka-protocol is on crates.io and the patch is removed). Library consumers
  should depend on this repository by git and/or add the same
  `[patch.crates-io]` entry to their workspace.
- Restore still splits produced batches so no batch spans more than
  `i32::MAX` ms; this is no longer required by the pinned encoder but keeps
  restores working when built against the unpatched crate.

## [0.17.2] - 2026-08-18

### Fixed
- `kafka-backup restore` now applies consumer group offsets on the target when
  the configuration asks for it (`reset_consumer_offsets: true`, or
  `auto_consumer_groups: true` with the backup's consumer-groups snapshot).
  Previously only `three-phase-restore` (and the Kubernetes operator) ran
  Phase 3; plain `restore` built the offset mapping, logged
  "imported N consumer group offsets" and "Restore completed successfully",
  and discarded it — no groups were created on the target. Fixes
  [#148](https://github.com/osodevops/kafka-backup/issues/148).
- A failed offset reset after an otherwise successful restore now makes
  `restore` exit non-zero, and the reset summary is printed. Offset commit
  failures name the Kafka error and what to do about it — in particular
  `error code 25 (UNKNOWN_MEMBER_ID: the group has active members on the
  target; stop those consumers, then re-run the reset)`.

### Added
- `ThreePhaseRestore::run_offset_reset_phase(&RestoreReport)` and
  `OffsetResetPhaseOutcome` — Phase 3 as a reusable step for a completed
  Phase 2 restore; `run_all_phases`, the `restore` command and the operator
  all go through it, so offsets are applied exactly once per run.
  `ThreePhaseRestore::wants_offset_reset(&RestoreOptions)`.
- `restore::offset_reset::describe_offset_commit_error(code)`.

### Changed
- The restore engine (Phase 2) still never commits consumer offsets; it now
  logs "Offset mapping ready for N consumer group(s); offsets are applied in
  Phase 3" when a reset is configured, so library callers running the engine
  directly are pointed at `run_offset_reset_phase`. Documented in
  `docs/restore_guide.md` and `docs/configuration.md`.

## [0.17.1] - 2026-08-18

### Fixed
- Connection-loss detection no longer depends on the wording of the OS error
  message. `KafkaClient::send_request` (reconnect + retry once) and the
  partition router's produce / delete-records retry loops classified a failed
  socket read or write by substring-matching `io::Error`'s Display text, which
  is localized and platform-specific: Windows' `WSAECONNABORTED`
  ("An established connection was aborted by the software in your host
  machine. (os error 10053)"), `WSAECONNRESET` (10054) and `WSAETIMEDOUT`
  (10060) never matched — nor did Unix `ETIMEDOUT` ("Operation timed out" /
  "Connection timed out") or `ENOTCONN` — so a dropped connection failed the
  whole backup or restore, sometimes hours in. I/O failures on the broker
  connection are now surfaced as `KafkaError::ConnectionIo`, which preserves
  the `std::io::ErrorKind` and raw OS error code, and a single shared
  classifier (`kafka::is_connection_error`) decides by kind / code —
  `ConnectionAborted`, `ConnectionReset`, `ConnectionRefused`, `BrokenPipe`,
  `NotConnected`, `TimedOut`, `UnexpectedEof`, `NetworkDown`,
  `NetworkUnreachable`, `HostUnreachable`, plus the Winsock codes `std` leaves
  uncategorized (`WSAENETRESET`, `WSAESHUTDOWN`) and, on Windows,
  `ERROR_NETNAME_DELETED` / `ERROR_SEM_TIMEOUT`. A message-based fallback is
  kept for errors still built as `KafkaError::Protocol(String)`. Fixes
  [#146](https://github.com/osodevops/kafka-backup/issues/146).
- Connection I/O errors are now attributed as `broker_connection` in
  `kafka_backup_errors_total` instead of `unknown`.
- `PartitionLeaderRouter::fetch` (the backup path) now retries connection
  errors up to 5 times with linear back-off, dropping cached broker
  connections in between — the same policy `produce` and `delete_records`
  already had. `KafkaClient::send_request` still reconnects and retries once
  immediately; the router loop covers a proxy or broker resetting connections
  for longer than that single retry, which previously failed the partition and
  the whole run.

### Added
- `KafkaError::ConnectionIo { operation, kind, raw_os_error, message }`
  (`KafkaError` is `#[non_exhaustive]`, so this is additive) and the public
  `kafka::is_connection_error` / `kafka::is_connection_io_kind` helpers.
  `kafka::is_connection_error_public` remains as an alias.

### Changed
- Socket I/O failures on a broker connection now render as
  `Connection error during <operation> (<ErrorKind>): <os message>` instead of
  `Protocol error: Failed to <operation>: <os message>`. Update any log-based
  alerting that matched the old prefix.

## [0.17.0] - 2026-08-18

### Fixed
- Recover from `OFFSET_OUT_OF_RANGE` (broker error code 1) during backup
  instead of failing the partition. In snapshot mode the `earliest` offset is
  captured for every partition up front, and on resume the start offset comes
  from the checkpoint; if retention deletes that offset before the partition is
  fetched, the fetch loop now re-reads the broker's log start offset and
  resumes from there. Previously the whole run failed — and a resumed backup
  whose checkpoint had aged out of retention failed identically on every retry
  and never self-healed. Fixes
  [#144](https://github.com/osodevops/kafka-backup/issues/144).

### Added
- The range skipped by an `OFFSET_OUT_OF_RANGE` recovery is recorded durably as
  an offset gap in the manifest (`topics[].partitions[].gaps[]`, with
  `start_offset`, `end_offset`, `reason`, `detected_at`) rather than only being
  logged: those records are gone from the source, and a backup that silently
  contains a hole is worse than one that fails loudly. `validate` and
  `describe` list recorded gaps; `describe --format json` includes them
  verbatim.
- Prometheus counters `kafka_backup_offset_gaps_total` (gap events) and
  `kafka_backup_offsets_skipped_total` (source offsets skipped), labelled by
  `backup_id`, so operators can alert on data loss.
- `BackupManifest::total_gaps()` / `gaps()`, `PartitionBackup::add_gap()`,
  and the `OffsetGap` / `OffsetGapReason` types in `kafka-backup-core`.

### Changed
- `kafka-backup-core`: `PartitionBackup` gained the public `gaps` field
  (breaking for struct-literal construction). Manifests written by earlier
  versions load unchanged — the field defaults to empty and is omitted from
  the JSON when empty.
- An `OFFSET_OUT_OF_RANGE` that cannot be recovered by skipping forward (the
  broker's log start offset has not moved past the fetch offset, so the offset
  is beyond the log end — truncation or a recreated topic) still fails the
  partition, now with an error that reports the broker's log range and the
  likely cause instead of a bare `code 1`.

## [0.16.0] - 2026-08-11

### Added
- `backup.fetch_max_bytes` — maximum bytes requested per Kafka Fetch request,
  decoupled from the segment size (previously always
  `min(segment_max_bytes, 16MB)`).
- `backup.segment_max_records` — rotate segments once they hold this many
  records, in addition to the existing size/interval thresholds.
- Warn on unknown config keys at load time instead of silently ignoring them.
  Requested in
  [strimzi-backup-operator#53](https://github.com/osodevops/strimzi-backup-operator/issues/53).

### Changed
- `kafka-backup-core`: `BackupOptions` and `SegmentWriterConfig` gained new
  public fields (breaking for struct-literal construction; use
  `..Default::default()`).

## [0.15.13] - 2026-08-11

### Fixed
- Advance fetches past record batches whose tail records were removed by log
  compaction instead of re-requesting the same batch forever (stalled backup,
  duplicate segments written to storage), and continue past fully-compacted
  batches instead of ending the partition backup early. Fixes
  [strimzi-backup-operator#54](https://github.com/osodevops/strimzi-backup-operator/issues/54).

## [0.15.12] - 2026-07-21

### Fixed
- Label segment storage write metrics (`kafka_backup_storage_write_bytes_total`,
  `kafka_backup_storage_write_latency_seconds`) with the storage backend that
  was actually written to (`s3`, `azure`, `gcs`, `filesystem`, `memory`)
  instead of a hardcoded `filesystem`. Addresses
  [strimzi-backup-operator#50](https://github.com/osodevops/strimzi-backup-operator/issues/50).
- Register counters without an explicit `_total` suffix so exposed series match
  the documented names (e.g. `kafka_backup_records_total`) instead of carrying
  a doubled suffix (`kafka_backup_records_total_total`); `prometheus-client`
  appends `_total` to counters at encode time. Dashboards or alerts built on
  the doubled names must move to the documented single-`_total` names.

## [0.15.11] - 2026-07-19

### Added
- Add low-cardinality `kafka_backup_snapshot_records_target` and
  `kafka_backup_snapshot_records_remaining` gauges for snapshot progress.
- Add a low-cardinality `kafka_backup_lag_records_sum` gauge for continuous
  backup lag, including partitions hidden by the detailed-series limit.
- Treat `metrics.max_partition_labels: 0` as an explicit unlimited mode.

### Fixed
- Count unique topic/partition label sets toward `max_partition_labels` instead
  of counting every update, which exhausted the default budget while fewer than
  100 partition series were visible. Addresses
  [strimzi-backup-operator#45](https://github.com/osodevops/strimzi-backup-operator/issues/45).

## [0.15.9] - 2026-07-07

### Added
- Add `metrics.keep_alive_seconds` for one-shot backup and restore jobs, keeping
  the Prometheus metrics server alive after operation completion so short-lived
  Kubernetes CronJobs can be scraped before the pod exits.

### Fixed
- Preserve immediate shutdown on SIGINT/SIGTERM during the post-completion
  metrics keep-alive window, so Kubernetes termination signals are not delayed.

## [0.15.7] - 2026-05-19

### Fixed
- Retry topic creation against the active controller when Kafka returns
  `NOT_CONTROLLER`, fixing restore topic creation through bootstrap endpoints
  that are not the controller.

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
