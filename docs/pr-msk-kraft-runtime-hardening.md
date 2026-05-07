# PR: Runtime hardening for MSK KRaft migration support

## Title

`fix(kafka): harden restore and coordinator routing for MSK migrations`

## Summary

This PR hardens the OSS Kafka client and restore path for the enterprise MSK ZooKeeper to KRaft migration workflow. The enterprise AWS E2E migration completed only after these fixes were present locally.

The key production issue was a restore failure on MSK target clusters when the Rust client produced large uncompressed record batches around 128 KiB with `acks=all`. Java Kafka tooling could produce the same record, proving broker/network/auth were healthy. Switching restored produce batches to zstd compression fixed the Rust client path and allowed the full migration to complete.

This PR does **not** add the enterprise MSK IAM/OAUTHBEARER plugin, and it does **not** add new GSSAPI/Kerberos implementation scope. Current `main` already contains the generic SASL plugin extension point and an optional GSSAPI feature from earlier PRs. This PR only adds the remaining OSS runtime behavior required by the production MSK migration.

If the project decides that the optional GSSAPI implementation should not remain in OSS, that should be handled as a separate cleanup PR that preserves the generic `SaslMechanismPlugin`/factory contract used by enterprise.

## What Changed

### Restore Produce Path

- Increased client response timeout from 10s to 60s.
- Updated timeout documentation so the client timeout stays above the default 30s broker-side produce timeout.
- Changed production record-batch encoding from no compression to zstd.
- Added an ignored external diagnostic test for large MSK produce cases using:
  - `KAFKA_BOOTSTRAP`
  - `KAFKA_SASL_USERNAME`
  - `KAFKA_SASL_PASSWORD`
  - `KAFKA_TEST_TOPIC`
  - `KAFKA_TEST_ACKS`
  - `KAFKA_TEST_TIMEOUT_MS`
  - `KAFKA_TEST_VALUE_BYTES`

### Restore Correctness

- Restore runs now fail if the report contains partition/topic errors instead of returning a successful `RestoreReport` with embedded errors.
- Offset mapping range updates now preserve target offsets when the restore pre-produce pass records source offsets with `None` target offsets.
- Added regression tests for:
  - `None` then `Some` offset-map updates
  - preserving existing target offsets
  - shifted source/target offset ranges
  - restore reports with errors failing the restore

### Consumer Group Coordinator Routing

- Added `FindCoordinator` support.
- Added `GroupCoordinator` parsing for modern response shapes.
- Added coordinator-routed offset commits through `PartitionLeaderRouter::commit_group_offsets`.
- Added retries for transient coordinator errors:
  - `COORDINATOR_LOAD_IN_PROGRESS`
  - `COORDINATOR_NOT_AVAILABLE`
  - `NOT_COORDINATOR`
- Added tests for coordinator response selection and transient commit retry classification.

### Kafka Admin APIs

- Added `DescribeConfigs`.
- Added `IncrementalAlterConfigs`.
- Added resource/config types for topic and broker config handling.
- Exported the new admin APIs through `kafka::mod`.
- Added API version mappings for:
  - `DescribeConfigs`
  - `IncrementalAlterConfigs`
  - `DescribeAcls`
  - `CreateAcls`

## Bugs Fixed

- Fixed MSK restore stalls/timeouts on large restored records by using zstd record batches.
- Fixed client-side timeout being shorter than the default broker-side produce timeout.
- Fixed restore success being reported even when restore errors were present.
- Fixed offset-map target offsets being lost when source ranges were first recorded without target offsets.
- Fixed group offset commits being sent to non-coordinator brokers.
- Added missing Kafka admin APIs used by enterprise topology/config migration.

## Release Metadata

- Bumped the workspace package version from `0.15.3` to `0.15.4`.
- Updated `Cargo.lock` package entries for `kafka-backup-core` and `kafka-backup-cli` to `0.15.4`.

## Relationship To Enterprise MSK KRaft PR

The enterprise migration PR depends on these OSS fixes. Without them:

- target restore can fail on large records;
- restore can hide partition-level failures in a nominally successful report;
- offset translation can be incomplete when offset-map target bounds remain `None`;
- consumer group offset commits can fail or hit the wrong broker in multi-broker clusters;
- enterprise topology/config diff and apply paths lack OSS admin API support.

The generic SASL plugin extension point is a prerequisite for enterprise MSK IAM/OAUTHBEARER, but that prerequisite is already on `main`. The enterprise-specific MSK IAM implementation remains in the enterprise repo.

## Verification

Local OSS checks:

```sh
cargo fmt --all --check
cargo test -p kafka-backup-core -p kafka-backup-cli -- --nocapture
```

Focused checks:

```sh
cargo test -p kafka-backup-core issue_67_fixes -- --nocapture
```

External MSK diagnostic used during AWS E2E:

```sh
KAFKA_BOOTSTRAP=<target-scram-bootstrap> \
KAFKA_SASL_USERNAME=migrator \
KAFKA_SASL_PASSWORD=<redacted> \
KAFKA_TEST_TOPIC=e2e.large \
KAFKA_TEST_ACKS=-1 \
KAFKA_TEST_VALUE_BYTES=131072 \
cargo test -p kafka-backup-core --test produce_debug \
  test_large_produce_via_client_method -- --ignored --nocapture
```

Enterprise AWS E2E proof:

- Migration ID: `aws-e2e-r4-20260507T200820Z`
- Source: MSK ZooKeeper provisioned cluster
- Target: MSK KRaft provisioned cluster
- Seed restored `119281` records.
- Tail replayed `1410` live records.
- Cutover translated `37` offsets across `3` consumer groups.
- Final validation found no count/offset mismatches and all comparable spot checks matched.
- Post-switch target produce/read succeeded.

## Out Of Scope

- No AWS resource creation or deletion.
- No enterprise migration code.
- No enterprise MSK IAM/OAUTHBEARER token provider.
- No new GSSAPI/Kerberos feature work.
- No change to topic deletion or cleanup behavior.
- No default credential changes.
