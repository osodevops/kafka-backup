# Schema Registry backup and restore

Kafka Backup snapshots a Confluent Schema Registry next to the topic data
and restores it before the records, so that every restored record still
resolves to its schema under its **original schema ID**.

Available from v0.30.0. It works with self-managed Confluent Schema Registry
and with Confluent Cloud. For Apicurio Registry, see
[Apicurio Registry backup and restore](apicurio-registry.md).

| | |
|---|---|
| Commands | `backup`, `restore`, `three-phase-restore`, `validate-restore`; each of `backup`, `restore` and `validate-restore` takes `--schema-only` |
| Configuration | The top-level `schema_registry` section |
| Authentication | None, HTTP Basic (including a Confluent Cloud API key), mutual TLS, OAuth |
| Schema types | Avro, JSON Schema, Protobuf |
| Stored at | `{backup_id}/schema-registry/` in the backup's storage |

- [1. Why](#1-why)
- [2. Quick start](#2-quick-start)
- [3. What a backup captures](#3-what-a-backup-captures)
- [4. Restore](#4-restore)
- [5. Configuration reference](#5-configuration-reference)
- [6. Authentication](#6-authentication)
- [7. Confluent Cloud](#7-confluent-cloud)
- [8. Tested versions and limits](#8-tested-versions-and-limits)
- [9. Troubleshooting](#9-troubleshooting)
- [10. Migrating from Kafka Backup Enterprise](#10-migrating-from-kafka-backup-enterprise)
- [11. Testing locally](#11-testing-locally)

## 1. Why

A record written with a Confluent serializer starts with a magic byte and a
4-byte schema ID. Kafka Backup restores those bytes exactly. If the target
registry does not hold the same schema under the same ID, consumers fail to
deserialise the record or, worse, decode it with a different schema.

Recreating schemas from source control or Terraform gives them new IDs.
When the cluster and the registry are lost together (a compromised account,
a deleted environment, a region outage), the registry has to come back from
the backup, with its IDs.

## 2. Quick start

Add a `schema_registry` section to the backup configuration. In a backup
configuration `url` is the registry to snapshot.

```yaml
mode: backup
backup_id: prod-2026-11
source:
  bootstrap_servers: ["kafka:9092"]
  topics:
    include: ["orders-*"]
storage:
  backend: s3
  bucket: kafka-backups
  region: eu-west-1
schema_registry:
  url: https://schema-registry.example.com
  auth:
    type: basic
    username: ${SR_USERNAME}
    password: ${SR_PASSWORD}
```

```bash
kafka-backup backup --config backup.yaml                 # topics, then the registry
kafka-backup backup --schema-only --config backup.yaml   # the registry only, no Kafka connection
```

In a restore configuration `url` is the registry to restore into:

```yaml
mode: restore
backup_id: prod-2026-11
target:
  bootstrap_servers: ["kafka-dr:9092"]
  topics:
    include: ["orders-*"]
storage:
  backend: s3
  bucket: kafka-backups
  region: eu-west-1
schema_registry:
  url: https://schema-registry-dr.example.com
  auth:
    type: basic
    username: ${SR_DR_USERNAME}
    password: ${SR_DR_PASSWORD}
```

```bash
kafka-backup validate-restore --config restore.yaml   # the plan; nothing is changed
kafka-backup restore --config restore.yaml            # the registry, then the records
```

## 3. What a backup captures

For every subject in scope, and every version of it:

- the schema and its type (Avro, JSON Schema or Protobuf);
- its original ID and version number;
- its references, with their exact versions;
- `metadata` and `ruleSet`, verbatim.

And also:

- **Soft-deleted subjects and versions** (`include_soft_deleted`, on by
  default). Records in the backup may still carry their IDs.
- **Referenced schemas outside the subject filter** (`include_references`,
  on by default), so every reference resolves on restore.
- **Schema contexts** (`:.ctx:subject`), each with its own ID space.
- **Compatibility and mode**: global, per context and per subject.

Subjects are selected with include (`subjects`) and exclude (`exclude`) glob
patterns. `include_versions: latest` keeps only the newest version of each
subject (plus whatever it references).

### Where it is stored

```
{backup_id}/schema-registry/
├── _status.json                  # {"complete": true|false, "error": ..., "updated_at": ...}
├── _manifest.json                # summary, registration order, and a checksum for every object
├── _global_config.json           # global and per-context compatibility and mode
└── subjects/{subject}/
    ├── _metadata.json            # versions, soft-deleted versions, compatibility, mode
    └── v{n}.json                 # one schema version, as the registry returned it
```

The manifest's `snapshot.objects` map holds the SHA-256 of every other
object. A restore checks all of them before it touches the target.

### When it is taken

- **One-shot backup**: once, after the topic data. It is taken even when the
  data backup failed, so a broker problem does not cost you the registry
  snapshot.
- **Continuous backup** (`backup.continuous: true`): at start, then every
  `schema_registry.backup.snapshot_interval` (default `15m`), then once more
  at shutdown. Each snapshot replaces the previous one.
- **`backup --schema-only`**: once, without connecting to Kafka.

### Incomplete snapshots

`_status.json` says `"complete": false` from the moment a snapshot starts
until its manifest is written. A snapshot that fails part-way (the registry
returns an error for one subject, the network drops) stays marked incomplete,
with the error. **A restore refuses an incomplete snapshot**: it could be
missing a schema that the records need.

What the `backup` command does when the snapshot is incomplete:

| | Exit status |
|---|---|
| `fail_on_partial: false` (default) | Decided by the topic data backup. The failure is logged as an error. |
| `fail_on_partial: true` | Non-zero. |
| `backup --schema-only` | Always non-zero. |

Set `fail_on_partial: true` if your scheduler should alert on a registry
snapshot that cannot be restored.

## 4. Restore

`restore` and `three-phase-restore` restore the registry **before the
records**. Any registry failure stops the command before a record is
produced. `restore --schema-only` restores the registry on its own.

A configuration without a `schema_registry` section (or with
`enabled: false`) restores records exactly as before.

### Strategies

**`preserve_ids`** (default) is for an empty target.

- The target must have no subjects, soft-deleted ones included.
- The registry, and each context in the backup, is put in IMPORT mode.
- Every version is registered with its original ID and version number.
- Compatibility and modes are restored, and the registry leaves IMPORT mode.
- Rerunning it against a target that already holds exactly this backup
  changes nothing.

**`into_existing`** is for a target that already has schemas, such as a
shared Confluent Cloud environment. For each version in the backup:

| The target has | What happens |
|---|---|
| The same ID with an identical schema | Skipped |
| Neither the ID nor the version | Imported with its original ID, under subject-level IMPORT mode |
| The ID or the version, with a different schema | A conflict |

Conflicts are strict by default: the restore fails **before changing
anything** and lists them. An existing version is never overwritten.

`into_existing` also resumes a `preserve_ids` restore that stopped part-way:
it skips what is already registered.

### `rewrite_ids`: resolving conflicts

With `rewrite_ids: true` (`into_existing` only), a conflicting schema is
registered normally, which gives it a new ID. The restore then rewrites the
schema IDs in the restored records through the existing
`restore.rewrite_schema_ids` path:

- the ID map covers every restored ID, so the strict check
  (`restore.schema_id_rewrite_strict`, on by default) still fails on a record
  that carries an ID the backup does not have;
- the rewrite is limited to the restored topics (`target.topics.include`)
  unless you set `restore.schema_id_rewrite_topics` yourself;
- it works on the 1:1 and on the repartitioning restore path.

The map is one per restore, so a rewrite that would be ambiguous is refused:
when a rewritten ID is also used in another schema context, restore the
contexts separately.

### Order of registration

A schema can only be registered once everything it references exists.
Versions are ordered on their references at version level, not subject
level, with ties broken by original ID. This handles histories in which
version 2 of a subject must be registered before version 1 of another, or
before version 1 of the same subject.

### Other options

- **`restore_soft_deleted`** (default `true`): versions that were
  soft-deleted in the source are registered with their original IDs and then
  soft-deleted again. `false` leaves them out.
- **`subject_mapping`**: renames subjects. The renames apply to references
  too.
- **`restore_global_config`**: apply the backup's global and context
  compatibility and mode. Default: on for `preserve_ids`, off for
  `into_existing`, where the shared registry keeps its own settings.
- **`max_schema_versions`**: the target's schema version limit. If the
  restore would exceed it, the restore fails before any change. The Schema
  Registry API does not expose the limit, so it cannot be detected; a limit
  hit during the restore is reported as such.
- **`verify`** (default `true`): after the restore, each restored
  (subject, version) must resolve to the expected ID, an identical schema,
  the same references and the same deleted state, and subject compatibility
  and mode must match the backup. A target that dropped or changed
  `metadata` or `ruleSet` on registration is reported as a warning.

### The mode guard

A restore has to change modes: IMPORT on the registry and its contexts for
`preserve_ids`, IMPORT on single subjects for `into_existing`. Before it
changes a mode it records the previous one, and it puts every one of them
back:

- after success (a subject gets the mode it had in the backup, or its
  previous one);
- after a failure;
- after cancellation: Ctrl-C or SIGTERM stops the restore at the next
  request and puts the modes back before the command exits.

Before putting a mode back, the restore reads the mode the target has now.
If someone else changed it while the restore ran, their change is left alone
and listed in the report (`modes_changed_by_others`).

A process that is killed outright (SIGKILL, an out-of-memory kill, a lost
node) cannot put anything back. See
[troubleshooting](#9-troubleshooting) for how to recover.

### Dry run and validation

```bash
kafka-backup validate-restore --config restore.yaml                 # registry plan + record dry run
kafka-backup validate-restore --schema-only --config restore.yaml   # registry plan only, no Kafka
kafka-backup validate-restore --schema-only --format json --config restore.yaml
```

The plan lists every version as `register`, `skip`, `rewrite`, `map` (an
identical schema the target holds under another ID) or `CONFLICT`.
`validate-restore` changes nothing and writes nothing, and exits non-zero
when the plan has a conflict.

`restore.dry_run: true` or `schema_registry.restore.dry_run: true` makes the
whole `restore` a dry run, for the registry and for the records, and writes
the plan as a report.

### The report

Every restore writes a JSON report to the backup storage, on failure too:

- `{backup_id}/restore/schema-registry-report.json`
- `{backup_id}/restore/schema-registry-report.dry-run.json` for a dry run

It holds the action taken for each version, the ID map, the capacity check,
the verification result, warnings, and whether the modes were put back. Any
failure gives a non-zero exit status.

## 5. Configuration reference

An unknown key anywhere in the section is an error.

```yaml
schema_registry:
  enabled: true                      # false: keep the section, skip the registry
  url: https://schema-registry:8081
  auth:
    type: basic                      # none | basic | mtls | oauth
    username: ${SR_USERNAME}
    password: ${SR_PASSWORD}
  tls:
    ca_cert: /etc/ssl/registry-ca.pem
  connection:
    timeout_ms: 30000
    max_retries: 3
    retry_backoff_ms: 1000
    rate_limit_rps: 25
  backup:
    subjects: ["*"]
    exclude: ["*-test"]
    include_versions: all            # or latest
    include_soft_deleted: true
    include_references: true
    fail_on_partial: false
    snapshot_interval: 15m           # continuous backups only
  restore:
    strategy: preserve_ids           # or into_existing
    rewrite_ids: false               # into_existing only
    restore_soft_deleted: true
    restore_global_config: true      # default: true for preserve_ids, false for into_existing
    verify: true
    max_schema_versions: 20000       # optional
    subject_mapping:
      orders-value: orders-v2-value
    dry_run: false
```

| Option | Default | Description |
|---|---|---|
| `enabled` | `true` | `false` skips the registry without removing the section |
| `url` | required | In a backup configuration, the registry to snapshot; in a restore configuration, the registry to restore into |
| `auth.type` | `none` | `none`, `basic`, `mtls` or `oauth`; see [Authentication](#6-authentication) |
| `tls.ca_cert` | system roots | PEM CA certificate that signs the registry's certificate |
| `connection.timeout_ms` | `30000` | Request timeout |
| `connection.max_retries` | `3` | Retries of a read after a 429, a 5xx or a connection error |
| `connection.retry_backoff_ms` | `1000` | First backoff; doubles per attempt, with jitter |
| `connection.rate_limit_rps` | `25` | Maximum requests per second |
| `backup.subjects` | `["*"]` | Subjects to capture (glob patterns) |
| `backup.exclude` | `[]` | Subjects to leave out (glob patterns) |
| `backup.include_versions` | `all` | `all` or `latest` |
| `backup.include_soft_deleted` | `true` | Capture soft-deleted subjects and versions |
| `backup.include_references` | `true` | Capture referenced versions outside the filter |
| `backup.fail_on_partial` | `false` | Fail the backup when the snapshot is incomplete |
| `backup.snapshot_interval` | `15m` | How often a continuous backup takes a new snapshot |
| `restore.strategy` | `preserve_ids` | `preserve_ids` or `into_existing` |
| `restore.rewrite_ids` | `false` | Register conflicting schemas under new IDs and rewrite the records (`into_existing` only) |
| `restore.restore_soft_deleted` | `true` | Restore soft-deleted versions and soft-delete them again |
| `restore.restore_global_config` | by strategy | Apply the backup's global and context settings |
| `restore.verify` | `true` | Check the target against the backup after the restore |
| `restore.max_schema_versions` | none | The target's version limit, checked before any change |
| `restore.subject_mapping` | `{}` | Source subject to target subject |
| `restore.dry_run` | `false` | Plan only |

**Retries.** Only reads are retried. A write that fails with a timeout or a
5xx may still have been applied, so it is sent once; the restore's
`into_existing` strategy is how an uncertain write is reconciled.

## 6. Authentication

```yaml
auth: { type: none }

auth:
  type: basic
  username: ${SR_USERNAME}
  password: ${SR_PASSWORD}

auth:
  type: mtls
  client_cert: /etc/ssl/client.pem
  client_key: /etc/ssl/client.key
  ca_cert: /etc/ssl/registry-ca.pem   # optional; the same as tls.ca_cert

auth:
  type: oauth
  provider: entra                     # a key of oauth.providers
  target_sr_cluster: lsrc-abc123      # Confluent Cloud: sent as the target-sr-cluster header
  identity_pool_id: pool-Ab1          # Confluent Cloud: sent as Confluent-Identity-Pool-Id (optional)
```

With `type: oauth`, the provider is one of the top-level
[`oauth.providers`](oauth.md). The registry client uses **the same token
cache** as every Kafka cluster bound to that provider, so one token request
serves both. A 401 from the registry drops the cached token and the request
is tried once more with a new one.

Passwords, API secrets and tokens never appear in logs, in `Debug` output or
in a serialised configuration.

## 7. Confluent Cloud

```yaml
schema_registry:
  url: https://psrc-xxxxx.westeurope.azure.confluent.cloud
  auth:
    type: basic
    username: ${SR_API_KEY}
    password: ${SR_API_SECRET}
  restore:
    strategy: into_existing          # a Confluent Cloud environment has one registry
    max_schema_versions: 20000       # your Stream Governance package's limit
```

- Authenticate with a Schema Registry **API key** (`type: basic`) or with
  **OAuth** (`type: oauth` with `target_sr_cluster`, and `identity_pool_id`
  unless your environment maps pools automatically).
- Roles:
  - backup: `DeveloperRead` on the subjects in scope and on `__GLOBAL`;
  - restore: `ResourceOwner` on the subjects in scope. Subject-level IMPORT
    mode needs it.
- A Stream Governance package limits the number of schema versions, and
  **soft-deleted versions count**. Set `max_schema_versions` so a restore
  that would exceed the limit fails before it changes anything. Without it,
  a restore into a `confluent.cloud` URL logs a warning that says how many
  versions it adds.
- An environment has a single registry. To restore next to existing schemas
  use `into_existing`, and run `validate-restore --schema-only` first.

> **Verified against Confluent Cloud with API keys** (November 2026, Stream
> Governance Essentials, version 0.30.1): backup of an environment's registry
> and topics; restore of both into an empty environment, read back with the
> official deserializers; `into_existing` into a populated environment (dry
> run, strict refusal, `rewrite_ids`); and the `max_schema_versions` check.
> The backup ran with `DeveloperRead` and the restore with `ResourceOwner` on
> the registry's subjects, nothing more. The script is
> `tests/schema-registry-test-infra/confluent-cloud.sh`. **OAuth to a
> Confluent Cloud registry is not verified yet**; it is tested against a mock
> registry. Reports are welcome on
> [issue #262](https://github.com/osodevops/kafka-backup/issues/262).

## 8. Tested versions and limits

Each release is tested against real Confluent Schema Registry 6.2.0, 7.6.0,
7.9.2, 8.1.1 and 8.3.1 (`.github/workflows/schema-registry-e2e.yml`):

- the registry suite: `preserve_ids`, `into_existing`, conflicts,
  `rewrite_ids`, interruption and resume;
- an end-to-end test in which records produced with the official Confluent
  serializers (Avro including a top-level `bytes` schema, JSON Schema,
  Protobuf with nested messages; tombstones, null keys, headers; lz4, zstd
  and gzip topics) are backed up, restored into a second cluster with an
  empty registry, and read back with the official deserializers pointed only
  at the target registry.

| Version | `preserve_ids` | `into_existing` | Tested with |
|---|---|---|---|
| 8.1.1, 8.3.1 | Yes | Yes | Contexts, `metadata`, and `ruleSet` (the registries run the rule-set resource extension) |
| 7.6.0, 7.9.2 | Yes | Yes | Contexts and `metadata` |
| 6.2.0 | Yes, into an empty registry | **Refused** | No contexts, no `metadata`; see below |

A registry stores `ruleSet` only when it runs the rule-set resource
extension. One that does not drops it on registration; the restore then
reports a warning for each version whose `ruleSet` the target dropped.

**Confluent Platform 6.2 cannot delete mode overrides.** A restore that
needs a temporary IMPORT mode on a subject with an inherited mode could not
remove it afterwards, so it is refused before any change, with a message
that says so. That covers every `into_existing` restore. A `preserve_ids`
restore into an empty 6.2 registry needs only the global IMPORT mode and
works. 6.2 has no schema contexts.

Other limits:

- **Apicurio Registry**, and restoring between Apicurio and Confluent Schema
  Registry, are covered by a separate engine and guide:
  [Apicurio Registry backup and restore](apicurio-registry.md).
- **Data contract rules are not executed** on restored records (for example
  CSFLE encryption), and the DEK Registry is not backed up. Rule sets are
  backed up verbatim.
- **Access controls on subjects** (ACLs, role bindings) are not part of the
  snapshot.
- The target registry must allow mode changes (`mode.mutability=true` in
  its configuration), because IMPORT is a mode.

## 9. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `strategy preserve_ids needs an empty target registry` | The target has subjects (soft-deleted ones count). Use `strategy: into_existing`, or empty the target. |
| `N version(s) conflict with the target and rewrite_ids is off; nothing was changed` | An original ID or version is held by a different schema. The message lists each one. Resolve them, or set `rewrite_ids: true`. |
| `the snapshot ... is incomplete` | The last snapshot did not finish. Take a new one with `backup --schema-only`. |
| `checksum mismatch` / `missing object` | The snapshot in storage was changed or is damaged. Restore from another backup. |
| `cannot delete mode overrides` | The target is Confluent Platform 6.2 or older. Use `preserve_ids` into an empty registry, or restore into 7.0 or later. |
| `Schema Registry capacity exceeded` | The restore would exceed `max_schema_versions`, or the registry's own limit was hit. Hard-delete unused schemas or raise the limit. Soft-deleted versions count. |
| `Overwrite new schema with id N ... is not permitted` | A registry remembers the ID of a schema it once held, even after a hard delete. If that ID was since imported for a different schema, the registry refuses to register the first schema again. This only happens after hard deletes followed by imports that reuse IDs; restore into a registry that never held the schema, or free the ID. |
| The registry is left in IMPORT mode | The restore process was killed and could not put the modes back. Set the global mode back with `PUT /mode {"mode":"READWRITE"}`, remove subject overrides with `DELETE /mode/{subject}`, then rerun with `strategy: into_existing`. |
| `401` / `403` on restore but not on backup | Restore needs write access: on Confluent Cloud, `ResourceOwner` on the subjects. |
| `auth uses OAuth provider '...', which was not set up` | A program that uses the library built a registry client without calling `kafka_backup_core::oauth::install` first. The CLI does it for you. |

## 10. Migrating from Kafka Backup Enterprise

Schema Registry backup and restore came from Kafka Backup Enterprise and
needs no licence here. Snapshots written by Enterprise use the same storage
layout and are read by this release.

| Enterprise | Open source |
|---|---|
| `enterprise.schema_registry` | Top-level `schema_registry` |
| `enterprise.snapshot_interval` | `schema_registry.backup.snapshot_interval` |
| `enterprise.oauth` providers, and `auth: { type: oauth, provider }` | Top-level `oauth.providers`; `auth` is unchanged |
| Inline OAuth in `auth` (`token_url`, `client_id`, `client_secret`, `scope`) | Removed. Define the client under `oauth.providers` (with `client_auth: post` to keep sending the credentials in the form body) and name it with `provider` |
| `schema_registry.restore.enabled` | Removed. A restore configuration with a `schema_registry` section restores the registry; use `schema_registry.enabled: false` to skip it |
| `restore.strategy: preserve` or `skip` | `into_existing` |
| `connection.concurrent_requests` | Removed (it was not used) |
| `enterprise.registry_restore`, serializer profiles | Top-level `registry_restore`, for Apicurio and for restores between Apicurio and Confluent ([guide](apicurio-registry.md)). A Confluent-to-Confluent restore uses `schema_registry.restore` |
| `mtls` without `client_cert` or `client_key` | Both are required |
| Licence checks, exit status 2 | None |

Two behaviours changed:

- With `fail_on_partial: false`, a failed registry snapshot no longer fails
  the `backup` command by itself (it is logged as an error and the snapshot
  is marked incomplete). Set `fail_on_partial: true` for the previous
  behaviour.
- A restore configuration that has a `schema_registry` section but no
  `restore` subsection now restores the registry, with the defaults.

Not included from Enterprise: the DEK Registry and CSFLE, Confluent RBAC and
authorization recovery, erasure, and the MSK migration.

## 11. Testing locally

```bash
# Mocked registries, no Docker
cargo test -p kafka-backup-core --test schema_registry_tests
cargo test -p kafka-backup-cli --test schema_registry_cli

# Real registries: two Kafka clusters and three registries
SR_VERSION=8.3.1 docker compose -f tests/schema-registry-test-infra/docker-compose.yml up -d --wait
SR_VERSION=8.3.1 cargo test -p kafka-backup-core --test schema_registry_real -- --ignored --test-threads=1
SR_VERSION=8.3.1 tests/schema-registry-test-infra/e2e/run.sh
docker compose -f tests/schema-registry-test-infra/docker-compose.yml down -v

# Every supported version
tests/schema-registry-test-infra/matrix.sh
```
