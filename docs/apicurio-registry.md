# Apicurio Registry backup and restore

Kafka Backup snapshots an Apicurio Registry next to the topic data and
restores it before the records, so restored records still resolve to their
schemas. It can also restore between Apicurio Registry and Confluent Schema
Registry, rewriting the registry identifiers inside the records.

Available from v0.30.0, for Apicurio Registry 2.x (API v2) and 3.x (API v3).
For Confluent Schema Registry on its own, see
[Schema Registry backup and restore](schema-registry.md).

| | |
|---|---|
| Commands | `backup`, `restore`, `three-phase-restore`, `validate-restore`; each of `backup`, `restore` and `validate-restore` takes `--schema-only` |
| Configuration | `apicurio_registry` (the connection and what a backup captures) and `registry_restore` (how a restore treats the target) |
| Authentication | None, HTTP Basic, mutual TLS, OIDC client credentials (for example Keycloak) |
| Artifact types | Avro, JSON Schema and Protobuf for record restores; any type (OpenAPI, GraphQL, ...) for a registry-only restore |
| Stored at | `{backup_id}/apicurio-registry/` in the backup's storage |

- [1. Quick start](#1-quick-start)
- [2. What a backup captures](#2-what-a-backup-captures)
- [3. Restore](#3-restore)
- [4. Serializer profiles](#4-serializer-profiles)
- [5. Restoring between Confluent and Apicurio](#5-restoring-between-confluent-and-apicurio)
- [6. Configuration reference](#6-configuration-reference)
- [7. Tested versions and limits](#7-tested-versions-and-limits)
- [8. Troubleshooting](#8-troubleshooting)
- [9. Migrating from Kafka Backup Enterprise](#9-migrating-from-kafka-backup-enterprise)
- [10. Testing locally](#10-testing-locally)

## 1. Quick start

In a backup configuration `url` is the registry to snapshot. `api_version`
is never guessed: set `v2` for Apicurio Registry 2.x.

```yaml
mode: backup
backup_id: prod-2026-11
source:
  bootstrap_servers: ["kafka:9092"]
  topics:
    include: ["orders"]
storage:
  backend: s3
  bucket: kafka-backups
  region: eu-west-1
apicurio_registry:
  url: https://apicurio.example.com
  api_version: v3
```

```bash
kafka-backup backup --config backup.yaml                 # topics, then the registry
kafka-backup backup --schema-only --config backup.yaml   # the registry only, no Kafka connection
```

In a restore configuration `url` is the registry to restore into. With no
`registry_restore` section the restore preserves the original identifiers,
which needs an empty target:

```yaml
mode: restore
backup_id: prod-2026-11
target:
  bootstrap_servers: ["kafka-dr:9092"]
  topics:
    include: ["orders"]
storage:
  backend: s3
  bucket: kafka-backups
  region: eu-west-1
apicurio_registry:
  url: https://apicurio-dr.example.com
  api_version: v3
```

```bash
kafka-backup validate-restore --config restore.yaml   # the plan; nothing is changed
kafka-backup restore --config restore.yaml            # the registry, then the records
```

## 2. What a backup captures

For every group and artifact in scope:

- every version, with its content, artifact type, state, original global ID
  and content ID, and a SHA-256 of the content;
- references, with their exact versions; referenced artifacts outside the
  filter are captured too (`include_references`, on by default);
- names, descriptions and labels (or properties, for API v2);
- rules at the global, group and artifact level;
- the registry's own export archive (`include_export`, on by default). An
  ID-preserving restore imports this archive.

Groups and artifacts are selected with glob patterns (`groups`,
`exclude_groups`, `artifacts`, `exclude_artifacts`). The export archive always
covers the whole registry; the manifest records that separately, and a
whole-registry archive is never used for a filtered restore.

```
{backup_id}/apicurio-registry/
├── _status.json                  # {"complete": true|false}
├── _manifest.json                # summary, order, and a checksum for every object
├── _global_rules.json
├── _export.zip                   # the registry's export archive (include_export)
└── groups/{group}/
    ├── _metadata.json
    └── artifacts/{artifact}/
        ├── _metadata.json        # rules, labels, versions
        └── versions/{version}.json | {version}.content
```

The snapshot is taken when the Confluent one is: once after a one-shot
backup (also when the topic data backup failed), at start, every
`apicurio_registry.backup.snapshot_interval` (default 15 minutes) and at
shutdown of a continuous backup, and by `backup --schema-only`.

A snapshot that could not be completed stays marked incomplete and a restore
refuses it. `apicurio_registry.backup.fail_on_partial: true` also fails the
`backup` command; `backup --schema-only` always fails.

## 3. Restore

`restore` and `three-phase-restore` restore the registry **before the
records**. A registry conflict, a missing object or reference, an identifier
in the records that the snapshot does not have, or a failed verification
stops the command before a record is produced. `restore --schema-only`
restores the registry on its own.

### Preserving identifiers (the default)

`id_policy: preserve` imports the registry's own export archive, so every
version keeps its global ID and content ID and records resolve unchanged. It
requires:

- a complete, unfiltered snapshot with its export archive;
- an empty target;
- the same API generation on both sides (v2 to v2, or v3 to v3).

After the import, every coordinate, ID, content, reference and state is
checked through the target's API. An import whose outcome is uncertain (a
timeout after the request was sent) is reconciled in full and never blindly
repeated.

### Allocating new identifiers

`id_policy: rewrite` registers each version through the normal API, so the
target allocates new IDs. Use it for a target that already has artifacts,
for a filtered restore, or between API generations. The identifiers inside
the restored records are then rewritten, which needs a
[serializer profile](#4-serializer-profiles) for every restored topic.

- An artifact version the target already holds is reused only after its
  content, references and metadata were checked. Nothing is overwritten.
- `coordinates` moves artifacts to another group or name.
- The target's global rules are kept. `restore_global_settings: true` applies
  the snapshot's instead, and needs an empty target.

```yaml
apicurio_registry:
  url: https://apicurio-dr.example.com
  api_version: v3
registry_restore:
  id_policy: rewrite
  serializers:
    orders:
      key:
        source: { type: raw }
        target: { type: raw }
      value:
        source: { type: apicurio, namespace: default, id_kind: global, width: 8, header: apicurio.registry.globalId }
        target: { type: apicurio, namespace: default, id_kind: global, width: 8, header: apicurio.registry.globalId }
```

### Checks around the records

Records are scanned twice. Before the registry is changed, every identifier
in the selected records must be in the snapshot and every configured frame
must parse. After the registry restore, the scan runs again with the IDs the
target actually assigned: an ID that does not fit the target's width, an
unavailable identifier or a malformed frame stops the restore before a
record is produced.

Archived records are never edited. The rewrite happens while restoring,
after point-in-time and record filters and before partitioning, on the 1:1
and the repartitioning path.

### Progress, cancellation and the report

Registry progress is journalled in a local file: next to
`restore.checkpoint_state` when that is set, otherwise under
`.kafka-backup/registry-restore/` in the working directory. The journal ties
the operation to the snapshot, the configuration and the target, and records
registrations whose outcome is uncertain. **Keep it to resume an interrupted
restore**: rerunning the same command reconciles what was already written
and registers nothing twice. Ctrl-C and SIGTERM stop the restore cleanly.

Each run prints a JSON report (created and reused entries, the complete ID
and version mappings, verification, failures) and stores it beside the
journal.

### Dry run

```bash
kafka-backup validate-restore --config restore.yaml
kafka-backup validate-restore --schema-only --format json --config restore.yaml
```

The plan lists every version with its action, the framing conversions, the
settings that would change, the limits that apply and every conflict, under
`registry_restore` in JSON and YAML. A conflict fails the validation. A
`dry_run: true` in `restore`, `registry_restore` or `schema_registry.restore`
makes the whole `restore` a dry run.

## 4. Serializer profiles

A serializer puts the schema's identifier in a prefix of the record or in a
header. Nothing is inferred from a magic byte: a restore that rewrites
identifiers needs to be told how each topic's keys and values are framed.

Profiles use exact source topic names. Keys and values are independent.
Every restored topic needs a profile, including topics that are explicitly
`raw`.

| `type` | Identifier | Framing |
|---|---|---|
| `raw` | None | Unchanged bytes |
| `apicurio` | `global` or `content` ID, per group (`namespace`) | Magic byte `0` and a 4- or 8-byte ID (`width`); with `header`, an 8-byte ID in that header instead |
| `apicurio_compatible` | Apicurio `global` or `content` ID | Confluent framing carrying an Apicurio identifier (the registry's Confluent-compatible API) |
| `confluent` | Numeric ID, per context (`namespace`) | Magic byte `0` and a 4-byte ID |
| `confluent_guid_header` | GUID, per context | A header with the GUID (and the Protobuf message indexes); needs Confluent Platform 8.1 or later |

The Apicurio 2.x and 3.x serializers have different defaults. Set the header
or prefix mode, the ID kind and the width that your serializers actually
use. A 4-byte target must fit a positive signed 32-bit integer.

**Protobuf.** Confluent and compatible profiles frame the message with
`indexes`. Native Apicurio profiles use `type_reference`, or with API v2
`bare` together with the header that names the message type. Converting
between framings needs the fully qualified `message` (for example
`acme.Event`). Imports are compiled at their exact referenced versions.

```yaml
value:
  source:
    type: apicurio
    namespace: default
    id_kind: global
    width: 8
    header: apicurio.registry.globalId
  target: { type: confluent, namespace: "" }
  protobuf_source: bare
  protobuf_source_type_header: apicurio.protobuf.type
  protobuf_target: indexes
  message: acme.Event
```

## 5. Restoring between Confluent and Apicurio

Set the snapshot's kind and the target's kind. The target's connection is the
matching section (`schema_registry` or `apicurio_registry`). Identifiers are
always allocated by the target (`id_policy: rewrite`), every restored subject
or artifact needs a `coordinates` entry, and every restored topic needs a
serializer profile.

```yaml
apicurio_registry:
  url: https://apicurio.example.com
  api_version: v3
registry_restore:
  source: confluent
  destination: apicurio
  id_policy: rewrite
  coordinates:
    - source_group: ""
      source_name: orders-value
      target_group: orders
      target_name: orders-value
  serializers:
    orders:
      key:
        source: { type: raw }
        target: { type: raw }
      value:
        source: { type: confluent, namespace: "" }
        target: { type: apicurio, namespace: orders, id_kind: global, width: 4 }
```

Avro binary, JSON Schema and Protobuf are converted when their meaning and
framing match on both sides. Rules, states and metadata that the other
registry cannot represent fail the validation; nothing is dropped silently.

A Confluent-to-Confluent restore does not use `registry_restore`: it uses
[`schema_registry.restore`](schema-registry.md#4-restore). A configuration
with both a `schema_registry` and an `apicurio_registry` section and no
`destination` restores each registry from its own snapshot.

## 6. Configuration reference

An unknown key anywhere in either section is an error.

```yaml
apicurio_registry:
  enabled: true                      # false: keep the section, skip the registry
  url: https://apicurio:8080
  api_version: v3                    # or v2
  auth:
    type: basic                      # none | basic | mtls | oidc
    username: ${APICURIO_USERNAME}
    password: ${APICURIO_PASSWORD}
  tls:
    ca_cert: /etc/ssl/registry-ca.pem
  connection:
    timeout_ms: 30000
    max_retries: 3
    retry_backoff_ms: 1000
    rate_limit_rps: 50
  backup:
    groups: ["*"]
    exclude_groups: []
    artifacts: ["*"]
    exclude_artifacts: []
    include_versions: all            # or latest
    include_references: true
    include_export: true
    fail_on_partial: false
    snapshot_interval: 15m           # continuous backups only

registry_restore:
  source: apicurio                   # the snapshot's kind; default: the destination's
  destination: apicurio              # default: the enabled section
  id_policy: preserve                # or rewrite
  restore_global_settings: false
  coordinates: []                    # source_group, source_name, target_group, target_name
  serializers: {}                    # per source topic: key and value profiles
  dry_run: false
```

| Option | Default | Description |
|---|---|---|
| `apicurio_registry.url` | required | Registry base URL; `/apis/registry/v2` or `/v3` is appended |
| `apicurio_registry.api_version` | `v3` | `v3`, or `v2` for Apicurio Registry 2.x |
| `apicurio_registry.auth.type` | `none` | `none`; `basic` (`username`, `password`); `mtls` (`client_cert`, `client_key`, optional `ca_cert`); `oidc` (`token_url`, `client_id`, `client_secret`, optional `scope`) |
| `apicurio_registry.tls.ca_cert` | system roots | PEM CA certificate for the registry's certificate |
| `apicurio_registry.connection.*` | see above | Timeout, retries of reads, backoff, requests per second |
| `apicurio_registry.backup.groups` / `exclude_groups` | `["*"]` / `[]` | Groups to capture (glob patterns) |
| `apicurio_registry.backup.artifacts` / `exclude_artifacts` | `["*"]` / `[]` | Artifact IDs to capture within those groups |
| `apicurio_registry.backup.include_versions` | `all` | `all` or `latest` |
| `apicurio_registry.backup.include_references` | `true` | Capture referenced artifacts outside the filter |
| `apicurio_registry.backup.include_export` | `true` | Store the registry's export archive; an ID-preserving restore needs it |
| `apicurio_registry.backup.fail_on_partial` | `false` | Fail the backup when the snapshot is incomplete |
| `apicurio_registry.backup.snapshot_interval` | `15m` | How often a continuous backup takes a new snapshot |
| `registry_restore.source` | the destination | `apicurio` or `confluent`: which snapshot to restore |
| `registry_restore.destination` | the enabled section | `apicurio` or `confluent`: which registry to restore into |
| `registry_restore.id_policy` | `preserve` | `preserve` (archive import, empty target) or `rewrite` (the target allocates IDs; records are rewritten) |
| `registry_restore.restore_global_settings` | `false` | Apply the snapshot's global rules; needs an empty target |
| `registry_restore.coordinates` | `[]` | Move subjects or artifacts; required for every one in a cross-registry restore |
| `registry_restore.serializers` | `{}` | Key and value [profiles](#4-serializer-profiles) per source topic |
| `registry_restore.dry_run` | `false` | Plan only |

Only reads are retried. A write is sent once, and an uncertain one is
reconciled from the journal. Passwords, client secrets and tokens never
appear in logs or serialised configuration.

## 7. Tested versions and limits

The feature is tested against real Apicurio Registry 2.6.13.Final (API v2)
and 3.0.15, 3.1.7 and 3.3.3 (API v3), together with Confluent Schema
Registry, by `tests/registry-restore-test-infra/run.sh` and the
`Registry restore E2E` workflow:

- ID-preserving restore of each registry, compared version by version;
- records produced with the released Apicurio and Confluent Java
  serializers (Avro including a top-level `bytes` schema, JSON Schema,
  Protobuf; tombstones, raw and serialized keys, duplicate and null headers;
  gzip and Snappy topics), restored with rewritten identifiers and read back
  with the released deserializers pointed only at the target registry:
  both directions between Confluent and Apicurio (4-byte and 8-byte prefix,
  ID headers, content IDs and Confluent-compatible framing), and Apicurio to
  Apicurio with newly allocated IDs, on the 1:1 and the repartitioning path.

Limits:

- **Preserving IDs needs the same API generation.** An archive from API v2
  cannot be imported into API v3; that restore is refused, and `id_policy:
  rewrite` allocates IDs instead.
- **Nested Protobuf messages with native Apicurio framing** are refused
  before any change: the tested Apicurio serializers cannot resolve them.
  Use Confluent or Confluent-compatible framing, or restore the registry
  only.
- **`DISABLED`, `SUNSET` and `DRAFT` versions** cannot back records that use
  global-ID framing; such records are refused before the restore. A
  registry-only restore keeps the states.
- **A new Confluent registration with a `ruleSet`** is refused through
  `registry_restore`, and records that use a schema with rules are not
  replayed: data contract rules are not executed.
- **Confluent Cloud** is not supported as a source or target of
  `registry_restore`. Use `schema_registry.restore` for Confluent-only
  recovery.
- **Confluent Platform 6.2** as a target of `registry_restore` is refused
  when a subject's mode is inherited, because 6.2 cannot delete a mode
  override.
- A source ID shared by several Apicurio artifacts is ambiguous for
  global-ID framing; use content-ID framing or narrow the restore.
- Avro JSON encoding and custom serializers are not converted.

## 8. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `Apicurio ID preservation requires ...` (empty target, complete archive, matching generation) | `id_policy: preserve` cannot be met. Empty the target and restore an unfiltered snapshot of the same API generation, or use `id_policy: rewrite` with serializer profiles. |
| `Record identifier preflight: ...` | A record carries an identifier the snapshot does not have, or a topic has no serializer profile. Add the profile (use `raw` for topics without a registry serializer) or widen the registry backup. |
| `Registry snapshot is incomplete` | The last snapshot did not finish. Take a new one with `backup --schema-only`. |
| `Snapshot hash mismatch` | The snapshot in storage was changed or is damaged. Restore from another backup. |
| `Registry checkpoint snapshot, target identity or configuration changed` | The journal belongs to a different restore. Finish that one, or remove the journal if it is no longer needed. |
| `Another registry restore is using this checkpoint` | Two restores share a journal. Run one at a time. |
| A 404 from every request | `api_version` does not match the registry. Apicurio Registry 2.x needs `api_version: v2`. |
| `Artifact deletion operation is not enabled` | Only seen in tests that delete fixtures; a restore never deletes. |

## 9. Migrating from Kafka Backup Enterprise

Apicurio Registry backup and restore came from Kafka Backup Enterprise and
needs no licence here. Snapshots written by Enterprise use the same storage
layout and are read by this release.

| Enterprise | Open source |
|---|---|
| `enterprise.apicurio_registry` | Top-level `apicurio_registry` |
| `enterprise.registry_restore` | Top-level `registry_restore` |
| `apicurio_registry.restore` (`enabled`, `use_import_api`, `strategy`, `rewrite_ids`, `dry_run`) | Removed. Use `registry_restore.id_policy` and `registry_restore.dry_run`; `apicurio_registry.enabled: false` skips the registry |
| `apicurio_registry.storage_backend`, `connection.concurrent_requests` | Removed (they were not used) |
| `registry_restore` with a Confluent source and a Confluent destination | Not routed through this engine; use `schema_registry.restore` |
| Licence checks, exit status 2 | None |

## 10. Testing locally

```bash
# Mocked registries, no Docker
cargo test -p kafka-backup-core --test schema_registry_tests

# Backup against real Apicurio 3.x
C=tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml
docker compose -f $C up -d
tests/registry-restore-test-infra/seed-apicurio.sh http://localhost:29485
cargo test -p kafka-backup-core --test apicurio_registry_real -- --ignored --test-threads=1
docker compose -f $C down -v

# Restore against real registries, with the released Java serializers
# (needs Docker, Java 17+ and Maven)
tests/registry-restore-test-infra/run.sh
REGISTRY_APICURIO_SOURCE_VERSION=2.6.13.Final REGISTRY_APICURIO_TARGET_VERSION=2.6.13.Final \
  tests/registry-restore-test-infra/run.sh
```
