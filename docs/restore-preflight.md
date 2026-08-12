# Phase 1 header preflight (restore tracking-metadata validation)

Status: implemented (issue #137). Implementation:
`kafka-backup-core/src/restore/preflight.rs`.

## What it proves

Three-phase restore promises consumer-offset recovery driven by tracking
metadata written at backup time:

- per-record `x-original-offset` / `x-original-timestamp` headers (and the
  optional `x-source-cluster` header) written when
  `backup.include_offset_headers: true`;
- the `{backup_id}/consumer-groups-snapshot.json` file used by
  `restore.auto_consumer_groups`.

The preflight scans the selected backup — every selected topic and partition,
every segment the restore would process (time-window and partition filters
applied), every record in those segments, across both the binary and legacy
JSON segment formats — and produces a structured coverage report **before the
restore mutates anything**. It is read-only and does not contact the target
cluster.

## Coverage states (per topic/partition)

| State | Meaning |
|---|---|
| `full` | every scanned record carries decodable offset+timestamp tracking headers (≥1 record) |
| `partial` | some records carry them, some do not |
| `missing` | records exist, none carry them (legacy / non-Phase-1 backup) |
| `empty` | no records for this partition in the selected window — explicitly not a pass |
| `data_missing` | manifest references segment objects absent from storage |
| `corrupt` | a segment exists but fails CRC/format/decompression/decoding |
| `indeterminate` | not scanned (mode) or storage errors — never a pass |

Zero records scanned is never reported as a positive pass: empty and
indeterminate are distinct explicit states, and an offset-recovery request
fails when no partition demonstrates full coverage.

## When it runs and what it blocks

"Consumer-offset recovery requested" means `restore.reset_consumer_offsets`
or `restore.auto_consumer_groups` is enabled.

| Entry point | Behaviour |
|---|---|
| `RestoreEngine::run` (plain `restore`, enterprise restore, seed jobs) | preflight runs after the manifest load and **before** connecting to the target, creating topics, applying configs, purging, or producing. Offset-recovery request + failed preflight ⇒ `Error::Preflight`, target untouched. |
| `ThreePhaseRestore::run_all_phases` | preflight runs first (structured result stored in `ThreePhaseReport.header_preflight`); failure aborts before Phase 2. The engine is told not to re-scan. |
| `ThreePhaseRestore::validate_phase1_headers` | runs the scanner and returns the structured report (replaces the former placeholder that always returned success with `sample_records_checked: 0`). |
| `validate-restore` / `RestoreEngine::dry_run` | runs the same scanner under the same conditions and embeds the result in `DryRunReport.header_preflight`; a preflight that would block the restore marks the dry run invalid (exit code 1). |

Additional required-metadata rules when offset recovery is requested:

- `auto_consumer_groups` with a missing or unparseable consumer-groups
  snapshot fails the preflight (previously this degraded silently mid-restore,
  after topics had been created).
- `reset_consumer_offsets: true` with no `consumer_groups` and no
  `auto_consumer_groups` fails: the reset was requested but could never act
  (previously silently skipped).
- A present snapshot listing zero groups passes with a warning (a source
  cluster may legitimately have no consumer groups).

Backups without tracking headers remain fully restorable when offset recovery
is **not** requested; coverage problems are then warnings.

## Modes (`restore.header_preflight`)

| Mode | Scan | Blocking |
|---|---|---|
| `auto` (default) | only when offset recovery is requested | strict when offset recovery is requested |
| `full` | always | strict when offset recovery is requested; warnings otherwise |
| `skip` | never | never blocks — explicit emergency escape hatch; an offset-recovery request proceeds UNVERIFIED with loud warnings, matching pre-#137 behaviour |

Scan cost: every selected segment is fetched and decoded once (the same I/O
the restore itself performs). Because segment CRC verification and
decompression already require reading whole segments, header inspection adds
only per-record header iteration.

## Failure semantics

Failures are actionable and bounded (up to three example problems per
partition), e.g.:

```
Preflight failed: consumer-offset recovery was requested but the backup failed
the tracking-metadata preflight; no topics were created and no records were
produced. backup 'orders-daily': 3 partition(s) [1 full, 2 missing], 15000
record(s) scanned, verdict: FAIL. Errors: orders/1: backup records carry no
offset/timestamp tracking headers (…). Consumer-offset recovery requires a
Phase 1 backup taken with include_offset_headers: true. | …
```
