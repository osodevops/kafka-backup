# Fork Fixes — himurafred/kafka-backup

This document tracks fixes applied on this fork that have not yet been merged into `osodevops/kafka-backup` upstream.
Each fix includes the root cause, the affected files, and the rationale for contributing it back.

---

## Fix 1 — `auto_consumer_groups`: Phase 3 offset reset silently skipped

**Status:** Pending upstream PR  
**Affected upstream version:** ≤ v0.11.4 (commit `91cedda`)

### Root cause

When `auto_consumer_groups: true` is set in the restore configuration, the restore engine (`engine.rs`) loads consumer groups from the snapshot file at runtime and injects them into `RestoreOptions.consumer_groups`. However, `ThreePhaseRestore` in `three_phase.rs` checks `restore_options.consumer_groups` **before** this injection happens — it sees an empty list and skips Phase 3 unconditionally.

As a result, consumer group offset reset is **never executed** when using `--auto-consumer-groups`, even if groups were successfully loaded from the snapshot.

### Reproduction

```bash
kafka-backup three-phase-restore --config restore.yaml
# restore.yaml contains: auto_consumer_groups: true
# consumer-groups-snapshot.json is present and non-empty
```

Expected output:
```
Phase 3: Generating and applying offset reset plan...
```

Actual output (before fix):
```
║ PHASE 3: OFFSET RESET (skipped)
║   No consumer groups configured or reset_consumer_offsets=false
```

### Fix

**4 files changed, 21 insertions(+), 5 deletions(-)**

#### `crates/kafka-backup-core/src/config.rs`

Allow `reset_consumer_offsets: true` without pre-specifying `consumer_groups` when `auto_consumer_groups: true`, since groups will be resolved at runtime from the snapshot.

```rust
// Before
if self.reset_consumer_offsets && self.consumer_groups.is_empty() {

// After
if self.reset_consumer_offsets && self.consumer_groups.is_empty() && !self.auto_consumer_groups {
```

#### `crates/kafka-backup-core/src/manifest.rs`

Add `resolved_consumer_groups` field to `RestoreReport` so the engine can propagate the runtime-resolved group list to the three-phase orchestrator.

```rust
pub struct RestoreReport {
    // ...existing fields...
    /// Consumer groups resolved during restore (includes auto-loaded groups from snapshot)
    #[serde(default)]
    pub resolved_consumer_groups: Vec<String>,
}
```

#### `crates/kafka-backup-core/src/restore/engine.rs`

Populate `resolved_consumer_groups` in the report after groups are loaded:

```rust
// End of restore(), where RestoreReport is built:
resolved_consumer_groups: restore_options.consumer_groups.clone(),
```

Also initialise to `Vec::new()` in the dry-run and early-exit paths.

#### `crates/kafka-backup-core/src/restore/three_phase.rs`

Use `resolved_consumer_groups` (the post-injection list) to determine whether Phase 3 should run, and activate Phase 3 automatically when `auto_consumer_groups=true` and groups were resolved:

```rust
let effective_consumer_groups = if !restore_report.resolved_consumer_groups.is_empty() {
    restore_report.resolved_consumer_groups.clone()
} else {
    restore_options.consumer_groups.clone()
};
let effective_reset = restore_options.reset_consumer_offsets
    || (restore_options.auto_consumer_groups && !effective_consumer_groups.is_empty());
```

### Impact

- No behaviour change when `auto_consumer_groups: false` (default)
- No behaviour change when `consumer_groups` is specified explicitly
- Fixes Phase 3 for all users of `auto_consumer_groups: true`

---

## Fix 2 — Validation: `MessageCountCheck` / `OffsetRangeCheck` fail on multi-broker clusters

**Status:** Implemented — commit on `fix/auto-consumer-groups-phase3`  
**Affected upstream version:** ≤ v0.11.4 (commit `91cedda`)

### Root cause

The `ValidationContext` held a `KafkaClient` — a single TCP connection to one bootstrap broker. Both `MessageCountCheck` and `OffsetRangeCheck` called `ctx.target_client.get_offsets(topic, partition)`, which sent a `ListOffsets` request to that single broker.

In a multi-broker cluster, partition leaders are distributed. When the connected broker is **not** the leader for a given partition, Kafka responds with error code **6 = NOT_LEADER_FOR_PARTITION**. The check treated this as a failure and did not add the partition's count to `total_restored`.

On a 3-broker cluster with balanced leadership, ~67% of partitions fail → the validation reports a large fraction of the cluster as "not restored", even after a successful restore.

### Evidence (before fix)

```
[FAILED] MessageCountCheck — 39 topics; 3638 messages expected, 917 restored; 37 discrepancies
[FAILED] OffsetRangeCheck  — 45 partitions checked; 13 passed; 32 issues
```

The restore itself succeeded (3637 records restored, 0 errors).

### Fix

Replace `Arc<KafkaClient>` with `Arc<PartitionLeaderRouter>` in `ValidationContext`. The `PartitionLeaderRouter` already has `get_offsets()` with per-partition leader routing and automatic retry on `NOT_LEADER_FOR_PARTITION`.

The `ConsumerGroupOffsetCheck` uses `ListGroups` / `OffsetFetch` which are forwarded by the broker to the group coordinator — no partition-leader routing needed. It accesses the underlying bootstrap client via the new `PartitionLeaderRouter::client()` accessor.

#### Files changed

| File | Change |
|------|--------|
| `crates/kafka-backup-core/src/kafka/partition_router.rs` | Added `pub fn client() -> &KafkaClient` accessor |
| `crates/kafka-backup-core/src/validation/context.rs` | `target_client: Arc<PartitionLeaderRouter>` |
| `crates/kafka-backup-cli/src/commands/validation.rs` | Instantiate `PartitionLeaderRouter::new(config.target).await?`; remove old `create_kafka_client` helper |
| `crates/kafka-backup-core/src/validation/consumer_group.rs` | Use `ctx.target_client.client()` for ListGroups / OffsetFetch |
| `crates/kafka-backup-core/src/validation/message_count.rs` | No change — `get_offsets()` signature identical |
| `crates/kafka-backup-core/src/validation/offset_range.rs` | No change — `get_offsets()` signature identical |

### Expected result after fix

```
[PASSED] MessageCountCheck — 39 topics; 3638 messages expected, 3637 restored; 0 discrepancies
[PASSED] OffsetRangeCheck  — 45 partitions checked; 45 passed; 0 issues
```

---

## Fix 3 — SQLite schema migration not run after loading offset DB from storage

**Status:** Implemented — commit on `fix/auto-consumer-groups-phase3`  
**Affected upstream version:** ≤ v0.11.4 (commit `91cedda`)

### Root cause

`load_from_storage()` downloads `offsets.db` from object storage and replaces the SQLite connection pool. However, `initialize_schema()` was **not called** on the newly loaded database.

If the stored DB was created by an older binary (before the `backup_jobs` table was added), or if backup data was wiped from object storage while `offsets.db` was left behind, the next startup would fail immediately:

```
Error: Storage error: Backend error: error returned from database: (code: 1) no such table: backup_jobs
[kafka-backup] WARNING: kafka-backup exited with code 0, restarting in 10s...
```

The pod would restart in a loop until the outdated/incomplete `offsets.db` was manually removed from storage.

### Fix

Call `initialize_schema()` after the pool is replaced in `load_from_storage()`. All DDL statements use `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS`, making this call idempotent on up-to-date databases.

#### File changed

| File | Change |
|------|--------|
| `crates/kafka-backup-core/src/offset_store/sqlite.rs` | Call `self.initialize_schema().await?` after pool replacement in `load_from_storage()` |

### Workaround (before rebuild)

Delete `kafka-backup/offsets.db` from object storage when resetting backup data.

---

## Fix 4 — Consumer groups snapshot overwritten with empty list on restart

**Status:** Implemented — commit `695efa4` on `fix/auto-consumer-groups-phase3`  
**Affected upstream version:** ≤ v0.11.4 (commit `91cedda`)

### Root cause

`snapshot_consumer_groups()` is called at every backup loop iteration with `consumer_group_snapshot: true`. It builds the list of groups with committed offsets for the backed-up topics, then **unconditionally overwrites** `consumer-groups-snapshot.json` in storage — even when the resulting list is empty.

This causes data loss in a common DR / upgrade scenario:

1. Applications run normally → snapshot is populated with real consumer groups ✅
2. `kafka-backup` pod is deleted/restarted (reinstall, crash, upgrade)
3. Pod restarts; backup loop begins; **applications are not yet reconnected** (still starting up)
4. `list_groups_all_brokers()` returns groups, but none have committed offsets on the backed-up topics yet
5. `snapshot_groups` is empty → snapshot written as `{ "groups": [] }` → **previous valid snapshot is lost** ❌
6. Restore runs → `auto_consumer_groups` reads the empty snapshot → Phase 3 skipped → **consumer offsets not restored**

### Fix

Skip the `storage.put()` call when `snapshot_groups` is empty. The existing snapshot (if any) is preserved.

#### File changed

| File | Change |
|------|--------|
| `crates/kafka-backup-core/src/backup/engine.rs` | Early return in `snapshot_consumer_groups()` when `snapshot_groups.is_empty()` |

```rust
// Before (always overwrites)
let snapshot = Snapshot { snapshot_time: ..., groups: snapshot_groups };
self.storage.put(&key, ...).await?;

// After (skip write if empty)
if snapshot_groups.is_empty() {
    debug!("No groups with committed offsets, keeping existing snapshot");
    return Ok(());
}
let snapshot = Snapshot { snapshot_time: ..., groups: snapshot_groups };
self.storage.put(&key, ...).await?;
```

### Impact

- No behaviour change when groups are present (normal operation)
- Protects the snapshot across pod restarts during DR / upgrade sequences
- **Correct restore order**: snapshot is captured while apps are live → kafka-backup can restart freely → restore runs → apps start and find their offsets already reset
