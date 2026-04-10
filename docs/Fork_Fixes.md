# Fork Fixes — Upstream Issues Resolved in This Fork

This document describes the bugs found in the upstream `kafka-backup` Rust codebase and how they were fixed in this fork. All changes are in `crates/kafka-backup-core` and `crates/kafka-backup-cli`.

---

## Fix 1 — New topics not discovered in continuous mode

### Symptom

A topic created after `kafka-backup` started was never picked up, even after several `poll_interval_ms` cycles. The only workaround was to restart the process.

### Root cause (upstream)

`resolve_topics()` was called **once before the backup loop**, then its result was reused indefinitely. No Kafka metadata refresh happened between cycles.

```rust
// upstream — topic discovery happens once, before the loop
let topics_metadata = self.resolve_topics(&source.topics, &backup_opts).await?;
loop {
    // topics_metadata is stale — new topics never appear here
}
```

### Fix

`resolve_topics()` is now called at the **start of every loop iteration**. The `Metadata` request costs a few milliseconds and is negligible relative to `poll_interval_ms`.

```rust
loop {
    // Re-discover topics at the start of every cycle so that new topics
    // created since the last cycle are picked up automatically.
    let topics_metadata = self.resolve_topics(&source.topics, &backup_opts).await?;

    if topics_metadata.is_empty() {
        warn!("No topics matched the configured patterns — skipping cycle");
        // wait poll_interval_ms then retry, no process restart required
        continue;
    }
    // … backup partitions …
}
```

**File:** `crates/kafka-backup-core/src/backup/engine.rs`

---

## Fix 2 — Topics disappearing from `manifest.json` every cycle

### Symptom

After a `poll_interval_ms` cycle, `kafka-backup describe` returned only the topics that received new messages in that cycle. Topics with no new data were completely absent from `manifest.json`.

### Root cause (upstream)

`save_manifest()` serialised the in-memory manifest — which only contains topics active in the **current session** — and overwrote the file on storage unconditionally. Previous entries were silently discarded.

This also caused an empty `manifest.json` at startup (written before the first flush cycle), making the backup temporarily unrestorable after every restart.

### Fix

`save_manifest()` now reads the existing manifest from storage, **union-merges** it with the current in-memory state, and writes the result back. The merge preserves all topics/partitions/segments ever seen across all sessions.

```rust
async fn save_manifest(&self) -> Result<()> {
    let current_manifest = self.manifest.lock().await.clone();
    let key = format!("{}/manifest.json", self.config.backup_id);

    let merged = match self.storage.get(&key).await {
        Ok(data) => match serde_json::from_slice::<BackupManifest>(&data) {
            Ok(existing) => merge_manifests(existing, current_manifest),
            Err(_) => current_manifest,   // corrupt — overwrite
        },
        Err(_) => current_manifest,       // first run — no existing manifest
    };

    self.storage.put(&key, Bytes::from(serde_json::to_string_pretty(&merged)?)).await
}
```

#### Merge algorithm

```
existing = manifest loaded from storage (cumulative history)
current  = in-memory manifest (only topics active this cycle)

merge(existing, current):
  topics only in existing       → preserved as-is
  topics only in current        → appended
  topics in both:
    original_partition_count    → updated from current (more accurate)
    partitions only in existing → preserved
    partitions only in current  → appended
    partitions in both:
      segments deduplicated by (key, start_offset); existing wins on conflict
      output sorted by (start_offset, key)
```

**Files:** `crates/kafka-backup-core/src/backup/engine.rs` (`save_manifest`, `merge_manifests`)

---

## Fix 3 — Segment files named by sequence number instead of first offset

### Symptom

Across restarts, segment files from different sessions could collide or produce ambiguous names. A session starting at offset 500,000 would produce `segment-00000000000000000002.bin.zst` while the previous session had already written a `segment-00000000000000000002.bin.zst` starting at offset 0.

### Root cause (upstream)

`segment_key()` took a `sequence: u64` counter that reset to 0 at every process start. The segment name bore no relationship to the Kafka offsets it contained.

```rust
// upstream
fn segment_key(&self, sequence: u64) -> String {
    format!("{}/topics/{}/partition={}/segment-{:020}.bin{}",
        self.backup_id, self.topic, self.partition, sequence, ext)
}
```

### Fix

The segment key is now derived from the **first Kafka offset** in the segment. This makes names globally unique and stable across restarts, and makes the storage layout self-describing.

```rust
fn segment_key(&self, start_offset: i64) -> String {
    format!("{}/topics/{}/partition={}/segment-{:020}.bin{}",
        self.backup_id, self.topic, self.partition, start_offset, ext)
}
```

`SegmentWriter` gained a `start_offset() -> Option<i64>` accessor, called just before each flush.

**Files:** `crates/kafka-backup-core/src/backup/engine.rs`, `crates/kafka-backup-core/src/segment/writer.rs`

---

## Fix 4 — `original_partition_count` missing from manifest

### Symptom

When restoring a topic where some partitions never received any messages, the restore engine could not determine the correct partition count and created topics with fewer partitions than the original.

### Root cause (upstream)

The manifest only stored partitions that had at least one segment. Empty partitions were simply absent, so `max(partition_id) + 1` underestimated the true partition count.

### Fix

At the start of each backup cycle, the engine reads the live partition count from Kafka metadata and writes it into the manifest as `original_partition_count`. The restore engine uses this field to create topics with the correct count.

```rust
// backup/engine.rs — recorded once per topic per cycle
let topic_entry = manifest.get_or_create_topic(topic);
topic_entry.original_partition_count = Some(partitions.len() as i32);
```

```rust
// manifest.rs — new field
pub struct TopicBackup {
    // …
    /// Original number of partitions in the source topic. Used by restore to
    /// recreate the topic correctly even when some partitions hold no data.
    #[serde(default)]
    pub original_partition_count: Option<i32>,
}
```

Priority at restore time:
1. `--repartitioning` override (if specified)
2. `original_partition_count` from `manifest.json`
3. `max(partition_id) + 1` from segments (fallback for older backups)

**Files:** `crates/kafka-backup-core/src/manifest.rs`, `crates/kafka-backup-core/src/backup/engine.rs`, `crates/kafka-backup-core/src/restore/engine.rs`

---

## Fix 5 — Consumer group offsets not captured during backup

### Symptom

After a full restore onto a new or reset cluster, all consumer groups restarted from offset 0 because the `__consumer_offsets` topic was not backed up.

### Fix

After each call to `save_manifest()`, the backup engine calls `snapshot_consumer_groups()` (enabled by default via `consumer_group_snapshot: true`). The function:

1. Reads backed-up topic names from the in-memory manifest
2. Lists all consumer groups across every broker (`ListGroups` per broker)
3. Fetches committed offsets (`OffsetFetch`) for each group
4. Filters to groups that have at least one offset on a backed-up topic
5. Writes `{backup_id}/consumer-groups-snapshot.json`

```rust
if backup_opts.consumer_group_snapshot {
    if let Err(e) = self.snapshot_consumer_groups().await {
        warn!("Consumer group snapshot failed (non-fatal): {}", e);
    }
}
```

Snapshot failures are non-fatal. Backup continues and the next cycle retries.

#### Snapshot format

```json
{
  "snapshot_time": 1731236400000,
  "groups": [
    {
      "group_id": "my-consumer-group",
      "offsets": {
        "my-topic": { "0": 12345, "1": 67890 }
      }
    }
  ]
}
```

At restore time, pass `auto_consumer_groups: true` (config) or the equivalent CLI flag. The restore engine reads `consumer-groups-snapshot.json`, maps source offsets to target offsets, and resets each group accordingly.

**New config field:** `BackupOptions.consumer_group_snapshot` (default: `true`)  
**New config field:** `RestoreOptions.auto_consumer_groups` (default: `false`)  
**Files:** `crates/kafka-backup-core/src/backup/engine.rs` (`snapshot_consumer_groups`), `crates/kafka-backup-core/src/config.rs`

---

## Fix 6 — Consumer group discovery incomplete on KRaft clusters

### Symptom

On KRaft clusters (no Zookeeper), `ListGroups` against a single broker returned only the groups whose coordinator lived on that broker. Groups coordinated by other brokers were silently missing from the snapshot.

### Root cause (upstream)

The existing `list_groups()` helper sent a single `ListGroups` request to whichever broker the bootstrap client happened to connect to.

### Fix

`PartitionLeaderRouter::list_groups_all_brokers()` iterates every known broker in the cluster and sends an independent `ListGroups` request to each, deduplicating results by `group_id`.

```rust
pub async fn list_groups_all_brokers(&self)
    -> Result<Vec<ConsumerGroup>>
{
    let broker_ids = self.broker_metadata.read().await.keys().copied().collect();
    let mut seen = HashSet::new();
    for broker_id in broker_ids {
        let client = self.get_broker_connection(broker_id).await?;
        for g in list_groups(&client).await? {
            if seen.insert(g.group_id.clone()) { all_groups.push(g); }
        }
    }
    Ok(all_groups)
}
```

**File:** `crates/kafka-backup-core/src/kafka/partition_router.rs`

---

## Fix 7 — Infinite hang when broker becomes unresponsive mid-request

### Symptom

When a broker stopped responding after accepting a TCP connection (network partition, broker crash during a produce), `kafka-backup` hung indefinitely. The Tokio task never completed, the circuit breaker was never triggered, and the restore stalled permanently.

### Root cause (upstream)

All `write_all` and `read_exact` calls on the TCP stream were unbounded. No client-side timeout was applied.

### Fix

Client-side timeouts wrap every read and write:

```rust
const WRITE_TIMEOUT_SECS: u64 = 10;
const RESPONSE_TIMEOUT_SECS: u64 = 10;

// Send request
timeout(Duration::from_secs(WRITE_TIMEOUT_SECS), conn.stream.write_all(buf))
    .await
    .map_err(|_| KafkaError::Protocol("Request timed out sending to broker"))?;

// Read response
timeout(Duration::from_secs(RESPONSE_TIMEOUT_SECS), conn.stream.read_exact(&mut len_buf))
    .await
    .map_err(|_| KafkaError::Protocol("Request timed out waiting for broker response"))?;
```

The resulting `"timed out after Xs"` error message is handled as a connection error by the reconnect logic, so the client automatically retries on a fresh connection.

**File:** `crates/kafka-backup-core/src/kafka/client.rs`

---

## Fix 8 — Produce fails permanently on `NOT_LEADER_FOR_PARTITION` or transient connection errors

### Symptom

During a restore, a `NOT_LEADER_FOR_PARTITION` error (triggered by a leader election or a partition rebalance) caused the restore to fail immediately with no retry. Transient TCP errors (broker restart, load balancer timeout) also caused permanent failure.

### Root cause (upstream)

`PartitionLeaderRouter::produce()` had no retry logic and no leader-refresh path.

### Fix

Two retry layers were added:

1. **NOT_LEADER_FOR_PARTITION** — one-shot retry after refreshing the partition leader map
2. **Connection errors** — up to 5 retries with exponential backoff (500 ms, 1 s, 1.5 s, 2 s, 2.5 s)

```rust
// One-shot retry for leader changes
match self.produce_internal(topic, partition, records.clone()).await {
    Ok(r) => return Ok(r),
    Err(e) if is_not_leader_error(&e) => {
        self.refresh_partition_leader(topic, partition).await?;
        self.clear_connection_cache().await;
        // retry once with fresh leader
    }
    Err(e) if is_connection_error(&e) => { /* fall through to retry loop */ }
    Err(e) => return Err(e),
}

// Exponential backoff for connection errors
for attempt in 1..=5 {
    tokio::time::sleep(Duration::from_millis(500 * attempt)).await;
    match self.produce_internal(topic, partition, records.clone()).await { … }
}
```

**File:** `crates/kafka-backup-core/src/kafka/partition_router.rs`

---

## Fix 9 — Producer ACKs set to `-1` (all replicas) causing restore performance issues

### Symptom

Restores were significantly slower than expected on clusters with replica lag (common on under-resourced disaster recovery clusters). Each produce call waited for **all in-sync replicas** to acknowledge before returning.

### Root cause (upstream)

`produce.rs` used `with_acks(-1)` (wait for all ISR) and `with_timeout_ms(30000)`. On a cluster with replica lag, every produce call blocked up to 30 seconds.

### Fix (initial)

Changed to `with_acks(1)` (leader only) and `with_timeout_ms(5000)`. With single-replica acknowledgement, produce latency tracks only the leader's write latency.

### Fix (subsequent) — configurable ACKs and timeout

The values are now configurable via `RestoreOptions`:

```yaml
# restore.yaml
restore:
  produce_acks: 1        # 0 = fire-and-forget, 1 = leader ack (default), -1 = all ISR
  produce_timeout_ms: 5000
```

These parameters are threaded through the full produce call chain:
`RestoreOptions` → `PartitionLeaderRouter::produce()` → `KafkaClient::produce()` → `produce::produce()`

**Files:** `crates/kafka-backup-core/src/config.rs`, `crates/kafka-backup-core/src/kafka/produce.rs`, `crates/kafka-backup-core/src/kafka/client.rs`, `crates/kafka-backup-core/src/kafka/partition_router.rs`, `crates/kafka-backup-core/src/restore/engine.rs`, `crates/kafka-backup-core/src/restore/repartition.rs`

---

## Fix 10 — No way to purge a target topic before restore

### Symptom

Replaying a restore onto an existing topic (e.g. after a failed first attempt) duplicated all messages. Topics managed by Strimzi could not be deleted and recreated programmatically.

### Fix

New `purge_topics: bool` option in `RestoreOptions` (default: `false`). When enabled, the restore engine calls `DeleteRecords` on every target partition before starting the restore, advancing the log-start-offset to the current end offset. This empties the topic without deleting it — safe for operator-managed topics.

```rust
if restore_options.purge_topics {
    self.purge_topics_before_restore(&topics_to_restore, &restore_options).await?;
}
```

The `DeleteRecords` API is implemented in `admin.rs` and exposed via `PartitionLeaderRouter::delete_records()`.

```yaml
# restore.yaml
restore:
  purge_topics: true
```

**Files:** `crates/kafka-backup-core/src/kafka/admin.rs`, `crates/kafka-backup-core/src/kafka/partition_router.rs`, `crates/kafka-backup-core/src/restore/engine.rs`, `crates/kafka-backup-core/src/config.rs`

---

## New CLI command — `snapshot-groups`

A standalone `snapshot-groups` command was added to `kafka-backup-cli`. It performs the same consumer group snapshot as the backup engine does automatically, but can be run on-demand against an existing backup:

```bash
kafka-backup snapshot-groups \
  --backup-id <id> \
  --storage-path <path> \
  --bootstrap-servers kafka:9092
```

This is useful for backups created before `consumer_group_snapshot` was introduced, or when the automatic snapshot needs to be refreshed manually before a restore.

**File:** `crates/kafka-backup-cli/src/commands/snapshot_groups.rs`

---

## Summary table

| # | Problem | File(s) changed |
|---|---------|-----------------|
| 1 | New topics not discovered between cycles | `backup/engine.rs` |
| 2 | Topics disappear from `manifest.json` after each cycle | `backup/engine.rs` |
| 3 | Segment files named by session sequence, not Kafka offset | `backup/engine.rs`, `segment/writer.rs` |
| 4 | `original_partition_count` missing — empty partitions lost at restore | `manifest.rs`, `backup/engine.rs`, `restore/engine.rs` |
| 5 | Consumer group offsets not captured during backup | `backup/engine.rs`, `config.rs` |
| 6 | Consumer groups incomplete on KRaft clusters | `kafka/partition_router.rs` |
| 7 | Infinite hang when broker becomes unresponsive | `kafka/client.rs` |
| 8 | Produce fails permanently on `NOT_LEADER` or connection errors | `kafka/partition_router.rs` |
| 9 | Produce ACKs hardcoded to `-1` — slow on replica-lagging clusters | `kafka/produce.rs`, `kafka/client.rs`, `kafka/partition_router.rs`, `restore/engine.rs`, `restore/repartition.rs`, `config.rs` |
| 10 | No way to purge a topic before restore (needed for Strimzi) | `kafka/admin.rs`, `kafka/partition_router.rs`, `restore/engine.rs`, `config.rs` |
