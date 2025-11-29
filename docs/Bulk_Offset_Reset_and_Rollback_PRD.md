# OSO Kafka Backup: Bulk Offset Reset & Offset Reset Rollback - Technical PRD

## Document Status
**Version:** 1.0  
**Date:** 2025-11-29  
**Audience:** Engineering Team, Code Review Agents  
**Priority:** High (OSS Feature, Day 2 Operations Critical)

---

## Executive Summary

This PRD details implementation of two critical offset reset features:

1. **Bulk Offset Reset** – Parallel consumer group offset resets using Kafka's OffsetCommitRequest batching
2. **Offset Reset Rollback** – Pre-restore snapshot + rollback mechanism using Kafka transaction semantics

Both features are informed by:
- Kafka broker internals (`__consumer_offsets` compacted topic, group coordinator)
- Kafka transaction protocol (atomic multi-partition writes)
- Flink checkpointing patterns (consistent snapshots)
- Apache Kafka's batching optimization principles

---

## Part 1: Bulk Offset Reset

### 1.1 Current Implementation (Sequential)

```rust
// CURRENT: Process groups one at a time
pub async fn reset_offsets_sequential(
    kafka_brokers: &str,
    offset_mappings: Vec<(String, TopicPartition, i64)>,
) -> Result<()> {
    for (group_id, tp, offset) in offset_mappings {
        let client = KafkaClient::new(kafka_brokers)?;
        
        // ❌ Problem: Wait for each commit to complete before next
        client.commit_offset(&group_id, &tp.topic, tp.partition, offset).await?;
        
        println!("Committed: group={}, topic={}, partition={}, offset={}", 
                 group_id, tp.topic, tp.partition, offset);
    }
    Ok(())
}

// Time complexity: O(n * round_trip_time)
// For 1000 offsets @ 10ms/roundtrip = 10 seconds
```

**Problem:** Network round-trip latency is serialized. 1000 groups × 10ms = 10 seconds.

---

### 1.2 Kafka Protocol: OffsetCommitRequest Internals

**Reference:** Apache Kafka Protocol (Fetch/Commit APIs)

```
OffsetCommitRequest Structure (v7+):
  GroupId: STRING
  GenerationId: INT32                          ← Fencing mechanism
  MemberId: STRING
  RetentionTime: INT64
  Topics: [
    Name: STRING
    Partitions: [
      PartitionIndex: INT32
      CommittedOffset: INT64
      CommittedLeaderEpoch: INT32              ← Zombie fencing
      CommittedMetadata: NULLABLE_STRING       ← Custom metadata
    ]
  ]

OffsetCommitResponse:
  ThrottleTimeMs: INT32
  Topics: [
    Name: STRING
    Partitions: [
      PartitionIndex: INT32
      ErrorCode: INT16                         ← Success/Failure per partition
      ErrorMessage: NULLABLE_STRING
    ]
  ]
```

**Key Insight:** OffsetCommitRequest accepts **multiple (topic, partition) pairs in a single request**. This is Kafka's built-in batching mechanism.

---

### 1.3 Kafka Broker Architecture: Group Coordinator

**How Kafka Handles Batch Offset Commits:**

```
Client sends OffsetCommitRequest:
  group_id: "app1"
  topics: [
    {topic: "orders", partition: 0, offset: 1000},
    {topic: "orders", partition: 1, offset: 2000},
    {topic: "payments", partition: 0, offset: 500},
  ]
                    ↓
        [Group Coordinator Broker]
        (elected via group.id hash)
                    ↓
1. Lookup broker responsible for __consumer_offsets topic
2. For EACH (topic, partition):
   - Generate key: [group_id, topic, partition] (compacted topic key)
   - Generate value: OffsetAndMetadata{offset, timestamp, epoch}
   - Append to __consumer_offsets partition (batched write)
                    ↓
3. __consumer_offsets receives SINGLE BATCH:
   Key 1: [app1, orders, 0]       → Value: {offset: 1000, ts: X}
   Key 2: [app1, orders, 1]       → Value: {offset: 2000, ts: Y}
   Key 3: [app1, payments, 0]     → Value: {offset: 500, ts: Z}
                    ↓
4. All in-sync replicas acknowledge receipt
                    ↓
5. Coordinator sends SINGLE response (all partitions at once)
```

**Crucial Detail:** Kafka brokers batch writes to `__consumer_offsets`. Multiple partition offsets in one request = one append to the compacted topic.

**Source:** Confluent's Kafka Design documentation + Apache Kafka source (`GroupCoordinator.scala`)

---

### 1.4 Proposed Implementation: Parallel Bulk Reset

**Architecture:**

```rust
// NEW: Batch offsets by group_id, send in parallel
pub struct BulkOffsetReset {
    /// Group coordinator client (maintains connection)
    coordinator: GroupCoordinatorClient,
    
    /// In-flight requests tracker
    inflight_requests: Arc<Mutex<HashMap<String, InflightRequest>>>,
    
    /// Metrics for observability
    metrics: OffsetResetMetrics,
}

#[derive(Clone, Debug)]
pub struct OffsetResetBatch {
    pub group_id: String,
    /// Vec of (topic, partition, new_offset)
    pub offsets: Vec<(String, i32, i64)>,
    /// Request ID for correlation
    pub request_id: u64,
}

#[derive(Clone, Debug)]
pub struct InflightRequest {
    pub batch: OffsetResetBatch,
    pub sent_at: Instant,
    pub correlation_id: u32,
}

pub struct OffsetResetMetrics {
    pub total_offsets_reset: Arc<AtomicU64>,
    pub successful_requests: Arc<AtomicU64>,
    pub failed_requests: Arc<AtomicU64>,
    pub average_latency_ms: Arc<Mutex<f64>>,
    pub p99_latency_ms: Arc<Mutex<f64>>,
}
```

**Step 1: Group Offsets by Consumer Group**

```rust
/// Organize offset_mappings by group_id for efficient batching
fn group_offsets_by_group(
    offset_mappings: Vec<OffsetMapping>,
) -> HashMap<String, Vec<(String, i32, i64)>> {
    let mut grouped: HashMap<String, Vec<_>> = HashMap::new();
    
    for mapping in offset_mappings {
        grouped
            .entry(mapping.group_id.clone())
            .or_insert_with(Vec::new)
            .push((
                mapping.topic.clone(),
                mapping.partition,
                mapping.new_offset,
            ));
    }
    
    grouped
}

// Example output:
// {
//   "accounting": [(orders, 0, 1000), (orders, 1, 2000), (payments, 0, 500)],
//   "analytics": [(orders, 0, 5000), (events, 0, 10000)],
//   "logging": [(orders, 0, 900), (orders, 1, 950)],
// }
//
// Now we can send 3 requests (one per group) in parallel instead of N
```

**Step 2: Build OffsetCommitRequest Packets**

```rust
/// Build a single OffsetCommitRequest for a group
fn build_offset_commit_request(
    group_id: &str,
    offsets: &[(String, i32, i64)],
) -> OffsetCommitRequest {
    // Organize by topic first (Kafka protocol structure)
    let mut topics_map: HashMap<String, Vec<_>> = HashMap::new();
    
    for (topic, partition, offset) in offsets {
        topics_map
            .entry(topic.clone())
            .or_insert_with(Vec::new)
            .push(PartitionOffset {
                partition: *partition,
                committed_offset: *offset,
                committed_metadata: None,
            });
    }
    
    // Convert to protocol format
    let topics = topics_map
        .into_iter()
        .map(|(topic_name, partitions)| TopicData {
            name: topic_name,
            partitions,
        })
        .collect();
    
    OffsetCommitRequest {
        group_id: group_id.to_string(),
        generation_id: -1,              // -1 = not using group membership
        member_id: String::new(),       // Can be empty for non-group resets
        retention_time_ms: -1,          // Use broker default
        topics,
    }
}
```

**Step 3: Send Requests in Parallel**

```rust
pub async fn reset_offsets_parallel(
    &self,
    offset_mappings: Vec<OffsetMapping>,
    max_concurrent_requests: usize,
) -> Result<OffsetResetReport> {
    // Step 1: Group by group_id
    let grouped = group_offsets_by_group(offset_mappings);
    let total_groups = grouped.len();
    
    // Step 2: Create futures for all groups
    let mut futures = Vec::new();
    
    for (group_id, offsets) in grouped {
        let batch = OffsetResetBatch {
            group_id: group_id.clone(),
            offsets,
            request_id: generate_request_id(),
        };
        
        let coordinator_clone = self.coordinator.clone();
        
        // Create async task for this group
        let future = async move {
            let request = build_offset_commit_request(&batch.group_id, &batch.offsets);
            
            coordinator_clone
                .send_offset_commit_request(&request)
                .await
        };
        
        futures.push(future);
    }
    
    // Step 3: Execute with concurrency limit using FuturesUnordered + tokio::semaphore
    let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent_requests));
    let mut results = Vec::new();
    
    for future in futures {
        let permit = semaphore.acquire().await?;
        
        // Execute with semaphore permit (max N concurrent)
        let task = async {
            let result = future.await;
            drop(permit);  // Release permit when done
            result
        };
        
        results.push(tokio::spawn(task));
    }
    
    // Step 4: Collect results
    let mut report = OffsetResetReport::new();
    
    for result_handle in results {
        match result_handle.await {
            Ok(Ok(response)) => {
                report.successful_groups += 1;
                self.metrics.successful_requests.fetch_add(1, Ordering::SeqCst);
            }
            Ok(Err(e)) => {
                report.failed_groups.push(e.to_string());
                self.metrics.failed_requests.fetch_add(1, Ordering::SeqCst);
            }
            Err(e) => {
                report.failed_groups.push(format!("Task error: {}", e));
                self.metrics.failed_requests.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    
    Ok(report)
}
```

**Step 4: Handle Per-Partition Errors**

```rust
/// OffsetCommitResponse can have per-partition errors
/// Retry only failed partitions, not entire batch
pub async fn retry_failed_partitions(
    &self,
    response: OffsetCommitResponse,
    original_batch: &OffsetResetBatch,
) -> Result<()> {
    let mut failed_offsets = Vec::new();
    
    // Check each topic's partition responses
    for topic_response in &response.topics {
        for partition_response in &topic_response.partitions {
            if partition_response.error_code != 0 {
                // This partition failed, retry it
                failed_offsets.push((
                    topic_response.name.clone(),
                    partition_response.partition,
                ));
            }
        }
    }
    
    if failed_offsets.is_empty() {
        return Ok(());  // All succeeded
    }
    
    tracing::warn!(
        "Offset reset failed for {} partitions, retrying...",
        failed_offsets.len()
    );
    
    // Extract only failed partitions and retry
    let retry_offsets: Vec<_> = original_batch
        .offsets
        .iter()
        .filter(|(topic, partition, _)| {
            failed_offsets.iter().any(|(t, p)| t == topic && *p == *partition)
        })
        .cloned()
        .collect();
    
    // Exponential backoff retry
    for attempt in 0..3 {
        let backoff_ms = 100 * (2_u64.pow(attempt));
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        
        let request = build_offset_commit_request(&original_batch.group_id, &retry_offsets);
        match self.coordinator.send_offset_commit_request(&request).await {
            Ok(_) => return Ok(()),
            Err(e) if attempt < 2 => {
                tracing::warn!("Retry attempt {} failed: {}, will retry...", attempt + 1, e);
            }
            Err(e) => return Err(anyhow!("Final retry failed: {}", e)),
        }
    }
    
    Ok(())
}
```

**Step 5: Observability & Metrics**

```rust
pub struct OffsetResetReport {
    pub total_groups_processed: usize,
    pub successful_groups: usize,
    pub failed_groups: Vec<String>,
    pub total_offsets_reset: u64,
    pub duration_ms: u64,
    pub p50_latency_ms: f64,
    pub p99_latency_ms: f64,
}

impl OffsetResetReport {
    pub fn as_json(&self) -> serde_json::Value {
        json!({
            "status": if self.failed_groups.is_empty() { "success" } else { "partial" },
            "groups": {
                "total": self.total_groups_processed,
                "successful": self.successful_groups,
                "failed": self.failed_groups.len(),
            },
            "offsets": {
                "total_reset": self.total_offsets_reset,
            },
            "performance": {
                "duration_ms": self.duration_ms,
                "p50_latency_ms": self.p50_latency_ms,
                "p99_latency_ms": self.p99_latency_ms,
                "offsets_per_second": (self.total_offsets_reset as f64 / self.duration_ms as f64) * 1000.0,
            }
        })
    }
}
```

### 1.5 Performance Analysis

**Comparison:**

```
Sequential Implementation:
  - Offsets: 5,000
  - Groups: 500
  - Avg latency per request: 10ms
  - Total time: 500 × 10ms = 5 seconds

Parallel Implementation (max_concurrent = 50):
  - Offsets: 5,000 (same)
  - Groups: 500 (same)
  - Avg latency per request: 10ms
  - Total time: ceil(500 / 50) × 10ms = 100ms
  - Speedup: ~50x
```

**Network Efficiency:**

- Sequential: 500 network round-trips
- Parallel (concurrency=50): 10 network round-trips
- Protocol: Each request can batch multiple (topic, partition) pairs

---

### 1.6 Implementation Checklist

**Phase 1: Core Implementation**
- [ ] `BulkOffsetReset` struct with semaphore-based concurrency control
- [ ] `group_offsets_by_group()` utility function
- [ ] `build_offset_commit_request()` protocol builder
- [ ] `reset_offsets_parallel()` main function
- [ ] Per-partition error handling + retry logic
- [ ] Test: Sequential vs parallel equivalence (same result, different timing)

**Phase 2: Observability**
- [ ] `OffsetResetMetrics` with latency tracking
- [ ] `OffsetResetReport` JSON serialization
- [ ] Prometheus metrics export (success/fail/latency)
- [ ] Structured logging at each stage
- [ ] Test: Metrics accuracy with known latencies

**Phase 3: Production Hardening**
- [ ] Circuit breaker for coordinator failures
- [ ] Jitter in retry backoff (avoid thundering herd)
- [ ] Max payload size validation (OffsetCommitRequest has limits)
- [ ] Dry-run mode (validate requests, don't send)
- [ ] Test: Large batch (10,000+ offsets), fault injection

---

## Part 2: Offset Reset Rollback

### 2.1 Current Implementation (No Rollback)

```rust
// CURRENT: Apply offsets, no way to undo if something fails
pub async fn restore_and_reset_offsets(
    kafka_brokers: &str,
    backup_id: &str,
    offset_mappings: Vec<OffsetMapping>,
) -> Result<()> {
    // Step 1: Restore data
    restore_data_from_backup(backup_id).await?;
    
    // Step 2: Reset offsets
    // ❌ Problem: If this fails partway through, offsets are inconsistent
    // with restored data. No way to recover.
    reset_offsets_parallel(offset_mappings).await?;
    
    Ok(())
}
```

**Problem:** If offset reset fails mid-operation:
- Some groups have new offsets (will skip restored messages)
- Some groups have old offsets (will reprocess)
- Data and offsets are inconsistent
- Manual intervention required

---

### 2.2 Kafka Transaction Model & Atomic Commits

**Reference:** Kafka Transactions (KIP-98, KIP-511)

```
Kafka Transaction Flow:
    
    TxnBegin Marker → [topic-A, topic-B, __consumer_offsets]
        ↓
    Write message to topic-A
        ↓
    Write message to topic-B
        ↓
    Write offset to __consumer_offsets
        ↓
    TxnCommit Marker → [all topics]
        ↓
    All writes appear atomically to consumers in READ_COMMITTED mode
    OR
    TxnAbort Marker → [all topics]
        ↓
    All writes are rolled back (never visible)
```

**Key:** `__consumer_offsets` is a Kafka topic. We can include offset writes in a transaction.

---

### 2.3 Pre-Restore Snapshot: Flink Checkpoint Pattern

**Reference:** Apache Flink Checkpointing Mechanism

```rust
/// Pre-restore snapshot: capture current state before any changes
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OffsetSnapshot {
    /// Timestamp when snapshot was taken
    pub snapshot_id: String,           // UUID
    pub timestamp: DateTime<Utc>,
    
    /// For each group, store current offset state
    pub group_offsets: HashMap<String, GroupOffsetState>,
    
    /// Metadata for recovery
    pub restore_plan_id: String,       // Links to restore operation
    pub cluster_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GroupOffsetState {
    pub group_id: String,
    /// Current offsets in Kafka before any changes
    pub current_offsets: HashMap<(String, i32), i64>,  // (topic, partition) -> offset
}

/// Create pre-restore snapshot
pub async fn snapshot_current_offsets(
    coordinator: &GroupCoordinatorClient,
    group_ids: &[String],
) -> Result<OffsetSnapshot> {
    let snapshot_id = uuid::Uuid::new_v4().to_string();
    let mut group_offsets = HashMap::new();
    
    // Fetch current offsets for each group from Kafka
    for group_id in group_ids {
        let offsets = coordinator.fetch_committed_offsets(group_id).await?;
        
        group_offsets.insert(
            group_id.clone(),
            GroupOffsetState {
                group_id: group_id.clone(),
                current_offsets: offsets,
            },
        );
    }
    
    Ok(OffsetSnapshot {
        snapshot_id,
        timestamp: Utc::now(),
        group_offsets,
        restore_plan_id: String::new(),  // Will be filled later
        cluster_id: String::new(),
    })
}
```

**Store Snapshot Durably:**

```rust
/// Save snapshot to persistent storage (S3, database, etc.)
pub async fn persist_offset_snapshot(
    snapshot: &OffsetSnapshot,
    storage: &Storage,
) -> Result<String> {
    let snapshot_path = format!(
        "offset-snapshots/{}/snapshot.json",
        snapshot.snapshot_id
    );
    
    let serialized = serde_json::to_string(snapshot)?;
    storage.write(&snapshot_path, serialized).await?;
    
    tracing::info!(
        "Offset snapshot persisted: {}",
        snapshot_path
    );
    
    Ok(snapshot_path)
}
```

---

### 2.4 Atomic Offset Reset with Rollback

**Architecture:**

```rust
pub struct OffsetResetWithRollback {
    /// Snapshot of offsets before reset
    snapshot: OffsetSnapshot,
    
    /// New offsets to apply
    new_offsets: Vec<OffsetMapping>,
    
    /// Transaction controller (uses Kafka transactions)
    transaction_controller: TransactionController,
    
    /// Persistent storage for snapshots & logs
    storage: Arc<Storage>,
}

/// Result of offset reset attempt
#[derive(Debug, Clone)]
pub struct OffsetResetResult {
    pub status: ResetStatus,
    pub groups_applied: Vec<String>,
    pub groups_failed: Vec<String>,
    pub snapshot_id: String,
    pub rollback_available: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ResetStatus {
    Success,
    PartialSuccess,
    Failed,
}
```

**Step 1: Validate Against Snapshot**

```rust
/// Before applying new offsets, validate they make sense
fn validate_offset_reset(
    snapshot: &OffsetSnapshot,
    new_offsets: &[OffsetMapping],
) -> Result<()> {
    for offset_mapping in new_offsets {
        // Validate: new offset doesn't exceed topic-partition's high watermark
        // (This requires broker metadata call, but worth doing)
        
        // Validate: offset mapping is for groups we have snapshots for
        if !snapshot.group_offsets.contains_key(&offset_mapping.group_id) {
            tracing::warn!(
                "No snapshot for group {}, snapshot may be incomplete",
                offset_mapping.group_id
            );
        }
    }
    
    Ok(())
}
```

**Step 2: Apply with Transactional Semantics**

```rust
pub async fn reset_offsets_with_transaction(
    &self,
    new_offsets: Vec<OffsetMapping>,
) -> Result<OffsetResetResult> {
    validate_offset_reset(&self.snapshot, &new_offsets)?;
    
    let mut groups_applied = Vec::new();
    let mut groups_failed = Vec::new();
    let mut final_status = ResetStatus::Success;
    
    // Group offsets by group_id (same as bulk reset)
    let grouped = group_offsets_by_group(new_offsets.clone());
    
    // Try to apply with transaction protection
    for (group_id, offsets) in grouped {
        match self.apply_offset_group_atomic(&group_id, &offsets).await {
            Ok(_) => {
                groups_applied.push(group_id);
            }
            Err(e) => {
                tracing::error!("Failed to reset offsets for {}: {}", group_id, e);
                groups_failed.push(group_id);
                final_status = ResetStatus::PartialSuccess;
            }
        }
    }
    
    let status = if groups_failed.is_empty() {
        ResetStatus::Success
    } else if groups_applied.is_empty() {
        ResetStatus::Failed
    } else {
        ResetStatus::PartialSuccess
    };
    
    Ok(OffsetResetResult {
        status,
        groups_applied,
        groups_failed,
        snapshot_id: self.snapshot.snapshot_id.clone(),
        rollback_available: true,  // Always true if snapshot exists
    })
}

/// Apply offsets for a single group with atomic semantics
async fn apply_offset_group_atomic(
    &self,
    group_id: &str,
    offsets: &[(String, i32, i64)],
) -> Result<()> {
    // Optional: For extra safety, wrap in Kafka transaction
    // This requires producer that can write to __consumer_offsets
    // Advanced feature, may not be necessary for initial release
    
    // For now: use standard OffsetCommitRequest (already atomic per group)
    let request = build_offset_commit_request(group_id, offsets);
    
    self.transaction_controller
        .send_offset_commit_request(&request)
        .await?;
    
    Ok(())
}
```

**Step 3: Rollback Mechanism**

```rust
/// Restore offsets from snapshot
pub async fn rollback_offset_reset(
    &self,
    snapshot_id: &str,
) -> Result<OffsetResetResult> {
    // Load snapshot from persistent storage
    let snapshot = self.storage.read_offset_snapshot(snapshot_id).await?;
    
    tracing::info!(
        "Rolling back offset reset to snapshot: {}",
        snapshot_id
    );
    
    let mut groups_applied = Vec::new();
    let mut groups_failed = Vec::new();
    
    // For each group in snapshot, restore original offsets
    for (group_id, group_state) in &snapshot.group_offsets {
        let offsets_to_restore: Vec<_> = group_state
            .current_offsets
            .iter()
            .map(|((topic, partition), offset)| {
                (topic.clone(), *partition, *offset)
            })
            .collect();
        
        match self.apply_offset_group_atomic(group_id, &offsets_to_restore).await {
            Ok(_) => {
                groups_applied.push(group_id.clone());
                tracing::info!("Rolled back {} to original state", group_id);
            }
            Err(e) => {
                tracing::error!("Failed to rollback {}: {}", group_id, e);
                groups_failed.push(group_id.clone());
            }
        }
    }
    
    Ok(OffsetResetResult {
        status: if groups_failed.is_empty() {
            ResetStatus::Success
        } else if groups_applied.is_empty() {
            ResetStatus::Failed
        } else {
            ResetStatus::PartialSuccess
        },
        groups_applied,
        groups_failed,
        snapshot_id: snapshot_id.to_string(),
        rollback_available: false,  // Already rolled back
    })
}
```

**Step 4: Rollback Verification**

```rust
/// Verify that offsets match snapshot (post-rollback validation)
pub async fn verify_rollback(
    &self,
    snapshot: &OffsetSnapshot,
) -> Result<OffsetVerification> {
    let mut verification = OffsetVerification {
        groups_verified: Vec::new(),
        groups_mismatched: Vec::new(),
    };
    
    for (group_id, expected_state) in &snapshot.group_offsets {
        // Fetch current offsets from Kafka
        let actual_offsets = self
            .transaction_controller
            .fetch_committed_offsets(group_id)
            .await?;
        
        // Compare
        if actual_offsets == expected_state.current_offsets {
            verification.groups_verified.push(group_id.clone());
        } else {
            tracing::warn!(
                "Offset mismatch for group {} after rollback",
                group_id
            );
            verification.groups_mismatched.push(group_id.clone());
        }
    }
    
    Ok(verification)
}

pub struct OffsetVerification {
    pub groups_verified: Vec<String>,
    pub groups_mismatched: Vec<String>,
}
```

---

### 2.5 Complete Restore + Reset + Rollback Flow

```rust
pub async fn restore_with_rollback_capability(
    &self,
    backup_id: &str,
    groups_to_reset: Vec<String>,
    offset_mappings: Vec<OffsetMapping>,
) -> Result<RestoreWithRollbackResult> {
    let mut result = RestoreWithRollbackResult {
        status: RestoreStatus::Success,
        backup_id: backup_id.to_string(),
        snapshot_id: String::new(),
        offsets_reset: false,
        rollback_available: false,
        errors: Vec::new(),
    };
    
    // Phase 1: Create snapshot BEFORE any changes
    tracing::info!("Phase 1: Creating offset snapshot...");
    
    let snapshot = match snapshot_current_offsets(&self.coordinator, &groups_to_reset).await {
        Ok(snap) => snap,
        Err(e) => {
            result.errors.push(format!("Failed to snapshot offsets: {}", e));
            result.status = RestoreStatus::Failed;
            return Ok(result);
        }
    };
    
    // Persist snapshot
    let snapshot_path = self.storage.persist_offset_snapshot(&snapshot).await?;
    result.snapshot_id = snapshot.snapshot_id.clone();
    result.rollback_available = true;
    
    tracing::info!("Snapshot saved to: {}", snapshot_path);
    
    // Phase 2: Restore data
    tracing::info!("Phase 2: Restoring data from backup...");
    
    match restore_data_from_backup(backup_id).await {
        Ok(_) => tracing::info!("Data restored successfully"),
        Err(e) => {
            result.errors.push(format!("Data restore failed: {}", e));
            result.status = RestoreStatus::Failed;
            // ✅ Offsets unchanged, no rollback needed
            return Ok(result);
        }
    }
    
    // Phase 3: Reset offsets
    tracing::info!("Phase 3: Resetting consumer group offsets...");
    
    match self.reset_offsets_with_transaction(offset_mappings).await {
        Ok(reset_result) => {
            result.offsets_reset = true;
            if reset_result.status == ResetStatus::Failed {
                result.status = RestoreStatus::PartialSuccess;
                result.errors.push(format!(
                    "Offset reset partially failed: {} groups failed",
                    reset_result.groups_failed.len()
                ));
                
                // Rollback offsets to snapshot
                tracing::warn!("Rolling back offset changes due to reset failure");
                
                match self.rollback_offset_reset(&snapshot.snapshot_id).await {
                    Ok(_) => {
                        tracing::info!("Successfully rolled back to snapshot");
                    }
                    Err(e) => {
                        result.status = RestoreStatus::Failed;
                        result.errors.push(format!("Rollback failed: {}", e));
                        result.rollback_available = false;
                    }
                }
            }
        }
        Err(e) => {
            result.status = RestoreStatus::PartialSuccess;
            result.errors.push(format!("Offset reset failed: {}", e));
            
            // Attempt rollback
            match self.rollback_offset_reset(&snapshot.snapshot_id).await {
                Ok(_) => {
                    tracing::info!("Successfully rolled back to snapshot after offset reset failure");
                }
                Err(rollback_err) => {
                    result.status = RestoreStatus::Failed;
                    result.errors.push(format!("Rollback also failed: {}", rollback_err));
                    result.rollback_available = false;
                }
            }
        }
    }
    
    Ok(result)
}

pub struct RestoreWithRollbackResult {
    pub status: RestoreStatus,
    pub backup_id: String,
    pub snapshot_id: String,
    pub offsets_reset: bool,
    pub rollback_available: bool,
    pub errors: Vec<String>,
}

pub enum RestoreStatus {
    Success,
    PartialSuccess,
    Failed,
}
```

---

### 2.6 Storage Interface for Snapshots

```rust
pub trait OffsetSnapshotStorage: Send + Sync {
    async fn persist_offset_snapshot(
        &self,
        snapshot: &OffsetSnapshot,
    ) -> Result<String>;  // Returns path/ID
    
    async fn read_offset_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<OffsetSnapshot>;
    
    async fn list_offset_snapshots(&self) -> Result<Vec<OffsetSnapshotInfo>>;
    
    async fn delete_offset_snapshot(&self, snapshot_id: &str) -> Result<()>;
}

pub struct OffsetSnapshotInfo {
    pub snapshot_id: String,
    pub created_at: DateTime<Utc>,
    pub restore_plan_id: String,
    pub group_count: usize,
    pub offset_count: usize,
}

/// S3 Implementation
pub struct S3OffsetSnapshotStorage {
    s3_client: S3Client,
    bucket: String,
    prefix: String,  // e.g., "offset-snapshots"
}

#[async_trait]
impl OffsetSnapshotStorage for S3OffsetSnapshotStorage {
    async fn persist_offset_snapshot(
        &self,
        snapshot: &OffsetSnapshot,
    ) -> Result<String> {
        let key = format!(
            "{}/{}/{}.json",
            self.prefix,
            snapshot.timestamp.format("%Y/%m/%d"),
            snapshot.snapshot_id
        );
        
        let serialized = serde_json::to_string(snapshot)?;
        
        self.s3_client
            .put_object()
            .bucket(self.bucket.clone())
            .key(&key)
            .body(serialized.into())
            .send()
            .await?;
        
        Ok(key)
    }
    
    async fn read_offset_snapshot(&self, snapshot_id: &str) -> Result<OffsetSnapshot> {
        // List objects to find snapshot by ID
        let key = format!("{}/{}.json", self.prefix, snapshot_id);
        
        let response = self.s3_client
            .get_object()
            .bucket(self.bucket.clone())
            .key(&key)
            .send()
            .await?;
        
        let bytes = response.body.collect().await?;
        let snapshot = serde_json::from_slice(&bytes)?;
        
        Ok(snapshot)
    }
    
    // ... other methods ...
}
```

---

### 2.7 Implementation Checklist

**Phase 1: Snapshot Mechanism**
- [ ] `OffsetSnapshot` struct (serializable, with UUID)
- [ ] `snapshot_current_offsets()` function (fetch from broker)
- [ ] `persist_offset_snapshot()` to S3 (durable storage)
- [ ] `OffsetSnapshotStorage` trait for pluggable backends
- [ ] Test: Snapshot consistency (same data in/out)

**Phase 2: Rollback**
- [ ] `rollback_offset_reset()` function (restore from snapshot)
- [ ] `verify_rollback()` function (validate offsets match)
- [ ] `reset_offsets_with_transaction()` wrapper
- [ ] Test: Snapshot → Apply Changes → Rollback → Verify Equality

**Phase 3: End-to-End**
- [ ] `restore_with_rollback_capability()` orchestration
- [ ] Error handling: partial failures, rollback on failure
- [ ] Audit logging: all state transitions
- [ ] Test: Chaos injection (fail at each phase, verify rollback)

**Phase 4: Observability**
- [ ] Snapshot metadata tracking (size, duration, group count)
- [ ] Rollback success/failure metrics
- [ ] Audit trail: "User X rolled back snapshot Y at time Z"
- [ ] Dashboard: Recent snapshots, rollback history

---

## Part 3: Integration with OSS Feature Gating

### 3.1 Update Feature Gate PRD

In `OSO_Feature_Gate_PRD.md`, Section 1.3, **update the table:**

```markdown
### 1.3 Offset Handling (OSS - Now Includes Automation)

| Feature | Implementation | License Check | Notes |
|---------|----------------|----------------|--------|
| **Consumer Group Offset Backup** | `core/offset_mapping.rs` | ❌ None | Read-only, always available |
| **Offset Mapping Report (JSON)** | `cli/commands/offset-reset.rs` | ❌ None | Manual user application |
| **Generate Reset Scripts** | `cli/commands/offset-reset.rs` | ❌ None | User runs kafka-consumer-groups CLI |
| **Header-Based Offset Tracking** | `core/offset_mapping.rs` | ❌ None | Stores x-original-offset header |
| **Timestamp Preservation** | `core/offset_mapping.rs` | ❌ None | For debugging/verification |
| **Automatic Offset Reset** | `core/offset_automation.rs` | ❌ None | Apply offsets to Kafka automatically |
| **Kafka API Integration (OffsetCommitRequest)** | `core/offset_automation.rs` | ❌ None | Direct Kafka broker communication |
| **Bulk Offset Reset (Parallel)** | `core/offset_automation.rs` | ❌ None | 50x speedup, groups processed in parallel |
| **Offset Reset Rollback** | `core/offset_rollback.rs` | ❌ None | Snapshot + transactional recovery |
```

### 3.2 Code Organization

```
core/
├── offset_mapping.rs       (OSS - existing, report generation)
├── offset_automation.rs    (OSS NEW - bulk + parallel reset)
└── offset_rollback.rs      (OSS NEW - snapshot + rollback)

cli/commands/
├── offset-reset.rs         (OSS - existing, JSON report)
└── offset-reset-bulk.rs    (OSS NEW - parallel + rollback CLI)
```

### 3.3 CLI Interface

```bash
# OSS: Old way (still works)
kafka-backup offset-reset --config config.yaml --output mapping.json
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --group app1 --topic orders --partition 0 --to-offset 1000 --execute

# OSS: New way (Bulk + Rollback)
kafka-backup offset-reset-bulk \
  --config config.yaml \
  --restore-id backup-20251129 \
  --max-concurrent 50 \
  --enable-rollback

# View snapshots
kafka-backup offset-reset-bulk --list-snapshots

# Rollback if needed
kafka-backup offset-reset-bulk \
  --rollback --snapshot-id <UUID>
```

---

## Part 4: Testing Strategy

### 4.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    // Bulk Offset Reset Tests
    
    #[test]
    fn test_group_offsets_by_group() {
        let mappings = vec![
            OffsetMapping { group_id: "app1".to_string(), topic: "orders".to_string(), partition: 0, new_offset: 100 },
            OffsetMapping { group_id: "app1".to_string(), topic: "orders".to_string(), partition: 1, new_offset: 200 },
            OffsetMapping { group_id: "app2".to_string(), topic: "events".to_string(), partition: 0, new_offset: 50 },
        ];
        
        let grouped = group_offsets_by_group(mappings);
        
        assert_eq!(grouped.len(), 2);  // 2 groups
        assert_eq!(grouped["app1"].len(), 2);
        assert_eq!(grouped["app2"].len(), 1);
    }
    
    #[tokio::test]
    async fn test_build_offset_commit_request() {
        let offsets = vec![
            ("orders".to_string(), 0, 1000),
            ("orders".to_string(), 1, 2000),
        ];
        
        let request = build_offset_commit_request("app1", &offsets);
        
        assert_eq!(request.group_id, "app1");
        assert_eq!(request.topics.len(), 1);  // 1 topic
        assert_eq!(request.topics[0].partitions.len(), 2);  // 2 partitions
    }
    
    // Offset Rollback Tests
    
    #[test]
    fn test_offset_snapshot_serialization() {
        let snapshot = OffsetSnapshot {
            snapshot_id: "test-123".to_string(),
            timestamp: Utc::now(),
            group_offsets: {
                let mut map = HashMap::new();
                let mut offsets = HashMap::new();
                offsets.insert(("orders".to_string(), 0), 1000);
                map.insert("app1".to_string(), GroupOffsetState {
                    group_id: "app1".to_string(),
                    current_offsets: offsets,
                });
                map
            },
            restore_plan_id: "plan-456".to_string(),
            cluster_id: "cluster-789".to_string(),
        };
        
        let serialized = serde_json::to_string(&snapshot).unwrap();
        let deserialized: OffsetSnapshot = serde_json::from_str(&serialized).unwrap();
        
        assert_eq!(snapshot.snapshot_id, deserialized.snapshot_id);
    }
}
```

### 4.2 Integration Tests

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_parallel_vs_sequential_equivalence() {
        // Create test Kafka cluster (testcontainers)
        let kafka = KafkaContainer::new();
        kafka.start().await;
        
        // Insert test data: 10 groups × 100 offsets = 1000 total
        let offset_mappings = create_test_offset_mappings(10, 100);
        
        // Sequential implementation
        let seq_result = reset_offsets_sequential(&offset_mappings).await;
        
        // Parallel implementation
        let par_result = reset_offsets_parallel(&offset_mappings, 5).await;
        
        // Verify same result
        assert_eq!(seq_result.successful_groups, par_result.successful_groups);
        assert_eq!(seq_result.failed_groups, par_result.failed_groups);
        
        // Verify parallel is faster (rough check)
        assert!(par_result.duration_ms < seq_result.duration_ms);
    }
    
    #[tokio::test]
    async fn test_rollback_restores_original_state() {
        let kafka = KafkaContainer::new();
        kafka.start().await;
        
        // Create snapshot of original state
        let snapshot = snapshot_current_offsets(&["app1", "app2"]).await.unwrap();
        
        // Apply new offsets
        let new_offsets = vec![
            OffsetMapping { group_id: "app1".to_string(), ... },
        ];
        reset_offsets_parallel(new_offsets).await.unwrap();
        
        // Verify offsets changed
        let offsets_after_reset = fetch_current_offsets(&["app1"]).await;
        assert_ne!(offsets_after_reset, snapshot.group_offsets["app1"].current_offsets);
        
        // Rollback
        rollback_offset_reset(&snapshot.snapshot_id).await.unwrap();
        
        // Verify offsets match snapshot
        let offsets_after_rollback = fetch_current_offsets(&["app1"]).await;
        assert_eq!(offsets_after_rollback, snapshot.group_offsets["app1"].current_offsets);
    }
    
    #[tokio::test]
    async fn test_restore_failure_triggers_rollback() {
        // Simulate: Data restore fails, offsets should not change
        
        let offset_result = restore_with_rollback_capability(
            "backup-123",
            vec!["app1".to_string()],
            vec![/* offset mappings */],
        ).await;
        
        assert_eq!(offset_result.status, RestoreStatus::PartialSuccess);
        assert_eq!(offset_result.offsets_reset, false);  // Never applied
    }
}
```

### 4.3 Chaos/Fault Injection Tests

```rust
#[cfg(test)]
mod chaos_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_partial_commit_failure_retry() {
        let kafka = KafkaContainer::new();
        kafka.start().await;
        
        // Simulate: 1st attempt fails, 2nd succeeds
        let mut mock_coordinator = MockGroupCoordinatorClient::new();
        mock_coordinator.set_fail_once();  // Fail on first call
        
        let result = reset_offsets_parallel_with_retry(
            &mock_coordinator,
            offset_mappings,
            3,  // max retries
        ).await;
        
        assert_eq!(result.status, ResetStatus::Success);
    }
    
    #[tokio::test]
    async fn test_network_partition_during_rollback() {
        let kafka = KafkaContainer::new();
        kafka.start().await;
        
        let snapshot = /* ... */;
        
        // Simulate: Network fails during rollback
        let mut coordinator = FailingMockCoordinator::new();
        coordinator.fail_after_n_requests(2);  // Fail on 3rd request
        
        let result = rollback_offset_reset_with_error_handling(&snapshot).await;
        
        // Should either succeed partially or provide clear rollback state
        assert!(
            result.status == ResetStatus::PartialSuccess ||
            result.rollback_available  // For manual recovery
        );
    }
}
```

---

## Part 5: Deployment & Rollout

### 5.1 Release Sequence

**Release 0.2.1 (Bulk Offset Reset)**
- Feature: Parallel offset reset (50x speedup)
- Breaking: None (backwards compatible)
- Migration: Auto-uses parallel if `--max-concurrent` not specified
- Rollout: Gradual (canary with 10% of users first)

**Release 0.2.2 (Offset Reset Rollback)**
- Feature: Snapshot + rollback capability
- Breaking: None (opt-in with `--enable-rollback`)
- Migration: No snapshot = no rollback (warn user)
- Rollout: Full (depends on 0.2.1)

### 5.2 Backwards Compatibility

```rust
// Old CLI still works
kafka-backup offset-reset --config config.yaml --output mapping.json
// Uses sequential implementation, but works

// New CLI is opt-in
kafka-backup offset-reset-bulk --config config.yaml --max-concurrent 50
// Uses parallel implementation
```

---

## Part 6: Security & Compliance

### 6.1 Snapshot Encryption (OSS Feature)

```rust
/// Snapshots contain sensitive offset state
/// Encrypt at rest when stored in S3

pub async fn persist_encrypted_snapshot(
    snapshot: &OffsetSnapshot,
    storage: &Storage,
    encryption_key: &[u8],
) -> Result<String> {
    // Serialize + encrypt with AES-256-GCM
    let serialized = serde_json::to_vec(snapshot)?;
    let encrypted = encrypt_aes_gcm(&serialized, encryption_key)?;
    
    storage.write(
        &format!("offset-snapshots/{}/{}.json.enc", /* ... */),
        encrypted,
    ).await?;
    
    Ok(/* ... */)
}
```

### 6.2 Audit Logging (OSS Feature)

```rust
/// Log all offset resets & rollbacks for compliance

pub async fn log_offset_reset_event(
    group_id: &str,
    action: OffsetResetAction,
    result: OffsetResetResult,
) -> Result<()> {
    tracing::info!(
        target: "offset-reset-audit",
        group = group_id,
        action = ?action,
        result = ?result,
        timestamp = ?Utc::now(),
        "Offset reset operation"
    );
    
    // Also write to persistent audit log
    // (for compliance systems to consume)
}

#[derive(Debug)]
pub enum OffsetResetAction {
    Reset,
    Rollback,
    Snapshot,
}
```

---

## Summary & Acceptance Criteria

### Bulk Offset Reset ✅

- [ ] **Performance:** ≥50x speedup for 500+ groups (parallel vs sequential)
- [ ] **Correctness:** Final state identical to sequential implementation
- [ ] **Reliability:** Retry logic handles per-partition failures without losing data
- [ ] **Observability:** Metrics exported (success/fail/latency p50/p99)
- [ ] **Usability:** CLI shows progress, human-readable output

### Offset Reset Rollback ✅

- [ ] **Atomicity:** Snapshot → Apply → Verify OR Snapshot → Rollback → Verify
- [ ] **Durability:** Snapshots persisted to S3 with encryption
- [ ] **Correctness:** Post-rollback state byte-for-byte matches pre-reset
- [ ] **Testability:** Chaos tests verify behavior under network partitions
- [ ] **Usability:** One-command rollback if needed

### Integration ✅

- [ ] Both features documented in Feature Gate PRD
- [ ] OSS feature gating section updated
- [ ] No license checks on either feature (OSS only)
- [ ] Backwards compatible with existing tools

