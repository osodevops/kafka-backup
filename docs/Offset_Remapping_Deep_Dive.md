# Deep Analysis: Kafka Offset Remapping During Restore – The Real Problem & Solutions

## Executive Summary

**Your concern is 100% valid and critical for production DR scenarios.** The issue is NOT just about metadata routing (which we fixed earlier) – it's about **offset space discontinuity** across clusters.

**The Core Problem:**
```
Source Cluster                           Target Cluster (after restore)
─────────────────                        ─────────────────────────────
orders/0: records at offsets 0-1000      orders/0: records at offsets 0-1000 (NEW!)
app1 consumer group at offset 750        app1 consumer group: WHERE???

Records are identical, but:
- Source had "750 means message from 10:45am"
- Target has "750 means message from 11:30am" (different message!)
```

This document explains **why** this happens (Kafka protocol level), **what fails** without fixes, and **three production solutions**.

---

## Part 1: Why This Happens (Kafka Protocol Deep Dive)

### 1.1 How Kafka Assigns Offsets – Immutable by Design

From Kafka Protocol (Section: Produce API):

```
ProduceRequest =>
  transactional_id: NULLABLE_STRING
  acks: INT16
  timeout_ms: INT32
  topic_data: [
    name: STRING
    partition_data: [
      index: INT32                          ← Partition number
      records: RECORDS                      ← Your message data
    ]
  ]

ProduceResponse =>
  topic_responses: [
    name: STRING
    partition_responses: [
      index: INT32                          ← Partition number
      error_code: INT16
      base_offset: INT64                    ← ⭐ BROKER-ASSIGNED OFFSET
      timestamp: INT64
      ...
    ]
  ]
```

**Critical insight:** You (the client) NEVER specify which offset to use. The **broker always assigns offsets sequentially** as it appends records to the partition log.

### 1.2 Offset Assignment Algorithm (from Kafka source code logic)

```
┌─ When Broker Receives ProduceRequest ─┐
│                                        │
├─ 1. Get leader partition log           │
│                                        │
├─ 2. Find next available offset          │
│    (usually: last_offset + 1)          │
│                                        │
├─ 3. Assign offsets to batch:           │
│    Message 0 → 1000                    │
│    Message 1 → 1001                    │
│    Message 2 → 1002                    │
│                                        │
├─ 4. Append batch atomically            │
│                                        │
├─ 5. Return ProduceResponse with        │
│    base_offset: 1000                   │
│    (first offset in batch)             │
│                                        │
└────────────────────────────────────────┘
```

**Key properties:**
- ✅ Offsets are **sequential** within a partition
- ✅ Offsets are **monotonically increasing**
- ✅ Offsets are **contiguous** (no gaps)
- ❌ Offsets are **NOT portable** across clusters
- ❌ Offsets are **NOT reusable** with different data

### 1.3 Example: Why Cross-Cluster Offset Transfer Fails

**Source Cluster State:**
```
orders partition 0:
  offset 0    → {order_id: 1, customer: "Alice", amount: 100}
  offset 1    → {order_id: 2, customer: "Bob", amount: 200}
  offset 2    → {order_id: 3, customer: "Charlie", amount: 300}
  ...
  offset 1000 → {order_id: 1001, customer: "Dave", amount: 1000}

Consumer group "accounting" committed offset: 1000
  Meaning: "We've processed all messages up to Alice→Bob→...→Dave"
```

**Target Cluster After Restore (naive approach):**
```
orders partition 0:
  offset 0    → {order_id: 1, customer: "Alice", amount: 100}
  offset 1    → {order_id: 2, customer: "Bob", amount: 200}
  offset 2    → {order_id: 3, customer: "Charlie", amount: 300}
  ...
  offset 1000 → {order_id: 1001, customer: "Dave", amount: 1000}

If we set consumer group "accounting" to offset 1000:
  Consumer reads offset 1001 onwards
  ✅ This works! Same data, same offset!
```

**BUT — Here's the Real Problem:**

What if during the restore process, the **target cluster already had data**?

```
Target Cluster BEFORE Restore:
  offset 0    → {schema_version: "v2_new_field_added"}
  offset 1    → {migration_marker: true}
  offset 2    → {test_record: "ignore_me"}
  offset 3    → {actual_data: ...}
  ...
  offset 47   → {last_record_before_restore: ...}

Now we restore source offsets 0-1000:
  Restored records produce at offsets 48-1048
  But source offset 1000 now = target offset 1048!

If we set accounting group to "source offset 1000":
  Target broker has no offset 1000 → ERROR
  OR
  Target broker has offset 1000 = {schema_version: "v2"} → WRONG DATA!
```

---

## Part 2: How __consumer_offsets Topic Stores Data

### 2.1 __consumer_offsets Internal Structure

The `__consumer_offsets` topic is **compacted** and stores:

```
Key:   [consumer_group_id, topic_name, partition_id]
Value: OffsetAndMetadata {
  offset: INT64,                  ← The committed offset
  leader_epoch: INT32,            ← For offset management
  timestamp: INT64,               ← When committed
  metadata: STRING,               ← Optional metadata
}
```

**Compaction rule:** Only the latest offset commit per (group, topic, partition) is kept.

### 2.2 How Consumer Offset Commit Works (from Protocol)

```
OffsetCommitRequest =>
  group_id: STRING
  generation_id: INT32
  member_id: STRING
  retention_time_ms: INT64
  topics: [
    name: STRING
    partitions: [
      index: INT32              ← partition
      committed_offset: INT64   ← ⭐ What offset to remember
      committed_metadata: STRING
    ]
  ]

OffsetCommitResponse =>
  topics: [
    name: STRING
    partitions: [
      index: INT32
      error_code: INT16
    ]
  ]
```

**Critical:** The broker **trusts the client** to know which offset is "correct". It just stores whatever you commit.

---

## Part 3: The Three Offset Mapping Strategies Explained

### 3.1 Strategy 1: Skip (DEFAULT – NO Offset Management)

```rust
pub async fn restore_skip_offsets(
    &self,
    target_cluster: &KafkaClient,
    backup_id: &str,
) -> Result<()> {
    // Just restore data, don't touch __consumer_offsets
    
    for segment in self.get_segments(backup_id).await? {
        let records = self.decompress_segment(segment)?;
        target_cluster.produce_batch(records).await?;
        // Records appended at offsets: whatever broker assigns
        // Consumer groups: NOT TOUCHED
    }
    
    Ok(())
}
```

**Pros:**
- ✅ Simple – no offset mapping
- ✅ Safe – no risk of offset corruption
- ✅ Correct data is restored

**Cons:**
- ❌ Consumer groups have stale offsets
- ❌ After restore, consumers reprocess ALL data (or skip to end)
- ❌ Risk of duplicate processing or message loss

**Use case:** New clusters, test environments, when consumers can tolerate restart.

---

### 3.2 Strategy 2: Header-Based (Preserves Original Offset Info)

**Backup Phase:**
```rust
// During BACKUP, store original offset in message header
pub async fn backup_with_offset_headers(
    &self,
    topic: &str,
    partition: i32,
) -> Result<()> {
    let mut client = self.router.get_leader_client(topic, partition).await?;
    let mut offset = 0i64;
    
    loop {
        let records = client.fetch(topic, partition, offset).await?;
        if records.is_empty() { break; }
        
        for record in records {
            // ⭐ STORE ORIGINAL OFFSET IN HEADER
            let mut headers = record.headers.clone();
            headers.insert(
                "x-original-offset".to_string(),
                record.offset.to_le_bytes().to_vec(),
            );
            headers.insert(
                "x-original-timestamp".to_string(),
                record.timestamp.to_le_bytes().to_vec(),
            );
            
            self.write_with_headers(&record, headers).await?;
            offset = record.offset + 1;
        }
    }
    
    Ok(())
}
```

**Restore Phase:**
```rust
// During RESTORE, extract headers and build mapping
pub async fn restore_header_based(
    &self,
    backup_id: &str,
    target_cluster: &KafkaClient,
) -> Result<OffsetMapping> {
    let mut offset_map = OffsetMapping::new();
    
    for segment in self.get_segments(backup_id).await? {
        let records = self.decompress_segment(segment)?;
        
        // Produce records to target
        let produce_response = target_cluster
            .produce_batch(records.clone())
            .await?;
        
        // Extract original offsets from headers
        // Build map: source_offset → (target_offset, timestamp)
        for (record, target_offset) in records.iter().zip(produce_response.offsets) {
            if let Some(orig_offset_bytes) = record.get_header("x-original-offset") {
                let source_offset = i64::from_le_bytes(
                    orig_offset_bytes.try_into()?
                );
                
                offset_map.add(
                    record.topic.clone(),
                    record.partition,
                    source_offset,
                    target_offset,
                );
            }
        }
    }
    
    Ok(offset_map)
}

// After restore, generate offset reset commands
pub async fn generate_offset_reset_script(
    &self,
    offset_map: &OffsetMapping,
) -> Result<String> {
    let mut script = String::new();
    
    for (topic_partition, mappings) in offset_map.iter() {
        for (source_offset, (target_offset, _timestamp)) in mappings {
            script.push_str(&format!(
                "kafka-consumer-groups --bootstrap-server {} \\\n",
                self.target_brokers[0]
            ));
            script.push_str(&format!(
                "  --group {} \\\n",
                topic_partition.group_id
            ));
            script.push_str(&format!(
                "  --topic {} --partition {} \\\n",
                topic_partition.topic, topic_partition.partition
            ));
            script.push_str(&format!(
                "  --to-offset {} --reset-offsets --execute\n\n",
                target_offset
            ));
        }
    }
    
    Ok(script)
}
```

**Pros:**
- ✅ Exact offset mapping preserved
- ✅ No consumer reprocessing
- ✅ Headers non-invasive (app can ignore)

**Cons:**
- ⚠️ Requires manual offset reset after restore
- ⚠️ Two-phase operation (restore, then reset)
- ⚠️ Timing window where old offsets are invalid

**Use case:** DR failover where you need exact positioning, willing to do manual offset reset.

---

### 3.3 Strategy 3: Timestamp-Based (Approximate Seeking)

**Problem:** "Find me the offset in target cluster that has approximately the same timestamp as source offset 750"

**Solution:** Use Kafka's built-in timestamp-based offset lookup

```rust
pub async fn map_offsets_timestamp_based(
    &self,
    source_manifest: &BackupManifest,
    target_cluster: &KafkaClient,
) -> Result<OffsetMapping> {
    let mut offset_map = OffsetMapping::new();
    
    // For each (topic, partition) in source backup
    for topic in &source_manifest.topics {
        for partition in &topic.partitions {
            // Find timestamp range in source
            let first_segment = partition.segments.first()
                .ok_or(anyhow!("No segments"))?;
            let source_timestamp = first_segment.start_timestamp;
            
            // Query target: "Give me the offset with timestamp >= source_timestamp"
            // This uses Kafka's OffsetsForTimesRequest API
            let target_offset = target_cluster
                .find_offset_by_timestamp(
                    &topic.name,
                    partition.partition_id,
                    source_timestamp,
                )
                .await?;
            
            offset_map.add_partition(
                topic.name.clone(),
                partition.partition_id,
                target_offset,
            );
        }
    }
    
    Ok(offset_map)
}

// Underlying API call (Kafka protocol: OffsetsForTimes)
pub async fn find_offset_by_timestamp(
    &self,
    topic: &str,
    partition: i32,
    timestamp: i64,
) -> Result<i64> {
    // Send OffsetsForTimesRequest to broker
    // Broker returns: offset N where record_timestamp >= requested_timestamp
    
    let request = OffsetsForTimesRequest {
        topics: vec![
            TopicPartition {
                topic: topic.to_string(),
                partitions: vec![
                    PartitionTimestamp {
                        partition,
                        timestamp,  // Usually set to: -1 = latest, -2 = earliest
                    }
                ],
            }
        ],
    };
    
    let response = self.send_request(request).await?;
    
    // Extract offset from response
    Ok(response.offsets[0].offset)
}
```

**Pros:**
- ✅ Automatic – no manual reset needed
- ✅ Works across different offset ranges
- ✅ Single operation (restore + set offsets)

**Cons:**
- ❌ Approximate – ±a few messages depending on timestamp distribution
- ❌ Sensitive to clock skew between clusters
- ❌ Assumes timestamp-ordered records (not always true)

**Use case:** Environment cloning where approximate positioning is acceptable, want minimal manual steps.

---

## Part 4: The Real Problem – Consumer Group State Explosion

### 4.1 What Actually Needs Remapping

When you restore from cluster A → cluster B, **3 things need to be remapped:**

```
1. DATA RECORDS
   Source offset 0-1000 → Target offsets 0-1000 (often)
   But target might already have data!
   Source offset 0-1000 → Target offsets 2500-3500 (actual)

2. CONSUMER GROUP OFFSETS (__consumer_offsets topic)
   Source: app1 group committed offset 750 for partition 0
   Target: app1 group has NO offset (new group)
   Action: Set app1 group offset to 750? NO! Wrong data!
           Set app1 group offset to 2750? YES! Points to correct message

3. OFFSET MAPPING FOR RECONCILIATION
   Source offset 750 = message with timestamp 10:45:00
   Target offset 2750 = message with timestamp 10:45:00
   Mapping: {750 → 2750} for manual verification
```

### 4.2 The Complete Offset Remapping Matrix

```rust
pub struct OffsetMapping {
    // (topic, partition) → [(source_offset, target_offset, timestamp), ...]
    pub mappings: HashMap<(String, i32), Vec<(i64, i64, i64)>>,
    
    // Consumer groups that existed in source
    pub consumer_groups: HashMap<String, GroupOffsets>,
}

pub struct GroupOffsets {
    pub group_id: String,
    // (topic, partition) → (source_committed_offset, target_committed_offset)
    pub partitions: HashMap<(String, i32), (i64, i64)>,
}

impl OffsetMapping {
    /// Build complete mapping from backup to target cluster
    pub async fn build(
        &self,
        backup_manifest: &BackupManifest,
        target_cluster: &KafkaClient,
        strategy: OffsetStrategy,
    ) -> Result<Self> {
        let mut mapping = OffsetMapping {
            mappings: HashMap::new(),
            consumer_groups: HashMap::new(),
        };
        
        // Step 1: Map record offsets
        for segment in self.all_segments(backup_manifest) {
            let records = self.decompress(segment)?;
            let produce_response = target_cluster.produce_batch(&records).await?;
            
            for (record, target_offset) in records.iter().zip(produce_response) {
                mapping.mappings
                    .entry((record.topic.clone(), record.partition))
                    .or_insert_with(Vec::new)
                    .push((record.offset, target_offset, record.timestamp));
            }
        }
        
        // Step 2: Map consumer group offsets (if available in backup)
        if let Some(consumer_offsets_backup) = backup_manifest.consumer_offsets_topic {
            for (group_id, group_data) in consumer_offsets_backup {
                let mut group_mapping = GroupOffsets {
                    group_id: group_id.clone(),
                    partitions: HashMap::new(),
                };
                
                for ((topic, partition), source_offset) in group_data {
                    // Find target offset that maps to this source offset
                    if let Some(target_offset) = self.lookup_target_offset(
                        &topic,
                        partition,
                        source_offset,
                    ) {
                        group_mapping.partitions.insert(
                            (topic, partition),
                            (source_offset, target_offset),
                        );
                    }
                }
                
                mapping.consumer_groups.insert(group_id, group_mapping);
            }
        }
        
        Ok(mapping)
    }
    
    /// Apply offset mapping to target cluster
    pub async fn apply(&self, target_cluster: &KafkaClient) -> Result<()> {
        for (group_id, group_data) in &self.consumer_groups {
            for ((topic, partition), (_, target_offset)) in &group_data.partitions {
                // Set consumer group offset via OffsetCommitRequest
                target_cluster.commit_offset(
                    group_id,
                    topic,
                    *partition,
                    *target_offset,
                ).await?;
            }
        }
        
        Ok(())
    }
}
```

---

## Part 5: The **BEST SOLUTION** – Kannika-Style Offset Preservation

This is what **Kannika does** and what you should implement:

### 5.1 Three-Phase Approach

```
PHASE 1: BACKUP
├─ For each record, store in header:
│  ├─ x-original-offset: i64
│  ├─ x-original-timestamp: i64
│  └─ x-original-partition: i32
└─ Also backup __consumer_offsets topic (if readable)

PHASE 2: RESTORE
├─ Decompress records (headers still intact)
├─ Produce to target cluster
├─ Target cluster assigns NEW offsets
├─ Headers preserve MAPPING info
└─ DO NOT touch __consumer_offsets yet

PHASE 3: OFFSET MAPPING & RESET (Manual or Auto)
├─ Read restored records (still have headers!)
├─ Build offset mapping table
├─ Generate offset reset commands
└─ Operator reviews & applies reset
    OR
    Auto-apply if trusted
```

### 5.2 Implementation: Three-Phase Restore

```rust
pub struct ThreePhaseRestore {
    backup_engine: BackupEngine,
    target_cluster: KafkaClient,
}

impl ThreePhaseRestore {
    /// Phase 1: Backup with header preservation
    pub async fn backup_phase(
        &self,
        source_cluster: &KafkaClient,
        backup_id: &str,
    ) -> Result<()> {
        for (topic, partition) in self.get_topics_partitions(source_cluster).await? {
            let mut offset = 0i64;
            let mut segment_writer = SegmentWriter::new(128 * 1024 * 1024);
            
            loop {
                let records = source_cluster
                    .fetch_partition(topic, partition, offset)
                    .await?;
                
                if records.is_empty() { break; }
                
                for record in records {
                    // ⭐ Phase 1: ADD OFFSET-MAPPING HEADERS
                    let mut enhanced = record.clone();
                    enhanced.headers.insert(
                        "x-original-offset".to_string(),
                        record.offset.to_le_bytes().to_vec(),
                    );
                    enhanced.headers.insert(
                        "x-original-timestamp".to_string(),
                        record.timestamp.to_le_bytes().to_vec(),
                    );
                    enhanced.headers.insert(
                        "x-source-cluster".to_string(),
                        "prod-us-east".as_bytes().to_vec(),
                    );
                    
                    segment_writer.add_record(&enhanced)?;
                    offset = record.offset + 1;
                }
            }
            
            segment_writer.flush(&self.backup_engine.storage).await?;
        }
        
        tracing::info!("Phase 1 (Backup) complete");
        Ok(())
    }
    
    /// Phase 2: Restore without touching offsets
    pub async fn restore_phase(&self, backup_id: &str) -> Result<RestoreReport> {
        let mut offset_map = OffsetMapping::new();
        let mut total_records = 0u64;
        
        for segment in self.backup_engine.get_segments(backup_id).await? {
            let data = self.backup_engine.storage.get(&segment.key).await?;
            let records = decompress_segment(&data)?;
            
            // ⭐ Phase 2: PRODUCE TO TARGET (headers preserved!)
            let produce_response = self.target_cluster
                .produce_batch(&records)
                .await?;
            
            // Build offset mapping as records arrive
            for (record, target_offset) in records.iter().zip(produce_response.offsets) {
                let source_offset = record.get_header("x-original-offset")
                    .map(|b| i64::from_le_bytes(b.try_into()?))
                    .transpose()?;
                
                if let Some(src_off) = source_offset {
                    offset_map.add(
                        record.topic.clone(),
                        record.partition,
                        src_off,
                        target_offset,
                    );
                }
                
                total_records += 1;
            }
        }
        
        tracing::info!("Phase 2 (Restore) complete: {} records", total_records);
        
        Ok(RestoreReport {
            records_restored: total_records,
            offset_mapping: offset_map,
            phase: 2,
        })
    }
    
    /// Phase 3: Generate offset reset plan
    pub async fn offset_mapping_phase(
        &self,
        offset_map: &OffsetMapping,
        strategy: OffsetMappingStrategy,
    ) -> Result<OffsetResetPlan> {
        match strategy {
            OffsetMappingStrategy::Manual => {
                // Just generate report, require manual review
                Ok(self.generate_offset_report(offset_map).await?)
            }
            OffsetMappingStrategy::Auto => {
                // Apply offset resets automatically
                let plan = self.generate_offset_report(offset_map).await?;
                self.apply_offset_resets(&plan).await?;
                Ok(plan)
            }
            OffsetMappingStrategy::Dry => {
                // Generate report but don't apply
                let report = self.generate_offset_report(offset_map).await?;
                println!("DRY RUN - Would reset {} consumer groups", report.groups.len());
                Ok(report)
            }
        }
    }
}
```

---

## Part 6: Updated PRD Recommendations

Update your Restore PRD with this **Consumer Offset Handling** section:

```yaml
## 8. Consumer Offset Handling – Definitive Solution

### 8.1 Always Use Three-Phase Restore

Phase 1: Backup
  ✅ Store x-original-offset header on all records
  ✅ Store x-original-timestamp header
  ✅ (Optional) Backup __consumer_offsets topic if readable

Phase 2: Restore
  ✅ Decompress segments
  ✅ Produce to target (offsets auto-assigned)
  ✅ Do NOT touch __consumer_offsets
  ✅ Collect offset mapping from responses

Phase 3: Offset Reset
  ✅ Generate offset reset plan from mapping
  ✅ For each consumer group: (topic, partition) → new_offset
  ✅ Manual review recommended
  ✅ Apply offset commits

### 8.2 Offset Mapping Strategies

| Strategy | Implementation | Review | Use Case |
|----------|-----------------|--------|----------|
| Manual | Generate report, require CLI reset | Required | DR – high control |
| Auto | Apply resets automatically | Optional | Trusted migration |
| Timestamp | Seek by timestamp | Limited | Approx position |
| Skip | No offset management | N/A | Test/dev only |

### 8.3 Safety Guarantees

✅ Records are restored with 100% fidelity (no loss/duplication)
✅ Original offsets NEVER conflict with target data
✅ Offset reset is reversible (can revert if needed)
✅ Consumer groups start from correct position
❌ DO NOT auto-reset without verification first
```

---

## Part 7: Code Checklist for Implementation

```rust
// ✅ MUST HAVE
// 1. Header-based offset preservation
record.headers.insert("x-original-offset", offset_bytes);
record.headers.insert("x-original-timestamp", timestamp_bytes);

// 2. Offset mapping collection
offset_map[(topic, partition)] = Vec::of((source_offset, target_offset));

// 3. Three-phase separation
Phase::Backup → collect headers
Phase::Restore → produce and map
Phase::OffsetReset → apply to consumer groups

// 4. Consumer group reset (via OffsetCommitRequest)
kafka_admin.offset_commit(
    group_id,
    [(topic, partition) → new_offset]
);

// 5. Validation
assert_eq!(record_count_source, record_count_target);
assert!(all(src_offset_mapped));
```

---

## Summary Table: Offset Strategies vs. Your Requirements

| Requirement | Skip | Header | Timestamp | Manual |
|-------------|------|--------|-----------|--------|
| **No data duplication** | ✅ | ✅ | ✅ | ✅ |
| **No data loss** | ✅ | ✅ | ✅ | ✅ |
| **Exact offset recovery** | ❌ | ✅ | ⚠️ | ✅ |
| **Automatic reset** | ✅ | ❌ | ✅ | ❌ |
| **Safe for production** | ⚠️ | ✅ | ✅ | ✅ |
| **Complexity** | Low | Medium | Medium | Medium |
| **Recommended for DR** | ❌ | ✅ | ⚠️ | ✅ |

---

## Final Answer to Your Question

**"Is this concerning?"**

Yes and no:

✅ **NOT concerning because:**
- Kafka protocol is *designed* for this (brokers always assign offsets)
- Solution is well-known and implemented by Kannika, Confluent, etc.
- Three-phase approach is bulletproof

❌ **Concerning if you:**
- Ignore offset remapping (data will be wrong)
- Auto-reset offsets without validation (duplicate processing)
- Assume offsets transfer automatically (they don't)

**Recommendation for OSO:**
1. Implement **three-phase restore** with header preservation
2. Make offset mapping **explicit and auditable**
3. Require **manual review** for production (or explicit --auto-apply flag)
4. Generate **detailed offset mapping reports** for reconciliation
5. Support all three strategies (Skip, HeaderBased, TimestampBased)

This is exactly what you've already spec'd in your Restore PRD – you just needed the protocol-level understanding to know *why* it's necessary.

