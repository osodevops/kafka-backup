# OSO Kafka Backup & Restore – Restore PRD (v1)

**Point-in-Time Restore with Consumer Offset Management**

---

## Table of Contents

1. [Product Summary](#1-product-summary)
2. [Goals & Requirements](#2-goals--requirements)
3. [Core Restore Use Cases](#3-core-restore-use-cases)
4. [Restore Modes](#4-restore-modes)
5. [Architecture Overview](#5-architecture-overview)
6. [Restore Engine Design](#6-restore-engine-design)
7. [Point-in-Time Restore (PITR)](#7-point-in-time-restore-pitr)
8. [Consumer Offset Handling](#8-consumer-offset-handling)
9. [Restore Safety & Constraints](#9-restore-safety--constraints)
10. [Configuration](#10-configuration)
11. [CLI & UX](#11-cli--ux)
12. [Error Handling & Recovery](#12-error-handling--recovery)
13. [Milestones](#13-milestones)

---

## 1. Product Summary

The **Restore Engine** is a high-performance, feature-complete recovery system for restoring backed-up Kafka topics with **full control**, **point-in-time semantics**, and **intelligent consumer offset management**.

It enables users to:
- Restore entire topics or filtered subsets with timestamp precision
- Restore to the same cluster (disaster recovery) or different clusters (migration, environment cloning)
- Preserve or remap consumer group offsets based on business requirements
- Validate data integrity before committing to target cluster
- Resume interrupted restores without re-processing all data

**Key Differentiators from Kannika:**
- ✅ **Point-in-time restore with timestamp precision** (down to millisecond)
- ✅ **Three consumer offset strategies** (header-based, timestamp-based, manual mapping)
- ✅ **Dry-run mode** to validate before production
- ✅ **Partial restore** (topic/partition filtering, time windows)
- ✅ **Environment cloning** with optional schema ID remapping
- ✅ **Resumable restores** (track progress, resume after failure)
- ✅ **Comprehensive metrics** (records restored, latencies, offsets mapped)

---

## 2. Goals & Requirements

### 2.1 Functional Goals

**MUST:**
- Restore all records from a backup with exact fidelity (no data loss, no duplication)
- Support time-windowed restore: `--from-ts` and `--to-ts` with millisecond precision
- Support topic/partition filtering during restore
- Support partition remapping: restore `topic-A/0` → `topic-B/1`
- Support both same-cluster (DR) and cross-cluster (migration) restore
- Preserve original message structure: key, value, headers, timestamp, offset

**SHOULD:**
- Support resumable restores (track which segments completed, resume from checkpoint)
- Support dry-run mode (validate without writing to target)
- Support consumer group offset reset strategies
- Support schema ID remapping for environment cloning
- Provide detailed metrics and progress reporting
- Support parallel restore across multiple partitions

### 2.2 Non-Functional Goals

- **Throughput**: 100+ MB/s restore speed (same as backup)
- **Latency**: <100ms for single-record seek operations
- **Memory**: <500MB for 4 concurrent partition restores
- **Reliability**: At-least-once delivery (safe to re-run)
- **Safety**: Never overwrite consumer offsets in `__consumer_offsets` without explicit opt-in

---

## 3. Core Restore Use Cases

### Use Case 1: Disaster Recovery – Same Cluster

**Scenario:** Topic `orders` was accidentally truncated. Restore from backup.

```bash
kafka-backup restore \
  --backup-id backup-2025-11-27-prod \
  --source-topic orders \
  --target-topic orders \
  --target-cluster bootstrap.kafka:9092 \
  --consumer-group-strategy skip  # Don't touch offsets, just replay data
```

**Outcome:** Records restored to `orders` topic, consumer groups pick up from existing committed offsets.

### Use Case 2: Point-in-Time Recovery – Corruption Detection

**Scenario:** Data corruption detected at 2025-11-27 15:30:00 UTC. Restore data from 15:00:00.

```bash
kafka-backup restore \
  --backup-id backup-2025-11-27-prod \
  --source-topic transactions \
  --target-topic transactions-recovered \
  --target-cluster bootstrap.kafka:9092 \
  --from-timestamp "2025-11-27T15:00:00Z" \
  --to-timestamp "2025-11-27T15:30:00Z" \
  --dry-run  # Validate first
```

**Outcome:** Only records between 15:00-15:30 UTC restored to new topic; consumers can validate before switching traffic.

### Use Case 3: Environment Cloning – New Test Environment

**Scenario:** Clone production data into staging environment for load testing.

```bash
kafka-backup restore \
  --backup-id backup-2025-11-27-prod \
  --source-topics "orders,payments,users" \
  --target-cluster staging.kafka:9092 \
  --target-topics "orders-clone,payments-clone,users-clone" \
  --consumer-group-strategy timestamp-based  # Approximate seek
  --dry-run
```

**Outcome:** Data cloned to staging cluster; consumer groups created and seeked to approximately same position as production.

### Use Case 4: Cross-Cluster Migration

**Scenario:** Migrate Kafka from on-prem to Confluent Cloud.

```bash
kafka-backup restore \
  --backup-id backup-2025-11-27-prod \
  --source-topics ".*"  # All topics matching regex
  --target-cluster "pkc-12345.us-east-1.provider.confluent.cloud:9092" \
  --target-topics-rewrite "s/^/prod-/"  # Prefix all restored topics
  --consumer-group-strategy header-based  # Use original offsets from headers
```

**Outcome:** All topics migrated with original consumer offsets available in message headers.

### Use Case 5: Partition-Level Restore

**Scenario:** Partition 0 of `high-volume-topic` is corrupted; restore just that partition.

```bash
kafka-backup restore \
  --backup-id backup-2025-11-27-prod \
  --source-topic high-volume-topic \
  --source-partitions 0 \
  --target-cluster bootstrap.kafka:9092 \
  --consumer-group-strategy skip  # Offsets managed externally
```

**Outcome:** Only partition 0 restored; other partitions unaffected.

---

## 4. Restore Modes

### 4.1 Full Restore
Restore entire topic(s) from backup start to end.

```bash
kafka-backup restore --backup-id ID --source-topic TOPIC --target-cluster BROKERS
```

### 4.2 Time-Windowed Restore (PITR)
Restore records within a specific timestamp range.

```bash
kafka-backup restore \
  --backup-id ID \
  --source-topic TOPIC \
  --from-timestamp "2025-11-27T10:00:00Z" \
  --to-timestamp "2025-11-27T11:00:00Z"
```

### 4.3 Filtered Restore
Restore subset of topics and/or partitions.

```bash
kafka-backup restore \
  --backup-id ID \
  --source-topics "topic-1,topic-2,topic-.*" \
  --source-partitions "0,1,2"  # Only these partitions
```

### 4.4 Remapped Restore
Restore to different topic/partition layout.

```bash
kafka-backup restore \
  --backup-id ID \
  --source-topic orders \
  --target-topic orders-v2 \
  --partition-mapping "0:0,1:2,2:4"  # src:dest partition mapping
```

### 4.5 Dry-Run Mode
Validate restore without writing to target cluster.

```bash
kafka-backup restore --backup-id ID --dry-run
```

Outputs:
- Segment count and total records
- Time range coverage
- Consumer group offsets that would be set
- Any validation errors

### 4.6 Resumable Restore
Restore large backups in stages with progress checkpointing.

```bash
kafka-backup restore \
  --backup-id ID \
  --checkpoint-state resume-state.json  # Load prior state
  --checkpoint-interval 60s  # Save progress every 60 seconds
```

---

## 5. Architecture Overview

### 5.1 Restore Engine Components

```
┌─────────────────────────────────────────┐
│   CLI / REST API                        │
│  - Restore configuration                │
│  - Progress monitoring                  │
└──────────────┬──────────────────────────┘
               │
┌──────────────▼──────────────────────────┐
│   Restore Coordinator                   │
│  - Segment selection (PITR filtering)   │
│  - Partition mapping                    │
│  - Offset strategy selection            │
│  - Resume checkpointing                 │
└──────────────┬──────────────────────────┘
               │
    ┌──────────┴──────────┐
    ▼                     ▼
┌──────────────┐   ┌──────────────────┐
│ Storage Read │   │ Offset Mapping   │
│ - List files │   │ - Header-based   │
│ - Decompress │   │ - Timestamp-base │
│ - Stream     │   │ - Cluster scan   │
└──────────────┘   └──────────────────┘
    │                     │
    └──────────────┬──────┘
                   ▼
        ┌────────────────────────┐
        │  Restore Worker Pool   │
        │  (Partition-Level)     │
        │  - Fetch from storage  │
        │  - Validate records    │
        │  - Produce to target   │
        │  - Track progress      │
        └────────────────────────┘
               │
               ▼
        ┌──────────────────────┐
        │  Target Kafka        │
        │  (Produce API)       │
        └──────────────────────┘
```

### 5.2 Data Flow – Full Restore

```
1. Load restore config (backup ID, time window, filters)
2. Load manifest from backup:
   - List all segments
   - Parse segment metadata (timestamps, offsets)
3. Filter segments based on:
   - Time window (from_ts, to_ts)
   - Topic/partition selection
   - Only include overlapping segments
4. For EACH SELECTED SEGMENT (parallel):
   a. Fetch from storage (S3/Azure/GCS)
   b. Decompress (zstd)
   c. For EACH RECORD:
      - Check timestamp against window
      - Validate key/value/headers
      - Optionally add/rewrite offset header
      - Batch into producer batch (50KB)
   d. Produce batch to target partition
5. After all segments processed:
   a. Generate offset mapping report
   b. Handle consumer group offset reset
   c. Report completion statistics

Result: All records in time window restored to target
```

### 5.3 Data Flow – Offset Mapping

```
1. Load backup manifest
2. Scan through ALL records (or sample) to track:
   - Original offset ranges per topic/partition
   - Original timestamps per offset
3. Determine offset mapping:
   - Source partition 0, offset 1000 → [messages with timestamps 10:00-10:05]
   - Target partition 0, offset ??? → [same messages after restore]
4. Select strategy:
   - Header-based: Store original offset in message header
   - Timestamp-based: Use Kafka API to find offset by timestamp
   - Cluster scan: Query target __consumer_offsets directly
   - Manual: Report mapping, let user reset with CLI
```

---

## 6. Restore Engine Design

### 6.1 Core Components

```rust
/// Restore configuration
pub struct RestoreConfig {
    pub backup_id: String,
    pub source_topics: Vec<String>,  // Regex patterns or exact names
    pub target_topics: Option<Vec<String>>,  // If different from source
    pub source_partitions: Option<Vec<i32>>,  // If filtering
    pub partition_mapping: Option<HashMap<i32, i32>>,  // src:dst
    
    // Time window for PITR
    pub from_timestamp: Option<i64>,  // epoch millis
    pub to_timestamp: Option<i64>,    // epoch millis
    
    // Target cluster
    pub target_bootstrap_servers: Vec<String>,
    pub target_security: KafkaSecurityConfig,
    
    // Restore strategy
    pub consumer_group_strategy: OffsetStrategy,
    pub dry_run: bool,
    pub resume_checkpoint: Option<String>,
    
    // Performance tuning
    pub max_concurrent_partitions: usize,
    pub produce_batch_size_bytes: usize,
    pub checkpoint_interval_secs: u64,
}

/// Consumer offset handling strategy
#[derive(Clone, Copy)]
pub enum OffsetStrategy {
    /// Skip offset handling entirely (just restore data)
    Skip,
    /// Store original offset in message header
    HeaderBased,
    /// Use timestamp-based seeking in target cluster
    TimestampBased,
    /// Scan target cluster and auto-map offsets
    ClusterScan,
    /// Report mapping, require manual reset
    Manual,
}

/// Main restore engine
pub struct RestoreEngine {
    storage: Arc<dyn StorageBackend>,
    target_client: KafkaClient,
    manifest: BackupManifest,
    offset_tracker: OffsetTracker,
}

impl RestoreEngine {
    pub async fn restore(&self, config: &RestoreConfig) -> Result<RestoreReport> {
        // 1. Load and filter segments
        let segments = self.filter_segments(config)?;
        
        // 2. If dry-run, validate and return without writing
        if config.dry_run {
            return self.validate_restore(&segments, config);
        }
        
        // 3. Prepare offset mapping
        let offset_map = self.prepare_offset_mapping(config, &segments).await?;
        
        // 4. Create restore tasks for each partition
        let mut tasks = vec![];
        for segment in segments {
            let task = self.restore_segment(&segment, config, &offset_map);
            tasks.push(task);
        }
        
        // 5. Execute in parallel
        let results = futures::future::join_all(tasks).await;
        
        // 6. Handle any failures
        for result in &results {
            result?;
        }
        
        // 7. Report results
        Ok(self.generate_report(&results, &offset_map))
    }
    
    async fn restore_segment(
        &self,
        segment: &SegmentMetadata,
        config: &RestoreConfig,
        offset_map: &OffsetMapping,
    ) -> Result<SegmentRestoreResult> {
        // Fetch segment from storage
        let data = self.storage.get(&segment.key).await?;
        let decompressed = zstd::decode_all(&data[..])?;
        
        // Parse and validate records
        let mut batch = Vec::new();
        let mut restored_count = 0;
        
        for record in parse_records(&decompressed) {
            // Apply PITR filter
            if !self.in_time_window(&record, config) {
                continue;
            }
            
            // Apply partition filter
            if !self.matches_partition_filter(&record, config) {
                continue;
            }
            
            // Optionally add offset header
            let record_with_header = self.add_offset_header(record, offset_map)?;
            
            batch.push(record_with_header);
            restored_count += 1;
            
            // Flush batch when full
            if batch.len() >= config.produce_batch_size_bytes {
                self.produce_batch(&batch, config).await?;
                batch.clear();
            }
        }
        
        // Flush remaining
        if !batch.is_empty() {
            self.produce_batch(&batch, config).await?;
        }
        
        Ok(SegmentRestoreResult {
            segment_key: segment.key.clone(),
            records_restored: restored_count,
            duration_ms: 0,
        })
    }
}
```

### 6.2 Segment Selection (PITR Logic)

```rust
pub fn filter_segments(
    &self,
    config: &RestoreConfig,
) -> Result<Vec<SegmentMetadata>> {
    let mut result = vec![];
    
    for topic in &self.manifest.topics {
        // Check if topic matches filter
        if !self.matches_topic_filter(&topic.name, config) {
            continue;
        }
        
        for partition in &topic.partitions {
            // Check if partition matches filter
            if let Some(partitions) = &config.source_partitions {
                if !partitions.contains(&partition.partition_id) {
                    continue;
                }
            }
            
            for segment in &partition.segments {
                // Check if segment overlaps with time window
                if !self.segment_overlaps_time_window(segment, config) {
                    continue;
                }
                
                result.push(segment.clone());
            }
        }
    }
    
    Ok(result)
}

fn segment_overlaps_time_window(
    &self,
    segment: &SegmentMetadata,
    config: &RestoreConfig,
) -> bool {
    let seg_start = segment.start_timestamp;
    let seg_end = segment.end_timestamp;
    
    let from_ts = config.from_timestamp.unwrap_or(i64::MIN);
    let to_ts = config.to_timestamp.unwrap_or(i64::MAX);
    
    // Segment overlaps window if:
    // seg_start <= window_end AND seg_end >= window_start
    seg_start <= to_ts && seg_end >= from_ts
}
```

### 6.3 Offset Mapping Strategies

#### Strategy 1: Header-Based (Kannika-Compatible)

```rust
pub async fn map_offsets_header_based(
    &self,
    segments: &[SegmentMetadata],
) -> Result<OffsetMapping> {
    let mut mapping = OffsetMapping::new();
    
    // During backup, original offset is stored in header
    // During restore, we extract it and make it available
    
    for segment in segments {
        let data = self.storage.get(&segment.key).await?;
        
        for record in parse_records(&data) {
            // Original offset is in header "x-original-offset"
            if let Some(offset_header) = record.get_header("x-original-offset") {
                let original_offset = i64::from_le_bytes(offset_header.try_into()?);
                
                // Map: (topic, partition, original_offset) -> message_timestamp
                mapping.add(
                    record.topic.clone(),
                    record.partition,
                    original_offset,
                    record.timestamp,
                );
            }
        }
    }
    
    Ok(mapping)
}
```

#### Strategy 2: Timestamp-Based (Approximate Seeking)

```rust
pub async fn map_offsets_timestamp_based(
    &self,
    target_client: &KafkaClient,
    source_manifest: &BackupManifest,
) -> Result<OffsetMapping> {
    let mut mapping = OffsetMapping::new();
    
    for topic in &source_manifest.topics {
        for partition in &topic.partitions {
            // Find first and last timestamp in source
            let first_ts = partition.segments
                .iter()
                .map(|s| s.start_timestamp)
                .min()
                .unwrap_or(0);
            
            let last_ts = partition.segments
                .iter()
                .map(|s| s.end_timestamp)
                .max()
                .unwrap_or(0);
            
            // Query target cluster: which offset has timestamp ~= first_ts?
            let target_offset = target_client
                .find_offset_by_timestamp(
                    &topic.name,
                    partition.partition_id,
                    first_ts,
                )
                .await?;
            
            mapping.add_partition(
                topic.name.clone(),
                partition.partition_id,
                target_offset,
            );
        }
    }
    
    Ok(mapping)
}
```

#### Strategy 3: Cluster Scan (Manual Post-Restore)

```rust
pub async fn generate_offset_report(
    &self,
    target_client: &KafkaClient,
    mapping: &OffsetMapping,
) -> Result<OffsetResetReport> {
    // Query __consumer_offsets from target cluster
    // Report which consumer groups need reset and to what offset
    
    let mut report = OffsetResetReport::new();
    
    for (topic_partition, recommended_offset) in mapping.iter() {
        report.add_recommendation(
            topic_partition,
            recommended_offset,
            "Recommended offset after restore",
        );
    }
    
    Ok(report)
}
```

---

## 7. Point-in-Time Restore (PITR)

### 7.1 Timestamp Semantics

**Definition:** Restore all records with `record.timestamp >= from_ts AND record.timestamp <= to_ts`

**Precision:** Millisecond-level (epoch milliseconds)

**Order Guarantee:** Records restored in original order (by offset within partition)

### 7.2 Segment-Level Filtering

```
Segment 1: start_ts=10:00:00, end_ts=10:05:00
Segment 2: start_ts=10:05:01, end_ts=10:10:00
Segment 3: start_ts=10:10:01, end_ts=10:15:00

PITR Request: from=10:03:00, to=10:12:00

Result: Include segments 1, 2 (partial), 3 (partial)
        At record level, filter by timestamp within window
```

### 7.3 Edge Cases

**Case 1: Empty time window**
```bash
--from-timestamp 10:15:00 --to-timestamp 10:14:00  # from > to
# Error: Invalid time window
```

**Case 2: Time window before backup data**
```bash
# Backup: 2025-11-27 10:00:00 to 15:00:00
--from-timestamp 2025-11-26 10:00:00  # Before backup started
# Result: No segments selected, restore 0 records
```

**Case 3: Time window spans multiple days**
```bash
--from-timestamp 2025-11-26T10:00:00Z --to-timestamp 2025-11-28T10:00:00Z
# Works: Selects all segments in range
```

---

## 8. Consumer Offset Handling

### 8.1 Strategy Comparison

| Strategy | Use Case | Implementation | Trade-off |
|----------|----------|-----------------|-----------|
| **Skip** | Replay data, existing offsets OK | None | Consumer lag increases |
| **Header-Based** | Exact offset recovery (if available) | Store original offset | Requires app support |
| **Timestamp-Based** | Approximate recovery | Query by timestamp | Not exact, ~millisecond precision |
| **Cluster Scan** | Post-restore offset mapping | Query __consumer_offsets | Manual reset required |
| **Manual** | Maximum control | Report only | Operator-driven |

### 8.2 Header-Based Strategy (Recommended)

During **backup**, store original offset:
```json
{
  "key": "order-123",
  "value": {...},
  "headers": {
    "x-original-offset": 1000,
    "x-original-timestamp": 1732696800000
  }
}
```

During **restore**, extract and make available:
```bash
kafka-backup show-offset-mapping \
  --backup-id backup-2025-11-27 \
  --format json > offsets.json

# Output:
# {
#   "orders/0": {"source_offset": 1000, "restored_timestamp": ...},
#   "orders/1": {"source_offset": 500, "restored_timestamp": ...},
# }

# Then reset consumer group offsets using CLI
kafka-consumer-groups --bootstrap-server ... \
  --group my-group \
  --topic orders:0 \
  --to-offset 1000 \
  --reset-offsets --execute
```

### 8.3 Timestamp-Based Strategy

```bash
# Find offset in target cluster that corresponds to timestamp
kafka-restore-offset-by-timestamp \
  --bootstrap-server target.kafka:9092 \
  --topic orders \
  --partition 0 \
  --timestamp 1732696800000  # Same as backup time
  
# Output: offset 2500 in target cluster corresponds to ~10:00:00

# Reset consumer group
kafka-consumer-groups --bootstrap-server ... \
  --group my-group \
  --topic orders:0 \
  --to-offset 2500 \
  --reset-offsets --execute
```

### 8.4 Never Auto-Reset Without Consent

**Golden Rule:** Restore engine NEVER automatically modifies `__consumer_offsets` without explicit flag.

```bash
# Safe (default): Restore data only, offsets unchanged
kafka-backup restore --backup-id ID --source-topic topic

# Explicit consent required to reset offsets
kafka-backup restore \
  --backup-id ID \
  --source-topic topic \
  --reset-consumer-offsets  # Dangerous flag, must be explicit
  --consumer-group my-group \
  --offset-strategy timestamp-based
```

---

## 9. Restore Safety & Constraints

### 9.1 Safety Principles

**CONSTRAINT 1: No Automatic Consumer Group Modification**
- By default, restore does NOT touch `__consumer_offsets`
- Consumer groups must be reset manually or with explicit opt-in
- Prevents accidental data loss from incorrect offset resets

**CONSTRAINT 2: Always Restore to Different Topic If Uncertain**
```bash
# SAFE: Restore to new topic first
kafka-backup restore \
  --source-topic orders \
  --target-topic orders-restored  # Different topic
  
# Then validate
kafka-console-consumer --topic orders-restored --from-beginning

# Then switch traffic or reset original topic offsets
```

**CONSTRAINT 3: Dry-Run Mode for Validation**
```bash
# ALWAYS dry-run first for critical restores
kafka-backup restore \
  --backup-id backup-2025-11-27 \
  --source-topic critical-topic \
  --dry-run  # Validate without writing

# Review report, then run actual restore
```

**CONSTRAINT 4: Partition Immutability During Restore**
- Cannot change partition count during restore
- Partition mapping must be 1:1 or filtered
- Repartitioning requires separate step

### 9.2 Validation Checks

```rust
pub fn validate_restore_config(config: &RestoreConfig) -> Result<()> {
    // Check 1: Time window sanity
    if let (Some(from), Some(to)) = (config.from_timestamp, config.to_timestamp) {
        if from > to {
            return Err(anyhow!("from_timestamp ({}) > to_timestamp ({})", from, to));
        }
    }
    
    // Check 2: Partition mapping consistency
    if let Some(mapping) = &config.partition_mapping {
        for (src, dst) in mapping {
            if src < 0 || dst < 0 {
                return Err(anyhow!("Invalid partition number: src={}, dst={}", src, dst));
            }
        }
    }
    
    // Check 3: Target cluster connectivity
    // (Will fail when restore starts, but good to verify early)
    
    // Check 4: Backup existence and accessibility
    // (Verify backup manifest can be loaded)
    
    Ok(())
}
```

---

## 10. Configuration

### 10.1 YAML Configuration Example

```yaml
# restore-config.yaml
restore:
  backup_id: "backup-2025-11-27-prod"
  
  source:
    topics:
      - "orders"
      - "payments"
      - "inventory-.*"  # Regex pattern
    partitions:
      - 0
      - 1
    # Optional: Time window for PITR
    from_timestamp: "2025-11-27T10:00:00Z"
    to_timestamp: "2025-11-27T11:00:00Z"
  
  target:
    bootstrap_servers:
      - "target-broker-1:9092"
      - "target-broker-2:9092"
    security:
      sasl_mechanism: "plain"
      sasl_username: "restore-user"
      sasl_password: "${KAFKA_RESTORE_PASSWORD}"
      security_protocol: "SASL_SSL"
    
    # Optional: Different topic names
    topics:
      - "orders-restored"
      - "payments-restored"
      - "inventory-.*-restored"
    
    # Optional: Partition mapping (src:dst)
    partition_mapping:
      orders: "0:0,1:2,2:4"  # Repartition
  
  restore_strategy:
    consumer_group_strategy: "header-based"  # skip | header-based | timestamp-based | cluster-scan | manual
    dry_run: false
    
    # Optional: Offset reset (requires explicit consent)
    reset_consumer_offsets: false
    reset_strategy: "header-based"
  
  performance:
    max_concurrent_partitions: 4
    produce_batch_size_bytes: 52428800  # 50MB
    checkpoint_interval_secs: 60
```

### 10.2 CLI Examples

```bash
# Full restore to new topic
kafka-backup restore \
  --backup-id backup-2025-11-27 \
  --source-topic orders \
  --target-topic orders-restored \
  --target-cluster bootstrap.kafka:9092

# Point-in-time restore (last 1 hour)
kafka-backup restore \
  --backup-id backup-2025-11-27 \
  --source-topic orders \
  --from-timestamp "2025-11-27T10:00:00Z" \
  --to-timestamp "2025-11-27T11:00:00Z"

# Dry-run validation
kafka-backup restore \
  --backup-id backup-2025-11-27 \
  --dry-run \
  --format json > restore-plan.json

# Restore with offset mapping report
kafka-backup restore \
  --backup-id backup-2025-11-27 \
  --source-topic orders \
  --consumer-group-strategy header-based \
  --offset-report offsets.json  # Output file

# Resume interrupted restore
kafka-backup restore \
  --backup-id backup-2025-11-27 \
  --checkpoint-state restore-checkpoint.json
```

---

## 11. CLI & UX

### 11.1 Restore Commands

```bash
# List available backups
kafka-backup list-backups

# Show backup contents
kafka-backup describe --backup-id ID

# Show backup manifest
kafka-backup show-manifest --backup-id ID

# Validate restore config
kafka-backup validate-restore --config restore-config.yaml

# Execute restore (with progress)
kafka-backup restore --config restore-config.yaml --progress

# Check restore status
kafka-backup restore-status --backup-id ID

# Cancel restore in progress
kafka-backup restore-cancel --backup-id ID

# Show offset mapping after restore
kafka-backup show-offset-mapping --backup-id ID --format json
```

### 11.2 Progress Output Example

```
Restoring backup: backup-2025-11-27-prod
├─ Segments selected: 156
├─ Total records: 45,234,567
├─ Time range: 2025-11-27 10:00:00 to 11:00:00 UTC
│
├─ Segment [1/156] orders/0
│  ├─ Progress: ████████░░ 80% (12.8 MB / 16 MB)
│  ├─ Records: 2.5M / 3.1M
│  └─ Elapsed: 45s
│
├─ Segment [2/156] orders/1
│  ├─ Progress: ███░░░░░░░ 30% (4.8 MB / 16 MB)
│  ├─ Records: 1.0M / 3.2M
│  └─ Elapsed: 15s
│
└─ Overall
   ├─ Progress: ████████░░ 74% (235 MB / 320 MB)
   ├─ Throughput: 95.2 MB/s
   ├─ ETA: 54s
   └─ Total records restored: 15,234,567 / 45,234,567
```

---

## 12. Error Handling & Recovery

### 12.1 Resumable Restore with Checkpoints

```rust
pub struct RestoreCheckpoint {
    pub backup_id: String,
    pub start_time: i64,
    pub last_checkpoint_time: i64,
    pub segments_completed: Vec<String>,
    pub segments_in_progress: Vec<(String, u64)>,  // key, bytes_processed
    pub records_restored: u64,
}

impl RestoreEngine {
    pub async fn save_checkpoint(&self, checkpoint: &RestoreCheckpoint) -> Result<()> {
        let json = serde_json::to_string(checkpoint)?;
        tokio::fs::write("restore-checkpoint.json", json).await?;
        Ok(())
    }
    
    pub async fn resume_from_checkpoint(
        &self,
        checkpoint_path: &str,
    ) -> Result<RestoreCheckpoint> {
        let json = tokio::fs::read_to_string(checkpoint_path).await?;
        Ok(serde_json::from_str(&json)?)
    }
}
```

### 12.2 Common Error Scenarios

| Error | Cause | Recovery |
|-------|-------|----------|
| **Segment not found** | Backup corrupted or incomplete | Validate manifest, retry with different time window |
| **Target cluster unreachable** | Network issue | Check connectivity, retry |
| **Produce timeout** | Target cluster overloaded | Increase batch timeout or reduce concurrency |
| **Partition count mismatch** | Target topic has wrong partition count | Delete and recreate target topic, or use remapping |
| **Offset out of range** | Attempting to seek beyond log end | Validate offset mapping, check time window |

---

## 13. Milestones

### Milestone 1: Core Restore Engine (Weeks 1-3)

**Deliverables:**
- Restore coordinator with segment selection
- Basic restore to target cluster (no filtering)
- Manifest parsing and validation
- Produce API integration

**Success criteria:**
- Restore full backup to target
- 50+ MB/s throughput
- All records arrive in order

### Milestone 2: PITR + Filtering (Weeks 4-5)

**Deliverables:**
- Time-window filtering (from_ts, to_ts)
- Topic and partition filtering
- Segment-level prefiltering (skip non-overlapping)
- Dry-run mode

**Success criteria:**
- PITR accurate to millisecond precision
- Dry-run reports match actual restore
- <100ms per dry-run check

### Milestone 3: Offset Management (Weeks 6-7)

**Deliverables:**
- Header-based offset extraction
- Timestamp-based offset mapping
- Cluster scan strategy
- Offset reset CLI

**Success criteria:**
- Offsets preserved with header strategy
- Timestamp-based seeking ±100ms accuracy
- Clear reporting of offset mappings

### Milestone 4: Partition Remapping (Week 8)

**Deliverables:**
- Partition mapping configuration
- Partition-level filtering
- Topic name remapping

**Success criteria:**
- Remap partitions 1:1, N:1, M:N
- Validate partition constraints

### Milestone 5: Resumable Restores (Weeks 9-10)

**Deliverables:**
- Checkpoint save/restore
- Resume from failure point
- Segment-level progress tracking

**Success criteria:**
- Restore 1TB+ backup in stages
- Resume after network failure
- No duplicate processing

### Milestone 6: Production Hardening (Weeks 11-12)

**Deliverables:**
- Comprehensive error handling
- Circuit breakers
- Load testing
- Documentation

**Success criteria:**
- Tested with 1TB+ backups
- Handles all error scenarios gracefully
- Production-ready deployment docs

---

## Glossary

| Term | Definition |
|------|-----------|
| **PITR** | Point-in-Time Restore - recover data from a specific timestamp |
| **Offset Strategy** | Method for mapping consumer group offsets during restore |
| **Dry-Run** | Validation without writing to target cluster |
| **Segment Overlap** | When backup segment timestamp range intersects with restore time window |
| **Consumer Group Reset** | Setting consumer group offsets to resume from specific position |
| **Partition Mapping** | Translation of source partition numbers to target partition numbers |
| **Header-Based Restore** | Using original offset stored in message header to find position in target cluster |
| **Timestamp-Based Seeking** | Using Kafka API to find offset by timestamp |
| **Resumable Restore** | Checkpoint-based restore that can continue after interruption |

