# Kafka Backup Restore Guide

A comprehensive guide to restoring Kafka topics from backups with full control over timing, filtering, offset management, and recovery options.

---

## Table of Contents

1. [Overview](#overview)
2. [Restore Modes](#restore-modes)
3. [Configuration Reference](#configuration-reference)
4. [Point-in-Time Restore](#point-in-time-restore)
5. [Topic and Partition Filtering](#topic-and-partition-filtering)
6. [Consumer Offset Management](#consumer-offset-management)
7. [Resumable Restores](#resumable-restores)
8. [Dry-Run Validation](#dry-run-validation)
9. [CLI Commands](#cli-commands)
10. [Use Cases](#use-cases)
11. [Best Practices](#best-practices)
12. [Troubleshooting](#troubleshooting)

---

## Overview

The Kafka Backup Restore Engine enables recovery of backed-up topics with:

- **Point-in-Time Restore (PITR)**: Recover data from any timestamp with millisecond precision
- **Flexible Filtering**: Restore specific topics, partitions, or time windows
- **Offset Management**: Multiple strategies for handling consumer group offsets
- **Resumable Operations**: Checkpoint-based restore for handling large datasets or interruptions
- **Safety Features**: Dry-run validation, offset protection, and topic remapping

### Key Differentiators

| Feature | Description |
|---------|-------------|
| PITR | Millisecond-precision timestamp filtering |
| Partition Remapping | Restore to different partition layouts |
| Offset Strategies | 5 different consumer offset handling strategies |
| Dry-Run Mode | Validate before writing to target |
| Checkpointing | Resume after failures without re-processing |
| Pattern Matching | Glob and regex topic filtering |

---

## Restore Modes

### Full Restore

Restore all data from a backup:

```yaml
mode: restore
backup_id: "backup-2025-01-15"

target:
  bootstrap_servers:
    - localhost:9092
  topics:
    include:
      - "*"  # All topics

storage:
  backend: filesystem
  path: /var/lib/kafka-backup/data
```

### Time-Windowed Restore (PITR)

Restore records within a specific time range:

```yaml
restore:
  time_window_start: 1705312800000  # 2024-01-15 10:00:00 UTC
  time_window_end: 1705316400000    # 2024-01-15 11:00:00 UTC
```

### Filtered Restore

Restore specific topics and/or partitions:

```yaml
target:
  topics:
    include:
      - orders
      - "payments-*"
    exclude:
      - "*-test"

restore:
  source_partitions:
    - 0
    - 1
```

### Remapped Restore

Restore to different topic/partition layout:

```yaml
restore:
  topic_mapping:
    orders: orders-recovered
    payments: payments-clone

  partition_mapping:
    0: 0
    1: 2
    2: 4
```

### Dry-Run Mode

Validate without writing:

```yaml
restore:
  dry_run: true
```

---

## Configuration Reference

### Complete Restore Configuration

```yaml
mode: restore
backup_id: "backup-2025-01-15"

# Target Kafka cluster
target:
  bootstrap_servers:
    - kafka-1:9092
    - kafka-2:9092

  security:
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: restore-user
    sasl_password: ${KAFKA_PASSWORD}
    ssl_ca_location: /etc/ssl/ca.pem

  topics:
    include:
      - orders
      - payments
      - "events-*"
      - "~transactions-\\d+"  # Regex pattern
    exclude:
      - "*-internal"
      - "__*"

# Storage backend
storage:
  backend: s3
  bucket: kafka-backups
  region: us-east-1
  prefix: prod-cluster

# Restore options
restore:
  # Time filtering (PITR)
  time_window_start: 1705312800000
  time_window_end: 1705316400000

  # Partition filtering
  source_partitions:
    - 0
    - 1
    - 2

  # Remapping
  topic_mapping:
    orders: orders-recovered
  partition_mapping:
    0: 0
    1: 2

  # Offset handling
  consumer_group_strategy: header-based
  include_original_offset_header: true

  # Safety
  dry_run: false

  # Performance
  max_concurrent_partitions: 4
  produce_batch_size: 1000
  rate_limit_records_per_sec: 50000

  # Checkpointing
  checkpoint_state: /var/lib/kafka-backup/restore-checkpoint.json
  checkpoint_interval_secs: 60

  # Offset reset (dangerous)
  reset_consumer_offsets: false
  consumer_groups: []

  # Reporting
  offset_report: /var/lib/kafka-backup/offset-mapping.json
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `time_window_start` | i64 | None | Start timestamp (epoch ms) for PITR |
| `time_window_end` | i64 | None | End timestamp (epoch ms) for PITR |
| `source_partitions` | [i32] | None | Specific partitions to restore |
| `partition_mapping` | {i32: i32} | {} | Source to target partition mapping |
| `topic_mapping` | {str: str} | {} | Source to target topic mapping |
| `consumer_group_strategy` | enum | skip | Offset handling strategy |
| `dry_run` | bool | false | Validate without writing |
| `include_original_offset_header` | bool | false | Add offset headers to records |
| `max_concurrent_partitions` | usize | 4 | Parallel partition restores |
| `produce_batch_size` | usize | 1000 | Records per produce batch |
| `rate_limit_records_per_sec` | u64 | None | Rate limit (records/sec) |
| `checkpoint_state` | path | None | Checkpoint file for resume |
| `checkpoint_interval_secs` | u64 | 60 | Checkpoint save interval |
| `reset_consumer_offsets` | bool | false | Auto-reset consumer offsets |
| `consumer_groups` | [str] | [] | Groups to reset |
| `offset_report` | path | None | Offset mapping output file |

---

## Point-in-Time Restore

### Understanding PITR

Point-in-Time Restore allows you to recover data from a specific moment, useful for:

- **Corruption Recovery**: Restore data before a bug introduced bad records
- **Compliance**: Restore data state as of a specific date
- **Testing**: Restore a snapshot for load testing

### Timestamp Semantics

- Timestamps are in **epoch milliseconds** (Unix timestamp * 1000)
- Filtering is **inclusive**: `timestamp >= start AND timestamp <= end`
- Both `start` and `end` are optional (unbounded if omitted)

### Converting Timestamps

```bash
# Get current epoch milliseconds
date +%s%3N

# Convert ISO 8601 to epoch milliseconds
date -d "2024-01-15T10:00:00Z" +%s%3N
# Output: 1705312800000

# Convert epoch to human-readable
date -d @1705312800 --utc
# Output: Mon Jan 15 10:00:00 UTC 2024
```

### PITR Configuration Examples

#### Restore Last Hour

```yaml
restore:
  # Calculate: current_time - 3600000 (1 hour in ms)
  time_window_start: 1705309200000
  time_window_end: 1705312800000
```

#### Restore Specific Day

```yaml
restore:
  time_window_start: 1705276800000  # 2024-01-15 00:00:00 UTC
  time_window_end: 1705363199999    # 2024-01-15 23:59:59.999 UTC
```

#### Restore Before Corruption

```yaml
restore:
  # Corruption detected at 15:30:00, restore up to 15:00:00
  time_window_end: 1705330800000  # 2024-01-15 15:00:00 UTC
  # No start time = from beginning of backup
```

### PITR Segment Filtering

The restore engine optimizes PITR by:

1. **Segment-level filtering**: Only reads segments that overlap the time window
2. **Record-level filtering**: Filters individual records within overlapping segments

This minimizes I/O and processing time for narrow time windows.

---

## Topic and Partition Filtering

### Topic Filtering

#### Glob Patterns (Default)

```yaml
topics:
  include:
    - orders           # Exact match
    - "payments-*"     # Wildcard: payments-v1, payments-v2, etc.
    - "*-events"       # Wildcard: user-events, order-events, etc.
    - "??-topic"       # Single char: us-topic, eu-topic, etc.
```

#### Regex Patterns (Prefix with `~`)

```yaml
topics:
  include:
    - "~orders-\\d+"           # orders-1, orders-123, etc.
    - "~^(payments|billing)$"  # Exact: payments OR billing
    - "~.*-v[0-9]+$"           # Versioned topics: topic-v1, topic-v2
```

#### Combining Include and Exclude

```yaml
topics:
  include:
    - "*"  # Start with all topics
  exclude:
    - "__*"           # Exclude internal topics
    - "*-changelog"   # Exclude Kafka Streams changelogs
    - "~.*-test$"     # Exclude topics ending in -test
```

### Partition Filtering

Restore only specific partitions:

```yaml
restore:
  source_partitions:
    - 0
    - 1
    - 2
```

This is useful for:
- Restoring a single corrupted partition
- Parallel restore across multiple processes
- Testing with subset of data

### Partition Remapping

Map source partitions to different target partitions:

```yaml
restore:
  partition_mapping:
    0: 0   # Source 0 -> Target 0
    1: 2   # Source 1 -> Target 2
    2: 4   # Source 2 -> Target 4
```

Use cases:
- Changing partition count
- Load balancing across different partition layout
- Testing specific partition scenarios

### Topic Remapping

Restore to different topic names:

```yaml
restore:
  topic_mapping:
    orders: orders-recovered
    payments: payments-backup
    events: events-clone
```

---

## Consumer Offset Management

### Overview

Consumer group offsets track consumption progress. During restore, you have several options for handling offsets:

| Strategy | Action | Use Case |
|----------|--------|----------|
| `skip` | Don't touch offsets | Replay data, existing offsets OK |
| `header-based` | Store in message headers | Exact recovery with app support |
| `timestamp-based` | Calculate from timestamps | Approximate recovery |
| `cluster-scan` | Scan target cluster | Post-restore mapping |
| `manual` | Report only | Operator-driven reset |

### Strategy: Skip (Default)

```yaml
restore:
  consumer_group_strategy: skip
```

- **Behavior**: Restore data only, don't modify consumer offsets
- **Result**: Consumer groups continue from their existing committed offsets
- **Use Case**: Replaying data where consumers should pick up from where they left off

### Strategy: Header-Based

```yaml
restore:
  consumer_group_strategy: header-based
  include_original_offset_header: true
```

- **Behavior**: Stores original offset in message header `x-original-offset`
- **Result**: Applications can read the original offset from headers
- **Use Case**: Exact offset recovery when applications support header-based seeking

**Headers Added:**
```
x-original-offset: 12345
x-original-timestamp: 1705312800000
```

**Application-Side Usage (Java):**
```java
ConsumerRecord<String, String> record = ...;
Header offsetHeader = record.headers().lastHeader("x-original-offset");
long originalOffset = Long.parseLong(new String(offsetHeader.value()));
```

### Strategy: Timestamp-Based

```yaml
restore:
  consumer_group_strategy: timestamp-based
```

- **Behavior**: Uses Kafka's timestamp-based offset seeking
- **Result**: Consumer groups can seek to approximate positions
- **Use Case**: When you need approximate positioning without header support

### Strategy: Manual

```yaml
restore:
  consumer_group_strategy: manual
  offset_report: ./offset-mapping.json
```

- **Behavior**: Generates offset mapping report only
- **Result**: Report shows source/target offset mappings
- **Use Case**: Operator reviews and manually resets offsets

### Viewing Offset Mapping

```bash
kafka-backup show-offset-mapping \
  --path /var/lib/kafka-backup/data \
  --backup-id backup-2025-01-15
```

**Output:**
```
╔══════════════════════════════════════════════════════════════════════════════════════╗
║                              OFFSET MAPPING REPORT                                   ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║ Backup ID: backup-2025-01-15                                                         ║
╠══════════════════════════════════════════════════════════════════════════════════════╣
║ Topic/Partition    │ Source Offset Range     │ First Timestamp        │ Last Timestamp          ║
╠════════════════════╪═════════════════════════╪════════════════════════╪═════════════════════════╣
║ orders/0           │ 1000 - 5000             │ 2024-01-15 10:00:00    │ 2024-01-15 11:00:00     ║
║ orders/1           │ 1500 - 4500             │ 2024-01-15 10:00:05    │ 2024-01-15 10:59:55     ║
╚══════════════════════════════════════════════════════════════════════════════════════╝

To reset consumer group offsets, use the following commands:

# Reset to start of backup for orders/0
kafka-consumer-groups --bootstrap-server <BROKER> \
  --group <GROUP_NAME> \
  --topic orders:0 \
  --reset-offsets --to-offset 1000 --execute
```

### Automatic Offset Reset (Dangerous)

```yaml
restore:
  reset_consumer_offsets: true
  consumer_groups:
    - my-consumer-group
    - another-group
```

**WARNING**: This modifies `__consumer_offsets` on the target cluster. Only use with explicit understanding of the implications.

---

## Resumable Restores

### Overview

Large restores can take hours or days. Checkpointing enables:

- **Resume after failure**: Continue from last checkpoint
- **Planned interruptions**: Stop and resume later
- **Progress tracking**: Monitor restore progress

### Configuration

```yaml
restore:
  checkpoint_state: /var/lib/kafka-backup/restore-checkpoint.json
  checkpoint_interval_secs: 60
```

### Checkpoint File Format

```json
{
  "backup_id": "backup-2025-01-15",
  "start_time": 1705312800000,
  "last_checkpoint_time": 1705313400000,
  "segments_completed": [
    "backup-2025-01-15/orders/0/segment-000001.bin",
    "backup-2025-01-15/orders/0/segment-000002.bin"
  ],
  "segments_in_progress": [
    ["backup-2025-01-15/orders/1/segment-000001.bin", 52428800]
  ],
  "records_restored": 125000,
  "bytes_restored": 52428800,
  "config_hash": "abc123..."
}
```

### Resuming a Restore

Simply run the same restore command again. The engine will:

1. Load the checkpoint file
2. Skip completed segments
3. Resume from in-progress segments
4. Continue processing remaining segments

```bash
# Initial restore (interrupted)
kafka-backup restore --config restore.yaml
# Ctrl+C or failure

# Resume
kafka-backup restore --config restore.yaml
# INFO Loaded checkpoint: 150 segments completed, 125000 records restored
# INFO Resuming from checkpoint...
```

### Checkpoint Best Practices

1. **Use local filesystem** for checkpoint files (not S3)
2. **Set appropriate interval**: 60s is good for most cases
3. **Clean up** checkpoint files after successful restore
4. **Don't modify config** between resume attempts

---

## Dry-Run Validation

### Overview

Always validate restore configurations before execution:

```yaml
restore:
  dry_run: true
```

Or use the CLI command:

```bash
kafka-backup validate-restore --config restore.yaml
```

### What Gets Validated

- **Backup existence**: Manifest can be loaded
- **Topic filtering**: Topics match configuration
- **Partition filtering**: Partitions are valid
- **Time window**: Overlapping segments exist
- **Configuration**: All settings are valid

### Validation Report

```
╔══════════════════════════════════════════════════════════════════════╗
║                    RESTORE VALIDATION REPORT                         ║
╠══════════════════════════════════════════════════════════════════════╣
║ Status:         ✓ VALID                                              ║
║ Backup ID:      backup-2025-01-15                                    ║
╠══════════════════════════════════════════════════════════════════════╣
║                         RESTORE SUMMARY                              ║
╠══════════════════════════════════════════════════════════════════════╣
║ Topics to restore:    5                                              ║
║ Segments to process:  156                                            ║
║ Records to restore:   45,234,567                                     ║
║ Bytes to restore:     12.5 GB                                        ║
╠══════════════════════════════════════════════════════════════════════╣
║                           TIME RANGE                                 ║
╠══════════════════════════════════════════════════════════════════════╣
║ From: 2024-01-15 10:00:00 UTC                                        ║
║ To:   2024-01-15 11:00:00 UTC                                        ║
╠══════════════════════════════════════════════════════════════════════╣
║                    CONSUMER OFFSET ACTIONS                           ║
╠══════════════════════════════════════════════════════════════════════╣
║ • Original offsets will be stored in message headers                 ║
╚══════════════════════════════════════════════════════════════════════╝
```

### Output Formats

```bash
# Text (default)
kafka-backup validate-restore --config restore.yaml

# JSON (for automation)
kafka-backup validate-restore --config restore.yaml --format json

# YAML
kafka-backup validate-restore --config restore.yaml --format yaml
```

---

## CLI Commands

### restore

Run a restore operation:

```bash
kafka-backup restore --config restore.yaml
```

### validate-restore

Validate restore configuration without executing:

```bash
kafka-backup validate-restore --config restore.yaml [--format text|json|yaml]
```

### describe

Show detailed backup information:

```bash
kafka-backup describe --path /storage --backup-id BACKUP_ID [--format text|json|yaml]
```

### show-offset-mapping

Display offset mappings for consumer group reset:

```bash
kafka-backup show-offset-mapping --path /storage --backup-id BACKUP_ID [--format text|json|yaml|csv]
```

---

## Use Cases

### Use Case 1: Disaster Recovery

**Scenario**: Topic `orders` was accidentally deleted.

```yaml
mode: restore
backup_id: "backup-2025-01-15"

target:
  bootstrap_servers:
    - localhost:9092
  topics:
    include:
      - orders

storage:
  backend: s3
  bucket: kafka-backups

restore:
  consumer_group_strategy: skip
```

### Use Case 2: Point-in-Time Recovery

**Scenario**: Bug introduced bad data at 15:30. Restore good data to a new topic.

```yaml
mode: restore
backup_id: "backup-2025-01-15"

target:
  bootstrap_servers:
    - localhost:9092
  topics:
    include:
      - transactions

storage:
  backend: s3
  bucket: kafka-backups

restore:
  time_window_end: 1705330800000  # Before 15:30
  topic_mapping:
    transactions: transactions-recovered
  dry_run: false
```

### Use Case 3: Environment Cloning

**Scenario**: Clone production data to staging for testing.

```yaml
mode: restore
backup_id: "backup-2025-01-15"

target:
  bootstrap_servers:
    - staging-kafka:9092
  topics:
    include:
      - orders
      - payments
      - inventory

storage:
  backend: s3
  bucket: kafka-backups

restore:
  topic_mapping:
    orders: orders-staging
    payments: payments-staging
    inventory: inventory-staging
  consumer_group_strategy: timestamp-based
  rate_limit_records_per_sec: 10000
```

### Use Case 4: Partition-Level Recovery

**Scenario**: Partition 2 of `high-volume-topic` is corrupted.

```yaml
mode: restore
backup_id: "backup-2025-01-15"

target:
  bootstrap_servers:
    - localhost:9092
  topics:
    include:
      - high-volume-topic

storage:
  backend: filesystem
  path: /var/lib/kafka-backup

restore:
  source_partitions:
    - 2
  consumer_group_strategy: skip
```

### Use Case 5: Cross-Cluster Migration

**Scenario**: Migrate from on-prem to cloud Kafka.

```yaml
mode: restore
backup_id: "backup-2025-01-15"

target:
  bootstrap_servers:
    - pkc-12345.us-east-1.aws.confluent.cloud:9092
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: ${CONFLUENT_KEY}
    sasl_password: ${CONFLUENT_SECRET}
  topics:
    include:
      - "*"
    exclude:
      - "__*"

storage:
  backend: s3
  bucket: kafka-backups

restore:
  consumer_group_strategy: header-based
  include_original_offset_header: true
  max_concurrent_partitions: 8
```

---

## Best Practices

### 1. Always Validate First

```bash
kafka-backup validate-restore --config restore.yaml
```

### 2. Restore to Different Topic First

```yaml
restore:
  topic_mapping:
    orders: orders-recovered
```

Then validate data before switching traffic.

### 3. Use Rate Limiting for Production

```yaml
restore:
  rate_limit_records_per_sec: 50000
```

### 4. Enable Checkpointing for Large Restores

```yaml
restore:
  checkpoint_state: ./checkpoint.json
  checkpoint_interval_secs: 60
```

### 5. Monitor Progress

```bash
# Watch checkpoint file
watch -n 5 'cat checkpoint.json | jq .records_restored'

# Check health endpoint (if enabled)
curl http://localhost:9090/health
```

### 6. Plan Consumer Group Reset

Before restore:
1. Stop consumers
2. Plan offset reset strategy
3. Test on non-production first

After restore:
1. Verify data integrity
2. Reset consumer offsets if needed
3. Start consumers

---

## Troubleshooting

### "Backup not found"

```
Error: Could not load manifest for backup 'backup-2025-01-15'
```

**Solutions:**
1. Verify backup exists: `kafka-backup list --path /storage`
2. Check backup_id spelling
3. Verify storage credentials

### "No topics matched"

```
WARN No topics matched for restore
```

**Solutions:**
1. Check topic patterns in config
2. Use `kafka-backup describe` to see available topics
3. Verify exclude patterns aren't too broad

### "No segments match time window"

**Solutions:**
1. Verify time window overlaps backup data
2. Use `kafka-backup describe` to see time range
3. Ensure timestamps are in milliseconds

### "Connection refused"

**Solutions:**
1. Verify Kafka is running
2. Check bootstrap_servers
3. Verify network connectivity
4. Check security configuration

### "Produce timeout"

**Solutions:**
1. Reduce `max_concurrent_partitions`
2. Reduce `produce_batch_size`
3. Add rate limiting
4. Check target cluster capacity

### Enable Debug Logging

```bash
RUST_LOG=kafka_backup_core=debug kafka-backup restore --config restore.yaml
```
