# Offset Reset & Rollback Guide

This guide covers the offset reset capabilities in kafka-backup, including bulk parallel offset resets and the safety-net rollback functionality.

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Bulk Offset Reset](#bulk-offset-reset)
- [Offset Rollback](#offset-rollback)
- [CLI Reference](#cli-reference)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)

---

## Overview

After restoring Kafka topics from a backup, consumer groups need their offsets adjusted to match the new message positions. kafka-backup provides two complementary features:

| Feature | Purpose |
|---------|---------|
| **Bulk Offset Reset** | Reset multiple consumer groups in parallel with ~50x speedup |
| **Offset Rollback** | Create snapshots before changes and rollback if something goes wrong |

### When to Use

- **Bulk Offset Reset**: When you have many consumer groups (10+) that need offset adjustments after a restore
- **Offset Rollback**: When you want a safety net before making offset changes, especially in production

---

## Quick Start

### 1. Create a Safety Snapshot (Recommended)

Before making any offset changes, create a snapshot:

```bash
kafka-backup offset-rollback snapshot \
  --path /backup/storage \
  --groups "my-consumer-group-1,my-consumer-group-2" \
  --bootstrap-servers "kafka1:9092,kafka2:9092"
```

Output:
```
Creating offset snapshot for 2 consumer groups...

Snapshot created successfully!
  Snapshot ID: snap-20241129-143052-a1b2c3d4
  Created at:  2024-11-29 14:30:52 UTC
  Groups:      2
  Offsets:     24

To rollback to this snapshot, run:
  kafka-backup offset-rollback rollback --path /backup/storage --snapshot-id snap-20241129-143052-a1b2c3d4
```

### 2. Execute Bulk Offset Reset

Reset offsets using the mapping from your restore operation:

```bash
kafka-backup offset-reset-bulk \
  --path /backup/storage \
  --backup-id backup-20241128 \
  --groups "my-consumer-group-1,my-consumer-group-2" \
  --bootstrap-servers "kafka1:9092,kafka2:9092" \
  --max-concurrent 50
```

### 3. Verify or Rollback

If something went wrong, rollback to your snapshot:

```bash
kafka-backup offset-rollback rollback \
  --path /backup/storage \
  --snapshot-id snap-20241129-143052-a1b2c3d4 \
  --bootstrap-servers "kafka1:9092,kafka2:9092" \
  --verify
```

---

## Bulk Offset Reset

### How It Works

The bulk offset reset feature:

1. Groups offset mappings by consumer group
2. Executes resets in parallel using a semaphore-controlled concurrency pool
3. Retries failed partitions with exponential backoff
4. Collects detailed metrics (latency percentiles, throughput)

### Basic Usage

```bash
kafka-backup offset-reset-bulk \
  --path /backup/storage \
  --backup-id <backup-id> \
  --groups "group1,group2,group3" \
  --bootstrap-servers "kafka:9092"
```

### Configuration Options

| Option | Default | Description |
|--------|---------|-------------|
| `--max-concurrent` | 50 | Maximum parallel reset operations |
| `--max-retries` | 3 | Retry attempts for failed partitions |
| `--security-protocol` | PLAINTEXT | Security protocol (PLAINTEXT, SSL, SASL_SSL, SASL_PLAINTEXT) |
| `--format` | text | Output format (text, json) |

### Performance Tuning

For large deployments with many consumer groups:

```bash
# High-throughput configuration
kafka-backup offset-reset-bulk \
  --path /backup/storage \
  --backup-id backup-20241128 \
  --groups "$(cat consumer-groups.txt | tr '\n' ',')" \
  --bootstrap-servers "kafka1:9092,kafka2:9092,kafka3:9092" \
  --max-concurrent 100 \
  --max-retries 5 \
  --format json
```

### Understanding the Output

Text format:
```
╔══════════════════════════════════════════════════════════════════════════════╗
║                          BULK OFFSET RESET REPORT                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ Status: SUCCESS                                                              ║
╟──────────────────────────────────────────────────────────────────────────────╢
║   Total Groups:      50                                                      ║
║   Successful:        50                                                      ║
║   Failed:            0                                                       ║
║   Total Offsets:     1,250                                                   ║
║   Duration:          2,340 ms                                                ║
╟──────────────────────────────────────────────────────────────────────────────╢
║ Performance                                                                  ║
║   Throughput:        534.2 offsets/sec                                       ║
║   Avg Latency:       45.2 ms                                                 ║
║   P50 Latency:       42.0 ms                                                 ║
║   P99 Latency:       89.0 ms                                                 ║
║   Max Concurrency:   50                                                      ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

JSON format (for automation):
```json
{
  "status": "success",
  "total_groups": 50,
  "successful_groups": 50,
  "failed_groups": 0,
  "total_offsets_reset": 1250,
  "duration_ms": 2340,
  "performance": {
    "offsets_per_second": 534.2,
    "avg_latency_ms": 45.2,
    "p50_latency_ms": 42.0,
    "p99_latency_ms": 89.0,
    "max_concurrency": 50
  }
}
```

---

## Offset Rollback

The rollback feature provides a safety net for offset reset operations.

### Workflow

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Create         │     │  Execute        │     │  Verify or      │
│  Snapshot       │────▶│  Offset Reset   │────▶│  Rollback       │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

### Creating Snapshots

Capture the current state of consumer group offsets:

```bash
kafka-backup offset-rollback snapshot \
  --path /backup/storage \
  --groups "order-processor,inventory-updater,notification-sender" \
  --bootstrap-servers "kafka1:9092" \
  --description "Pre-restore snapshot for incident #1234"
```

With SASL authentication:

```bash
export KAFKA_USERNAME="admin"
export KAFKA_PASSWORD="secret"

kafka-backup offset-rollback snapshot \
  --path /backup/storage \
  --groups "order-processor" \
  --bootstrap-servers "kafka1:9092" \
  --security-protocol SASL_SSL \
  --description "Production pre-restore snapshot"
```

### Listing Snapshots

View all available snapshots:

```bash
kafka-backup offset-rollback list --path /backup/storage
```

Output:
```
Available offset snapshots:

SNAPSHOT ID                              CREATED AT               GROUPS    OFFSETS  DESCRIPTION
---------------------------------------- ------------------------ -------- --------  ------------------------------
snap-20241129-143052-a1b2c3d4            2024-11-29 14:30:52 UTC        3       36  Pre-restore snapshot for inc...
snap-20241128-091523-e5f6g7h8            2024-11-28 09:15:23 UTC        5       60  Daily backup checkpoint
snap-20241127-160000-i9j0k1l2            2024-11-27 16:00:00 UTC        2       24  Testing offset reset

Total: 3 snapshots
```

### Viewing Snapshot Details

Inspect a specific snapshot:

```bash
kafka-backup offset-rollback show \
  --path /backup/storage \
  --snapshot-id snap-20241129-143052-a1b2c3d4
```

Output:
```
╔══════════════════════════════════════════════════════════════════════════════╗
║                           OFFSET SNAPSHOT DETAILS                            ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ Snapshot ID: snap-20241129-143052-a1b2c3d4                                   ║
║ Created:     2024-11-29 14:30:52 UTC                                         ║
║ Groups:      3                                                               ║
║ Offsets:     36                                                              ║
║ Description: Pre-restore snapshot for incident #1234                         ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ Consumer Groups                                                              ║
╟──────────────────────────────────────────────────────────────────────────────╢
║ Group: order-processor                                                       ║
║   Partitions: 12                                                             ║
║     orders:0 -> offset 15234                                                 ║
║     orders:1 -> offset 15198                                                 ║
║     orders:2 -> offset 15301                                                 ║
║     ...                                                                      ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

### Executing a Rollback

Restore offsets to a previous snapshot:

```bash
kafka-backup offset-rollback rollback \
  --path /backup/storage \
  --snapshot-id snap-20241129-143052-a1b2c3d4 \
  --bootstrap-servers "kafka1:9092" \
  --verify
```

Output:
```
Rolling back to snapshot: snap-20241129-143052-a1b2c3d4
  Created: 2024-11-29 14:30:52 UTC
  Groups: 3
  Offsets: 36

╔══════════════════════════════════════════════════════════════════════════════╗
║                             ROLLBACK RESULT                                  ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ Status: ✓ SUCCESS                                                            ║
╟──────────────────────────────────────────────────────────────────────────────╢
║   Groups Rolled Back: 3                                                      ║
║   Groups Failed:      0                                                      ║
║   Offsets Restored:   36                                                     ║
║   Duration:           156 ms                                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

╔══════════════════════════════════════════════════════════════════════════════╗
║                           VERIFICATION RESULT                                ║
╠══════════════════════════════════════════════════════════════════════════════╣
║ ✓ VERIFIED - All offsets match snapshot                                      ║
╟──────────────────────────────────────────────────────────────────────────────╢
║   Groups Verified:   3                                                       ║
║   Groups Mismatched: 0                                                       ║
╚══════════════════════════════════════════════════════════════════════════════╝
```

### Verifying Offsets

Check if current offsets match a snapshot without making changes:

```bash
kafka-backup offset-rollback verify \
  --path /backup/storage \
  --snapshot-id snap-20241129-143052-a1b2c3d4 \
  --bootstrap-servers "kafka1:9092"
```

### Deleting Snapshots

Remove old snapshots:

```bash
kafka-backup offset-rollback delete \
  --path /backup/storage \
  --snapshot-id snap-20241127-160000-i9j0k1l2
```

---

## CLI Reference

### offset-reset-bulk

Execute parallel bulk offset reset.

```bash
kafka-backup offset-reset-bulk [OPTIONS]
```

| Option | Required | Description |
|--------|----------|-------------|
| `--path`, `-p` | Yes | Path to storage location |
| `--backup-id`, `-b` | Yes | Backup ID with offset mapping |
| `--groups`, `-g` | Yes | Consumer groups (comma-separated) |
| `--bootstrap-servers` | Yes | Kafka brokers (comma-separated) |
| `--max-concurrent` | No | Max parallel operations (default: 50) |
| `--max-retries` | No | Retry attempts (default: 3) |
| `--security-protocol` | No | Security protocol |
| `--format`, `-f` | No | Output format: text, json (default: text) |

### offset-rollback snapshot

Create an offset snapshot.

```bash
kafka-backup offset-rollback snapshot [OPTIONS]
```

| Option | Required | Description |
|--------|----------|-------------|
| `--path`, `-p` | Yes | Path to storage location |
| `--groups`, `-g` | Yes | Consumer groups (comma-separated) |
| `--bootstrap-servers` | Yes | Kafka brokers (comma-separated) |
| `--description`, `-d` | No | Snapshot description |
| `--security-protocol` | No | Security protocol |
| `--format`, `-f` | No | Output format: text, json |

### offset-rollback list

List available snapshots.

```bash
kafka-backup offset-rollback list [OPTIONS]
```

| Option | Required | Description |
|--------|----------|-------------|
| `--path`, `-p` | Yes | Path to storage location |
| `--format`, `-f` | No | Output format: text, json |

### offset-rollback show

Show snapshot details.

```bash
kafka-backup offset-rollback show [OPTIONS]
```

| Option | Required | Description |
|--------|----------|-------------|
| `--path`, `-p` | Yes | Path to storage location |
| `--snapshot-id`, `-s` | Yes | Snapshot ID to show |
| `--format`, `-f` | No | Output format: text, json |

### offset-rollback rollback

Execute rollback to a snapshot.

```bash
kafka-backup offset-rollback rollback [OPTIONS]
```

| Option | Required | Description |
|--------|----------|-------------|
| `--path`, `-p` | Yes | Path to storage location |
| `--snapshot-id`, `-s` | Yes | Snapshot ID to rollback to |
| `--bootstrap-servers` | Yes | Kafka brokers (comma-separated) |
| `--security-protocol` | No | Security protocol |
| `--verify` | No | Verify after rollback (default: true) |
| `--format`, `-f` | No | Output format: text, json |

### offset-rollback verify

Verify offsets match a snapshot.

```bash
kafka-backup offset-rollback verify [OPTIONS]
```

| Option | Required | Description |
|--------|----------|-------------|
| `--path`, `-p` | Yes | Path to storage location |
| `--snapshot-id`, `-s` | Yes | Snapshot ID to verify against |
| `--bootstrap-servers` | Yes | Kafka brokers (comma-separated) |
| `--security-protocol` | No | Security protocol |
| `--format`, `-f` | No | Output format: text, json |

### offset-rollback delete

Delete a snapshot.

```bash
kafka-backup offset-rollback delete [OPTIONS]
```

| Option | Required | Description |
|--------|----------|-------------|
| `--path`, `-p` | Yes | Path to storage location |
| `--snapshot-id`, `-s` | Yes | Snapshot ID to delete |

---

## Best Practices

### 1. Always Create Snapshots Before Changes

```bash
# Create a snapshot before any offset modifications
kafka-backup offset-rollback snapshot \
  --path /backup/storage \
  --groups "$(kafka-consumer-groups.sh --list --bootstrap-server kafka:9092 | tr '\n' ',')" \
  --bootstrap-servers "kafka:9092" \
  --description "Pre-maintenance snapshot $(date +%Y%m%d-%H%M%S)"
```

### 2. Use JSON Format for Automation

```bash
# Capture result for CI/CD pipelines
RESULT=$(kafka-backup offset-reset-bulk \
  --path /backup/storage \
  --backup-id backup-123 \
  --groups "group1,group2" \
  --bootstrap-servers "kafka:9092" \
  --format json)

# Check status
if echo "$RESULT" | jq -e '.status == "success"' > /dev/null; then
  echo "Offset reset successful"
else
  echo "Offset reset failed, initiating rollback..."
  kafka-backup offset-rollback rollback ...
fi
```

### 3. Tune Concurrency Based on Cluster Size

| Cluster Size | Recommended `--max-concurrent` |
|--------------|-------------------------------|
| Small (1-3 brokers) | 10-20 |
| Medium (3-10 brokers) | 30-50 |
| Large (10+ brokers) | 50-100 |

### 4. Retain Snapshots for Audit Trail

```bash
# Keep snapshots for 30 days
find /backup/storage/snapshots -name "*.json" -mtime +30 -exec \
  kafka-backup offset-rollback delete --path /backup/storage --snapshot-id {} \;
```

### 5. Verify After Critical Operations

```bash
# Always verify after rollback in production
kafka-backup offset-rollback rollback \
  --path /backup/storage \
  --snapshot-id $SNAPSHOT_ID \
  --bootstrap-servers "kafka:9092" \
  --verify

# Exit code will be non-zero if verification fails
if [ $? -ne 0 ]; then
  echo "ALERT: Rollback verification failed!"
  exit 1
fi
```

---

## Troubleshooting

### Consumer Group Not Found

```
Error: Consumer group 'my-group' not found
```

**Cause**: The consumer group doesn't exist or has no committed offsets.

**Solution**: Verify the group exists:
```bash
kafka-consumer-groups.sh --describe --group my-group --bootstrap-server kafka:9092
```

### Connection Timeout

```
Error: Failed to connect to Kafka: Connection timed out
```

**Solution**:
1. Verify bootstrap servers are reachable
2. Check firewall rules
3. Ensure correct security protocol is specified

### Partial Success

```
Status: PARTIAL_SUCCESS
Groups Failed: 2
```

**Cause**: Some consumer groups failed to reset.

**Solution**:
1. Check the errors in the output
2. Verify the failed groups exist
3. Retry with increased `--max-retries`

### Snapshot Not Found

```
Error: Snapshot snap-xyz not found
```

**Solution**: List available snapshots:
```bash
kafka-backup offset-rollback list --path /backup/storage
```

### Authentication Errors

```
Error: SASL authentication failed
```

**Solution**: Ensure credentials are set:
```bash
export KAFKA_USERNAME="your-username"
export KAFKA_PASSWORD="your-password"

kafka-backup offset-rollback snapshot \
  --security-protocol SASL_SSL \
  ...
```

---

## Examples

### Complete Restore Workflow

```bash
#!/bin/bash
set -e

BACKUP_ID="backup-20241128"
STORAGE_PATH="/backup/storage"
BOOTSTRAP="kafka1:9092,kafka2:9092,kafka3:9092"
GROUPS="order-processor,inventory-service,notification-handler"

# Step 1: Create pre-restore snapshot
echo "Creating safety snapshot..."
kafka-backup offset-rollback snapshot \
  --path "$STORAGE_PATH" \
  --groups "$GROUPS" \
  --bootstrap-servers "$BOOTSTRAP" \
  --description "Pre-restore for $BACKUP_ID" \
  --format json > /tmp/snapshot.json

SNAPSHOT_ID=$(jq -r '.snapshot_id' /tmp/snapshot.json)
echo "Created snapshot: $SNAPSHOT_ID"

# Step 2: Execute restore (Phase 1 & 2)
echo "Restoring backup..."
kafka-backup restore --config restore-config.yaml

# Step 3: Execute bulk offset reset (Phase 3)
echo "Resetting consumer group offsets..."
kafka-backup offset-reset-bulk \
  --path "$STORAGE_PATH" \
  --backup-id "$BACKUP_ID" \
  --groups "$GROUPS" \
  --bootstrap-servers "$BOOTSTRAP" \
  --max-concurrent 50 \
  --format json > /tmp/reset-result.json

# Step 4: Verify success
STATUS=$(jq -r '.status' /tmp/reset-result.json)
if [ "$STATUS" != "success" ]; then
  echo "Offset reset failed! Rolling back..."
  kafka-backup offset-rollback rollback \
    --path "$STORAGE_PATH" \
    --snapshot-id "$SNAPSHOT_ID" \
    --bootstrap-servers "$BOOTSTRAP" \
    --verify
  exit 1
fi

echo "Restore completed successfully!"
echo "Snapshot $SNAPSHOT_ID retained for 24 hours"
```

### Bulk Reset with Error Handling

```bash
#!/bin/bash

RESULT=$(kafka-backup offset-reset-bulk \
  --path /backup/storage \
  --backup-id backup-123 \
  --groups "group1,group2,group3" \
  --bootstrap-servers "kafka:9092" \
  --format json 2>&1)

EXIT_CODE=$?

if [ $EXIT_CODE -ne 0 ]; then
  echo "Command failed with exit code $EXIT_CODE"
  echo "$RESULT"
  exit $EXIT_CODE
fi

FAILED=$(echo "$RESULT" | jq '.failed_groups')
if [ "$FAILED" -gt 0 ]; then
  echo "Warning: $FAILED groups failed to reset"
  echo "$RESULT" | jq '.group_outcomes[] | select(.status == "failed")'
fi
```

---

## Security Considerations

### Credential Management

Never pass credentials as command-line arguments. Use environment variables:

```bash
export KAFKA_USERNAME="admin"
export KAFKA_PASSWORD="$(vault read -field=password secret/kafka)"
export KAFKA_SSL_CA_CERT="/etc/kafka/ca.crt"
```

### Access Control

Ensure the user running kafka-backup has appropriate Kafka ACLs:

```
# Required permissions
- Describe consumer groups
- Read consumer group offsets
- Alter consumer group offsets (for reset/rollback)
```

### Audit Logging

Enable verbose logging for audit trails:

```bash
RUST_LOG=info kafka-backup offset-reset-bulk ...
```
