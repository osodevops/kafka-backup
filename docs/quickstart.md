# Kafka Backup Quickstart Guide

Get started with Kafka Backup in 5 minutes. This guide covers the essential operations: backing up topics, restoring data, and validating backups.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Your First Backup](#your-first-backup)
4. [Your First Restore](#your-first-restore)
5. [Validate Your Backup](#validate-your-backup)
6. [Point-in-Time Restore](#point-in-time-restore)
7. [Next Steps](#next-steps)

---

## Prerequisites

- **Rust 1.70+** (for building from source)
- **Apache Kafka** cluster accessible
- **Storage**: Local filesystem, S3, Azure, or GCS

---

## Installation

### Option 1: Build from Source

```bash
# Clone the repository
git clone https://github.com/your-org/kafka-backup.git
cd kafka-backup

# Build in release mode
cargo build --release

# Binary is at target/release/kafka-backup
./target/release/kafka-backup --help
```

### Option 2: Install with Cargo

```bash
cargo install --path crates/kafka-backup-cli

# Now available in your PATH
kafka-backup --help
```

### Verify Installation

```bash
kafka-backup --version
# kafka-backup 0.1.0
```

---

## Your First Backup

### Step 1: Create the Backup Configuration

Create a file named `backup.yaml`:

```yaml
mode: backup
backup_id: "quickstart-backup-001"

source:
  bootstrap_servers:
    - localhost:9092
  topics:
    include:
      - my-topic
      - "orders-*"  # Glob patterns supported

storage:
  backend: filesystem
  path: ./backups

backup:
  compression: zstd
  continuous: false
```

### Step 2: Run the Backup

```bash
kafka-backup backup --config backup.yaml
```

**Expected Output:**

```
INFO Loading backup configuration...
INFO Connecting to Kafka cluster: localhost:9092
INFO Starting backup: quickstart-backup-001
INFO Backing up topic: my-topic (3 partitions)
INFO Backing up topic: orders-v1 (2 partitions)
INFO Backup completed successfully
INFO Total records: 125,432
INFO Total size: 45.2 MB (compressed: 12.8 MB)
```

### Step 3: Verify the Backup

```bash
kafka-backup list --path ./backups
```

**Output:**

```
Available backups:
  - quickstart-backup-001
```

Get more details:

```bash
kafka-backup describe --path ./backups --backup-id quickstart-backup-001
```

---

## Your First Restore

### Step 1: Create the Restore Configuration

Create a file named `restore.yaml`:

```yaml
mode: restore
backup_id: "quickstart-backup-001"

target:
  bootstrap_servers:
    - localhost:9092
  topics:
    include:
      - my-topic
      - "orders-*"

storage:
  backend: filesystem
  path: ./backups

restore:
  dry_run: false
  include_original_offset_header: true
```

> **Tip:** If restoring to a new cluster where topics don't exist, add `create_topics: true` to auto-create them:
> ```yaml
> restore:
>   create_topics: true
>   default_replication_factor: 3  # optional, defaults to broker default
> ```

### Step 2: Validate Before Restoring (Recommended)

```bash
kafka-backup validate-restore --config restore.yaml
```

**Output:**

```
╔══════════════════════════════════════════════════════════════════════╗
║                    RESTORE VALIDATION REPORT                         ║
╠══════════════════════════════════════════════════════════════════════╣
║ Status:         ✓ VALID                                              ║
║ Backup ID:      quickstart-backup-001                                ║
╠══════════════════════════════════════════════════════════════════════╣
║                         RESTORE SUMMARY                              ║
╠══════════════════════════════════════════════════════════════════════╣
║ Topics to restore:    2                                              ║
║ Segments to process:  15                                             ║
║ Records to restore:   125,432                                        ║
║ Bytes to restore:     45.2 MB                                        ║
╚══════════════════════════════════════════════════════════════════════╝
```

### Step 3: Run the Restore

```bash
kafka-backup restore --config restore.yaml
```

**Expected Output:**

```
INFO Loading restore configuration...
INFO Loading backup manifest: quickstart-backup-001
INFO Connecting to target Kafka cluster...
INFO Restoring 2 topics
INFO Restoring topic my-topic -> my-topic (3 partitions)
INFO Restored my-topic:0 -> my-topic:0 (42,000 records, 5 segments)
INFO Restored my-topic:1 -> my-topic:1 (41,432 records, 5 segments)
INFO Restored my-topic:2 -> my-topic:2 (42,000 records, 5 segments)
INFO Restore completed successfully
INFO Total records restored: 125,432
INFO Duration: 12.5s
INFO Throughput: 10,034 records/sec
```

---

## Validate Your Backup

Ensure backup integrity with the validate command:

### Shallow Validation (Fast)

```bash
kafka-backup validate --path ./backups --backup-id quickstart-backup-001
```

Checks:
- Manifest exists and is valid JSON
- All segment files exist
- Segment sizes match manifest

### Deep Validation (Thorough)

```bash
kafka-backup validate --path ./backups --backup-id quickstart-backup-001 --deep
```

Additional checks:
- Reads and parses every segment
- Validates CRC checksums
- Verifies record counts match manifest

---

## Point-in-Time Restore

Restore data from a specific time window.

### Step 1: Determine the Time Range

Use the describe command to see available time ranges:

```bash
kafka-backup describe --path ./backups --backup-id quickstart-backup-001 --format json | jq '.time_range'
```

### Step 2: Configure Time-Windowed Restore

```yaml
mode: restore
backup_id: "quickstart-backup-001"

target:
  bootstrap_servers:
    - localhost:9092
  topics:
    include:
      - my-topic

storage:
  backend: filesystem
  path: ./backups

restore:
  # Restore only records from this 1-hour window
  time_window_start: 1705312800000  # 2024-01-15 10:00:00 UTC
  time_window_end: 1705316400000    # 2024-01-15 11:00:00 UTC

  # Restore to a different topic for safety
  topic_mapping:
    my-topic: my-topic-recovered

  dry_run: false
```

### Step 3: Validate and Restore

```bash
# Validate first
kafka-backup validate-restore --config restore-pitr.yaml

# If validation passes, restore
kafka-backup restore --config restore-pitr.yaml
```

---

## Quick Reference

### Common Commands

```bash
# Backup
kafka-backup backup --config backup.yaml

# Restore
kafka-backup restore --config restore.yaml

# List backups
kafka-backup list --path /path/to/storage

# Describe backup
kafka-backup describe --path /path/to/storage --backup-id BACKUP_ID

# Validate backup
kafka-backup validate --path /path/to/storage --backup-id BACKUP_ID --deep

# Validate restore config
kafka-backup validate-restore --config restore.yaml

# Show offset mapping
kafka-backup show-offset-mapping --path /path/to/storage --backup-id BACKUP_ID
```

### Enable Verbose Logging

```bash
kafka-backup --verbose backup --config backup.yaml
```

### Output Formats

Most commands support multiple output formats:

```bash
# Text (default)
kafka-backup describe --path ./backups --backup-id BACKUP_ID

# JSON
kafka-backup describe --path ./backups --backup-id BACKUP_ID --format json

# YAML
kafka-backup describe --path ./backups --backup-id BACKUP_ID --format yaml
```

---

## Next Steps

Now that you've completed the quickstart, explore these advanced features:

### 1. Cloud Storage

Store backups in S3, Azure, or GCS:

```yaml
storage:
  backend: s3
  bucket: my-kafka-backups
  region: us-east-1
```

See [Storage Guide](storage_guide.md) for details.

### 2. Continuous Backup

Run backup continuously to capture all changes:

```yaml
backup:
  continuous: true
  checkpoint_interval_secs: 5
```

### 3. Consumer Offset Management

Preserve and restore consumer group offsets:

```yaml
restore:
  consumer_group_strategy: header-based
  include_original_offset_header: true
```

See [Restore Guide](restore_guide.md) for details.

### 4. Resumable Restores

Handle large restores with checkpointing:

```yaml
restore:
  checkpoint_state: ./restore-checkpoint.json
  checkpoint_interval_secs: 60
```

### 5. Partition Filtering and Remapping

Restore specific partitions or remap to different partitions:

```yaml
restore:
  source_partitions:
    - 0
    - 1
  partition_mapping:
    0: 0
    1: 2
```

---

## Troubleshooting

### "Connection refused" to Kafka

1. Verify Kafka is running:
   ```bash
   kafka-broker-api-versions --bootstrap-server localhost:9092
   ```

2. Check bootstrap servers in config

### "Backup not found"

1. Verify backup exists:
   ```bash
   kafka-backup list --path /path/to/storage
   ```

2. Check backup_id matches exactly

### "Permission denied" on storage

1. For filesystem: Check directory permissions
2. For S3: Verify AWS credentials and bucket policy
3. For Azure: Check account key or managed identity
4. For GCS: Verify service account permissions

### Enable Debug Logging

```bash
RUST_LOG=debug kafka-backup backup --config backup.yaml
```

---

## Getting Help

- **Documentation**: See the [docs](.) directory
- **Issues**: Report bugs on GitHub
- **Discussions**: Ask questions on GitHub Discussions
