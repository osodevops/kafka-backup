# Three-Phase Restore Guide

This guide explains how to use the three-phase offset remapping system for Kafka backup and restore operations, ensuring correct consumer group offset positioning after DR failover.

## Overview

The three-phase restore system solves a critical problem in Kafka backup/restore: **offset space discontinuity across clusters**. When you restore data from one Kafka cluster to another, the offsets assigned to records will be different. This means consumer group offsets from the source cluster will point to the wrong records (or no records at all) in the target cluster.

### The Problem

```
Source Cluster                           Target Cluster (after restore)
─────────────────                        ─────────────────────────────
orders/0: records at offsets 0-1000      orders/0: records at offsets 0-1000 (NEW!)
app1 consumer group at offset 750        app1 consumer group: WHERE???

Records are identical, but:
- Source had "750 means message from 10:45am"
- Target might have "750 means a different message"
```

### The Solution: Three-Phase Restore

1. **Phase 1 (Backup)**: Store original offset metadata in message headers
2. **Phase 2 (Restore)**: Produce records and collect source→target offset mappings
3. **Phase 3 (Offset Reset)**: Apply consumer group offset resets using the mapping

## Configuration

### Phase 1: Backup Configuration

```yaml
mode: backup
backup_id: prod-backup-2024

source:
  bootstrap_servers:
    - kafka-1:9092
    - kafka-2:9092
  topics:
    include:
      - "orders-*"
      - "payments-*"

storage:
  backend: s3
  bucket: kafka-backups
  prefix: prod

backup:
  # Enable offset headers for three-phase restore (default: true)
  include_offset_headers: true

  # Optional: identify source cluster in headers
  source_cluster_id: "prod-us-east-1"

  compression: zstd
  segment_max_bytes: 134217728  # 128MB
```

### Phase 2: Restore Configuration

```yaml
mode: restore
backup_id: prod-backup-2024

target:
  bootstrap_servers:
    - dr-kafka-1:9092
    - dr-kafka-2:9092
  topics:
    include:
      - "orders-*"
      - "payments-*"

storage:
  backend: s3
  bucket: kafka-backups
  prefix: prod

restore:
  # Use header-based offset strategy
  consumer_group_strategy: header-based

  # Include original offset headers in produced records
  include_original_offset_header: true

  # Output offset mapping for Phase 3
  offset_report: /var/kafka-backup/offset-mapping.json

  # Consumer groups to reset (Phase 3)
  consumer_groups:
    - order-processor
    - payment-processor
    - notification-service

  # Enable automatic offset reset (or use 'manual' for review first)
  reset_consumer_offsets: false  # Set to true for auto-reset
```

## Usage Examples

### Example 1: Complete Three-Phase Restore with Auto-Reset

```rust
use kafka_backup_core::restore::ThreePhaseRestore;
use kafka_backup_core::Config;

// Load configuration
let config = Config::from_file("restore.yaml")?;

// Create orchestrator
let orchestrator = ThreePhaseRestore::new(config)?;

// Run all phases
let report = orchestrator.run_all_phases().await?;

println!("Restore complete!");
println!("Records restored: {}", report.restore_report.records_restored);
println!("Offset mappings: {}", report.restore_report.offset_mapping.detailed_mapping_count());

if let Some(reset_report) = &report.offset_reset_report {
    println!("Consumer groups reset: {}", reset_report.groups_reset.len());
    println!("Partitions reset: {}", reset_report.partitions_reset);
}
```

### Example 2: Manual Review and Apply

```rust
use kafka_backup_core::restore::{ThreePhaseRestore, OffsetResetStrategy};
use kafka_backup_core::Config;

// Phase 2: Restore
let config = Config::from_file("restore.yaml")?;
let orchestrator = ThreePhaseRestore::new(config)?;
let restore_report = orchestrator.run_restore_phase().await?;

// Generate Phase 3 plan for review
let consumer_groups = vec![
    "order-processor".to_string(),
    "payment-processor".to_string(),
];

let reset_plan = orchestrator.generate_offset_reset_plan(
    &restore_report.offset_mapping,
    &consumer_groups,
    OffsetResetStrategy::Manual,
).await?;

// Generate shell script for manual execution
let script = orchestrator.generate_offset_reset_script(&reset_plan);
std::fs::write("/var/kafka-backup/offset-reset.sh", script)?;

println!("Review the script at /var/kafka-backup/offset-reset.sh");
println!("Then execute manually or call apply_offset_reset()");

// After review, apply the plan
let reset_report = orchestrator.apply_offset_reset(&reset_plan).await?;
```

### Example 3: Using the Offset Reset Executor Directly

```rust
use kafka_backup_core::restore::{OffsetResetExecutor, OffsetResetStrategy};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::OffsetMapping;

// Load offset mapping from restore report
let offset_mapping: OffsetMapping = serde_json::from_reader(
    std::fs::File::open("/var/kafka-backup/offset-mapping.json")?
)?;

// Create executor
let client = KafkaClient::new(target_config);
client.connect().await?;

let executor = OffsetResetExecutor::new(
    client,
    vec!["dr-kafka-1:9092".to_string()],
);

// Generate plan
let plan = executor.generate_plan(
    &offset_mapping,
    &["order-processor", "payment-processor"],
    OffsetResetStrategy::DryRun,
).await?;

// Review plan
println!("Reset Plan:");
for group in &plan.groups {
    println!("  Group: {}", group.group_id);
    for partition in &group.partitions {
        println!(
            "    {}:{}: {} -> {}",
            partition.topic,
            partition.partition,
            partition.source_offset,
            partition.target_offset
        );
    }
}

// Generate reports in various formats
println!("\nJSON Report:");
println!("{}", executor.generate_json_report(&plan)?);

println!("\nCSV Report:");
println!("{}", executor.generate_csv_report(&plan));

// Execute if satisfied
if !plan.dry_run {
    let report = executor.execute_plan(&plan).await?;
    println!("Reset complete: {} partitions", report.partitions_reset);
}
```

## CLI Commands

### Generate Offset Reset Script

```bash
# After restore, generate a shell script for manual offset reset
kafka-backup offset-mapping \
    --backup-id prod-backup-2024 \
    --storage-path /backups \
    --format shell-script \
    --groups order-processor,payment-processor \
    --output /var/kafka-backup/offset-reset.sh

# Review and execute
cat /var/kafka-backup/offset-reset.sh
chmod +x /var/kafka-backup/offset-reset.sh
./var/kafka-backup/offset-reset.sh
```

### Generate Offset Mapping Report

```bash
# JSON format
kafka-backup offset-mapping \
    --backup-id prod-backup-2024 \
    --format json \
    --output offset-mapping.json

# CSV format for analysis
kafka-backup offset-mapping \
    --backup-id prod-backup-2024 \
    --format csv \
    --output offset-mapping.csv

# Text summary
kafka-backup offset-mapping \
    --backup-id prod-backup-2024 \
    --format text
```

## Offset Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `skip` | No offset handling | Dev/test environments |
| `header-based` | Store/read offset from message headers | Production DR |
| `timestamp-based` | Seek by timestamp | Approximate positioning |
| `cluster-scan` | Scan target and match by content | Recovery scenarios |
| `manual` | Generate report only | High-control environments |

### Recommendation

For production DR scenarios, use **header-based** strategy:

1. It provides exact offset mapping (no approximation)
2. Headers are preserved through the entire backup/restore pipeline
3. No external state required
4. Works across different cluster configurations

## Offset Mapping Report Format

The offset mapping report contains:

```json
{
  "entries": {
    "orders/0": {
      "topic": "orders",
      "partition": 0,
      "source_first_offset": 0,
      "source_last_offset": 1000,
      "target_first_offset": 5000,
      "target_last_offset": 6000,
      "first_timestamp": 1700000000000,
      "last_timestamp": 1700001000000
    }
  },
  "detailed_mappings": {
    "orders/0": [
      { "source_offset": 0, "target_offset": 5000, "timestamp": 1700000000000 },
      { "source_offset": 1, "target_offset": 5001, "timestamp": 1700000001000 }
    ]
  },
  "consumer_groups": {
    "order-processor": {
      "group_id": "order-processor",
      "offsets": {
        "orders": {
          "0": {
            "source_offset": 750,
            "target_offset": 5750,
            "timestamp": 1700000750000
          }
        }
      }
    }
  },
  "source_cluster_id": "prod-us-east-1",
  "created_at": 1700002000000
}
```

## Safety Guarantees

- **No data loss**: Records are restored with 100% fidelity
- **No duplicates**: Each record is produced exactly once
- **Exact positioning**: Consumer groups resume at the exact logical position
- **Reversible**: Offset resets can be reverted if needed
- **Auditable**: Full mapping report for verification

## Best Practices

1. **Always enable offset headers** during backup (`include_offset_headers: true`)
2. **Review offset reset plans** before applying in production
3. **Use dry-run mode** first to validate the plan
4. **Save offset mapping reports** for audit trails
5. **Test DR procedures** regularly in non-production environments
6. **Monitor consumer lag** after offset reset

## Troubleshooting

### Consumer group offset not found in mapping

This can happen if:
- The consumer group wasn't active when backup was taken
- The topic/partition wasn't included in the backup
- The offset is outside the backed-up range

**Solution**: Use timestamp-based seeking as fallback:
```yaml
restore:
  consumer_group_strategy: timestamp-based
```

### Offset headers missing in backup

Check if backup was created with `include_offset_headers: true`. If not, re-run the backup or use `cluster-scan` strategy.

### Target cluster already has data

If the target cluster already has data at overlapping offsets, the offset mapping will be incorrect. **Solution**: Restore to a clean topic or use timestamp-based seeking.
