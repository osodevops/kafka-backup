# Kafka Backup Configuration Reference

Complete reference for all configuration options in Kafka Backup.

---

## Table of Contents

1. [Configuration File Format](#configuration-file-format)
2. [Global Options](#global-options)
3. [Source Configuration](#source-configuration)
4. [Target Configuration](#target-configuration)
5. [Storage Configuration](#storage-configuration)
6. [Backup Configuration](#backup-configuration)
7. [Restore Configuration](#restore-configuration)
8. [Environment Variables](#environment-variables)
9. [CLI Arguments](#cli-arguments)

---

## Configuration File Format

Configuration files use YAML format. The top-level `mode` field determines whether it's a backup or restore configuration.

```yaml
# Backup configuration
mode: backup
backup_id: "my-backup"
source: { ... }
storage: { ... }
backup: { ... }
```

```yaml
# Restore configuration
mode: restore
backup_id: "my-backup"
target: { ... }
storage: { ... }
restore: { ... }
```

---

## Global Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `mode` | string | Yes | - | Operation mode: `backup` or `restore` |
| `backup_id` | string | Yes | - | Unique identifier for the backup |

### Example

```yaml
mode: backup
backup_id: "daily-backup-2025-01-15"
```

---

## Source Configuration

Configuration for the Kafka cluster to read from (backup mode).

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `bootstrap_servers` | list[string] | Yes | - | Kafka broker addresses |
| `topics.include` | list[string] | No | `["*"]` | Topics to include (glob patterns) |
| `topics.exclude` | list[string] | No | `[]` | Topics to exclude (glob patterns) |
| `security_protocol` | string | No | `PLAINTEXT` | Security protocol |
| `sasl_mechanism` | string | No | - | SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) |
| `sasl_username` | string | No | - | SASL username |
| `sasl_password` | string | No | - | SASL password |
| `ssl_ca_location` | string | No | - | Path to CA certificate |
| `ssl_certificate_location` | string | No | - | Path to client certificate |
| `ssl_key_location` | string | No | - | Path to client key |

### Topic Patterns

Topics support glob patterns:

| Pattern | Matches |
|---------|---------|
| `*` | All topics |
| `orders` | Exact match |
| `orders-*` | Topics starting with "orders-" |
| `*-events` | Topics ending with "-events" |
| `orders-*-v2` | Topics like "orders-us-v2", "orders-eu-v2" |

### Example

```yaml
source:
  bootstrap_servers:
    - kafka1.example.com:9092
    - kafka2.example.com:9092
    - kafka3.example.com:9092
  topics:
    include:
      - "orders-*"
      - "payments"
      - "user-events"
    exclude:
      - "*-test"
      - "*-staging"
  security_protocol: SASL_SSL
  sasl_mechanism: SCRAM-SHA-512
  sasl_username: backup-user
  sasl_password: ${KAFKA_PASSWORD}
  ssl_ca_location: /etc/kafka/certs/ca.pem
```

---

## Target Configuration

Configuration for the Kafka cluster to write to (restore mode).

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `bootstrap_servers` | list[string] | Yes | - | Kafka broker addresses |
| `topics.include` | list[string] | No | `["*"]` | Topics to restore (glob/regex patterns) |
| `topics.exclude` | list[string] | No | `[]` | Topics to exclude |
| `security_protocol` | string | No | `PLAINTEXT` | Security protocol |
| `sasl_mechanism` | string | No | - | SASL mechanism |
| `sasl_username` | string | No | - | SASL username |
| `sasl_password` | string | No | - | SASL password |
| `ssl_ca_location` | string | No | - | Path to CA certificate |
| `ssl_certificate_location` | string | No | - | Path to client certificate |
| `ssl_key_location` | string | No | - | Path to client key |

### Regex Pattern Support

In restore mode, topic patterns support regex when prefixed with `~`:

| Pattern | Type | Matches |
|---------|------|---------|
| `orders-*` | Glob | Topics starting with "orders-" |
| `~orders-.*` | Regex | Topics matching regex `orders-.*` |
| `~^(orders\|payments)$` | Regex | Exactly "orders" or "payments" |

### Example

```yaml
target:
  bootstrap_servers:
    - kafka-restore.example.com:9092
  topics:
    include:
      - "orders-*"           # Glob pattern
      - "~payments-v[0-9]+"  # Regex pattern
    exclude:
      - "*-internal"
  security_protocol: SASL_SSL
  sasl_mechanism: PLAIN
  sasl_username: restore-user
  sasl_password: ${KAFKA_PASSWORD}
```

---

## Storage Configuration

Configuration for the backup storage backend.

### Common Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `backend` | string | Yes | - | Storage backend: `filesystem`, `s3`, `azure`, `gcs` |

### Filesystem Backend

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `path` | string | Yes | - | Directory path for backups |

```yaml
storage:
  backend: filesystem
  path: /var/lib/kafka-backup/data
```

### AWS S3 Backend

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `bucket` | string | Yes | - | S3 bucket name |
| `region` | string | Yes | - | AWS region |
| `prefix` | string | No | `""` | Key prefix (folder) |
| `endpoint` | string | No | - | Custom endpoint (for MinIO, etc.) |
| `path_style` | bool | No | `false` | Use path-style URLs |
| `access_key_id` | string | No | - | AWS access key (or use env/IAM) |
| `secret_access_key` | string | No | - | AWS secret key |

```yaml
storage:
  backend: s3
  bucket: my-kafka-backups
  region: us-east-1
  prefix: production/
```

#### MinIO Configuration

```yaml
storage:
  backend: s3
  bucket: kafka-backups
  endpoint: http://minio.local:9000
  path_style: true
  access_key_id: minioadmin
  secret_access_key: ${MINIO_SECRET}
```

### Azure Blob Storage Backend

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `account_name` | string | Yes | - | Azure storage account name |
| `container_name` | string | Yes | - | Blob container name |
| `prefix` | string | No | `""` | Blob prefix (folder) |
| `account_key` | string | No | - | Account key (or use managed identity) |
| `sas_token` | string | No | - | SAS token |
| `use_managed_identity` | bool | No | `false` | Use managed identity |

```yaml
storage:
  backend: azure
  account_name: mybackupstorage
  container_name: kafka-backups
  prefix: production/
  use_managed_identity: true
```

### Google Cloud Storage Backend

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `bucket` | string | Yes | - | GCS bucket name |
| `prefix` | string | No | `""` | Object prefix (folder) |
| `project_id` | string | No | - | GCP project ID |
| `credentials_file` | string | No | - | Path to service account JSON |

```yaml
storage:
  backend: gcs
  bucket: my-kafka-backups
  prefix: production/
  project_id: my-gcp-project
```

---

## Backup Configuration

Options specific to backup operations.

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `compression` | string | No | `zstd` | Compression algorithm |
| `continuous` | bool | No | `false` | Run continuously |
| `checkpoint_interval_secs` | int | No | `60` | Checkpoint interval |
| `segment_max_records` | int | No | `100000` | Max records per segment |
| `segment_max_bytes` | int | No | `104857600` | Max bytes per segment (100MB) |
| `segment_max_age_secs` | int | No | `3600` | Max segment age |
| `max_concurrent_partitions` | int | No | `4` | Concurrent partitions |
| `fetch_max_bytes` | int | No | `1048576` | Max bytes per fetch |

### Compression Options

| Algorithm | Description |
|-----------|-------------|
| `none` | No compression |
| `zstd` | Zstandard (default, best ratio) |
| `lz4` | LZ4 (faster, lower ratio) |
| `gzip` | Gzip (widely compatible) |
| `snappy` | Snappy (balanced) |

### Example

```yaml
backup:
  compression: zstd
  continuous: true
  checkpoint_interval_secs: 30
  segment_max_records: 50000
  segment_max_bytes: 52428800  # 50MB
  segment_max_age_secs: 1800   # 30 minutes
  max_concurrent_partitions: 8
  fetch_max_bytes: 5242880     # 5MB
```

---

## Restore Configuration

Options specific to restore operations.

### Core Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `dry_run` | bool | No | `false` | Simulate restore without writing |
| `include_original_offset_header` | bool | No | `false` | Add original offset to headers |

### Time Window Filtering

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `time_window_start` | int | No | - | Start timestamp (ms since epoch) |
| `time_window_end` | int | No | - | End timestamp (ms since epoch) |

```yaml
restore:
  # Restore records from Jan 15, 2025 00:00 to 23:59 UTC
  time_window_start: 1736899200000
  time_window_end: 1736985600000
```

### Partition Filtering

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `source_partitions` | list[int] | No | - | Specific partitions to restore |

```yaml
restore:
  # Only restore partitions 0, 1, and 2
  source_partitions:
    - 0
    - 1
    - 2
```

### Topic and Partition Mapping

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `topic_mapping` | map[string, string] | No | `{}` | Rename topics during restore |
| `partition_mapping` | map[int, int] | No | `{}` | Remap partitions during restore |

```yaml
restore:
  # Rename topics
  topic_mapping:
    orders: orders-restored
    payments: payments-restored

  # Remap partitions (source -> target)
  partition_mapping:
    0: 0
    1: 2
    2: 4
```

### Consumer Offset Management

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `consumer_group_strategy` | string | No | `skip` | Offset handling strategy |
| `consumer_groups` | list[string] | No | `[]` | Consumer groups to process |
| `reset_consumer_offsets` | bool | No | `false` | Actually reset offsets |
| `offset_report` | string | No | - | Path to write offset report |

#### Consumer Group Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `skip` | Don't modify consumer offsets | Data replay, offsets already correct |
| `header-based` | Use original offset from record headers | Exact offset recovery |
| `timestamp-based` | Calculate offsets from record timestamps | Approximate recovery |
| `cluster-scan` | Scan target cluster for matching offsets | Post-restore mapping |
| `manual` | Generate report only, no action | Operator-driven reset |

```yaml
restore:
  consumer_group_strategy: header-based
  consumer_groups:
    - order-processor
    - payment-handler
  reset_consumer_offsets: true
  offset_report: ./offset-mapping.json
```

### Performance Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `max_concurrent_partitions` | int | No | `4` | Concurrent partition restores |
| `produce_batch_size` | int | No | `1000` | Records per produce batch |
| `rate_limit_records_per_sec` | int | No | - | Rate limit (records/sec) |

```yaml
restore:
  max_concurrent_partitions: 8
  produce_batch_size: 500
  rate_limit_records_per_sec: 10000
```

### Resumable Restores

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `checkpoint_state` | string | No | - | Path to checkpoint file |
| `checkpoint_interval_secs` | int | No | `60` | Checkpoint save interval |

```yaml
restore:
  checkpoint_state: ./restore-checkpoint.json
  checkpoint_interval_secs: 30
```

### Topic Auto-Creation

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `create_topics` | bool | No | `false` | Auto-create missing topics before restore |
| `default_replication_factor` | int | No | `-1` | Replication factor for created topics (-1 = broker default) |

When `create_topics` is enabled:
- Missing target topics are created automatically before restore begins
- Partition count is derived from the backup manifest
- Replication factor can be specified or left to broker default
- The engine waits for topic metadata to propagate before producing

```yaml
restore:
  # Enable auto-creation of missing topics
  create_topics: true

  # Use replication factor of 3 for created topics
  default_replication_factor: 3

  # Topic mapping with auto-creation
  topic_mapping:
    orders: orders-restored      # Will be created if missing
    payments: payments-restored  # Will be created if missing
```

> **Note:** The number of partitions for auto-created topics is determined from the backup manifest. If the source topic had 6 partitions, the target topic will be created with 6 partitions.

### Complete Restore Example

```yaml
mode: restore
backup_id: "production-backup-2025-01-15"

target:
  bootstrap_servers:
    - kafka-restore.example.com:9092
  topics:
    include:
      - "orders-*"
      - "payments"
    exclude:
      - "*-test"

storage:
  backend: s3
  bucket: my-kafka-backups
  region: us-east-1

restore:
  # Dry run first
  dry_run: false

  # Time window (Jan 15, 2025 10:00-11:00 UTC)
  time_window_start: 1736935200000
  time_window_end: 1736938800000

  # Partition filtering
  source_partitions:
    - 0
    - 1

  # Topic remapping (with auto-creation)
  create_topics: true
  default_replication_factor: 3
  topic_mapping:
    orders: orders-recovered
    payments: payments-recovered

  # Consumer offset management
  consumer_group_strategy: header-based
  include_original_offset_header: true
  consumer_groups:
    - order-processor
  reset_consumer_offsets: false
  offset_report: ./offset-report.json

  # Performance
  max_concurrent_partitions: 4
  produce_batch_size: 1000

  # Checkpointing
  checkpoint_state: ./restore-checkpoint.json
  checkpoint_interval_secs: 60
```

---

## Environment Variables

Configuration values can reference environment variables using `${VAR_NAME}` syntax:

```yaml
source:
  sasl_password: ${KAFKA_PASSWORD}
```

### Common Environment Variables

| Variable | Description |
|----------|-------------|
| `KAFKA_PASSWORD` | Kafka SASL password |
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_REGION` | AWS region |
| `AZURE_STORAGE_ACCOUNT` | Azure storage account |
| `AZURE_STORAGE_KEY` | Azure storage key |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to GCP credentials |
| `RUST_LOG` | Logging level (debug, info, warn, error) |

---

## CLI Arguments

CLI arguments override configuration file values.

### Global Arguments

| Argument | Short | Description |
|----------|-------|-------------|
| `--config` | `-c` | Path to configuration file |
| `--verbose` | `-v` | Enable verbose output |
| `--format` | `-f` | Output format: text, json, yaml |
| `--help` | `-h` | Show help |
| `--version` | `-V` | Show version |

### Backup Command

```bash
kafka-backup backup --config backup.yaml [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `--config` | Path to backup configuration file |

### Restore Command

```bash
kafka-backup restore --config restore.yaml [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `--config` | Path to restore configuration file |

### List Command

```bash
kafka-backup list --path /path/to/storage [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `--path` | Path to storage location |
| `--format` | Output format |

### Describe Command

```bash
kafka-backup describe --path /path/to/storage --backup-id BACKUP_ID [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `--path` | Path to storage location |
| `--backup-id` | Backup identifier |
| `--format` | Output format |

### Validate Command

```bash
kafka-backup validate --path /path/to/storage --backup-id BACKUP_ID [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `--path` | Path to storage location |
| `--backup-id` | Backup identifier |
| `--deep` | Perform deep validation |

### Validate-Restore Command

```bash
kafka-backup validate-restore --config restore.yaml [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `--config` | Path to restore configuration file |

### Show-Offset-Mapping Command

```bash
kafka-backup show-offset-mapping --path /path/to/storage --backup-id BACKUP_ID [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `--path` | Path to storage location |
| `--backup-id` | Backup identifier |
| `--format` | Output format |

---

## Configuration Precedence

Values are resolved in this order (later overrides earlier):

1. Default values
2. Configuration file
3. Environment variables (in config file)
4. CLI arguments

---

## Validation

The system validates configuration on load:

- Required fields must be present
- `backup_id` must be non-empty
- Bootstrap servers must be specified
- Storage backend must be valid
- Time windows must have start < end
- Partition mappings must be valid
- Consumer group strategy must be recognized

### Example Validation Error

```
Error: Invalid configuration
  - time_window_start (1736985600000) must be before time_window_end (1736899200000)
  - source_partitions contains negative value: -1
```

---

## Sample Configurations

### Minimal Backup

```yaml
mode: backup
backup_id: "quick-backup"

source:
  bootstrap_servers:
    - localhost:9092
  topics:
    include:
      - my-topic

storage:
  backend: filesystem
  path: ./backups
```

### Production Backup to S3

```yaml
mode: backup
backup_id: "prod-backup-$(date +%Y%m%d)"

source:
  bootstrap_servers:
    - kafka1.prod:9092
    - kafka2.prod:9092
    - kafka3.prod:9092
  topics:
    include:
      - "*"
    exclude:
      - "__*"
      - "*-test"
  security_protocol: SASL_SSL
  sasl_mechanism: SCRAM-SHA-512
  sasl_username: backup-service
  sasl_password: ${KAFKA_PASSWORD}

storage:
  backend: s3
  bucket: company-kafka-backups
  region: us-east-1
  prefix: production/

backup:
  compression: zstd
  continuous: true
  checkpoint_interval_secs: 30
  max_concurrent_partitions: 8
```

### Disaster Recovery Restore

```yaml
mode: restore
backup_id: "prod-backup-20250115"

target:
  bootstrap_servers:
    - kafka-dr.example.com:9092
  topics:
    include:
      - "*"

storage:
  backend: s3
  bucket: company-kafka-backups
  region: us-east-1
  prefix: production/

restore:
  dry_run: false
  include_original_offset_header: true
  consumer_group_strategy: header-based
  max_concurrent_partitions: 8
  checkpoint_state: ./dr-restore-checkpoint.json
```

### Environment Cloning

```yaml
mode: restore
backup_id: "prod-backup-20250115"

target:
  bootstrap_servers:
    - kafka-staging.example.com:9092
  topics:
    include:
      - orders
      - payments
      - users

storage:
  backend: s3
  bucket: company-kafka-backups
  region: us-east-1

restore:
  topic_mapping:
    orders: orders-staging
    payments: payments-staging
    users: users-staging
  consumer_group_strategy: skip
  max_concurrent_partitions: 4
```
