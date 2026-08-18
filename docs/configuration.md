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
7. [Offset Storage Configuration](#offset-storage-configuration)
8. [Restore Configuration](#restore-configuration)
9. [Environment Variables](#environment-variables)
10. [CLI Arguments](#cli-arguments)

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
| `security.security_protocol` | string | No | `PLAINTEXT` | Security protocol |
| `security.sasl_mechanism` | string | No | - | SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) |
| `security.sasl_username` | string | No | - | SASL username |
| `security.sasl_password` | string | No | - | SASL password |
| `security.ssl_ca_location` | string | No | - | Path to CA certificate |
| `security.ssl_certificate_location` | string | No | - | Path to client certificate |
| `security.ssl_key_location` | string | No | - | Path to client key |

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
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: backup-user
    sasl_password: ${KAFKA_PASSWORD}
    ssl_ca_location: /etc/kafka/certs/ca.pem
```

### Connection Options

TCP-level settings for the source (and, for restore, the target) cluster.
All are optional and live under `source.connection` / `target.connection`.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `connection.tcp_keepalive` | bool | `true` | Send TCP keepalive probes so idle connections are not silently dropped by NATs, load balancers or cloud Kafka front-ends |
| `connection.keepalive_time_secs` | integer | `60` | Idle time before the first keepalive probe |
| `connection.keepalive_interval_secs` | integer | `20` | Interval between keepalive probes |
| `connection.tcp_nodelay` | bool | `true` | Disable Nagle's algorithm (lower request latency) |
| `connection.connections_per_broker` | integer | `4` | TCP connections kept per broker; concurrent partitions share them, so raise this on high-latency links |

```yaml
source:
  bootstrap_servers: ["kafka1.example.com:9092"]
  connection:
    tcp_keepalive: true
    keepalive_time_secs: 30
    keepalive_interval_secs: 10
    connections_per_broker: 8
```

#### Connection loss and automatic reconnect

A dropped or reset broker connection does not fail a run. When a request
fails with a connection-level error the client reconnects (TCP, TLS and SASL
again) and retries the request once, immediately; the backup fetch path,
restore produce path and record purging additionally retry up to 5 times with
linear back-off (0.5 s, 1 s, … 2.5 s), so a broker or proxy that resets
connections for a few seconds is ridden out. What counts as a connection-level
error is decided from the OS error *kind* and code — connection reset,
aborted or refused, broken pipe, not connected, timed out, unexpected EOF,
network/host unreachable, and the Windows Winsock equivalents (`10052`,
`10053`, `10054`, `10057`, `10058`, `10060`, `10061`) — not from the error
message text, which is localized and differs per platform.

You will see it in the logs as:

```
WARN kafka_backup_core::kafka::client: Connection error on Fetch request, reconnecting and retrying: Kafka error: Connection error during read response length (ConnectionReset): Connection reset by peer (os error 104)
WARN kafka_backup_core::kafka::partition_router: Connection error fetching orders/3 (attempt 1/5), retrying after 500ms: ...
```

Such errors are counted under `error_type="broker_connection"` in
`kafka_backup_errors_total`. If a run still fails after the retries, the
outage lasted longer than the retry window (roughly 8 s per request); check
broker availability and the keepalive settings above, and simply re-run — a
backup resumes from its checkpoint.

### TLS/SSL Configuration

kafka-backup supports TLS encryption for Kafka connections, including custom CA certificates and mutual TLS (mTLS) authentication.

#### Security Protocols

| Protocol | Description |
|----------|-------------|
| `PLAINTEXT` | No encryption (default) |
| `SSL` | TLS encryption |
| `SASL_PLAINTEXT` | SASL authentication without TLS |
| `SASL_SSL` | SASL authentication with TLS encryption |

#### Custom CA Certificates

When connecting to Kafka brokers that use internal or self-signed certificates, specify the CA certificate path:

```yaml
source:
  bootstrap_servers: ["kafka:9093"]
  security:
    security_protocol: SSL
    ssl_ca_location: /etc/kafka/certs/ca.pem
```

This is required when:
- Your Kafka cluster uses certificates signed by an internal CA
- You're using self-signed certificates
- The default system/webpki root certificates don't include your CA

#### Mutual TLS (mTLS)

For clusters requiring client certificate authentication, provide both the client certificate and private key:

```yaml
source:
  bootstrap_servers: ["kafka:9093"]
  security:
    security_protocol: SSL
    ssl_ca_location: /etc/kafka/certs/ca.pem
    ssl_certificate_location: /etc/kafka/certs/client.pem
    ssl_key_location: /etc/kafka/certs/client-key.pem
```

**Important:** Both `ssl_certificate_location` and `ssl_key_location` must be provided together for mTLS. Providing only one will result in an error.

#### SASL + SSL

Combine TLS encryption with SASL authentication:

```yaml
source:
  bootstrap_servers: ["kafka:9093"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: SCRAM-SHA-512
    sasl_username: myuser
    sasl_password: ${KAFKA_PASSWORD}
    ssl_ca_location: /etc/kafka/certs/ca.pem
```

#### Certificate Format

All certificate and key files must be in PEM format:
- CA certificate: Standard X.509 PEM certificate
- Client certificate: X.509 PEM certificate
- Private key: PKCS#1 (RSA), PKCS#8, or SEC1 (EC) PEM format

---

## Target Configuration

Configuration for the Kafka cluster to write to (restore mode).

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `bootstrap_servers` | list[string] | Yes | - | Kafka broker addresses |
| `topics.include` | list[string] | No | `["*"]` | Topics to restore (glob/regex patterns) |
| `topics.exclude` | list[string] | No | `[]` | Topics to exclude |
| `security.security_protocol` | string | No | `PLAINTEXT` | Security protocol |
| `security.sasl_mechanism` | string | No | - | SASL mechanism |
| `security.sasl_username` | string | No | - | SASL username |
| `security.sasl_password` | string | No | - | SASL password |
| `security.ssl_ca_location` | string | No | - | Path to CA certificate |
| `security.ssl_certificate_location` | string | No | - | Path to client certificate |
| `security.ssl_key_location` | string | No | - | Path to client key |

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
  security:
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
| `continuous` | bool | No | `false` | Run continuously (streaming replication) |
| `stop_at_current_offsets` | bool | No | `false` | Snapshot mode: capture HWMs at start, exit when caught up |
| `checkpoint_interval_secs` | int | No | `60` | Checkpoint interval |
| `segment_max_records` | int | No | `100000` | Max records per segment |
| `segment_max_bytes` | int | No | `104857600` | Max bytes per segment (100MB) |
| `segment_max_age_secs` | int | No | `3600` | Max segment age |
| `max_concurrent_partitions` | int | No | `8` | Maximum concurrent partition backups (limits parallelism) |
| `poll_interval_ms` | int | No | `100` | Delay between backup passes in continuous mode (milliseconds) |
| `fetch_max_bytes` | int | No | `1048576` | Max bytes per fetch |

### Performance Tuning

**`max_concurrent_partitions`**: Controls how many partitions are backed up simultaneously. Higher values increase throughput but may cause resource contention (CPU, memory, network, storage I/O). Default is 8.

**`poll_interval_ms`**: In continuous mode, this controls the delay between backup passes. Lower values reduce lag but increase CPU usage. Default is 100ms. Set to 0 for maximum throughput (no delay).

### Backup Modes

| Mode | Configuration | Behavior |
|------|---------------|----------|
| **One-shot** (default) | `continuous: false` | Backup current data and exit |
| **Continuous** | `continuous: true` | Run forever, backing up new data as it arrives |
| **Snapshot** | `stop_at_current_offsets: true` | Capture high watermarks at start, backup until caught up, then exit cleanly |

**Snapshot mode** (`stop_at_current_offsets: true`) is ideal for scheduled DR backups:
1. At startup, captures high watermarks for ALL partitions (atomic snapshot point)
2. Backs up all data up to those fixed offsets
3. Exits cleanly with exit code 0 when complete

This ensures consistent "point-in-time" snapshots for disaster recovery, even with high-throughput topics.

> **Note:** `stop_at_current_offsets` is incompatible with `continuous: true`. Use snapshot mode for scheduled backups (CronJobs), and continuous mode for streaming replication.

#### Retention deleting data before it is fetched

The offsets a backup starts from are captured before the fetch happens — for
every partition up front in snapshot mode, and from the checkpoint on resume.
With many partitions, bounded `max_concurrent_partitions`, or a long gap
between runs, retention can delete those offsets before the partition's turn
comes up. The broker then rejects the fetch with `OFFSET_OUT_OF_RANGE`.

The backup does **not** fail in that case. It re-reads the partition's current
log start offset, resumes from there, and records the skipped range as an
**offset gap** in the manifest:

```json
{
  "partition_id": 7,
  "segments": [ ... ],
  "gaps": [
    { "start_offset": 0, "end_offset": 100000,
      "reason": "offset_out_of_range", "detected_at": 1755522569000 }
  ]
}
```

Records inside a gap were deleted from the source before they could be
fetched and cannot be recovered. `kafka-backup validate` and
`kafka-backup describe` list recorded gaps, and the
`kafka_backup_offset_gaps_total` / `kafka_backup_offsets_skipped_total`
counters let you alert on them. If gaps appear regularly, raise
`max_concurrent_partitions`, shorten the interval between runs, or increase
the topic's retention so a full run fits inside the retention window.

### Compression Options

| Algorithm | Description |
|-----------|-------------|
| `none` | No compression |
| `zstd` | Zstandard (default, best ratio) |
| `lz4` | LZ4 (faster, lower ratio) |
| `gzip` | Gzip (widely compatible) |
| `snappy` | Snappy (balanced) |

### Examples

**Continuous Backup (Streaming Replication):**
```yaml
backup:
  compression: zstd
  continuous: true
  checkpoint_interval_secs: 30
  segment_max_records: 50000
  segment_max_bytes: 52428800  # 50MB
  segment_max_age_secs: 1800   # 30 minutes
  max_concurrent_partitions: 8 # Limit concurrent partition tasks
  poll_interval_ms: 100        # Delay between backup passes (lower = less lag)
  fetch_max_bytes: 5242880     # 5MB
```

**Snapshot Backup (DR/Scheduled):**
```yaml
backup:
  compression: zstd
  stop_at_current_offsets: true  # Exit when caught up
  include_offset_headers: true    # For offset remapping on restore
  checkpoint_interval_secs: 30
  segment_max_bytes: 134217728    # 128MB
```

---

## Offset Storage Configuration

Configuration for the local SQLite database used to track backup progress. When present, this enables resumable incremental backups in **any** mode (one-shot, snapshot, or continuous). For continuous mode, the offset store is created automatically even without this section.

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `offset_storage.backend` | string | No | `sqlite` | Storage backend: `sqlite` or `memory` |
| `offset_storage.db_path` | string | No | `$TMPDIR/{backup_id}-offsets.db` | Path to local SQLite database file |
| `offset_storage.s3_key` | string | No | - | Remote storage key for syncing the database |
| `offset_storage.sync_interval_secs` | int | No | `30` | How often to sync the local DB to remote storage |

The offset store is created when `continuous: true` is set **or** when `offset_storage` is explicitly configured. This allows incremental one-shot and snapshot backups by adding the `offset_storage` section to your config:

```yaml
# Incremental one-shot backup (resume from last run):
backup:
  compression: zstd
  stop_at_current_offsets: true

offset_storage:
  db_path: /data/offsets.db
  sync_interval_secs: 30
```

### Default Behavior

By default, the offset database is created in the system temp directory (`$TMPDIR` or `/tmp`). This works out of the box on most systems, including Kubernetes pods with `readOnlyRootFilesystem: true` (where `/tmp` is typically a tmpfs).

### Kubernetes Deployment

For Kubernetes pods with `readOnlyRootFilesystem: true`, ensure `/tmp` is writable by mounting an `emptyDir` volume:

```yaml
volumes:
  - name: tmp
    emptyDir: {}
containers:
  - name: kafka-backup
    volumeMounts:
      - name: tmp
        mountPath: /tmp
```

For persistent offset tracking across pod restarts (optional — offsets are also synced to remote storage), mount a volume and configure `db_path`:

```yaml
# In your backup config
offset_storage:
  db_path: /data/offsets.db
```

```yaml
# In your K8s pod spec
volumes:
  - name: data
    emptyDir: {}
containers:
  - name: kafka-backup
    volumeMounts:
      - name: data
        mountPath: /data
```

### Example

```yaml
offset_storage:
  backend: sqlite
  db_path: /data/my-backup-offsets.db
  sync_interval_secs: 60
```

---

## Metrics Configuration

Configuration for the Prometheus metrics and health check HTTP server.

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `metrics.enabled` | bool | No | `true` | Enable the metrics HTTP server |
| `metrics.port` | int | No | `8080` | Port for the metrics server |
| `metrics.bind_address` | string | No | `0.0.0.0` | Bind address (use `0.0.0.0` for Kubernetes Service routing) |
| `metrics.path` | string | No | `/metrics` | Path for the Prometheus metrics endpoint |
| `metrics.keep_alive_seconds` | int | No | `0` | Keep serving metrics for N seconds after one-shot backup or restore completion |
| `metrics.max_partition_labels` | int | No | `100` | Maximum unique topic/partition label sets; `0` disables the limit |

When enabled, the server exposes:
- `GET /metrics` — Prometheus/OpenMetrics scrape endpoint
- `GET /health` — Liveness check (returns 200 OK)

For short-lived CronJob backups or restores, set `keep_alive_seconds` to at
least 2x your Prometheus scrape interval so Prometheus has more than one chance
to scrape the final metrics snapshot. For example, use `60` to `90` seconds
with a 30 second scrape interval. In Kubernetes, ensure
`terminationGracePeriodSeconds` and any Job `activeDeadlineSeconds` account for
this extra serving window.

### Example

```yaml
metrics:
  enabled: true
  port: 8080
  bind_address: "0.0.0.0"   # Required for Kubernetes Service routing
  path: "/metrics"
  keep_alive_seconds: 90     # Optional: useful for short-lived CronJobs
  max_partition_labels: 100  # Use 0 only when unbounded cardinality is acceptable
```

> **Kubernetes note:** `bind_address: "0.0.0.0"` is required for Kubernetes
> Service routing. `keep_alive_seconds` extends the pod lifetime after work
> completes; keep it within the pod termination grace period.

### Key Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `kafka_backup_lag_records` | Gauge | Consumer lag per topic/partition |
| `kafka_backup_lag_records_sum` | Gauge | Current lag summed across all partitions |
| `kafka_backup_snapshot_records_target` | Gauge | Total captured snapshot offset span |
| `kafka_backup_snapshot_records_remaining` | Gauge | Captured snapshot offset span still to process |
| `kafka_backup_records_total` | Counter | Total records backed up |
| `kafka_backup_bytes_total` | Counter | Total bytes backed up |
| `kafka_backup_offset_gaps_total` | Counter | Offset ranges skipped because the source no longer had the records (see [retention gaps](#retention-deleting-data-before-it-is-fetched)) |
| `kafka_backup_offsets_skipped_total` | Counter | Source offsets skipped across all recorded gaps |
| `kafka_backup_compression_ratio` | Gauge | Compression efficiency |
| `kafka_backup_storage_write_latency_seconds` | Histogram | Storage write latency |
| `kafka_backup_storage_write_bytes_total` | Counter | Storage I/O bytes |
| `kafka_backup_errors_total` | Counter | Errors by type |

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
| `reset_consumer_offsets` | bool | No | `false` | Commit translated offsets for `consumer_groups` on the target after the data restore (Phase 3) |
| `auto_consumer_groups` | bool | No | `false` | Load the groups (and their offsets) from the backup's `consumer-groups-snapshot.json`; implies `reset_consumer_offsets` |
| `offset_report` | string | No | - | Path to write offset report |

`reset_consumer_offsets` / `auto_consumer_groups` are honoured by
`kafka-backup restore` and `kafka-backup three-phase-restore` alike: the data
restore runs first, then the offsets are applied exactly once from the mapping
built during that run. A failed reset (e.g. the group still has active
consumers on the target — `error code 25`) makes the command exit non-zero.
See [Automatic Offset Reset](restore_guide.md#automatic-offset-reset-dangerous).

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
| `--format` | Output format (text, json, yaml, csv) |

### Status Command

```bash
kafka-backup status [OPTIONS]
```

| Argument | Description |
|----------|-------------|
| `--path` | Path to storage location (static inspection mode) |
| `--backup-id` | Backup ID to inspect |
| `--config` | Path to config file (live monitoring mode) |
| `--watch` | Enable continuous refresh (requires `--config`) |
| `--interval` | Refresh interval in seconds (default: 2) |
| `--db-path` | Path to the offset database |

### Three-Phase-Restore Command

```bash
kafka-backup three-phase-restore --config restore.yaml
```

| Argument | Description |
|----------|-------------|
| `--config` | Path to restore configuration file |

### Offset-Reset Command

```bash
kafka-backup offset-reset <plan|execute|script> [OPTIONS]
```

**Subcommands:**

| Subcommand | Description |
|------------|-------------|
| `plan` | Generate an offset reset plan (dry-run by default) |
| `execute` | Execute an offset reset plan |
| `script` | Generate a shell script for manual offset reset |

Common arguments for all subcommands:

| Argument | Description |
|----------|-------------|
| `--path` | Path to storage location |
| `--backup-id` | Backup ID with offset mapping |
| `--groups` | Consumer groups (comma-separated) |
| `--bootstrap-servers` | Kafka brokers (comma-separated) |

### Offset-Reset-Bulk Command

```bash
kafka-backup offset-reset-bulk --path /path --backup-id ID --groups g1,g2 --bootstrap-servers kafka:9092
```

| Argument | Description |
|----------|-------------|
| `--path` | Path to storage location |
| `--backup-id` | Backup ID with offset mapping |
| `--groups` | Consumer groups (comma-separated) |
| `--bootstrap-servers` | Kafka brokers (comma-separated) |
| `--max-concurrent` | Maximum parallel operations (default: 50) |
| `--max-retries` | Retry attempts (default: 3) |
| `--security-protocol` | Security protocol |
| `--format` | Output format (text, json) |

### Offset-Rollback Command

```bash
kafka-backup offset-rollback <snapshot|list|show|rollback|verify|delete> [OPTIONS]
```

| Subcommand | Description |
|------------|-------------|
| `snapshot` | Create a snapshot of current consumer group offsets |
| `list` | List available snapshots |
| `show` | Show details of a specific snapshot |
| `rollback` | Rollback offsets to a previous snapshot |
| `verify` | Verify current offsets match a snapshot |
| `delete` | Delete a snapshot |

### Validation Command

```bash
kafka-backup validation <run|evidence-list|evidence-get|evidence-verify> [OPTIONS]
```

| Subcommand | Description |
|------------|-------------|
| `run` | Run validation checks and generate evidence reports |
| `evidence-list` | List evidence reports in storage |
| `evidence-get` | Download an evidence report |
| `evidence-verify` | Verify an evidence report's cryptographic signature |

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
backup_id: "prod-backup-20250115"

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
  security:
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

# Optional: configure offset storage path (defaults to $TMPDIR)
# offset_storage:
#   db_path: /data/offsets.db
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
