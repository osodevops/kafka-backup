<p align="center">
  <h1 align="center">kafka-backup</h1>
  <p align="center">
    High-performance Kafka backup and restore with point-in-time recovery
  </p>
</p>

<p align="center">
  <a href="https://github.com/osodevops/kafka-backup/actions/workflows/test.yml">
    <img src="https://github.com/osodevops/kafka-backup/actions/workflows/test.yml/badge.svg" alt="CI Status">
  </a>
  <a href="https://github.com/osodevops/kafka-backup/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License: MIT">
  </a>
  <a href="https://github.com/osodevops/kafka-backup/releases">
    <img src="https://img.shields.io/github/v/release/osodevops/kafka-backup" alt="Release">
  </a>
</p>

---

**kafka-backup** is a production-grade tool written in Rust for backing up and restoring Apache Kafka topics to cloud storage or local filesystem. It supports **point-in-time recovery (PITR)** with millisecond precision and solves the **consumer group offset discontinuity problem** when restoring to different clusters.

## Features

- **Multi-cloud storage** — S3, Azure Blob, GCS, or local filesystem
- **Point-in-time recovery** — Restore to any millisecond within your backup window
- **Consumer offset recovery** — Automatically reset consumer group offsets after restore
- **Compliance evidence** — Signed JSON/PDF reports for SOX, CMMC, and GDPR auditors
- **High performance** — 100+ MB/s throughput with zstd/lz4 compression
- **Incremental backups** — Resume from where you left off
- **Topic filtering** — Wildcard patterns for include/exclude
- **Auto-repartitioning** — Restore to clusters with different partition counts
- **Deployment agnostic** — Bare metal, VM, Docker, or Kubernetes

## Installation

Download the latest binary from the [GitHub Releases](https://github.com/osodevops/kafka-backup/releases) page.

### macOS (Homebrew)

```bash
brew install osodevops/tap/kafka-backup
```

### Linux / macOS (Shell Installer)

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/osodevops/kafka-backup/releases/latest/download/kafka-backup-cli-installer.sh | sh
```

### Linux (Manual)

Download the appropriate binary for your architecture from [releases](https://github.com/osodevops/kafka-backup/releases):

```bash
# Example for x86_64
curl -LO https://github.com/osodevops/kafka-backup/releases/latest/download/kafka-backup-cli-x86_64-unknown-linux-gnu.tar.xz
tar -xJf kafka-backup-cli-x86_64-unknown-linux-gnu.tar.xz
sudo mv kafka-backup /usr/local/bin/
```

### Windows (PowerShell Installer)

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://github.com/osodevops/kafka-backup/releases/latest/download/kafka-backup-cli-installer.ps1 | iex"
```

### Windows (Scoop)

We use [Scoop](https://scoop.sh/) to distribute releases for Windows.

```powershell
scoop bucket add oso https://github.com/osodevops/scoop-bucket.git
scoop install kafka-backup
```

### Docker

```bash
docker pull osodevops/kafka-backup
docker run --rm -v /path/to/config:/config osodevops/kafka-backup backup --config /config/backup.yaml
```

See the image on [Docker Hub](https://hub.docker.com/r/osodevops/kafka-backup).

### From Source

```bash
git clone https://github.com/osodevops/kafka-backup.git
cd kafka-backup
cargo build --release
```

Binary location: `target/release/kafka-backup`

## Try It Yourself

Want to see kafka-backup in action? Check out our **[Demo Repository](https://github.com/osodevops/kafka-backup-demos)** with ready-to-run examples:

```bash
git clone https://github.com/osodevops/kafka-backup-demos
cd kafka-backup-demos
docker compose up -d
cd cli/backup-basic && ./demo.sh
```

**Available demos:**
- **Backup & Restore** — Full backup/restore cycle with S3/MinIO
- **Point-in-Time Recovery** — Restore to any millisecond with rollback safety
- **Large Messages** — Handle 1-10MB payloads with compression comparisons
- **Offset Management** — Consumer group offset snapshots and resets
- **Kafka Streams** — PITR with stateful stream processing apps
- **Spring Boot** — Microservice integration patterns
- **Benchmarks** — Throughput, latency, and scaling tests

## Quick Start

### Backup

Create a backup configuration file `backup.yaml`:

```yaml
mode: backup
backup_id: "daily-backup-001"

source:
  bootstrap_servers: ["kafka:9092"]
  topics:
    include: ["orders-*", "payments-*"]
    exclude: ["*-internal"]

storage:
  backend: s3
  bucket: my-kafka-backups
  region: us-east-1
  prefix: prod/

backup:
  compression: zstd
  segment_max_bytes: 134217728  # 128MB
```

Run the backup:
```bash
kafka-backup backup --config backup.yaml
```

### Restore

Create a restore configuration file `restore.yaml`:

```yaml
mode: restore
backup_id: "daily-backup-001"

target:
  bootstrap_servers: ["kafka-dr:9092"]

storage:
  backend: s3
  bucket: my-kafka-backups
  region: us-east-1
  prefix: prod/

restore:
  # Point-in-time recovery (optional)
  time_window_start: 1736899200000  # epoch millis
  time_window_end: 1736985600000

  # Remap topics (optional)
  topic_mapping:
    orders-prod: orders-recovered
```

Run the restore:
```bash
kafka-backup restore --config restore.yaml
```

## Why OSO Kafka Backup?

| Feature | OSO Kafka Backup | itadventurer/kafka-backup | Kannika Armory | Confluent Replicator | MirrorMaker 2 | Lenses K2K |
|---------|:---:|:---:|:---:|:---:|:---:|:---:|
| **PITR** | Yes (ms precision) | No | Yes (ms precision) | No | No | No |
| **Cloud storage** | S3, Azure, GCS | Filesystem only | S3, Azure, GCS & K8s PV | No | No | No |
| **Offset recovery** | Yes (multi-strategy) | Partial | Yes | Limited | Limited | Limited |
| **Compliance evidence** | Yes (signed JSON/PDF) | No | No | No | No | No |
| **SOX/CMMC/GDPR mapping** | Yes (automatic) | No | No | No | No | No |
| **Air-gapped DR** | Yes | Partial | Yes (commercial) | No | No | No |
| **Auto-repartitioning** | Yes | No | No | No | No | No |
| **Platform dependency** | None (single binary) | Kafka Connect | K8s platform | Confluent Platform | MM2 framework | Lenses platform |
| **License** | MIT (OSS) | MIT (unmaintained) | Commercial | Commercial | Apache 2.0 | Commercial |

> 📖 **[See the full comparison guide](docs/comparison.md)** for detailed analysis of each solution.

**OSO Kafka Backup is the only option that combines millisecond‑precision PITR, cloud‑native cold backups, and automated consumer offset recovery in a single, OSS‑friendly binary.**

Competing tools either:
- Only do filesystem backups
- Are commercial platforms you have to buy and operate
- Are replication tools that don't give you true, air‑gapped backups

This makes OSO Kafka Backup the highest‑leverage choice for teams that need real Kafka disaster recovery without adopting a whole new proprietary platform.

## When NOT to use kafka-backup

- **Real-time replication** — Use MirrorMaker 2 for active-active or active-passive replication
- **Schema evolution** — kafka-backup preserves bytes exactly; it doesn't handle schema registry
- **Infinite retention** — For long-term archival, consider Tiered Storage (KIP-405)

## Documentation

**Full documentation is available at [osodevops.github.io/kafka-backup-docs](https://osodevops.github.io/kafka-backup-docs/)**

| Document | Description |
|----------|-------------|
| [Quick Start](docs/quickstart.md) | Get started in 5 minutes |
| [Configuration Reference](docs/configuration.md) | All configuration options |
| [Storage Guide](docs/storage_guide.md) | S3, Azure, GCS setup |
| [Restore Guide](docs/restore_guide.md) | Restore scenarios and examples |
| [Offset Recovery](docs/Three_Phase_Restore_Guide.md) | Consumer offset strategies |
| [Offset Reset & Rollback](docs/offset-reset-guide.md) | Bulk offset resets and rollback safety net |

## CLI Reference

```bash
# Backup & restore
kafka-backup backup --config backup.yaml
kafka-backup restore --config restore.yaml
kafka-backup three-phase-restore --config restore.yaml   # restore + offset recovery

# Inspect backups
kafka-backup list --path s3://bucket/prefix
kafka-backup describe --path s3://bucket --backup-id backup-001 --format json
kafka-backup status --config backup.yaml --watch          # live monitoring
kafka-backup validate --path s3://bucket --backup-id backup-001 --deep

# Restore validation (dry-run)
kafka-backup validate-restore --config restore.yaml

# Offset mapping & consumer offset management
kafka-backup show-offset-mapping --path s3://bucket --backup-id backup-001 --format json
kafka-backup offset-reset plan --path s3://bucket --backup-id backup-001 --groups my-group
kafka-backup offset-reset execute --path s3://bucket --backup-id backup-001 --groups my-group
kafka-backup offset-reset script --path s3://bucket --backup-id backup-001 --groups my-group

# Bulk parallel offset reset (~50x faster)
kafka-backup offset-reset-bulk --path s3://bucket --backup-id backup-001 \
  --groups group1,group2 --bootstrap-servers kafka:9092

# Offset snapshots & rollback (safety net)
kafka-backup offset-rollback snapshot --path s3://bucket --groups my-group --bootstrap-servers kafka:9092
kafka-backup offset-rollback list --path s3://bucket
kafka-backup offset-rollback rollback --path s3://bucket --snapshot-id <id> --bootstrap-servers kafka:9092

# Validation & compliance evidence
kafka-backup validation run --config validation.yaml
kafka-backup validation run --config validation.yaml --triggered-by "KPMG Q1 audit"
kafka-backup validation evidence-list --path s3://bucket
kafka-backup validation evidence-get --path s3://bucket --report-id <id> --format pdf --output report.pdf
kafka-backup validation evidence-verify --report report.json --signature report.sig --public-key key.pem
```

### Backup Validation & Compliance Evidence

Validate restored data against the original backup and generate signed evidence reports for auditors:

```yaml
# validation.yaml
backup_id: "production-daily-001"
storage:
  backend: s3
  bucket: my-kafka-backups
target:
  bootstrap_servers: ["restored-kafka:9092"]
checks:
  message_count:
    enabled: true
    mode: exact
  offset_range:
    enabled: true
evidence:
  formats: [json, pdf]
  signing:
    enabled: true
    private_key_path: "/etc/kafka-backup/signing-key.pem"
  storage:
    prefix: "evidence-reports/"
    retention_days: 2555  # 7 years (SOX)
notifications:
  slack:
    webhook_url: "https://hooks.slack.com/services/..."
```

**Validation checks:** MessageCountCheck, OffsetRangeCheck, ConsumerGroupOffsetCheck, CustomWebhookCheck

**Evidence outputs:** JSON (machine-readable) + PDF (auditor-ready) + ECDSA-P256-SHA256 signatures

**Compliance mappings:** SOX ITGC, CMMC RE.3.139, GDPR Article 32 — automatically included in every report

> 📖 **[Full validation & compliance guide](https://osodevops.github.io/kafka-backup-docs/guides/validation-compliance)**

## Storage Layout

Backups are stored in a structured format:

```
s3://kafka-backups/
└── {prefix}/
    └── {backup_id}/
        ├── manifest.json           # Backup metadata
        ├── state/
        │   └── offsets.db          # Checkpoint state (synced from local)
        └── topics/
            └── {topic}/
                └── partition={id}/
                    ├── segment-0001.zst
                    └── segment-0002.zst
```

A local SQLite offset database is maintained at `$TMPDIR/{backup_id}-offsets.db` (configurable via `offset_storage.db_path`) and periodically synced to remote storage for durability.

## Metrics & Monitoring

kafka-backup exposes Prometheus metrics at `/metrics` for monitoring backup operations:

```yaml
# Enable metrics in your config
metrics:
  enabled: true
  port: 8080
```

**Key metrics:**
- `kafka_backup_lag_records` — Consumer lag per partition
- `kafka_backup_records_total` — Total records backed up
- `kafka_backup_compression_ratio` — Compression efficiency
- `kafka_backup_storage_write_latency_seconds` — Storage I/O latency
- `kafka_backup_validation_checks_passed_total` — Validation checks passed
- `kafka_backup_validation_consecutive_failures` — Consecutive validation failures (SLO alerting)

A complete **Grafana + Prometheus monitoring stack** is available in the [demos repository](https://github.com/osodevops/kafka-backup-demos/tree/main/monitoring-stack):

```bash
cd kafka-backup-demos/monitoring-stack
docker-compose -f docker-compose.metrics.yml up -d
# Grafana at http://localhost:3000 (admin/admin)
```

> 📖 **[Full metrics reference](https://osodevops.github.io/kafka-backup-docs/docs/reference/metrics)**

## Performance

| Metric | Target |
|--------|--------|
| Throughput | 100+ MB/s per partition |
| Checkpoint latency | <100ms p99 |
| Compression ratio | 3-5x (typical JSON/Avro) |
| Memory usage | <500MB for 4 partitions |

## Building from Source

**Requirements:**
- Rust 1.75+
- OpenSSL development libraries

```bash
# Clone the repository
git clone https://github.com/osodevops/kafka-backup.git
cd kafka-backup

# Build release binary
cargo build --release

# Run tests
cargo test

# Run with debug logging
RUST_LOG=debug cargo run -p kafka-backup-cli -- --help
```

## Running Tests

```bash
# Unit tests
cargo test

# Integration tests (requires Docker)
cargo test --test integration_suite_tests

# All tests including ignored (Docker required)
cargo test -- --include-ignored

# With coverage
cargo tarpaulin --out Html
```

## Project Structure

```
kafka-backup/
├── crates/
│   ├── kafka-backup-core/        # Core library
│   │   ├── src/
│   │   │   ├── backup/           # Backup engine
│   │   │   ├── restore/          # Restore engine + offset recovery
│   │   │   ├── validation/       # Validation check framework
│   │   │   ├── evidence/         # Evidence reports, signing, PDF
│   │   │   ├── notification/     # Slack, PagerDuty webhooks
│   │   │   ├── kafka/            # Kafka protocol client
│   │   │   ├── storage/          # S3, Azure, GCS, filesystem
│   │   │   ├── metrics/          # Prometheus metrics
│   │   │   └── compression.rs
│   │   └── tests/                # Unit, integration, chaos tests
│   └── kafka-backup-cli/         # CLI binary
├── config/                       # Example configs
└── docs/                         # Documentation
```

## Looking for Enterprise Apache Kafka Support?

[OSO](https://oso.sh) engineers are solely focused on deploying, operating, and maintaining Apache Kafka platforms. If you need SLA-backed support or advanced features for compliance and security, our **Enterprise Edition** extends the core tool with capabilities designed for large-scale, regulated environments.

### OSO Kafka Backup: Enterprise Edition

| Feature Category | Enterprise Capability |
|------------------|----------------------|
| **Security & Compliance** | AES-256 Encryption (client-side encryption at rest) |
| | GDPR Compliance Tools (right-to-be-forgotten, PII masking) |
| | Audit Logging (comprehensive trail of all backup/restore ops) |
| | Role-Based Access Control (granular permissions) |
| **Advanced Integrations** | Schema Registry Integration (backup & restore schemas with ID remapping) |
| | Secrets Management (Vault / AWS Secrets Manager integration) |
| | SSO / OIDC (Okta, Azure AD, Google Auth) |
| **Scale & Operations** | Multi-Region Replication (active-active disaster recovery) |
| | Log Shipping (Datadog, Splunk, Grafana Loki) |
| | Advanced Metrics & Dashboard (throughput, latency, drill-down UI) |
| **Support** | 24/7 SLA-Backed Support & dedicated Kafka consulting |

Need help resolving operational issues or planning a failover strategy? Our team of experts can recover data from non-responsive clusters, fix configuration errors, and get your environment operational as fast as possible.

👉 **[Talk with an expert today](https://oso.sh/contact/)** or email us at **enquiries@oso.sh**.

## Contributing

We welcome contributions of all kinds!

- **Report Bugs:** Found a bug? Open an [issue on GitHub](https://github.com/osodevops/kafka-backup/issues).
- **Suggest Features:** Have an idea? [Request a feature](https://github.com/osodevops/kafka-backup/issues/new?template=feature.yml).
- **Contribute Code:** Check out our [good first issues](https://github.com/osodevops/kafka-backup/labels/good%20first%20issue) for beginner-friendly tasks.
- **Improve Docs:** Help us improve the documentation by submitting pull requests.

See [CLAUDE.md](CLAUDE.md) for development guidelines and architecture overview.

## License

kafka-backup is licensed under the [MIT License](LICENSE) © [OSO](https://oso.sh).

## Acknowledgments

Built with these excellent Rust crates:
- [kafka-protocol](https://crates.io/crates/kafka-protocol) — Kafka protocol implementation
- [object_store](https://crates.io/crates/object_store) — Cloud storage abstraction
- [tokio](https://tokio.rs) — Async runtime
- [zstd](https://crates.io/crates/zstd) — Compression

---

<p align="center">
  Made with ❤️ by <a href="https://oso.sh">OSO</a>
</p>
