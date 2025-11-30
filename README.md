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
- **High performance** — 100+ MB/s throughput with zstd/lz4 compression
- **Incremental backups** — Resume from where you left off
- **Topic filtering** — Wildcard patterns for include/exclude
- **Deployment agnostic** — Bare metal, VM, Docker, or Kubernetes

## Quick Start

### Installation

**From source:**
```bash
git clone https://github.com/osodevops/kafka-backup.git
cd kafka-backup
cargo build --release
```

**Binary location:** `target/release/kafka-backup`

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

| Feature | OSO Kafka Backup | itadventurer/kafka-backup | Kannika Armory | Confluent Replicator | MirrorMaker 2 |
|---------|------------------|---------------------------|----------------|---------------------|---------------|
| **PITR** | Yes (ms precision) | No | Yes (proprietary UI) | No | No |
| **Cloud storage** | S3, Azure, GCS | Filesystem only | K8s PV / enterprise | No | No |
| **Offset recovery** | Yes (multi-strategy) | Partial | Yes | Limited | Limited |
| **Air-gapped DR** | Yes | Partial | Yes (commercial) | No | No |
| **Platform dependency** | None (single binary) | Kafka Connect | K8s platform | Confluent Platform | MM2 framework |
| **Operational simplicity** | High | Medium | Medium/Low | Medium | Low |
| **License** | MIT (OSS) | MIT (unmaintained) | Commercial | Commercial | Apache 2.0 |

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

| Document | Description |
|----------|-------------|
| [Quick Start](docs/quickstart.md) | Get started in 5 minutes |
| [Configuration Reference](docs/configuration.md) | All configuration options |
| [Storage Guide](docs/storage_guide.md) | S3, Azure, GCS setup |
| [Restore Guide](docs/restore_guide.md) | Restore scenarios and examples |
| [Offset Recovery](docs/Three_Phase_Restore_Guide.md) | Consumer offset strategies |
| [Architecture](CLAUDE.md) | Technical deep-dive |

## CLI Reference

```bash
# Backup operations
kafka-backup backup --config backup.yaml

# Restore operations
kafka-backup restore --config restore.yaml

# List available backups
kafka-backup list --path s3://bucket/prefix

# Describe a specific backup
kafka-backup describe --path s3://bucket --backup-id backup-001 --format json

# Validate backup integrity
kafka-backup validate --path s3://bucket --backup-id backup-001 --deep

# Consumer offset management
kafka-backup offset-reset plan --path s3://bucket --backup-id backup-001 --groups my-group
kafka-backup offset-reset execute --path s3://bucket --backup-id backup-001 --groups my-group
```

## Storage Layout

Backups are stored in a structured format:

```
s3://kafka-backups/
└── {prefix}/
    └── {backup_id}/
        ├── manifest.json           # Backup metadata
        ├── state/
        │   └── offsets.db          # Checkpoint state
        └── topics/
            └── {topic}/
                └── partition={id}/
                    ├── segment-0001.zst
                    └── segment-0002.zst
```

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
│   ├── kafka-backup-core/    # Core library
│   │   ├── src/
│   │   │   ├── backup/       # Backup engine
│   │   │   ├── restore/      # Restore engine
│   │   │   ├── kafka/        # Kafka protocol client
│   │   │   ├── storage/      # Storage backends
│   │   │   └── compression.rs
│   │   └── tests/            # Test suites
│   └── kafka-backup-cli/     # CLI binary
├── config/                   # Example configs
└── docs/                     # Documentation
```

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting a PR.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Security

For security vulnerabilities, please email security@osodevops.io instead of opening a public issue.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

Built with these excellent Rust crates:
- [kafka-protocol](https://crates.io/crates/kafka-protocol) — Kafka protocol implementation
- [object_store](https://crates.io/crates/object_store) — Cloud storage abstraction
- [tokio](https://tokio.rs) — Async runtime
- [zstd](https://crates.io/crates/zstd) — Compression

---

<p align="center">
  Made with ❤️ by <a href="https://osodevops.io">OSO DevOps</a>
</p>
