# CLAUDE.md - Development Guide for OSO Kafka Backup & Restore

This document provides context for AI-assisted development on this project.

## Project Overview

**OSO Kafka Backup & Restore** is a high-performance, production-grade tool written in Rust for backing up and restoring Apache Kafka topics to cloud storage or local filesystem. It supports point-in-time recovery (PITR) with millisecond precision and handles consumer group offset recovery across different clusters.

### Key Problems Solved
- Durable backup of Kafka topics to S3, Azure Blob, GCS, or local filesystem
- Disaster recovery with exact data fidelity
- Point-in-time restore (PITR) with millisecond precision
- Consumer group offset recovery across different clusters (offset space discontinuity problem)
- Deployment-agnostic: works on bare metal, VM, Docker, Kubernetes

## Repository Structure

```
kafka-backup/
├── Cargo.toml                    # Workspace root
├── crates/
│   ├── kafka-backup-core/        # Core library (no K8s dependencies)
│   │   └── src/
│   │       ├── lib.rs            # Module exports
│   │       ├── backup/           # Backup engine
│   │       ├── restore/          # Restore engine + offset handling
│   │       ├── kafka/            # Kafka protocol client
│   │       ├── storage/          # Multi-backend storage abstraction
│   │       ├── segment/          # Record batching & serialization
│   │       ├── offset_store/     # SQLite-based offset tracking
│   │       ├── config.rs         # Configuration structures
│   │       ├── manifest.rs       # Backup manifest & reports
│   │       ├── compression.rs    # Compression algorithms
│   │       ├── error.rs          # Error types
│   │       ├── circuit_breaker.rs
│   │       ├── health.rs
│   │       └── metrics/          # Prometheus metrics
│   └── kafka-backup-cli/         # Binary CLI wrapper
│       └── src/
│           ├── main.rs           # CLI entry point
│           └── commands/         # Command implementations
├── config/                       # Config templates
└── docs/                         # Comprehensive documentation
```

## Build & Test Commands

```bash
# Build the project
cargo build

# Build release binary
cargo build --release

# Run tests
cargo test

# Run tests with output
cargo test -- --nocapture

# Check compilation without building
cargo check

# Format code
cargo fmt

# Run clippy lints
cargo clippy

# Run the CLI
cargo run -p kafka-backup-cli -- --help
cargo run -p kafka-backup-cli -- backup --config config/backup.yaml
cargo run -p kafka-backup-cli -- restore --config config/restore.yaml
```

## Architecture Overview

```
┌──────────────────────────────────────────────┐
│  CLI Layer (kafka-backup-cli)                │
│  - backup, restore, list, describe, validate │
│  - offset-reset, three-phase orchestration   │
└────────────────┬─────────────────────────────┘
                 │
┌────────────────▼─────────────────────────────┐
│  Core Engine (kafka-backup-core library)     │
├──────────────────────────────────────────────┤
│  ├─ Backup Engine                            │
│  │  ├─ Segment Writer (mini-batching)        │
│  │  ├─ Compression (zstd/lz4)                │
│  │  └─ Manifest Management                   │
│  ├─ Restore Engine                           │
│  │  ├─ Segment Reader & Decompression        │
│  │  ├─ PITR Filtering (time-window)          │
│  │  └─ Partition Remapping                   │
│  ├─ Offset Management                        │
│  │  ├─ SQLite-based Offset Store             │
│  │  ├─ Consumer Group Reset Executor         │
│  │  └─ Three-Phase Restore Orchestrator      │
│  ├─ Kafka Client                             │
│  │  ├─ kafka-protocol crate (raw protocol)   │
│  │  ├─ Metadata, Fetch, Produce APIs         │
│  └─ Health & Metrics                         │
└────────────────┬─────────────────────────────┘
                 │
┌────────────────▼─────────────────────────────┐
│  Pluggable Storage Layer (object_store)      │
│  S3 | Azure Blob | GCS | Filesystem | Memory │
└──────────────────────────────────────────────┘
```

## Key Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| `kafka-protocol` | 0.14 | Raw Kafka protocol (Metadata, Fetch, Produce APIs) |
| `tokio` | 1.x | Async runtime |
| `object_store` | 0.11 | S3, Azure, GCS, filesystem abstraction |
| `sqlx` | 0.8 | SQLite offset storage |
| `zstd` | 0.13 | Compression |
| `lz4_flex` | 0.11 | Fast compression |
| `clap` | 4.x | CLI argument parsing |
| `tracing` | 0.3 | Structured logging |
| `serde` / `serde_json` / `serde_yaml` | 1.x | Serialization |
| `thiserror` / `anyhow` | 2.x / 1.x | Error handling |
| `prometheus` | 0.13 | Metrics |
| `chrono` | 0.4 | Timestamps |

## Core Patterns

### Error Handling
```rust
// Custom error enum in error.rs
#[derive(Error, Debug)]
pub enum Error {
    #[error("Kafka error: {0}")]
    Kafka(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Configuration error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;
```

### Async Pattern
```rust
// All I/O is async using Tokio
pub async fn backup(&self) -> Result<()> {
    let tasks: Vec<_> = partitions
        .iter()
        .map(|p| self.replicate_partition(p.clone()))
        .collect();

    futures::future::join_all(tasks).await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    Ok(())
}
```

### Storage Trait
```rust
#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn put(&self, key: &str, data: Bytes) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Bytes>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn exists(&self, key: &str) -> Result<bool>;
    async fn delete(&self, key: &str) -> Result<()>;
}
```

## Key Types

```rust
// Manifest structures (manifest.rs)
pub struct BackupManifest {
    pub backup_id: String,
    pub created_at: i64,
    pub topics: Vec<TopicBackup>,
}

pub struct SegmentMetadata {
    pub key: String,
    pub start_offset: i64,
    pub end_offset: i64,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub record_count: i64,
}

// Config structures (config.rs)
pub struct BackupConfig { /* ... */ }
pub struct RestoreConfig { /* ... */ }
pub struct SourceConfig { /* ... */ }
pub struct StorageBackendConfig { /* ... */ }
```

## CLI Commands

```bash
# Backup
kafka-backup backup --config backup.yaml

# Restore
kafka-backup restore --config restore.yaml

# List backups
kafka-backup list --path s3://bucket/prefix

# Describe backup
kafka-backup describe --path s3://bucket --backup-id backup-001 --format json

# Validate backup integrity
kafka-backup validate --path s3://bucket --backup-id backup-001 --deep

# Offset management
kafka-backup offset-reset plan --path s3://bucket --backup-id backup-001 --groups my-group
kafka-backup offset-reset execute --path s3://bucket --backup-id backup-001 --groups my-group

# Three-phase restore (backup + restore + offset reset)
kafka-backup three-phase-restore --config restore.yaml
```

## Storage Data Layout

```
s3://kafka-backups/
└── {prefix}/
    └── {backup_id}/
        ├── manifest.json
        ├── state/
        │   └── offsets.db           # SQLite checkpoint
        └── topics/
            └── {topic}/
                └── partition={id}/
                    ├── segment-0001.zst
                    └── segment-0002.zst
```

## Configuration Format

```yaml
mode: backup | restore
backup_id: "my-backup"

source:  # For backup
  bootstrap_servers: ["kafka:9092"]
  topics:
    include: ["orders-*"]
    exclude: ["__*"]
  security_protocol: SASL_SSL
  sasl_mechanism: SCRAM-SHA-512
  sasl_username: user
  sasl_password: ${KAFKA_PASSWORD}

target:  # For restore
  bootstrap_servers: ["kafka-dr:9092"]
  # Same auth options as source

storage:
  backend: s3 | azure | gcs | filesystem | memory
  bucket: my-bucket
  region: us-east-1
  prefix: backups/

backup:
  compression: zstd | lz4 | gzip | snappy | none
  segment_max_bytes: 134217728  # 128MB
  checkpoint_interval_secs: 5
  max_concurrent_partitions: 4

restore:
  time_window_start: 1736899200000  # epoch millis (PITR)
  time_window_end: 1736985600000
  topic_mapping:
    orders: orders-recovered
  consumer_group_strategy: skip | header-based | timestamp-based | cluster-scan | manual
  dry_run: false
```

## Performance Targets

- **Throughput:** 100+ MB/s per partition
- **Latency:** <100ms p99 for checkpoints
- **Compression ratio:** 3-5x typical Kafka data
- **Memory:** <500MB for 4 concurrent partitions

## Consumer Offset Strategies

The project solves the **offset space discontinuity problem** (source cluster offsets != target cluster offsets):

1. **skip** - Restore data only, don't touch consumer offsets
2. **header-based** - Extract original offset from `x-original-offset` header
3. **timestamp-based** - Query target for offset by timestamp
4. **cluster-scan** - Scan target `__consumer_offsets` for recommendations
5. **manual** - Report mapping only, operator-driven reset

## Three-Phase Restore

For exact consumer offset recovery:
1. **Phase 1 (Backup):** Store original offset in message headers
2. **Phase 2 (Restore):** Produce records, track source→target offset mapping
3. **Phase 3 (Reset):** Apply offset resets using generated mapping

## Important Design Decisions

- **kafka-protocol crate** used for raw protocol control (not rdkafka/librdkafka)
- **object_store crate** for unified cloud storage abstraction
- **SQLite** for checkpoint persistence (synced to cloud storage)
- **Mini-batching** (128MB segments) to reduce I/O operations
- **At-least-once semantics** - safe to re-process overlap on resume
- **Circuit breaker** pattern for transient failure handling

## Documentation Reference

| Doc | Purpose |
|-----|---------|
| `docs/mvp-prd.md` | Core MVP requirements |
| `docs/performance-prd.md` | Performance targets |
| `docs/restore_prd_v1.md` | Restore feature spec |
| `docs/configuration.md` | Complete config reference |
| `docs/storage_guide.md` | Storage backend setup |
| `docs/restore_guide.md` | Restore examples |
| `docs/Three_Phase_Restore_Guide.md` | Offset recovery system |
| `docs/Offset_Remapping_Deep_Dive.md` | Offset mapping technical details |
| `docs/quickstart.md` | Getting started |

## Development Notes

### Adding a New CLI Command
1. Create `commands/{command}.rs` in kafka-backup-cli
2. Add command struct with clap derive macros
3. Implement `run()` async method
4. Register in `main.rs` command enum

### Adding a New Storage Backend
1. Implement `StorageBackend` trait in `storage/`
2. Add variant to `StorageBackendConfig` enum
3. Update `create_backend()` factory function
4. Add configuration parsing in `config.rs`

### Adding Compression Algorithm
1. Add to `CompressionAlgorithm` enum in `compression.rs`
2. Implement compress/decompress functions
3. Update segment writer/reader

## Testing

```bash
# Unit tests
cargo test

# Integration tests (requires Docker for testcontainers)
cargo test --features integration

# Specific test
cargo test test_backup_engine

# With logging
RUST_LOG=debug cargo test -- --nocapture
```

## Common Tasks

### Debug a failing backup
```bash
RUST_LOG=kafka_backup_core=debug cargo run -p kafka-backup-cli -- backup --config config.yaml
```

### Inspect a backup manifest
```bash
cargo run -p kafka-backup-cli -- describe --path /path/to/backups --backup-id my-backup --format json
```

### Validate backup integrity
```bash
cargo run -p kafka-backup-cli -- validate --path s3://bucket --backup-id my-backup --deep
```
