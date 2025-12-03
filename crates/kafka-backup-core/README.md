# kafka-backup-core

Core engine for high-performance Kafka backup and restore operations with point-in-time recovery (PITR).

[![Crates.io](https://img.shields.io/crates/v/kafka-backup-core.svg)](https://crates.io/crates/kafka-backup-core)
[![Documentation](https://docs.rs/kafka-backup-core/badge.svg)](https://docs.rs/kafka-backup-core)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/osodevops/kafka-backup/blob/main/LICENSE)

## Features

- **Multi-cloud storage** - S3, Azure Blob, GCS, or local filesystem
- **Point-in-time recovery** - Restore to any millisecond within your backup window
- **Consumer offset recovery** - Automatically reset consumer group offsets after restore
- **High performance** - 100+ MB/s throughput with zstd/lz4 compression
- **Fault tolerance** - Circuit breaker pattern for resilient operations

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
kafka-backup-core = "0.1"
tokio = { version = "1", features = ["full"] }
```

## Usage

### Backup Example

```rust,no_run
use kafka_backup_core::{Config, backup::BackupEngine};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load_from_file("backup.yaml")?;
    let engine = BackupEngine::new(config).await?;
    engine.run().await?;
    Ok(())
}
```

### Restore Example

```rust,no_run
use kafka_backup_core::{Config, RestoreEngine};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::load_from_file("restore.yaml")?;
    let engine = RestoreEngine::new(config).await?;
    engine.run().await?;
    Ok(())
}
```

### Consumer Offset Reset

```rust,no_run
use kafka_backup_core::{OffsetResetExecutor, OffsetResetPlan};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let executor = OffsetResetExecutor::new("kafka:9092").await?;
    let plan = OffsetResetPlan::from_file("offset-mapping.json")?;
    executor.execute(plan).await?;
    Ok(())
}
```

## Main Components

| Component | Description |
|-----------|-------------|
| `BackupEngine` | Orchestrates backup operations to cloud storage |
| `RestoreEngine` | Handles restore with PITR filtering |
| `StorageBackend` | Abstraction over S3, Azure, GCS, filesystem |
| `OffsetResetExecutor` | Consumer group offset management |
| `ThreePhaseRestore` | Complete restore with offset recovery |
| `CircuitBreaker` | Fault tolerance for transient failures |

## Storage Backends

```rust,no_run
use kafka_backup_core::storage::{StorageBackendConfig, create_backend};

// S3
let s3 = StorageBackendConfig::S3 {
    bucket: "my-bucket".into(),
    region: Some("us-east-1".into()),
    prefix: Some("backups/".into()),
};

// Azure Blob
let azure = StorageBackendConfig::Azure {
    container: "my-container".into(),
    account: "myaccount".into(),
    prefix: Some("backups/".into()),
};

// Local filesystem
let fs = StorageBackendConfig::Filesystem {
    path: "/data/backups".into(),
};
```

## Configuration

See the [configuration guide](https://github.com/osodevops/kafka-backup/blob/main/docs/configuration.md) for complete options.

## Documentation

- [Full API Documentation](https://docs.rs/kafka-backup-core)
- [GitHub Repository](https://github.com/osodevops/kafka-backup)
- [Configuration Guide](https://github.com/osodevops/kafka-backup/blob/main/docs/configuration.md)
- [Restore Guide](https://github.com/osodevops/kafka-backup/blob/main/docs/restore_guide.md)
- [Three-Phase Restore](https://github.com/osodevops/kafka-backup/blob/main/docs/Three_Phase_Restore_Guide.md)

## CLI Tool

For command-line usage, install the `kafka-backup` CLI:

```bash
# Homebrew (macOS/Linux)
brew install osodevops/tap/kafka-backup

# Or download from GitHub Releases
```

## License

MIT License - see [LICENSE](https://github.com/osodevops/kafka-backup/blob/main/LICENSE)
