# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.4] - 2025-12-03

### Added
- crates.io publishing for `kafka-backup-core` library
- Semantic version checking workflow for breaking change detection
- Dependabot configuration for operator repository
- Crate-specific README for kafka-backup-core

### Changed
- Updated kafka-backup-core package metadata for crates.io compatibility

## [0.1.3] - 2025-12-01

### Added
- Try It Yourself section linking to demos repository
- Suggest Features link to Contributing section
- GitHub issue templates for bugs and feature requests
- Contributing section in README

### Changed
- Improved issue templates structure

## [0.1.2] - 2025-11-30

### Added
- Scoop package manager support for Windows installation
- Docker Hub automated publishing on releases
- Comprehensive installation guide in README

### Changed
- Simplified Homebrew install to one-liner (`brew install osodevops/tap/kafka-backup`)
- Renamed Homebrew formula to `kafka-backup`
- Updated README installation instructions
- Fixed Docker image naming to use semantic versions

## [0.1.0] - 2025-11-30

### Added
- Initial release of kafka-backup
- `BackupEngine` for backing up Kafka topics to cloud storage
- `RestoreEngine` with point-in-time recovery (PITR) support
- Multi-cloud storage support:
  - Amazon S3
  - Azure Blob Storage
  - Google Cloud Storage
  - Local filesystem
  - In-memory (for testing)
- Consumer group offset recovery with multiple strategies:
  - `skip` - restore data only
  - `header-based` - extract offset from message headers
  - `timestamp-based` - query target by timestamp
  - `cluster-scan` - scan target `__consumer_offsets`
  - `manual` - operator-driven reset
- Three-phase restore orchestration for exact offset recovery
- Offset snapshot and rollback functionality
- Compression support: zstd, lz4, gzip, snappy
- Prometheus metrics integration
- Circuit breaker pattern for fault tolerance
- SQLite-based offset tracking with cloud sync
- CLI with commands: backup, restore, list, describe, validate, offset-reset
- cargo-dist release workflow with cross-platform binaries
- Homebrew tap for macOS/Linux installation

[Unreleased]: https://github.com/osodevops/kafka-backup/compare/v0.1.4...HEAD
[0.1.4]: https://github.com/osodevops/kafka-backup/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/osodevops/kafka-backup/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/osodevops/kafka-backup/compare/v0.1.0...v0.1.2
[0.1.0]: https://github.com/osodevops/kafka-backup/releases/tag/v0.1.0
