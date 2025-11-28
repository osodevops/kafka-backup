//! Kafka Backup Core Library
//!
//! This crate provides the core functionality for backing up and restoring
//! Kafka topics to/from various storage backends.

pub mod backup;
pub mod circuit_breaker;
pub mod compression;
pub mod config;
pub mod error;
pub mod health;
pub mod kafka;
pub mod manifest;
pub mod metrics;
pub mod offset_store;
pub mod restore;
pub mod segment;
pub mod storage;

pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
pub use config::{Config, OffsetStorageBackend, OffsetStorageConfig, OffsetStrategy, RestoreOptions};
pub use error::{Error, Result};
pub use health::{HealthCheck, HealthStatus};
pub use manifest::{
    BackupManifest, BackupRecord, ConsumerGroupOffset, ConsumerGroupOffsets, DryRunPartitionReport,
    DryRunReport, DryRunTopicReport, OffsetMapping, OffsetMappingEntry, OffsetPair, PartitionBackup,
    PartitionRestoreReport, RecordHeader, RestoreCheckpoint, RestoreReport, SegmentMetadata,
    TopicBackup, TopicRestoreReport,
};
pub use metrics::{MetricsReport, MetricsServer, MetricsServerConfig, PerformanceMetrics};
pub use offset_store::{OffsetStore, OffsetStoreConfig, SqliteOffsetStore};
pub use restore::{
    engine::{RestoreEngine, RestoreProgress},
    offset_reset::{
        GroupResetPlan, GroupResetResult, OffsetResetExecutor, OffsetResetPlan,
        OffsetResetPlanBuilder, OffsetResetReport, OffsetResetStrategy, PartitionResetPlan,
    },
    three_phase::{Phase1ValidationReport, ThreePhaseReport, ThreePhaseRestore},
};
pub use kafka::{
    CommittedOffset, ConsumerGroup, ConsumerGroupDescription, ConsumerGroupMember, TimestampOffset,
};
