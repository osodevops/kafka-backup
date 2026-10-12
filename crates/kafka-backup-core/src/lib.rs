//! Kafka Backup Core Library
//!
//! This crate provides the core functionality for backing up and restoring
//! Kafka topics to/from various storage backends.

/// Fault-injection point for the chaos suite. Compiles to nothing unless the
/// `failpoints` feature is enabled. With it, a `return(msg)` action makes the
/// enclosing function return `Err(Error::Io(..))`; a callback action runs
/// the callback and continues (used to simulate a crash at this point).
macro_rules! failpoint {
    ($name:literal) => {
        #[cfg(feature = "failpoints")]
        fail::fail_point!($name, |msg: Option<String>| {
            Err($crate::Error::Io(std::io::Error::other(format!(
                "failpoint {}: {}",
                $name,
                msg.unwrap_or_default()
            ))))
        });
    };
}

pub mod backup;
pub mod circuit_breaker;
pub mod compression;
pub mod config;
pub mod error;
pub mod evidence;
pub mod health;
pub mod kafka;
pub mod manifest;
pub mod metrics;
pub mod notification;
pub mod offset_headers;
pub mod offset_store;
pub mod restore;
pub mod segment;
pub mod storage;
pub mod util;
pub mod validation;

pub use circuit_breaker::{CircuitBreaker, CircuitBreakerConfig, CircuitState};
pub use config::{
    CircuitBreakerSettings, Config, HeaderPreflightMode, MetricsConfig, OffsetStorageBackend,
    OffsetStorageConfig, OffsetStrategy, RestoreOptions,
};
pub use error::{Error, Result};
pub use health::{HealthCheck, HealthStatus};
pub use kafka::{
    CommittedOffset, ConsumerGroup, ConsumerGroupDescription, ConsumerGroupMember, SaslAuthOutcome,
    SaslMechanismPlugin, SaslMechanismPluginFactory, SaslMechanismPluginFactoryHandle,
    SaslMechanismPluginHandle, SaslPluginError, SharedPluginFactory, TimestampOffset,
};
pub use manifest::{
    BackupManifest, BackupRecord, ConsumerGroupOffset, ConsumerGroupOffsets, DryRunPartitionReport,
    DryRunRepartitioningInfo, DryRunReport, DryRunTopicReport, OffsetGap, OffsetGapReason,
    OffsetMapping, OffsetMappingEntry, OffsetPair, PartitionBackup, PartitionRestoreReport,
    RecordHeader, RestoreCheckpoint, RestoreReport, SegmentMetadata, TopicBackup,
    TopicRestoreReport,
};
pub use metrics::{
    create_instrumented_backend, ErrorType, InstrumentedStorageBackend, MetricsReport,
    MetricsServer, MetricsServerConfig, OperationStatus, PerformanceMetrics, PrometheusMetrics,
    StorageOperation, TimerGuard,
};
pub use offset_store::{OffsetStore, OffsetStoreConfig, SqliteOffsetStore};
pub use restore::{
    engine::{RestoreEngine, RestoreProgress},
    offset_automation::{
        BulkOffsetReset, BulkOffsetResetConfig, BulkOffsetResetReport, BulkResetStatus,
        GroupResetOutcome, OffsetMapping as BulkOffsetMapping, OffsetResetBatch,
        OffsetResetMetrics, PartitionError, PerformanceStats,
    },
    offset_reset::{
        GroupResetPlan, GroupResetResult, OffsetResetExecutor, OffsetResetPlan,
        OffsetResetPlanBuilder, OffsetResetReport, OffsetResetStrategy, PartitionResetPlan,
    },
    offset_rollback::{
        reset_offsets_with_rollback, rollback_offset_reset, snapshot_current_offsets,
        verify_rollback, GroupOffsetState, OffsetMismatch, OffsetSnapshot, OffsetSnapshotMetadata,
        OffsetSnapshotStorage, PartitionOffsetState, RestoreWithRollbackResult,
        RestoreWithRollbackStatus, RollbackResult, RollbackStatus, StorageBackendSnapshotStore,
        VerificationResult,
    },
    preflight::{
        HeaderPreflightReport, PartitionCoverageState, PartitionHeaderCoverage, SnapshotCheck,
    },
    three_phase::{OffsetResetPhaseOutcome, ThreePhaseReport, ThreePhaseRestore},
};
