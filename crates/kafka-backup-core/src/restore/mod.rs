//! Restore engine module.

pub mod engine;
pub mod offset_automation;
pub mod offset_reset;
pub mod offset_rollback;
pub mod three_phase;

pub use engine::{RestoreEngine, RestoreProgress};
pub use offset_automation::{
    BulkOffsetReset, BulkOffsetResetConfig, BulkOffsetResetReport, BulkResetStatus,
    GroupResetOutcome, OffsetMapping as BulkOffsetMapping, OffsetResetBatch, OffsetResetMetrics,
    PartitionError, PerformanceStats,
};
pub use offset_reset::{
    GroupResetPlan, GroupResetResult, OffsetResetExecutor, OffsetResetPlan,
    OffsetResetPlanBuilder, OffsetResetReport, OffsetResetStrategy, PartitionResetPlan,
};
pub use offset_rollback::{
    rollback_offset_reset, reset_offsets_with_rollback, snapshot_current_offsets, verify_rollback,
    GroupOffsetState, OffsetMismatch, OffsetSnapshot, OffsetSnapshotMetadata,
    OffsetSnapshotStorage, PartitionOffsetState, RestoreWithRollbackResult,
    RestoreWithRollbackStatus, RollbackResult, RollbackStatus, StorageBackendSnapshotStore,
    VerificationResult,
};
pub use three_phase::{Phase1ValidationReport, ThreePhaseReport, ThreePhaseRestore};
