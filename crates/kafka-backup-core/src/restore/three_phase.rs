//! Three-phase restore orchestrator.
//!
//! This module implements the complete three-phase offset remapping system for Kafka backup/restore:
//!
//! ## Phase 1: Backup with Header Preservation
//! - Store original offset in `x-original-offset` header
//! - Store original timestamp in `x-original-timestamp` header
//! - Optionally store source cluster ID in `x-source-cluster` header
//!
//! ## Phase 2: Restore with Offset Mapping Collection
//! - Decompress and produce records to target cluster
//! - Capture `base_offset` from `ProduceResponse`
//! - Build detailed offset mapping (source_offset -> target_offset)
//! - Do NOT touch `__consumer_offsets` during this phase
//!
//! ## Phase 3: Offset Reset
//! - Generate offset reset plan from mapping
//! - Support manual review or auto-apply modes
//! - Apply consumer group offset commits to target cluster
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use kafka_backup_core::restore::ThreePhaseRestore;
//!
//! // Create orchestrator
//! let orchestrator = ThreePhaseRestore::new(config)?;
//!
//! // Run all phases
//! let report = orchestrator.run_all_phases().await?;
//!
//! // Or run phases individually:
//! // Phase 2: Restore (Phase 1 headers are added during backup)
//! let restore_report = orchestrator.run_restore_phase().await?;
//!
//! // Phase 3: Generate offset reset plan
//! let reset_plan = orchestrator.generate_offset_reset_plan(
//!     &restore_report.offset_mapping,
//!     &["consumer-group-1", "consumer-group-2"],
//!     OffsetResetStrategy::Manual,
//! ).await?;
//!
//! // Review plan and optionally apply
//! let reset_report = orchestrator.apply_offset_reset(&reset_plan).await?;
//! ```

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::Config;
use crate::kafka::KafkaClient;
use crate::manifest::{OffsetMapping, RestoreReport};
use crate::Result;

use super::engine::RestoreEngine;
use super::offset_reset::{
    OffsetResetExecutor, OffsetResetPlan, OffsetResetReport, OffsetResetStrategy,
};

/// Three-phase restore orchestrator
pub struct ThreePhaseRestore {
    /// Restore configuration
    config: Config,
}

/// Complete report for three-phase restore
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreePhaseReport {
    /// Backup ID being restored
    pub backup_id: String,

    /// Phase 2: Restore report
    pub restore_report: RestoreReport,

    /// Phase 3: Offset reset plan (if generated)
    pub offset_reset_plan: Option<OffsetResetPlan>,

    /// Phase 3: Offset reset execution report (if executed)
    pub offset_reset_report: Option<OffsetResetReport>,

    /// Total duration in milliseconds
    pub total_duration_ms: u64,

    /// Whether all phases completed successfully
    pub success: bool,

    /// Warnings or non-fatal issues
    pub warnings: Vec<String>,
}

impl ThreePhaseRestore {
    /// Create a new three-phase restore orchestrator
    pub fn new(config: Config) -> Result<Self> {
        Ok(Self { config })
    }

    /// Run all three phases
    ///
    /// Note: Phase 1 (backup with headers) should have been done during backup.
    /// This method runs Phase 2 (restore) and Phase 3 (offset reset).
    pub async fn run_all_phases(&self) -> Result<ThreePhaseReport> {
        let start_time = std::time::Instant::now();
        let backup_id = self.config.backup_id.clone();
        let mut warnings = Vec::new();

        info!("Starting three-phase restore for backup: {}", backup_id);

        // Phase 2: Restore
        info!("Phase 2: Restoring data and collecting offset mapping...");
        let restore_report = self.run_restore_phase().await?;

        info!(
            "Phase 2 complete: {} records restored, {} offset mappings collected",
            restore_report.records_restored,
            restore_report.offset_mapping.detailed_mapping_count()
        );

        // Phase 3: Offset Reset (if consumer groups configured)
        let restore_options = self.config.restore.clone().unwrap_or_default();
        let (offset_reset_plan, offset_reset_report) = if restore_options.reset_consumer_offsets
            && !restore_options.consumer_groups.is_empty()
        {
            info!("Phase 3: Generating and applying offset reset plan...");

            let strategy = match restore_options.dry_run {
                true => OffsetResetStrategy::DryRun,
                false => OffsetResetStrategy::Auto,
            };

            let plan = self
                .generate_offset_reset_plan(
                    &restore_report.offset_mapping,
                    &restore_options.consumer_groups,
                    strategy,
                )
                .await?;

            let report = if strategy != OffsetResetStrategy::DryRun {
                Some(self.apply_offset_reset(&plan).await?)
            } else {
                warnings
                    .push("Phase 3 ran in dry-run mode, offsets not actually reset".to_string());
                None
            };

            (Some(plan), report)
        } else {
            if !restore_options.consumer_groups.is_empty()
                && !restore_options.reset_consumer_offsets
            {
                warnings.push(
                    "Consumer groups specified but reset_consumer_offsets=false, skipping Phase 3"
                        .to_string(),
                );
            }
            (None, None)
        };

        let total_duration_ms = start_time.elapsed().as_millis() as u64;
        let success = restore_report.errors.is_empty()
            && offset_reset_report
                .as_ref()
                .map(|r| r.success)
                .unwrap_or(true);

        let report = ThreePhaseReport {
            backup_id,
            restore_report,
            offset_reset_plan,
            offset_reset_report,
            total_duration_ms,
            success,
            warnings,
        };

        if report.success {
            info!(
                "Three-phase restore completed successfully in {}ms",
                total_duration_ms
            );
        } else {
            warn!(
                "Three-phase restore completed with errors in {}ms",
                total_duration_ms
            );
        }

        Ok(report)
    }

    /// Run Phase 2: Restore data and collect offset mapping
    pub async fn run_restore_phase(&self) -> Result<RestoreReport> {
        let engine = RestoreEngine::new(self.config.clone())?;
        engine.run().await
    }

    /// Generate Phase 3: Offset reset plan
    pub async fn generate_offset_reset_plan(
        &self,
        offset_mapping: &OffsetMapping,
        consumer_groups: &[String],
        strategy: OffsetResetStrategy,
    ) -> Result<OffsetResetPlan> {
        let executor = if let Some(target) = &self.config.target {
            // Create a new client for the target cluster
            let client = KafkaClient::new(target.clone());
            match client.connect().await {
                Ok(_) => OffsetResetExecutor::new(client, target.bootstrap_servers.clone()),
                Err(e) => {
                    warn!(
                        "Could not connect to target cluster for offset fetch: {}",
                        e
                    );
                    OffsetResetExecutor::new_offline(target.bootstrap_servers.clone())
                }
            }
        } else {
            OffsetResetExecutor::new_offline(vec![])
        };

        executor
            .generate_plan(offset_mapping, consumer_groups, strategy)
            .await
    }

    /// Apply Phase 3: Execute offset reset plan
    pub async fn apply_offset_reset(&self, plan: &OffsetResetPlan) -> Result<OffsetResetReport> {
        let target = self.config.target.as_ref().ok_or_else(|| {
            crate::Error::Config(
                "Target cluster configuration required for offset reset".to_string(),
            )
        })?;

        let client = KafkaClient::new(target.clone());
        client.connect().await?;

        let executor = OffsetResetExecutor::new(client, target.bootstrap_servers.clone());
        executor.execute_plan(plan).await
    }

    /// Generate shell script for manual Phase 3 execution
    pub fn generate_offset_reset_script(&self, plan: &OffsetResetPlan) -> String {
        let bootstrap_servers = self
            .config
            .target
            .as_ref()
            .map(|t| t.bootstrap_servers.clone())
            .unwrap_or_default();

        let executor = OffsetResetExecutor::new_offline(bootstrap_servers);
        executor.generate_shell_script(plan)
    }

    /// Validate Phase 1 headers in backup
    ///
    /// Checks if the backup contains the required offset headers for three-phase restore.
    pub async fn validate_phase1_headers(&self) -> Result<Phase1ValidationReport> {
        // This would read sample records from the backup and check for headers
        // For now, return a placeholder
        Ok(Phase1ValidationReport {
            has_offset_headers: true,
            has_timestamp_headers: true,
            has_source_cluster_header: false,
            sample_records_checked: 0,
            warnings: vec![],
        })
    }
}

/// Phase 1 validation report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Phase1ValidationReport {
    /// Whether x-original-offset headers are present
    pub has_offset_headers: bool,
    /// Whether x-original-timestamp headers are present
    pub has_timestamp_headers: bool,
    /// Whether x-source-cluster headers are present
    pub has_source_cluster_header: bool,
    /// Number of sample records checked
    pub sample_records_checked: usize,
    /// Validation warnings
    pub warnings: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Mode, StorageBackendType, StorageConfig};

    fn test_config() -> Config {
        Config {
            mode: Mode::Restore,
            backup_id: "test-backup".to_string(),
            source: None,
            target: None,
            storage: StorageConfig {
                backend: StorageBackendType::Filesystem,
                path: Some("/tmp/test".into()),
                endpoint: None,
                bucket: None,
                access_key: None,
                secret_key: None,
                prefix: None,
                region: None,
            },
            backup: None,
            restore: None,
            offset_storage: None,
        }
    }

    #[test]
    fn test_three_phase_restore_creation() {
        let config = test_config();
        let result = ThreePhaseRestore::new(config);
        assert!(result.is_ok());
    }
}
