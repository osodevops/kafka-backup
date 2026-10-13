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

use crate::config::{Config, RestoreOptions};
use crate::kafka::{KafkaClient, PartitionLeaderRouter};
use crate::manifest::{BackupManifest, OffsetMapping, RestoreReport};
use crate::storage::create_backend;
use crate::{Error, Result};

use super::engine::RestoreEngine;
use super::offset_reset::{
    OffsetResetExecutor, OffsetResetPlan, OffsetResetReport, OffsetResetStrategy,
    RestoreTopicMapping,
};
use super::preflight::{self, HeaderPreflightReport};

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

    /// Phase 1 preflight: tracking-metadata coverage of the backup,
    /// evaluated before any target mutation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub header_preflight: Option<HeaderPreflightReport>,

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

/// Result of running Phase 3 (consumer group offset reset) for a completed
/// Phase 2 restore — see [`ThreePhaseRestore::run_offset_reset_phase`].
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OffsetResetPhaseOutcome {
    /// The plan that was generated, if any consumer groups were in scope.
    pub plan: Option<OffsetResetPlan>,
    /// The execution report, if the plan was applied (not in dry-run mode).
    pub report: Option<OffsetResetReport>,
    /// Non-fatal notes: skipped repartitioned topics, dry-run, groups
    /// configured without `reset_consumer_offsets`, …
    pub warnings: Vec<String>,
}

impl OffsetResetPhaseOutcome {
    /// True when nothing was applied *or* everything that was applied
    /// succeeded. A run that applied a plan with any commit failure is
    /// **not** a success — the restore's exit status must reflect that.
    pub fn success(&self) -> bool {
        self.report.as_ref().map(|r| r.success).unwrap_or(true)
    }

    /// True when offsets were actually committed to the target.
    pub fn applied(&self) -> bool {
        self.report.is_some()
    }
}

impl ThreePhaseRestore {
    /// Create a new three-phase restore orchestrator
    pub fn new(config: Config) -> Result<Self> {
        Ok(Self { config })
    }

    /// True if this restore configuration asks for consumer group offsets to
    /// be applied after the data restore — either explicitly
    /// (`reset_consumer_offsets: true`) or implicitly via the backup's
    /// consumer-groups snapshot (`auto_consumer_groups: true`).
    ///
    /// The restore *engine* never commits offsets (Phase 2 must not touch
    /// `__consumer_offsets`); callers that run the engine directly and want
    /// the documented behaviour of those flags must follow it with
    /// [`Self::run_offset_reset_phase`]. `kafka-backup restore` and
    /// `three-phase-restore` do this automatically (issue #148).
    pub fn wants_offset_reset(restore_options: &RestoreOptions) -> bool {
        restore_options.reset_consumer_offsets || restore_options.auto_consumer_groups
    }

    /// Run all three phases
    ///
    /// Note: Phase 1 (backup with headers) should have been done during backup.
    /// This method validates Phase 1 tracking metadata (preflight), then runs
    /// Phase 2 (restore) and Phase 3 (offset reset).
    ///
    /// The preflight runs before any target mutation: when consumer-offset
    /// recovery is requested and required tracking metadata is missing,
    /// partial, corrupt, or unprovable, this method fails without creating
    /// topics or producing any record.
    pub async fn run_all_phases(&self) -> Result<ThreePhaseReport> {
        let start_time = std::time::Instant::now();
        let backup_id = self.config.backup_id.clone();
        let mut warnings = Vec::new();

        info!("Starting three-phase restore for backup: {}", backup_id);

        // Phase 1 preflight: prove tracking-metadata coverage before Phase 2
        // touches the target cluster (issue #137).
        let preflight_options = self.config.restore.clone().unwrap_or_default();
        let mode = preflight_options.header_preflight;
        let recovery = preflight::offset_recovery_requested(&preflight_options);
        info!(
            "Phase 1: Validating backup tracking metadata (mode: {}, offset recovery requested: {})",
            mode, recovery
        );
        let header_preflight = self.validate_phase1_headers().await?;
        warnings.extend(header_preflight.warnings.clone());

        if recovery && !header_preflight.passed {
            return Err(Error::Preflight(format!(
                "consumer-offset recovery was requested but the backup failed the tracking-\
                 metadata preflight; no topics were created and no records were produced. {}. \
                 Errors: {}",
                header_preflight.summary(),
                header_preflight.errors.join(" | ")
            )));
        }

        // Phase 2: Restore. The engine is told the preflight already ran so
        // it does not scan the backup a second time.
        info!("Phase 2: Restoring data and collecting offset mapping...");
        let restore_report = self.run_restore_phase_internal(true).await?;

        info!(
            "Phase 2 complete: {} records restored, {} offset mappings collected",
            restore_report.records_restored,
            restore_report.offset_mapping.detailed_mapping_count()
        );

        let outcome = self.run_offset_reset_phase(&restore_report).await?;
        warnings.extend(outcome.warnings);
        let (offset_reset_plan, offset_reset_report) = (outcome.plan, outcome.report);

        let total_duration_ms = start_time.elapsed().as_millis() as u64;
        let success = restore_report.errors.is_empty()
            && offset_reset_report
                .as_ref()
                .map(|r| r.success)
                .unwrap_or(true);

        let report = ThreePhaseReport {
            backup_id,
            header_preflight: Some(header_preflight),
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

    /// Run Phase 3 for a completed Phase 2 restore: generate the consumer group
    /// offset reset plan from the report's offset mapping and apply it to the
    /// target cluster (or only plan it, in dry-run mode).
    ///
    /// Groups come from `restore_report.resolved_consumer_groups` — which
    /// includes groups the engine auto-loaded from the backup's snapshot
    /// (`auto_consumer_groups`) — falling back to the configured
    /// `consumer_groups`. Nothing is applied unless the configuration asks for
    /// it (see [`Self::wants_offset_reset`]); repartitioned topics are skipped
    /// with a warning because no source→target offset mapping exists for them.
    ///
    /// This is the single place Phase 3 happens: `run_all_phases`, the
    /// `restore` CLI command and the operator all go through it, so offsets
    /// are applied exactly once per run.
    pub async fn run_offset_reset_phase(
        &self,
        restore_report: &RestoreReport,
    ) -> Result<OffsetResetPhaseOutcome> {
        let restore_options = self.config.restore.clone().unwrap_or_default();
        let mut warnings = Vec::new();

        // Warn about repartitioned topics — offset reset is not supported for them
        if !restore_options.repartitioning.is_empty() {
            let topics: Vec<&String> = restore_options.repartitioning.keys().collect();
            warn!(
                "Repartitioned topics {:?} will be skipped during Phase 3 offset reset \
                 (source→target offset mapping is not available for repartitioned data)",
                topics
            );
            warnings.push(format!(
                "Repartitioned topics skipped for offset reset: {:?}",
                topics
            ));
        }

        // Use resolved_consumer_groups from the restore report — this includes
        // groups auto-loaded from snapshot by the engine (auto_consumer_groups).
        // The config's consumer_groups list may be empty if auto_consumer_groups
        // was used, because the engine resolves groups at runtime.
        let effective_consumer_groups = if !restore_report.resolved_consumer_groups.is_empty() {
            restore_report.resolved_consumer_groups.clone()
        } else {
            restore_options.consumer_groups.clone()
        };
        let effective_reset = restore_options.reset_consumer_offsets
            || (restore_options.auto_consumer_groups && !effective_consumer_groups.is_empty());

        if !effective_reset || effective_consumer_groups.is_empty() {
            if !effective_consumer_groups.is_empty()
                && !restore_options.reset_consumer_offsets
                && !restore_options.auto_consumer_groups
            {
                warnings.push(
                    "Consumer groups specified but reset_consumer_offsets=false, skipping Phase 3"
                        .to_string(),
                );
            }
            return Ok(OffsetResetPhaseOutcome {
                plan: None,
                report: None,
                warnings,
            });
        }

        info!("Phase 3: Generating and applying offset reset plan...");

        let strategy = match restore_options.dry_run {
            true => OffsetResetStrategy::DryRun,
            false => OffsetResetStrategy::Auto,
        };

        let plan = self
            .generate_offset_reset_plan(
                &restore_report.offset_mapping,
                &effective_consumer_groups,
                strategy,
            )
            .await?;

        let report = if strategy != OffsetResetStrategy::DryRun {
            Some(self.apply_offset_reset(&plan).await?)
        } else {
            warnings.push("Phase 3 ran in dry-run mode, offsets not actually reset".to_string());
            None
        };

        Ok(OffsetResetPhaseOutcome {
            plan: Some(plan),
            report,
            warnings,
        })
    }

    /// Run Phase 2: Restore data and collect offset mapping
    ///
    /// When called directly (outside [`run_all_phases`]) the restore engine
    /// performs its own header preflight according to the configuration.
    pub async fn run_restore_phase(&self) -> Result<RestoreReport> {
        self.run_restore_phase_internal(false).await
    }

    async fn run_restore_phase_internal(
        &self,
        preflight_already_ran: bool,
    ) -> Result<RestoreReport> {
        let mut config = self.config.clone();
        if preflight_already_ran {
            if let Some(restore) = config.restore.as_mut() {
                restore.header_preflight_external = true;
            }
        }
        let engine = RestoreEngine::new(config)?;
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

        let restore_options = self.config.restore.clone().unwrap_or_default();
        executor
            .with_restore_mapping(RestoreTopicMapping::from_restore_options(&restore_options))
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

        let router = PartitionLeaderRouter::new(target.clone()).await?;
        let executor = OffsetResetExecutor::new_with_router(
            std::sync::Arc::new(router),
            target.bootstrap_servers.clone(),
        );
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

    /// Validate Phase 1 tracking metadata in the backup.
    ///
    /// Loads the backup manifest and scans every selected topic/partition
    /// through the segment reader (all supported segment formats), reporting
    /// per-partition offset/timestamp/source-cluster header coverage with
    /// explicit empty, missing-data, corrupt, and indeterminate states.
    /// Zero records scanned is never reported as a positive pass.
    ///
    /// This is read-only and never contacts the target cluster. The scan
    /// depth follows `restore.header_preflight` (`auto`/`full`/`skip`); in
    /// `auto` mode segments are opened only when consumer-offset recovery is
    /// requested, otherwise coverage is reported as indeterminate from
    /// manifest metadata alone.
    pub async fn validate_phase1_headers(&self) -> Result<HeaderPreflightReport> {
        let storage = create_backend(&self.config.storage)?;
        let manifest = self.load_manifest(storage.as_ref()).await?;
        let restore_options = self.config.restore.clone().unwrap_or_default();
        let selection = self
            .config
            .target
            .as_ref()
            .map(|t| t.topics.clone())
            .unwrap_or_default();

        Ok(preflight::run_header_preflight(
            storage.as_ref(),
            &manifest,
            &selection,
            &restore_options,
            restore_options.header_preflight,
        )
        .await)
    }

    /// Load the backup manifest from storage.
    async fn load_manifest(
        &self,
        storage: &dyn crate::storage::StorageBackend,
    ) -> Result<BackupManifest> {
        let manifest_key = format!("{}/manifest.json", self.config.backup_id);
        let data = storage.get(&manifest_key).await.map_err(|e| {
            Error::BackupNotFound(format!(
                "Could not load manifest for backup '{}': {}",
                self.config.backup_id, e
            ))
        })?;
        Ok(serde_json::from_slice(&data)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Mode;
    use crate::storage::StorageBackendConfig;

    fn test_config() -> Config {
        Config {
            mode: Mode::Restore,
            backup_id: "test-backup".to_string(),
            source: None,
            target: None,
            storage: StorageBackendConfig::Filesystem {
                path: "/tmp/test".into(),
            },
            backup: None,
            restore: None,
            offset_storage: None,
            metrics: None,
        }
    }

    #[test]
    fn test_three_phase_restore_creation() {
        let config = test_config();
        let result = ThreePhaseRestore::new(config);
        assert!(result.is_ok());
    }

    fn restore_report_with_groups(groups: &[&str]) -> RestoreReport {
        RestoreReport {
            backup_id: "test-backup".to_string(),
            dry_run: false,
            start_time: 0,
            end_time: 0,
            duration_ms: 0,
            topics_restored: vec![],
            segments_processed: 0,
            records_restored: 0,
            bytes_restored: 0,
            throughput_records_per_sec: 0.0,
            throughput_bytes_per_sec: 0.0,
            errors: vec![],
            offset_mapping: OffsetMapping::new(),
            resolved_consumer_groups: groups.iter().map(|g| g.to_string()).collect(),
            records_dropped_by_filter: 0,
            records_tombstoned_by_filter: 0,
            record_filter: None,
            topic_configs_skipped: Vec::new(),
            topic_configs_adjusted: Vec::new(),
        }
    }

    #[test]
    fn wants_offset_reset_follows_either_flag() {
        use crate::config::RestoreOptions;
        let mut opts = RestoreOptions::default();
        assert!(!ThreePhaseRestore::wants_offset_reset(&opts));
        opts.reset_consumer_offsets = true;
        assert!(ThreePhaseRestore::wants_offset_reset(&opts));
        opts.reset_consumer_offsets = false;
        opts.auto_consumer_groups = true;
        assert!(ThreePhaseRestore::wants_offset_reset(&opts));
    }

    /// No consumer groups in scope → Phase 3 is a no-op: no plan, no report,
    /// counts as success, and — crucially — no network access is attempted
    /// (the test config has no target cluster).
    #[tokio::test]
    async fn offset_reset_phase_is_noop_without_groups() {
        let mut config = test_config();
        config.restore = Some(crate::config::RestoreOptions {
            reset_consumer_offsets: true,
            ..Default::default()
        });
        let orchestrator = ThreePhaseRestore::new(config).unwrap();
        let outcome = orchestrator
            .run_offset_reset_phase(&restore_report_with_groups(&[]))
            .await
            .unwrap();
        assert!(outcome.plan.is_none());
        assert!(outcome.report.is_none());
        assert!(!outcome.applied());
        assert!(outcome.success());
        assert!(outcome.warnings.is_empty());
    }

    /// Groups resolved (e.g. from the snapshot) but neither flag set → skipped
    /// with an explicit warning rather than silently.
    #[tokio::test]
    async fn offset_reset_phase_warns_when_groups_present_but_reset_not_requested() {
        let mut config = test_config();
        config.restore = Some(crate::config::RestoreOptions::default());
        let orchestrator = ThreePhaseRestore::new(config).unwrap();
        let outcome = orchestrator
            .run_offset_reset_phase(&restore_report_with_groups(&["orders-app"]))
            .await
            .unwrap();
        assert!(outcome.plan.is_none());
        assert!(!outcome.applied());
        assert!(outcome.success());
        assert_eq!(outcome.warnings.len(), 1);
        assert!(outcome.warnings[0].contains("reset_consumer_offsets=false"));
    }

    /// Dry run: a plan is generated (offline — no target needed) but nothing
    /// is applied, and the outcome says so.
    #[tokio::test]
    async fn offset_reset_phase_plans_only_in_dry_run() {
        let mut config = test_config();
        config.restore = Some(crate::config::RestoreOptions {
            reset_consumer_offsets: true,
            dry_run: true,
            consumer_groups: vec!["orders-app".to_string()],
            ..Default::default()
        });
        let orchestrator = ThreePhaseRestore::new(config).unwrap();
        let outcome = orchestrator
            .run_offset_reset_phase(&restore_report_with_groups(&[]))
            .await
            .unwrap();
        let plan = outcome
            .plan
            .as_ref()
            .expect("dry run still produces a plan");
        assert!(plan.dry_run);
        assert_eq!(plan.groups.len(), 1);
        assert!(outcome.report.is_none());
        assert!(!outcome.applied());
        assert!(outcome.success());
        assert!(outcome.warnings.iter().any(|w| w.contains("dry-run")));
    }

    /// Phase 2 -> Phase 3 across gaps in the restored source offsets: every
    /// group resumes on the first record it had not consumed, never past it.
    ///
    /// Source partition `orders/0` holds offsets 0..30. Restored are all but:
    /// 5..=9 (compacted away before the backup), 15, 16 (dropped by a record
    /// filter), 20..=23 (an aborted transaction and its marker) and 27..=29
    /// (dropped tail). The target partition already held 100 records, so the
    /// restore writes densely from target offset 100.
    #[tokio::test]
    async fn offset_reset_phase_resumes_groups_on_the_next_unconsumed_record() {
        let gaps = |o: &i64| matches!(o, 5..=9 | 15 | 16 | 20..=23 | 27..=29);
        let restored: Vec<i64> = (0..30).filter(|o| !gaps(o)).collect();
        let base = 100i64;

        // Phase 2, recorded as the engine does: one batch per produce request.
        let mut mapping = OffsetMapping::new();
        let target_log: Vec<(i64, i64)> = restored
            .iter()
            .enumerate()
            .map(|(i, &src)| (base + i as i64, src))
            .collect();
        for chunk in target_log.chunks(4) {
            mapping.add_detailed_batch(
                "orders",
                0,
                chunk
                    .iter()
                    .map(|&(target, src)| crate::manifest::OffsetPair {
                        source_offset: src,
                        target_offset: target,
                        timestamp: 1_700_000_000_000 + src,
                    })
                    .collect(),
            );
        }
        let target_log_end = base + restored.len() as i64;

        // Committed source offsets, one group each (as from the snapshot).
        let commits: &[(&str, i64)] = &[
            ("from-start", 0),
            ("at-gap-start", 5),
            ("inside-gap", 7),
            ("right-after-gap", 10),
            ("inside-filter-drop", 16),
            ("inside-aborted-txn", 22),
            ("at-tail-drop", 28),
            ("at-high-watermark", 30),
            ("beyond-the-end", 42),
        ];
        for (group, committed) in commits {
            mapping.add_consumer_group_offset(group, "orders", 0, *committed, 0, None);
        }
        let groups: Vec<String> = commits.iter().map(|(g, _)| g.to_string()).collect();

        let mut config = test_config();
        config.restore = Some(crate::config::RestoreOptions {
            reset_consumer_offsets: true,
            dry_run: true,
            consumer_groups: groups.clone(),
            ..Default::default()
        });
        let mut report = restore_report_with_groups(&[]);
        report.offset_mapping = mapping;
        let outcome = ThreePhaseRestore::new(config)
            .unwrap()
            .run_offset_reset_phase(&report)
            .await
            .unwrap();
        let plan = outcome.plan.expect("dry run produces a plan");
        assert_eq!(plan.groups.len(), commits.len());

        for (group, committed) in commits {
            let group_plan = plan.groups.iter().find(|g| g.group_id == *group).unwrap();
            assert!(group_plan.complete, "{group}");
            let p = &group_plan.partitions[0];
            assert!(!p.approximate, "{group}: per-record mapping is exact");
            assert!(
                p.target_offset <= target_log_end,
                "{group}: resumes past the target log end ({} > {target_log_end})",
                p.target_offset
            );

            // What a consumer reads first after the reset ...
            let first_read = target_log
                .iter()
                .find(|(t, _)| *t >= p.target_offset)
                .map(|&(_, src)| src);
            // ... must be the first restored record it had not consumed.
            let expected = restored.iter().copied().find(|src| src >= committed);
            assert_eq!(first_read, expected, "{group} committed at {committed}");

            // And nothing the group had not consumed is behind the reset.
            for &(target, src) in &target_log {
                if src >= *committed {
                    assert!(target >= p.target_offset, "{group} would skip source {src}");
                }
            }
        }
    }

    #[test]
    fn outcome_success_reflects_applied_report() {
        use crate::restore::offset_reset::OffsetResetReport;
        let ok = OffsetResetPhaseOutcome {
            plan: None,
            report: Some(OffsetResetReport {
                executed_at: 0,
                groups_reset: vec![],
                partitions_reset: 3,
                errors: vec![],
                success: true,
                duration_ms: 0,
            }),
            warnings: vec![],
        };
        assert!(ok.applied() && ok.success());

        let failed = OffsetResetPhaseOutcome {
            plan: None,
            report: Some(OffsetResetReport {
                executed_at: 0,
                groups_reset: vec![],
                partitions_reset: 0,
                errors: vec!["g:t:0 - error code 25 (UNKNOWN_MEMBER_ID: …)".to_string()],
                success: false,
                duration_ms: 0,
            }),
            warnings: vec![],
        };
        assert!(failed.applied() && !failed.success());
    }
}
