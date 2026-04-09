//! Backup validation check framework.
//!
//! Provides a trait-based validation system for verifying that restored Kafka
//! data matches the original backup. Each check implements [`ValidationCheck`]
//! and the [`ValidationRunner`] orchestrates execution of all enabled checks.

pub mod config;
pub mod consumer_group;
pub mod context;
pub mod message_count;
pub mod offset_range;
pub mod webhook;

use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

pub use config::*;
pub use context::ValidationContext;

use crate::Result;

/// Outcome of a single validation check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum CheckOutcome {
    Passed,
    Failed,
    Skipped,
    Warning,
}

impl std::fmt::Display for CheckOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CheckOutcome::Passed => write!(f, "PASSED"),
            CheckOutcome::Failed => write!(f, "FAILED"),
            CheckOutcome::Skipped => write!(f, "SKIPPED"),
            CheckOutcome::Warning => write!(f, "WARNING"),
        }
    }
}

/// Result of a single validation check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Name of the check that produced this result.
    pub check_name: String,
    /// Pass/fail outcome.
    pub outcome: CheckOutcome,
    /// Human-readable detail string.
    pub detail: String,
    /// Machine-readable data for the JSON evidence report.
    pub data: serde_json::Value,
    /// How long the check took in milliseconds.
    pub duration_ms: u64,
}

/// Aggregate summary of all validation checks in a run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationSummary {
    pub checks_total: usize,
    pub checks_passed: usize,
    pub checks_failed: usize,
    pub checks_skipped: usize,
    pub checks_warned: usize,
    pub overall_result: CheckOutcome,
    pub results: Vec<ValidationResult>,
    pub total_duration_ms: u64,
}

/// Trait that all validation checks implement.
#[async_trait]
pub trait ValidationCheck: Send + Sync {
    /// Short identifier for this check (e.g. "MessageCountCheck").
    fn name(&self) -> &str;

    /// Human-readable description of what this check verifies.
    fn description(&self) -> &str;

    /// Execute the check against the given context.
    async fn run(&self, ctx: &ValidationContext) -> Result<ValidationResult>;
}

/// Orchestrates execution of all enabled validation checks.
pub struct ValidationRunner {
    checks: Vec<Box<dyn ValidationCheck>>,
}

impl ValidationRunner {
    /// Build a runner from the given config, instantiating only enabled checks.
    pub fn from_config(config: &ChecksConfig) -> Self {
        let mut checks: Vec<Box<dyn ValidationCheck>> = Vec::new();

        if config.message_count.enabled {
            checks.push(Box::new(message_count::MessageCountCheck::new(
                config.message_count.clone(),
            )));
        }

        if config.offset_range.enabled {
            checks.push(Box::new(offset_range::OffsetRangeCheck));
        }

        if config.consumer_group_offsets.enabled {
            checks.push(Box::new(consumer_group::ConsumerGroupOffsetCheck::new(
                config.consumer_group_offsets.clone(),
            )));
        }

        for wh in &config.custom_webhooks {
            checks.push(Box::new(webhook::CustomWebhookCheck::new(wh.clone())));
        }

        Self { checks }
    }

    /// Run every registered check and return an aggregate summary.
    pub async fn run_all(&self, ctx: &ValidationContext) -> Result<ValidationSummary> {
        let start = Instant::now();
        let mut results = Vec::with_capacity(self.checks.len());

        for check in &self.checks {
            info!(check = check.name(), "Running validation check");
            let check_start = Instant::now();

            match check.run(ctx).await {
                Ok(result) => {
                    match result.outcome {
                        CheckOutcome::Passed => info!(check = check.name(), "Check passed"),
                        CheckOutcome::Failed => {
                            warn!(check = check.name(), detail = %result.detail, "Check FAILED")
                        }
                        CheckOutcome::Warning => {
                            warn!(check = check.name(), detail = %result.detail, "Check warning")
                        }
                        CheckOutcome::Skipped => info!(check = check.name(), "Check skipped"),
                    }
                    results.push(result);
                }
                Err(e) => {
                    warn!(check = check.name(), error = %e, "Check errored");
                    results.push(ValidationResult {
                        check_name: check.name().to_string(),
                        outcome: CheckOutcome::Failed,
                        detail: format!("Check errored: {e}"),
                        data: serde_json::json!({"error": e.to_string()}),
                        duration_ms: check_start.elapsed().as_millis() as u64,
                    });
                }
            }
        }

        let total_duration_ms = start.elapsed().as_millis() as u64;
        let checks_passed = results
            .iter()
            .filter(|r| r.outcome == CheckOutcome::Passed)
            .count();
        let checks_failed = results
            .iter()
            .filter(|r| r.outcome == CheckOutcome::Failed)
            .count();
        let checks_skipped = results
            .iter()
            .filter(|r| r.outcome == CheckOutcome::Skipped)
            .count();
        let checks_warned = results
            .iter()
            .filter(|r| r.outcome == CheckOutcome::Warning)
            .count();

        let overall_result = if checks_failed > 0 {
            CheckOutcome::Failed
        } else if checks_warned > 0 {
            CheckOutcome::Warning
        } else {
            CheckOutcome::Passed
        };

        Ok(ValidationSummary {
            checks_total: results.len(),
            checks_passed,
            checks_failed,
            checks_skipped,
            checks_warned,
            overall_result,
            results,
            total_duration_ms,
        })
    }
}
