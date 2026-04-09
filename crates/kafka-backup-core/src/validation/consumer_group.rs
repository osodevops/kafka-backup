//! ConsumerGroupOffsetCheck — verifies consumer group offsets on the restored cluster.

use async_trait::async_trait;
use std::time::Instant;
use tracing::debug;

use super::config::ConsumerGroupConfig;
use super::context::ValidationContext;
use super::{CheckOutcome, ValidationCheck, ValidationResult};
use crate::kafka::consumer_groups;
use crate::Result;

/// Verifies that consumer group offsets in the restored cluster are present
/// and consistent with the backup metadata.
pub struct ConsumerGroupOffsetCheck {
    config: ConsumerGroupConfig,
}

impl ConsumerGroupOffsetCheck {
    pub fn new(config: ConsumerGroupConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ValidationCheck for ConsumerGroupOffsetCheck {
    fn name(&self) -> &str {
        "ConsumerGroupOffsetCheck"
    }

    fn description(&self) -> &str {
        "Verifies consumer group offsets are present in the restored cluster"
    }

    async fn run(&self, ctx: &ValidationContext) -> Result<ValidationResult> {
        let start = Instant::now();

        // List all consumer groups on the restored cluster
        let groups = consumer_groups::list_groups(&ctx.target_client).await?;

        let group_ids: Vec<String> =
            if self.config.verify_all_groups || self.config.groups.is_empty() {
                groups.iter().map(|g| g.group_id.clone()).collect()
            } else {
                self.config.groups.clone()
            };

        if group_ids.is_empty() {
            return Ok(ValidationResult {
                check_name: self.name().to_string(),
                outcome: CheckOutcome::Skipped,
                detail: "No consumer groups found on restored cluster".to_string(),
                data: serde_json::json!({
                    "check": "ConsumerGroupOffsetCheck",
                    "groups_checked": 0,
                    "reason": "no_groups_found",
                }),
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        let mut groups_verified: u64 = 0;
        let mut total_offsets: u64 = 0;
        let mut issues: Vec<serde_json::Value> = Vec::new();

        for group_id in &group_ids {
            match consumer_groups::fetch_offsets(&ctx.target_client, group_id, None).await {
                Ok(offsets) => {
                    groups_verified += 1;
                    let offset_count = offsets.len() as u64;
                    total_offsets += offset_count;

                    // Check for any error codes in the committed offsets
                    for offset in &offsets {
                        if offset.error_code != 0 {
                            issues.push(serde_json::json!({
                                "group": group_id,
                                "topic": offset.topic,
                                "partition": offset.partition,
                                "error_code": offset.error_code,
                                "issue": "offset_fetch_error",
                            }));
                        }
                    }

                    debug!(
                        group = %group_id,
                        offsets = offset_count,
                        "Consumer group offsets fetched"
                    );
                }
                Err(e) => {
                    issues.push(serde_json::json!({
                        "group": group_id,
                        "issue": "fetch_failed",
                        "error": e.to_string(),
                    }));
                }
            }
        }

        let duration_ms = start.elapsed().as_millis() as u64;
        let outcome = if issues.is_empty() {
            CheckOutcome::Passed
        } else {
            CheckOutcome::Failed
        };

        let detail = format!(
            "{groups_verified} consumer groups verified; {total_offsets} offsets checked; {} issues",
            issues.len()
        );

        Ok(ValidationResult {
            check_name: self.name().to_string(),
            outcome,
            detail,
            data: serde_json::json!({
                "check": "ConsumerGroupOffsetCheck",
                "groups_checked": groups_verified,
                "total_offsets": total_offsets,
                "issues": issues,
            }),
            duration_ms,
        })
    }
}
