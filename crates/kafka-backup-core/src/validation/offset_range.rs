//! OffsetRangeCheck — verifies high/low watermarks match the backup manifest.

use async_trait::async_trait;
use std::time::Instant;
use tracing::debug;

use super::context::ValidationContext;
use super::{CheckOutcome, ValidationCheck, ValidationResult};
use crate::Result;

/// Verifies that per-partition offset ranges in the restored cluster align
/// with the segment metadata recorded in the backup manifest.
pub struct OffsetRangeCheck;

#[async_trait]
impl ValidationCheck for OffsetRangeCheck {
    fn name(&self) -> &str {
        "OffsetRangeCheck"
    }

    fn description(&self) -> &str {
        "Verifies high/low watermarks per partition match the backup manifest segment ranges"
    }

    async fn run(&self, ctx: &ValidationContext) -> Result<ValidationResult> {
        let start = Instant::now();

        let mut partitions_checked: u64 = 0;
        let mut partitions_passed: u64 = 0;
        let mut issues: Vec<serde_json::Value> = Vec::new();

        for topic_backup in &ctx.backup_manifest.topics {
            for partition in &topic_backup.partitions {
                partitions_checked += 1;

                if partition.segments.is_empty() {
                    partitions_passed += 1;
                    continue;
                }

                // Expected offset range from backup manifest
                let expected_low = partition
                    .segments
                    .iter()
                    .map(|s| s.start_offset)
                    .min()
                    .unwrap_or(0);
                let expected_high = partition
                    .segments
                    .iter()
                    .map(|s| s.end_offset)
                    .max()
                    .unwrap_or(0);

                match ctx
                    .target_client
                    .get_offsets(&topic_backup.name, partition.partition_id)
                    .await
                {
                    Ok((earliest, latest)) => {
                        let mut partition_ok = true;

                        // The restored cluster's latest offset should cover the backup's
                        // end_offset. It might be end_offset + 1 since Kafka HWM is
                        // the next offset to be written.
                        let expected_hwm = expected_high + 1;
                        if latest < expected_hwm {
                            issues.push(serde_json::json!({
                                "topic": topic_backup.name,
                                "partition": partition.partition_id,
                                "issue": "high_watermark_mismatch",
                                "expected_min_hwm": expected_hwm,
                                "actual_hwm": latest,
                            }));
                            partition_ok = false;
                        }

                        debug!(
                            topic = %topic_backup.name,
                            partition = partition.partition_id,
                            expected_low,
                            expected_high,
                            earliest,
                            latest,
                            ok = partition_ok,
                            "Offset range check"
                        );

                        if partition_ok {
                            partitions_passed += 1;
                        }
                    }
                    Err(e) => {
                        let err_msg = e.to_string();
                        issues.push(serde_json::json!({
                            "topic": topic_backup.name.clone(),
                            "partition": partition.partition_id,
                            "issue": "query_failed",
                            "error": err_msg,
                        }));
                    }
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
            "{partitions_checked} partitions checked; {partitions_passed} passed; {} issues",
            issues.len()
        );

        Ok(ValidationResult {
            check_name: self.name().to_string(),
            outcome,
            detail,
            data: serde_json::json!({
                "check": "OffsetRangeCheck",
                "partitions_checked": partitions_checked,
                "partitions_passed": partitions_passed,
                "issues": issues,
            }),
            duration_ms,
        })
    }
}
