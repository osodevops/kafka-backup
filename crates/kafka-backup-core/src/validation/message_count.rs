//! MessageCountCheck — verifies per-partition record counts match the backup manifest.

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::time::Instant;
use tracing::debug;

use super::config::MessageCountConfig;
use super::context::ValidationContext;
use super::{CheckOutcome, ValidationCheck, ValidationResult};
use crate::Result;

/// Compares message counts between the backup manifest and the restored cluster.
pub struct MessageCountCheck {
    config: MessageCountConfig,
}

impl MessageCountCheck {
    pub fn new(config: MessageCountConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ValidationCheck for MessageCountCheck {
    fn name(&self) -> &str {
        "MessageCountCheck"
    }

    fn description(&self) -> &str {
        "Verifies per-topic/partition message counts in the restored cluster match the backup manifest"
    }

    async fn run(&self, ctx: &ValidationContext) -> Result<ValidationResult> {
        let start = Instant::now();

        let mut topics_verified: u64 = 0;
        let mut total_expected: i64 = 0;
        let mut total_restored: i64 = 0;
        let mut discrepancies: Vec<serde_json::Value> = Vec::new();
        let mut offset_summary = String::new();

        for topic_backup in &ctx.backup_manifest.topics {
            // Apply topic filter if configured
            if !self.config.topics.is_empty() && !self.config.topics.contains(&topic_backup.name) {
                continue;
            }

            topics_verified += 1;

            for partition in &topic_backup.partitions {
                let expected_count: i64 = partition.segments.iter().map(|s| s.record_count).sum();
                total_expected += expected_count;

                // Query the restored cluster for this partition's offset range
                match ctx
                    .target_client
                    .get_offsets(&topic_backup.name, partition.partition_id)
                    .await
                {
                    Ok((earliest, latest)) => {
                        let restored_count = latest - earliest;
                        total_restored += restored_count;

                        offset_summary.push_str(&format!(
                            "{}:{}:{}:{}\n",
                            topic_backup.name, partition.partition_id, earliest, latest
                        ));

                        let diff = (expected_count - restored_count).unsigned_abs();
                        if diff > self.config.fail_threshold {
                            discrepancies.push(serde_json::json!({
                                "topic": topic_backup.name,
                                "partition": partition.partition_id,
                                "expected": expected_count,
                                "restored": restored_count,
                                "difference": diff,
                            }));
                        }

                        debug!(
                            topic = %topic_backup.name,
                            partition = partition.partition_id,
                            expected = expected_count,
                            restored = restored_count,
                            "Partition count check"
                        );
                    }
                    Err(e) => {
                        let err_msg = e.to_string();
                        discrepancies.push(serde_json::json!({
                            "topic": topic_backup.name.clone(),
                            "partition": partition.partition_id,
                            "error": err_msg,
                        }));
                    }
                }
            }
        }

        // Compute SHA-256 of the offset summary for the evidence report
        let mut hasher = Sha256::new();
        hasher.update(offset_summary.as_bytes());
        let sha256_hex = hex_encode(hasher.finalize().as_slice());

        let duration_ms = start.elapsed().as_millis() as u64;
        let outcome = if discrepancies.is_empty() {
            CheckOutcome::Passed
        } else {
            CheckOutcome::Failed
        };

        let detail = format!(
            "{topics_verified} topics; {total_expected} messages expected, {total_restored} restored; {} discrepancies",
            discrepancies.len()
        );

        Ok(ValidationResult {
            check_name: self.name().to_string(),
            outcome,
            detail,
            data: serde_json::json!({
                "check": "MessageCountCheck",
                "topics_verified": topics_verified,
                "total_messages_expected": total_expected,
                "total_messages_restored": total_restored,
                "discrepancies": discrepancies,
                "sha256_offset_summary": sha256_hex,
            }),
            duration_ms,
        })
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
