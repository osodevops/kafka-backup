//! MessageCountCheck — verifies per-partition record counts match the backup manifest.

use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::time::Instant;
use tracing::debug;

use super::config::MessageCountConfig;
use super::context::ValidationContext;
use super::{CheckOutcome, ValidationCheck, ValidationResult};
use crate::config::IsolationLevel;
use crate::kafka::PartitionFetcher;
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
                let options = crate::config::RestoreOptions {
                    time_window_end: ctx.pitr_timestamp,
                    ..Default::default()
                };
                let segments: Vec<_> = partition
                    .segments
                    .iter()
                    .filter(|s| s.overlaps_time_window(None, ctx.pitr_timestamp))
                    .collect();
                let expected_count = match crate::restore::helpers::count_archive_records(
                    ctx.storage.as_ref(),
                    &topic_backup.name,
                    &segments,
                    &options,
                )
                .await
                {
                    Ok(count) => count,
                    Err(error) => {
                        discrepancies.push(serde_json::json!({
                            "topic": topic_backup.name, "partition": partition.partition_id,
                            "error": format!("Cannot count backup records: {error}"),
                        }));
                        continue;
                    }
                };
                total_expected += expected_count;

                // Scan visible target records; offsets alone are not counts.
                match count_partition(
                    ctx.target_client.as_ref(),
                    &topic_backup.name,
                    partition.partition_id,
                )
                .await
                {
                    Ok((earliest, latest, restored_count)) => {
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
        let sha256_hex = hex_encode(&hasher.finalize());

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
                "count_method": "read_committed_record_scan",
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

// Captures one partition's count and offset bounds for the evidence summary.
async fn count_partition(
    fetcher: &dyn PartitionFetcher,
    topic: &str,
    partition: i32,
) -> Result<(i64, i64, i64)> {
    // Offsets include compacted holes and transaction markers. Count visible
    // records, bounded by the LSO captured before scanning, rather than treating
    // the offset span as a record count. Restored records are nontransactional;
    // read_committed also makes this safe when validating a transactional source.
    let isolation = IsolationLevel::ReadCommitted;
    let (earliest, latest) = fetcher
        .partition_offsets(topic, partition, isolation)
        .await?;
    let mut offset = earliest;
    let mut count = 0;
    let mut stalled = 0;
    while offset < latest {
        let response = fetcher
            .fetch_partition(topic, partition, offset, 4 * 1024 * 1024, isolation)
            .await?;
        if response.log_start_offset > earliest {
            return Err(crate::Error::Config(format!(
                "{topic}:{partition}: retention advanced during message count; retry on a quiet target"
            )));
        }
        if response.next_offset <= offset {
            stalled += 1;
            if stalled >= 3 {
                return Err(crate::Error::Config(format!(
                    "{topic}:{partition}: message count scan stalled at offset {offset} before captured end {latest}"
                )));
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        }
        stalled = 0;
        count += response
            .records
            .iter()
            .filter(|r| r.offset >= offset && r.offset < latest)
            .count() as i64;
        offset = response.next_offset.min(latest);
    }
    Ok((earliest, latest, count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::FetchResponse;
    use crate::manifest::BackupRecord;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    struct Source {
        bounds: (i64, i64),
        pages: Mutex<VecDeque<FetchResponse>>,
    }

    #[async_trait]
    impl PartitionFetcher for Source {
        async fn partition_offsets(
            &self,
            _: &str,
            _: i32,
            isolation: IsolationLevel,
        ) -> Result<(i64, i64)> {
            assert_eq!(isolation, IsolationLevel::ReadCommitted);
            Ok(self.bounds)
        }
        async fn fetch_partition(
            &self,
            _: &str,
            _: i32,
            _: i64,
            _: i32,
            isolation: IsolationLevel,
        ) -> Result<FetchResponse> {
            assert_eq!(isolation, IsolationLevel::ReadCommitted);
            Ok(self.pages.lock().unwrap().pop_front().unwrap_or_default())
        }
    }

    fn page(offsets: &[i64], next_offset: i64) -> FetchResponse {
        FetchResponse {
            records: offsets
                .iter()
                .map(|&offset| BackupRecord {
                    offset,
                    timestamp: offset,
                    key: None,
                    value: Some(vec![1]),
                    headers: vec![],
                })
                .collect(),
            next_offset,
            high_watermark: 20,
            last_stable_offset: 20,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn count_records_not_offset_span_with_compaction_and_transaction_gaps() {
        let source = Source {
            bounds: (4, 12),
            pages: Mutex::new(VecDeque::from([
                page(&[4, 7], 8),
                page(&[], 10),
                page(&[10], 12),
            ])),
        };
        assert_eq!(
            count_partition(&source, "orders", 0).await.unwrap(),
            (4, 12, 3)
        );
    }

    #[tokio::test]
    async fn count_stops_at_captured_end_even_if_new_records_arrive() {
        let source = Source {
            bounds: (4, 8),
            pages: Mutex::new(VecDeque::from([page(&[4, 7, 8, 9], 10)])),
        };
        assert_eq!(
            count_partition(&source, "orders", 0).await.unwrap(),
            (4, 8, 2)
        );
    }

    #[tokio::test]
    async fn stalled_scan_fails_instead_of_claiming_an_exact_count() {
        let source = Source {
            bounds: (4, 8),
            pages: Mutex::new(VecDeque::new()),
        };
        assert!(count_partition(&source, "orders", 0).await.is_err());
    }
    #[tokio::test]
    async fn empty_partition_has_zero_records_without_fetching() {
        let source = Source {
            bounds: (9, 9),
            pages: Mutex::new(VecDeque::new()),
        };
        assert_eq!(
            count_partition(&source, "orders", 0).await.unwrap(),
            (9, 9, 0)
        );
    }

    #[tokio::test]
    async fn advancing_retention_invalidates_the_count() {
        let mut response = page(&[7], 8);
        response.log_start_offset = 7;
        let source = Source {
            bounds: (4, 8),
            pages: Mutex::new(VecDeque::from([response])),
        };
        assert!(count_partition(&source, "orders", 0)
            .await
            .unwrap_err()
            .to_string()
            .contains("retention advanced"));
    }

    #[tokio::test]
    async fn transient_empty_fetch_is_retried() {
        let source = Source {
            bounds: (4, 8),
            pages: Mutex::new(VecDeque::from([page(&[], 4), page(&[4, 7], 8)])),
        };
        assert_eq!(
            count_partition(&source, "orders", 0).await.unwrap(),
            (4, 8, 2)
        );
    }
}
