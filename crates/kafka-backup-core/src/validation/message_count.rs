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
        self.run_counts(
            &ctx.backup_manifest,
            ctx.target_client.as_ref(),
            ctx.storage.as_ref(),
            ctx.pitr_timestamp,
        )
        .await
    }
}

impl MessageCountCheck {
    async fn run_counts(
        &self,
        manifest: &crate::manifest::BackupManifest,
        fetcher: &dyn PartitionFetcher,
        storage: &dyn crate::storage::StorageBackend,
        pitr_timestamp: Option<i64>,
    ) -> Result<ValidationResult> {
        let start = Instant::now();
        let mut total_expected = 0i64;
        let mut total_restored = 0i64;
        let mut discrepancies = Vec::new();
        let mut topic_counts = Vec::new();
        let mut offset_summary = String::new();
        let mut counts_complete = true;

        for topic in &manifest.topics {
            if !self.config.topics.is_empty() && !self.config.topics.contains(&topic.name) {
                continue;
            }
            let mut partitions = Vec::new();
            let mut expected = Some(0i64);
            let mut observed = Some(0i64);
            let mut topic_passed = true;
            let mut topic_complete = true;
            for partition in &topic.partitions {
                let options = crate::config::RestoreOptions {
                    time_window_end: pitr_timestamp,
                    ..Default::default()
                };
                let segments: Vec<_> = partition
                    .segments
                    .iter()
                    .filter(|s| s.overlaps_time_window(None, pitr_timestamp))
                    .collect();
                let expected_count = crate::restore::helpers::count_archive_records(
                    storage,
                    &topic.name,
                    &segments,
                    &options,
                )
                .await;
                let scan = scan_partition(fetcher, &topic.name, partition.partition_id).await;
                let mut errors = Vec::new();
                let expected_count = match expected_count {
                    Ok(count) => {
                        total_expected += count;
                        Some(count)
                    }
                    Err(error) => {
                        errors.push(format!("Cannot count backup records: {error}"));
                        None
                    }
                };
                if let Some(error) = &scan.error {
                    errors.push(error.clone());
                }
                if let Some(count) = scan.observed {
                    total_restored += count;
                }
                let complete = errors.is_empty();
                let passed = complete
                    && expected_count
                        .zip(scan.observed)
                        .is_some_and(|(e, o)| (e - o).unsigned_abs() <= self.config.fail_threshold);
                topic_passed &= passed;
                topic_complete &= complete;
                expected = expected.zip(expected_count).map(|(sum, n)| sum + n);
                observed = observed.zip(scan.observed).map(|(sum, n)| sum + n);
                let mut row = comparison(expected_count, scan.observed, passed, complete);
                row["partition"] = serde_json::json!(partition.partition_id);
                row["manifest_count"] = serde_json::json!(partition
                    .segments
                    .iter()
                    .map(|s| s.record_count)
                    .sum::<i64>());
                row["archive_start_offset"] =
                    serde_json::json!(segments.iter().map(|s| s.start_offset).min());
                row["archive_end_offset"] =
                    serde_json::json!(segments.iter().map(|s| s.end_offset).max());
                row["target_start_offset"] = serde_json::json!(scan.start);
                row["target_end_offset"] = serde_json::json!(scan.end);
                row["scan_reached_offset"] = serde_json::json!(scan.reached);
                row["high_watermark"] = serde_json::json!(scan.high_watermark);
                row["last_stable_offset"] = serde_json::json!(scan.last_stable_offset);
                row["error"] = serde_json::json!((!errors.is_empty()).then(|| errors.join("; ")));
                if !passed {
                    // Retain the previous discrepancy keys for existing JSON consumers.
                    let mut discrepancy = row.clone();
                    discrepancy["topic"] = serde_json::json!(topic.name);
                    discrepancy["restored"] = serde_json::json!(scan.observed);
                    discrepancy["difference"] = serde_json::json!(expected_count
                        .zip(scan.observed)
                        .map(|(e, o)| (e - o).unsigned_abs()));
                    discrepancies.push(discrepancy);
                }
                if let (Some(first), Some(end)) = (scan.start, scan.end) {
                    offset_summary.push_str(&format!(
                        "{}:{}:{}:{}\n",
                        topic.name, partition.partition_id, first, end
                    ));
                }
                debug!(topic = %topic.name, partition = partition.partition_id, ?expected_count,
                    observed = ?scan.observed, complete, passed, "Partition count check");
                partitions.push(row);
            }
            counts_complete &= topic_complete;
            let mut row = comparison(expected, observed, topic_passed, topic_complete);
            row["topic"] = serde_json::json!(topic.name);
            row["manifest_count"] = serde_json::json!(topic
                .partitions
                .iter()
                .flat_map(|p| &p.segments)
                .map(|s| s.record_count)
                .sum::<i64>());
            row["partitions"] = serde_json::json!(partitions);
            topic_counts.push(row);
        }
        let mut hasher = Sha256::new();
        hasher.update(offset_summary.as_bytes());
        let outcome = if discrepancies.is_empty() {
            CheckOutcome::Passed
        } else {
            CheckOutcome::Failed
        };
        let mut detail = format!(
            "{} topics; {total_expected} messages expected, {total_restored} restored; {} discrepancies",
            topic_counts.len(), discrepancies.len()
        );
        if !counts_complete {
            detail.push_str(
                "; INCOMPLETE counts: see partition errors (unknown counts are not zero)",
            );
        }
        Ok(ValidationResult {
            check_name: self.name().to_string(),
            outcome,
            detail,
            data: serde_json::json!({
                "check": "MessageCountCheck", "count_method": "read_committed_record_scan",
                "expected_count_method": "archive_replay_selection",
                "backup_isolation_level": manifest.isolation_level,
                "pitr_timestamp": pitr_timestamp, "fail_threshold": self.config.fail_threshold,
                "topics_verified": topic_counts.len(), "total_messages_expected": total_expected,
                "total_messages_restored": total_restored, "counts_complete": counts_complete,
                "topic_counts": topic_counts, "discrepancies": discrepancies,
                "sha256_offset_summary": hex_encode(&hasher.finalize()),
            }),
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}

fn comparison(
    expected: Option<i64>,
    observed: Option<i64>,
    passed: bool,
    complete: bool,
) -> serde_json::Value {
    let delta = expected.zip(observed).map(|(e, o)| o - e);
    let percentage = expected.zip(delta).and_then(|(e, d)| {
        if e == 0 {
            (d == 0).then_some(0.0)
        } else {
            Some(d as f64 / e as f64 * 100.0)
        }
    });
    serde_json::json!({
        "expected": expected, "observed": observed, "delta": delta,
        "delta_percentage": percentage, "outcome": if passed { "PASSED" } else { "FAILED" },
        "scan_complete": complete,
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[derive(Default)]
struct PartitionScan {
    start: Option<i64>,
    end: Option<i64>,
    reached: Option<i64>,
    observed: Option<i64>,
    high_watermark: Option<i64>,
    last_stable_offset: Option<i64>,
    error: Option<String>,
}

async fn scan_partition(
    fetcher: &dyn PartitionFetcher,
    topic: &str,
    partition: i32,
) -> PartitionScan {
    let mut scan = PartitionScan::default();
    let result: Result<i64> = async {
        let isolation = IsolationLevel::ReadCommitted;
        let (earliest, latest) = fetcher.partition_offsets(topic, partition, isolation).await?;
        scan.start = Some(earliest);
        scan.end = Some(latest);
        scan.reached = Some(earliest);
        let mut offset = earliest;
        let mut count = 0;
        let mut stalled = 0;
        while offset < latest {
            let response = fetcher.fetch_partition(topic, partition, offset, 4 * 1024 * 1024, isolation).await?;
            scan.high_watermark = Some(response.high_watermark);
            scan.last_stable_offset = Some(response.last_stable_offset);
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
            count += response.records.iter().filter(|r| r.offset >= offset && r.offset < latest).count() as i64;
            offset = response.next_offset.min(latest);
            scan.reached = Some(offset);
        }
        Ok(count)
    }.await;
    match result {
        Ok(count) => scan.observed = Some(count),
        Err(error) => scan.error = Some(error.to_string()),
    }
    scan
}

#[cfg(test)]
async fn count_partition(
    fetcher: &dyn PartitionFetcher,
    topic: &str,
    partition: i32,
) -> Result<(i64, i64, i64)> {
    let scan = scan_partition(fetcher, topic, partition).await;
    if let Some(error) = scan.error {
        return Err(crate::Error::Config(error));
    }
    Ok((
        scan.start.unwrap(),
        scan.end.unwrap(),
        scan.observed.unwrap(),
    ))
}

/// Human-readable topic/partition diagnostics shared by the CLI and PDF.
/// Older results without a breakdown return no lines.
pub fn diagnostic_lines(result: &ValidationResult) -> Vec<String> {
    let mut lines = Vec::new();
    if let Some(topics) = result.data.get("topic_counts").and_then(|v| v.as_array()) {
        for topic in topics {
            lines.push(format!(
                "Topic: {} [{}]",
                topic["topic"].as_str().unwrap_or("?"),
                topic["outcome"].as_str().unwrap_or("?")
            ));
            lines.push(format!(
                "Manifest records: {}",
                number(&topic["manifest_count"])
            ));
            lines.push(comparison_line(topic));
            if let Some(partitions) = topic["partitions"].as_array() {
                for partition in partitions {
                    lines.push(format!(
                        "Partition {} [{}] - {}",
                        partition["partition"],
                        partition["outcome"].as_str().unwrap_or("?"),
                        comparison_line(partition)
                    ));
                    lines.push(format!(
                        "Scan: {} -> {} (exclusive); reached {}; complete: {}",
                        number(&partition["target_start_offset"]),
                        number(&partition["target_end_offset"]),
                        number(&partition["scan_reached_offset"]),
                        partition["scan_complete"]
                    ));
                    if let Some(error) = partition["error"].as_str() {
                        lines.push(format!("Error: {error}"));
                    }
                }
            }
        }
    }
    lines
}

fn number(value: &serde_json::Value) -> String {
    value
        .as_i64()
        .map_or_else(|| "unknown".into(), |n| n.to_string())
}

fn comparison_line(row: &serde_json::Value) -> String {
    let percentage = row["delta_percentage"]
        .as_f64()
        .map_or_else(|| "n/a".into(), |v| format!("{v:+.2}%"));
    let delta = row["delta"]
        .as_i64()
        .map_or_else(|| "unknown".into(), |n| format!("{n:+}"));
    format!(
        "Expected: {} | Observed: {} | Delta: {delta} ({percentage})",
        number(&row["expected"]),
        number(&row["observed"])
    )
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
    fn manifest_topic(manifest: &mut crate::manifest::BackupManifest, name: &str, counts: &[i64]) {
        for (partition, count) in counts.iter().enumerate() {
            let segment: crate::manifest::SegmentMetadata =
                serde_json::from_value(serde_json::json!({
                    "key": format!("{name}/{partition}.bin"), "start_offset": 0,
                    "end_offset": count - 1, "start_timestamp": 0, "end_timestamp": 100,
                    "record_count": count, "compressed_size": 1, "uncompressed_size": 1,
                    "compression": "none", "checksum": "", "control_records_skipped": 0,
                    "aborted_records_dropped": 0,
                }))
                .unwrap();
            manifest
                .get_or_create_topic(name)
                .get_or_create_partition(partition as i32)
                .add_segment(segment);
        }
    }

    #[tokio::test]
    async fn topic_breakdown_preserves_passes_and_opposite_partition_mismatches() {
        let mut manifest = crate::manifest::BackupManifest::new("issue-117".into());
        manifest_topic(&mut manifest, "balanced-but-wrong", &[2, 4]);
        manifest_topic(&mut manifest, "control", &[3]);
        manifest_topic(&mut manifest, "excluded", &[99]);
        let source = Source {
            bounds: (0, 3),
            pages: Mutex::new(VecDeque::from([
                page(&[0, 1, 2], 3),
                page(&[0, 1, 2], 3),
                page(&[0, 1, 2], 3),
            ])),
        };
        let result = MessageCountCheck::new(MessageCountConfig {
            topics: vec!["balanced-but-wrong".into(), "control".into()],
            ..Default::default()
        })
        .run_counts(
            &manifest,
            &source,
            &crate::storage::MemoryBackend::new(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(result.outcome, CheckOutcome::Failed);
        assert_eq!(result.data["total_messages_expected"], 9);
        assert_eq!(result.data["total_messages_restored"], 9);
        let topics = result.data["topic_counts"]
            .as_array()
            .expect("issue #117: missing topic breakdown");
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0]["topic"], "balanced-but-wrong");
        assert_eq!(topics[0]["delta"], 0);
        assert_eq!(
            topics[0]["outcome"], "FAILED",
            "opposite discrepancies must not cancel"
        );
        assert_eq!(topics[0]["partitions"][0]["delta"], 1);
        assert_eq!(topics[0]["partitions"][0]["delta_percentage"], 50.0);
        assert_eq!(topics[0]["partitions"][1]["delta"], -1);
        assert_eq!(topics[0]["partitions"][1]["delta_percentage"], -25.0);
        assert_eq!(topics[1]["outcome"], "PASSED");
        assert_eq!(topics[1]["observed"], 3);
        assert_eq!(topics[1]["partitions"][0]["target_end_offset"], 3);
        assert_eq!(result.data["counts_complete"], true);
    }

    #[tokio::test]
    async fn failed_scan_is_unknown_instead_of_a_zero_count_or_a_false_shortfall() {
        let mut manifest = crate::manifest::BackupManifest::new("issue-117".into());
        manifest_topic(&mut manifest, "unreadable", &[5]);
        let source = Source {
            bounds: (0, 5),
            pages: Mutex::new(VecDeque::new()),
        };
        let result = MessageCountCheck::new(Default::default())
            .run_counts(
                &manifest,
                &source,
                &crate::storage::MemoryBackend::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.outcome, CheckOutcome::Failed);
        assert_eq!(result.data["counts_complete"], false);
        let topic = &result.data["topic_counts"][0];
        assert_eq!(topic["expected"], 5);
        assert!(topic["observed"].is_null());
        assert!(topic["delta"].is_null());
        assert!(topic["delta_percentage"].is_null());
        assert_eq!(topic["scan_complete"], false);
        assert!(topic["partitions"][0]["error"]
            .as_str()
            .unwrap()
            .contains("stalled"));
    }
    #[tokio::test]
    async fn per_partition_threshold_and_zero_denominator_are_explicit() {
        let mut manifest = crate::manifest::BackupManifest::new("threshold".into());
        manifest_topic(&mut manifest, "within-tolerance", &[2]);
        manifest_topic(&mut manifest, "unexpected-records", &[0]);
        let source = Source {
            bounds: (0, 3),
            pages: Mutex::new(VecDeque::from([page(&[0, 1, 2], 3), page(&[0, 1, 2], 3)])),
        };
        let result = MessageCountCheck::new(MessageCountConfig {
            fail_threshold: 1,
            ..Default::default()
        })
        .run_counts(
            &manifest,
            &source,
            &crate::storage::MemoryBackend::new(),
            None,
        )
        .await
        .unwrap();
        let topics = &result.data["topic_counts"];
        assert_eq!(topics[0]["outcome"], "PASSED");
        assert_eq!(topics[0]["delta"], 1);
        assert_eq!(topics[1]["outcome"], "FAILED");
        assert_eq!(topics[1]["delta"], 3);
        assert!(topics[1]["delta_percentage"].is_null());
        assert!(diagnostic_lines(&result)
            .iter()
            .any(|l| l.contains("+3 (n/a)")));
        let source = Source {
            bounds: (0, 0),
            pages: Mutex::new(VecDeque::new()),
        };
        manifest.topics.remove(0);
        let result = MessageCountCheck::new(Default::default())
            .run_counts(
                &manifest,
                &source,
                &crate::storage::MemoryBackend::new(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(result.data["topic_counts"][0]["delta_percentage"], 0.0);
        assert_eq!(result.outcome, CheckOutcome::Passed);
    }

    #[tokio::test]
    async fn pitr_archive_count_failure_is_not_hidden_as_a_shortfall() {
        let mut manifest = crate::manifest::BackupManifest::new("broken-archive".into());
        manifest_topic(&mut manifest, "unreadable-archive", &[5]);
        let source = Source {
            bounds: (0, 1),
            pages: Mutex::new(VecDeque::from([page(&[0], 1)])),
        };
        // Partial PITR requires archive data; the segment is deliberately missing.
        let result = MessageCountCheck::new(Default::default())
            .run_counts(
                &manifest,
                &source,
                &crate::storage::MemoryBackend::new(),
                Some(50),
            )
            .await
            .unwrap();
        let topic = &result.data["topic_counts"][0];
        assert_eq!(topic["manifest_count"], 5);
        assert!(topic["expected"].is_null());
        assert_eq!(topic["observed"], 1);
        assert!(topic["delta"].is_null());
        assert!(topic["partitions"][0]["error"]
            .as_str()
            .unwrap()
            .contains("Cannot count backup records"));
        assert_eq!(result.data["counts_complete"], false);
        assert!(result.detail.contains("INCOMPLETE"));
    }
}
