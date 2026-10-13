//! Restore accounting for Prometheus metrics and [`RestoreProgress`] events
//! (issue #173).
//!
//! One [`RestoreTelemetry`] lives in each [`RestoreEngine`](super::RestoreEngine)
//! and is shared by the 1:1 partition tasks and the repartition writers. They
//! report at the point where a produce is accounted for; this module turns
//! that into the `kafka_restore_*` series and the progress broadcast.
//!
//! The `restore_id` label is the backup id, as a restore has no id of its own.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use tokio::sync::broadcast;

use super::engine::RestoreProgress;
use crate::config::RestoreOptions;
use crate::manifest::{BackupRecord, RestoreCheckpoint, TopicBackup};
use crate::metrics::{ErrorType, OperationStatus, PrometheusMetrics};
use crate::Error;

/// Least time between two progress updates (gauges and broadcast) while
/// records flow. The first batch, the plan and the end of the run always
/// update.
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

/// Capacity of the progress broadcast channel.
const PROGRESS_CHANNEL_CAPACITY: usize = 100;

/// Highest percentage reported before the restore has finished.
const IN_FLIGHT_MAX_PERCENT: f64 = 99.9;

/// What a restore is about to read, for progress percentages.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestorePlan {
    pub topics: usize,
    pub segments: u64,
    pub records: u64,
    /// Uncompressed segment bytes: an upper bound on the key and value
    /// bytes a restore produces.
    pub bytes: u64,
}

impl RestorePlan {
    /// Segments of the partitions the restore keeps
    /// (`restore.source_partitions`) that overlap the time window, less those
    /// a resumed checkpoint has already completed.
    pub fn new(
        topics: &[TopicBackup],
        options: &RestoreOptions,
        checkpoint: Option<&RestoreCheckpoint>,
    ) -> Self {
        let completed: HashSet<&str> = checkpoint
            .map(|cp| cp.segments_completed.iter().map(String::as_str).collect())
            .unwrap_or_default();
        let mut plan = Self {
            topics: topics.len(),
            ..Self::default()
        };
        for topic in topics {
            for partition in &topic.partitions {
                if options
                    .source_partitions
                    .as_ref()
                    .is_some_and(|keep| !keep.contains(&partition.partition_id))
                {
                    continue;
                }
                for segment in &partition.segments {
                    if completed.contains(segment.key.as_str())
                        || !segment.overlaps_time_window(
                            options.time_window_start,
                            options.time_window_end,
                        )
                    {
                        continue;
                    }
                    plan.segments += 1;
                    plan.records += segment.record_count.max(0) as u64;
                    plan.bytes += segment.uncompressed_size;
                }
            }
        }
        plan
    }
}

/// How a restore run ended, for [`RestoreTelemetry::finish`].
pub(crate) enum RestoreOutcome<'a> {
    /// A dry run: nothing was restored, nothing is recorded.
    DryRun,
    Success,
    Failure(&'a Error),
}

/// Key and value bytes of the records, as the restore report counts them.
pub(crate) fn payload_bytes(records: &[BackupRecord]) -> u64 {
    records
        .iter()
        .map(|r| {
            r.key.as_ref().map_or(0, |k| k.len() as u64)
                + r.value.as_ref().map_or(0, |v| v.len() as u64)
        })
        .sum()
}

/// Restore counters, Prometheus series and progress broadcast for one engine.
pub(crate) struct RestoreTelemetry {
    restore_id: String,
    backup_id: String,
    prometheus: Option<Arc<PrometheusMetrics>>,
    progress_tx: broadcast::Sender<RestoreProgress>,
    interval: Duration,
    started: Mutex<Instant>,
    plan: Mutex<RestorePlan>,
    current: Mutex<(String, i32)>,
    last_update: Mutex<Option<Instant>>,
    topics_completed: AtomicUsize,
    segments_completed: AtomicU64,
    records: AtomicU64,
    bytes: AtomicU64,
    errors: AtomicU64,
}

impl RestoreTelemetry {
    pub fn new(backup_id: &str, prometheus: Option<Arc<PrometheusMetrics>>) -> Self {
        let (progress_tx, _) = broadcast::channel(PROGRESS_CHANNEL_CAPACITY);
        Self {
            restore_id: backup_id.to_string(),
            backup_id: backup_id.to_string(),
            prometheus,
            progress_tx,
            interval: PROGRESS_INTERVAL,
            started: Mutex::new(Instant::now()),
            plan: Mutex::new(RestorePlan::default()),
            current: Mutex::new((String::new(), 0)),
            last_update: Mutex::new(None),
            topics_completed: AtomicUsize::new(0),
            segments_completed: AtomicU64::new(0),
            records: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }

    /// Telemetry that records nothing anywhere (no metrics, no subscribers).
    #[cfg(test)]
    pub fn disabled() -> Self {
        Self::new("", None)
    }

    pub fn prometheus(&self) -> Option<Arc<PrometheusMetrics>> {
        self.prometheus.clone()
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RestoreProgress> {
        self.progress_tx.subscribe()
    }

    /// Start of a run: progress counters and the clock start from zero
    /// (Prometheus counters keep accumulating).
    pub fn start(&self) {
        *self.started.lock() = Instant::now();
        *self.plan.lock() = RestorePlan::default();
        *self.last_update.lock() = None;
        self.topics_completed.store(0, Ordering::Relaxed);
        self.segments_completed.store(0, Ordering::Relaxed);
        self.records.store(0, Ordering::Relaxed);
        self.bytes.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
    }

    /// The restore's totals are known: publish 0%. The first produced batch
    /// publishes again.
    pub fn plan(&self, plan: RestorePlan) {
        *self.plan.lock() = plan;
        self.update(None, true);
        *self.last_update.lock() = None;
    }

    /// A batch was produced and acknowledged.
    pub fn record_produced(&self, topic: &str, partition: i32, batch: &[BackupRecord]) {
        let records = batch.len() as u64;
        let bytes = payload_bytes(batch);
        self.records.fetch_add(records, Ordering::Relaxed);
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
        if let Some(prom) = &self.prometheus {
            prom.inc_restore_records(&self.restore_id, records);
            prom.inc_restore_bytes(&self.restore_id, bytes);
        }
        {
            let mut current = self.current.lock();
            if current.0 != topic {
                current.0 = topic.to_string();
            }
            current.1 = partition;
        }
        self.update(None, false);
    }

    /// A segment was read for restore.
    pub fn segment_processed(&self) {
        self.segments_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// A partition (a target partition on the repartition path) finished.
    pub fn partition_completed(&self) {
        if let Some(prom) = &self.prometheus {
            prom.inc_restore_partitions_completed(&self.restore_id);
        }
    }

    /// A topic finished.
    pub fn topic_completed(&self) {
        self.topics_completed.fetch_add(1, Ordering::Relaxed);
    }

    /// An error the restore reports (a failed topic).
    pub fn record_error(&self, error: &Error) {
        self.errors.fetch_add(1, Ordering::Relaxed);
        if let Some(prom) = &self.prometheus {
            prom.record_restore_error(&self.restore_id, ErrorType::from_error(error));
        }
    }

    /// End of a run: duration, final progress and, for a failure no topic
    /// already reported (e.g. the backup could not be read), one error.
    pub fn finish(&self, outcome: RestoreOutcome<'_>, elapsed: Duration) {
        let status = match outcome {
            RestoreOutcome::DryRun => return,
            RestoreOutcome::Success => OperationStatus::Success,
            RestoreOutcome::Failure(error) => {
                if self.errors.load(Ordering::Relaxed) == 0 {
                    self.record_error(error);
                }
                OperationStatus::Failure
            }
        };
        if let Some(prom) = &self.prometheus {
            prom.record_restore_duration(&self.backup_id, status, elapsed.as_secs_f64());
        }
        self.update(Some(status), true);
    }

    /// Publish the gauges and a progress event: always when `force`,
    /// otherwise at most once per interval.
    fn update(&self, finished: Option<OperationStatus>, force: bool) {
        {
            let mut last = self.last_update.lock();
            if !force && last.is_some_and(|at| at.elapsed() < self.interval) {
                return;
            }
            *last = Some(Instant::now());
        }
        let progress = self.snapshot(finished);
        if let Some(prom) = &self.prometheus {
            prom.record_restore_progress(
                &self.restore_id,
                &self.backup_id,
                progress.percentage,
                progress.eta_ms.map(|ms| ms as f64 / 1000.0),
                Some(progress.throughput_records_per_sec),
            );
        }
        // No subscriber is not an error.
        let _ = self.progress_tx.send(progress);
    }

    fn snapshot(&self, finished: Option<OperationStatus>) -> RestoreProgress {
        let plan = *self.plan.lock();
        let (current_topic, current_partition) = self.current.lock().clone();
        let elapsed = self.started.lock().elapsed();
        let records = self.records.load(Ordering::Relaxed);
        let bytes = self.bytes.load(Ordering::Relaxed);
        let secs = elapsed.as_secs_f64();
        let (records_per_sec, bytes_per_sec) = if secs > 0.0 {
            (records as f64 / secs, bytes as f64 / secs)
        } else {
            (0.0, 0.0)
        };

        let (percentage, eta_ms) = match finished {
            Some(OperationStatus::Success) => (100.0, Some(0)),
            _ => {
                let percentage = if plan.records > 0 {
                    (records as f64 / plan.records as f64 * 100.0).min(IN_FLIGHT_MAX_PERCENT)
                } else {
                    0.0
                };
                let eta_ms = (finished.is_none() && records_per_sec > 0.0).then(|| {
                    let left = plan.records.saturating_sub(records) as f64;
                    (left / records_per_sec * 1000.0) as u64
                });
                (percentage, eta_ms)
            }
        };

        RestoreProgress {
            current_topic,
            current_partition,
            total_topics: plan.topics,
            topics_completed: self.topics_completed.load(Ordering::Relaxed),
            total_segments: plan.segments,
            segments_completed: self.segments_completed.load(Ordering::Relaxed),
            total_records: plan.records,
            records_restored: records,
            total_bytes: plan.bytes,
            bytes_restored: bytes,
            throughput_records_per_sec: records_per_sec,
            throughput_bytes_per_sec: bytes_per_sec,
            elapsed_ms: elapsed.as_millis() as u64,
            eta_ms,
            percentage,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{BackupManifest, SegmentMetadata};

    fn segment(key: &str, records: i64, start_ts: i64, end_ts: i64) -> SegmentMetadata {
        SegmentMetadata {
            key: key.to_string(),
            start_offset: 0,
            end_offset: records - 1,
            start_timestamp: start_ts,
            end_timestamp: end_ts,
            record_count: records,
            uncompressed_size: records as u64 * 10,
            compressed_size: 1,
            sha256: String::new(),
            uploaded_at: 0,
            covered_start_offset: None,
            covered_end_offset: None,
            control_records_skipped: 0,
            aborted_records_dropped: 0,
        }
    }

    fn topic(name: &str, partitions: Vec<(i32, Vec<SegmentMetadata>)>) -> TopicBackup {
        let mut manifest = BackupManifest::new("bk".to_string());
        let topic = manifest.get_or_create_topic(name);
        for (id, segments) in partitions {
            let partition = topic.get_or_create_partition(id);
            for s in segments {
                partition.add_segment(s);
            }
        }
        topic.clone()
    }

    fn record(key: &[u8], value: Option<&[u8]>) -> BackupRecord {
        BackupRecord {
            key: Some(key.to_vec()),
            value: value.map(<[u8]>::to_vec),
            headers: Vec::new(),
            timestamp: 0,
            offset: 0,
        }
    }

    #[test]
    fn plan_counts_kept_partitions_window_and_unfinished_segments() {
        let topics = vec![
            topic(
                "a",
                vec![
                    (
                        0,
                        vec![segment("a0-1", 10, 100, 200), segment("a0-2", 5, 300, 400)],
                    ),
                    (1, vec![segment("a1-1", 7, 100, 200)]),
                ],
            ),
            topic("b", vec![(0, vec![segment("b0-1", 3, 900, 950)])]),
        ];
        let all = RestorePlan::new(&topics, &RestoreOptions::default(), None);
        assert_eq!(
            all,
            RestorePlan {
                topics: 2,
                segments: 4,
                records: 25,
                bytes: 250
            }
        );

        let options = RestoreOptions {
            source_partitions: Some(vec![0]),
            time_window_end: Some(350),
            ..Default::default()
        };
        let mut checkpoint = RestoreCheckpoint::new("b".into(), "h".into());
        checkpoint.segments_completed.push("a0-1".into());
        let plan = RestorePlan::new(&topics, &options, Some(&checkpoint));
        // a0-1 done, a1-1 filtered by partition, b0-1 outside the window.
        assert_eq!((plan.segments, plan.records), (1, 5));
    }

    #[test]
    fn counts_records_bytes_and_publishes_progress() {
        let prom = Arc::new(PrometheusMetrics::new());
        let telemetry = RestoreTelemetry::new("bk", Some(prom.clone()));
        let mut rx = telemetry.subscribe();
        telemetry.start();
        telemetry.plan(RestorePlan {
            topics: 1,
            segments: 2,
            records: 4,
            bytes: 100,
        });
        let planned = rx.try_recv().expect("plan publishes 0%");
        assert_eq!((planned.percentage, planned.total_records), (0.0, 4));

        telemetry.segment_processed();
        telemetry.record_produced("t", 1, &[record(b"k", Some(b"vv")), record(b"k", None)]);
        let first = rx.try_recv().expect("the first batch publishes");
        assert_eq!((first.records_restored, first.bytes_restored), (2, 4));
        assert_eq!(
            (first.current_topic.as_str(), first.current_partition),
            ("t", 1)
        );
        assert_eq!(first.percentage, 50.0);
        assert!(first.eta_ms.is_some());

        // Within the interval: counted, not published.
        telemetry.segment_processed();
        telemetry.record_produced("t", 0, &[record(b"k", None), record(b"k", None)]);
        assert!(rx.try_recv().is_err());

        telemetry.partition_completed();
        telemetry.topic_completed();
        telemetry.finish(RestoreOutcome::Success, Duration::from_millis(1500));
        let last = rx.try_recv().expect("finish publishes");
        assert_eq!(last.records_restored, 4);
        assert_eq!(last.bytes_restored, 6);
        assert_eq!((last.segments_completed, last.topics_completed), (2, 1));
        assert_eq!((last.percentage, last.eta_ms), (100.0, Some(0)));

        let encoded = prom.encode();
        for expected in [
            "kafka_restore_records_total{restore_id=\"bk\"} 4",
            "kafka_restore_bytes_total{restore_id=\"bk\"} 6",
            "kafka_restore_partitions_completed_total{restore_id=\"bk\"} 1",
            "kafka_restore_duration_seconds_count{backup_id=\"bk\",status=\"success\"} 1",
            "kafka_restore_progress_percent{restore_id=\"bk\",backup_id=\"bk\"} 100.0",
            "kafka_restore_eta_seconds{restore_id=\"bk\"} 0.0",
        ] {
            assert!(encoded.contains(expected), "{expected} missing:\n{encoded}");
        }
        assert!(
            !encoded.contains("kafka_restore_errors_total{"),
            "{encoded}"
        );
    }

    #[test]
    fn in_flight_progress_stays_below_100() {
        let telemetry = RestoreTelemetry::new("bk", None);
        let mut rx = telemetry.subscribe();
        telemetry.start();
        // The plan undercounts (e.g. a segment written after it was made).
        telemetry.plan(RestorePlan {
            records: 1,
            ..RestorePlan::default()
        });
        telemetry.record_produced("t", 0, &[record(b"k", None), record(b"k", None)]);
        let events: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok()).collect();
        assert_eq!(events.last().unwrap().percentage, IN_FLIGHT_MAX_PERCENT);
    }

    #[test]
    fn failure_counts_one_error_unless_a_topic_already_did() {
        let prom = Arc::new(PrometheusMetrics::new());
        let telemetry = RestoreTelemetry::new("bk", Some(prom.clone()));
        let topic_error = Error::Connection("broker went away".into());
        let summary = Error::Config("Restore completed with 1 error(s)".into());

        // A topic failed, then the run failed with a summary of it.
        telemetry.start();
        telemetry.record_error(&topic_error);
        telemetry.finish(RestoreOutcome::Failure(&summary), Duration::from_secs(1));
        // A run that failed before any topic (manifest unreadable).
        telemetry.start();
        telemetry.finish(
            RestoreOutcome::Failure(&Error::Manifest("missing".into())),
            Duration::from_secs(1),
        );
        // Dry runs record nothing.
        telemetry.start();
        telemetry.finish(RestoreOutcome::DryRun, Duration::from_secs(1));

        let encoded = prom.encode();
        let errors: f64 = encoded
            .lines()
            .filter(|l| l.starts_with("kafka_restore_errors_total{"))
            .filter_map(|l| l.rsplit_once(' ')?.1.parse::<f64>().ok())
            .sum();
        assert_eq!(errors, 2.0, "{encoded}");
        assert!(encoded.contains(
            "kafka_restore_duration_seconds_count{backup_id=\"bk\",status=\"failure\"} 2"
        ));
        assert!(!encoded.contains("status=\"success\"} 1"), "{encoded}");
    }

    #[test]
    fn disabled_telemetry_is_inert() {
        let telemetry = RestoreTelemetry::disabled();
        assert!(telemetry.prometheus().is_none());
        telemetry.start();
        telemetry.record_produced("t", 0, &[record(b"k", None)]);
        telemetry.finish(RestoreOutcome::Success, Duration::from_secs(1));
    }
}
