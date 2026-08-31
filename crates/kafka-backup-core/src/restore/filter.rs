//! Programmatic per-record filtering for restores.
//!
//! A [`RecordFilter`] is set on [`RestoreOptions`](crate::config::RestoreOptions)
//! by *code* (never from YAML) and is consulted once per archived record after
//! time-window filtering, on every restore path (standard, repartitioning,
//! three-phase). It decides whether the record is produced unchanged, dropped,
//! or produced as a tombstone (same key, null value).
//!
//! The engine reports how many records a filter affected
//! (`records_dropped_by_filter` / `records_tombstoned_by_filter` in the
//! restore report) and keeps consumer-group offset mapping exact when records
//! are dropped by mapping each dropped source offset to the next surviving
//! record's target offset.

use std::fmt;
use std::sync::Arc;

use crate::manifest::BackupRecord;

/// What to do with a single archived record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterAction {
    /// Produce the record unchanged.
    Keep,
    /// Do not produce the record at all.
    Drop,
    /// Produce the record with its value set to null (a Kafka tombstone),
    /// keeping key, headers, timestamp and offset. On a compacted target
    /// topic this also retires any earlier copy of the key.
    Tombstone,
}

/// A programmatic restore-time record filter.
///
/// Implementations must be cheap per call — `evaluate` runs once per record
/// on the restore hot path.
pub trait RecordFilter: Send + Sync {
    /// Short name used in logs and the restore report (e.g. shown as
    /// `Records filtered: 12 (filter: <name>)`).
    fn name(&self) -> &str;

    /// Whether this filter has any interest in `source_topic`. Returning
    /// `false` skips `evaluate` for the whole topic.
    fn applies_to(&self, _source_topic: &str) -> bool {
        true
    }

    /// Decide what happens to `record` from `source_topic`.
    fn evaluate(&self, source_topic: &str, record: &BackupRecord) -> FilterAction;
}

/// Cloneable handle around a [`RecordFilter`], so it can live on
/// [`RestoreOptions`](crate::config::RestoreOptions) (which is `Clone` and
/// serde-serialisable; the handle itself is `#[serde(skip)]`).
#[derive(Clone)]
pub struct RecordFilterHandle(Arc<dyn RecordFilter>);

impl RecordFilterHandle {
    pub fn new(filter: Arc<dyn RecordFilter>) -> Self {
        Self(filter)
    }

    pub fn name(&self) -> &str {
        self.0.name()
    }

    pub fn applies_to(&self, source_topic: &str) -> bool {
        self.0.applies_to(source_topic)
    }

    pub fn evaluate(&self, source_topic: &str, record: &BackupRecord) -> FilterAction {
        self.0.evaluate(source_topic, record)
    }
}

impl fmt::Debug for RecordFilterHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RecordFilterHandle({})", self.0.name())
    }
}

/// Result of applying a filter to one batch of records.
#[derive(Debug, Clone, Default)]
pub struct FilterOutcome {
    /// Records removed entirely.
    pub dropped: u64,
    /// Records turned into tombstones.
    pub tombstoned: u64,
    /// `(source_offset, timestamp)` of every dropped record, in offset order —
    /// used to keep the consumer-group offset mapping exact.
    pub dropped_records: Vec<(i64, i64)>,
}

/// Apply `filter` to `records` from `source_topic`.
///
/// Returns the surviving records (tombstoned records keep their position with
/// `value = None`) and the outcome counts.
pub fn apply_record_filter(
    source_topic: &str,
    records: Vec<BackupRecord>,
    filter: &RecordFilterHandle,
) -> (Vec<BackupRecord>, FilterOutcome) {
    if !filter.applies_to(source_topic) {
        return (records, FilterOutcome::default());
    }

    let mut outcome = FilterOutcome::default();
    let mut kept = Vec::with_capacity(records.len());
    for mut record in records {
        match filter.evaluate(source_topic, &record) {
            FilterAction::Keep => kept.push(record),
            FilterAction::Tombstone => {
                outcome.tombstoned += 1;
                record.value = None;
                kept.push(record);
            }
            FilterAction::Drop => {
                outcome.dropped += 1;
                outcome
                    .dropped_records
                    .push((record.offset, record.timestamp));
            }
        }
    }
    (kept, outcome)
}

/// Map each dropped source offset to a target offset, so
/// `OffsetMapping::lookup_target_offset` stays exact when records were
/// dropped: a committed consumer offset pointing at a dropped record must
/// resolve to the next surviving record's target offset (or one past the
/// last survivor when the drop was at the tail).
///
/// `survivors` is `(source_offset, target_offset)` for every produced record
/// of the same partition slice, in ascending source-offset order. Returns
/// `(source_offset, target_offset, timestamp)` triples; empty when there were
/// no survivors to anchor on.
pub fn map_dropped_offsets(
    dropped: &[(i64, i64)],
    survivors: &[(i64, i64)],
) -> Vec<(i64, i64, i64)> {
    if survivors.is_empty() {
        return Vec::new();
    }
    let last_target = survivors[survivors.len() - 1].1;
    dropped
        .iter()
        .map(|&(src, ts)| {
            let target = match survivors.binary_search_by_key(&src, |&(s, _)| s) {
                // A survivor with the same source offset cannot exist (the
                // record was dropped); Err gives the insertion point = first
                // survivor with a larger source offset.
                Ok(idx) => survivors[idx].1,
                Err(idx) if idx < survivors.len() => survivors[idx].1,
                Err(_) => last_target + 1,
            };
            (src, target, ts)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DropEvens;
    impl RecordFilter for DropEvens {
        fn name(&self) -> &str {
            "drop-evens"
        }
        fn evaluate(&self, _topic: &str, record: &BackupRecord) -> FilterAction {
            if record.offset % 2 == 0 {
                FilterAction::Drop
            } else {
                FilterAction::Keep
            }
        }
    }

    struct TombstoneAll;
    impl RecordFilter for TombstoneAll {
        fn name(&self) -> &str {
            "tombstone-all"
        }
        fn applies_to(&self, source_topic: &str) -> bool {
            source_topic == "in-scope"
        }
        fn evaluate(&self, _topic: &str, _record: &BackupRecord) -> FilterAction {
            FilterAction::Tombstone
        }
    }

    fn record(offset: i64) -> BackupRecord {
        BackupRecord {
            key: Some(vec![offset as u8]),
            value: Some(vec![1, 2, 3]),
            headers: Vec::new(),
            timestamp: offset * 1000,
            offset,
        }
    }

    #[test]
    fn drop_removes_records_and_reports_offsets_in_order() {
        let handle = RecordFilterHandle::new(Arc::new(DropEvens));
        let (kept, outcome) = apply_record_filter(
            "t",
            vec![record(0), record(1), record(2), record(3)],
            &handle,
        );
        assert_eq!(
            kept.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(outcome.dropped, 2);
        assert_eq!(outcome.tombstoned, 0);
        assert_eq!(outcome.dropped_records, vec![(0, 0), (2, 2000)]);
    }

    #[test]
    fn tombstone_nulls_value_and_keeps_key_headers_offset() {
        let handle = RecordFilterHandle::new(Arc::new(TombstoneAll));
        let (kept, outcome) = apply_record_filter("in-scope", vec![record(7)], &handle);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].value, None);
        assert_eq!(kept[0].key, Some(vec![7]));
        assert_eq!(kept[0].offset, 7);
        assert_eq!(outcome.tombstoned, 1);
        assert_eq!(outcome.dropped, 0);
    }

    #[test]
    fn out_of_scope_topic_is_a_noop() {
        let handle = RecordFilterHandle::new(Arc::new(TombstoneAll));
        let (kept, outcome) = apply_record_filter("other", vec![record(1)], &handle);
        assert_eq!(kept[0].value, Some(vec![1, 2, 3]));
        assert_eq!(outcome.tombstoned, 0);
    }

    #[test]
    fn map_dropped_offsets_points_at_the_next_survivor() {
        // survivors: source 1 -> target 100, source 3 -> target 101
        let survivors = vec![(1, 100), (3, 101)];
        // dropped: 0 (before first), 2 (between), 4 (tail)
        let dropped = vec![(0, 0), (2, 2000), (4, 4000)];
        assert_eq!(
            map_dropped_offsets(&dropped, &survivors),
            vec![(0, 100, 0), (2, 101, 2000), (4, 102, 4000)]
        );
    }

    #[test]
    fn map_dropped_offsets_without_survivors_maps_nothing() {
        assert!(map_dropped_offsets(&[(0, 0)], &[]).is_empty());
    }

    #[test]
    fn handle_debug_names_the_filter() {
        let handle = RecordFilterHandle::new(Arc::new(DropEvens));
        assert_eq!(format!("{:?}", handle), "RecordFilterHandle(drop-evens)");
    }
}
