//! #251: restore's performance collector must count the acknowledged output
//! payload, including when filters and transforms change its size. These
//! tests exercise both produce paths without a Prometheus registry attached.

use std::collections::HashMap;
use std::sync::Arc;

use kafka_backup_core::config::{RepartitioningStrategy, TopicRepartitioning};
use kafka_backup_core::manifest::BackupRecord;
use kafka_backup_core::restore::filter::{FilterAction, RecordFilter, RecordFilterHandle};
use kafka_backup_core::restore::transform::{RecordTransform, RecordTransformHandle};
use kafka_backup_core::restore::RestoreEngine;
use tempfile::TempDir;

use super::produce_mock_broker::MockBroker;
use super::restore_produce_path::{base_options, record, restore_config, write_backup, TOPIC};

struct DropEvenTombstoneSome;
impl RecordFilter for DropEvenTombstoneSome {
    fn name(&self) -> &str {
        "drop-even-tombstone-some"
    }
    fn evaluate(&self, _: &str, record: &BackupRecord) -> FilterAction {
        match record.offset % 4 {
            0 | 2 => FilterAction::Drop,
            1 => FilterAction::Tombstone,
            _ => FilterAction::Keep,
        }
    }
}

struct Resize;
impl RecordTransform for Resize {
    fn name(&self) -> &str {
        "resize"
    }
    fn fingerprint(&self) -> &str {
        "resize-v1"
    }
    fn transform(&self, _: &str, record: &mut BackupRecord) -> kafka_backup_core::Result<()> {
        if let Some(key) = &mut record.key {
            *key = b"key".to_vec();
        }
        if let Some(value) = &mut record.value {
            *value = vec![b'x'; 513];
        }
        Ok(())
    }
}

async fn check_output_payload(repartitioned: bool) {
    let broker = MockBroker::start(TOPIC, 2).await;
    let dir = TempDir::new().unwrap();
    let mut source: Vec<_> = (0..8).map(|offset| record(offset, 32, false)).collect();
    source[3].key = None;
    source[5].value = None;
    source[7].key = None;
    source[7].value = None;
    write_backup(dir.path(), "payload", &HashMap::from([(0, source)]), 4).await;

    let mut options = base_options();
    options.record_filter = Some(RecordFilterHandle::new(Arc::new(DropEvenTombstoneSome)));
    options.record_transform = Some(RecordTransformHandle::new(Arc::new(Resize)));
    // Added headers must not change key+value accounting.
    options.include_original_offset_header = true;
    options.produce_batch_size = 1;
    if repartitioned {
        options.repartitioning.insert(
            TOPIC.to_string(),
            TopicRepartitioning {
                target_partitions: 2,
                strategy: RepartitioningStrategy::Murmur2,
            },
        );
    }
    let engine = RestoreEngine::new(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "payload",
        options,
    ))
    .unwrap();
    let report = engine.run().await.expect("restore");
    // Kept offsets: 1 = 3-byte key/tombstone, 3 = null key/513-byte
    // value, 5 = 3-byte key/tombstone, 7 = null key/null value.
    assert_eq!(report.records_restored, 4);
    assert_eq!(report.bytes_restored, 519);
    assert_eq!(report.records_dropped_by_filter, 4);
    assert_eq!(report.records_tombstoned_by_filter, 2);
    let metrics = engine.metrics().report();
    assert_eq!(metrics.records_processed, 4);
    assert_eq!(metrics.bytes_written, 519);
    assert_eq!(metrics.bytes_uncompressed, 519);
    assert!(metrics.throughput_mbps > 0.0);
    let written_bytes: usize = (0..2)
        .flat_map(|partition| broker.log(partition))
        .map(|r| r.key.map_or(0, |k| k.len()) + r.value.map_or(0, |v| v.len()))
        .sum();
    assert_eq!(written_bytes, 519);
}

#[tokio::test]
async fn standard_restore_counts_filtered_transformed_and_null_payloads() {
    check_output_payload(false).await;
}

#[tokio::test]
async fn repartitioned_restore_counts_filtered_transformed_and_null_payloads() {
    check_output_payload(true).await;
}

#[tokio::test]
async fn dry_run_does_not_count_planned_bytes_as_restored() {
    let broker = MockBroker::start(TOPIC, 1).await;
    let dir = TempDir::new().unwrap();
    write_backup(
        dir.path(),
        "dry-run",
        &HashMap::from([(0, vec![record(0, 32, false)])]),
        1,
    )
    .await;
    let mut options = base_options();
    options.dry_run = true;
    let engine = RestoreEngine::new(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "dry-run",
        options,
    ))
    .unwrap();
    let report = engine.run().await.expect("dry run");
    assert!(report.dry_run);
    assert!(report.bytes_restored > 0, "dry-run report holds estimates");
    assert_eq!(engine.metrics().report().bytes_written, 0);
    assert_eq!(engine.metrics().report().records_processed, 0);
    assert!(broker.log(0).is_empty());
}

#[tokio::test]
async fn fully_filtered_restore_counts_no_bytes_on_either_path() {
    for repartitioned in [false, true] {
        let broker = MockBroker::start(TOPIC, 2).await;
        let dir = TempDir::new().unwrap();
        write_backup(
            dir.path(),
            "filtered",
            &HashMap::from([(0, vec![record(0, 32, false), record(2, 32, false)])]),
            2,
        )
        .await;
        let mut options = base_options();
        options.record_filter = Some(RecordFilterHandle::new(Arc::new(DropEvenTombstoneSome)));
        if repartitioned {
            options.repartitioning.insert(
                TOPIC.to_string(),
                TopicRepartitioning {
                    target_partitions: 2,
                    strategy: RepartitioningStrategy::Murmur2,
                },
            );
        }
        let engine = RestoreEngine::new(restore_config(
            &broker.bootstrap(),
            dir.path(),
            "filtered",
            options,
        ))
        .unwrap();
        let report = engine.run().await.expect("filtered restore");
        assert_eq!((report.records_restored, report.bytes_restored), (0, 0));
        assert_eq!(report.records_dropped_by_filter, 2);
        let metrics = engine.metrics().report();
        assert_eq!((metrics.records_processed, metrics.bytes_written), (0, 0));
        assert_eq!(metrics.throughput_mbps, 0.0);
        assert!(broker.log(0).is_empty());
        assert!(broker.log(1).is_empty());
    }
}
