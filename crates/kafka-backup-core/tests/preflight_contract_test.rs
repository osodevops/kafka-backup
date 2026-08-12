//! Issue #137: Phase 1 header preflight contract tests.
//!
//! These tests build real backups on filesystem storage (binary and legacy
//! JSON segment formats) and prove the preflight's coverage semantics:
//! present, missing, partial, legacy, empty, data-missing, and corrupt
//! backups; consumer-groups snapshot requirements; and fail-before-target-
//! mutation ordering for both the restore engine and the three-phase
//! orchestrator. No Kafka broker is required: mutation-ordering tests use an
//! unreachable target address and assert on the error class.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use tempfile::TempDir;

use kafka_backup_core::config::{
    Config, HeaderPreflightMode, KafkaConfig, Mode, RestoreOptions, SecurityConfig, TopicSelection,
};
use kafka_backup_core::manifest::{BackupManifest, PartitionBackup, SegmentMetadata, TopicBackup};
use kafka_backup_core::metrics::PerformanceMetrics;
use kafka_backup_core::restore::preflight::{run_header_preflight, PartitionCoverageState};
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::segment::{BinaryRecord, SegmentWriter, SegmentWriterConfig};
use kafka_backup_core::storage::{create_backend, StorageBackendConfig};
use kafka_backup_core::{Error, ThreePhaseRestore};

const TOPIC: &str = "orders";
const BACKUP_ID: &str = "preflight-backup";

fn record(offset: i64, with_headers: bool) -> BinaryRecord {
    let mut headers: Vec<(String, Option<Bytes>)> = vec![(
        "app-header".to_string(),
        Some(Bytes::from_static(b"app-value")),
    )];
    if with_headers {
        headers.push((
            "x-original-offset".to_string(),
            Some(Bytes::from(offset.to_le_bytes().to_vec())),
        ));
        headers.push((
            "x-original-timestamp".to_string(),
            Some(Bytes::from(
                (1_700_000_000_000 + offset).to_le_bytes().to_vec(),
            )),
        ));
    }
    BinaryRecord {
        timestamp: 1_700_000_000_000 + offset,
        offset,
        key: Some(Bytes::from(format!("key-{offset}"))),
        value: Some(Bytes::from(format!("value-{offset}"))),
        headers,
    }
}

/// Write one binary segment and return its manifest metadata.
async fn write_binary_segment(
    dir: &Path,
    partition: i32,
    start_offset: i64,
    count: i64,
    with_headers: bool,
) -> SegmentMetadata {
    let storage = Arc::new(kafka_backup_core::storage::FilesystemBackend::new(
        dir.to_path_buf(),
    ));
    let metrics = Arc::new(PerformanceMetrics::new());
    let mut writer = SegmentWriter::new(SegmentWriterConfig::default(), storage, metrics);
    for i in 0..count {
        writer
            .add_record(record(start_offset + i, with_headers))
            .unwrap();
    }
    let key = format!("{BACKUP_ID}/{TOPIC}/{partition}/{start_offset:020}.bin");
    writer
        .flush(&key)
        .await
        .unwrap()
        .expect("segment metadata for non-empty segment")
}

/// Write one legacy JSON segment (pre-binary format) and return metadata.
fn write_legacy_json_segment(
    dir: &Path,
    partition: i32,
    start_offset: i64,
    count: i64,
    with_headers: bool,
) -> SegmentMetadata {
    use kafka_backup_core::manifest::{BackupRecord, RecordHeader};
    let records: Vec<BackupRecord> = (0..count)
        .map(|i| {
            let offset = start_offset + i;
            let mut headers = Vec::new();
            if with_headers {
                headers.push(RecordHeader {
                    key: "x-original-offset".to_string(),
                    value: offset.to_le_bytes().to_vec(),
                });
                headers.push(RecordHeader {
                    key: "x-original-timestamp".to_string(),
                    value: (1_700_000_000_000 + offset).to_le_bytes().to_vec(),
                });
            }
            BackupRecord {
                key: Some(format!("key-{offset}").into_bytes()),
                value: Some(format!("value-{offset}").into_bytes()),
                headers,
                timestamp: 1_700_000_000_000 + offset,
                offset,
            }
        })
        .collect();

    let key = format!("{BACKUP_ID}/{TOPIC}/{partition}/{start_offset:020}.json");
    let path = dir.join(&key);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec(&records).unwrap()).unwrap();

    SegmentMetadata {
        key,
        start_offset,
        end_offset: start_offset + count - 1,
        start_timestamp: 1_700_000_000_000 + start_offset,
        end_timestamp: 1_700_000_000_000 + start_offset + count - 1,
        record_count: count,
        uncompressed_size: 0,
        compressed_size: 0,
    }
}

fn write_manifest(dir: &Path, partitions: Vec<PartitionBackup>) -> BackupManifest {
    let manifest = BackupManifest {
        backup_id: BACKUP_ID.to_string(),
        created_at: 1_700_000_000_000,
        source_cluster_id: Some("src-cluster".to_string()),
        source_brokers: vec!["source:9092".to_string()],
        compression: "zstd".to_string(),
        topics: vec![TopicBackup {
            name: TOPIC.to_string(),
            original_partition_count: Some(partitions.len().max(1) as i32),
            source_replication_factor: None,
            configurations: Default::default(),
            partitions,
        }],
    };
    let path = dir.join(BACKUP_ID).join("manifest.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    manifest
}

fn write_consumer_group_snapshot(dir: &Path, groups_json: &str) {
    let path = dir.join(BACKUP_ID).join("consumer-groups-snapshot.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, groups_json).unwrap();
}

fn recovery_options() -> RestoreOptions {
    RestoreOptions {
        reset_consumer_offsets: true,
        consumer_groups: vec!["cg-1".to_string()],
        ..RestoreOptions::default()
    }
}

async fn scan(
    dir: &Path,
    manifest: &BackupManifest,
    options: &RestoreOptions,
    mode: HeaderPreflightMode,
) -> kafka_backup_core::HeaderPreflightReport {
    let storage = create_backend(&StorageBackendConfig::Filesystem {
        path: dir.to_path_buf(),
    })
    .unwrap();
    run_header_preflight(
        storage.as_ref(),
        manifest,
        &TopicSelection::default(),
        options,
        mode,
    )
    .await
}

fn restore_config(dir: &Path, options: RestoreOptions) -> Config {
    Config {
        mode: Mode::Restore,
        backup_id: BACKUP_ID.to_string(),
        source: None,
        // Port 1 on localhost: connection refused immediately. Any attempt
        // to reach the target fails with a connection error, so a Preflight
        // error proves the preflight ran strictly before target contact.
        target: Some(KafkaConfig {
            bootstrap_servers: vec!["127.0.0.1:1".to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection::default(),
            connection: Default::default(),
        }),
        storage: StorageBackendConfig::Filesystem {
            path: dir.to_path_buf(),
        },
        backup: None,
        restore: Some(options),
        offset_storage: None,
        metrics: None,
    }
}

// ---------------------------------------------------------------------------
// Coverage state detection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn full_coverage_passes_offset_recovery() {
    let dir = TempDir::new().unwrap();
    let seg0 = write_binary_segment(dir.path(), 0, 0, 25, true).await;
    let seg1 = write_binary_segment(dir.path(), 1, 0, 15, true).await;
    let manifest = write_manifest(
        dir.path(),
        vec![
            PartitionBackup {
                partition_id: 0,
                segments: vec![seg0],
            },
            PartitionBackup {
                partition_id: 1,
                segments: vec![seg1],
            },
        ],
    );

    let report = scan(
        dir.path(),
        &manifest,
        &recovery_options(),
        HeaderPreflightMode::Auto,
    )
    .await;

    assert!(report.passed, "errors: {:?}", report.errors);
    assert!(report.scan_performed);
    assert!(report.offset_recovery_requested);
    assert_eq!(report.partitions.len(), 2);
    assert_eq!(report.records_scanned_total, 40);
    for p in &report.partitions {
        assert_eq!(p.state, PartitionCoverageState::Full);
        assert_eq!(p.records_with_required_headers, p.records_scanned);
        assert_eq!(p.manifest_record_count, p.records_scanned as i64);
    }
}

#[tokio::test]
async fn missing_headers_fails_recovery_but_warns_without() {
    let dir = TempDir::new().unwrap();
    let seg = write_binary_segment(dir.path(), 0, 0, 20, false).await;
    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    // Offset recovery requested: hard failure.
    let report = scan(
        dir.path(),
        &manifest,
        &recovery_options(),
        HeaderPreflightMode::Auto,
    )
    .await;
    assert!(!report.passed);
    assert_eq!(report.partitions[0].state, PartitionCoverageState::Missing);
    assert!(report
        .errors
        .iter()
        .any(|e| e.contains("include_offset_headers")));

    // No offset recovery: full-mode scan reports warnings but passes.
    let report = scan(
        dir.path(),
        &manifest,
        &RestoreOptions::default(),
        HeaderPreflightMode::Full,
    )
    .await;
    assert!(report.passed);
    assert!(!report.offset_recovery_requested);
    assert_eq!(report.partitions[0].state, PartitionCoverageState::Missing);
    assert!(report
        .warnings
        .iter()
        .any(|w| w.contains("Offset recovery from this backup will not be possible")));
}

#[tokio::test]
async fn partial_coverage_fails_offset_recovery() {
    let dir = TempDir::new().unwrap();
    let seg_with = write_binary_segment(dir.path(), 0, 0, 10, true).await;
    let seg_without = write_binary_segment(dir.path(), 0, 10, 10, false).await;
    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg_with, seg_without],
        }],
    );

    let report = scan(
        dir.path(),
        &manifest,
        &recovery_options(),
        HeaderPreflightMode::Auto,
    )
    .await;

    assert!(!report.passed);
    let p = &report.partitions[0];
    assert_eq!(p.state, PartitionCoverageState::Partial);
    assert_eq!(p.records_scanned, 20);
    assert_eq!(p.records_with_required_headers, 10);
    assert!(report.errors.iter().any(|e| e.contains("partial")));
}

#[tokio::test]
async fn legacy_json_segments_are_scanned_and_flagged() {
    let dir = TempDir::new().unwrap();
    // Legacy JSON format without tracking headers: classic legacy backup.
    let seg = write_legacy_json_segment(dir.path(), 0, 0, 12, false);
    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let report = scan(
        dir.path(),
        &manifest,
        &recovery_options(),
        HeaderPreflightMode::Auto,
    )
    .await;
    assert!(!report.passed);
    let p = &report.partitions[0];
    assert_eq!(p.state, PartitionCoverageState::Missing);
    assert_eq!(p.segments_legacy_format, 1);
    assert_eq!(p.records_scanned, 12);

    // Legacy JSON format WITH headers still counts as full coverage: the
    // scanner supports every segment format the restore engine reads.
    let dir2 = TempDir::new().unwrap();
    let seg2 = write_legacy_json_segment(dir2.path(), 0, 0, 8, true);
    let manifest2 = write_manifest(
        dir2.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg2],
        }],
    );
    let report2 = scan(
        dir2.path(),
        &manifest2,
        &recovery_options(),
        HeaderPreflightMode::Auto,
    )
    .await;
    assert!(report2.passed, "errors: {:?}", report2.errors);
    assert_eq!(report2.partitions[0].state, PartitionCoverageState::Full);
}

#[tokio::test]
async fn empty_backup_is_never_a_positive_pass() {
    let dir = TempDir::new().unwrap();
    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![],
        }],
    );

    // Offset recovery requested: zero records must fail.
    let report = scan(
        dir.path(),
        &manifest,
        &recovery_options(),
        HeaderPreflightMode::Auto,
    )
    .await;
    assert!(!report.passed);
    assert_eq!(report.partitions[0].state, PartitionCoverageState::Empty);
    assert_eq!(report.records_scanned_total, 0);
    assert!(report
        .errors
        .iter()
        .any(|e| e.contains("no records") || e.contains("zero records")));

    // Without offset recovery an empty backup is not blocking.
    let report = scan(
        dir.path(),
        &manifest,
        &RestoreOptions::default(),
        HeaderPreflightMode::Full,
    )
    .await;
    assert!(report.passed);
    assert_eq!(report.partitions[0].state, PartitionCoverageState::Empty);
}

#[tokio::test]
async fn corrupt_segment_is_detected() {
    let dir = TempDir::new().unwrap();
    let seg = write_binary_segment(dir.path(), 0, 0, 10, true).await;
    // Flip bytes in the middle of the segment: CRC verification fails.
    let path = dir.path().join(&seg.key);
    let mut bytes = std::fs::read(&path).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xFF;
    bytes[mid + 1] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let report = scan(
        dir.path(),
        &manifest,
        &recovery_options(),
        HeaderPreflightMode::Auto,
    )
    .await;
    assert!(!report.passed);
    let p = &report.partitions[0];
    assert_eq!(p.state, PartitionCoverageState::Corrupt);
    assert_eq!(p.segments_corrupt, 1);
    assert!(report.errors.iter().any(|e| e.contains("corrupt")));
}

#[tokio::test]
async fn missing_segment_object_is_detected() {
    let dir = TempDir::new().unwrap();
    let seg = write_binary_segment(dir.path(), 0, 0, 10, true).await;
    std::fs::remove_file(dir.path().join(&seg.key)).unwrap();

    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let report = scan(
        dir.path(),
        &manifest,
        &recovery_options(),
        HeaderPreflightMode::Auto,
    )
    .await;
    assert!(!report.passed);
    let p = &report.partitions[0];
    assert_eq!(p.state, PartitionCoverageState::DataMissing);
    assert_eq!(p.segments_missing, 1);
    assert!(report
        .errors
        .iter()
        .any(|e| e.contains("missing from") || e.contains("missing from storage")));
}

#[tokio::test]
async fn string_encoded_headers_count_as_coverage() {
    // The restore engine accepts UTF-8 decimal header values for backwards
    // compatibility; the preflight must accept the same encodings.
    let dir = TempDir::new().unwrap();
    let storage = Arc::new(kafka_backup_core::storage::FilesystemBackend::new(
        dir.path().to_path_buf(),
    ));
    let metrics = Arc::new(PerformanceMetrics::new());
    let mut writer = SegmentWriter::new(SegmentWriterConfig::default(), storage, metrics);
    for i in 0..5i64 {
        writer
            .add_record(BinaryRecord {
                timestamp: 1_700_000_000_000 + i,
                offset: i,
                key: None,
                value: Some(Bytes::from(format!("v{i}"))),
                headers: vec![
                    (
                        "x-original-offset".to_string(),
                        Some(Bytes::from(i.to_string())),
                    ),
                    (
                        "x-original-timestamp".to_string(),
                        Some(Bytes::from((1_700_000_000_000i64 + i).to_string())),
                    ),
                ],
            })
            .unwrap();
    }
    let key = format!("{BACKUP_ID}/{TOPIC}/0/{:020}.bin", 0);
    let seg = writer.flush(&key).await.unwrap().unwrap();
    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let report = scan(
        dir.path(),
        &manifest,
        &recovery_options(),
        HeaderPreflightMode::Auto,
    )
    .await;
    assert!(report.passed, "errors: {:?}", report.errors);
    assert_eq!(report.partitions[0].state, PartitionCoverageState::Full);
}

#[tokio::test]
async fn time_window_limits_the_scan_to_restorable_segments() {
    let dir = TempDir::new().unwrap();
    // Records at timestamps 1_700_000_000_000..+10 (headers absent).
    let seg = write_binary_segment(dir.path(), 0, 0, 10, false).await;
    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    // Window entirely after the segment: nothing would be restored, so the
    // missing headers are irrelevant -> Empty, and recovery fails on "no
    // records", not on missing headers.
    let mut options = recovery_options();
    options.time_window_start = Some(1_800_000_000_000);
    let report = scan(dir.path(), &manifest, &options, HeaderPreflightMode::Auto).await;
    assert_eq!(report.partitions[0].state, PartitionCoverageState::Empty);
    assert!(!report.passed);
}

// ---------------------------------------------------------------------------
// Consumer-groups snapshot requirements
// ---------------------------------------------------------------------------

#[tokio::test]
async fn auto_consumer_groups_requires_snapshot() {
    let dir = TempDir::new().unwrap();
    let seg = write_binary_segment(dir.path(), 0, 0, 10, true).await;
    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let options = RestoreOptions {
        auto_consumer_groups: true,
        ..RestoreOptions::default()
    };

    // Missing snapshot: fail with actionable error.
    let report = scan(dir.path(), &manifest, &options, HeaderPreflightMode::Auto).await;
    assert!(!report.passed);
    let snapshot = report.consumer_group_snapshot.as_ref().unwrap();
    assert_eq!(snapshot.state, "missing");
    assert!(report
        .errors
        .iter()
        .any(|e| e.contains("consumer-groups snapshot")));

    // Invalid snapshot: fail.
    write_consumer_group_snapshot(dir.path(), "{not json");
    let report = scan(dir.path(), &manifest, &options, HeaderPreflightMode::Auto).await;
    assert!(!report.passed);
    assert_eq!(
        report.consumer_group_snapshot.as_ref().unwrap().state,
        "invalid"
    );

    // Valid snapshot: pass, with group/offset counts reported.
    write_consumer_group_snapshot(
        dir.path(),
        r#"{"snapshot_time": 1700000000000, "groups": [
            {"group_id": "cg-1", "offsets": {"orders": {"0": 5}}}
        ]}"#,
    );
    let report = scan(dir.path(), &manifest, &options, HeaderPreflightMode::Auto).await;
    assert!(report.passed, "errors: {:?}", report.errors);
    let snapshot = report.consumer_group_snapshot.as_ref().unwrap();
    assert_eq!(snapshot.state, "present");
    assert_eq!(snapshot.groups, 1);
    assert_eq!(snapshot.offsets, 1);

    // Present but zero groups: pass with warning (a source cluster may
    // legitimately have no consumer groups).
    write_consumer_group_snapshot(dir.path(), r#"{"groups": []}"#);
    let report = scan(dir.path(), &manifest, &options, HeaderPreflightMode::Auto).await;
    assert!(report.passed, "errors: {:?}", report.errors);
    assert!(report.warnings.iter().any(|w| w.contains("no groups")));
}

#[tokio::test]
async fn reset_request_that_can_never_act_fails() {
    let dir = TempDir::new().unwrap();
    let seg = write_binary_segment(dir.path(), 0, 0, 10, true).await;
    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let options = RestoreOptions {
        reset_consumer_offsets: true,
        consumer_groups: vec![],
        auto_consumer_groups: false,
        ..RestoreOptions::default()
    };
    let report = scan(dir.path(), &manifest, &options, HeaderPreflightMode::Auto).await;
    assert!(!report.passed);
    assert!(report
        .errors
        .iter()
        .any(|e| e.contains("silently do nothing")));
}

// ---------------------------------------------------------------------------
// Modes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn skip_mode_never_blocks_but_warns_loudly() {
    let dir = TempDir::new().unwrap();
    let seg = write_binary_segment(dir.path(), 0, 0, 10, false).await;
    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let mut options = recovery_options();
    options.header_preflight = HeaderPreflightMode::Skip;
    let report = scan(dir.path(), &manifest, &options, HeaderPreflightMode::Skip).await;
    assert!(report.passed, "skip mode must never block");
    assert!(!report.scan_performed);
    assert!(report.warnings.iter().any(|w| w.contains("UNVERIFIED")));
}

#[tokio::test]
async fn auto_mode_without_recovery_reports_indeterminate_without_scanning() {
    let dir = TempDir::new().unwrap();
    let seg = write_binary_segment(dir.path(), 0, 0, 10, false).await;
    let manifest = write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let report = scan(
        dir.path(),
        &manifest,
        &RestoreOptions::default(),
        HeaderPreflightMode::Auto,
    )
    .await;
    assert!(report.passed);
    assert!(!report.scan_performed);
    assert_eq!(
        report.partitions[0].state,
        PartitionCoverageState::Indeterminate
    );
    assert_eq!(report.records_scanned_total, 0);
}

// ---------------------------------------------------------------------------
// Wiring: three-phase orchestrator and restore engine fail before any
// target interaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn validate_phase1_headers_returns_structured_report() {
    let dir = TempDir::new().unwrap();
    let seg = write_binary_segment(dir.path(), 0, 0, 10, true).await;
    write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let config = restore_config(dir.path(), recovery_options());
    let orchestrator = ThreePhaseRestore::new(config).unwrap();
    let report = orchestrator.validate_phase1_headers().await.unwrap();

    assert!(report.passed, "errors: {:?}", report.errors);
    assert_eq!(report.backup_id, BACKUP_ID);
    assert_eq!(report.partitions.len(), 1);
    assert_eq!(report.partitions[0].state, PartitionCoverageState::Full);
    assert_eq!(report.records_scanned_total, 10);
}

#[tokio::test]
async fn run_all_phases_fails_preflight_before_target_connection() {
    let dir = TempDir::new().unwrap();
    // Backup WITHOUT tracking headers + offset recovery requested.
    let seg = write_binary_segment(dir.path(), 0, 0, 10, false).await;
    write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let mut options = recovery_options();
    options.create_topics = true;
    let config = restore_config(dir.path(), options);
    let orchestrator = ThreePhaseRestore::new(config).unwrap();
    let err = orchestrator.run_all_phases().await.unwrap_err();

    // A Preflight error proves the failure happened before the engine tried
    // to reach the (unreachable) target: any target interaction would have
    // produced a connection error instead.
    assert!(
        matches!(err, Error::Preflight(_)),
        "expected Error::Preflight, got: {err:?}"
    );
    let msg = err.to_string();
    assert!(msg.contains("no topics were created"));
}

#[tokio::test]
async fn restore_engine_fails_preflight_before_target_connection() {
    let dir = TempDir::new().unwrap();
    let seg = write_binary_segment(dir.path(), 0, 0, 10, false).await;
    write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let mut options = recovery_options();
    options.create_topics = true;
    let config = restore_config(dir.path(), options);
    let engine = RestoreEngine::new(config).unwrap();
    let err = engine.run().await.unwrap_err();
    assert!(
        matches!(err, Error::Preflight(_)),
        "expected Error::Preflight, got: {err:?}"
    );

    // Control: with full header coverage the preflight passes and the SAME
    // config proceeds until the unreachable target rejects the connection —
    // proving the preflight verdict (not the dead target) produced the
    // Preflight error above.
    let dir2 = TempDir::new().unwrap();
    let seg2 = write_binary_segment(dir2.path(), 0, 0, 10, true).await;
    write_manifest(
        dir2.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg2],
        }],
    );
    write_consumer_group_snapshot(
        dir2.path(),
        r#"{"groups": [{"group_id": "cg-1", "offsets": {"orders": {"0": 5}}}]}"#,
    );
    let mut options2 = recovery_options();
    options2.create_topics = true;
    let config2 = restore_config(dir2.path(), options2);
    let engine2 = RestoreEngine::new(config2).unwrap();
    let err2 = engine2.run().await.unwrap_err();
    assert!(
        !matches!(err2, Error::Preflight(_)),
        "preflight must pass for covered backup; got: {err2:?}"
    );
}

#[tokio::test]
async fn dry_run_embeds_preflight_and_blocks_invalid_recovery() {
    let dir = TempDir::new().unwrap();
    let seg = write_binary_segment(dir.path(), 0, 0, 10, false).await;
    write_manifest(
        dir.path(),
        vec![PartitionBackup {
            partition_id: 0,
            segments: vec![seg],
        }],
    );

    let config = restore_config(dir.path(), recovery_options());
    let engine = RestoreEngine::new(config).unwrap();
    let report = engine.dry_run().await.unwrap();

    assert!(
        !report.valid,
        "dry run must fail the offset-recovery request"
    );
    let preflight = report.header_preflight.expect("preflight embedded");
    assert!(!preflight.passed);
    assert_eq!(
        preflight.partitions[0].state,
        PartitionCoverageState::Missing
    );
    assert!(!report.errors.is_empty());

    // Without offset recovery the same backup dry-runs clean (auto mode does
    // not scan and nothing blocks).
    let config = restore_config(dir.path(), RestoreOptions::default());
    let engine = RestoreEngine::new(config).unwrap();
    let report = engine.dry_run().await.unwrap();
    assert!(report.valid);
    assert!(report.header_preflight.is_none());
}
