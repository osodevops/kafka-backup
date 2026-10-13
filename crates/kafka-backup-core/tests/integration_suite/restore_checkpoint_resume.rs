//! Restore checkpoint durability and resume reconciliation against an
//! in-process broker mock (no Docker): the checkpoint exists from the start
//! (#160) and only ever holds acknowledged work; a killed run resumes with
//! zero duplicates and nothing missing (the unsaved tail is verified against
//! the target and skipped); another writer is detected; a failing partition
//! stops its siblings; the remote copy lets a new pod resume; repartitioned
//! segments complete only once acknowledged.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use kafka_backup_core::config::{
    RepartitioningStrategy, RestoreCheckpointStorage, RestoreOptions, TopicRepartitioning,
};
use kafka_backup_core::manifest::{BackupRecord, RestoreCheckpoint};
use kafka_backup_core::restore::RestoreEngine;
use tempfile::TempDir;

use super::produce_mock_broker::{Fault, MockBroker};
use super::restore_produce_path::{
    base_options, log_digest, record, restore_config, source_digest, write_backup, TOPIC,
};

fn read_checkpoint(path: &Path) -> RestoreCheckpoint {
    serde_json::from_slice(&std::fs::read(path).expect("checkpoint file")).expect("checkpoint json")
}

/// Move the checkpoint's last save an hour back, as if the restarted run
/// came up after the interrupted run's in-doubt window.
fn age_checkpoint(path: &Path) {
    let mut cp = read_checkpoint(path);
    cp.last_checkpoint_time -= 3_600_000;
    std::fs::write(path, serde_json::to_vec_pretty(&cp).unwrap()).unwrap();
}

fn records(n: i64) -> Vec<BackupRecord> {
    (0..n).map(|i| record(i, 48, false)).collect()
}

fn checkpoint_options(path: &Path) -> RestoreOptions {
    RestoreOptions {
        checkpoint_state: Some(path.to_path_buf()),
        // Only the start, per-topic and final saves in these tests.
        checkpoint_interval_secs: 3_600,
        produce_timeout_ms: 1_000,
        ..base_options()
    }
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    for _ in 0..1_000 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("condition not reached");
}

/// Run a restore until the broker has received `attempts` produce
/// requests, the last of which hangs, then kill the run.
async fn run_and_kill(
    broker: &MockBroker,
    config: kafka_backup_core::config::Config,
    attempts: usize,
) {
    broker.schedule(attempts - 1, Fault::Hang);
    let run = tokio::spawn(async move { RestoreEngine::new(config).unwrap().run().await });
    let state = broker.state.clone();
    wait_for(|| state.lock().unwrap().attempts.len() >= attempts).await;
    run.abort();
    let _ = run.await;
}

/// #160: with `checkpoint_state` set, the file exists as soon as the
/// restore starts, with a baseline per partition, before anything was
/// acknowledged.
#[tokio::test]
async fn checkpoint_file_is_written_before_anything_is_produced() {
    let broker = MockBroker::start(TOPIC, 2).await;
    let dir = TempDir::new().unwrap();
    let cp_path = dir.path().join("state").join("restore-checkpoint.json");
    write_backup(
        dir.path(),
        "start",
        &HashMap::from([(0, records(20)), (1, records(20))]),
        10,
    )
    .await;

    // The first produce hangs (and holds the one pooled connection).
    broker.schedule(0, Fault::Hang);
    let config = restore_config(
        &broker.bootstrap(),
        dir.path(),
        "start",
        checkpoint_options(&cp_path),
    );
    let run = tokio::spawn(async move { RestoreEngine::new(config).unwrap().run().await });
    let state = broker.state.clone();
    wait_for(|| !state.lock().unwrap().attempts.is_empty()).await;

    let cp = read_checkpoint(&cp_path);
    assert!(!cp.stopped_cleanly);
    assert!(cp.segments_completed.is_empty());
    assert_eq!(cp.partitions.len(), 2);
    for p in cp.partitions.values() {
        assert_eq!(p.last_source_offset_acked, -1);
        assert_eq!(p.target_hw_after_ack, 0);
    }
    assert!(cp.restore_id.is_some());
    run.abort();
}

/// A finished restore leaves a checkpoint with every segment, the progress
/// of every partition and the totals (records_restored / bytes_restored
/// were never updated before).
#[tokio::test]
async fn finished_restore_checkpoint_records_all_acknowledged_progress() {
    let broker = MockBroker::start(TOPIC, 2).await;
    let dir = TempDir::new().unwrap();
    let cp_path = dir.path().join("cp.json");
    write_backup(
        dir.path(),
        "done",
        &HashMap::from([(0, records(25)), (1, records(12))]),
        10,
    )
    .await;

    let report = RestoreEngine::new(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "done",
        checkpoint_options(&cp_path),
    ))
    .unwrap()
    .run()
    .await
    .expect("restore");
    assert_eq!(report.records_restored, 37);

    let cp = read_checkpoint(&cp_path);
    assert!(cp.stopped_cleanly);
    assert_eq!(cp.segments_completed.len(), 3 + 2);
    assert_eq!(cp.records_restored, 37);
    assert!(cp.bytes_restored > 0);
    let p0 = &cp.partitions[&RestoreCheckpoint::partition_key(TOPIC, 0)];
    assert_eq!(
        (p0.last_source_offset_acked, p0.target_hw_after_ack),
        (24, 25)
    );
    assert_eq!(p0.records_acked, 25);
    let p1 = &cp.partitions[&RestoreCheckpoint::partition_key(TOPIC, 1)];
    assert_eq!(
        (p1.last_source_offset_acked, p1.target_hw_after_ack),
        (11, 12)
    );

    // Re-running the finished restore produces nothing.
    let attempts = broker.attempts().len();
    RestoreEngine::new(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "done",
        checkpoint_options(&cp_path),
    ))
    .unwrap()
    .run()
    .await
    .expect("re-run");
    assert_eq!(broker.attempts().len(), attempts);
    assert_eq!(broker.log(0).len(), 25);
}

/// Killed after five acknowledged batches that no checkpoint save
/// recorded: the resumed run finds them through the target's high
/// watermark, verifies them record by record, and produces only the rest.
#[tokio::test]
async fn killed_restore_resumes_with_zero_duplicates_and_nothing_missing() {
    let broker = MockBroker::start(TOPIC, 1).await;
    let dir = TempDir::new().unwrap();
    let cp_path = dir.path().join("cp.json");
    let source = records(100);
    write_backup(
        dir.path(),
        "kill",
        &HashMap::from([(0, source.clone())]),
        40,
    )
    .await;
    let config = || {
        restore_config(
            &broker.bootstrap(),
            dir.path(),
            "kill",
            checkpoint_options(&cp_path),
        )
    };

    run_and_kill(&broker, config(), 6).await;
    assert_eq!(
        broker.log(0).len(),
        50,
        "five batches acknowledged before the kill"
    );
    let saved = read_checkpoint(&cp_path);
    assert!(!saved.stopped_cleanly);
    assert!(
        saved.segments_completed.is_empty(),
        "no save after the baseline"
    );
    age_checkpoint(&cp_path);

    let attempts_before = broker.attempts().len();
    let report = RestoreEngine::new(config())
        .unwrap()
        .run()
        .await
        .expect("resume");

    let log = broker.log(0);
    assert_eq!(log.len(), 100, "zero duplicates, nothing missing");
    assert_eq!(log_digest(&log), source_digest(&source));
    assert_eq!(
        broker.attempts().len() - attempts_before,
        5,
        "only the rest was produced"
    );
    // The resumed run's offset mapping covers the verified tail too.
    let pairs = &report.offset_mapping.detailed_mappings[&format!("{TOPIC}/0")];
    assert_eq!(pairs.len(), 100);
    assert!(pairs.iter().all(|p| p.source_offset == p.target_offset));
    let cp = read_checkpoint(&cp_path);
    assert!(cp.stopped_cleanly);
    assert_eq!(
        cp.partitions[&RestoreCheckpoint::partition_key(TOPIC, 0)].target_hw_after_ack,
        100
    );
}

/// A record another producer wrote after the restore's own records is
/// detected when the tail is verified: the resume fails instead of
/// skipping or duplicating.
#[tokio::test]
async fn resume_detects_another_writer() {
    let broker = MockBroker::start(TOPIC, 1).await;
    let dir = TempDir::new().unwrap();
    let cp_path = dir.path().join("cp.json");
    write_backup(dir.path(), "other", &HashMap::from([(0, records(60))]), 30).await;
    let config = || {
        restore_config(
            &broker.bootstrap(),
            dir.path(),
            "other",
            checkpoint_options(&cp_path),
        )
    };

    run_and_kill(&broker, config(), 3).await;
    assert_eq!(broker.log(0).len(), 20);
    broker.append_foreign(0, &[b"not from the backup"]);
    age_checkpoint(&cp_path);

    let err = RestoreEngine::new(config())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("Another writer"), "{message}");
    assert!(message.contains("target offset 20"), "{message}");
    assert_eq!(broker.log(0).len(), 21, "nothing produced");
}

/// The target holding fewer records than the checkpoint says were
/// acknowledged (topic recreated) fails the resume with a clear error.
#[tokio::test]
async fn resume_detects_a_truncated_target() {
    let broker = MockBroker::start(TOPIC, 1).await;
    let dir = TempDir::new().unwrap();
    let cp_path = dir.path().join("cp.json");
    write_backup(dir.path(), "trunc", &HashMap::from([(0, records(30))]), 10).await;
    let config = || {
        restore_config(
            &broker.bootstrap(),
            dir.path(),
            "trunc",
            checkpoint_options(&cp_path),
        )
    };

    run_and_kill(&broker, config(), 2).await;
    let mut cp = read_checkpoint(&cp_path);
    let key = RestoreCheckpoint::partition_key(TOPIC, 0);
    cp.partitions.get_mut(&key).unwrap().target_hw_after_ack = 500;
    cp.last_checkpoint_time -= 3_600_000;
    std::fs::write(&cp_path, serde_json::to_vec(&cp).unwrap()).unwrap();

    let err = RestoreEngine::new(config())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("below the 500"), "{err}");
}

/// One partition failing stops its siblings after their in-flight batch;
/// all are awaited, so the saved checkpoint matches exactly what the
/// target acknowledged, and a resume completes without duplicates.
#[tokio::test]
async fn failing_partition_stops_its_siblings_and_the_checkpoint_holds_only_acked_work() {
    let broker = MockBroker::start(TOPIC, 3).await;
    let dir = TempDir::new().unwrap();
    let cp_path = dir.path().join("cp.json");
    let sources: HashMap<i32, Vec<BackupRecord>> = (0..3).map(|p| (p, records(300))).collect();
    write_backup(dir.path(), "fail", &sources, 50).await;
    let config = || {
        restore_config(
            &broker.bootstrap(),
            dir.path(),
            "fail",
            checkpoint_options(&cp_path),
        )
    };

    // Partition 1's third batch is rejected with a non-retriable error.
    broker.queue_partition_faults(1, &[Fault::Pass, Fault::Pass, Fault::Error(87)]);
    let err = RestoreEngine::new(config())
        .unwrap()
        .run()
        .await
        .unwrap_err();
    assert!(err.to_string().contains("87"), "{err}");

    let cp = read_checkpoint(&cp_path);
    assert!(cp.stopped_cleanly, "nothing was left in flight");
    let mut acked_total = 0;
    for p in 0..3 {
        let progress = &cp.partitions[&RestoreCheckpoint::partition_key(TOPIC, p)];
        let written = broker.log(p).len() as i64;
        assert_eq!(progress.target_hw_after_ack, written, "partition {p}");
        assert_eq!(progress.records_acked as i64, written, "partition {p}");
        assert_eq!(
            progress.last_source_offset_acked,
            written - 1,
            "partition {p}"
        );
        acked_total += written;
    }
    assert_eq!(broker.log(1).len(), 20);
    assert!(
        acked_total < 900,
        "siblings stopped early ({acked_total} acknowledged)"
    );

    let report = RestoreEngine::new(config())
        .unwrap()
        .run()
        .await
        .expect("resume");
    assert!(report.errors.is_empty());
    for p in 0..3 {
        assert_eq!(
            log_digest(&broker.log(p)),
            source_digest(&sources[&p]),
            "partition {p}: zero duplicates, nothing missing"
        );
    }
}

/// `checkpoint_storage: remote`: a new pod without the local file resumes
/// from `{backup_id}/restore-checkpoints/{restore_id}.json`.
#[tokio::test]
async fn remote_checkpoint_lets_a_new_pod_resume() {
    let broker = MockBroker::start(TOPIC, 1).await;
    let dir = TempDir::new().unwrap();
    let source = records(80);
    write_backup(
        dir.path(),
        "remote",
        &HashMap::from([(0, source.clone())]),
        20,
    )
    .await;
    let options = RestoreOptions {
        checkpoint_state: None,
        checkpoint_storage: RestoreCheckpointStorage::Remote,
        ..checkpoint_options(Path::new("unused"))
    };
    let config = || restore_config(&broker.bootstrap(), dir.path(), "remote", options.clone());

    run_and_kill(&broker, config(), 4).await;
    let remote_dir = dir.path().join("remote").join("restore-checkpoints");
    let files: Vec<PathBuf> = std::fs::read_dir(&remote_dir)
        .expect("remote checkpoint directory")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "json"))
        .collect();
    assert_eq!(files.len(), 1, "{files:?}");
    let restore_id = read_checkpoint(&files[0]).restore_id.expect("restore id");
    assert_eq!(
        files[0].file_name().unwrap().to_string_lossy(),
        format!("{restore_id}.json")
    );
    age_checkpoint(&files[0]);

    RestoreEngine::new(config())
        .unwrap()
        .run()
        .await
        .expect("resume");
    assert_eq!(log_digest(&broker.log(0)), source_digest(&source));
}

/// Fan-out repartitioning: a segment is listed as complete only once every
/// one of its records was acknowledged (it used to be marked when its
/// records were queued).
#[tokio::test]
async fn repartitioned_segments_complete_only_once_acknowledged() {
    let broker = MockBroker::start(TOPIC, 2).await;
    let dir = TempDir::new().unwrap();
    let cp_path = dir.path().join("cp.json");
    let sources: HashMap<i32, Vec<BackupRecord>> = (0..2)
        .map(|p| {
            let rs = (0..60)
                .map(|i| {
                    let mut r = record(i, 32, false);
                    r.key = Some(format!("p{p}-k{i}").into_bytes());
                    r
                })
                .collect();
            (p, rs)
        })
        .collect();
    write_backup(dir.path(), "repart", &sources, 10).await;

    broker.queue_partition_faults(1, &[Fault::Pass, Fault::Error(87)]);
    let options = RestoreOptions {
        repartitioning: HashMap::from([(
            TOPIC.to_string(),
            TopicRepartitioning {
                strategy: RepartitioningStrategy::Murmur2,
                target_partitions: 2,
            },
        )]),
        produce_batch_size: 5,
        ..checkpoint_options(&cp_path)
    };
    let err = RestoreEngine::new(restore_config(
        &broker.bootstrap(),
        dir.path(),
        "repart",
        options,
    ))
    .unwrap()
    .run()
    .await
    .unwrap_err();
    assert!(err.to_string().contains("87"), "{err}");

    let written: HashSet<Vec<u8>> = (0..2)
        .flat_map(|p| broker.log(p))
        .filter_map(|r| r.key.map(|k| k.to_vec()))
        .collect();
    let cp = read_checkpoint(&cp_path);
    assert!(
        cp.segments_completed.len() < 12,
        "the failure left segments incomplete"
    );
    for key in &cp.segments_completed {
        let p: i32 = key
            .split("partition=")
            .nth(1)
            .and_then(|s| s.split('/').next())
            .unwrap()
            .parse()
            .unwrap();
        let start: i64 = key
            .rsplit("segment-")
            .next()
            .and_then(|s| s.strip_suffix(".bin"))
            .unwrap()
            .parse()
            .unwrap();
        for r in &sources[&p][start as usize..start as usize + 10] {
            assert!(
                written.contains(r.key.as_ref().unwrap()),
                "segment {key} is complete but a record of it was never acknowledged"
            );
        }
    }
}
