//! Crash-consistency of backup checkpoints (F2).
//!
//! Per seed: a topic with three partitions is backed up repeatedly while it
//! grows. Each iteration runs the engine on a fresh Tokio runtime and injects
//! one fault chosen by the seed:
//!
//! - **crash** — a failpoint callback freezes the thread that hits it (the
//!   process "dies" at exactly that point) and the runtime is shut down
//!   without running any cleanup;
//! - **error** — a failpoint makes that call return an error;
//! - **graceful** — the engine's shutdown signal fires mid-run;
//! - **none** — the run completes.
//!
//! After every iteration the stored state must satisfy the invariants:
//! every checkpoint in the stored `offsets.db` is within the stored
//! manifest's reach (I1/I2), and the listed segments' covered ranges are
//! contiguous and never overlap (I3). After `k` faulty iterations a final
//! clean run must succeed, and reading every listed segment back must give
//! each produced record exactly once: nothing missing, nothing duplicated.
//!
//! Seeds: `KAFKA_BACKUP_CHAOS_SEEDS` (default 10; 100 for nightly runs),
//! starting at `KAFKA_BACKUP_CHAOS_SEED_BASE` (default 0x5EED).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use tempfile::TempDir;

use kafka_backup_core::backup::BackupEngine;
use kafka_backup_core::config::{
    BackupOptions, CompressionType, Config, KafkaConfig, Mode, OffsetStorageConfig, SecurityConfig,
    TopicSelection,
};
use kafka_backup_core::kafka::TopicToCreate;
use kafka_backup_core::manifest::{BackupManifest, BackupRecord};
use kafka_backup_core::segment::SegmentReader;
use kafka_backup_core::storage::StorageBackendConfig;
use kafka_backup_core::{OffsetStore, OffsetStoreConfig, SqliteOffsetStore};

use super::common::KafkaTestCluster;

const PARTITIONS: i32 = 3;

const FAILPOINTS: [&str; 5] = [
    "backup::segment_put::after",
    "backup::manifest_put::before",
    "backup::manifest_put::after",
    "backup::offsets_sync::before",
    "backup::fetch::after",
];

/// Deterministic per-seed randomness (splitmix64) — no extra dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `lo..=hi`.
    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.next() as usize % items.len()]
    }
}

#[derive(Debug, Clone)]
enum Fault {
    Crash { failpoint: &'static str, nth: usize },
    Error { failpoint: &'static str, nth: usize },
    Graceful { after: Duration },
    None,
}

impl Fault {
    fn choose(rng: &mut Rng) -> Fault {
        let failpoint = *rng.pick(&FAILPOINTS);
        let nth = rng.range(1, 8) as usize;
        match rng.range(0, 99) {
            0..=44 => Fault::Crash { failpoint, nth },
            45..=74 => Fault::Error { failpoint, nth },
            75..=89 => Fault::Graceful {
                after: Duration::from_millis(rng.range(0, 1_500)),
            },
            _ => Fault::None,
        }
    }
}

/// How an iteration ended.
#[derive(Debug)]
enum Outcome {
    Finished(Result<(), String>),
    Crashed,
}

/// The per-seed backup configuration.
#[derive(Debug, Clone)]
struct Plan {
    snapshot: bool,
    segment_records: u64,
    sync_secs: u64,
    fresh_local_db: bool,
}

fn config(
    bootstrap: &str,
    storage: &Path,
    db_path: PathBuf,
    backup_id: &str,
    topic: &str,
    plan: &Plan,
) -> Config {
    Config {
        mode: Mode::Backup,
        backup_id: backup_id.to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec![bootstrap.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: vec![topic.to_string()],
                exclude: vec![],
            },
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem {
            path: storage.to_path_buf(),
        },
        backup: Some(BackupOptions {
            compression: CompressionType::Zstd,
            segment_max_records: Some(plan.segment_records),
            segment_max_interval_ms: 60_000,
            // Small fetches so a run takes several fetches per segment.
            fetch_max_bytes: Some(4 * 1024),
            stop_at_current_offsets: plan.snapshot,
            continuous: false,
            sync_interval_secs: plan.sync_secs,
            max_concurrent_partitions: PARTITIONS as usize,
            shutdown_grace_secs: 10,
            capture_topic_configs: false,
            ..Default::default()
        }),
        restore: None,
        offset_storage: Some(OffsetStorageConfig {
            db_path,
            ..Default::default()
        }),
        metrics: None,
    }
}

fn record_value(partition: i32, n: usize) -> Vec<u8> {
    format!("p{partition}-n{n}-{}", "x".repeat(40 + (n * 7) % 90)).into_bytes()
}

/// Produce `count` more records to every partition. `produced[p]` holds the
/// values in offset order (no compaction, no transactions: offset == index).
async fn produce(
    cluster: &KafkaTestCluster,
    topic: &str,
    count: usize,
    produced: &mut [Vec<Vec<u8>>],
) {
    let client = cluster.create_client();
    client.connect().await.expect("connect");
    for partition in 0..PARTITIONS {
        let existing = produced[partition as usize].len();
        let values: Vec<Vec<u8>> = (existing..existing + count)
            .map(|n| record_value(partition, n))
            .collect();
        for chunk in values.chunks(100) {
            let records = chunk
                .iter()
                .map(|value| BackupRecord {
                    key: Some(format!("k{partition}").into_bytes()),
                    value: Some(value.clone()),
                    headers: vec![],
                    timestamp: chrono::Utc::now().timestamp_millis(),
                    offset: 0,
                })
                .collect();
            client
                .produce(topic, partition, records, -1, 30_000)
                .await
                .expect("produce");
        }
        produced[partition as usize].extend(values);
    }
}

/// Run one backup on its own runtime with `fault` armed, then tear the
/// runtime down without letting anything clean up.
fn run_iteration(config: Config, fault: &Fault) -> Outcome {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("runtime");

    let (crash_tx, crash_rx) = mpsc::channel::<()>();
    match fault {
        Fault::Crash { failpoint, nth } => {
            let hits = Arc::new(AtomicUsize::new(0));
            let crash_tx = Mutex::new(crash_tx.clone());
            let nth = *nth;
            fail::cfg_callback(*failpoint, move || {
                if hits.fetch_add(1, Ordering::SeqCst) + 1 == nth {
                    let _ = crash_tx.lock().unwrap().send(());
                    // The "process" dies here: this thread never runs
                    // another instruction of the engine.
                    loop {
                        std::thread::park();
                    }
                }
            })
            .expect("cfg_callback");
        }
        Fault::Error { failpoint, nth } => {
            let actions = if *nth == 1 {
                "1*return(chaos)".to_string()
            } else {
                format!("{}*off->1*return(chaos)", nth - 1)
            };
            fail::cfg(*failpoint, &actions).expect("cfg");
        }
        Fault::Graceful { .. } | Fault::None => {}
    }

    let (done_tx, done_rx) = mpsc::channel::<Result<(), String>>();
    let (handle_tx, handle_rx) = mpsc::channel();
    runtime.spawn(async move {
        let engine = match BackupEngine::new(config).await {
            Ok(engine) => engine,
            Err(e) => {
                let _ = done_tx.send(Err(format!("engine: {e}")));
                return;
            }
        };
        let _ = handle_tx.send(engine.shutdown_handle());
        let result = engine.run().await.map_err(|e| e.to_string());
        let _ = done_tx.send(result);
    });

    if let Fault::Graceful { after } = fault {
        if let Ok(handle) = handle_rx.recv_timeout(Duration::from_secs(60)) {
            std::thread::sleep(*after);
            let _ = handle.send(());
        }
    }

    // Wait for whichever comes first: a crash or the end of the run.
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    let outcome = loop {
        if crash_rx.try_recv().is_ok() {
            break Outcome::Crashed;
        }
        match done_rx.recv_timeout(Duration::from_millis(20)) {
            Ok(result) => break Outcome::Finished(result),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                // The task was dropped without reporting: only a crash does that.
                break Outcome::Crashed;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "backup iteration hung (fault {fault:?})"
        );
    };

    for failpoint in FAILPOINTS {
        fail::remove(failpoint);
    }
    // Drop every task without running it further. The frozen thread is
    // leaked. Waiting (bounded) for the blocking pool lets a file write that
    // was already executing finish now rather than land during the next
    // iteration — a single writer that dies, not a zombie that keeps going.
    runtime.shutdown_timeout(Duration::from_secs(2));
    outcome
}

fn read_manifest(storage: &Path, backup_id: &str) -> Option<BackupManifest> {
    let path = storage.join(backup_id).join("manifest.json");
    let data = std::fs::read(path).ok()?;
    Some(serde_json::from_slice(&data).expect("stored manifest parses"))
}

/// Checkpoints in the stored (remote) offsets.db.
fn stored_checkpoints(storage: &Path, backup_id: &str) -> HashMap<(String, i32), i64> {
    let remote = storage.join(backup_id).join("offsets.db");
    let Ok(bytes) = std::fs::read(&remote) else {
        return HashMap::new();
    };
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("copy.db");
    std::fs::write(&path, bytes).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let store = SqliteOffsetStore::new(OffsetStoreConfig {
            db_path: path,
            ..Default::default()
        })
        .await
        .expect("open stored offsets.db");
        store
            .get_all_offsets(backup_id)
            .await
            .unwrap()
            .into_iter()
            .map(|o| ((o.topic, o.partition), o.last_offset))
            .collect()
    })
}

/// I1/I2 and I3 on what is in storage right now.
fn assert_invariants(storage: &Path, backup_id: &str, context: &str) {
    let manifest = read_manifest(storage, backup_id);
    for ((topic, partition), checkpoint) in stored_checkpoints(storage, backup_id) {
        let reach = manifest
            .as_ref()
            .and_then(|m| m.topics.iter().find(|t| t.name == topic))
            .and_then(|t| t.partitions.iter().find(|p| p.partition_id == partition))
            .and_then(|p| p.reach());
        assert!(
            reach.is_some_and(|r| checkpoint <= r),
            "{context}: stored checkpoint {topic}:{partition}={checkpoint} is ahead of the \
             stored manifest reach {reach:?}"
        );
    }
    let Some(manifest) = manifest else { return };
    for topic in &manifest.topics {
        for part in &topic.partitions {
            let mut ranges: Vec<(i64, i64)> = part
                .segments
                .iter()
                .map(|s| (s.covered_start(), s.covered_end()))
                .chain(part.gaps.iter().map(|g| (g.start_offset, g.end_offset - 1)))
                .collect();
            ranges.sort();
            for pair in ranges.windows(2) {
                assert_eq!(
                    pair[1].0,
                    pair[0].1 + 1,
                    "{context}: {}:{} covered ranges not contiguous / overlapping: {ranges:?}",
                    topic.name,
                    part.partition_id
                );
            }
        }
    }
}

/// Read every listed segment back and compare with what was produced: each
/// partition must hold offsets `0..n` exactly once with the produced values,
/// where `n` is everything produced (`complete`) or the manifest reach + 1.
/// Returns the reach per partition.
fn assert_stored(
    storage: &Path,
    backup_id: &str,
    topic: &str,
    produced: &[Vec<Vec<u8>>],
    complete: bool,
) -> Vec<i64> {
    let manifest = read_manifest(storage, backup_id).expect("stored manifest");
    assert_eq!(manifest.total_gaps(), 0, "no retention, so no gaps");
    let topic_backup = manifest
        .topics
        .iter()
        .find(|t| t.name == topic)
        .expect("topic in manifest");

    let mut reaches = Vec::new();
    for partition in 0..PARTITIONS {
        let expected = &produced[partition as usize];
        let part = topic_backup
            .partitions
            .iter()
            .find(|p| p.partition_id == partition)
            .unwrap_or_else(|| panic!("partition {partition} in manifest"));
        let reach = part.reach().unwrap_or(-1);
        let n = if complete {
            assert_eq!(
                reach,
                expected.len() as i64 - 1,
                "partition {partition}: reach is the last offset"
            );
            expected.len()
        } else {
            (reach + 1) as usize
        };

        let mut stored: Vec<(i64, Vec<u8>)> = Vec::new();
        for segment in &part.segments {
            let data = std::fs::read(storage.join(&segment.key))
                .unwrap_or_else(|e| panic!("listed segment {} unreadable: {e}", segment.key));
            let mut reader = SegmentReader::open(Bytes::from(data)).expect("segment opens");
            for record in reader.read_all().expect("segment decodes") {
                stored.push((record.offset, record.value.unwrap_or_default().to_vec()));
            }
        }
        stored.sort_by_key(|(offset, _)| *offset);

        let offsets: Vec<i64> = stored.iter().map(|(o, _)| *o).collect();
        let mut unique = offsets.clone();
        unique.dedup();
        assert_eq!(
            unique.len(),
            offsets.len(),
            "partition {partition}: {} duplicate record(s) in listed segments",
            offsets.len() - unique.len()
        );
        let want: Vec<i64> = (0..n as i64).collect();
        if unique != want {
            let missing: Vec<i64> = want
                .iter()
                .filter(|o| !unique.contains(o))
                .copied()
                .collect();
            panic!(
                "partition {partition}: {} record(s) missing (first few: {:?}); stored {} of {}",
                missing.len(),
                &missing[..missing.len().min(10)],
                unique.len(),
                want.len()
            );
        }
        for (offset, value) in &stored {
            assert_eq!(
                value, &expected[*offset as usize],
                "partition {partition} offset {offset}: value differs from what was produced"
            );
        }
        reaches.push(reach);
    }
    reaches
}

async fn create_topic(cluster: &KafkaTestCluster, topic: &str) {
    let client = cluster.create_client();
    client.connect().await.expect("connect");
    let results = client
        .create_topics(
            vec![TopicToCreate {
                name: topic.to_string(),
                num_partitions: PARTITIONS,
                replication_factor: 1,
            }],
            30_000,
        )
        .await
        .expect("create topic");
    assert!(results.iter().all(|r| r.is_success_or_exists()));
    tokio::time::sleep(Duration::from_secs(1)).await;
}

/// Start Kafka, run `body`, and always drop the container inside a runtime
/// (testcontainers' async drop needs one) before re-raising a failure.
fn with_cluster(body: impl FnOnce(&tokio::runtime::Runtime, &KafkaTestCluster)) {
    // Serialises failpoint use process-wide and clears it on teardown.
    let scenario = fail::FailScenario::setup();
    let setup = tokio::runtime::Runtime::new().unwrap();
    let cluster = setup.block_on(async {
        let cluster = KafkaTestCluster::start().await.expect("start Kafka");
        cluster
            .wait_for_ready(Duration::from_secs(60))
            .await
            .expect("Kafka ready");
        cluster
    });
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| body(&setup, &cluster)));
    setup.block_on(async move { drop(cluster) });
    scenario.teardown();
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
}

fn run_seed(setup: &tokio::runtime::Runtime, cluster: &KafkaTestCluster, seed: u64) {
    let mut rng = Rng(seed);
    let topic = format!("chaos-{seed:x}");
    let backup_id = format!("chaos-{seed:x}");
    let plan = Plan {
        snapshot: rng.range(0, 1) == 1,
        segment_records: rng.range(15, 80),
        sync_secs: rng.range(0, 1),
        fresh_local_db: rng.range(0, 1) == 1,
    };
    let crashes = rng.range(2, 4) as usize;
    println!("seed {seed:#x}: {plan:?}, {crashes} faulty iterations");

    setup.block_on(create_topic(cluster, &topic));

    let storage = TempDir::new().unwrap();
    let local = TempDir::new().unwrap();
    let mut produced: Vec<Vec<Vec<u8>>> = vec![Vec::new(); PARTITIONS as usize];
    let db_path = |i: usize| {
        if plan.fresh_local_db {
            local.path().join(format!("offsets-{i}.db"))
        } else {
            local.path().join("offsets.db")
        }
    };

    for i in 0..crashes {
        let more = rng.range(60, 300) as usize;
        setup.block_on(produce(cluster, &topic, more, &mut produced));
        let fault = Fault::choose(&mut rng);
        let cfg = config(
            &cluster.bootstrap_servers,
            storage.path(),
            db_path(i),
            &backup_id,
            &topic,
            &plan,
        );
        let outcome = run_iteration(cfg, &fault);
        println!("  iteration {i}: {fault:?} -> {outcome:?}");
        if let (Fault::Graceful { .. } | Fault::None, Outcome::Finished(result)) =
            (&fault, &outcome)
        {
            assert!(
                result.is_ok(),
                "seed {seed:#x} iteration {i}: a run without injected errors failed: {result:?}"
            );
        }
        assert_invariants(
            storage.path(),
            &backup_id,
            &format!("seed {seed:#x} after iteration {i} ({fault:?})"),
        );
    }

    // Final clean run: must succeed and leave a complete, duplicate-free set.
    let more = rng.range(10, 100) as usize;
    setup.block_on(produce(cluster, &topic, more, &mut produced));
    let cfg = config(
        &cluster.bootstrap_servers,
        storage.path(),
        db_path(crashes),
        &backup_id,
        &topic,
        &plan,
    );
    match run_iteration(cfg, &Fault::None) {
        Outcome::Finished(Ok(())) => {}
        other => panic!("seed {seed:#x}: final clean run failed: {other:?}"),
    }
    assert_invariants(storage.path(), &backup_id, &format!("seed {seed:#x} final"));
    assert_stored(storage.path(), &backup_id, &topic, &produced, true);
    let total: usize = produced.iter().map(Vec::len).sum();
    println!("seed {seed:#x}: OK ({total} records, 0 missing, 0 duplicates)");
}

#[test]
#[ignore = "requires Docker"]
fn backup_survives_random_crashes_without_loss_or_duplicates() {
    let seeds: u64 = std::env::var("KAFKA_BACKUP_CHAOS_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let base: u64 = std::env::var("KAFKA_BACKUP_CHAOS_SEED_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0x5EED);

    with_cluster(|setup, cluster| {
        for seed in base..base + seeds {
            run_seed(setup, cluster, seed);
        }
    });
}

/// SIGTERM mid-run: every partition stops fetching, writes what it has
/// buffered and records it before the process exits, and the stored
/// checkpoint is exactly the manifest reach. (Before the fix, shutdown did
/// not wait for partition tasks and synced offsets.db before the manifest.)
#[test]
#[ignore = "requires Docker"]
fn graceful_shutdown_flushes_every_partition_before_exiting() {
    with_cluster(|setup, cluster| {
        let topic = "chaos-graceful";
        let backup_id = "chaos-graceful";
        setup.block_on(create_topic(cluster, topic));
        let mut produced: Vec<Vec<Vec<u8>>> = vec![Vec::new(); PARTITIONS as usize];
        setup.block_on(produce(cluster, topic, 3_000, &mut produced));

        let storage = TempDir::new().unwrap();
        let local = TempDir::new().unwrap();
        // Segments big enough that nothing rotates before the signal, and no
        // progress saves: whatever is stored was written by the shutdown.
        let plan = Plan {
            snapshot: true,
            segment_records: 100_000,
            sync_secs: 3_600,
            fresh_local_db: false,
        };
        let cfg = |name: &str| {
            config(
                &cluster.bootstrap_servers,
                storage.path(),
                local.path().join(name),
                backup_id,
                topic,
                &plan,
            )
        };

        // ~100 records per fetch, 100ms each: 3,000 per partition take ~3s.
        fail::cfg("backup::fetch::after", "sleep(100)").unwrap();
        let outcome = run_iteration(
            cfg("offsets.db"),
            &Fault::Graceful {
                after: Duration::from_millis(1_200),
            },
        );
        assert!(
            matches!(outcome, Outcome::Finished(Ok(()))),
            "graceful shutdown must exit cleanly: {outcome:?}"
        );

        assert_invariants(storage.path(), backup_id, "after graceful shutdown");
        let reaches = assert_stored(storage.path(), backup_id, topic, &produced, false);
        let checkpoints = stored_checkpoints(storage.path(), backup_id);
        for (partition, reach) in reaches.iter().enumerate() {
            assert!(
                *reach >= 0,
                "partition {partition}: buffered records were not flushed at shutdown"
            );
            assert_eq!(
                checkpoints.get(&(topic.to_string(), partition as i32)),
                Some(reach),
                "partition {partition}: checkpoint after shutdown is the manifest reach"
            );
        }
        assert!(
            reaches.iter().any(|r| *r < 2_999),
            "the signal must arrive mid-run for this test to mean anything: {reaches:?}"
        );

        // The next run carries on from there.
        let outcome = run_iteration(cfg("offsets.db"), &Fault::None);
        assert!(matches!(outcome, Outcome::Finished(Ok(()))), "{outcome:?}");
        assert_stored(storage.path(), backup_id, topic, &produced, true);
    });
}
