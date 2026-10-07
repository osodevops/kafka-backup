//! Issue #224 — group-scoped requests (OffsetFetch, OffsetCommit) must reach
//! the group's coordinator.
//!
//! A broker that does not coordinate a group answers OffsetFetch with a
//! top-level NOT_COORDINATOR and no topics. `fetch_offsets` used to ignore
//! that code and report "no committed offsets", so `snapshot-groups`,
//! offset-rollback and the consumer-group validation check silently dropped
//! every group coordinated by a non-bootstrap broker.
//!
//! In-process mock cluster, no Docker.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use kafka_backup_core::config::{ConnectionConfig, KafkaConfig, SecurityConfig, TopicSelection};
use kafka_backup_core::error::KafkaError;
use kafka_backup_core::kafka::consumer_groups::{commit_offsets, fetch_offsets};
use kafka_backup_core::kafka::{KafkaClient, PartitionLeaderRouter};
use kafka_backup_core::manifest::BackupManifest;
use kafka_backup_core::storage::{create_backend, StorageBackendConfig};
use kafka_backup_core::validation::consumer_group::ConsumerGroupOffsetCheck;
use kafka_backup_core::validation::{
    CheckOutcome, ConsumerGroupConfig, ValidationCheck, ValidationContext,
};
use kafka_backup_core::{rollback_offset_reset, snapshot_current_offsets, verify_rollback};

use super::group_coordinator_mock::{
    MockCluster, COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE,
    GROUP_AUTHORIZATION_FAILED, TOPIC,
};

fn config(bootstrap: &str) -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec![bootstrap.to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection::default(),
        connection: ConnectionConfig {
            connections_per_broker: 1,
            ..Default::default()
        },
    }
}

async fn client(cluster: &MockCluster, broker_id: i32) -> KafkaClient {
    let client = KafkaClient::new(config(&cluster.addr(broker_id)));
    client.connect().await.expect("connect to mock broker");
    client
}

fn as_map(
    offsets: Vec<kafka_backup_core::kafka::consumer_groups::CommittedOffset>,
) -> BTreeMap<(String, i32), i64> {
    offsets
        .into_iter()
        .map(|o| ((o.topic, o.partition), o.offset))
        .collect()
}

fn expected(offsets: &[(&str, i32, i64)]) -> BTreeMap<(String, i32), i64> {
    offsets
        .iter()
        .map(|(t, p, o)| ((t.to_string(), *p), *o))
        .collect()
}

fn broker_error_code(err: &kafka_backup_core::Error) -> Option<i16> {
    match err {
        kafka_backup_core::Error::Kafka(KafkaError::BrokerError { code, .. }) => Some(*code),
        _ => None,
    }
}

#[tokio::test]
async fn fetch_offsets_reads_group_from_its_coordinator_not_the_bootstrap() {
    let cluster = MockCluster::start(3).await;
    let offsets = [(TOPIC, 0, 42), (TOPIC, 1, 7)];
    cluster.add_group("on-broker-2", 2, &offsets);
    let bootstrap = client(&cluster, 1).await;

    let fetched = fetch_offsets(&bootstrap, "on-broker-2", None)
        .await
        .expect("fetch through bootstrap");

    assert_eq!(as_map(fetched), expected(&offsets));
    assert_eq!(cluster.fetches_for("on-broker-2").last(), Some(&2));
}

#[tokio::test]
async fn fetch_offsets_filters_topics_on_the_coordinator() {
    let cluster = MockCluster::start(3).await;
    cluster.add_group("g", 3, &[(TOPIC, 0, 5), ("other", 0, 9)]);
    let bootstrap = client(&cluster, 1).await;

    let fetched = fetch_offsets(&bootstrap, "g", Some(&[TOPIC.to_string()]))
        .await
        .unwrap();

    assert_eq!(as_map(fetched), expected(&[(TOPIC, 0, 5)]));
}

#[tokio::test]
async fn fetch_offsets_on_the_coordinator_itself_sends_one_request() {
    let cluster = MockCluster::start(3).await;
    cluster.add_group("local", 1, &[(TOPIC, 0, 1)]);
    let bootstrap = client(&cluster, 1).await;

    fetch_offsets(&bootstrap, "local", None).await.unwrap();

    assert_eq!(cluster.fetches_for("local"), vec![1]);
}

#[tokio::test]
async fn fetch_offsets_reports_group_errors_instead_of_no_offsets() {
    let cluster = MockCluster::start(3).await;
    cluster.add_group("denied", 2, &[(TOPIC, 0, 1)]);
    cluster.fail_fetches("denied", &[GROUP_AUTHORIZATION_FAILED]);
    let bootstrap = client(&cluster, 1).await;

    let err = fetch_offsets(&bootstrap, "denied", None)
        .await
        .expect_err("GROUP_AUTHORIZATION_FAILED must not read as an empty group");

    assert_eq!(broker_error_code(&err), Some(GROUP_AUTHORIZATION_FAILED));
}

#[tokio::test]
async fn fetch_offsets_retries_a_loading_coordinator() {
    let cluster = MockCluster::start(3).await;
    cluster.add_group("loading", 2, &[(TOPIC, 0, 11)]);
    cluster.fail_fetches(
        "loading",
        &[COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_LOAD_IN_PROGRESS],
    );
    let bootstrap = client(&cluster, 1).await;

    let fetched = fetch_offsets(&bootstrap, "loading", None).await.unwrap();

    assert_eq!(as_map(fetched), expected(&[(TOPIC, 0, 11)]));
    assert_eq!(cluster.fetches_for("loading"), vec![1, 2, 2, 2]);
}

#[tokio::test]
async fn fetch_offsets_follows_a_coordinator_that_moved() {
    let cluster = MockCluster::start(3).await;
    cluster.add_group("moved", 3, &[(TOPIC, 2, 99)]);
    // FindCoordinator first hands out broker 2, which no longer owns the group.
    cluster.stale_coordinator("moved", &[2]);
    let bootstrap = client(&cluster, 1).await;

    let fetched = fetch_offsets(&bootstrap, "moved", None).await.unwrap();

    assert_eq!(as_map(fetched), expected(&[(TOPIC, 2, 99)]));
    assert_eq!(cluster.fetches_for("moved"), vec![1, 2, 3]);
}

#[tokio::test]
async fn fetch_offsets_retries_find_coordinator_while_unavailable() {
    let cluster = MockCluster::start(3).await;
    cluster.add_group("electing", 2, &[(TOPIC, 0, 3)]);
    cluster.fail_find_coordinator("electing", &[COORDINATOR_NOT_AVAILABLE]);
    let bootstrap = client(&cluster, 1).await;

    let fetched = fetch_offsets(&bootstrap, "electing", None).await.unwrap();

    assert_eq!(as_map(fetched), expected(&[(TOPIC, 0, 3)]));
}

#[tokio::test]
async fn fetch_offsets_for_unknown_group_is_empty() {
    let cluster = MockCluster::start(3).await;
    cluster.state.lock().unwrap().default_coordinator = 2;
    let bootstrap = client(&cluster, 1).await;

    let fetched = fetch_offsets(&bootstrap, "never-committed", None)
        .await
        .unwrap();

    assert!(fetched.is_empty());
}

#[tokio::test]
async fn fetch_offsets_gives_up_on_a_coordinator_that_never_loads() {
    let cluster = MockCluster::start(2).await;
    cluster.add_group("stuck", 2, &[(TOPIC, 0, 1)]);
    cluster.fail_fetches("stuck", &[COORDINATOR_LOAD_IN_PROGRESS; 64]);
    let bootstrap = client(&cluster, 1).await;

    let started = Instant::now();
    let err = fetch_offsets(&bootstrap, "stuck", None)
        .await
        .expect_err("a coordinator that never loads must fail, not hang or read as empty");

    assert_eq!(broker_error_code(&err), Some(COORDINATOR_LOAD_IN_PROGRESS));
    assert!(started.elapsed() < Duration::from_secs(30));
}

#[tokio::test]
async fn fetch_offsets_reuses_one_connection_per_coordinator() {
    let cluster = MockCluster::start(3).await;
    for i in 0..5 {
        cluster.add_group(&format!("g{i}"), 2, &[(TOPIC, 0, i)]);
    }
    let bootstrap = client(&cluster, 1).await;

    for i in 0..5 {
        fetch_offsets(&bootstrap, &format!("g{i}"), None)
            .await
            .unwrap();
    }

    assert_eq!(cluster.connections(2), 1);
}

#[tokio::test]
async fn commit_offsets_commits_on_the_group_coordinator() {
    let cluster = MockCluster::start(3).await;
    cluster.add_group("commit-me", 3, &[(TOPIC, 0, 1)]);
    let bootstrap = client(&cluster, 1).await;

    let results = commit_offsets(
        &bootstrap,
        "commit-me",
        &[
            (TOPIC.to_string(), 0, 100, None),
            (TOPIC.to_string(), 1, 200, None),
        ],
    )
    .await
    .unwrap();

    assert!(
        results.iter().all(|(_, _, code)| *code == 0),
        "commit results: {results:?}"
    );
    assert_eq!(
        cluster.committed("commit-me"),
        expected(&[(TOPIC, 0, 100), (TOPIC, 1, 200)])
    );
    assert_eq!(cluster.commits_for("commit-me").last(), Some(&3));
}

/// Offset rollback: snapshot, verify and rollback all go through one bootstrap
/// `KafkaClient` (the CLI and the operator both call them that way).
#[tokio::test]
async fn offset_rollback_round_trip_covers_groups_on_every_coordinator() {
    let cluster = MockCluster::start(3).await;
    let groups = ["g-b1", "g-b2", "g-b3"];
    for (i, group) in groups.iter().enumerate() {
        cluster.add_group(group, i as i32 + 1, &[(TOPIC, 0, 10 * (i as i64 + 1))]);
    }
    let bootstrap = client(&cluster, 1).await;
    let group_ids: Vec<String> = groups.iter().map(|g| g.to_string()).collect();

    let snapshot = snapshot_current_offsets(&bootstrap, &group_ids, vec![cluster.addr(1)])
        .await
        .unwrap();
    for (i, group) in groups.iter().enumerate() {
        let state = &snapshot.group_offsets[*group];
        assert_eq!(
            state.offsets[TOPIC][&0].offset,
            10 * (i as i64 + 1),
            "snapshot for {group}"
        );
    }

    // Offsets move on every coordinator; verification must notice all three.
    for group in groups {
        cluster.set_offset(group, TOPIC, 0, 500);
    }
    let verification = verify_rollback(&bootstrap, &snapshot).await.unwrap();
    assert!(!verification.verified);
    let mut mismatched = verification.groups_mismatched.clone();
    mismatched.sort();
    assert_eq!(mismatched, group_ids);

    let rollback = rollback_offset_reset(&bootstrap, &snapshot).await.unwrap();
    assert!(rollback.errors.is_empty(), "{:?}", rollback.errors);
    for (i, group) in groups.iter().enumerate() {
        assert_eq!(
            cluster.committed(group),
            expected(&[(TOPIC, 0, 10 * (i as i64 + 1))])
        );
    }
    assert!(
        verify_rollback(&bootstrap, &snapshot)
            .await
            .unwrap()
            .verified
    );
}

/// A rollback snapshot that cannot read a group must not record it as having
/// no offsets — rolling back to that snapshot would then restore nothing.
#[tokio::test]
async fn snapshot_current_offsets_fails_when_a_group_cannot_be_read() {
    let cluster = MockCluster::start(3).await;
    cluster.add_group("ok", 1, &[(TOPIC, 0, 1)]);
    cluster.add_group("denied", 3, &[(TOPIC, 0, 1)]);
    cluster.fail_fetches("denied", &[GROUP_AUTHORIZATION_FAILED]);
    let bootstrap = client(&cluster, 1).await;

    let result = snapshot_current_offsets(
        &bootstrap,
        &["ok".to_string(), "denied".to_string()],
        vec![cluster.addr(1)],
    )
    .await;

    let err = result.expect_err("snapshot must fail rather than record `denied` as empty");
    assert!(err.to_string().contains("denied"), "{err}");
}

#[tokio::test]
async fn consumer_group_validation_checks_groups_on_every_broker() {
    let cluster = MockCluster::start(3).await;
    cluster.add_group("v1", 1, &[(TOPIC, 0, 1)]);
    cluster.add_group("v2", 2, &[(TOPIC, 0, 2), (TOPIC, 1, 2)]);
    cluster.add_group("v3", 3, &[(TOPIC, 2, 3)]);
    let router = PartitionLeaderRouter::new(config(&cluster.addr(1)))
        .await
        .unwrap();
    let ctx = ValidationContext {
        backup_id: "issue-224".to_string(),
        backup_manifest: BackupManifest::new("issue-224".to_string()),
        target_client: Arc::new(router),
        storage: create_backend(&StorageBackendConfig::Memory).unwrap(),
        pitr_timestamp: None,
        http_client: reqwest::Client::new(),
        target_bootstrap_servers: vec![cluster.addr(1)],
    };

    let result = ConsumerGroupOffsetCheck::new(ConsumerGroupConfig::default())
        .run(&ctx)
        .await
        .unwrap();

    assert_eq!(result.outcome, CheckOutcome::Passed, "{}", result.detail);
    assert_eq!(result.data["groups_checked"], 3, "{}", result.data);
    assert_eq!(result.data["total_offsets"], 4, "{}", result.data);
}
