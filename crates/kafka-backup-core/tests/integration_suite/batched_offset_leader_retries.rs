//! Batched offset reads must recover after a new topic's leader is not ready.
use super::produce_mock_broker::MockBroker;
use kafka_backup_core::config::{IsolationLevel, KafkaConfig};
use kafka_backup_core::kafka::PartitionLeaderRouter;
use serde_json::json;

async fn router(broker: &MockBroker) -> PartitionLeaderRouter {
    let config: KafkaConfig =
        serde_json::from_value(json!({"bootstrap_servers":[broker.bootstrap()]})).unwrap();
    PartitionLeaderRouter::new(config).await.unwrap()
}

#[tokio::test]
async fn latest_offsets_refresh_metadata_after_leader_errors() {
    let broker = MockBroker::start("offsets", 2).await;
    broker.append_foreign(0, &[b"one", b"two"]);
    broker.append_foreign(1, &[b"one"]);
    broker
        .state
        .lock()
        .unwrap()
        .list_offset_errors
        .extend([5, 6]);
    let router = router(&broker).await;
    let before_metadata = broker.state.lock().unwrap().metadata_requests;
    let partitions = vec![("offsets".into(), 0), ("offsets".into(), 1)];
    let offsets = router
        .batch_get_latest_offsets(&partitions, IsolationLevel::ReadUncommitted)
        .await
        .unwrap();
    assert_eq!(offsets[&("offsets".into(), 0)], 2);
    assert_eq!(offsets[&("offsets".into(), 1)], 1);
    let state = broker.state.lock().unwrap();
    assert_eq!(state.list_offset_requests, 3);
    assert!(
        state.metadata_requests >= before_metadata + 2,
        "leader errors must refresh routing metadata"
    );
}

#[tokio::test]
async fn earliest_and_latest_offsets_retry_the_complete_read() {
    let broker = MockBroker::start("offsets", 1).await;
    broker.append_foreign(0, &[b"one", b"two"]);
    // Earliest succeeds, latest reports a stale leader. Both must be read
    // again before returning a consistent complete result.
    broker
        .state
        .lock()
        .unwrap()
        .list_offset_errors
        .extend([0, 6]);
    let router = router(&broker).await;
    let offsets = router
        .batch_get_all_offsets(&[("offsets".into(), 0)], IsolationLevel::ReadCommitted)
        .await
        .unwrap();
    assert_eq!(offsets[&("offsets".into(), 0)], (0, 2));
    let state = broker.state.lock().unwrap();
    assert_eq!(state.list_offset_requests, 4);
    assert!(state.metadata_requests >= 2);
}

#[tokio::test]
async fn authorization_error_is_returned_without_retrying() {
    let broker = MockBroker::start("offsets", 1).await;
    broker
        .state
        .lock()
        .unwrap()
        .list_offset_errors
        .push_back(29);
    let router = router(&broker).await;
    let result = router
        .batch_get_latest_offsets(&[("offsets".into(), 0)], IsolationLevel::ReadUncommitted)
        .await;
    assert!(result.is_err());
    assert_eq!(broker.state.lock().unwrap().list_offset_requests, 1);
}
