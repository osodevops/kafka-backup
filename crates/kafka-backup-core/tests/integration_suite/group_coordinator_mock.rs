//! In-process multi-broker cluster for consumer-group coordinator tests
//! (issue #224). No Docker.
//!
//! Each group lives on one coordinator broker. Every other broker answers
//! group-scoped requests the way real Kafka does: `OffsetFetch` with a
//! top-level `NOT_COORDINATOR` and no topics, `OffsetCommit` with
//! `NOT_COORDINATOR` on every partition, and `ListGroups` lists only the
//! groups that broker coordinates.
//!
//! Self-contained (no `super::` imports) so the CLI tests can include it
//! with `#[path]`.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::find_coordinator_response::FindCoordinatorResponse;
use kafka_protocol::messages::list_groups_response::{ListGroupsResponse, ListedGroup};
use kafka_protocol::messages::metadata_response::{
    MetadataResponse, MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponse, OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponse, OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{
    ApiKey, BrokerId, FindCoordinatorRequest, GroupId, OffsetCommitRequest, OffsetFetchRequest,
    RequestHeader, ResponseHeader, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
pub const COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const NOT_COORDINATOR: i16 = 16;
pub const GROUP_AUTHORIZATION_FAILED: i16 = 30;

/// Topic every mock broker reports in Metadata.
pub const TOPIC: &str = "orders";

#[derive(Default)]
pub struct ClusterState {
    pub brokers: Vec<(i32, SocketAddr)>,
    /// group -> coordinating broker id
    pub coordinator: HashMap<String, i32>,
    /// group -> (topic, partition) -> committed offset
    pub offsets: HashMap<String, BTreeMap<(String, i32), i64>>,
    /// Errors the coordinator returns for the group's next OffsetFetch requests.
    pub fetch_errors: HashMap<String, VecDeque<i16>>,
    /// Errors FindCoordinator returns for the group before answering.
    pub find_errors: HashMap<String, VecDeque<i16>>,
    /// Stale coordinator ids FindCoordinator hands out before the real one,
    /// modelling a coordinator that moved between lookup and request.
    pub stale_coordinator: HashMap<String, VecDeque<i32>>,
    /// Coordinator FindCoordinator reports for groups that do not exist.
    pub default_coordinator: i32,
    /// (broker id, group) for every OffsetFetch received.
    pub offset_fetches: Vec<(i32, String)>,
    /// (broker id, group) for every OffsetCommit received.
    pub offset_commits: Vec<(i32, String)>,
}

pub struct MockCluster {
    pub state: Arc<Mutex<ClusterState>>,
    connections: HashMap<i32, Arc<AtomicUsize>>,
    handles: Vec<JoinHandle<()>>,
}

impl MockCluster {
    /// Start `broker_count` brokers with ids `1..=broker_count`.
    pub async fn start(broker_count: i32) -> Self {
        let state = Arc::new(Mutex::new(ClusterState {
            default_coordinator: 1,
            ..Default::default()
        }));
        let mut connections = HashMap::new();
        let mut handles = Vec::new();

        for broker_id in 1..=broker_count {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local addr");
            state.lock().unwrap().brokers.push((broker_id, addr));

            let accepted = Arc::new(AtomicUsize::new(0));
            connections.insert(broker_id, accepted.clone());
            let state = state.clone();
            handles.push(tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    accepted.fetch_add(1, Ordering::SeqCst);
                    tokio::spawn(serve(stream, broker_id, state.clone()));
                }
            }));
        }

        Self {
            state,
            connections,
            handles,
        }
    }

    pub fn addr(&self, broker_id: i32) -> String {
        let state = self.state.lock().unwrap();
        let (_, addr) = state
            .brokers
            .iter()
            .find(|(id, _)| *id == broker_id)
            .expect("known broker");
        addr.to_string()
    }

    /// Register a group coordinated by `coordinator` with committed offsets
    /// on [`TOPIC`] (or any topic) as `(topic, partition, offset)`.
    pub fn add_group(&self, group: &str, coordinator: i32, offsets: &[(&str, i32, i64)]) {
        let mut state = self.state.lock().unwrap();
        state.coordinator.insert(group.to_string(), coordinator);
        state.offsets.insert(
            group.to_string(),
            offsets
                .iter()
                .map(|(t, p, o)| ((t.to_string(), *p), *o))
                .collect(),
        );
    }

    pub fn fail_fetches(&self, group: &str, codes: &[i16]) {
        self.state
            .lock()
            .unwrap()
            .fetch_errors
            .insert(group.to_string(), codes.iter().copied().collect());
    }

    pub fn fail_find_coordinator(&self, group: &str, codes: &[i16]) {
        self.state
            .lock()
            .unwrap()
            .find_errors
            .insert(group.to_string(), codes.iter().copied().collect());
    }

    pub fn stale_coordinator(&self, group: &str, brokers: &[i32]) {
        self.state
            .lock()
            .unwrap()
            .stale_coordinator
            .insert(group.to_string(), brokers.iter().copied().collect());
    }

    pub fn set_offset(&self, group: &str, topic: &str, partition: i32, offset: i64) {
        self.state
            .lock()
            .unwrap()
            .offsets
            .entry(group.to_string())
            .or_default()
            .insert((topic.to_string(), partition), offset);
    }

    pub fn committed(&self, group: &str) -> BTreeMap<(String, i32), i64> {
        self.state
            .lock()
            .unwrap()
            .offsets
            .get(group)
            .cloned()
            .unwrap_or_default()
    }

    /// Brokers that received an OffsetFetch for `group`, in order.
    pub fn fetches_for(&self, group: &str) -> Vec<i32> {
        self.state
            .lock()
            .unwrap()
            .offset_fetches
            .iter()
            .filter(|(_, g)| g == group)
            .map(|(b, _)| *b)
            .collect()
    }

    /// Brokers that received an OffsetCommit for `group`, in order.
    pub fn commits_for(&self, group: &str) -> Vec<i32> {
        self.state
            .lock()
            .unwrap()
            .offset_commits
            .iter()
            .filter(|(_, g)| g == group)
            .map(|(b, _)| *b)
            .collect()
    }

    /// TCP connections a broker has accepted.
    pub fn connections(&self, broker_id: i32) -> usize {
        self.connections[&broker_id].load(Ordering::SeqCst)
    }
}

impl Drop for MockCluster {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

async fn serve(mut stream: TcpStream, broker_id: i32, state: Arc<Mutex<ClusterState>>) {
    while let Some((api_key, version, correlation_id, mut body)) = read_request(&mut stream).await {
        let mut out = BytesMut::new();
        {
            let mut state = state.lock().unwrap();
            match api_key {
                ApiKey::Metadata => metadata(&state).encode(&mut out, version),
                ApiKey::ListGroups => list_groups(&state, broker_id).encode(&mut out, version),
                ApiKey::FindCoordinator => {
                    let req = FindCoordinatorRequest::decode(&mut body, version).unwrap();
                    find_coordinator(&mut state, req.key.as_str()).encode(&mut out, version)
                }
                ApiKey::OffsetFetch => {
                    let req = OffsetFetchRequest::decode(&mut body, version).unwrap();
                    offset_fetch(&mut state, broker_id, &req).encode(&mut out, version)
                }
                ApiKey::OffsetCommit => {
                    let req = OffsetCommitRequest::decode(&mut body, version).unwrap();
                    offset_commit(&mut state, broker_id, &req).encode(&mut out, version)
                }
                other => panic!("group-coordinator mock broker {broker_id}: unexpected {other:?}"),
            }
            .expect("encode mock response");
        }
        write_response(&mut stream, api_key, version, correlation_id, &out).await;
    }
}

fn list_groups(state: &ClusterState, broker_id: i32) -> ListGroupsResponse {
    let mut groups: Vec<_> = state
        .coordinator
        .iter()
        .filter(|(_, b)| **b == broker_id)
        .map(|(g, _)| g.clone())
        .collect();
    groups.sort();
    ListGroupsResponse::default().with_groups(
        groups
            .into_iter()
            .map(|g| {
                ListedGroup::default()
                    .with_group_id(GroupId(StrBytes::from_string(g)))
                    .with_protocol_type(StrBytes::from_static_str("consumer"))
            })
            .collect(),
    )
}

fn metadata(state: &ClusterState) -> MetadataResponse {
    let brokers = state
        .brokers
        .iter()
        .map(|(id, addr)| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(*id))
                .with_host(StrBytes::from_string(addr.ip().to_string()))
                .with_port(i32::from(addr.port()))
        })
        .collect();
    let partitions = state
        .brokers
        .iter()
        .enumerate()
        .map(|(i, (id, _))| {
            MetadataResponsePartition::default()
                .with_partition_index(i as i32)
                .with_leader_id(BrokerId(*id))
                .with_replica_nodes(vec![BrokerId(*id)])
                .with_isr_nodes(vec![BrokerId(*id)])
        })
        .collect();
    MetadataResponse::default()
        .with_brokers(brokers)
        .with_controller_id(BrokerId(state.brokers[0].0))
        .with_topics(vec![MetadataResponseTopic::default()
            .with_name(Some(TopicName(StrBytes::from_static_str(TOPIC))))
            .with_partitions(partitions)])
}

fn find_coordinator(state: &mut ClusterState, group: &str) -> FindCoordinatorResponse {
    if let Some(code) = state.find_errors.get_mut(group).and_then(|q| q.pop_front()) {
        return FindCoordinatorResponse::default()
            .with_error_code(code)
            .with_node_id(BrokerId(-1))
            .with_port(-1);
    }
    let node = state
        .stale_coordinator
        .get_mut(group)
        .and_then(|q| q.pop_front())
        .or_else(|| state.coordinator.get(group).copied())
        .unwrap_or(state.default_coordinator);
    let (_, addr) = state
        .brokers
        .iter()
        .find(|(id, _)| *id == node)
        .expect("coordinator is a known broker");
    FindCoordinatorResponse::default()
        .with_node_id(BrokerId(node))
        .with_host(StrBytes::from_string(addr.ip().to_string()))
        .with_port(i32::from(addr.port()))
}

fn coordinator_of(state: &ClusterState, group: &str) -> i32 {
    state
        .coordinator
        .get(group)
        .copied()
        .unwrap_or(state.default_coordinator)
}

fn offset_fetch(
    state: &mut ClusterState,
    broker_id: i32,
    req: &OffsetFetchRequest,
) -> OffsetFetchResponse {
    let group = req.group_id.as_str().to_string();
    state.offset_fetches.push((broker_id, group.clone()));

    if coordinator_of(state, &group) != broker_id {
        return OffsetFetchResponse::default().with_error_code(NOT_COORDINATOR);
    }
    if let Some(code) = state
        .fetch_errors
        .get_mut(&group)
        .and_then(|q| q.pop_front())
    {
        return OffsetFetchResponse::default().with_error_code(code);
    }

    let wanted: Option<Vec<String>> = req
        .topics
        .as_ref()
        .map(|ts| ts.iter().map(|t| t.name.as_str().to_string()).collect());
    let mut by_topic: BTreeMap<String, Vec<OffsetFetchResponsePartition>> = BTreeMap::new();
    for ((topic, partition), offset) in state.offsets.get(&group).into_iter().flatten() {
        if wanted.as_ref().is_some_and(|w| !w.contains(topic)) {
            continue;
        }
        by_topic.entry(topic.clone()).or_default().push(
            OffsetFetchResponsePartition::default()
                .with_partition_index(*partition)
                .with_committed_offset(*offset)
                .with_metadata(Some(StrBytes::from_static_str(""))),
        );
    }
    OffsetFetchResponse::default().with_topics(
        by_topic
            .into_iter()
            .map(|(topic, partitions)| {
                OffsetFetchResponseTopic::default()
                    .with_name(TopicName(StrBytes::from_string(topic)))
                    .with_partitions(partitions)
            })
            .collect(),
    )
}

fn offset_commit(
    state: &mut ClusterState,
    broker_id: i32,
    req: &OffsetCommitRequest,
) -> OffsetCommitResponse {
    let group = req.group_id.as_str().to_string();
    state.offset_commits.push((broker_id, group.clone()));
    let code = if coordinator_of(state, &group) == broker_id {
        0
    } else {
        NOT_COORDINATOR
    };

    let mut topics = Vec::new();
    for topic in &req.topics {
        let mut partitions = Vec::new();
        for p in &topic.partitions {
            if code == 0 {
                state.offsets.entry(group.clone()).or_default().insert(
                    (topic.name.as_str().to_string(), p.partition_index),
                    p.committed_offset,
                );
            }
            partitions.push(
                OffsetCommitResponsePartition::default()
                    .with_partition_index(p.partition_index)
                    .with_error_code(code),
            );
        }
        topics.push(
            OffsetCommitResponseTopic::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions),
        );
    }
    OffsetCommitResponse::default().with_topics(topics)
}

async fn read_request(stream: &mut TcpStream) -> Option<(ApiKey, i16, i32, Bytes)> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.ok()?;
    let mut frame = vec![0u8; i32::from_be_bytes(len_buf) as usize];
    stream.read_exact(&mut frame).await.ok()?;

    let api_key = ApiKey::try_from(i16::from_be_bytes([frame[0], frame[1]])).ok()?;
    let version = i16::from_be_bytes([frame[2], frame[3]]);
    let mut body = Bytes::from(frame);
    let header = RequestHeader::decode(&mut body, api_key.request_header_version(version)).ok()?;
    Some((api_key, version, header.correlation_id, body))
}

async fn write_response(
    stream: &mut TcpStream,
    api_key: ApiKey,
    version: i16,
    correlation_id: i32,
    body: &[u8],
) {
    let mut buf = BytesMut::new();
    buf.put_i32(0);
    ResponseHeader::default()
        .with_correlation_id(correlation_id)
        .encode(&mut buf, api_key.response_header_version(version))
        .unwrap();
    buf.extend_from_slice(body);
    let len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());
    let _ = stream.write_all(&buf).await;
}
