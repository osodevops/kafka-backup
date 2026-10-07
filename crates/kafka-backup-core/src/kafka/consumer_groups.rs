//! Consumer group operations for offset management.
//!
//! This module implements Kafka protocol operations for consumer group offset management:
//! - ListGroups: List all consumer groups
//! - DescribeGroups: Get consumer group details
//! - OffsetFetch: Get committed offsets for a group
//! - OffsetCommit: Commit offsets for a group
//! - ListOffsetsForTimes: Find offsets by timestamp
//!
//! OffsetFetch and OffsetCommit are routed to the group's coordinator (see
//! [`send_to_group_coordinator`]); the other requests go to the broker the
//! client is connected to.

use kafka_protocol::messages::{
    ApiKey, DescribeGroupsRequest, DescribeGroupsResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, GroupId, ListGroupsRequest, ListGroupsResponse, ListOffsetsRequest,
    ListOffsetsResponse, OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest,
    OffsetFetchResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

use super::KafkaClient;
use crate::error::KafkaError;
use crate::Result;

const COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
const COORDINATOR_NOT_AVAILABLE: i16 = 15;
const NOT_COORDINATOR: i16 = 16;

/// Attempts a group-scoped request gets to reach a ready coordinator.
const MAX_COORDINATOR_ATTEMPTS: u32 = 12;

/// Consumer group metadata
#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// Consumer group ID
    pub group_id: String,
    /// Protocol type (e.g., "consumer")
    pub protocol_type: String,
    /// Group state (e.g., "Stable", "Empty", "Dead")
    pub state: Option<String>,
}

/// Detailed consumer group description
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescription {
    /// Consumer group ID
    pub group_id: String,
    /// Group state
    pub state: String,
    /// Protocol type
    pub protocol_type: String,
    /// Protocol name (e.g., "range", "roundrobin")
    pub protocol: String,
    /// Group members
    pub members: Vec<ConsumerGroupMember>,
    /// Error code (0 = success)
    pub error_code: i16,
}

/// Consumer group member
#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    /// Member ID
    pub member_id: String,
    /// Client ID
    pub client_id: String,
    /// Client host
    pub client_host: String,
    /// Assigned partitions (topic -> partitions)
    pub assignment: HashMap<String, Vec<i32>>,
}

/// Committed offset for a partition
#[derive(Debug, Clone)]
pub struct CommittedOffset {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Committed offset
    pub offset: i64,
    /// Commit metadata
    pub metadata: Option<String>,
    /// Error code (0 = success)
    pub error_code: i16,
}

/// Offset for timestamp lookup result
#[derive(Debug, Clone)]
pub struct TimestampOffset {
    /// Topic name
    pub topic: String,
    /// Partition ID
    pub partition: i32,
    /// Offset at or after the timestamp
    pub offset: i64,
    /// Timestamp of the offset
    pub timestamp: i64,
    /// Error code (0 = success)
    pub error_code: i16,
}

/// Broker that coordinates a consumer group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupCoordinator {
    /// Coordinator broker node ID
    pub node_id: i32,
    /// Coordinator broker host
    pub host: String,
    /// Coordinator broker port
    pub port: i32,
}

/// List all consumer groups on the cluster
pub async fn list_groups(client: &KafkaClient) -> Result<Vec<ConsumerGroup>> {
    let request = ListGroupsRequest::default();

    let response: ListGroupsResponse = client.send_request(ApiKey::ListGroups, request).await?;

    if response.error_code != 0 {
        return Err(KafkaError::BrokerError {
            code: response.error_code,
            message: format!("ListGroups failed with error code {}", response.error_code),
        }
        .into());
    }

    let group_count = response.groups.len();
    let groups = response
        .groups
        .into_iter()
        .map(|g| ConsumerGroup {
            group_id: g.group_id.to_string(),
            protocol_type: g.protocol_type.to_string(),
            // group_state is Option<StrBytes>, convert to Option<String>
            state: if g.group_state.is_empty() {
                None
            } else {
                Some(g.group_state.to_string())
            },
        })
        .collect();

    debug!("Listed {} consumer groups", group_count);
    Ok(groups)
}

/// Describe consumer groups
pub async fn describe_groups(
    client: &KafkaClient,
    group_ids: &[String],
) -> Result<Vec<ConsumerGroupDescription>> {
    let groups: Vec<GroupId> = group_ids
        .iter()
        .map(|id| GroupId(StrBytes::from_string(id.clone())))
        .collect();

    let request = DescribeGroupsRequest::default().with_groups(groups);

    let response: DescribeGroupsResponse =
        client.send_request(ApiKey::DescribeGroups, request).await?;

    let descriptions = response
        .groups
        .into_iter()
        .map(|g| {
            let members = g
                .members
                .into_iter()
                .map(|m| {
                    // Parse member assignment to get topic-partition mapping
                    let assignment = parse_member_assignment(&m.member_assignment);

                    ConsumerGroupMember {
                        member_id: m.member_id.to_string(),
                        client_id: m.client_id.to_string(),
                        client_host: m.client_host.to_string(),
                        assignment,
                    }
                })
                .collect();

            ConsumerGroupDescription {
                group_id: g.group_id.to_string(),
                state: g.group_state.to_string(),
                protocol_type: g.protocol_type.to_string(),
                protocol: g.protocol_data.to_string(),
                members,
                error_code: g.error_code,
            }
        })
        .collect();

    Ok(descriptions)
}

/// Fetch committed offsets for a consumer group from its coordinator.
///
/// Fails on a group-level error (e.g. GROUP_AUTHORIZATION_FAILED, or a
/// coordinator that stays unavailable); an `Ok` with no offsets means the
/// group has none committed. Partition-level errors are returned in
/// [`CommittedOffset::error_code`].
pub async fn fetch_offsets(
    client: &KafkaClient,
    group_id: &str,
    topics: Option<&[String]>,
) -> Result<Vec<CommittedOffset>> {
    let request = if let Some(topic_list) = topics {
        // Fetch offsets for specific topics
        let topics: Vec<_> = topic_list
            .iter()
            .map(|t| {
                kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_string(t.clone())))
                    .with_partition_indexes(vec![]) // Empty = all partitions
            })
            .collect();

        OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
            .with_topics(Some(topics))
    } else {
        // Fetch all offsets for the group
        OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
            .with_topics(None)
    };

    let response: OffsetFetchResponse = send_to_group_coordinator(
        client,
        group_id,
        ApiKey::OffsetFetch,
        request,
        offset_fetch_coordinator_error,
    )
    .await?;

    // A group-level error comes with no topics; reading it as "no committed
    // offsets" silently drops the group (#224).
    if response.error_code != 0 {
        return Err(KafkaError::BrokerError {
            code: response.error_code,
            message: format!(
                "OffsetFetch for group {group_id} failed with error code {}",
                response.error_code
            ),
        }
        .into());
    }

    let mut offsets = Vec::new();

    // response.topics is a Vec, not an Option
    for topic in response.topics {
        for partition in topic.partitions {
            offsets.push(CommittedOffset {
                topic: topic.name.to_string(),
                partition: partition.partition_index,
                offset: partition.committed_offset,
                metadata: partition
                    .metadata
                    .as_ref()
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string()),
                error_code: partition.error_code,
            });
        }
    }

    debug!(
        "Fetched {} committed offsets for group {}",
        offsets.len(),
        group_id
    );
    Ok(offsets)
}

/// Find the broker coordinating a consumer group.
pub async fn find_group_coordinator(
    client: &KafkaClient,
    group_id: &str,
) -> Result<GroupCoordinator> {
    let request = FindCoordinatorRequest::default()
        .with_key(StrBytes::from_string(group_id.to_string()))
        .with_key_type(0);

    let response: FindCoordinatorResponse = client
        .send_request(ApiKey::FindCoordinator, request)
        .await?;

    group_coordinator_from_response(group_id, response)
}

fn group_coordinator_from_response(
    group_id: &str,
    response: FindCoordinatorResponse,
) -> Result<GroupCoordinator> {
    if response.coordinators.is_empty() {
        if response.error_code != 0 {
            return Err(KafkaError::BrokerError {
                code: response.error_code,
                message: response
                    .error_message
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| {
                        format!(
                            "FindCoordinator failed for group {group_id} with error code {}",
                            response.error_code
                        )
                    }),
            }
            .into());
        }
        return Ok(GroupCoordinator {
            node_id: response.node_id.0,
            host: response.host.to_string(),
            port: response.port,
        });
    }

    let mut fallback = None;
    for coordinator in response.coordinators {
        let is_match = coordinator.key.as_str() == group_id;
        if fallback.is_none() {
            fallback = Some(coordinator.clone());
        }
        if !is_match {
            continue;
        }
        if coordinator.error_code != 0 {
            return Err(KafkaError::BrokerError {
                code: coordinator.error_code,
                message: coordinator
                    .error_message
                    .map(|m| m.to_string())
                    .unwrap_or_else(|| {
                        format!(
                            "FindCoordinator failed for group {group_id} with error code {}",
                            coordinator.error_code
                        )
                    }),
            }
            .into());
        }
        return Ok(GroupCoordinator {
            node_id: coordinator.node_id.0,
            host: coordinator.host.to_string(),
            port: coordinator.port,
        });
    }

    let coordinator = fallback.ok_or_else(|| {
        KafkaError::Protocol(format!(
            "FindCoordinator returned no coordinator for group {group_id}"
        ))
    })?;
    if coordinator.error_code != 0 {
        return Err(KafkaError::BrokerError {
            code: coordinator.error_code,
            message: coordinator
                .error_message
                .map(|m| m.to_string())
                .unwrap_or_else(|| {
                    format!(
                        "FindCoordinator failed for group {group_id} with error code {}",
                        coordinator.error_code
                    )
                }),
        }
        .into());
    }
    Ok(GroupCoordinator {
        node_id: coordinator.node_id.0,
        host: coordinator.host.to_string(),
        port: coordinator.port,
    })
}

/// Commit offsets for a consumer group on its coordinator.
///
/// Returns `(topic, partition, error_code)` for every committed partition.
pub async fn commit_offsets(
    client: &KafkaClient,
    group_id: &str,
    offsets: &[(String, i32, i64, Option<String>)], // (topic, partition, offset, metadata)
) -> Result<Vec<(String, i32, i16)>> {
    // (topic, partition, error_code)
    // Group offsets by topic
    let mut topics_map: HashMap<String, Vec<(i32, i64, Option<String>)>> = HashMap::new();
    for (topic, partition, offset, metadata) in offsets {
        topics_map
            .entry(topic.clone())
            .or_default()
            .push((*partition, *offset, metadata.clone()));
    }

    let topics: Vec<_> = topics_map
        .into_iter()
        .map(|(topic, partitions)| {
            let partition_data: Vec<_> = partitions
                .into_iter()
                .map(|(partition, offset, metadata)| {
                    kafka_protocol::messages::offset_commit_request::OffsetCommitRequestPartition::default()
                        .with_partition_index(partition)
                        .with_committed_offset(offset)
                        .with_committed_metadata(metadata.map(StrBytes::from_string))
                })
                .collect();

            kafka_protocol::messages::offset_commit_request::OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_string(topic)))
                .with_partitions(partition_data)
        })
        .collect();

    let request = OffsetCommitRequest::default()
        .with_group_id(GroupId(StrBytes::from_string(group_id.to_string())))
        .with_topics(topics);

    let response: OffsetCommitResponse = send_to_group_coordinator(
        client,
        group_id,
        ApiKey::OffsetCommit,
        request,
        offset_commit_coordinator_error,
    )
    .await?;

    let mut results = Vec::new();
    for topic in response.topics {
        for partition in topic.partitions {
            if partition.error_code != 0 {
                warn!(
                    "Failed to commit offset for {}:{} - error code {}",
                    topic.name.as_str(),
                    partition.partition_index,
                    partition.error_code
                );
            }
            results.push((
                topic.name.to_string(),
                partition.partition_index,
                partition.error_code,
            ));
        }
    }

    debug!("Committed {} offsets for group {}", results.len(), group_id);
    Ok(results)
}

/// Send a group-scoped request (OffsetFetch, OffsetCommit) to the group's
/// coordinator.
///
/// The request goes to `client` first, so nothing extra happens when that
/// broker coordinates the group. Any other broker answers with
/// NOT_COORDINATOR; we then look the coordinator up with FindCoordinator and
/// send the request there over a cached connection, retrying with backoff
/// while the coordinator loads (same coordinator) or moves (fresh lookup).
///
/// `coordinator_error` returns the response's coordinator error code, if
/// any. The last attempt's response is returned as is, for the caller to
/// surface its error codes.
async fn send_to_group_coordinator<Req, Resp>(
    client: &KafkaClient,
    group_id: &str,
    api_key: ApiKey,
    request: Req,
    coordinator_error: fn(&Resp) -> Option<i16>,
) -> Result<Resp>
where
    Req: Encodable + Default + Clone,
    Resp: Decodable + Default,
{
    // `None` while the request is still going to `client` itself.
    let mut coordinator: Option<(i32, Arc<KafkaClient>)> = None;
    let mut relocate = false;

    for attempt in 1..=MAX_COORDINATOR_ATTEMPTS {
        let last_attempt = attempt == MAX_COORDINATOR_ATTEMPTS;

        if relocate {
            match locate_group_coordinator(client, group_id).await {
                Ok(found) => {
                    coordinator = Some(found);
                    relocate = false;
                }
                Err(e) if is_transient_coordinator_error(&e) && !last_attempt => {
                    let backoff = coordinator_backoff(attempt);
                    warn!(
                        "FindCoordinator for group {} failed on attempt {}/{}; retrying after {:?}: {}",
                        group_id, attempt, MAX_COORDINATOR_ATTEMPTS, backoff, e
                    );
                    tokio::time::sleep(backoff).await;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        let target = coordinator.as_ref().map_or(client, |(_, c)| c.as_ref());
        let response: Resp = match target.send_request(api_key, request.clone()).await {
            Ok(response) => response,
            // The coordinator connection failed even after send_request's
            // reconnect: the broker may be gone, so look the group up again.
            Err(e)
                if coordinator.is_some()
                    && super::connection_error::is_connection_error(&e)
                    && !last_attempt =>
            {
                if let Some((node_id, _)) = coordinator.take() {
                    client.forget_coordinator_connection(node_id).await;
                }
                let backoff = coordinator_backoff(attempt);
                warn!(
                    "{:?} to the coordinator of group {} failed on attempt {}/{}; retrying after {:?}: {}",
                    api_key, group_id, attempt, MAX_COORDINATOR_ATTEMPTS, backoff, e
                );
                tokio::time::sleep(backoff).await;
                relocate = true;
                continue;
            }
            Err(e) => return Err(e),
        };

        let code = match coordinator_error(&response) {
            Some(code) if !last_attempt => code,
            _ => return Ok(response),
        };
        if code != COORDINATOR_LOAD_IN_PROGRESS {
            relocate = true;
        }
        if coordinator.is_none() && relocate {
            // The broker we were given is not the coordinator: just route.
            debug!(
                "{:?} for group {} needs its coordinator (error code {})",
                api_key, group_id, code
            );
            continue;
        }
        let backoff = coordinator_backoff(attempt);
        warn!(
            "{:?} for group {} returned coordinator error code {} on attempt {}/{}; retrying after {:?}",
            api_key, group_id, code, attempt, MAX_COORDINATOR_ATTEMPTS, backoff
        );
        tokio::time::sleep(backoff).await;
    }

    unreachable!("the last attempt always returns")
}

/// Find the coordinator of `group_id` and connect to it.
async fn locate_group_coordinator(
    client: &KafkaClient,
    group_id: &str,
) -> Result<(i32, Arc<KafkaClient>)> {
    let coordinator = find_group_coordinator(client, group_id).await?;
    if coordinator.node_id < 0 {
        return Err(KafkaError::BrokerError {
            code: COORDINATOR_NOT_AVAILABLE,
            message: format!("FindCoordinator returned no coordinator for group {group_id}"),
        }
        .into());
    }
    let addr = format!("{}:{}", coordinator.host, coordinator.port);
    let connection = client
        .coordinator_connection(coordinator.node_id, &addr)
        .await?;
    Ok((coordinator.node_id, connection))
}

fn is_coordinator_error_code(code: i16) -> bool {
    matches!(
        code,
        COORDINATOR_LOAD_IN_PROGRESS | COORDINATOR_NOT_AVAILABLE | NOT_COORDINATOR
    )
}

/// Whether `error` is a coordinator error worth retrying.
fn is_transient_coordinator_error(error: &crate::Error) -> bool {
    matches!(
        error,
        crate::Error::Kafka(KafkaError::BrokerError { code, .. }) if is_coordinator_error_code(*code)
    )
}

fn coordinator_backoff(attempt: u32) -> Duration {
    Duration::from_millis((250 * attempt as u64).min(2_000))
}

/// OffsetFetch v2+ reports coordinator errors at the top level; v0/v1 on
/// every partition.
fn offset_fetch_coordinator_error(response: &OffsetFetchResponse) -> Option<i16> {
    std::iter::once(response.error_code)
        .chain(
            response
                .topics
                .iter()
                .flat_map(|t| t.partitions.iter().map(|p| p.error_code)),
        )
        .find(|code| is_coordinator_error_code(*code))
}

/// OffsetCommit reports coordinator errors on every partition.
fn offset_commit_coordinator_error(response: &OffsetCommitResponse) -> Option<i16> {
    response
        .topics
        .iter()
        .flat_map(|t| t.partitions.iter().map(|p| p.error_code))
        .find(|code| is_coordinator_error_code(*code))
}

/// Find offsets by timestamp
pub async fn offsets_for_times(
    client: &KafkaClient,
    requests: &[(String, i32, i64)], // (topic, partition, timestamp)
) -> Result<Vec<TimestampOffset>> {
    // Group by topic
    let mut topics_map: HashMap<String, Vec<(i32, i64)>> = HashMap::new();
    for (topic, partition, timestamp) in requests {
        topics_map
            .entry(topic.clone())
            .or_default()
            .push((*partition, *timestamp));
    }

    let topics: Vec<_> = topics_map
        .into_iter()
        .map(|(topic, partitions)| {
            let partition_data: Vec<_> = partitions
                .into_iter()
                .map(|(partition, timestamp)| {
                    kafka_protocol::messages::list_offsets_request::ListOffsetsPartition::default()
                        .with_partition_index(partition)
                        .with_timestamp(timestamp)
                })
                .collect();

            kafka_protocol::messages::list_offsets_request::ListOffsetsTopic::default()
                .with_name(TopicName(StrBytes::from_string(topic)))
                .with_partitions(partition_data)
        })
        .collect();

    let request = ListOffsetsRequest::default()
        .with_replica_id(kafka_protocol::messages::BrokerId(-1)) // Consumer
        .with_isolation_level(0) // Read uncommitted
        .with_topics(topics);

    let response: ListOffsetsResponse = client.send_request(ApiKey::ListOffsets, request).await?;

    let mut results = Vec::new();
    for topic in response.topics {
        for partition in topic.partitions {
            results.push(TimestampOffset {
                topic: topic.name.to_string(),
                partition: partition.partition_index,
                offset: partition.offset,
                timestamp: partition.timestamp,
                error_code: partition.error_code,
            });
        }
    }

    debug!("Found {} offsets by timestamp", results.len());
    Ok(results)
}

/// Parse member assignment bytes to get topic-partition mapping
fn parse_member_assignment(bytes: &[u8]) -> HashMap<String, Vec<i32>> {
    // The member assignment is a Kafka protocol encoded structure
    // For simplicity, we return an empty map if parsing fails
    // A full implementation would decode the ConsumerProtocolAssignment

    if bytes.is_empty() {
        return HashMap::new();
    }

    // TODO: Implement full parsing of ConsumerProtocolAssignment
    // For now, return empty map - the assignment data is encoded in Kafka's internal format
    HashMap::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kafka_protocol::messages::find_coordinator_response::Coordinator;
    use kafka_protocol::messages::{BrokerId, FindCoordinatorResponse};
    use kafka_protocol::protocol::StrBytes;

    #[test]
    fn test_parse_empty_member_assignment() {
        let result = parse_member_assignment(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn group_coordinator_from_response_picks_matching_modern_response() {
        let response = FindCoordinatorResponse::default().with_coordinators(vec![
            Coordinator::default()
                .with_key(StrBytes::from_static_str("other"))
                .with_node_id(BrokerId(1))
                .with_host(StrBytes::from_static_str("broker-1"))
                .with_port(9092),
            Coordinator::default()
                .with_key(StrBytes::from_static_str("analytics"))
                .with_node_id(BrokerId(2))
                .with_host(StrBytes::from_static_str("broker-2"))
                .with_port(9093),
        ]);

        let coordinator = group_coordinator_from_response("analytics", response).unwrap();

        assert_eq!(coordinator.node_id, 2);
        assert_eq!(coordinator.host, "broker-2");
        assert_eq!(coordinator.port, 9093);
    }

    #[test]
    fn transient_coordinator_errors_are_retryable() {
        for code in [
            COORDINATOR_LOAD_IN_PROGRESS,
            COORDINATOR_NOT_AVAILABLE,
            NOT_COORDINATOR,
        ] {
            let error = crate::Error::Kafka(KafkaError::BrokerError {
                code,
                message: "coordinator transient".to_string(),
            });
            assert!(is_transient_coordinator_error(&error));
        }

        let fatal = crate::Error::Kafka(KafkaError::BrokerError {
            code: 30,
            message: "group authorization failed".to_string(),
        });
        assert!(!is_transient_coordinator_error(&fatal));
    }

    #[test]
    fn offset_fetch_coordinator_error_reads_top_level_and_partitions() {
        use kafka_protocol::messages::offset_fetch_response::{
            OffsetFetchResponsePartition, OffsetFetchResponseTopic,
        };

        let top_level = OffsetFetchResponse::default().with_error_code(NOT_COORDINATOR);
        assert_eq!(
            offset_fetch_coordinator_error(&top_level),
            Some(NOT_COORDINATOR)
        );

        // v0/v1 brokers put the coordinator error on every partition.
        let per_partition =
            OffsetFetchResponse::default().with_topics(vec![OffsetFetchResponseTopic::default()
                .with_partitions(vec![OffsetFetchResponsePartition::default()
                    .with_error_code(COORDINATOR_LOAD_IN_PROGRESS)])]);
        assert_eq!(
            offset_fetch_coordinator_error(&per_partition),
            Some(COORDINATOR_LOAD_IN_PROGRESS)
        );

        let denied = OffsetFetchResponse::default().with_error_code(30);
        assert_eq!(offset_fetch_coordinator_error(&denied), None);
    }

    #[test]
    fn offset_commit_coordinator_error_reads_partitions() {
        use kafka_protocol::messages::offset_commit_response::{
            OffsetCommitResponsePartition, OffsetCommitResponseTopic,
        };

        let response = |codes: &[i16]| {
            OffsetCommitResponse::default().with_topics(vec![OffsetCommitResponseTopic::default()
                .with_partitions(
                    codes
                        .iter()
                        .map(|c| OffsetCommitResponsePartition::default().with_error_code(*c))
                        .collect(),
                )])
        };

        assert_eq!(
            offset_commit_coordinator_error(&response(&[0, COORDINATOR_NOT_AVAILABLE])),
            Some(COORDINATOR_NOT_AVAILABLE)
        );
        assert_eq!(offset_commit_coordinator_error(&response(&[0, 30])), None);
    }
}
