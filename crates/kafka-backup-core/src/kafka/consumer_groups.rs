//! Consumer group operations for offset management.
//!
//! This module implements Kafka protocol operations for consumer group offset management:
//! - ListGroups: List all consumer groups
//! - DescribeGroups: Get consumer group details
//! - OffsetFetch: Get committed offsets for a group
//! - OffsetCommit: Commit offsets for a group
//! - ListOffsetsForTimes: Find offsets by timestamp

use kafka_protocol::messages::{
    ApiKey, DescribeGroupsRequest, DescribeGroupsResponse, GroupId, ListGroupsRequest,
    ListGroupsResponse, ListOffsetsRequest, ListOffsetsResponse, OffsetCommitRequest,
    OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use std::collections::HashMap;
use tracing::{debug, warn};

use super::KafkaClient;
use crate::error::KafkaError;
use crate::Result;

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

/// Fetch committed offsets for a consumer group
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

    let response: OffsetFetchResponse = client.send_request(ApiKey::OffsetFetch, request).await?;

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

/// Commit offsets for a consumer group
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
                        .with_committed_metadata(metadata.map(|m| StrBytes::from_string(m)))
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

    let response: OffsetCommitResponse = client.send_request(ApiKey::OffsetCommit, request).await?;

    let mut results = Vec::new();
    for topic in response.topics {
        for partition in topic.partitions {
            if partition.error_code != 0 {
                warn!(
                    "Failed to commit offset for {}:{} - error code {}",
                    topic.name.as_str(), partition.partition_index, partition.error_code
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

    #[test]
    fn test_parse_empty_member_assignment() {
        let result = parse_member_assignment(&[]);
        assert!(result.is_empty());
    }
}
