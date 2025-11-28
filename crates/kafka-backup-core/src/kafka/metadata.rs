//! Kafka Metadata API implementation.

use kafka_protocol::messages::{ApiKey, MetadataRequest, MetadataResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use tracing::debug;

use super::KafkaClient;
use crate::Result;

/// Broker metadata
#[derive(Debug, Clone)]
pub struct BrokerMetadata {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

/// Topic metadata
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

/// Partition metadata
#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub partition_id: i32,
    pub leader_id: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

/// Fetch cluster metadata from the broker
pub async fn fetch_metadata(
    client: &KafkaClient,
    topics: Option<&[String]>,
) -> Result<Vec<TopicMetadata>> {
    let request = match topics {
        Some(topic_names) => {
            let topic_list: Vec<_> = topic_names
                .iter()
                .map(|name| {
                    kafka_protocol::messages::metadata_request::MetadataRequestTopic::default()
                        .with_name(Some(TopicName(StrBytes::from_string(name.clone()))))
                })
                .collect();
            MetadataRequest::default()
                .with_topics(Some(topic_list))
                .with_allow_auto_topic_creation(false)
        }
        None => {
            // Fetch all topics
            MetadataRequest::default()
                .with_topics(None)
                .with_allow_auto_topic_creation(false)
        }
    };

    let response: MetadataResponse = client.send_request(ApiKey::Metadata, request).await?;

    // Update broker cache
    let brokers: Vec<BrokerMetadata> = response
        .brokers
        .iter()
        .map(|broker| BrokerMetadata {
            node_id: broker.node_id.0,
            host: broker.host.to_string(),
            port: broker.port,
            rack: broker.rack.as_ref().map(|r| r.to_string()),
        })
        .collect();

    debug!("Discovered {} brokers", brokers.len());
    client.update_brokers(brokers).await;

    // Parse topic metadata
    let topics: Vec<TopicMetadata> = response
        .topics
        .iter()
        .filter_map(|topic| {
            if topic.error_code != 0 {
                let name = topic.name.as_ref().map(|n| n.as_str()).unwrap_or("unknown");
                debug!(
                    "Topic {} has error code {}",
                    name,
                    topic.error_code
                );
                return None;
            }

            let name = topic.name.as_ref()?.to_string();

            let partitions: Vec<PartitionMetadata> = topic
                .partitions
                .iter()
                .map(|p| PartitionMetadata {
                    partition_id: p.partition_index,
                    leader_id: p.leader_id.0,
                    replica_nodes: p.replica_nodes.iter().map(|n| n.0).collect(),
                    isr_nodes: p.isr_nodes.iter().map(|n| n.0).collect(),
                })
                .collect();

            Some(TopicMetadata {
                name,
                is_internal: topic.is_internal,
                partitions,
            })
        })
        .collect();

    debug!("Discovered {} topics", topics.len());
    Ok(topics)
}

/// Check if a topic exists and get its partition count
#[allow(dead_code)]
pub async fn topic_exists(client: &KafkaClient, topic: &str) -> Result<bool> {
    let topics = fetch_metadata(client, Some(&[topic.to_string()])).await?;
    Ok(topics.iter().any(|t| t.name == topic))
}
