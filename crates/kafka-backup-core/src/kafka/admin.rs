//! Kafka Admin API implementation (CreateTopics, DeleteRecords, DescribeConfigs,
//! IncrementalAlterConfigs, etc.)

use kafka_protocol::messages::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    delete_records_request::{DeleteRecordsPartition, DeleteRecordsTopic},
    describe_configs_request::{
        DescribeConfigsRequest, DescribeConfigsResource as DescribeConfigsRequestResource,
    },
    incremental_alter_configs_request::{
        AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
    },
    ApiKey, CreateTopicsResponse, DeleteRecordsRequest, DeleteRecordsResponse,
    DescribeConfigsResponse, IncrementalAlterConfigsResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use std::collections::HashMap;
use tracing::{debug, info, warn};

use super::KafkaClient;
use crate::error::KafkaError;
use crate::Result;

/// Configuration for a topic to be created
#[derive(Debug, Clone)]
pub struct TopicToCreate {
    /// Topic name
    pub name: String,
    /// Number of partitions
    pub num_partitions: i32,
    /// Replication factor (-1 for broker default)
    pub replication_factor: i16,
}

/// Result of creating a single topic
#[derive(Debug, Clone)]
pub struct CreateTopicResult {
    /// Topic name
    pub name: String,
    /// Error code (0 = success, 36 = TOPIC_ALREADY_EXISTS)
    pub error_code: i16,
    /// Error message (if any)
    pub error_message: Option<String>,
}

/// Kafka config-resource type values used by DescribeConfigs and
/// IncrementalAlterConfigs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfigResourceType {
    Topic,
    Broker,
}

impl ConfigResourceType {
    fn wire(self) -> i8 {
        match self {
            Self::Topic => 2,
            Self::Broker => 4,
        }
    }

    fn from_wire(value: i8) -> Option<Self> {
        match value {
            2 => Some(Self::Topic),
            4 => Some(Self::Broker),
            _ => None,
        }
    }
}

/// One configuration entry returned by DescribeConfigs.
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    pub config_source: i8,
    pub is_default: bool,
    pub is_sensitive: bool,
}

impl ConfigEntry {
    /// True for explicit topic-level configuration values. Kafka's
    /// ConfigSource value `1` is TOPIC_CONFIG.
    pub fn is_topic_override(&self) -> bool {
        self.config_source == 1
    }
}

/// One incremental config operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigOp {
    Set { name: String, value: String },
    Delete { name: String },
}

impl ConfigOp {
    fn name(&self) -> &str {
        match self {
            Self::Set { name, .. } | Self::Delete { name } => name,
        }
    }

    fn operation(&self) -> i8 {
        match self {
            Self::Set { .. } => 0,
            Self::Delete { .. } => 1,
        }
    }

    fn value(&self) -> Option<StrBytes> {
        match self {
            Self::Set { value, .. } => Some(StrBytes::from_string(value.clone())),
            Self::Delete { .. } => None,
        }
    }
}

/// Incremental config changes for one resource.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigChange {
    pub resource_type: ConfigResourceType,
    pub resource_name: String,
    pub ops: Vec<ConfigOp>,
}

impl CreateTopicResult {
    /// Check if the topic was created successfully or already existed
    pub fn is_success_or_exists(&self) -> bool {
        self.error_code == 0 || self.error_code == 36 // TOPIC_ALREADY_EXISTS
    }

    /// Check if the operation was successful (topic created)
    pub fn is_success(&self) -> bool {
        self.error_code == 0
    }

    /// Check if topic already existed
    pub fn already_exists(&self) -> bool {
        self.error_code == 36
    }
}

/// Create multiple topics in the Kafka cluster
///
/// # Arguments
/// * `client` - Kafka client connection
/// * `topics` - List of topics to create
/// * `timeout_ms` - Timeout for the operation in milliseconds
///
/// # Returns
/// Vec of results, one per topic
pub async fn create_topics(
    client: &KafkaClient,
    topics: Vec<TopicToCreate>,
    timeout_ms: i32,
) -> Result<Vec<CreateTopicResult>> {
    if topics.is_empty() {
        return Ok(Vec::new());
    }

    debug!("Creating {} topics", topics.len());

    // Build CreatableTopic list
    let creatable_topics: Vec<CreatableTopic> = topics
        .iter()
        .map(|t| {
            CreatableTopic::default()
                .with_name(TopicName(StrBytes::from_string(t.name.clone())))
                .with_num_partitions(t.num_partitions)
                .with_replication_factor(t.replication_factor)
        })
        .collect();

    // Build request
    let request = CreateTopicsRequest::default()
        .with_topics(creatable_topics)
        .with_timeout_ms(timeout_ms)
        .with_validate_only(false);

    // Send request
    let response: CreateTopicsResponse = client.send_request(ApiKey::CreateTopics, request).await?;

    // Parse response
    let mut results = Vec::with_capacity(topics.len());

    for topic_result in &response.topics {
        let name = topic_result.name.to_string();
        let error_code = topic_result.error_code;
        let error_message = topic_result.error_message.as_ref().map(|s| s.to_string());

        if error_code == 0 {
            info!("Created topic: {}", name);
        } else if error_code == 36 {
            // TOPIC_ALREADY_EXISTS
            debug!("Topic already exists: {}", name);
        } else {
            warn!(
                "Failed to create topic {}: error_code={}, message={:?}",
                name, error_code, error_message
            );
        }

        results.push(CreateTopicResult {
            name,
            error_code,
            error_message,
        });
    }

    // Check for any unexpected failures (not success or already exists)
    let failures: Vec<_> = results
        .iter()
        .filter(|r| !r.is_success_or_exists())
        .collect();

    if !failures.is_empty() {
        let failure_msgs: Vec<String> = failures
            .iter()
            .map(|r| {
                format!(
                    "{}: code={}, msg={:?}",
                    r.name, r.error_code, r.error_message
                )
            })
            .collect();

        return Err(KafkaError::Protocol(format!(
            "Failed to create topics: {}",
            failure_msgs.join("; ")
        ))
        .into());
    }

    Ok(results)
}

/// Delete records from Kafka partitions by advancing the log-start-offset.
///
/// Records **before** `before_offset` in each partition become inaccessible
/// (the new log-start-offset is set to `before_offset`). This empties a topic
/// without deleting it — the approach required for Strimzi-managed topics that
/// cannot be deleted and recreated (Issue #67 bug 10).
///
/// # Arguments
/// * `client`       – Kafka client (broker routes internally)
/// * `topic`        – Target topic name
/// * `partitions`   – `(partition_id, before_offset)` pairs
/// * `timeout_ms`   – Broker-side request timeout
pub async fn delete_records(
    client: &KafkaClient,
    topic: &str,
    partitions: &[(i32, i64)],
    timeout_ms: i32,
) -> Result<()> {
    if partitions.is_empty() {
        return Ok(());
    }

    let dr_partitions: Vec<DeleteRecordsPartition> = partitions
        .iter()
        .map(|(pid, offset)| {
            DeleteRecordsPartition::default()
                .with_partition_index(*pid)
                .with_offset(*offset)
        })
        .collect();

    let topics = vec![DeleteRecordsTopic::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_owned())))
        .with_partitions(dr_partitions)];

    let request = DeleteRecordsRequest::default()
        .with_topics(topics)
        .with_timeout_ms(timeout_ms);

    let response: DeleteRecordsResponse =
        client.send_request(ApiKey::DeleteRecords, request).await?;

    for topic_result in &response.topics {
        for part in &topic_result.partitions {
            if part.error_code != 0 {
                warn!(
                    "DeleteRecords failed for {}[{}]: error_code={}",
                    topic_result.name.0, part.partition_index, part.error_code
                );
            } else {
                debug!(
                    "Purged {}[{}] new log-start-offset={}",
                    topic_result.name.0, part.partition_index, part.low_watermark
                );
            }
        }
    }

    Ok(())
}

/// Describe configuration entries for the requested resources.
pub async fn describe_configs(
    client: &KafkaClient,
    resources: &[(ConfigResourceType, String)],
) -> Result<HashMap<(ConfigResourceType, String), Vec<ConfigEntry>>> {
    if resources.is_empty() {
        return Ok(HashMap::new());
    }

    let request_resources: Vec<DescribeConfigsRequestResource> = resources
        .iter()
        .map(|(resource_type, resource_name)| {
            DescribeConfigsRequestResource::default()
                .with_resource_type(resource_type.wire())
                .with_resource_name(StrBytes::from_string(resource_name.clone()))
                .with_configuration_keys(None)
        })
        .collect();

    let request = DescribeConfigsRequest::default()
        .with_resources(request_resources)
        .with_include_synonyms(false)
        .with_include_documentation(false);

    let response: DescribeConfigsResponse = client
        .send_request(ApiKey::DescribeConfigs, request)
        .await?;

    let mut out = HashMap::new();
    for result in response.results {
        if result.error_code != 0 {
            let msg = result
                .error_message
                .as_ref()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "<no-message>".to_string());
            return Err(KafkaError::Protocol(format!(
                "DescribeConfigs failed for {}/{}: error_code={} message={}",
                result.resource_type, result.resource_name, result.error_code, msg
            ))
            .into());
        }

        let Some(resource_type) = ConfigResourceType::from_wire(result.resource_type) else {
            continue;
        };
        let resource_name = result.resource_name.to_string();
        let entries = result
            .configs
            .into_iter()
            .map(|entry| ConfigEntry {
                name: entry.name.to_string(),
                value: entry.value.map(|v| v.to_string()),
                read_only: entry.read_only,
                config_source: entry.config_source,
                is_default: entry.config_source == 5,
                is_sensitive: entry.is_sensitive,
            })
            .collect();
        out.insert((resource_type, resource_name), entries);
    }

    Ok(out)
}

/// Apply incremental config changes. Returns an error if any resource fails.
pub async fn incremental_alter_configs(
    client: &KafkaClient,
    changes: &[ConfigChange],
) -> Result<()> {
    let resources: Vec<AlterConfigsResource> = changes
        .iter()
        .filter(|change| !change.ops.is_empty())
        .map(|change| {
            let configs: Vec<AlterableConfig> = change
                .ops
                .iter()
                .map(|op| {
                    AlterableConfig::default()
                        .with_name(StrBytes::from_string(op.name().to_string()))
                        .with_config_operation(op.operation())
                        .with_value(op.value())
                })
                .collect();
            AlterConfigsResource::default()
                .with_resource_type(change.resource_type.wire())
                .with_resource_name(StrBytes::from_string(change.resource_name.clone()))
                .with_configs(configs)
        })
        .collect();

    if resources.is_empty() {
        return Ok(());
    }

    let request = IncrementalAlterConfigsRequest::default()
        .with_resources(resources)
        .with_validate_only(false);

    let response: IncrementalAlterConfigsResponse = client
        .send_request(ApiKey::IncrementalAlterConfigs, request)
        .await?;

    let failures: Vec<String> = response
        .responses
        .into_iter()
        .filter(|r| r.error_code != 0)
        .map(|r| {
            let msg = r
                .error_message
                .as_ref()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "<no-message>".to_string());
            format!(
                "{}/{}: error_code={} message={}",
                r.resource_type, r.resource_name, r.error_code, msg
            )
        })
        .collect();

    if !failures.is_empty() {
        return Err(KafkaError::Protocol(format!(
            "IncrementalAlterConfigs failed: {}",
            failures.join("; ")
        ))
        .into());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_topic_result_success() {
        let result = CreateTopicResult {
            name: "test".to_string(),
            error_code: 0,
            error_message: None,
        };
        assert!(result.is_success());
        assert!(result.is_success_or_exists());
        assert!(!result.already_exists());
    }

    #[test]
    fn test_create_topic_result_already_exists() {
        let result = CreateTopicResult {
            name: "test".to_string(),
            error_code: 36,
            error_message: Some("Topic already exists".to_string()),
        };
        assert!(!result.is_success());
        assert!(result.is_success_or_exists());
        assert!(result.already_exists());
    }

    #[test]
    fn test_create_topic_result_failure() {
        let result = CreateTopicResult {
            name: "test".to_string(),
            error_code: 73, // INVALID_TOPIC_EXCEPTION
            error_message: Some("Invalid topic name".to_string()),
        };
        assert!(!result.is_success());
        assert!(!result.is_success_or_exists());
        assert!(!result.already_exists());
    }
}
