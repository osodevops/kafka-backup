//! Kafka Admin API implementation (CreateTopics, etc.)

use kafka_protocol::messages::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    ApiKey, CreateTopicsResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
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
