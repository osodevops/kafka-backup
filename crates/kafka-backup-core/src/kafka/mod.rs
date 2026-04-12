//! Kafka protocol client implementation.

mod admin;
mod client;
pub mod consumer_groups;
mod fetch;
mod metadata;
mod partition_router;
mod produce;
mod scram;
pub mod tls;

pub use admin::{create_topics, delete_records, CreateTopicResult, TopicToCreate};
pub use client::{KafkaClient, RESPONSE_TIMEOUT_SECS, WRITE_TIMEOUT_SECS};
pub use consumer_groups::{
    commit_offsets, describe_groups, fetch_offsets, list_groups, offsets_for_times,
    CommittedOffset, ConsumerGroup, ConsumerGroupDescription, ConsumerGroupMember, TimestampOffset,
};
pub use fetch::FetchResponse;
pub use metadata::{BrokerMetadata, PartitionMetadata, TopicMetadata};
pub use partition_router::PartitionLeaderRouter;
pub use produce::ProduceResponse;

/// Public test helper: returns true if `error` is a connection-level error that
/// the client classifies as retriable (broken pipe, timeout, etc.).
/// Exposed for integration tests that verify the timeout classification path.
pub fn is_connection_error_public(error: &crate::Error) -> bool {
    KafkaClient::is_connection_error_pub(error)
}
