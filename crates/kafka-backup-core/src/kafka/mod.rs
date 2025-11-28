//! Kafka protocol client implementation.

mod client;
pub mod consumer_groups;
mod fetch;
mod metadata;
mod partition_router;
mod produce;

pub use client::KafkaClient;
pub use consumer_groups::{
    commit_offsets, describe_groups, fetch_offsets, list_groups, offsets_for_times,
    CommittedOffset, ConsumerGroup, ConsumerGroupDescription, ConsumerGroupMember, TimestampOffset,
};
pub use fetch::FetchResponse;
pub use metadata::{BrokerMetadata, PartitionMetadata, TopicMetadata};
pub use partition_router::PartitionLeaderRouter;
pub use produce::ProduceResponse;
