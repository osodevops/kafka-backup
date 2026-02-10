//! Partition leader routing for multi-broker Kafka clusters.
//!
//! This module provides a router that directs fetch/produce requests to the
//! correct broker based on partition leadership. In multi-broker clusters,
//! different brokers are leaders for different partitions, and requests must
//! be sent to the leader to avoid NOT_LEADER_FOR_PARTITION errors.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::config::KafkaConfig;
use crate::error::KafkaError;
use crate::Result;

use super::metadata::{BrokerMetadata, PartitionMetadata, TopicMetadata};
use super::{FetchResponse, KafkaClient, ProduceResponse};
use crate::manifest::BackupRecord;

/// Routes Kafka requests to the correct partition leader broker.
///
/// This router maintains:
/// - A mapping of (topic, partition) -> leader broker ID
/// - A connection pool to all discovered brokers
/// - Automatic metadata refresh on leadership changes
pub struct PartitionLeaderRouter {
    /// Bootstrap servers for initial connection and metadata refresh
    #[allow(dead_code)]
    bootstrap_servers: Vec<String>,

    /// Kafka configuration (for creating new connections)
    config: KafkaConfig,

    /// Map of broker_id -> BrokerMetadata (host:port info)
    broker_metadata: Arc<RwLock<HashMap<i32, BrokerMetadata>>>,

    /// Map of (topic, partition) -> leader broker_id
    partition_leaders: Arc<RwLock<HashMap<(String, i32), i32>>>,

    /// Connection pool: broker_id -> Vec of KafkaClients
    /// Multiple connections per broker allow parallel in-flight requests,
    /// preventing the single-connection mutex from becoming a bottleneck
    /// on high-latency connections (Issue #29).
    connections: Arc<RwLock<HashMap<i32, Vec<Arc<KafkaClient>>>>>,

    /// Round-robin counter for distributing requests across connections
    connection_index: AtomicUsize,

    /// Bootstrap client for metadata operations
    bootstrap_client: Arc<KafkaClient>,
}

impl PartitionLeaderRouter {
    /// Create a new partition leader router.
    ///
    /// This connects to the bootstrap servers, fetches cluster metadata,
    /// and builds the initial partition leader map.
    pub async fn new(config: KafkaConfig) -> Result<Self> {
        let bootstrap_servers = config.bootstrap_servers.clone();

        // Create and connect bootstrap client
        let bootstrap_client = Arc::new(KafkaClient::new(config.clone()));
        bootstrap_client.connect().await?;

        let router = Self {
            bootstrap_servers,
            config,
            broker_metadata: Arc::new(RwLock::new(HashMap::new())),
            partition_leaders: Arc::new(RwLock::new(HashMap::new())),
            connections: Arc::new(RwLock::new(HashMap::new())),
            connection_index: AtomicUsize::new(0),
            bootstrap_client,
        };

        // Fetch initial metadata
        router.refresh_metadata().await?;

        Ok(router)
    }

    /// Refresh cluster metadata and update partition leader mappings.
    ///
    /// This should be called periodically or when a NOT_LEADER_FOR_PARTITION
    /// error is encountered.
    pub async fn refresh_metadata(&self) -> Result<()> {
        debug!("Refreshing cluster metadata");

        // Fetch all topics metadata
        let topics = self.bootstrap_client.fetch_metadata(None).await?;

        // Get broker metadata from the client's cache
        let brokers = {
            // The fetch_metadata call updates the brokers cache internally
            // We need to re-fetch it
            let broker_list = self.get_brokers_from_topics(&topics).await?;
            broker_list
        };

        // Update broker metadata
        {
            let mut broker_meta = self.broker_metadata.write().await;
            broker_meta.clear();
            for broker in brokers {
                debug!(
                    "Discovered broker {}: {}:{}",
                    broker.node_id, broker.host, broker.port
                );
                broker_meta.insert(broker.node_id, broker);
            }
        }

        // Update partition leaders
        {
            let mut leaders = self.partition_leaders.write().await;
            leaders.clear();

            for topic in &topics {
                for partition in &topic.partitions {
                    leaders.insert(
                        (topic.name.clone(), partition.partition_id),
                        partition.leader_id,
                    );
                    debug!(
                        "Partition {}/{} leader: broker {}",
                        topic.name, partition.partition_id, partition.leader_id
                    );
                }
            }

            info!(
                "Refreshed metadata: {} brokers, {} partition leaders",
                self.broker_metadata.read().await.len(),
                leaders.len()
            );
        }

        Ok(())
    }

    /// Get brokers from metadata response by re-fetching metadata
    async fn get_brokers_from_topics(
        &self,
        _topics: &[TopicMetadata],
    ) -> Result<Vec<BrokerMetadata>> {
        // Re-fetch metadata to get broker info - the fetch_metadata call
        // updates the internal broker cache
        use kafka_protocol::messages::{ApiKey, MetadataRequest, MetadataResponse};

        let request = MetadataRequest::default()
            .with_topics(None)
            .with_allow_auto_topic_creation(false);

        let response: MetadataResponse = self
            .bootstrap_client
            .send_request(ApiKey::Metadata, request)
            .await?;

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

        Ok(brokers)
    }

    /// Refresh metadata for a specific topic/partition.
    ///
    /// Use this for targeted refresh after a NOT_LEADER_FOR_PARTITION error.
    pub async fn refresh_partition_leader(&self, topic: &str, partition: i32) -> Result<()> {
        debug!("Refreshing leader for {}/{}", topic, partition);

        // Fetch metadata for just this topic
        let topics = self
            .bootstrap_client
            .fetch_metadata(Some(&[topic.to_string()]))
            .await?;

        // Also refresh broker metadata
        let brokers = self.get_brokers_from_topics(&topics).await?;
        {
            let mut broker_meta = self.broker_metadata.write().await;
            for broker in brokers {
                broker_meta.insert(broker.node_id, broker);
            }
        }

        // Update the specific partition leader
        for t in topics {
            if t.name == topic {
                for p in t.partitions {
                    if p.partition_id == partition {
                        let mut leaders = self.partition_leaders.write().await;
                        leaders.insert((topic.to_string(), partition), p.leader_id);
                        info!(
                            "Updated leader for {}/{}: broker {}",
                            topic, partition, p.leader_id
                        );
                        return Ok(());
                    }
                }
            }
        }

        Err(KafkaError::PartitionNotAvailable {
            topic: topic.to_string(),
            partition,
        }
        .into())
    }

    /// Get the leader broker ID for a partition.
    pub async fn get_leader(&self, topic: &str, partition: i32) -> Result<i32> {
        let leaders = self.partition_leaders.read().await;
        leaders
            .get(&(topic.to_string(), partition))
            .copied()
            .ok_or_else(|| {
                KafkaError::PartitionNotAvailable {
                    topic: topic.to_string(),
                    partition,
                }
                .into()
            })
    }

    /// Get or create a connection to a specific broker.
    ///
    /// Maintains a pool of N connections per broker (configured via
    /// `connections_per_broker`). Uses round-robin selection to distribute
    /// requests across connections, reducing mutex contention.
    async fn get_broker_connection(&self, broker_id: i32) -> Result<Arc<KafkaClient>> {
        let pool_size = self.config.connection.connections_per_broker.max(1);

        // Check if we already have a full pool
        {
            let connections = self.connections.read().await;
            if let Some(pool) = connections.get(&broker_id) {
                if pool.len() >= pool_size {
                    // Round-robin selection
                    let idx = self.connection_index.fetch_add(1, Ordering::Relaxed) % pool.len();
                    return Ok(Arc::clone(&pool[idx]));
                }
            }
        }

        // Get broker address
        let broker_addr = {
            let brokers = self.broker_metadata.read().await;
            let broker = brokers
                .get(&broker_id)
                .ok_or_else(|| KafkaError::Protocol(format!("Unknown broker ID: {}", broker_id)))?;
            format!("{}:{}", broker.host, broker.port)
        };

        // Create the full pool of connections
        debug!(
            "Creating {} connections to broker {} at {}",
            pool_size, broker_id, broker_addr
        );

        let mut pool = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            let mut broker_config = self.config.clone();
            broker_config.bootstrap_servers = vec![broker_addr.clone()];

            let client = Arc::new(KafkaClient::new(broker_config));
            client.connect().await?;
            pool.push(client);
        }

        // Store in connection pool and return one via round-robin
        let idx = self.connection_index.fetch_add(1, Ordering::Relaxed) % pool.len();
        let selected = Arc::clone(&pool[idx]);

        {
            let mut connections = self.connections.write().await;
            connections.insert(broker_id, pool);
        }

        Ok(selected)
    }

    /// Get a client connected to the partition's leader broker.
    pub async fn get_leader_client(&self, topic: &str, partition: i32) -> Result<Arc<KafkaClient>> {
        let leader_id = self.get_leader(topic, partition).await?;
        self.get_broker_connection(leader_id).await
    }

    /// Fetch records from a partition, routing to the correct leader.
    ///
    /// This method handles NOT_LEADER_FOR_PARTITION errors by refreshing
    /// metadata and retrying once.
    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: i32,
    ) -> Result<FetchResponse> {
        // First attempt
        match self
            .fetch_internal(topic, partition, offset, max_bytes)
            .await
        {
            Ok(response) => Ok(response),
            Err(e) if is_not_leader_error(&e) => {
                // Refresh metadata and retry
                warn!(
                    "NOT_LEADER_FOR_PARTITION error for {}/{}, refreshing metadata",
                    topic, partition
                );
                self.refresh_partition_leader(topic, partition).await?;

                // Clear cached connection for old leader
                self.clear_connection_cache().await;

                // Retry with new leader
                self.fetch_internal(topic, partition, offset, max_bytes)
                    .await
            }
            Err(e) => Err(e),
        }
    }

    /// Internal fetch implementation.
    async fn fetch_internal(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: i32,
    ) -> Result<FetchResponse> {
        let client = self.get_leader_client(topic, partition).await?;
        client.fetch(topic, partition, offset, max_bytes).await
    }

    /// Batch get offsets for all given topic-partitions in minimal network calls.
    ///
    /// Groups partitions by their leader broker and sends one batched ListOffsetsRequest
    /// per broker per timestamp. For N partitions across B brokers, this makes 2*B requests
    /// (earliest + latest) instead of 2*N individual requests.
    ///
    /// Returns a map of (topic, partition) -> (earliest, latest) offset.
    pub async fn batch_get_all_offsets(
        &self,
        partitions: &[(String, i32)],
    ) -> Result<HashMap<(String, i32), (i64, i64)>> {
        // Group partitions by leader broker
        let mut broker_partitions: HashMap<i32, Vec<(String, i32)>> = HashMap::new();
        for (topic, partition) in partitions {
            let leader_id = self.get_leader(topic, *partition).await?;
            broker_partitions
                .entry(leader_id)
                .or_default()
                .push((topic.clone(), *partition));
        }

        debug!(
            "batch_get_all_offsets: {} partitions across {} brokers",
            partitions.len(),
            broker_partitions.len()
        );

        let mut all_results: HashMap<(String, i32), (i64, i64)> = HashMap::new();

        // For each broker, send one batched request for earliest and one for latest
        for (broker_id, broker_parts) in &broker_partitions {
            let client = self.get_broker_connection(*broker_id).await?;

            // Build earliest requests (timestamp = -2)
            let earliest_requests: Vec<(String, i32, i64)> = broker_parts
                .iter()
                .map(|(t, p)| (t.clone(), *p, -2i64))
                .collect();

            // Build latest requests (timestamp = -1)
            let latest_requests: Vec<(String, i32, i64)> = broker_parts
                .iter()
                .map(|(t, p)| (t.clone(), *p, -1i64))
                .collect();

            let earliest_offsets: HashMap<(String, i32), i64> =
                super::fetch::batch_get_offsets(&client, &earliest_requests).await?;
            let latest_offsets: HashMap<(String, i32), i64> =
                super::fetch::batch_get_offsets(&client, &latest_requests).await?;

            // Merge results
            for (topic, partition) in broker_parts {
                let key = (topic.clone(), *partition);
                let earliest = earliest_offsets.get(&key).copied().unwrap_or(0);
                let latest = latest_offsets.get(&key).copied().unwrap_or(0);
                all_results.insert(key, (earliest, latest));
            }
        }

        debug!(
            "batch_get_all_offsets: completed with {} results ({} broker calls)",
            all_results.len(),
            broker_partitions.len() * 2
        );

        Ok(all_results)
    }

    /// Get the earliest and latest offsets for a partition.
    pub async fn get_offsets(&self, topic: &str, partition: i32) -> Result<(i64, i64)> {
        // First attempt
        match self.get_offsets_internal(topic, partition).await {
            Ok(offsets) => Ok(offsets),
            Err(e) if is_not_leader_error(&e) => {
                // Refresh metadata and retry
                warn!(
                    "NOT_LEADER_FOR_PARTITION error for {}/{} during get_offsets, refreshing metadata",
                    topic, partition
                );
                self.refresh_partition_leader(topic, partition).await?;
                self.clear_connection_cache().await;
                self.get_offsets_internal(topic, partition).await
            }
            Err(e) => Err(e),
        }
    }

    /// Internal get_offsets implementation.
    async fn get_offsets_internal(&self, topic: &str, partition: i32) -> Result<(i64, i64)> {
        let client = self.get_leader_client(topic, partition).await?;
        client.get_offsets(topic, partition).await
    }

    /// Produce records to a partition, routing to the correct leader.
    pub async fn produce(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<BackupRecord>,
    ) -> Result<ProduceResponse> {
        // First attempt
        match self
            .produce_internal(topic, partition, records.clone())
            .await
        {
            Ok(response) => Ok(response),
            Err(e) if is_not_leader_error(&e) => {
                // Refresh metadata and retry
                warn!(
                    "NOT_LEADER_FOR_PARTITION error for {}/{} during produce, refreshing metadata",
                    topic, partition
                );
                self.refresh_partition_leader(topic, partition).await?;
                self.clear_connection_cache().await;
                self.produce_internal(topic, partition, records).await
            }
            Err(e) => Err(e),
        }
    }

    /// Internal produce implementation.
    async fn produce_internal(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<BackupRecord>,
    ) -> Result<ProduceResponse> {
        let client = self.get_leader_client(topic, partition).await?;
        client.produce(topic, partition, records).await
    }

    /// Clear the connection cache (useful after metadata refresh).
    async fn clear_connection_cache(&self) {
        let mut connections = self.connections.write().await;
        connections.clear();
        debug!("Cleared connection cache");
    }

    /// Fetch metadata using the bootstrap client.
    ///
    /// This is useful for operations that don't need leader routing,
    /// such as listing topics.
    pub async fn fetch_metadata(&self, topics: Option<&[String]>) -> Result<Vec<TopicMetadata>> {
        self.bootstrap_client.fetch_metadata(topics).await
    }

    /// Create topics in the Kafka cluster.
    ///
    /// This sends the CreateTopics request to any broker (the broker will
    /// route to the controller internally).
    pub async fn create_topics(
        &self,
        topics: Vec<super::TopicToCreate>,
        timeout_ms: i32,
    ) -> Result<Vec<super::CreateTopicResult>> {
        self.bootstrap_client
            .create_topics(topics, timeout_ms)
            .await
    }

    /// Get metadata for a specific topic.
    pub async fn get_topic_metadata(&self, topic: &str) -> Result<TopicMetadata> {
        self.bootstrap_client.get_topic_metadata(topic).await
    }

    /// Get partition metadata for a topic.
    pub async fn get_partitions(&self, topic: &str) -> Result<Vec<PartitionMetadata>> {
        let metadata = self.get_topic_metadata(topic).await?;
        Ok(metadata.partitions)
    }
}

/// Check if an error is a NOT_LEADER_FOR_PARTITION error (code 6).
fn is_not_leader_error(error: &crate::Error) -> bool {
    match error {
        crate::Error::Kafka(KafkaError::BrokerError { code, .. }) => *code == 6,
        _ => {
            // Also check error message as fallback
            let msg = error.to_string();
            msg.contains("NOT_LEADER") || msg.contains("code 6")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_not_leader_error() {
        let error = crate::Error::Kafka(KafkaError::BrokerError {
            code: 6,
            message: "NOT_LEADER_FOR_PARTITION".to_string(),
        });
        assert!(is_not_leader_error(&error));

        let other_error = crate::Error::Kafka(KafkaError::BrokerError {
            code: 1,
            message: "Some other error".to_string(),
        });
        assert!(!is_not_leader_error(&other_error));
    }
}
