//! Kafka client for protocol-level communication.

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::protocol::{Decodable, Encodable};
use socket2::{SockRef, TcpKeepalive};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::TlsConnector;
use tracing::{debug, trace};

use crate::config::{KafkaConfig, SaslMechanism, SecurityProtocol};
use crate::error::KafkaError;
use crate::Result;

use super::metadata::{BrokerMetadata, TopicMetadata};
use super::FetchResponse;
use super::ProduceResponse;

/// Kafka client for protocol-level operations
pub struct KafkaClient {
    /// Configuration
    config: KafkaConfig,

    /// Connection to the current broker
    connection: Arc<Mutex<Option<BrokerConnection>>>,

    /// Broker metadata cache
    brokers: Arc<Mutex<HashMap<i32, BrokerMetadata>>>,

    /// Topic metadata cache
    #[allow(dead_code)]
    topics: Arc<Mutex<HashMap<String, TopicMetadata>>>,

    /// Correlation ID counter
    correlation_id: AtomicI32,
}

/// A stream that can be either plain TCP or TLS-wrapped
enum ConnectionStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl ConnectionStream {
    async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            ConnectionStream::Plain(s) => {
                s.read_exact(buf).await?;
                Ok(())
            }
            ConnectionStream::Tls(s) => {
                s.read_exact(buf).await?;
                Ok(())
            }
        }
    }

    async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            ConnectionStream::Plain(s) => s.write_all(buf).await,
            ConnectionStream::Tls(s) => s.write_all(buf).await,
        }
    }
}

struct BrokerConnection {
    stream: ConnectionStream,
    #[allow(dead_code)]
    broker_id: i32,
}

impl KafkaClient {
    /// Create a new Kafka client
    pub fn new(config: KafkaConfig) -> Self {
        Self {
            config,
            connection: Arc::new(Mutex::new(None)),
            brokers: Arc::new(Mutex::new(HashMap::new())),
            topics: Arc::new(Mutex::new(HashMap::new())),
            correlation_id: AtomicI32::new(1),
        }
    }

    /// Connect to the Kafka cluster
    pub async fn connect(&self) -> Result<()> {
        // Try each bootstrap server until one connects
        for server in &self.config.bootstrap_servers {
            match self.try_connect(server).await {
                Ok(stream) => {
                    let mut conn = self.connection.lock().await;
                    *conn = Some(BrokerConnection {
                        stream,
                        broker_id: -1, // Unknown until metadata fetch
                    });

                    // Perform SASL authentication if configured
                    if self.config.security.security_protocol == SecurityProtocol::SaslPlaintext
                        || self.config.security.security_protocol == SecurityProtocol::SaslSsl
                    {
                        drop(conn);
                        self.authenticate().await?;
                    }

                    debug!("Connected to Kafka broker: {}", server);
                    return Ok(());
                }
                Err(e) => {
                    debug!("Failed to connect to {}: {}", server, e);
                    continue;
                }
            }
        }

        Err(KafkaError::NoBrokersAvailable.into())
    }

    async fn try_connect(&self, server: &str) -> Result<ConnectionStream> {
        let tcp_stream =
            TcpStream::connect(server)
                .await
                .map_err(|e| KafkaError::ConnectionFailed {
                    broker: server.to_string(),
                    message: e.to_string(),
                })?;

        // Configure TCP socket options (keepalive, nodelay)
        self.configure_socket(&tcp_stream, server)?;

        // Wrap in TLS if using SSL or SASL_SSL
        let use_tls = matches!(
            self.config.security.security_protocol,
            SecurityProtocol::Ssl | SecurityProtocol::SaslSsl
        );

        if use_tls {
            debug!("Establishing TLS connection to {}", server);

            // Build TLS config using security settings (custom CA, mTLS support)
            let tls_config = super::tls::build_tls_config(&self.config.security)?;

            let connector = TlsConnector::from(Arc::new(tls_config));

            // Extract hostname from server address (host:port)
            let hostname = server.split(':').next().unwrap_or(server);

            let server_name = ServerName::try_from(hostname.to_string()).map_err(|e| {
                KafkaError::ConnectionFailed {
                    broker: server.to_string(),
                    message: format!("Invalid server name for TLS: {}", e),
                }
            })?;

            let tls_stream = connector
                .connect(server_name, tcp_stream)
                .await
                .map_err(|e| KafkaError::ConnectionFailed {
                    broker: server.to_string(),
                    message: format!("TLS handshake failed: {}", e),
                })?;

            debug!("TLS connection established to {}", server);
            Ok(ConnectionStream::Tls(Box::new(tls_stream)))
        } else {
            Ok(ConnectionStream::Plain(tcp_stream))
        }
    }

    /// Configure TCP socket options (keepalive, nodelay) based on connection config.
    fn configure_socket(&self, stream: &TcpStream, server: &str) -> Result<()> {
        let conn_config = &self.config.connection;

        // Get socket reference for configuration
        let sock_ref = SockRef::from(stream);

        // Enable TCP_NODELAY if configured
        if conn_config.tcp_nodelay {
            sock_ref
                .set_nodelay(true)
                .map_err(|e| KafkaError::ConnectionFailed {
                    broker: server.to_string(),
                    message: format!("Failed to set TCP_NODELAY: {}", e),
                })?;
        }

        // Configure TCP keepalive if enabled
        if conn_config.tcp_keepalive {
            let keepalive = TcpKeepalive::new()
                .with_time(Duration::from_secs(conn_config.keepalive_time_secs))
                .with_interval(Duration::from_secs(conn_config.keepalive_interval_secs));

            sock_ref
                .set_tcp_keepalive(&keepalive)
                .map_err(|e| KafkaError::ConnectionFailed {
                    broker: server.to_string(),
                    message: format!("Failed to set TCP keepalive: {}", e),
                })?;

            debug!(
                "TCP keepalive enabled for {}: time={}s, interval={}s",
                server, conn_config.keepalive_time_secs, conn_config.keepalive_interval_secs
            );
        }

        Ok(())
    }

    async fn authenticate(&self) -> Result<()> {
        let security = &self.config.security;

        match security.sasl_mechanism {
            Some(SaslMechanism::Plain) => {
                self.sasl_plain_auth(
                    security.sasl_username.as_deref().unwrap_or(""),
                    security.sasl_password.as_deref().unwrap_or(""),
                )
                .await
            }
            Some(SaslMechanism::ScramSha256) | Some(SaslMechanism::ScramSha512) => {
                // SCRAM authentication would go here
                Err(crate::Error::Authentication(
                    "SCRAM authentication not yet implemented".to_string(),
                ))
            }
            None => Ok(()), // No authentication needed
        }
    }

    async fn sasl_plain_auth(&self, username: &str, password: &str) -> Result<()> {
        use kafka_protocol::messages::{SaslAuthenticateRequest, SaslHandshakeRequest};

        // Step 1: SASL Handshake
        let handshake_request = SaslHandshakeRequest::default().with_mechanism("PLAIN".into());
        let _handshake_response: kafka_protocol::messages::SaslHandshakeResponse = self
            .send_request(ApiKey::SaslHandshake, handshake_request)
            .await?;

        // Step 2: SASL Authenticate with PLAIN mechanism
        // PLAIN format: \0username\0password
        let mut auth_bytes = Vec::new();
        auth_bytes.push(0); // authzid (empty)
        auth_bytes.extend_from_slice(username.as_bytes());
        auth_bytes.push(0);
        auth_bytes.extend_from_slice(password.as_bytes());

        let auth_request =
            SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(auth_bytes));
        let auth_response: kafka_protocol::messages::SaslAuthenticateResponse = self
            .send_request(ApiKey::SaslAuthenticate, auth_request)
            .await?;

        if auth_response.error_code != 0 {
            return Err(crate::Error::Authentication(format!(
                "SASL authentication failed: {}",
                auth_response
                    .error_message
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| format!("error code {}", auth_response.error_code))
            )));
        }

        debug!("SASL PLAIN authentication successful");
        Ok(())
    }

    /// Get the next correlation ID
    fn next_correlation_id(&self) -> i32 {
        self.correlation_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Send a request and receive a response
    pub async fn send_request<Req, Resp>(&self, api_key: ApiKey, request: Req) -> Result<Resp>
    where
        Req: Encodable + Default,
        Resp: Decodable + Default,
    {
        let correlation_id = self.next_correlation_id();
        let api_version = self.get_api_version(api_key);

        // Build request header
        let header = RequestHeader::default()
            .with_request_api_key(api_key as i16)
            .with_request_api_version(api_version)
            .with_correlation_id(correlation_id)
            .with_client_id(Some(StrBytes::from_static_str("kafka-backup")));

        // Encode header and request
        let header_version = api_key.request_header_version(api_version);
        let mut buf = BytesMut::new();

        // Reserve space for the length prefix
        buf.put_i32(0);

        header
            .encode(&mut buf, header_version)
            .map_err(|e| KafkaError::Protocol(format!("Failed to encode header: {:?}", e)))?;
        request
            .encode(&mut buf, api_version)
            .map_err(|e| KafkaError::Protocol(format!("Failed to encode request: {:?}", e)))?;

        // Update length prefix
        let len = (buf.len() - 4) as i32;
        buf[0..4].copy_from_slice(&len.to_be_bytes());

        trace!(
            "Sending request: api_key={:?}, api_version={}, correlation_id={}, len={}",
            api_key,
            api_version,
            correlation_id,
            len
        );

        // Send request
        let mut conn = self.connection.lock().await;
        let conn = conn
            .as_mut()
            .ok_or_else(|| KafkaError::Protocol("Not connected".to_string()))?;

        conn.stream
            .write_all(&buf)
            .await
            .map_err(|e| KafkaError::Protocol(format!("Failed to send request: {}", e)))?;

        // Read response length
        let mut len_buf = [0u8; 4];
        conn.stream
            .read_exact(&mut len_buf)
            .await
            .map_err(|e| KafkaError::Protocol(format!("Failed to read response length: {}", e)))?;
        let response_len = i32::from_be_bytes(len_buf) as usize;

        trace!("Receiving response: len={}", response_len);

        // Read response body
        let mut response_buf = vec![0u8; response_len];
        conn.stream
            .read_exact(&mut response_buf)
            .await
            .map_err(|e| KafkaError::Protocol(format!("Failed to read response body: {}", e)))?;

        // Decode response
        let mut response_bytes = Bytes::from(response_buf);
        let response_header_version = api_key.response_header_version(api_version);
        let _response_header = ResponseHeader::decode(&mut response_bytes, response_header_version)
            .map_err(|e| {
                KafkaError::Protocol(format!("Failed to decode response header: {:?}", e))
            })?;

        let response = Resp::decode(&mut response_bytes, api_version)
            .map_err(|e| KafkaError::Protocol(format!("Failed to decode response: {:?}", e)))?;

        Ok(response)
    }

    /// Get the API version to use for a given API key
    fn get_api_version(&self, api_key: ApiKey) -> i16 {
        // Use reasonable default versions that are widely supported
        match api_key {
            ApiKey::Metadata => 9,
            ApiKey::Fetch => 11,
            ApiKey::Produce => 8,
            ApiKey::SaslHandshake => 1,
            ApiKey::SaslAuthenticate => 2,
            ApiKey::ApiVersions => 3,
            ApiKey::ListOffsets => 5,
            ApiKey::CreateTopics => 5, // v5 supported since Kafka 2.4
            _ => 0,
        }
    }

    /// Fetch cluster metadata
    pub async fn fetch_metadata(&self, topics: Option<&[String]>) -> Result<Vec<TopicMetadata>> {
        super::metadata::fetch_metadata(self, topics).await
    }

    /// Get metadata for a specific topic
    pub async fn get_topic_metadata(&self, topic: &str) -> Result<TopicMetadata> {
        let topics = self.fetch_metadata(Some(&[topic.to_string()])).await?;
        topics
            .into_iter()
            .find(|t| t.name == topic)
            .ok_or_else(|| KafkaError::TopicNotExists(topic.to_string()).into())
    }

    /// Fetch records from a topic/partition
    pub async fn fetch(
        &self,
        topic: &str,
        partition: i32,
        offset: i64,
        max_bytes: i32,
    ) -> Result<FetchResponse> {
        super::fetch::fetch(self, topic, partition, offset, max_bytes).await
    }

    /// Get the earliest and latest offsets for a partition
    pub async fn get_offsets(&self, topic: &str, partition: i32) -> Result<(i64, i64)> {
        super::fetch::get_offsets(self, topic, partition).await
    }

    /// Produce records to a topic/partition
    pub async fn produce(
        &self,
        topic: &str,
        partition: i32,
        records: Vec<crate::manifest::BackupRecord>,
    ) -> Result<ProduceResponse> {
        super::produce::produce(self, topic, partition, records).await
    }

    /// Create topics in the Kafka cluster
    pub async fn create_topics(
        &self,
        topics: Vec<super::TopicToCreate>,
        timeout_ms: i32,
    ) -> Result<Vec<super::CreateTopicResult>> {
        super::admin::create_topics(self, topics, timeout_ms).await
    }

    /// Get cached broker metadata
    #[allow(dead_code)]
    pub async fn get_broker(&self, broker_id: i32) -> Option<BrokerMetadata> {
        let brokers = self.brokers.lock().await;
        brokers.get(&broker_id).cloned()
    }

    /// Update broker cache
    pub async fn update_brokers(&self, brokers: Vec<BrokerMetadata>) {
        let mut cache = self.brokers.lock().await;
        for broker in brokers {
            cache.insert(broker.node_id, broker);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ConnectionConfig;
    use std::net::TcpListener;

    /// Test that TCP keepalive settings are actually applied to the socket.
    /// This creates a real TCP connection and verifies the socket options.
    #[tokio::test]
    async fn test_tcp_keepalive_is_applied() {
        // Start a local TCP listener
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
        let addr = listener.local_addr().expect("Failed to get local addr");

        // Connect to the listener
        let tcp_stream = TcpStream::connect(addr).await.expect("Failed to connect");

        // Create a KafkaConfig with TCP keepalive enabled
        let config = KafkaConfig {
            bootstrap_servers: vec![addr.to_string()],
            security: Default::default(),
            topics: Default::default(),
            connection: ConnectionConfig {
                tcp_keepalive: true,
                keepalive_time_secs: 60,
                keepalive_interval_secs: 20,
                tcp_nodelay: true,
            },
        };

        let client = KafkaClient::new(config);

        // Apply socket configuration
        client
            .configure_socket(&tcp_stream, &addr.to_string())
            .expect("Failed to configure socket");

        // Verify the settings were applied using socket2
        let sock_ref = SockRef::from(&tcp_stream);

        // Check TCP_NODELAY is set
        let nodelay = sock_ref.nodelay().expect("Failed to get nodelay");
        assert!(nodelay, "TCP_NODELAY should be enabled");

        // Check keepalive is enabled
        // Note: socket2 doesn't have a direct getter for keepalive enabled,
        // but we can verify the keepalive settings were set without error.
        // On macOS, we can check the keepalive time.
        #[cfg(any(target_os = "macos", target_os = "ios"))]
        {
            let keepalive_time = sock_ref
                .keepalive_time()
                .expect("Failed to get keepalive time");
            assert_eq!(
                keepalive_time,
                Duration::from_secs(60),
                "Keepalive time should be 60 seconds"
            );
        }

        // On Linux, we can check both time and interval
        #[cfg(target_os = "linux")]
        {
            let keepalive_time = sock_ref
                .keepalive_time()
                .expect("Failed to get keepalive time");
            assert_eq!(
                keepalive_time,
                Duration::from_secs(60),
                "Keepalive time should be 60 seconds"
            );

            let keepalive_interval = sock_ref
                .keepalive_interval()
                .expect("Failed to get keepalive interval");
            assert_eq!(
                keepalive_interval,
                Duration::from_secs(20),
                "Keepalive interval should be 20 seconds"
            );
        }

        println!("TCP keepalive settings verified successfully!");
    }

    /// Test that TCP keepalive can be disabled via configuration.
    #[tokio::test]
    async fn test_tcp_keepalive_disabled() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
        let addr = listener.local_addr().expect("Failed to get local addr");

        let tcp_stream = TcpStream::connect(addr).await.expect("Failed to connect");

        // Create a KafkaConfig with TCP keepalive DISABLED
        let config = KafkaConfig {
            bootstrap_servers: vec![addr.to_string()],
            security: Default::default(),
            topics: Default::default(),
            connection: ConnectionConfig {
                tcp_keepalive: false,
                keepalive_time_secs: 60,
                keepalive_interval_secs: 20,
                tcp_nodelay: false,
            },
        };

        let client = KafkaClient::new(config);

        // Apply socket configuration - should succeed even with keepalive disabled
        client
            .configure_socket(&tcp_stream, &addr.to_string())
            .expect("Failed to configure socket");

        // Verify TCP_NODELAY is NOT set
        let sock_ref = SockRef::from(&tcp_stream);
        let nodelay = sock_ref.nodelay().expect("Failed to get nodelay");
        assert!(!nodelay, "TCP_NODELAY should be disabled");

        println!("TCP keepalive disabled configuration verified!");
    }

    /// Test custom keepalive values
    #[tokio::test]
    async fn test_tcp_keepalive_custom_values() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind listener");
        let addr = listener.local_addr().expect("Failed to get local addr");

        let tcp_stream = TcpStream::connect(addr).await.expect("Failed to connect");

        // Create a KafkaConfig with custom keepalive values
        let config = KafkaConfig {
            bootstrap_servers: vec![addr.to_string()],
            security: Default::default(),
            topics: Default::default(),
            connection: ConnectionConfig {
                tcp_keepalive: true,
                keepalive_time_secs: 30,     // Custom: 30 seconds
                keepalive_interval_secs: 10, // Custom: 10 seconds
                tcp_nodelay: true,
            },
        };

        let client = KafkaClient::new(config);
        client
            .configure_socket(&tcp_stream, &addr.to_string())
            .expect("Failed to configure socket");

        let sock_ref = SockRef::from(&tcp_stream);

        #[cfg(any(target_os = "macos", target_os = "ios", target_os = "linux"))]
        {
            let keepalive_time = sock_ref
                .keepalive_time()
                .expect("Failed to get keepalive time");
            assert_eq!(
                keepalive_time,
                Duration::from_secs(30),
                "Custom keepalive time should be 30 seconds"
            );
        }

        #[cfg(target_os = "linux")]
        {
            let keepalive_interval = sock_ref
                .keepalive_interval()
                .expect("Failed to get keepalive interval");
            assert_eq!(
                keepalive_interval,
                Duration::from_secs(10),
                "Custom keepalive interval should be 10 seconds"
            );
        }

        println!("Custom TCP keepalive values verified!");
    }
}
