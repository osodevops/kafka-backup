//! Error types for the Kafka backup core library.

use thiserror::Error;

/// Result type alias using the library's Error type.
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for the Kafka backup library.
#[derive(Error, Debug)]
pub enum Error {
    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Kafka protocol error
    #[error("Kafka error: {0}")]
    Kafka(#[from] KafkaError),

    /// Storage error
    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    /// Compression error
    #[error("Compression error: {0}")]
    Compression(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// IO error
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Manifest error
    #[error("Manifest error: {0}")]
    Manifest(String),

    /// Topic not found
    #[error("Topic not found: {0}")]
    TopicNotFound(String),

    /// Backup not found
    #[error("Backup not found: {0}")]
    BackupNotFound(String),

    /// Connection error
    #[error("Connection error: {0}")]
    Connection(String),

    /// Authentication error
    #[error("Authentication error: {0}")]
    Authentication(String),
}

/// Kafka-specific errors
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum KafkaError {
    /// Connection failed
    #[error("Failed to connect to broker {broker}: {message}")]
    ConnectionFailed { broker: String, message: String },

    /// Protocol error
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// Broker error response
    #[error("Broker returned error code {code}: {message}")]
    BrokerError { code: i16, message: String },

    /// Timeout
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// No available brokers
    #[error("No available brokers")]
    NoBrokersAvailable,

    /// Topic does not exist
    #[error("Topic does not exist: {0}")]
    TopicNotExists(String),

    /// Partition not available
    #[error("Partition {partition} not available for topic {topic}")]
    PartitionNotAvailable { topic: String, partition: i32 },

    /// TLS configuration error
    #[error("TLS configuration error: {0}")]
    TlsConfig(String),

    /// Certificate loading error
    #[error("Failed to load certificate from {path}: {message}")]
    CertificateLoad { path: String, message: String },

    /// Private key loading error
    #[error("Failed to load private key from {path}: {message}")]
    PrivateKeyLoad { path: String, message: String },
}

/// Storage-specific errors
#[derive(Error, Debug)]
pub enum StorageError {
    /// Object not found
    #[error("Object not found: {0}")]
    NotFound(String),

    /// Permission denied
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    /// Storage backend error
    #[error("Backend error: {0}")]
    Backend(String),

    /// Invalid path
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<serde_yaml::Error> for Error {
    fn from(err: serde_yaml::Error) -> Self {
        Error::Serialization(err.to_string())
    }
}

impl From<sqlx::Error> for Error {
    fn from(err: sqlx::Error) -> Self {
        Error::Storage(StorageError::Backend(err.to_string()))
    }
}
