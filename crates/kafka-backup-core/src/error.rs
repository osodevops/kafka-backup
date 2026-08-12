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

    /// Validation check error
    #[error("Validation error: {0}")]
    Validation(String),

    /// Restore preflight failed before any target mutation
    #[error("Preflight failed: {0}")]
    Preflight(String),

    /// Evidence generation or signing error
    #[error("Evidence error: {0}")]
    Evidence(String),

    /// Notification delivery error
    #[error("Notification error: {0}")]
    Notification(String),
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

    /// I/O failure on an established broker connection: sending a request,
    /// reading a response, or one of our own request timeouts.
    ///
    /// Carries the [`std::io::ErrorKind`] and raw OS error code so callers can
    /// decide whether the connection is lost without parsing the OS message —
    /// which is localized (`FormatMessageW` on Windows, `strerror_r` on Unix)
    /// and differs per platform (issue #146). Use
    /// [`crate::kafka::is_connection_error`] rather than matching on `message`.
    #[error("Connection error during {operation} ({kind:?}): {message}")]
    ConnectionIo {
        /// What the client was doing, e.g. `"send request"`.
        operation: String,
        /// `io::Error::kind()`; `TimedOut` for the client's own request timeouts.
        kind: std::io::ErrorKind,
        /// `io::Error::raw_os_error()` when the OS supplied one (errno / Winsock code).
        raw_os_error: Option<i32>,
        /// The OS or library message, for logs and operators.
        message: String,
    },

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
