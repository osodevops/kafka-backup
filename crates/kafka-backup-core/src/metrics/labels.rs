//! Label types for Prometheus metrics.
//!
//! This module defines the label types used for metrics dimensions,
//! following the prometheus-client crate patterns.

use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use std::fmt::Write;

/// Labels for consumer lag metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct LagLabels {
    pub topic: String,
    pub partition: String,
    pub backup_id: String,
}

impl LagLabels {
    pub fn new(topic: impl Into<String>, partition: i32, backup_id: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            partition: partition.to_string(),
            backup_id: backup_id.into(),
        }
    }
}

/// Labels for throughput metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ThroughputLabels {
    pub backup_id: String,
    pub topic: String,
}

impl ThroughputLabels {
    pub fn new(backup_id: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            backup_id: backup_id.into(),
            topic: topic.into(),
        }
    }
}

/// Labels for storage operation metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StorageLabels {
    pub backend: String,
    pub operation: String,
}

impl StorageLabels {
    pub fn new(backend: impl Into<String>, operation: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            operation: operation.into(),
        }
    }
}

/// Labels for storage bytes metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StorageBytesLabels {
    pub backend: String,
    pub backup_id: String,
}

impl StorageBytesLabels {
    pub fn new(backend: impl Into<String>, backup_id: impl Into<String>) -> Self {
        Self {
            backend: backend.into(),
            backup_id: backup_id.into(),
        }
    }
}

/// Labels for storage error metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StorageErrorLabels {
    pub backend: String,
    pub error_type: String,
}

impl StorageErrorLabels {
    pub fn new(backend: impl Into<String>, error_type: ErrorType) -> Self {
        Self {
            backend: backend.into(),
            error_type: error_type.as_str().to_string(),
        }
    }
}

/// Labels for error metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ErrorLabels {
    pub backup_id: String,
    pub error_type: String,
}

impl ErrorLabels {
    pub fn new(backup_id: impl Into<String>, error_type: ErrorType) -> Self {
        Self {
            backup_id: backup_id.into(),
            error_type: error_type.as_str().to_string(),
        }
    }
}

/// Labels for retry metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RetryLabels {
    pub backup_id: String,
    pub operation: String,
}

impl RetryLabels {
    pub fn new(backup_id: impl Into<String>, operation: impl Into<String>) -> Self {
        Self {
            backup_id: backup_id.into(),
            operation: operation.into(),
        }
    }
}

/// Labels for restore operation metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RestoreLabels {
    pub restore_id: String,
    pub backup_id: String,
}

impl RestoreLabels {
    pub fn new(restore_id: impl Into<String>, backup_id: impl Into<String>) -> Self {
        Self {
            restore_id: restore_id.into(),
            backup_id: backup_id.into(),
        }
    }
}

/// Labels for restore progress metrics (without backup_id for simpler tracking).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RestoreProgressLabels {
    pub restore_id: String,
}

impl RestoreProgressLabels {
    pub fn new(restore_id: impl Into<String>) -> Self {
        Self {
            restore_id: restore_id.into(),
        }
    }
}

/// Labels for operation duration metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DurationLabels {
    pub backup_id: String,
    pub status: String,
}

impl DurationLabels {
    pub fn new(backup_id: impl Into<String>, status: OperationStatus) -> Self {
        Self {
            backup_id: backup_id.into(),
            status: status.as_str().to_string(),
        }
    }
}

/// Labels for compression metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CompressionLabels {
    pub algorithm: String,
    pub backup_id: String,
}

impl CompressionLabels {
    pub fn new(algorithm: impl Into<String>, backup_id: impl Into<String>) -> Self {
        Self {
            algorithm: algorithm.into(),
            backup_id: backup_id.into(),
        }
    }
}

/// Labels for backup-level metrics (backup_id only).
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BackupLabels {
    pub backup_id: String,
}

impl BackupLabels {
    pub fn new(backup_id: impl Into<String>) -> Self {
        Self {
            backup_id: backup_id.into(),
        }
    }
}

/// Labels for last successful commit metrics.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct CommitLabels {
    pub backup_id: String,
    pub topic: String,
    pub partition: String,
}

impl CommitLabels {
    pub fn new(backup_id: impl Into<String>, topic: impl Into<String>, partition: i32) -> Self {
        Self {
            backup_id: backup_id.into(),
            topic: topic.into(),
            partition: partition.to_string(),
        }
    }
}

/// Operation status for duration metrics.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum OperationStatus {
    Success,
    Failure,
}

impl OperationStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationStatus::Success => "success",
            OperationStatus::Failure => "failure",
        }
    }
}

impl EncodeLabelValue for OperationStatus {
    fn encode(
        &self,
        encoder: &mut prometheus_client::encoding::LabelValueEncoder,
    ) -> std::result::Result<(), std::fmt::Error> {
        encoder.write_str(self.as_str())
    }
}

/// Error type classification for metrics.
///
/// Categories based on PRD requirements:
/// - broker_connection: Connection failures to Kafka brokers
/// - consumer_timeout: Consumer poll/fetch timeouts
/// - deserialization: Message parsing/decoding errors
/// - storage_io: Storage backend I/O errors
/// - codec: Compression/decompression errors
/// - offset_invalid: Invalid offset errors
/// - auth: Authentication/authorization errors
/// - timeout: General operation timeouts
/// - not_found: Resource not found errors
/// - quota: Rate limiting/quota errors
/// - unknown: Unclassified errors
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum ErrorType {
    BrokerConnection,
    ConsumerTimeout,
    Deserialization,
    StorageIo,
    Codec,
    OffsetInvalid,
    Auth,
    Timeout,
    NotFound,
    Quota,
    Unknown,
}

impl ErrorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorType::BrokerConnection => "broker_connection",
            ErrorType::ConsumerTimeout => "consumer_timeout",
            ErrorType::Deserialization => "deserialization",
            ErrorType::StorageIo => "storage_io",
            ErrorType::Codec => "codec",
            ErrorType::OffsetInvalid => "offset_invalid",
            ErrorType::Auth => "auth",
            ErrorType::Timeout => "timeout",
            ErrorType::NotFound => "not_found",
            ErrorType::Quota => "quota",
            ErrorType::Unknown => "unknown",
        }
    }

    /// Classify an error into an ErrorType.
    pub fn from_error(error: &crate::Error) -> Self {
        match error {
            crate::Error::Kafka(kafka_err) => Self::classify_kafka_error(kafka_err),
            crate::Error::Storage(storage_err) => Self::classify_storage_error(storage_err),
            crate::Error::Compression(_) => ErrorType::Codec,
            crate::Error::Serialization(_) => ErrorType::Deserialization,
            crate::Error::Io(_) => ErrorType::StorageIo,
            crate::Error::Connection(_) => ErrorType::BrokerConnection,
            crate::Error::Authentication(_) => ErrorType::Auth,
            crate::Error::Config(_) => ErrorType::Unknown,
            crate::Error::Manifest(_) => ErrorType::Unknown,
            crate::Error::TopicNotFound(_) => ErrorType::NotFound,
            crate::Error::BackupNotFound(_) => ErrorType::NotFound,
        }
    }

    fn classify_kafka_error(error: &crate::error::KafkaError) -> Self {
        match error {
            crate::error::KafkaError::ConnectionFailed { .. } => ErrorType::BrokerConnection,
            crate::error::KafkaError::Protocol(_) => ErrorType::Unknown,
            crate::error::KafkaError::BrokerError { code, .. } => {
                // Classify based on Kafka error codes
                match *code {
                    1 => ErrorType::OffsetInvalid,    // OFFSET_OUT_OF_RANGE
                    3 => ErrorType::NotFound,         // UNKNOWN_TOPIC_OR_PARTITION
                    6 => ErrorType::BrokerConnection, // NOT_LEADER_FOR_PARTITION
                    7 => ErrorType::Timeout,          // REQUEST_TIMED_OUT
                    14 | 31 => ErrorType::Auth, // TOPIC_AUTHORIZATION_FAILED, SASL_AUTHENTICATION_FAILED
                    _ => ErrorType::Unknown,
                }
            }
            crate::error::KafkaError::Timeout(_) => ErrorType::Timeout,
            crate::error::KafkaError::NoBrokersAvailable => ErrorType::BrokerConnection,
            crate::error::KafkaError::TopicNotExists(_) => ErrorType::NotFound,
            crate::error::KafkaError::PartitionNotAvailable { .. } => ErrorType::NotFound,
            crate::error::KafkaError::TlsConfig(_) => ErrorType::Auth,
            crate::error::KafkaError::CertificateLoad { .. } => ErrorType::Auth,
            crate::error::KafkaError::PrivateKeyLoad { .. } => ErrorType::Auth,
        }
    }

    fn classify_storage_error(error: &crate::error::StorageError) -> Self {
        match error {
            crate::error::StorageError::NotFound(_) => ErrorType::NotFound,
            crate::error::StorageError::PermissionDenied(_) => ErrorType::Auth,
            crate::error::StorageError::Backend(msg) => {
                // Try to classify based on error message content
                let msg_lower = msg.to_lowercase();
                if msg_lower.contains("timeout") {
                    ErrorType::Timeout
                } else if msg_lower.contains("quota") || msg_lower.contains("rate limit") {
                    ErrorType::Quota
                } else {
                    ErrorType::StorageIo
                }
            }
            crate::error::StorageError::InvalidPath(_) => ErrorType::Unknown,
        }
    }
}

impl EncodeLabelValue for ErrorType {
    fn encode(
        &self,
        encoder: &mut prometheus_client::encoding::LabelValueEncoder,
    ) -> std::result::Result<(), std::fmt::Error> {
        encoder.write_str(self.as_str())
    }
}

/// Storage operation type for latency metrics.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum StorageOperation {
    Segment,
    Manifest,
    Checkpoint,
    Other,
}

impl StorageOperation {
    pub fn as_str(&self) -> &'static str {
        match self {
            StorageOperation::Segment => "segment",
            StorageOperation::Manifest => "manifest",
            StorageOperation::Checkpoint => "checkpoint",
            StorageOperation::Other => "other",
        }
    }

    /// Classify a storage key into an operation type.
    pub fn from_key(key: &str) -> Self {
        if key.ends_with(".zst")
            || key.ends_with(".lz4")
            || key.ends_with(".bin")
            || key.contains("/segment-")
        {
            StorageOperation::Segment
        } else if key.contains("manifest") {
            StorageOperation::Manifest
        } else if key.contains("offsets.db") || key.contains("checkpoint") {
            StorageOperation::Checkpoint
        } else {
            StorageOperation::Other
        }
    }
}

impl EncodeLabelValue for StorageOperation {
    fn encode(
        &self,
        encoder: &mut prometheus_client::encoding::LabelValueEncoder,
    ) -> std::result::Result<(), std::fmt::Error> {
        encoder.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lag_labels() {
        let labels = LagLabels::new("orders", 0, "backup-001");
        assert_eq!(labels.topic, "orders");
        assert_eq!(labels.partition, "0");
        assert_eq!(labels.backup_id, "backup-001");
    }

    #[test]
    fn test_error_type_as_str() {
        assert_eq!(ErrorType::BrokerConnection.as_str(), "broker_connection");
        assert_eq!(ErrorType::StorageIo.as_str(), "storage_io");
        assert_eq!(ErrorType::Auth.as_str(), "auth");
    }

    #[test]
    fn test_storage_operation_from_key() {
        assert_eq!(
            StorageOperation::from_key("backup-001/topics/orders/partition=0/segment-000001.zst"),
            StorageOperation::Segment
        );
        assert_eq!(
            StorageOperation::from_key("backup-001/manifest.json"),
            StorageOperation::Manifest
        );
        assert_eq!(
            StorageOperation::from_key("backup-001/offsets.db"),
            StorageOperation::Checkpoint
        );
        assert_eq!(
            StorageOperation::from_key("backup-001/random-file.txt"),
            StorageOperation::Other
        );
    }

    #[test]
    fn test_operation_status_as_str() {
        assert_eq!(OperationStatus::Success.as_str(), "success");
        assert_eq!(OperationStatus::Failure.as_str(), "failure");
    }
}
