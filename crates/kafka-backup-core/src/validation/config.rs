//! Configuration structures for backup validation.

use serde::{Deserialize, Serialize};

use crate::config::KafkaConfig;
use crate::storage::StorageBackendConfig;

/// Top-level validation configuration, loaded from YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationConfig {
    /// Backup ID to validate against.
    pub backup_id: String,

    /// Storage backend where the backup lives.
    pub storage: StorageBackendConfig,

    /// Target Kafka cluster (the restored cluster to validate).
    pub target: KafkaConfig,

    /// Which checks to run and their parameters.
    #[serde(default)]
    pub checks: ChecksConfig,

    /// Evidence report generation settings.
    #[serde(default)]
    pub evidence: EvidenceConfig,

    /// Optional notification settings (Slack, PagerDuty).
    #[serde(default)]
    pub notifications: Option<NotificationsConfig>,

    /// PITR timestamp used during restore (epoch milliseconds), if any.
    #[serde(default)]
    pub pitr_timestamp: Option<i64>,

    /// Human-readable string recording who/what triggered this run.
    #[serde(default)]
    pub triggered_by: Option<String>,
}

/// Configuration for all validation checks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChecksConfig {
    #[serde(default)]
    pub message_count: MessageCountConfig,

    #[serde(default)]
    pub offset_range: OffsetRangeConfig,

    #[serde(default)]
    pub consumer_group_offsets: ConsumerGroupConfig,

    #[serde(default)]
    pub custom_webhooks: Vec<WebhookConfig>,
}

/// Mode for message count comparison.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CountMode {
    /// Every partition must match exactly.
    #[default]
    Exact,
    /// Validate a random sample of partitions.
    Sample,
}

/// Configuration for the message count check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageCountConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,

    #[serde(default)]
    pub mode: CountMode,

    /// Percentage of partitions to sample (1-100). Only used in Sample mode.
    #[serde(default = "default_100")]
    pub sample_percentage: u8,

    /// Topic filter — empty means all topics.
    #[serde(default)]
    pub topics: Vec<String>,

    /// Number of records difference allowed before failing. 0 = exact match.
    #[serde(default)]
    pub fail_threshold: u64,
}

impl Default for MessageCountConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            mode: CountMode::Exact,
            sample_percentage: 100,
            topics: Vec::new(),
            fail_threshold: 0,
        }
    }
}

/// Configuration for the offset range check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetRangeConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Verify high watermarks match.
    #[serde(default = "default_true")]
    pub verify_high_watermark: bool,

    /// Verify low watermarks match.
    #[serde(default = "default_true")]
    pub verify_low_watermark: bool,
}

impl Default for OffsetRangeConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            verify_high_watermark: true,
            verify_low_watermark: true,
        }
    }
}

/// Configuration for consumer group offset check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// If true, verify all consumer groups found. If false, only check groups listed below.
    #[serde(default = "default_true")]
    pub verify_all_groups: bool,

    /// Specific consumer groups to check. Empty + verify_all_groups=true means all.
    #[serde(default)]
    pub groups: Vec<String>,
}

impl Default for ConsumerGroupConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            verify_all_groups: true,
            groups: Vec::new(),
        }
    }
}

/// Configuration for a custom webhook validation check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Display name for this check in the report.
    pub name: String,

    /// URL to POST the validation payload to.
    pub url: String,

    /// Timeout in seconds.
    #[serde(default = "default_webhook_timeout")]
    pub timeout_seconds: u64,

    /// Expected HTTP status code for success.
    #[serde(default = "default_200")]
    pub expected_status_code: u16,

    /// Whether to treat a timeout as failure.
    #[serde(default = "default_true")]
    pub fail_on_timeout: bool,
}

/// Evidence report generation settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceConfig {
    /// Output formats to generate.
    #[serde(default = "default_evidence_formats")]
    pub formats: Vec<EvidenceFormat>,

    /// Cryptographic signing configuration.
    #[serde(default)]
    pub signing: SigningConfig,

    /// Object storage settings for evidence upload.
    #[serde(default)]
    pub storage: EvidenceStorageConfig,
}

impl Default for EvidenceConfig {
    fn default() -> Self {
        Self {
            formats: default_evidence_formats(),
            signing: SigningConfig::default(),
            storage: EvidenceStorageConfig::default(),
        }
    }
}

/// Supported evidence output formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EvidenceFormat {
    Json,
    Pdf,
}

/// Cryptographic signing configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SigningConfig {
    #[serde(default)]
    pub enabled: bool,

    /// Path to a PEM-encoded ECDSA P-256 private key.
    #[serde(default)]
    pub private_key_path: Option<String>,

    /// Path to the corresponding PEM-encoded public key or certificate.
    #[serde(default)]
    pub public_key_path: Option<String>,
}

/// Storage location for evidence reports.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvidenceStorageConfig {
    /// Prefix under which evidence is stored.
    #[serde(default = "default_evidence_prefix")]
    pub prefix: String,

    /// Retention in days (default: 2555 = ~7 years for SOX).
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
}

impl Default for EvidenceStorageConfig {
    fn default() -> Self {
        Self {
            prefix: default_evidence_prefix(),
            retention_days: default_retention_days(),
        }
    }
}

/// Notification configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationsConfig {
    #[serde(default)]
    pub slack: Option<SlackConfig>,

    #[serde(default)]
    pub pagerduty: Option<PagerDutyConfig>,
}

/// Slack webhook configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackConfig {
    pub webhook_url: String,
}

/// PagerDuty integration configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PagerDutyConfig {
    pub integration_key: String,

    #[serde(default = "default_pagerduty_severity")]
    pub severity: String,
}

// Default helpers

fn default_true() -> bool {
    true
}

fn default_100() -> u8 {
    100
}

fn default_200() -> u16 {
    200
}

fn default_webhook_timeout() -> u64 {
    120
}

fn default_evidence_formats() -> Vec<EvidenceFormat> {
    vec![EvidenceFormat::Json]
}

fn default_evidence_prefix() -> String {
    "evidence-reports/".to_string()
}

fn default_retention_days() -> u32 {
    2555 // ~7 years (SOX requirement)
}

fn default_pagerduty_severity() -> String {
    "critical".to_string()
}
