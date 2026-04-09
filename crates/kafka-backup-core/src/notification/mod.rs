//! Notification delivery for validation results (Slack, PagerDuty).

pub mod pagerduty;
pub mod slack;

use async_trait::async_trait;

use crate::evidence::EvidenceReport;
use crate::Result;

/// Trait for sending validation result notifications.
#[async_trait]
pub trait NotificationSender: Send + Sync {
    /// Send a notification for a successful validation run.
    async fn send_success(&self, report: &EvidenceReport, evidence_url: &str) -> Result<()>;

    /// Send a notification for a failed validation run.
    async fn send_failure(&self, report: &EvidenceReport, evidence_url: &str) -> Result<()>;
}
