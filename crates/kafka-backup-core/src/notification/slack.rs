//! Slack webhook notification sender using Block Kit format.

use async_trait::async_trait;
use tracing::{info, warn};

use super::NotificationSender;
use crate::evidence::EvidenceReport;
use crate::Result;

pub struct SlackNotifier {
    webhook_url: String,
    client: reqwest::Client,
}

impl SlackNotifier {
    pub fn new(webhook_url: String) -> Self {
        Self {
            webhook_url,
            client: reqwest::Client::new(),
        }
    }

    async fn send(&self, payload: serde_json::Value) -> Result<()> {
        let resp = self
            .client
            .post(&self.webhook_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| crate::Error::Notification(format!("Slack request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(status = %status, body = %body, "Slack webhook returned error");
            return Err(crate::Error::Notification(format!(
                "Slack webhook returned {status}: {body}"
            )));
        }

        info!("Slack notification sent successfully");
        Ok(())
    }

    fn build_payload(
        report: &EvidenceReport,
        evidence_url: &str,
        emoji: &str,
    ) -> serde_json::Value {
        let result_text = format!(
            "{emoji} Kafka Backup Validation {}",
            report.validation.overall_result
        );

        serde_json::json!({
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": result_text
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        { "type": "mrkdwn", "text": format!("*Backup ID:*\n{}", report.backup.id) },
                        { "type": "mrkdwn", "text": format!("*Result:*\n{} ({}/{} checks)", report.validation.overall_result, report.validation.checks_passed, report.validation.checks_total) },
                        { "type": "mrkdwn", "text": format!("*Duration:*\n{}ms", report.validation.total_duration_ms) },
                        { "type": "mrkdwn", "text": format!("*Evidence:*\n<{}|Download Report>", evidence_url) }
                    ]
                }
            ]
        })
    }
}

#[async_trait]
impl NotificationSender for SlackNotifier {
    async fn send_success(&self, report: &EvidenceReport, evidence_url: &str) -> Result<()> {
        let payload = Self::build_payload(report, evidence_url, "\u{2705}");
        self.send(payload).await
    }

    async fn send_failure(&self, report: &EvidenceReport, evidence_url: &str) -> Result<()> {
        let payload = Self::build_payload(report, evidence_url, "\u{274c}");
        self.send(payload).await
    }
}
