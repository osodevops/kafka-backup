//! PagerDuty Events API v2 notification sender.

use async_trait::async_trait;
use tracing::{info, warn};

use super::NotificationSender;
use crate::evidence::EvidenceReport;
use crate::Result;

const PAGERDUTY_EVENTS_URL: &str = "https://events.pagerduty.com/v2/enqueue";

pub struct PagerDutyNotifier {
    integration_key: String,
    severity: String,
    client: reqwest::Client,
}

impl PagerDutyNotifier {
    pub fn new(integration_key: String, severity: String) -> Self {
        Self {
            integration_key,
            severity,
            client: reqwest::Client::new(),
        }
    }

    async fn send_event(&self, payload: serde_json::Value) -> Result<()> {
        let resp = self
            .client
            .post(PAGERDUTY_EVENTS_URL)
            .json(&payload)
            .send()
            .await
            .map_err(|e| crate::Error::Notification(format!("PagerDuty request failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            warn!(status = %status, body = %body, "PagerDuty API returned error");
            return Err(crate::Error::Notification(format!(
                "PagerDuty API returned {status}: {body}"
            )));
        }

        info!("PagerDuty event sent successfully");
        Ok(())
    }
}

#[async_trait]
impl NotificationSender for PagerDutyNotifier {
    async fn send_success(&self, report: &EvidenceReport, evidence_url: &str) -> Result<()> {
        let payload = serde_json::json!({
            "routing_key": self.integration_key,
            "event_action": "resolve",
            "dedup_key": format!("kafka-backup-validation-{}", report.backup.id),
            "payload": {
                "summary": format!("Kafka backup validation PASSED: {}", report.report_id),
                "severity": "info",
                "source": "kafka-backup",
                "component": report.report_id,
                "custom_details": {
                    "checks_passed": report.validation.checks_passed,
                    "checks_total": report.validation.checks_total,
                    "duration_ms": report.validation.total_duration_ms,
                    "evidence_url": evidence_url,
                }
            },
            "links": [{
                "href": evidence_url,
                "text": "Evidence Report"
            }]
        });

        self.send_event(payload).await
    }

    async fn send_failure(&self, report: &EvidenceReport, evidence_url: &str) -> Result<()> {
        let payload = serde_json::json!({
            "routing_key": self.integration_key,
            "event_action": "trigger",
            "dedup_key": format!("kafka-backup-validation-{}", report.backup.id),
            "payload": {
                "summary": format!("Kafka backup validation FAILED: {}", report.report_id),
                "severity": self.severity,
                "source": "kafka-backup",
                "component": report.report_id,
                "custom_details": {
                    "checks_passed": report.validation.checks_passed,
                    "checks_failed": report.validation.checks_failed,
                    "checks_total": report.validation.checks_total,
                    "duration_ms": report.validation.total_duration_ms,
                    "evidence_url": evidence_url,
                    "failed_checks": report.validation.results.iter()
                        .filter(|r| r.outcome == crate::validation::CheckOutcome::Failed)
                        .map(|r| format!("{}: {}", r.check_name, r.detail))
                        .collect::<Vec<_>>(),
                }
            },
            "links": [{
                "href": evidence_url,
                "text": "Evidence Report"
            }]
        });

        self.send_event(payload).await
    }
}
