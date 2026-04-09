//! CustomWebhookCheck — calls an external HTTP endpoint for custom validation logic.

use async_trait::async_trait;
use std::time::{Duration, Instant};
use tracing::{debug, warn};

use super::config::WebhookConfig;
use super::context::ValidationContext;
use super::{CheckOutcome, ValidationCheck, ValidationResult};
use crate::Result;

/// Calls a user-configured webhook URL, passing restored cluster details,
/// and interprets the response as a pass/fail result.
pub struct CustomWebhookCheck {
    config: WebhookConfig,
}

impl CustomWebhookCheck {
    pub fn new(config: WebhookConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl ValidationCheck for CustomWebhookCheck {
    fn name(&self) -> &str {
        &self.config.name
    }

    fn description(&self) -> &str {
        "Calls an external webhook for custom validation logic"
    }

    async fn run(&self, ctx: &ValidationContext) -> Result<ValidationResult> {
        let start = Instant::now();

        let payload = serde_json::json!({
            "event": "kafka_backup_validation",
            "backup_id": ctx.backup_id,
            "pitr_timestamp": ctx.pitr_timestamp,
            "restored_cluster": {
                "bootstrap_servers": ctx.target_bootstrap_servers,
            },
        });

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(self.config.timeout_seconds))
            .build()
            .map_err(|e| crate::Error::Validation(format!("Failed to create HTTP client: {e}")))?;

        debug!(url = %self.config.url, "Calling custom webhook");

        let response = match client.post(&self.config.url).json(&payload).send().await {
            Ok(resp) => resp,
            Err(e) if e.is_timeout() && self.config.fail_on_timeout => {
                return Ok(ValidationResult {
                    check_name: self.config.name.clone(),
                    outcome: CheckOutcome::Failed,
                    detail: format!("Webhook timed out after {}s", self.config.timeout_seconds),
                    data: serde_json::json!({"error": "timeout"}),
                    duration_ms: start.elapsed().as_millis() as u64,
                });
            }
            Err(e) => {
                return Ok(ValidationResult {
                    check_name: self.config.name.clone(),
                    outcome: CheckOutcome::Failed,
                    detail: format!("Webhook request failed: {e}"),
                    data: serde_json::json!({"error": e.to_string()}),
                    duration_ms: start.elapsed().as_millis() as u64,
                });
            }
        };

        let status = response.status().as_u16();
        if status != self.config.expected_status_code {
            warn!(
                url = %self.config.url,
                expected = self.config.expected_status_code,
                actual = status,
                "Webhook returned unexpected status"
            );
            return Ok(ValidationResult {
                check_name: self.config.name.clone(),
                outcome: CheckOutcome::Failed,
                detail: format!(
                    "Expected status {}, got {status}",
                    self.config.expected_status_code
                ),
                data: serde_json::json!({"status_code": status}),
                duration_ms: start.elapsed().as_millis() as u64,
            });
        }

        // Parse the response body for result/detail/data
        let body: serde_json::Value = response
            .json()
            .await
            .unwrap_or_else(|_| serde_json::json!({}));

        let result_str = body
            .get("result")
            .and_then(|v| v.as_str())
            .unwrap_or("passed");
        let detail = body
            .get("detail")
            .and_then(|v| v.as_str())
            .unwrap_or("Webhook returned success")
            .to_string();
        let data = body.get("data").cloned().unwrap_or(serde_json::json!({}));

        let outcome = match result_str {
            "passed" => CheckOutcome::Passed,
            "failed" => CheckOutcome::Failed,
            "warning" => CheckOutcome::Warning,
            "skipped" => CheckOutcome::Skipped,
            _ => CheckOutcome::Passed,
        };

        Ok(ValidationResult {
            check_name: self.config.name.clone(),
            outcome,
            detail,
            data,
            duration_ms: start.elapsed().as_millis() as u64,
        })
    }
}
