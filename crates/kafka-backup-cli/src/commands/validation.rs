//! CLI commands for backup validation and evidence management.
//!
//! Evidence artifact creation and verification are centralized in
//! `kafka_backup_core::evidence` (issue #138): this module only loads
//! configuration, runs the checks, and prints results.

use std::sync::Arc;

use anyhow::{Context, Result};
use tracing::info;
use uuid::Uuid;

use kafka_backup_core::evidence::{self, EvidenceReportParams};
use kafka_backup_core::kafka::PartitionLeaderRouter;
use kafka_backup_core::manifest::BackupManifest;
use kafka_backup_core::notification::pagerduty::PagerDutyNotifier;
use kafka_backup_core::notification::slack::SlackNotifier;
use kafka_backup_core::notification::NotificationSender;
use kafka_backup_core::storage::create_backend;
use kafka_backup_core::validation::config::ValidationConfig;
use kafka_backup_core::validation::context::ValidationContext;
use kafka_backup_core::validation::{CheckOutcome, ValidationRunner};

/// Execute a full validation run: load manifest, connect to target, run checks, generate evidence.
pub async fn run(config_path: &str, pitr: Option<i64>, triggered_by: Option<&str>) -> Result<()> {
    let config_data = std::fs::read_to_string(config_path)
        .with_context(|| format!("Failed to read config file: {config_path}"))?;
    let mut config: ValidationConfig =
        serde_yaml::from_str(&config_data).with_context(|| "Failed to parse validation config")?;

    // Override from CLI args
    if let Some(ts) = pitr {
        config.pitr_timestamp = Some(ts);
    }
    if let Some(tb) = triggered_by {
        config.triggered_by = Some(tb.to_string());
    }

    let run_id = format!("validation-{}", Uuid::new_v4().as_hyphenated());
    info!(run_id = %run_id, backup_id = %config.backup_id, "Starting validation run");

    // Set up storage backend
    let storage = create_backend(&config.storage)?;

    // Load backup manifest
    let manifest_key = format!("{}/manifest.json", config.backup_id);
    let manifest_bytes = storage
        .get(&manifest_key)
        .await
        .with_context(|| format!("Failed to load manifest from {manifest_key}"))?;
    let manifest: BackupManifest = serde_json::from_slice(&manifest_bytes)
        .with_context(|| "Failed to parse backup manifest")?;
    let manifest_sha256 = evidence::sha256_hex(&manifest_bytes);

    info!(
        topics = manifest.topics.len(),
        total_records = manifest.total_records(),
        "Manifest loaded"
    );

    // Connect to restored Kafka cluster via PartitionLeaderRouter so that
    // ListOffsets requests are routed to the leader of each partition.
    let target_router = PartitionLeaderRouter::new(config.target.clone())
        .await
        .with_context(|| "Failed to connect to target Kafka cluster")?;

    // Build validation context
    let ctx = ValidationContext {
        backup_id: config.backup_id.clone(),
        backup_manifest: manifest.clone(),
        target_client: Arc::new(target_router),
        storage: storage.clone(),
        pitr_timestamp: config.pitr_timestamp,
        http_client: reqwest::Client::new(),
        target_bootstrap_servers: config.target.bootstrap_servers.clone(),
    };

    // Run validation checks
    let runner = ValidationRunner::from_config(&config.checks);
    let summary = runner.run_all(&ctx).await?;

    println!("\n=== Validation Results ===");
    println!("Overall: {}", summary.overall_result);
    println!(
        "Checks: {}/{} passed, {} failed, {} skipped",
        summary.checks_passed, summary.checks_total, summary.checks_failed, summary.checks_skipped
    );
    println!("Duration: {}ms\n", summary.total_duration_ms);

    for result in &summary.results {
        println!(
            "  [{}] {} — {}",
            result.outcome, result.check_name, result.detail
        );
    }

    // Build and emit evidence artifacts through the shared core pipeline.
    let report = evidence::build_evidence_report(EvidenceReportParams {
        run_id: &run_id,
        tool_version: env!("CARGO_PKG_VERSION"),
        backup_id: &config.backup_id,
        manifest: &manifest,
        manifest_sha256,
        storage_backend: format!("{:?}", config.storage),
        pitr_timestamp: config.pitr_timestamp,
        target_bootstrap_servers: config.target.bootstrap_servers.clone(),
        summary: summary.clone(),
        retention_days: config.evidence.storage.retention_days,
        signing_enabled: config.evidence.signing.enabled,
        triggered_by: config.triggered_by.clone(),
    });

    let emission = evidence::emit_evidence(storage.as_ref(), &report, &config.evidence)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to emit evidence: {e}"))?;

    for warning in &emission.warnings {
        eprintln!("Warning: {warning}");
    }
    println!(
        "\nReport SHA-256 (stored JSON bytes): {}",
        emission.report_sha256
    );
    let mut evidence_url = String::new();
    if let Some(key) = &emission.json_key {
        evidence_url = key.clone();
        println!("JSON evidence uploaded: {key}");
    }
    if let Some(key) = &emission.pdf_key {
        println!("PDF evidence uploaded: {key}");
    }
    if let Some(key) = &emission.signature_key {
        println!("Signature envelope uploaded: {key}");
    }

    // Send notifications
    if let Some(ref notif) = config.notifications {
        let senders = build_notification_senders(notif);
        for sender in &senders {
            let result = if summary.overall_result == CheckOutcome::Passed {
                sender.send_success(&report, &evidence_url).await
            } else {
                sender.send_failure(&report, &evidence_url).await
            };
            if let Err(e) = result {
                eprintln!("Warning: Notification failed: {e}");
            }
        }
    }

    // Exit with appropriate code
    if summary.overall_result == CheckOutcome::Failed {
        std::process::exit(1);
    }

    Ok(())
}

/// List evidence reports in storage.
pub async fn evidence_list(path: &str, limit: usize) -> Result<()> {
    let storage_config = kafka_backup_core::storage::StorageBackendConfig::from_url(path)?;
    let storage = create_backend(&storage_config)?;

    let reports = kafka_backup_core::evidence::storage::list_evidence_reports(
        storage.as_ref(),
        "evidence-reports/",
    )
    .await?;

    if reports.is_empty() {
        println!("No evidence reports found.");
        return Ok(());
    }

    println!("Evidence reports ({} found):", reports.len());
    for key in reports.iter().take(limit) {
        println!("  {key}");
    }

    Ok(())
}

/// Download an evidence report from storage.
pub async fn evidence_get(path: &str, report_id: &str, format: &str, output: &str) -> Result<()> {
    let storage_config = kafka_backup_core::storage::StorageBackendConfig::from_url(path)?;
    let storage = create_backend(&storage_config)?;

    let ext = match format {
        "pdf" => "pdf",
        "sig" => "sig",
        _ => "json",
    };

    // Search for the report key
    let prefix = format!("evidence-reports/{report_id}/");
    let keys = storage.list(&prefix).await?;
    let key = keys
        .iter()
        .find(|k| k.ends_with(&format!(".{ext}")))
        .ok_or_else(|| anyhow::anyhow!("No {ext} report found for {report_id}"))?;

    let data =
        kafka_backup_core::evidence::storage::download_evidence_report(storage.as_ref(), key)
            .await?;
    std::fs::write(output, &data)
        .with_context(|| format!("Failed to write output file: {output}"))?;

    println!("Evidence report saved to: {output}");
    Ok(())
}

/// Verify an evidence report against its detached signature artifact.
///
/// Supports both the v2 JSON envelope and the legacy v1 text `.sig` format;
/// the version is detected from the artifact content. The stored report
/// bytes are verified exactly as read — no re-serialization.
pub async fn evidence_verify(
    report_path: &str,
    signature_path: &str,
    public_key_path: Option<&str>,
) -> Result<()> {
    let report_bytes = std::fs::read(report_path)
        .with_context(|| format!("Failed to read report: {report_path}"))?;
    let sig_content = std::fs::read_to_string(signature_path)
        .with_context(|| format!("Failed to read signature: {signature_path}"))?;

    let public_pem = match public_key_path {
        Some(path) => Some(
            std::fs::read_to_string(path)
                .with_context(|| format!("Failed to read public key: {path}"))?,
        ),
        None => None,
    };

    let outcome = evidence::verify_evidence(&report_bytes, &sig_content, public_pem.as_deref())
        .map_err(|e| anyhow::anyhow!("Verification error: {e}"))?;

    println!("Evidence contract: {}", outcome.contract);
    println!("Report ID: {}", outcome.report_id);
    println!("Algorithm: {}", outcome.algorithm);
    if let Some(schema) = &outcome.report_schema_version {
        println!("Report schema version: {schema}");
    }
    println!("Expected SHA-256: {}", outcome.expected_sha256);
    println!("Actual SHA-256:   {}", outcome.actual_sha256);
    println!(
        "SHA-256 digest: {}",
        if outcome.digest_valid {
            "VALID"
        } else {
            "MISMATCH"
        }
    );
    if outcome.signature_checked {
        println!(
            "ECDSA signature: {}",
            if outcome.signature_valid {
                "VALID"
            } else {
                "INVALID"
            }
        );
    } else {
        println!("(No public key provided — skipping signature verification)");
    }
    for message in &outcome.messages {
        println!("  note: {message}");
    }

    if !outcome.verified() {
        eprintln!("\nFAILED: evidence report integrity could NOT be verified");
        std::process::exit(1);
    }

    println!("\nEvidence report integrity: VERIFIED");
    Ok(())
}

fn build_notification_senders(
    config: &kafka_backup_core::validation::config::NotificationsConfig,
) -> Vec<Box<dyn NotificationSender>> {
    let mut senders: Vec<Box<dyn NotificationSender>> = Vec::new();

    if let Some(ref slack) = config.slack {
        senders.push(Box::new(SlackNotifier::new(slack.webhook_url.clone())));
    }
    if let Some(ref pd) = config.pagerduty {
        senders.push(Box::new(PagerDutyNotifier::new(
            pd.integration_key.clone(),
            pd.severity.clone(),
        )));
    }

    senders
}
