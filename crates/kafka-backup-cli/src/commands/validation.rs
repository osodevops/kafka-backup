//! CLI commands for backup validation and evidence management.

use std::sync::Arc;

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::info;
use uuid::Uuid;

use kafka_backup_core::config::KafkaConfig;
use kafka_backup_core::evidence::report::{
    hex_encode, BackupInfo, EvidenceReport, IntegrityInfo, RestoreInfo, ToolInfo,
};
use kafka_backup_core::evidence::{pdf, signing, storage as evidence_storage};
use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::manifest::BackupManifest;
use kafka_backup_core::notification::pagerduty::PagerDutyNotifier;
use kafka_backup_core::notification::slack::SlackNotifier;
use kafka_backup_core::notification::NotificationSender;
use kafka_backup_core::storage::create_backend;
use kafka_backup_core::validation::config::{EvidenceFormat, ValidationConfig};
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

    // Compute manifest SHA-256
    let manifest_sha256 = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&manifest_bytes);
        hex_encode(&hasher.finalize())
    };

    info!(
        topics = manifest.topics.len(),
        total_records = manifest.total_records(),
        "Manifest loaded"
    );

    // Connect to restored Kafka cluster
    let target_client = create_kafka_client(&config.target).await?;

    // Build validation context
    let ctx = ValidationContext {
        backup_id: config.backup_id.clone(),
        backup_manifest: manifest.clone(),
        target_client: Arc::new(target_client),
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

    // Build evidence report
    let check_names: Vec<String> = summary
        .results
        .iter()
        .map(|r| r.check_name.clone())
        .collect();
    let total_partitions: usize = manifest.topics.iter().map(|t| t.partitions.len()).sum();

    let compliance = EvidenceReport::build_compliance_mappings(
        &check_names,
        config.evidence.storage.retention_days,
        None,
    );

    let mut report = EvidenceReport {
        schema_version: "1.0".to_string(),
        report_id: run_id.clone(),
        generated_at: Utc::now().to_rfc3339(),
        tool: ToolInfo {
            name: "kafka-backup".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        },
        backup: BackupInfo {
            id: config.backup_id.clone(),
            source_cluster_id: manifest.source_cluster_id.clone(),
            source_brokers: manifest.source_brokers.clone(),
            storage_backend: format!("{:?}", config.storage),
            pitr_timestamp: config.pitr_timestamp,
            created_at: manifest.created_at,
            total_topics: manifest.topics.len(),
            total_partitions,
            total_segments: manifest.total_segments(),
            total_records: manifest.total_records(),
        },
        restore: Some(RestoreInfo {
            target_bootstrap_servers: config.target.bootstrap_servers.clone(),
            start_time: None,
            end_time: None,
            duration_seconds: None,
        }),
        validation: summary.clone(),
        integrity: IntegrityInfo {
            backup_manifest_sha256: manifest_sha256,
            report_sha256: String::new(), // Populated below
            checksums_valid: true,
            signature_algorithm: if config.evidence.signing.enabled {
                "ECDSA-P256-SHA256".to_string()
            } else {
                "none".to_string()
            },
            signed_by: None,
        },
        compliance_mappings: compliance,
        triggered_by: config.triggered_by.clone(),
    };

    // Compute report SHA-256
    let canonical = report
        .to_canonical_json()
        .map_err(|e| anyhow::anyhow!("Failed to serialize report: {e}"))?;
    let report_digest = report
        .sha256_digest()
        .map_err(|e| anyhow::anyhow!("Failed to compute report hash: {e}"))?;
    report.integrity.report_sha256 = hex_encode(&report_digest);

    let mut evidence_url = String::new();

    // Upload JSON evidence
    // When signing is enabled, store canonical JSON (matches what was signed).
    // Otherwise, store pretty-printed JSON for readability.
    if config.evidence.formats.contains(&EvidenceFormat::Json) {
        let json_bytes = if config.evidence.signing.enabled {
            canonical.clone()
        } else {
            report
                .to_pretty_json()
                .map_err(|e| anyhow::anyhow!("Failed to serialize report: {e}"))?
        };
        let key = evidence_storage::upload_evidence_json(
            storage.as_ref(),
            &config.evidence.storage.prefix,
            &run_id,
            &json_bytes,
        )
        .await?;
        evidence_url = key.clone();
        println!("\nJSON evidence uploaded: {key}");
    }

    // Upload PDF evidence
    if config.evidence.formats.contains(&EvidenceFormat::Pdf) {
        match pdf::generate_pdf(&report) {
            Ok(pdf_bytes) => {
                let key = evidence_storage::upload_evidence_pdf(
                    storage.as_ref(),
                    &config.evidence.storage.prefix,
                    &run_id,
                    &pdf_bytes,
                )
                .await?;
                println!("PDF evidence uploaded: {key}");
            }
            Err(e) => {
                eprintln!("Warning: PDF generation failed: {e}");
            }
        }
    }

    // Sign the report
    if config.evidence.signing.enabled {
        if let Some(ref key_path) = config.evidence.signing.private_key_path {
            let pem = std::fs::read_to_string(key_path)
                .with_context(|| format!("Failed to read signing key: {key_path}"))?;
            let bundle = signing::sign_report(&canonical, &pem, &run_id)
                .map_err(|e| anyhow::anyhow!("Signing failed: {e}"))?;
            let sig_content = bundle.to_sig_file();
            let key = evidence_storage::upload_evidence_signature(
                storage.as_ref(),
                &config.evidence.storage.prefix,
                &run_id,
                &sig_content,
            )
            .await?;
            println!("Signature uploaded: {key}");
        } else {
            eprintln!("Warning: Signing enabled but no private_key_path configured");
        }
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

    let reports =
        evidence_storage::list_evidence_reports(storage.as_ref(), "evidence-reports/").await?;

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
        _ => "json",
    };

    // Search for the report key
    let prefix = format!("evidence-reports/{report_id}/");
    let keys = storage.list(&prefix).await?;
    let key = keys
        .iter()
        .find(|k| k.ends_with(&format!(".{ext}")))
        .ok_or_else(|| anyhow::anyhow!("No {ext} report found for {report_id}"))?;

    let data = evidence_storage::download_evidence_report(storage.as_ref(), key).await?;
    std::fs::write(output, &data)
        .with_context(|| format!("Failed to write output file: {output}"))?;

    println!("Evidence report saved to: {output}");
    Ok(())
}

/// Verify an evidence report's detached signature.
pub async fn evidence_verify(
    report_path: &str,
    signature_path: &str,
    public_key_path: Option<&str>,
) -> Result<()> {
    let report_json = std::fs::read_to_string(report_path)
        .with_context(|| format!("Failed to read report: {report_path}"))?;
    let sig_content = std::fs::read_to_string(signature_path)
        .with_context(|| format!("Failed to read signature: {signature_path}"))?;

    let bundle = signing::SignatureBundle::from_sig_file(&sig_content)
        .map_err(|e| anyhow::anyhow!("Invalid signature file: {e}"))?;

    println!("Report ID: {}", bundle.report_id);
    println!("Algorithm: {}", bundle.algorithm);
    println!("Report SHA-256: {}", bundle.report_sha256);

    // Verify SHA-256 of report matches
    let actual_sha256 = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(report_json.as_bytes());
        hex_encode(&hasher.finalize())
    };

    if actual_sha256 != bundle.report_sha256 {
        eprintln!("FAILED: Report SHA-256 mismatch");
        eprintln!("  Expected: {}", bundle.report_sha256);
        eprintln!("  Actual:   {actual_sha256}");
        std::process::exit(1);
    }
    println!("SHA-256 checksum: VALID");

    // Verify signature if public key provided
    if let Some(pk_path) = public_key_path {
        let public_pem = std::fs::read_to_string(pk_path)
            .with_context(|| format!("Failed to read public key: {pk_path}"))?;
        let valid = signing::verify_report(report_json.as_bytes(), &bundle, &public_pem)
            .map_err(|e| anyhow::anyhow!("Verification error: {e}"))?;

        if valid {
            println!("ECDSA signature: VALID");
        } else {
            eprintln!("FAILED: ECDSA signature is INVALID");
            std::process::exit(1);
        }
    } else {
        println!("(No public key provided — skipping signature verification)");
    }

    println!("\nEvidence report integrity: VERIFIED");
    Ok(())
}

async fn create_kafka_client(config: &KafkaConfig) -> Result<KafkaClient> {
    let client = KafkaClient::new(config.clone());
    client
        .connect()
        .await
        .with_context(|| "Failed to connect to target Kafka cluster")?;
    Ok(client)
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
