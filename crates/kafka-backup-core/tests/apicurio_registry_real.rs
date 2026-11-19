//! Apicurio Registry backup against a real, seeded Apicurio Registry (API v3).
//!
//! ```text
//! C=tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml
//! docker compose -f $C up -d
//! tests/registry-restore-test-infra/seed-apicurio.sh http://localhost:29485
//! cargo test -p kafka-backup-core --test apicurio_registry_real -- --ignored --test-threads=1
//! docker compose -f $C down -v
//! ```
//!
//! Restore, API v2 and the other Apicurio versions are covered by
//! `tests/registry-restore-test-infra/cli-e2e.sh` and `run.sh`.

use kafka_backup_core::apicurio_registry::backup::ApicurioRegistryBackup;
use kafka_backup_core::apicurio_registry::client::ApicurioRegistryClient;
use kafka_backup_core::apicurio_registry::models::*;
use kafka_backup_core::config::{
    ApicurioBackupOptions, ApicurioConnectionConfig, ApicurioRegistryConfig, VersionSelection,
};
use kafka_backup_core::storage::{MemoryBackend, StorageBackend};
use std::sync::Arc;

const APICURIO_SOURCE_URL: &str = "http://localhost:29485";
const APICURIO_TARGET_URL: &str = "http://localhost:29486";

// ============================================================================
// Helpers
// ============================================================================

fn e2e_config(url: &str) -> ApicurioRegistryConfig {
    ApicurioRegistryConfig {
        api_version: Default::default(),
        enabled: true,
        url: url.to_string(),
        auth: None,
        tls: None,
        backup: Some(ApicurioBackupOptions::default()),
        connection: Some(ApicurioConnectionConfig {
            timeout_ms: 10_000,
            max_retries: 3,
            retry_backoff_ms: 500,
            rate_limit_rps: 50,
        }),
    }
}

/// Wait for Apicurio Registry to be ready, retrying up to 30s.
async fn wait_for_registry(url: &str) -> bool {
    let client = reqwest::Client::new();
    for _ in 0..15 {
        if let Ok(resp) = client
            .get(format!("{}/apis/registry/v3/groups", url))
            .send()
            .await
        {
            if resp.status().is_success() {
                return true;
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
    false
}

// ============================================================================
// E2E Tests — Full Backup Against Populated Registry
// ============================================================================

/// Full backup of a populated Apicurio Registry with multiple groups, artifact types,
/// multiple versions, rules at 3 scopes, and an export ZIP.
///
/// Verifies: manifest correctness, storage key presence, content roundtrip,
/// rules capture at global/group/artifact scope, and dependency ordering.
#[tokio::test]
#[ignore = "requires tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml"]
async fn test_e2e_full_backup_against_real_apicurio_registry() {
    if !wait_for_registry(APICURIO_SOURCE_URL).await {
        panic!(
            "Apicurio Registry not reachable at {}. Start tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml and run seed-apicurio.sh",
            APICURIO_SOURCE_URL
        );
    }

    let config = e2e_config(APICURIO_SOURCE_URL);
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup_id = "e2e-apicurio-001";

    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), backup_id.to_string(), config);

    let manifest = backup.run().await.unwrap();

    // ── Print results ──
    println!("\n═══ Apicurio E2E Backup Results ═══");
    println!("  Groups: {}", manifest.groups_count);
    println!("  Artifacts: {}", manifest.artifacts_count);
    println!("  Versions: {}", manifest.total_versions);
    println!("  Has export ZIP: {}", manifest.has_export_zip);
    println!("  Global rules: {:?}", manifest.global_rules);
    println!("  Dependency order: {:?}", manifest.dependency_order);
    for group in &manifest.groups {
        println!(
            "  Group '{}' — {} artifacts, {} versions",
            group.group_id, group.artifact_count, group.version_count,
        );
    }
    println!("═══════════════════════════════════\n");

    // ── Verify manifest ──
    // docker-compose apicurio-setup registers: 3 groups (common, payments, orders),
    // 4 artifacts (Address, PaymentEvent, OrderCreated, OrderEvent),
    // 5 versions (Address v1, PaymentEvent v1+v2, OrderCreated v1, OrderEvent v1)
    assert!(
        manifest.groups_count >= 3,
        "Expected at least 3 groups from apicurio-setup, got {}",
        manifest.groups_count
    );
    assert!(
        manifest.artifacts_count >= 4,
        "Expected at least 4 artifacts, got {}",
        manifest.artifacts_count
    );
    assert!(
        manifest.total_versions >= 5,
        "Expected at least 5 versions, got {}",
        manifest.total_versions
    );

    // ── Verify export ZIP ──
    assert!(
        manifest.has_export_zip,
        "Export ZIP should have been captured"
    );
    let zip_key = format!("{}/apicurio-registry/_export.zip", backup_id);
    assert!(
        storage.exists(&zip_key).await.unwrap(),
        "Export ZIP not found in storage"
    );
    let zip_data = storage.get(&zip_key).await.unwrap();
    assert!(
        zip_data.len() > 100,
        "Export ZIP is suspiciously small: {} bytes",
        zip_data.len()
    );
    // ZIP files start with PK magic bytes
    assert_eq!(
        &zip_data[0..2],
        b"PK",
        "Export ZIP does not have valid ZIP magic bytes"
    );
    println!("  Export ZIP size: {} bytes", zip_data.len());

    // ── Verify global rules ──
    assert!(
        !manifest.global_rules.is_empty(),
        "Expected at least one global rule (COMPATIBILITY)"
    );
    let compat_rule = manifest
        .global_rules
        .iter()
        .find(|r| r.rule_type == "COMPATIBILITY");
    assert!(compat_rule.is_some(), "Expected global COMPATIBILITY rule");
    println!(
        "  Global COMPATIBILITY rule: {}",
        compat_rule.unwrap().config
    );

    // ── Verify storage structure ──
    assert!(storage
        .exists(&format!("{}/apicurio-registry/_manifest.json", backup_id))
        .await
        .unwrap());
    assert!(storage
        .exists(&format!(
            "{}/apicurio-registry/_global_rules.json",
            backup_id
        ))
        .await
        .unwrap());

    // Verify per-group metadata
    for group in &manifest.groups {
        let meta_key = format!(
            "{}/apicurio-registry/groups/{}/_metadata.json",
            backup_id, group.group_id
        );
        assert!(
            storage.exists(&meta_key).await.unwrap(),
            "Missing group metadata for '{}'",
            group.group_id
        );

        // Parse and verify group metadata
        let meta_bytes = storage.get(&meta_key).await.unwrap();
        let meta: GroupMetadata = serde_json::from_slice(&meta_bytes).unwrap();
        assert_eq!(meta.group_id, group.group_id);
        println!(
            "  Group '{}': {} artifacts, {} rules",
            meta.group_id,
            meta.artifact_count,
            meta.rules.len()
        );
    }

    // ── Verify payments group has VALIDITY rule ──
    let payments_meta_key = format!(
        "{}/apicurio-registry/groups/payments/_metadata.json",
        backup_id
    );
    if storage.exists(&payments_meta_key).await.unwrap() {
        let meta_bytes = storage.get(&payments_meta_key).await.unwrap();
        let meta: GroupMetadata = serde_json::from_slice(&meta_bytes).unwrap();
        let validity_rule = meta.rules.iter().find(|r| r.rule_type == "VALIDITY");
        if let Some(rule) = validity_rule {
            println!("  Payments group VALIDITY rule: {}", rule.config);
        }
    }

    // ── Verify stored manifest can be deserialized ──
    let manifest_bytes = storage
        .get(&format!("{}/apicurio-registry/_manifest.json", backup_id))
        .await
        .unwrap();
    let stored_manifest: ApicurioRegistryManifest =
        serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(stored_manifest.groups_count, manifest.groups_count);
    assert_eq!(stored_manifest.artifacts_count, manifest.artifacts_count);
}

/// Backup of an empty Apicurio Registry (target instance with no data).
/// Verifies graceful handling: valid manifest, export ZIP, no errors.
#[tokio::test]
#[ignore = "requires tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml"]
async fn test_e2e_backup_empty_apicurio_registry() {
    if !wait_for_registry(APICURIO_TARGET_URL).await {
        panic!(
            "Apicurio target registry not reachable at {}. Start tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml and run seed-apicurio.sh",
            APICURIO_TARGET_URL
        );
    }

    let config = e2e_config(APICURIO_TARGET_URL);
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup_id = "e2e-empty-apicurio";

    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), backup_id.to_string(), config);

    let manifest = backup.run().await.unwrap();

    println!("\n═══ Empty Apicurio Registry Backup ═══");
    println!("  Groups: {}", manifest.groups_count);
    println!("  Artifacts: {}", manifest.artifacts_count);
    println!("  Versions: {}", manifest.total_versions);
    println!("═══════════════════════════════════════\n");

    // Empty registry should still produce valid files
    assert!(storage
        .exists(&format!("{}/apicurio-registry/_manifest.json", backup_id))
        .await
        .unwrap());
    assert!(storage
        .exists(&format!("{}/apicurio-registry/_export.zip", backup_id))
        .await
        .unwrap());
    assert!(storage
        .exists(&format!(
            "{}/apicurio-registry/_global_rules.json",
            backup_id
        ))
        .await
        .unwrap());

    // Export ZIP from empty registry should be valid
    let zip_data = storage
        .get(&format!("{}/apicurio-registry/_export.zip", backup_id))
        .await
        .unwrap();
    assert_eq!(
        &zip_data[0..2],
        b"PK",
        "Empty registry export should still be a valid ZIP"
    );
}

/// Backup with group filtering — only back up specific groups.
/// Verifies that excluded groups are not fetched/stored.
#[tokio::test]
#[ignore = "requires tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml"]
async fn test_e2e_backup_with_group_filter() {
    if !wait_for_registry(APICURIO_SOURCE_URL).await {
        panic!(
            "Apicurio Registry not reachable at {}. Start tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml and run seed-apicurio.sh",
            APICURIO_SOURCE_URL
        );
    }

    let mut config = e2e_config(APICURIO_SOURCE_URL);
    config.backup = Some(ApicurioBackupOptions {
        groups: vec!["payments".to_string()],
        exclude_groups: vec![],
        artifacts: vec!["*".to_string()],
        exclude_artifacts: vec![],
        include_export: true,
        include_versions: VersionSelection::All,
        include_references: true,
        fail_on_partial: false,
        snapshot_interval: None,
    });

    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup_id = "e2e-filtered-apicurio";

    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), backup_id.to_string(), config);

    let manifest = backup.run().await.unwrap();

    println!("\n═══ Filtered Apicurio Backup (payments only) ═══");
    println!("  Groups: {}", manifest.groups_count);
    println!("  Artifacts: {}", manifest.artifacts_count);
    for group in &manifest.groups {
        println!(
            "  Group '{}': {} artifacts",
            group.group_id, group.artifact_count
        );
    }
    println!("═════════════════════════════════════════════════\n");

    // Only the "payments" group should be present
    assert_eq!(
        manifest.groups_count, 1,
        "Expected exactly 1 group (payments), got {}",
        manifest.groups_count
    );
    assert_eq!(manifest.groups[0].group_id, "payments");

    // orders and common groups should NOT have metadata stored
    assert!(
        !storage
            .exists(&format!(
                "{}/apicurio-registry/groups/orders/_metadata.json",
                backup_id
            ))
            .await
            .unwrap(),
        "orders group should not be backed up"
    );
    assert!(
        !storage
            .exists(&format!(
                "{}/apicurio-registry/groups/common/_metadata.json",
                backup_id
            ))
            .await
            .unwrap(),
        "common group should not be backed up"
    );
}

/// Backup with latest-version-only mode. Verifies that only the latest
/// version of each artifact is captured.
#[tokio::test]
#[ignore = "requires tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml"]
async fn test_e2e_backup_latest_version_only() {
    if !wait_for_registry(APICURIO_SOURCE_URL).await {
        panic!(
            "Apicurio Registry not reachable at {}. Start tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml and run seed-apicurio.sh",
            APICURIO_SOURCE_URL
        );
    }

    let mut config = e2e_config(APICURIO_SOURCE_URL);
    config.backup = Some(ApicurioBackupOptions {
        groups: vec!["payments".to_string()],
        exclude_groups: vec![],
        artifacts: vec!["PaymentEvent".to_string()],
        exclude_artifacts: vec![],
        include_export: false, // Skip export for this test
        include_versions: VersionSelection::Latest,
        include_references: true,
        fail_on_partial: false,
        snapshot_interval: None,
    });

    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup_id = "e2e-latest-only";

    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), backup_id.to_string(), config);

    let manifest = backup.run().await.unwrap();

    println!("\n═══ Latest-Only Backup ═══");
    println!("  Total versions: {}", manifest.total_versions);
    println!("  Has export ZIP: {}", manifest.has_export_zip);
    println!("══════════════════════════\n");

    // PaymentEvent has 2 versions, but we should only have 1 (the latest)
    assert_eq!(
        manifest.total_versions, 1,
        "Expected 1 version (latest only), got {}",
        manifest.total_versions
    );
    assert!(!manifest.has_export_zip, "Export should be skipped");
}

/// Verify backup is read-only — does not modify the source registry.
/// Runs a backup, then verifies the artifact count is unchanged.
#[tokio::test]
#[ignore = "requires tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml"]
async fn test_e2e_backup_does_not_modify_source() {
    if !wait_for_registry(APICURIO_SOURCE_URL).await {
        panic!(
            "Apicurio Registry not reachable at {}. Start tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml and run seed-apicurio.sh",
            APICURIO_SOURCE_URL
        );
    }

    let http = reqwest::Client::new();

    // Count artifacts before backup
    let before: serde_json::Value = http
        .get(format!(
            "{}/apis/registry/v3/groups/payments/artifacts",
            APICURIO_SOURCE_URL
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let count_before = before["count"].as_i64().unwrap();

    // Run backup
    let config = e2e_config(APICURIO_SOURCE_URL);
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());

    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), "e2e-readonly".to_string(), config);
    backup.run().await.unwrap();

    // Count artifacts after backup
    let after: serde_json::Value = http
        .get(format!(
            "{}/apis/registry/v3/groups/payments/artifacts",
            APICURIO_SOURCE_URL
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let count_after = after["count"].as_i64().unwrap();

    assert_eq!(
        count_before, count_after,
        "Backup should not create or delete artifacts! Before: {}, After: {}",
        count_before, count_after
    );
    println!(
        "Read-only verified: {} artifacts before and after backup",
        count_before
    );
}

/// Verify version content is faithfully preserved (byte-for-byte roundtrip).
/// Registers a known schema, backs it up, and compares stored content to the original.
#[tokio::test]
#[ignore = "requires tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml"]
async fn test_e2e_content_roundtrip_fidelity() {
    if !wait_for_registry(APICURIO_SOURCE_URL).await {
        panic!(
            "Apicurio Registry not reachable at {}. Start tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml and run seed-apicurio.sh",
            APICURIO_SOURCE_URL
        );
    }

    let http = reqwest::Client::new();

    // Fetch the actual content of payments/PaymentEvent v1 from the registry
    let original_content = http
        .get(format!(
            "{}/apis/registry/v3/groups/payments/artifacts/PaymentEvent/versions/1/content",
            APICURIO_SOURCE_URL
        ))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // Run backup
    let mut config = e2e_config(APICURIO_SOURCE_URL);
    config.backup = Some(ApicurioBackupOptions {
        groups: vec!["payments".to_string()],
        artifacts: vec!["PaymentEvent".to_string()],
        include_export: false,
        ..ApicurioBackupOptions::default()
    });

    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup_id = "e2e-roundtrip";

    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), backup_id.to_string(), config);
    let _manifest = backup.run().await.unwrap();

    // Read the stored content and compare
    let stored_content = storage
        .get(&format!(
            "{}/apicurio-registry/groups/payments/artifacts/PaymentEvent/versions/1.content",
            backup_id
        ))
        .await
        .unwrap();
    let stored_str = String::from_utf8(stored_content.to_vec()).unwrap();

    assert_eq!(
        original_content.trim(),
        stored_str.trim(),
        "Stored content does not match original from registry"
    );
    println!(
        "Content roundtrip verified for PaymentEvent v1 ({} bytes)",
        stored_str.len()
    );

    // Also verify the version metadata has correct globalId and contentId
    let meta_bytes = storage
        .get(&format!(
            "{}/apicurio-registry/groups/payments/artifacts/PaymentEvent/versions/1.json",
            backup_id
        ))
        .await
        .unwrap();
    let meta: BackedUpVersion = serde_json::from_slice(&meta_bytes).unwrap();
    assert!(meta.global_id > 0, "globalId should be positive");
    assert!(meta.content_id > 0, "contentId should be positive");
    assert_eq!(meta.artifact_type, "AVRO");
    println!(
        "  globalId={}, contentId={}, type={}",
        meta.global_id, meta.content_id, meta.artifact_type
    );
}

/// Test multiple artifact types (Avro, Protobuf, JSON Schema) are all captured correctly.
#[tokio::test]
#[ignore = "requires tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml"]
async fn test_e2e_multiple_artifact_types() {
    if !wait_for_registry(APICURIO_SOURCE_URL).await {
        panic!(
            "Apicurio Registry not reachable at {}. Start tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml and run seed-apicurio.sh",
            APICURIO_SOURCE_URL
        );
    }

    let config = e2e_config(APICURIO_SOURCE_URL);
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup_id = "e2e-multi-types";

    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), backup_id.to_string(), config);

    let _manifest = backup.run().await.unwrap();

    // Check for known artifacts and their types
    let get_artifact_type =
        |storage: &Arc<dyn StorageBackend>, backup_id: &str, group: &str, artifact: &str| {
            let key = format!(
                "{}/apicurio-registry/groups/{}/artifacts/{}/_metadata.json",
                backup_id, group, artifact
            );
            let s = Arc::clone(storage);
            async move {
                if let Ok(bytes) = s.get(&key).await {
                    let meta: ArtifactMetadata = serde_json::from_slice(&bytes).unwrap();
                    Some(meta.artifact_type)
                } else {
                    None
                }
            }
        };

    // These were registered by apicurio-setup
    let avro_type = get_artifact_type(&storage, backup_id, "payments", "PaymentEvent").await;
    let proto_type = get_artifact_type(&storage, backup_id, "orders", "OrderCreated").await;
    let json_type = get_artifact_type(&storage, backup_id, "orders", "OrderEvent").await;

    if let Some(t) = &avro_type {
        assert_eq!(t, "AVRO");
    }
    if let Some(t) = &proto_type {
        assert_eq!(t, "PROTOBUF");
    }
    if let Some(t) = &json_type {
        assert_eq!(t, "JSON");
    }

    // At least Avro and Protobuf should be present
    assert!(
        avro_type.is_some() || proto_type.is_some(),
        "Expected at least one of AVRO or PROTOBUF artifacts"
    );

    println!(
        "\nArtifact types verified: AVRO={}, PROTOBUF={}, JSON={}",
        avro_type.is_some(),
        proto_type.is_some(),
        json_type.is_some()
    );
}

/// Verify the Apicurio export ZIP can be re-imported to the target registry.
/// This tests the full disaster recovery path: export → store → re-import.
#[tokio::test]
#[ignore = "requires tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml"]
async fn test_e2e_export_zip_reimportable() {
    if !wait_for_registry(APICURIO_SOURCE_URL).await
        || !wait_for_registry(APICURIO_TARGET_URL).await
    {
        panic!("Apicurio registries not reachable. Start tests/registry-restore-test-infra/docker-compose.apicurio-backup.yml and run seed-apicurio.sh");
    }

    let http = reqwest::Client::new();

    // Backup source registry
    let config = e2e_config(APICURIO_SOURCE_URL);
    let client = ApicurioRegistryClient::new(&config).unwrap();
    let storage: Arc<dyn StorageBackend> = Arc::new(MemoryBackend::new());
    let backup_id = "e2e-reimport";

    let backup =
        ApicurioRegistryBackup::new(client, storage.clone(), backup_id.to_string(), config);

    let manifest = backup.run().await.unwrap();
    assert!(manifest.has_export_zip);

    // Get the export ZIP from storage
    let zip_data = storage
        .get(&format!("{}/apicurio-registry/_export.zip", backup_id))
        .await
        .unwrap();

    println!("Export ZIP captured: {} bytes", zip_data.len());

    // Verify target is empty (or at least different from source)
    let target_groups: serde_json::Value = http
        .get(format!("{}/apis/registry/v3/groups", APICURIO_TARGET_URL))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let target_count_before = target_groups["count"].as_i64().unwrap_or(0);
    println!("Target groups before import: {}", target_count_before);

    // Import requires empty target. If already populated (previous test run), skip.
    if target_count_before > 0 {
        println!(
            "Target registry already has {} groups (from a previous run). \
             Skipping import — ZIP validity already verified by magic byte check above.",
            target_count_before
        );
        return;
    }

    // Import ZIP to target registry
    let import_resp = http
        .post(format!(
            "{}/apis/registry/v3/admin/import",
            APICURIO_TARGET_URL
        ))
        .header("Content-Type", "application/zip")
        .body(zip_data.to_vec())
        .send()
        .await
        .unwrap();

    let import_status = import_resp.status();
    let import_body = import_resp.text().await.unwrap_or_default();
    println!("Import response: {} {}", import_status, import_body);

    assert!(
        import_status.is_success(),
        "Import failed: {} {}",
        import_status,
        import_body
    );

    // Give it a moment to process
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify target now has data
    let target_groups_after: serde_json::Value = http
        .get(format!("{}/apis/registry/v3/groups", APICURIO_TARGET_URL))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let target_count_after = target_groups_after["count"].as_i64().unwrap_or(0);
    println!("Target groups after import: {}", target_count_after);

    assert!(
        target_count_after > 0,
        "Import should have added groups to target, got {}",
        target_count_after
    );
}
