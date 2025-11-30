# OSO Kafka Backup: Open Source vs Enterprise Features PRD

## Document Purpose

**This document is the source of truth for feature gating.**

Use this checklist during code reviews to ensure:
1. ✅ Enterprise features are NOT in open source codebase
2. ✅ Open source features are NOT gated behind license keys
3. ✅ License key validation only gates enterprise features
4. ✅ No "accidental leakage" of paid features to OSS

---

## Executive Summary

**Two Products, One Codebase Architecture:**

```
osodevops/kafka-backup/
├── core/                          (Shared between OSS + EE)
│   ├── backup.rs                  (OSS)
│   ├── restore.rs                 (OSS)
│   ├── offset_mapping.rs          (OSS)
│   ├── encryption.rs              ⚠️  (Feature-gated: EE only)
│   ├── gdpr.rs                    ⚠️  (Feature-gated: EE only)
│   └── lib.rs
│
├── cli/
│   ├── commands/
│   │   ├── backup.rs             (OSS)
│   │   ├── restore.rs            (OSS)
│   │   └── offset-reset.rs       (OSS)
│   ├── enterprise/
│   │   ├── encryption.rs         ⚠️  (Gated)
│   │   ├── gdpr.rs               ⚠️  (Gated)
│   │   └── license.rs
│   └── main.rs
│
├── enterprise/                    (EE only - separate directory)
│   ├── Cargo.toml
│   ├── src/
│   │   ├── rbac.rs
│   │   ├── audit.rs
│   │   ├── schema_registry.rs
│   │   └── lib.rs
│   └── LICENSE                    (Proprietary)
│
└── Cargo.toml (workspace)
```

---

## Part 1: Open Source Features (100% Free, No Gating)

These features are ALWAYS available. Never add license checks.

### 1.1 Core Backup Functionality

| Feature | Implementation | License Check | Notes |
|---------|----------------|----------------|--------|
| **Basic Backup to S3** | `core/backup.rs` | ❌ None | Always available |
| **Basic Backup to Azure** | `core/backup.rs` | ❌ None | Always available |
| **Basic Backup to GCS** | `core/backup.rs` | ❌ None | Always available |
| **Topic Filtering (Regex)** | `core/backup.rs` | ❌ None | Always available |
| **Partition Selection** | `core/backup.rs` | ❌ None | Always available |
| **Compression (zstd)** | `core/backup.rs` | ❌ None | Always available |
| **Parallel Backup Workers** | `core/backup.rs` | ❌ None | Always available |
| **Metrics Export (Basic)** | `core/metrics.rs` | ❌ None | JSON output only |

### 1.2 Point-in-Time Restore (PITR)

| Feature | Implementation | License Check | Notes |
|---------|----------------|----------------|--------|
| **Timestamp-Based Restore** | `core/restore.rs` | ❌ None | Always available |
| **Time Window Filtering** | `core/restore.rs` | ❌ None | Always available |
| **Segment Decompression** | `core/restore.rs` | ❌ None | Always available |
| **Dry-Run Validation** | `cli/commands/restore.rs` | ❌ None | Always available |
| **Data Integrity Check** | `core/restore.rs` | ❌ None | Always available |

### 1.3 Offset Handling (Manual)

| Feature | Implementation | License Check | Notes |
|---------|----------------|----------------|--------|
| **Consumer Group Offset Backup** | `core/offset_mapping.rs` | ❌ None | Read-only, always available |
| **Offset Mapping Report (JSON)** | `cli/commands/offset-reset.rs` | ❌ None | Manual user application |
| **Generate Reset Scripts** | `cli/commands/offset-reset.rs` | ❌ None | User runs kafka-consumer-groups CLI |
| **Header-Based Offset Tracking** | `core/offset_mapping.rs` | ❌ None | Stores x-original-offset header |
| **Timestamp Preservation** | `core/offset_mapping.rs` | ❌ None | For debugging/verification |

### 1.4 Configuration & Deployment

| Feature | Implementation | License Check | Notes |
|---------|----------------|----------------|--------|
| **YAML Config Files** | `cli/config.rs` | ❌ None | Always available |
| **Environment Variables** | `cli/config.rs` | ❌ None | Always available |
| **Docker Image** | `Dockerfile` | ❌ None | Multi-stage build, public |
| **CLI Help & Documentation** | `cli/main.rs` | ❌ None | Always available |

### 1.5 Observability (Basic)

| Feature | Implementation | License Check | Notes |
|---------|----------------|----------------|--------|
| **Structured Logging** | `core/logging.rs` | ❌ None | RUST_LOG environment variable |
| **Basic Metrics (Prometheus)** | `core/metrics.rs` | ❌ None | JSON output to stdout |
| **Execution Summary** | `cli/commands/*.rs` | ❌ None | Records backed up, time, size |

---

## Part 2: Enterprise Edition Features (Licensed, Gated)

These features are ONLY available with valid Enterprise Edition license.

### 2.1 Security & Access Control (GATE THESE)

| Feature | Implementation | License Check | Where Check | Notes |
|---------|----------------|----------------|-------------|--------|
| **Client-Side Encryption (AES-256)** | `enterprise/encryption.rs` | ✅ `is_ee_licensed()` | Function entry | Must fail if OSS |
| **Encryption Key Management** | `enterprise/secrets.rs` | ✅ `is_ee_licensed()` | Function entry | Vault integration |
| **Role-Based Access Control (RBAC)** | `enterprise/rbac.rs` | ✅ License check at startup | Service init | Check in CLI bootstrap |
| **SSO/OIDC Integration** | `enterprise/auth.rs` | ✅ License check at startup | Service init | Okta, Azure AD, Google |
| **Secrets Manager Integration (Vault, AWS Secrets)** | `enterprise/secrets.rs` | ✅ `is_ee_licensed()` | Function entry | HashiCorp Vault, AWS Secrets Manager |

**CRITICAL:** If license check fails for ANY of these:
- ❌ Do NOT fall back to OSS alternative
- ❌ Do NOT log credentials to YAML
- ✅ Fail with clear error: "Feature requires Enterprise Edition"

### 2.2 Audit Logging & Compliance (GATE THESE)

| Feature | Implementation | License Check | Where Check | Notes |
|---------|----------------|----------------|-------------|--------|
| **Audit Trail (Who/What/When)** | `enterprise/audit.rs` | ✅ License check at startup | Service init | Every restore logged |
| **Compliance Reporting (SOC2, HIPAA, GDPR)** | `enterprise/compliance.rs` | ✅ License check at startup | Report generation | Template-based |
| **Log Shipping (Datadog, Splunk, Grafana Loki)** | `enterprise/log_shipping.rs` | ✅ `is_ee_licensed()` | Function entry | Forward logs to external systems |
| **Queryable Audit Dashboard** | N/A (SaaS UI only) | ✅ SaaS session token | API authentication | Not applicable to CLI |

**CRITICAL:** OSS version must NOT include audit logging beyond structured logs to STDOUT.

### 2.3 GDPR & Data Compliance (GATE THESE)

| Feature | Implementation | License Check | Where Check | Notes |
|---------|----------------|----------------|-------------|--------|
| **GDPR "Right to be Forgotten"** | `enterprise/gdpr.rs` | ✅ License check at startup | Command entry | Re-encrypt + remove keys |
| **Data Masking / PII Redaction** | `enterprise/data_masking.rs` | ✅ License check at startup | Config validation | Mask credit cards, SSNs, emails |
| **Crypto-Shredding** | `enterprise/crypto_shred.rs` | ✅ License check at startup | Command entry | Destroy keys without rewriting |
| **HIPAA Compliance Audit Trail** | `enterprise/compliance.rs` | ✅ License check at startup | Report generation | Healthcare-specific |

**CRITICAL:** OSS backups are unencrypted. EE is encrypted. Never blur this line.

### 2.4 Consumer Group Offset Automation (GATE THESE)

| Feature | Implementation | License Check | Where Check | Notes |
|---------|----------------|----------------|-------------|--------|
| **Automatic Offset Reset** | `enterprise/offset_auto_reset.rs` | ✅ `is_ee_licensed()` | Command entry | Apply offsets to Kafka automatically |
| **Kafka API Integration (OffsetCommitRequest)** | `enterprise/offset_auto_reset.rs` | ✅ `is_ee_licensed()` | Function entry | Direct Kafka broker communication |
| **Bulk Offset Reset** | `enterprise/offset_auto_reset.rs` | ✅ `is_ee_licensed()` | Function entry | Apply 1000+ offsets in parallel |
| **Offset Reset Rollback** | `enterprise/offset_rollback.rs` | ✅ `is_ee_licensed()` | Function entry | Revert offset changes if restore fails |

**CRITICAL:** OSS generates JSON reports. EE applies them. Never automate in OSS.

### 2.5 Schema Registry Integration (GATE THESE)

| Feature | Implementation | License Check | Where Check | Notes |
|---------|----------------|----------------|-------------|--------|
| **Schema Registry Backup** | `enterprise/schema_registry.rs` | ✅ License check at startup | Command entry | Backup Confluent / Apicurio schemas |
| **Schema Registry Restore** | `enterprise/schema_registry.rs` | ✅ License check at startup | Command entry | Re-register schemas |
| **Schema ID Remapping** | `enterprise/schema_registry.rs` | ✅ License check at startup | Function entry | Source ID 5 → Target ID 47 |
| **Schema Version Pinning** | `enterprise/schema_registry.rs` | ✅ License check at startup | Config validation | Lock specific schema versions |

**CRITICAL:** OSS does NOT touch Schema Registry. EE has full integration.

### 2.6 Advanced Observability & Operations (GATE THESE)

| Feature | Implementation | License Check | Where Check | Notes |
|---------|----------------|----------------|-------------|--------|
| **Advanced Metrics (Throughput, Compression Ratio, per-partition latency)** | `enterprise/metrics.rs` | ✅ `is_ee_licensed()` | Metrics service init | Beyond basic count/success |
| **Log Shipping (Datadog/Splunk/Grafana Loki)** | `enterprise/log_shipping.rs` | ✅ `is_ee_licensed()` | Function entry | Auto-forward logs |
| **Web Dashboard (HTML UI)** | N/A (SaaS UI only) | ✅ SaaS session token | API authentication | Monitoring + drill-down |
| **Backup Validation Test Runs** | `enterprise/validation.rs` | ✅ License check at startup | Command entry | Periodic restore tests |
| **Health Checks & Alerting** | `enterprise/health.rs` | ✅ License check at startup | Service init | Prometheus + alerting rules |

### 2.7 Scale & Performance (GATE THESE)

| Feature | Implementation | License Check | Where Check | Notes |
|---------|----------------|----------------|-------------|--------|
| **Kafka Backend (vs SQLite for state)** | `enterprise/kafka_backend.rs` | ✅ License check at startup | Storage init | For multi-petabyte backups |
| **Elasticsearch Backend (for logs)** | `enterprise/elasticsearch.rs` | ✅ License check at startup | Log storage init | Searchable log archive |
| **Worker Groups / Isolation** | `enterprise/worker_groups.rs` | ✅ License check at startup | Config validation | "Finance" backups on secure servers |
| **Versioned Plugins** | `enterprise/plugins.rs` | ✅ License check at startup | Plugin loading | Pin specific versions |
| **Multi-Region Replication** | `enterprise/replication.rs` | ✅ License check at startup | Config validation | Active-active across regions |

---

## Part 3: License Key Validation Pattern

### 3.1 License Key Structure

```rust
// enterprise/license.rs

#[derive(Debug, Clone)]
pub struct LicenseKey {
    pub id: String,                    // e.g., "KAFKA-BACKUP-EE-001"
    pub customer: String,              // Customer company name
    pub expiration: DateTime<Utc>,      // When license expires
    pub tier: LicenseTier,              // Starter, Growth, Enterprise
    pub max_clusters: u32,              // Number of clusters allowed
    pub features: Vec<String>,          // List of enabled features
    pub fingerprint: String,            // Public key fingerprint
    pub signature: Vec<u8>,             // RSA signature (private key signed)
}

pub enum LicenseTier {
    Starter,    // $500/month: 5 clusters, basic encryption
    Growth,     // $2000/month: unlimited clusters, full encryption, GDPR
    Enterprise, // Custom: everything + dedicated support
}
```

### 3.2 License Validation Entry Points

**CRITICAL: Check license at function entry, not internal logic**

```rust
// ✅ CORRECT: Check license before executing
pub async fn encrypt_backup(config: &Config) -> Result<()> {
    if !is_ee_licensed()? {
        return Err(anyhow!(
            "Client-side encryption requires Enterprise Edition. \
             Visit https://oso.sh/pricing for more information."
        ));
    }
    
    // ... actual encryption logic ...
}

// ❌ WRONG: Check license deep in function
pub async fn encrypt_backup(config: &Config) -> Result<()> {
    // ... lots of setup ...
    
    if !is_ee_licensed()? {
        // User wasted 10 seconds before finding out it's not allowed
    }
    
    // ... encryption logic ...
}
```

### 3.3 License Validation Locations (Comprehensive Checklist)

**CLI Startup:**
```rust
// cli/main.rs
#[tokio::main]
async fn main() -> Result<()> {
    let args = Cli::parse();
    
    // Check license EARLY if EE features requested
    if args.command.requires_ee() {
        validate_license().await?;
    }
    
    match args.command {
        Command::Backup(opts) => backup_cmd(opts).await,
        Command::EncryptBackup(opts) => {
            // ✅ This automatically checks license (requires_ee = true)
            enterprise::encrypt_backup(opts).await
        }
        _ => {}
    }
}
```

**Enterprise Command Handlers:**
```rust
// enterprise/offset_auto_reset.rs
pub async fn auto_reset_offsets(config: &Config) -> Result<()> {
    validate_license()?;  // ← License check first
    
    // Then execute
    let offsets = config.load_offset_mapping()?;
    apply_offsets_to_kafka(offsets).await?;
    
    Ok(())
}

// enterprise/gdpr.rs
pub async fn delete_pii_from_backup(backup_id: &str, rules: &[PiiRule]) -> Result<()> {
    validate_license()?;  // ← License check first
    
    // Then execute
    let segments = load_backup_segments(backup_id)?;
    let masked = apply_pii_rules(segments, rules)?;
    save_backup_segments(masked)?;
    
    Ok(())
}
```

### 3.4 License File Location

```
Config Directory:
~/.config/kafka-backup/         (Linux)
~/Library/Application Support/  (macOS)
%APPDATA%/kafka-backup/         (Windows)

Files:
├── license.key                  (Encrypted license JSON + signature)
└── license.pub                  (Public key for verification - embedded in binary)
```

**Environment Variable Override (for Docker/K8s):**
```bash
export KAFKA_BACKUP_LICENSE_KEY="eyJ0eXAiOiJKV1QiLCJhbGc..."
```

### 3.5 License Validation Function (Reference Implementation)

```rust
// enterprise/license.rs

use anyhow::anyhow;

pub fn validate_license() -> Result<()> {
    let license_key = load_license_key()?;
    
    // 1. Check expiration
    if license_key.expiration < Utc::now() {
        return Err(anyhow!(
            "License expired on {}. \
             Please renew at https://oso.sh/license",
            license_key.expiration.format("%Y-%m-%d")
        ));
    }
    
    // 2. Verify signature (RSA with embedded public key)
    if !verify_signature(&license_key)? {
        return Err(anyhow!(
            "License signature invalid. \
             License may be corrupted or fraudulent."
        ));
    }
    
    // 3. Check cluster count
    if active_clusters()? > license_key.max_clusters {
        return Err(anyhow!(
            "Active clusters ({}) exceeds license limit ({}).",
            active_clusters()?,
            license_key.max_clusters
        ));
    }
    
    Ok(())
}

pub fn is_ee_licensed() -> Result<bool> {
    match validate_license() {
        Ok(()) => Ok(true),
        Err(_) => Ok(false),
    }
}

pub fn require_feature(feature: &str) -> Result<()> {
    let license_key = load_license_key()?;
    
    if !license_key.features.contains(&feature.to_string()) {
        return Err(anyhow!(
            "Feature '{}' requires {} license tier. \
             Current: {}. Upgrade at https://oso.sh/pricing",
            feature,
            feature_required_tier(feature),
            license_key.tier,
        ));
    }
    
    Ok(())
}
```

---

## Part 4: Code Review Checklist

**Use this during every PR review to open source code.**

### Question 1: Does this PR add a new feature?

```
Yes → Go to Question 2
No → Go to Question 4
```

### Question 2: Is this feature in the Enterprise Features list (Part 2)?

```
Yes → ❌ REJECT PR
     "This feature is enterprise-only: " + [feature name]
     "Add it to enterprise/ directory instead"

No → Go to Question 3
```

### Question 3: Did you add license check code to core/ or cli/?

```
Yes → ❌ REJECT PR
     "Open source features must not have license checks"
     "Remove is_ee_licensed() / validate_license() calls"

No → ✅ APPROVE (after code review)
```

### Question 4: Is this PR modifying enterprise/ directory?

```
Yes → ✅ Check for missing license validation
     "Does this function have validate_license() at entry?"
     If no: ❌ REJECT
     If yes: ✅ APPROVE (after code review)

No → Go to Question 5
```

### Question 5: Is this PR modifying license validation or key loading?

```
Yes → 🚨 SECURITY REVIEW REQUIRED
     Ask: "Does this weaken license checking?"
     Ask: "Could this allow bypassing license?"
     Ask: "Is the change backward compatible?"

No → ✅ APPROVE (after code review)
```

---

## Part 5: Feature Parity Guarantee

**OSS and EE must have identical behavior for overlapping features.**

| Aspect | OSS | EE | Parity |
|--------|-----|----|----|
| **PITR Accuracy** | ±0 messages | ±0 messages | ✅ Identical |
| **Backup Speed (MB/s)** | Same | Same | ✅ Identical |
| **Compression Ratio** | Same | Same | ✅ Identical |
| **Data Integrity** | 100% | 100% | ✅ Identical |
| **Consumer Offset Mapping** | Report (JSON) | Report (JSON) + Auto-apply | ⚠️ Same core, different delivery |
| **Disaster Recovery Capability** | Data restored correctly | + Encrypted + audited | ⚠️ Same result, different security |

**CRITICAL:** If a bug exists in OSS PITR, it MUST be fixed in EE too (and vice versa).

---

## Part 6: Configuration Separation

### 6.1 OSS Configuration (YAML / Environment)

```yaml
# config.yaml (OSS)
kafka:
  brokers:
    - localhost:9092

backup:
  topics: ".*"
  partitions: [0, 1, 2]
  compression: zstd
  parallel_workers: 4
  time_window:
    start: 2025-01-01T00:00:00Z
    end: 2025-12-31T23:59:59Z

storage:
  type: s3
  bucket: my-backups
  region: us-east-1
  access_key: "${S3_ACCESS_KEY}"  # From env, NOT in file
  secret_key: "${S3_SECRET_KEY}"  # From env, NOT in file

logging:
  level: info
  format: json
```

### 6.2 Enterprise Configuration (YAML / Environment)

```yaml
# config.yaml (Enterprise Edition)

# All OSS config above, plus:

# License
license:
  key_path: ~/.config/kafka-backup/license.key

# Security
security:
  encryption:
    enabled: true
    algorithm: AES-256-GCM
    key_source: vault  # or aws_secrets, or file
  rbac:
    enabled: true
    default_role: viewer

# Vault Integration
vault:
  addr: https://vault.company.com
  token_path: ~/.vault-token
  engine: kafka-backup
  role: backup-service

# Audit Logging
audit:
  enabled: true
  log_level: info
  backends:
    - datadog
    - splunk

# Schema Registry
schema_registry:
  enabled: true
  url: https://schema-registry.company.com
  username: "${SCHEMA_REGISTRY_USER}"
  password: "${SCHEMA_REGISTRY_PASS}"
```

**CRITICAL:** OSS must ignore enterprise config keys without error.

---

## Part 7: Telemetry & Privacy

### 7.1 OSS Telemetry (No Collection)

```rust
// ✅ ALLOWED in OSS:
tracing::info!("Backup started for {} topics", topic_count);
tracing::info!("Backup completed: {} records, {} MB", records, mb);
println!("Backup completed successfully");

// ❌ NOT ALLOWED in OSS:
// - Phone home to any server
// - Collect customer data
// - Send metrics to OSO servers
// - Any external API calls beyond storage (S3, Azure, GCS)
```

### 7.2 Enterprise Telemetry (With Consent)

```rust
// ✅ ALLOWED in EE (with license check):
pub async fn send_telemetry(metrics: &Metrics) -> Result<()> {
    validate_license()?;  // Only if licensed
    
    // Send to OSO telemetry endpoint
    telemetry_client
        .send_metrics(metrics)
        .await?;
    
    Ok(())
}
```

---

## Part 8: Testing Requirements

### 8.1 OSS Tests (No Mocking Enterprise)

```rust
// ✅ CORRECT: Test OSS features independently
#[tokio::test]
async fn test_pitr_restore_accuracy() {
    let backup = create_test_backup();
    let restored = restore_at_timestamp(&backup, TIMESTAMP).await;
    
    assert_eq!(backup.record_count, restored.record_count);
}

// ❌ WRONG: Test with mocked enterprise license
#[tokio::test]
async fn test_encryption() {
    mock_license();  // Don't do this in OSS tests
    
    let encrypted = encrypt_backup(&config).await;
    assert!(encrypted.is_ok());
}
```

### 8.2 Enterprise Tests (Test License Validation)

```rust
// ✅ CORRECT: Test that features fail without license
#[tokio::test]
async fn test_encryption_requires_license() {
    let result = encrypt_backup(&config).await;
    
    match result {
        Err(e) if e.to_string().contains("Enterprise Edition") => {
            // ✅ Correct: Feature gated
        }
        Ok(_) => panic!("Encryption should require license"),
        Err(e) => panic!("Unexpected error: {}", e),
    }
}

// ✅ CORRECT: Test that features work WITH license
#[tokio::test]
async fn test_encryption_with_valid_license() {
    set_test_license(LicenseTier::Growth);
    
    let encrypted = encrypt_backup(&config).await;
    assert!(encrypted.is_ok());
}
```

---

## Part 9: Documentation Requirements

### 9.1 OSS Documentation (What NOT to Say)

```markdown
❌ DON'T include:
- "Enterprise features include..."
- "Contact sales for encryption"
- "Available in Enterprise Edition"
- Links to pricing page scattered throughout

✅ DO include:
- Feature list (all OSS features)
- Getting started guide
- Example configurations
- Troubleshooting
- Community contribution guidelines
```

### 9.2 Enterprise Documentation (Separate Repo or Docs Site)

```
https://docs.oso.sh/enterprise/
├── Overview
├── Encryption Setup
├── GDPR Compliance
├── RBAC Configuration
├── Audit Logging
├── License Management
└── Pricing & Billing
```

---

## Part 10: Release Checklist

### Before Publishing OSS Release

- [ ] No `is_ee_licensed()` calls in `core/` or `cli/`
- [ ] No encryption code in `core/backup.rs` or `core/restore.rs`
- [ ] No GDPR code in `core/`
- [ ] No audit logging in `core/`
- [ ] No enterprise config keys documented in OSS README
- [ ] No enterprise features in CLI help output
- [ ] Docker image builds cleanly without enterprise/ directory
- [ ] All tests pass (OSS tests only)
- [ ] No secrets or license keys in example configs
- [ ] Version number bumped in Cargo.toml

### Before Publishing Enterprise Release

- [ ] All enterprise features have license checks
- [ ] All license checks at function entry (not internal)
- [ ] License validation code in `enterprise/license.rs` only
- [ ] All enterprise tests pass
- [ ] Audit logging captures all restore operations
- [ ] Encryption is transparent to user (no key management in CLI)
- [ ] Schema Registry integration tested
- [ ] Multi-region replication tested
- [ ] Version number bumped and matches OSS core version
- [ ] License key management documented

---

## Part 11: Common Pitfalls & How to Avoid Them

### Pitfall 1: Encryption in Core

```rust
// ❌ WRONG: Encryption in core/backup.rs
pub async fn backup_partition(topic: &str, partition: i32) -> Result<Vec<u8>> {
    let records = fetch_records(topic, partition).await?;
    
    if config.encryption.enabled {  // ← Should NOT be here
        return encrypt_records(&records);
    }
    
    Ok(records)
}

// ✅ CORRECT: Encryption in enterprise layer
pub async fn backup_partition(topic: &str, partition: i32) -> Result<Vec<u8>> {
    // core/backup.rs - NO encryption logic
}

pub async fn enterprise_backup_partition(topic: &str, partition: i32) -> Result<Vec<u8>> {
    // enterprise/backup.rs - WITH encryption
    validate_license()?;
    
    let records = core::backup_partition(topic, partition).await?;
    encrypt_records(&records)
}
```

### Pitfall 2: Audit Logging in Core

```rust
// ❌ WRONG: Logging in core/restore.rs
pub async fn restore(backup_id: &str) -> Result<()> {
    audit_log(&format!("User {} restored backup {}", user, backup_id));  // ← No!
    
    do_restore(backup_id).await
}

// ✅ CORRECT: Logging in enterprise layer
pub async fn enterprise_restore(backup_id: &str) -> Result<()> {
    validate_license()?;
    
    audit_log(&format!("User {} restored backup {}", user, backup_id));  // ← OK here
    
    core::restore(backup_id).await
}
```

### Pitfall 3: Feature Parity Difference

```rust
// ❌ WRONG: Different behavior between OSS and EE
// OSS version:
pub async fn restore(backup_id: &str) -> Result<()> {
    let records = load_segments(backup_id)?;
    write_to_kafka(&records).await?;
    Ok(())
}

// EE version:
pub async fn enterprise_restore(backup_id: &str) -> Result<()> {
    validate_license()?;
    let records = load_segments(backup_id)?;
    decrypt_records(&records)?;  // ← Bug: different decoding
    write_to_kafka(&records).await?;
    Ok(())
}

// ✅ CORRECT: Identical core logic
pub async fn restore(backup_id: &str) -> Result<Vec<Record>> {
    let records = load_segments(backup_id)?;
    Ok(records)  // Return raw records
}

// OSS usage:
pub async fn oss_main() -> Result<()> {
    let records = restore(backup_id).await?;
    write_to_kafka(&records).await?;
}

// EE usage:
pub async fn enterprise_main() -> Result<()> {
    validate_license()?;
    let records = restore(backup_id).await?;
    let decrypted = decrypt_records(&records)?;  // EE adds decryption
    write_to_kafka(&decrypted).await?;
}
```

### Pitfall 4: Configuration Leakage

```rust
// ❌ WRONG: Accepting enterprise config in OSS
if let Some(vault_config) = &config.vault {
    connect_to_vault(vault_config)?;  // ← OSS doesn't understand this
}

// ✅ CORRECT: Ignore unknown config keys
fn load_config(yaml: &str) -> Result<Config> {
    let config: serde_yaml::Value = serde_yaml::from_str(yaml)?;
    
    // Load only OSS keys, ignore others
    Config {
        kafka: config["kafka"].clone(),
        backup: config["backup"].clone(),
        storage: config["storage"].clone(),
        // ← Ignore "license", "vault", "audit", "security"
    }
}
```

---

## Part 12: Enterprise Build Separation (Optional)

If you want complete separation, use feature flags:

```toml
# Cargo.toml
[features]
default = ["oss"]
oss = []
enterprise = ["oss"]

[dependencies.enterprise]
path = "enterprise"
optional = true
```

```rust
// cli/main.rs
#[cfg(feature = "enterprise")]
use enterprise;

fn main() {
    match args.command {
        #[cfg(feature = "enterprise")]
        Command::EncryptBackup(opts) => enterprise::encrypt_backup(opts).await?,
        
        #[cfg(not(feature = "enterprise"))]
        Command::EncryptBackup(_) => {
            eprintln!("Error: Encryption requires Enterprise Edition");
            std::process::exit(1);
        }
    }
}
```

Build commands:
```bash
# Build OSS
cargo build --release --features oss

# Build Enterprise
cargo build --release --features enterprise
```

---

## Part 13: Final Checklist Before Release

**For Open Source Release:**
- [ ] `cargo build --release` succeeds with no warnings
- [ ] `cargo test` - all tests pass
- [ ] `cargo clippy` - no warnings
- [ ] No `enterprise/` directory or binaries included
- [ ] Docker image size < 100MB
- [ ] Help text mentions no enterprise features
- [ ] README only documents OSS features
- [ ] License is Apache 2.0
- [ ] No customer data collection code
- [ ] No telemetry calls to OSO servers

**For Enterprise Release:**
- [ ] All features behind license checks
- [ ] `cargo build --features enterprise` succeeds
- [ ] Enterprise tests pass
- [ ] License validation tested with expired/invalid keys
- [ ] Audit logging captures all operations
- [ ] Encryption verified end-to-end
- [ ] Schema Registry integration tested
- [ ] Docker image tagged as `-ee` variant
- [ ] License.key generation documented
- [ ] Upgrade path from OSS to EE documented

---

## Summary

**Give This Document to Your Agent:**

Use this as the reference for feature gating. Every PR should answer:

1. ✅ Is this feature in Part 1 (OSS) or Part 2 (Enterprise)?
2. ✅ Does it have the right license checks?
3. ✅ Are the tests appropriate for OSS or Enterprise?
4. ✅ Does it break feature parity?

**This ensures:**
- ✅ No accidental leakage of enterprise features to OSS
- ✅ No misconfigured license checks
- ✅ Consistent behavior between OSS and EE
- ✅ Clear separation of concerns

