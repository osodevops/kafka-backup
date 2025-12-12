# Product Requirements Document: Azure Blob Storage Backend Support

**Document Version:** 1.0
**Date:** 2025-12-12
**Author:** StreamPlus Platform Team
**Status:** Draft

---

## Executive Summary

This PRD outlines the requirements for implementing native Azure Blob Storage support in the `kafka-backup-core` library. Currently, the library only supports filesystem and S3 storage backends. The kafka-backup-operator has CRD definitions for Azure Blob Storage, but these are non-functional stubs that fall through to an empty S3 configuration, causing runtime failures.

Adding Azure Blob Storage support will enable organisations using Azure cloud infrastructure to back up and restore Kafka topics directly to/from Azure Blob Storage without requiring intermediate gateways or proxy services.

---

## Problem Statement

### Current State

1. **kafka-backup-core** supports two storage backends:
    - `Filesystem` - Local/PVC storage
    - `S3` - AWS S3 and S3-compatible storage (MinIO, Ceph, etc.)

2. **kafka-backup-operator** includes CRD definitions for Azure Blob Storage:
   ```yaml
   storage:
     storageType: azure
     azure:
       accountName: mystorageaccount
       container: kafka-backups
       prefix: production
       credentialsSecret:
         name: azure-storage-credentials
         accountKeyKey: AZURE_STORAGE_KEY
   ```

3. **The Problem:** When the operator processes an Azure storage configuration, `core_integration.rs` maps it to an S3 backend with empty values:
   ```rust
   ResolvedStorage::Azure(_azure) => {
       // TODO: Map Azure to S3-compatible or extend core library
       StorageConfig {
           backend: StorageBackendType::S3,
           bucket: None,  // Azure container is discarded
           // ... all other fields None
       }
   }
   ```

4. **Result:** The core library's config validation fails with:
   ```
   Configuration error: Bucket is required for S3 backend
   ```

### Impact

- Organisations using Azure cannot use kafka-backup-operator for Kafka disaster recovery
- Workarounds (MinIO gateway, PVC + Azure Backup) add complexity and cost
- The operator's Azure CRD definitions create false expectations of Azure support

---

## Goals and Objectives

### Primary Goals

1. Implement native Azure Blob Storage backend in `kafka-backup-core`
2. Support both Storage Account Key and Azure Workload Identity authentication
3. Maintain feature parity with the existing S3 backend
4. Enable seamless integration with the existing kafka-backup-operator

### Success Metrics

| Metric | Target |
|--------|--------|
| Backup throughput to Azure Blob | >= 90% of S3 backend throughput |
| Restore throughput from Azure Blob | >= 90% of S3 backend throughput |
| Authentication methods supported | Storage Key + Workload Identity |
| Test coverage | >= 80% for new code |

---

## Requirements

### Functional Requirements

#### FR-1: Storage Backend Type

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-1.1 | Add `AzureBlob` variant to `StorageBackendType` enum | P0 |
| FR-1.2 | Implement `AzureBlobBackend` struct implementing the storage trait | P0 |
| FR-1.3 | Support Azure Blob Storage container as the backup destination | P0 |

#### FR-2: Configuration

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-2.1 | Add Azure-specific fields to `StorageConfig` struct | P0 |
| FR-2.2 | Support `account_name` configuration | P0 |
| FR-2.3 | Support `container` configuration | P0 |
| FR-2.4 | Support optional `prefix` for blob path organization | P0 |
| FR-2.5 | Support optional custom `endpoint` for Azure Government/China clouds | P1 |

#### FR-3: Authentication - Storage Account Key

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-3.1 | Support authentication via Storage Account Key | P0 |
| FR-3.2 | Accept account key via configuration field | P0 |
| FR-3.3 | Accept account key via `AZURE_STORAGE_KEY` environment variable | P1 |

#### FR-4: Authentication - Azure Workload Identity

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-4.1 | Support authentication via Azure Workload Identity (federated tokens) | P0 |
| FR-4.2 | Auto-detect Workload Identity when running in AKS with mounted tokens | P0 |
| FR-4.3 | Support `AZURE_CLIENT_ID` environment variable | P0 |
| FR-4.4 | Support `AZURE_TENANT_ID` environment variable | P0 |
| FR-4.5 | Support `AZURE_FEDERATED_TOKEN_FILE` environment variable | P0 |
| FR-4.6 | Fall back to DefaultAzureCredential chain when no explicit auth configured | P1 |

#### FR-5: Blob Operations

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-5.1 | Implement blob upload (put) with streaming support | P0 |
| FR-5.2 | Implement blob download (get) with streaming support | P0 |
| FR-5.3 | Implement blob list with prefix filtering | P0 |
| FR-5.4 | Implement blob delete | P0 |
| FR-5.5 | Implement blob exists check | P0 |
| FR-5.6 | Support block blobs for large file uploads (> 256MB) | P0 |
| FR-5.7 | Support concurrent block uploads for improved throughput | P1 |

#### FR-6: Backup Operations

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-6.1 | Write backup segments to Azure Blob Storage | P0 |
| FR-6.2 | Write backup metadata/manifest files | P0 |
| FR-6.3 | Support checkpoint files for resumable backups | P0 |
| FR-6.4 | Organize blobs with configurable prefix structure | P0 |

#### FR-7: Restore Operations

| ID | Requirement | Priority |
|----|-------------|----------|
| FR-7.1 | Read backup segments from Azure Blob Storage | P0 |
| FR-7.2 | Read backup metadata/manifest files | P0 |
| FR-7.3 | Support checkpoint-based resume for interrupted restores | P0 |
| FR-7.4 | List available backups in a container | P0 |

### Non-Functional Requirements

#### NFR-1: Performance

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-1.1 | Single blob upload throughput | >= 100 MB/s |
| NFR-1.2 | Single blob download throughput | >= 100 MB/s |
| NFR-1.3 | Concurrent partition backup throughput | >= 500 MB/s aggregate |
| NFR-1.4 | Memory usage during streaming | <= 256 MB per concurrent operation |

#### NFR-2: Reliability

| ID | Requirement | Target |
|----|-------------|--------|
| NFR-2.1 | Automatic retry on transient failures | 3 retries with exponential backoff |
| NFR-2.2 | Connection timeout | Configurable, default 30s |
| NFR-2.3 | Operation timeout | Configurable, default 5 minutes |
| NFR-2.4 | Graceful handling of Azure throttling (429 responses) | Required |

#### NFR-3: Security

| ID | Requirement |
|----|-------------|
| NFR-3.1 | TLS required for all Azure API communications |
| NFR-3.2 | Storage account keys must not be logged |
| NFR-3.3 | Support Azure Private Endpoints |
| NFR-3.4 | Workload Identity tokens must not be persisted to disk |

#### NFR-4: Observability

| ID | Requirement |
|----|-------------|
| NFR-4.1 | Emit metrics for blob operations (count, duration, bytes) |
| NFR-4.2 | Structured logging for Azure operations |
| NFR-4.3 | Include correlation IDs for Azure API calls |

---

## Technical Specifications

### Proposed Storage Config Changes

```rust
pub struct StorageConfig {
    pub backend: StorageBackendType,

    // Existing fields for Filesystem
    pub path: Option<PathBuf>,

    // Existing fields for S3
    pub endpoint: Option<String>,
    pub bucket: Option<String>,
    pub access_key: Option<String>,
    pub secret_key: Option<String>,
    pub region: Option<String>,

    // New fields for Azure Blob Storage
    pub azure_account_name: Option<String>,
    pub azure_container: Option<String>,
    pub azure_account_key: Option<String>,
    pub azure_use_workload_identity: Option<bool>,
    pub azure_endpoint: Option<String>,  // For sovereign clouds

    // Common
    pub prefix: Option<String>,
}

pub enum StorageBackendType {
    Filesystem,
    S3,
    AzureBlob,  // New variant
}
```

### Proposed Azure Backend Implementation

```rust
pub struct AzureBlobBackend {
    client: ContainerClient,
    container: String,
    prefix: Option<String>,
}

impl AzureBlobBackend {
    pub async fn new(config: &StorageConfig) -> Result<Self> {
        let account_name = config.azure_account_name
            .as_ref()
            .ok_or(ConfigError::MissingField("azure_account_name"))?;

        let container = config.azure_container
            .as_ref()
            .ok_or(ConfigError::MissingField("azure_container"))?;

        // Build credential based on configuration
        let credential = Self::build_credential(config).await?;

        let account_url = config.azure_endpoint
            .clone()
            .unwrap_or_else(|| format!("https://{}.blob.core.windows.net", account_name));

        let blob_service_client = BlobServiceClient::new(account_url, credential);
        let container_client = blob_service_client.container_client(container);

        Ok(Self {
            client: container_client,
            container: container.clone(),
            prefix: config.prefix.clone(),
        })
    }

    async fn build_credential(config: &StorageConfig) -> Result<Arc<dyn TokenCredential>> {
        // Priority:
        // 1. Explicit account key
        // 2. Workload Identity (if enabled or auto-detected)
        // 3. DefaultAzureCredential chain

        if let Some(account_key) = &config.azure_account_key {
            return Ok(Arc::new(StorageSharedKeyCredential::new(
                config.azure_account_name.as_ref().unwrap(),
                account_key,
            )));
        }

        // Check for Workload Identity environment
        if config.azure_use_workload_identity.unwrap_or(false)
            || std::env::var("AZURE_FEDERATED_TOKEN_FILE").is_ok()
        {
            return Ok(Arc::new(WorkloadIdentityCredential::new()?));
        }

        // Fall back to default credential chain
        Ok(Arc::new(DefaultAzureCredential::new()?))
    }
}

impl StorageBackend for AzureBlobBackend {
    async fn put(&self, key: &str, data: &[u8]) -> Result<()>;
    async fn put_stream(&self, key: &str, stream: impl AsyncRead) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Vec<u8>>;
    async fn get_stream(&self, key: &str) -> Result<impl AsyncRead>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn exists(&self, key: &str) -> Result<bool>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
}
```

### Recommended Rust Crates

| Crate | Purpose | Version |
|-------|---------|---------|
| `azure_storage_blobs` | Azure Blob Storage SDK | Latest |
| `azure_identity` | Azure authentication (Workload Identity, DefaultCredential) | Latest |
| `azure_core` | Core Azure SDK utilities | Latest |

### Operator Integration Changes

After core library implementation, update `kafka-backup-operator/src/adapters/core_integration.rs`:

```rust
ResolvedStorage::Azure(azure) => StorageConfig {
    backend: StorageBackendType::AzureBlob,
    azure_account_name: Some(azure.account_name.clone()),
    azure_container: Some(azure.container.clone()),
    azure_account_key: if azure.account_key.is_empty() {
        None
    } else {
        Some(azure.account_key.clone())
    },
    azure_use_workload_identity: Some(azure.account_key.is_empty()),
    prefix: azure.prefix.clone(),
    // S3/Filesystem fields remain None
    ..Default::default()
},
```

---

## Test Plan

### Unit Tests

| Test Case | Description |
|-----------|-------------|
| `test_azure_config_validation` | Verify config validation for Azure backend |
| `test_azure_credential_priority` | Verify credential selection priority |
| `test_blob_path_construction` | Verify prefix/key path construction |
| `test_workload_identity_detection` | Verify auto-detection of Workload Identity env vars |

### Integration Tests

| Test Case | Description |
|-----------|-------------|
| `test_azure_blob_put_get` | Upload and download blob roundtrip |
| `test_azure_blob_list` | List blobs with prefix filtering |
| `test_azure_blob_delete` | Delete blob and verify removal |
| `test_azure_large_blob` | Upload/download blob > 256MB using blocks |
| `test_azure_streaming` | Streaming upload/download |

### End-to-End Tests

| Test Case | Description |
|-----------|-------------|
| `test_backup_to_azure` | Full backup of test topics to Azure Blob |
| `test_restore_from_azure` | Full restore from Azure Blob backup |
| `test_backup_resume_azure` | Interrupted backup resume from checkpoint |
| `test_concurrent_partitions_azure` | Multi-partition concurrent backup |

### Authentication Tests

| Test Case | Description |
|-----------|-------------|
| `test_storage_account_key_auth` | Authentication with storage account key |
| `test_workload_identity_auth` | Authentication with Workload Identity (requires AKS) |
| `test_invalid_credentials` | Proper error handling for invalid credentials |

---

## Rollout Plan

### Phase 1: Core Library Implementation
- Implement `AzureBlobBackend` with storage account key authentication
- Add unit tests and integration tests
- Update configuration validation

### Phase 2: Workload Identity Support
- Add Workload Identity credential support
- Add DefaultAzureCredential fallback
- Test in AKS environment

### Phase 3: Operator Integration
- Update `core_integration.rs` to properly map Azure storage
- Update operator CRD if needed (e.g., add `useWorkloadIdentity` field)
- Update Helm chart documentation

### Phase 4: Documentation and Release
- Update README with Azure configuration examples
- Add Azure-specific troubleshooting guide
- Release new versions of core library and operator

---

## Dependencies

| Dependency | Owner | Status |
|------------|-------|--------|
| Azure SDK Rust crates | Microsoft/Azure | Available |
| kafka-backup-core storage trait | OSO DevOps | Exists |
| AKS Workload Identity | Azure Platform | Available |

---

## Open Questions

1. **Block size for large blobs**: What block size should be used for blobs > 256MB? (Suggested: 100MB blocks)

2. **Retry configuration**: Should retry settings be configurable per-backend or global?

3. **Azure Government/China clouds**: Is explicit support for sovereign clouds required in initial release?

4. **Managed Identity types**: Should we support User-Assigned Managed Identity in addition to Workload Identity?

5. **GCS support**: Should GCS backend be implemented in the same effort using similar patterns?

---

## Appendix

### A. Current Error Trace

```
Error: Kafka backup core error: Failed to build core config: Configuration error: Bucket is required for S3 backend
```

This occurs because `core_integration.rs:150-161` maps Azure storage to an empty S3 config:
```rust
ResolvedStorage::Azure(_azure) => {
    StorageConfig {
        backend: StorageBackendType::S3,
        bucket: None,  // <-- Causes validation failure
        ...
    }
}
```

### B. Related Files

**kafka-backup-operator:**
- `src/adapters/storage_config.rs` - Resolves Azure credentials from secrets
- `src/adapters/core_integration.rs` - Maps to core library config (needs update)
- `src/crd/kafka_backup.rs` - CRD definitions including Azure storage spec

**kafka-backup-core:**
- `src/config.rs` - Configuration structures (needs Azure fields)
- `src/storage/mod.rs` - Storage backend trait (needs Azure implementation)

### C. Azure Blob Storage Pricing Considerations

| Tier | Storage (per GB/month) | Operations (per 10,000) |
|------|------------------------|-------------------------|
| Hot | $0.0184 | Write: $0.055, Read: $0.0044 |
| Cool | $0.01 | Write: $0.10, Read: $0.01 |
| Archive | $0.00099 | Write: $0.11, Read: $5.50 |

**Recommendation:** Use Hot tier for active backups, consider lifecycle policies to move older backups to Cool/Archive.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2025-12-12 | StreamPlus Platform Team | Initial draft |