# Pluggable Storage Interface with Azure Blob Storage Support

## Overview

The `object_store` crate provides a **unified ObjectStore trait** that abstracts over multiple storage backends:
- AWS S3 (via `aws` feature)
- Google Cloud Storage (via `gcp` feature)
- Azure Blob Storage (via `azure` feature)
- Local filesystem (via `local` feature)
- In-memory storage
- HTTP/WebDAV

This means your Kafka backup engine can support **all storage backends with zero code changes** – just configuration.

---

## Architecture: Storage Abstraction Layer

### Design Principle

```
┌─────────────────────────────────────┐
│   Kafka Backup Engine               │
│  - Partition-level backup logic     │
│  - Segment writing + compression    │
│  - Offset tracking                  │
└─────────────────────┬───────────────┘
                      │
         Uses trait  │
                      │
┌─────────────────────▼───────────────┐
│    StorageBackend trait             │
│  - put()                            │
│  - get()                            │
│  - delete()                         │
│  - list()                           │
└─────────────────────┬───────────────┘
                      │
         Implements  │
        ┌────────────┼────────────┐
        ▼            ▼            ▼
    ┌────────┐  ┌────────┐  ┌────────┐
    │   S3   │  │ Azure  │  │  GCS   │
    │(object │  │(object │  │(object │
    │ store) │  │ store) │  │ store) │
    └────────┘  └────────┘  └────────┘
       + ceph     + blobs     + storage
       + minio    + adls2     + buckets
```

---

## Step 1: Add Dependencies

### Cargo.toml with Multi-Backend Support

```toml
[dependencies]
# Core async runtime
tokio = { version = "1", features = ["full"] }

# Storage abstraction - UNIFIED API for all backends
object_store = { version = "0.11", features = [
    "aws",      # S3 + S3-compatible (Ceph, MinIO)
    "azure",    # Azure Blob Storage + ADLS Gen2
    "gcp",      # Google Cloud Storage
    "http",     # HTTP/WebDAV
] }

# Authentication for cloud providers
aws-credential-types = "1"
azure_identity = "0.20"  # Azure AD authentication
azure_core = "0.20"

# Other deps
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
tracing = "0.1"
anyhow = "1"
thiserror = "1"
```

---

## Step 2: Define Storage Abstraction Trait

### File: `src/storage/backend.rs`

```rust
use anyhow::Result;
use bytes::Bytes;
use object_store::{ObjectStore, Path};
use std::sync::Arc;

/// Unified storage backend interface
/// 
/// Abstracts over AWS S3, Azure Blob, GCS, local filesystem, etc.
/// 
/// This trait is intentionally simple - it matches what object_store provides.
pub trait StorageBackend: Send + Sync {
    /// Put data at key (create or overwrite)
    fn put(&self, key: &str, data: Bytes) -> impl std::future::Future<Output = Result<()>> + Send;
    
    /// Get data at key
    fn get(&self, key: &str) -> impl std::future::Future<Output = Result<Bytes>> + Send;
    
    /// Delete object at key
    fn delete(&self, key: &str) -> impl std::future::Future<Output = Result<()>> + Send;
    
    /// List objects with prefix (like a directory)
    fn list(
        &self,
        prefix: &str,
    ) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;
    
    /// Get object metadata (size, last modified, etc.)
    fn head(&self, key: &str) -> impl std::future::Future<Output = Result<ObjectMetadata>> + Send;
    
    /// Copy object from source to destination
    fn copy(&self, src: &str, dest: &str) -> impl std::future::Future<Output = Result<()>> + Send;
}

#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    pub size: u64,
    pub modified: i64,  // epoch millis
}

/// Type-erased storage backend wrapper
/// 
/// Wraps the underlying object_store::ObjectStore into our trait.
pub struct StorageBackendImpl {
    inner: Arc<dyn ObjectStore>,
}

impl StorageBackendImpl {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl StorageBackend for StorageBackendImpl {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        self.inner
            .put(&key.try_into()?, data)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to put {}: {}", key, e))?;
        Ok(())
    }
    
    async fn get(&self, key: &str) -> Result<Bytes> {
        self.inner
            .get(&key.try_into()?)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get {}: {}", key, e))
            .map(|b| b.bytes())
    }
    
    async fn delete(&self, key: &str) -> Result<()> {
        self.inner
            .delete(&key.try_into()?)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to delete {}: {}", key, e))?;
        Ok(())
    }
    
    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let prefix_path: Path = prefix.try_into()?;
        let list_stream = self.inner.list(Some(&prefix_path)).await?;
        
        let mut results = Vec::new();
        futures::pin_mut!(list_stream);
        
        while let Some(obj_meta) = futures::stream::StreamExt::next(&mut list_stream).await {
            let obj_meta = obj_meta?;
            results.push(obj_meta.location.to_string());
        }
        
        Ok(results)
    }
    
    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let meta = self.inner
            .head(&key.try_into()?)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to head {}: {}", key, e))?;
        
        Ok(ObjectMetadata {
            size: meta.size,
            modified: meta.last_modified.timestamp_millis(),
        })
    }
    
    async fn copy(&self, src: &str, dest: &str) -> Result<()> {
        self.inner
            .copy(&src.try_into()?, &dest.try_into()?)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to copy {} to {}: {}", src, dest, e))?;
        Ok(())
    }
}
```

---

## Step 3: Configuration & Factory

### File: `src/storage/config.rs`

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend")]
pub enum StorageConfig {
    /// AWS S3 or S3-compatible (MinIO, Ceph RGW, etc.)
    #[serde(rename = "s3")]
    S3 {
        endpoint: String,
        bucket: String,
        region: Option<String>,
        access_key: String,
        secret_key: String,
        #[serde(default)]
        path_style: bool,  // For MinIO/Ceph RGW
    },
    
    /// Azure Blob Storage
    #[serde(rename = "azure")]
    Azure {
        account_name: String,
        container_name: String,
        account_key: Option<String>,  // If None, uses DefaultAzureCredential
    },
    
    /// Google Cloud Storage
    #[serde(rename = "gcs")]
    Gcs {
        bucket: String,
        service_account_path: Option<String>,
    },
    
    /// Local filesystem
    #[serde(rename = "filesystem")]
    Filesystem {
        path: String,
    },
    
    /// In-memory (for testing)
    #[serde(rename = "memory")]
    Memory,
}

impl StorageConfig {
    /// Parse from connection string
    /// 
    /// Examples:
    /// - "s3://bucket-name?endpoint=https://s3.amazonaws.com"
    /// - "azure://container@account.blob.core.windows.net"
    /// - "gcs://bucket-name"
    /// - "file:///path/to/data"
    pub fn from_url(url: &str) -> anyhow::Result<Self> {
        let parsed = url::Url::parse(url)?;
        
        match parsed.scheme() {
            "s3" | "s3a" => {
                Ok(Self::S3 {
                    endpoint: parsed.host_str().unwrap_or("s3.amazonaws.com").to_string(),
                    bucket: parsed.path().trim_start_matches('/').to_string(),
                    region: parsed.query_pairs()
                        .find(|(k, _)| k == "region")
                        .map(|(_, v)| v.to_string()),
                    access_key: std::env::var("AWS_ACCESS_KEY_ID")?,
                    secret_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
                    path_style: parsed.query_pairs()
                        .find(|(k, _)| k == "path_style")
                        .map(|(_, v)| v == "true")
                        .unwrap_or(false),
                })
            }
            "azure" => {
                let account_name = parsed.host_str()
                    .unwrap_or_default()
                    .split('.')
                    .next()
                    .unwrap_or("")
                    .to_string();
                
                Ok(Self::Azure {
                    account_name,
                    container_name: parsed.path().trim_start_matches('/').to_string(),
                    account_key: std::env::var("AZURE_STORAGE_KEY").ok(),
                })
            }
            "gcs" | "gs" => {
                Ok(Self::Gcs {
                    bucket: parsed.path().trim_start_matches('/').to_string(),
                    service_account_path: std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
                })
            }
            "file" => {
                Ok(Self::Filesystem {
                    path: parsed.path().to_string(),
                })
            }
            "memory" => {
                Ok(Self::Memory)
            }
            scheme => {
                Err(anyhow::anyhow!("Unknown storage scheme: {}", scheme))
            }
        }
    }
}
```

---

## Step 4: Storage Factory

### File: `src/storage/factory.rs`

```rust
use crate::storage::{StorageBackendImpl, StorageConfig};
use anyhow::Result;
use object_store::{
    aws::AmazonS3Builder,
    azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder,
    local::LocalFileSystem,
    memory::InMemory,
    ObjectStore,
};
use std::sync::Arc;

/// Factory for creating storage backends from config
pub struct StorageFactory;

impl StorageFactory {
    /// Create storage backend from configuration
    pub async fn create(config: &StorageConfig) -> Result<StorageBackendImpl> {
        let store: Arc<dyn ObjectStore> = match config {
            StorageConfig::S3 {
                endpoint,
                bucket,
                region,
                access_key,
                secret_key,
                path_style,
            } => {
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(bucket)
                    .with_access_key_id(access_key)
                    .with_secret_access_key(secret_key);
                
                // Configure for S3-compatible services (MinIO, Ceph RGW)
                if *path_style {
                    builder = builder.with_virtual_hosted_style();
                }
                
                if let Some(region) = region {
                    builder = builder.with_region(region);
                }
                
                // If endpoint is not AWS, set it (for MinIO, Ceph RGW, etc.)
                if !endpoint.contains("amazonaws.com") {
                    builder = builder.with_endpoint(endpoint);
                    builder = builder.with_virtual_hosted_style();
                }
                
                Arc::new(
                    builder
                        .build()
                        .map_err(|e| anyhow::anyhow!("Failed to build S3 client: {}", e))?,
                )
            }
            
            StorageConfig::Azure {
                account_name,
                container_name,
                account_key,
            } => {
                let mut builder = MicrosoftAzureBuilder::new()
                    .with_account(account_name)
                    .with_container_name(container_name);
                
                // If account_key provided, use it
                // Otherwise, Azure SDK will use DefaultAzureCredential chain:
                // 1. Environment variables
                // 2. Managed Identity
                // 3. Azure CLI credentials
                // 4. Azure AD device code flow
                if let Some(key) = account_key {
                    builder = builder.with_access_key(key);
                }
                
                Arc::new(
                    builder
                        .build()
                        .map_err(|e| anyhow::anyhow!("Failed to build Azure client: {}", e))?,
                )
            }
            
            StorageConfig::Gcs {
                bucket,
                service_account_path,
            } => {
                let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);
                
                if let Some(path) = service_account_path {
                    builder = builder.with_service_account_path(path);
                }
                
                Arc::new(
                    builder
                        .build()
                        .map_err(|e| anyhow::anyhow!("Failed to build GCS client: {}", e))?,
                )
            }
            
            StorageConfig::Filesystem { path } => {
                Arc::new(LocalFileSystem::new_with_prefix(path)?)
            }
            
            StorageConfig::Memory => Arc::new(InMemory::new()),
        };
        
        Ok(StorageBackendImpl::new(store))
    }
}
```

---

## Step 5: Integration with Backup Engine

### File: `src/backup/backup_engine.rs` (Updated)

```rust
use crate::storage::{StorageBackend, StorageBackendImpl, StorageConfig};
use anyhow::Result;
use std::sync::Arc;

pub struct BackupEngine {
    storage: Arc<dyn StorageBackend>,
    // ... other fields
}

impl BackupEngine {
    /// Create backup engine from storage config
    pub async fn new(storage_config: &StorageConfig) -> Result<Self> {
        // Create storage backend using factory
        let storage = Arc::new(StorageFactory::create(storage_config).await?);
        
        Ok(Self {
            storage,
            // ... initialize other fields
        })
    }
    
    /// Backup a topic/partition
    pub async fn backup_partition(&self, topic: &str, partition: i32) -> Result<()> {
        // Your existing backup logic here
        // Use self.storage to write segments
        
        let segment_data = vec![1, 2, 3];  // Your compressed segment
        let key = format!("backups/topic-{}/partition-{}/segment-001.zst", topic, partition);
        
        self.storage.put(&key, segment_data.into()).await?;
        
        Ok(())
    }
}
```

---

## Step 6: Configuration Examples

### YAML Configuration Examples

#### AWS S3

```yaml
# backup-config.yaml
storage:
  backend: s3
  endpoint: "https://s3.us-east-1.amazonaws.com"
  bucket: "kafka-backups"
  region: "us-east-1"
  access_key: "${AWS_ACCESS_KEY_ID}"
  secret_key: "${AWS_SECRET_ACCESS_KEY}"
  path_style: false
```

#### S3-Compatible (MinIO)

```yaml
storage:
  backend: s3
  endpoint: "http://minio.kafka-backup.svc:9000"
  bucket: "kafka-backups"
  access_key: "${MINIO_ACCESS_KEY}"
  secret_key: "${MINIO_SECRET_KEY}"
  path_style: true  # Critical for MinIO/Ceph
```

#### Azure Blob Storage (with Account Key)

```yaml
storage:
  backend: azure
  account_name: "mystorageaccount"
  container_name: "kafka-backups"
  account_key: "${AZURE_STORAGE_KEY}"
```

#### Azure Blob Storage (with Managed Identity / DefaultAzureCredential)

```yaml
storage:
  backend: azure
  account_name: "mystorageaccount"
  container_name: "kafka-backups"
  # account_key omitted - uses DefaultAzureCredential chain
  # Works with: env vars, managed identity, Azure CLI, device code flow
```

#### Google Cloud Storage

```yaml
storage:
  backend: gcs
  bucket: "kafka-backups"
  service_account_path: "/secrets/gcp-service-account.json"
```

#### Local Filesystem (Development)

```yaml
storage:
  backend: filesystem
  path: "/var/kafka-backups"
```

---

## Step 7: CLI Integration

### File: `src/cli/main.rs` (Updated)

```rust
use clap::{Parser, Subcommand};
use serde_yaml;
use std::fs;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    
    /// Storage backend URL (overrides config file)
    #[arg(long, global = true)]
    storage: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    Backup {
        /// Config file path
        #[arg(short, long)]
        config: String,
    },
    Restore {
        /// Config file path
        #[arg(short, long)]
        config: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    
    match cli.command {
        Commands::Backup { config } => {
            // Load config from file
            let config_str = fs::read_to_string(&config)?;
            let mut config: serde_yaml::Value = serde_yaml::from_str(&config_str)?;
            
            // Override storage if provided via CLI
            if let Some(storage_url) = cli.storage {
                config["storage"]["url"] = serde_yaml::to_value(storage_url)?;
            }
            
            // Create backup engine with resolved storage config
            let storage_config = serde_yaml::from_value(config["storage"].clone())?;
            let engine = BackupEngine::new(&storage_config).await?;
            
            engine.backup_partition("orders", 0).await?;
        }
        Commands::Restore { config } => {
            // Similar pattern for restore
            todo!()
        }
    }
    
    Ok(())
}
```

---

## Step 8: Running with Different Backends

### Run with AWS S3

```bash
kafka-backup backup --config config.yaml
```

With environment variables:
```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

kafka-backup backup --config config.yaml
```

### Run with Azure Blob Storage

```bash
# Using account key
export AZURE_STORAGE_KEY=myaccountkey

kafka-backup backup --config config-azure.yaml
```

Or with managed identity (automatic):
```bash
# No env var needed - uses DefaultAzureCredential chain
# Works with service principal, managed identity, etc.

kafka-backup backup --config config-azure.yaml
```

### Run with MinIO (S3-compatible)

```bash
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin

kafka-backup backup --config config-minio.yaml
```

### Run with Local Filesystem (Development)

```bash
kafka-backup backup --config config-local.yaml
```

---

## Key Design Benefits

### 1. **Single Codebase, Multiple Backends**
- No backend-specific code in backup engine
- Switch backends with config change only
- Identical performance characteristics

### 2. **Object Store Crate Handles Complexity**
- Multi-part uploads for large files
- Retry logic for transient failures
- Credential management
- Region-aware routing

### 3. **Azure Credential Chain (DefaultAzureCredential)**
- Automatically tries multiple auth methods
- Environment variables
- Managed Identity (for VMs, Kubernetes, etc.)
- Azure CLI credentials
- Device code flow
- No hardcoding secrets needed

### 4. **S3-Compatible Services**
- Works with AWS S3, MinIO, Ceph RGW, DigitalOcean Spaces, etc.
- Single S3 backend implementation
- Path-style vs virtual-hosted-style configurable

### 5. **Easy Testing**
- In-memory backend for unit tests
- Local filesystem for integration tests
- No network I/O in tests

---

## Performance Considerations

### Multipart Upload (Automatic)

```rust
// object_store handles this automatically
// Large segments automatically split into parts
self.storage.put(&key, large_data).await?;

// For Kafka backup segments:
// - 128MB segment → 4 parts of 32MB each (configurable)
// - Uploaded in parallel
// - All-or-nothing atomicity
```

### Batching with object_store

```rust
// object_store respects put_multipart settings
// See config builder for tuning:
// - chunk_size: Size of each part
// - parts: Number of parallel uploads
// - retries: Automatic retry on transient failures
```

---

## Migration Path

### Week 1: S3-Compatible (MinIO/Ceph)
```rust
StorageConfig::S3 { ... }  // Already works
```

### Week 2: Add Azure Support
```rust
StorageConfig::Azure { ... }  // object_store handles it
```

### Week 3: Add GCS Support
```rust
StorageConfig::Gcs { ... }  // object_store handles it
```

### Week 4: Add Local FS Support
```rust
StorageConfig::Filesystem { ... }  // For testing
```

**Total code changes:** ~200 lines (mostly config handling)

---

## Testing Strategy

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_backup_with_memory_storage() {
        let config = StorageConfig::Memory;
        let engine = BackupEngine::new(&config).await.unwrap();
        
        // Test backup logic without network I/O
        engine.backup_partition("test-topic", 0).await.unwrap();
    }
    
    #[tokio::test]
    #[ignore]  // Run with: cargo test -- --ignored
    async fn test_backup_with_azure() {
        let config = StorageConfig::Azure {
            account_name: std::env::var("AZURE_ACCOUNT").unwrap(),
            container_name: "test-backups".to_string(),
            account_key: None,  // Uses DefaultAzureCredential
        };
        let engine = BackupEngine::new(&config).await.unwrap();
        
        engine.backup_partition("test-topic", 0).await.unwrap();
    }
}
```

---

## Summary

**With `object_store` crate, you get:**

| Feature | Benefit |
|---------|---------|
| Unified trait | Single API for all backends |
| Azure built-in | Full Azure Blob + ADLS2 support |
| Multipart uploads | Automatic for large files |
| Credential chains | No secret management hassle |
| S3-compatible | Works with any S3-like service |
| Tested | Production-grade implementation |
| **Lines of code** | ~300 for pluggable storage layer |

**Implementation time:** 2-3 days for full multi-backend support

**Maintenance burden:** Minimal (object_store community maintains backends)
