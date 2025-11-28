# Storage API Reference

Complete API documentation for the Kafka Backup pluggable storage interface.

---

## Module: `kafka_backup_core::storage`

### Exports

```rust
pub use azure::{AzureBackend, AzureConfig};
pub use backend::{ObjectMetadata, StorageBackend};
pub use config::StorageBackendConfig;
pub use filesystem::FilesystemBackend;
pub use gcs::{GcsBackend, GcsConfig};
pub use memory::MemoryBackend;
pub use s3::{S3Backend, S3Config};

pub fn create_backend_from_config(config: &StorageBackendConfig) -> Result<Arc<dyn StorageBackend>>;
pub fn create_backend(config: &crate::config::StorageConfig) -> Result<Arc<dyn StorageBackend>>;
```

---

## Traits

### `StorageBackend`

**Location:** `storage/backend.rs`

The core abstraction for all storage operations.

```rust
#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn put(&self, key: &str, data: Bytes) -> Result<()>;
    async fn get(&self, key: &str) -> Result<Bytes>;
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;
    async fn exists(&self, key: &str) -> Result<bool>;
    async fn delete(&self, key: &str) -> Result<()>;
    async fn size(&self, key: &str) -> Result<u64>;
    async fn head(&self, key: &str) -> Result<ObjectMetadata>;
    async fn copy(&self, src: &str, dest: &str) -> Result<()>;
}
```

#### `put`

Writes data to storage at the specified key. Creates or overwrites existing data.

```rust
async fn put(&self, key: &str, data: Bytes) -> Result<()>
```

**Parameters:**
- `key` - Storage key/path (e.g., `"backups/2024/manifest.json"`)
- `data` - Binary data to store

**Returns:** `Result<()>`

**Errors:**
- `StorageError::Backend` - Write operation failed
- `StorageError::PermissionDenied` - Insufficient permissions

**Example:**
```rust
let data = Bytes::from(r#"{"version": 1}"#);
backend.put("backups/manifest.json", data).await?;
```

---

#### `get`

Reads data from storage at the specified key.

```rust
async fn get(&self, key: &str) -> Result<Bytes>
```

**Parameters:**
- `key` - Storage key/path to read

**Returns:** `Result<Bytes>` - The stored data

**Errors:**
- `StorageError::NotFound` - Key does not exist
- `StorageError::Backend` - Read operation failed
- `StorageError::PermissionDenied` - Insufficient permissions

**Example:**
```rust
let data = backend.get("backups/manifest.json").await?;
let manifest: Manifest = serde_json::from_slice(&data)?;
```

---

#### `list`

Lists all keys with the specified prefix.

```rust
async fn list(&self, prefix: &str) -> Result<Vec<String>>
```

**Parameters:**
- `prefix` - Key prefix to filter by (e.g., `"backups/"` or `""` for all)

**Returns:** `Result<Vec<String>>` - List of matching keys

**Errors:**
- `StorageError::Backend` - List operation failed

**Notes:**
- Returns keys with configured storage prefix already stripped
- Empty prefix (`""`) returns all keys
- Results are sorted alphabetically

**Example:**
```rust
// List all backup manifests
let manifests = backend.list("backups/").await?;
for key in manifests {
    if key.ends_with("manifest.json") {
        println!("Found: {}", key);
    }
}
```

---

#### `exists`

Checks if a key exists in storage.

```rust
async fn exists(&self, key: &str) -> Result<bool>
```

**Parameters:**
- `key` - Storage key/path to check

**Returns:** `Result<bool>` - `true` if exists, `false` otherwise

**Errors:**
- `StorageError::Backend` - Check operation failed

**Example:**
```rust
if backend.exists("backups/manifest.json").await? {
    println!("Manifest found");
} else {
    println!("No existing backup");
}
```

---

#### `delete`

Deletes the object at the specified key.

```rust
async fn delete(&self, key: &str) -> Result<()>
```

**Parameters:**
- `key` - Storage key/path to delete

**Returns:** `Result<()>`

**Errors:**
- `StorageError::NotFound` - Key does not exist
- `StorageError::Backend` - Delete operation failed
- `StorageError::PermissionDenied` - Insufficient permissions

**Example:**
```rust
// Delete old backup
backend.delete("backups/old/manifest.json").await?;
```

---

#### `size`

Gets the size of an object in bytes.

```rust
async fn size(&self, key: &str) -> Result<u64>
```

**Parameters:**
- `key` - Storage key/path

**Returns:** `Result<u64>` - Size in bytes

**Errors:**
- `StorageError::NotFound` - Key does not exist
- `StorageError::Backend` - Operation failed

**Example:**
```rust
let size = backend.size("backups/segment-001.zst").await?;
println!("Segment size: {} bytes", size);
```

---

#### `head`

Gets object metadata without downloading the content.

```rust
async fn head(&self, key: &str) -> Result<ObjectMetadata>
```

**Parameters:**
- `key` - Storage key/path

**Returns:** `Result<ObjectMetadata>`

**Errors:**
- `StorageError::NotFound` - Key does not exist
- `StorageError::Backend` - Operation failed

**Example:**
```rust
let meta = backend.head("backups/segment-001.zst").await?;
println!("Size: {} bytes", meta.size);
println!("Modified: {}", meta.last_modified);
if let Some(etag) = meta.e_tag {
    println!("ETag: {}", etag);
}
```

---

#### `copy`

Copies an object from source to destination within the same backend.

```rust
async fn copy(&self, src: &str, dest: &str) -> Result<()>
```

**Parameters:**
- `src` - Source key/path
- `dest` - Destination key/path

**Returns:** `Result<()>`

**Errors:**
- `StorageError::NotFound` - Source key does not exist
- `StorageError::Backend` - Copy operation failed

**Example:**
```rust
// Create a backup copy
backend.copy("backups/manifest.json", "backups/manifest.json.bak").await?;
```

---

## Structs

### `ObjectMetadata`

**Location:** `storage/backend.rs`

Metadata about a stored object.

```rust
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// Size in bytes
    pub size: u64,

    /// Last modified timestamp (epoch milliseconds)
    pub last_modified: i64,

    /// ETag or content hash (if available)
    pub e_tag: Option<String>,
}
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `size` | `u64` | Object size in bytes |
| `last_modified` | `i64` | Unix timestamp in milliseconds |
| `e_tag` | `Option<String>` | Content hash (cloud backends only) |

---

### `S3Config`

**Location:** `storage/s3.rs`

Configuration for S3 and S3-compatible backends.

```rust
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<String>,
    pub secret_access_key: Option<String>,
    pub prefix: Option<String>,
    pub allow_http: bool,
}
```

**Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | `String` | Required | S3 bucket name |
| `region` | `Option<String>` | `"us-east-1"` | AWS region |
| `endpoint` | `Option<String>` | `None` | Custom endpoint URL |
| `access_key_id` | `Option<String>` | `None` | AWS access key |
| `secret_access_key` | `Option<String>` | `None` | AWS secret key |
| `prefix` | `Option<String>` | `None` | Key prefix |
| `allow_http` | `bool` | `false` | Allow insecure HTTP |

**Example:**
```rust
let config = S3Config {
    bucket: "kafka-backups".to_string(),
    region: Some("us-west-2".to_string()),
    endpoint: None,
    access_key_id: None,  // Uses AWS_ACCESS_KEY_ID
    secret_access_key: None,  // Uses AWS_SECRET_ACCESS_KEY
    prefix: Some("prod-cluster".to_string()),
    allow_http: false,
};
let backend = S3Backend::new(config)?;
```

---

### `AzureConfig`

**Location:** `storage/azure.rs`

Configuration for Azure Blob Storage backend.

```rust
#[derive(Debug, Clone)]
pub struct AzureConfig {
    pub account_name: String,
    pub container_name: String,
    pub account_key: Option<String>,
    pub prefix: Option<String>,
}
```

**Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `account_name` | `String` | Required | Storage account name |
| `container_name` | `String` | Required | Blob container name |
| `account_key` | `Option<String>` | `None` | Account key (uses DefaultAzureCredential if None) |
| `prefix` | `Option<String>` | `None` | Key prefix |

**Example:**
```rust
let config = AzureConfig {
    account_name: "mystorageaccount".to_string(),
    container_name: "kafka-backups".to_string(),
    account_key: None,  // Uses Managed Identity
    prefix: Some("prod".to_string()),
};
let backend = AzureBackend::new(config)?;
```

---

### `GcsConfig`

**Location:** `storage/gcs.rs`

Configuration for Google Cloud Storage backend.

```rust
#[derive(Debug, Clone)]
pub struct GcsConfig {
    pub bucket: String,
    pub service_account_path: Option<String>,
    pub prefix: Option<String>,
}
```

**Fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `bucket` | `String` | Required | GCS bucket name |
| `service_account_path` | `Option<String>` | `None` | Service account JSON path |
| `prefix` | `Option<String>` | `None` | Key prefix |

**Example:**
```rust
let config = GcsConfig {
    bucket: "kafka-backups".to_string(),
    service_account_path: None,  // Uses Application Default Credentials
    prefix: Some("prod".to_string()),
};
let backend = GcsBackend::new(config)?;
```

---

## Enums

### `StorageBackendConfig`

**Location:** `storage/config.rs`

Tagged enum for type-safe storage configuration. Supports YAML/JSON serialization.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend")]
pub enum StorageBackendConfig {
    #[serde(rename = "s3")]
    S3 {
        bucket: String,
        #[serde(default)]
        region: Option<String>,
        #[serde(default)]
        endpoint: Option<String>,
        #[serde(default)]
        access_key: Option<String>,
        #[serde(default)]
        secret_key: Option<String>,
        #[serde(default)]
        prefix: Option<String>,
        #[serde(default)]
        path_style: bool,
        #[serde(default)]
        allow_http: bool,
    },

    #[serde(rename = "azure")]
    Azure {
        account_name: String,
        container_name: String,
        #[serde(default)]
        account_key: Option<String>,
        #[serde(default)]
        prefix: Option<String>,
    },

    #[serde(rename = "gcs")]
    Gcs {
        bucket: String,
        #[serde(default)]
        service_account_path: Option<String>,
        #[serde(default)]
        prefix: Option<String>,
    },

    #[serde(rename = "filesystem")]
    Filesystem {
        path: PathBuf,
    },

    #[serde(rename = "memory")]
    Memory,
}
```

#### Methods

##### `from_url`

Parses configuration from a URL string.

```rust
pub fn from_url(url: &str) -> Result<Self>
```

**Parameters:**
- `url` - URL string (see supported formats below)

**Returns:** `Result<StorageBackendConfig>`

**Supported URL Formats:**

| Scheme | Example |
|--------|---------|
| `s3://` | `s3://bucket?region=us-east-1` |
| `azure://` | `azure://account.blob.core.windows.net/container` |
| `gcs://` | `gcs://bucket` |
| `gs://` | `gs://bucket` |
| `file://` | `file:///var/backups` |
| `memory://` | `memory://` |

**Example:**
```rust
let config = StorageBackendConfig::from_url("s3://my-bucket?region=us-west-2")?;
let backend = create_backend_from_config(&config)?;
```

##### `prefix`

Returns the configured prefix, if any.

```rust
pub fn prefix(&self) -> Option<&str>
```

**Returns:** `Option<&str>` - The prefix or `None`

---

## Factory Functions

### `create_backend_from_config`

**Location:** `storage/mod.rs`

Creates a storage backend from the new tagged enum configuration.

```rust
pub fn create_backend_from_config(
    config: &StorageBackendConfig,
) -> Result<Arc<dyn StorageBackend>>
```

**Parameters:**
- `config` - Storage backend configuration

**Returns:** `Result<Arc<dyn StorageBackend>>` - Thread-safe storage backend

**Example:**
```rust
use kafka_backup_core::storage::{create_backend_from_config, StorageBackendConfig};

// From enum variant
let config = StorageBackendConfig::Memory;
let backend = create_backend_from_config(&config)?;

// From YAML
let yaml = r#"
backend: s3
bucket: kafka-backups
region: us-east-1
"#;
let config: StorageBackendConfig = serde_yaml::from_str(yaml)?;
let backend = create_backend_from_config(&config)?;

// From URL
let config = StorageBackendConfig::from_url("s3://bucket?region=us-east-1")?;
let backend = create_backend_from_config(&config)?;
```

---

### `create_backend`

**Location:** `storage/mod.rs`

Creates a storage backend from the legacy flat configuration structure.

```rust
pub fn create_backend(
    config: &crate::config::StorageConfig,
) -> Result<Arc<dyn StorageBackend>>
```

**Parameters:**
- `config` - Legacy storage configuration

**Returns:** `Result<Arc<dyn StorageBackend>>`

**Note:** Maintained for backward compatibility. Prefer `create_backend_from_config` for new code.

---

## Backend Implementations

### `S3Backend`

**Location:** `storage/s3.rs`

S3 and S3-compatible storage backend.

```rust
pub struct S3Backend { /* private */ }

impl S3Backend {
    pub fn new(config: S3Config) -> Result<Self>
}
```

**Supported Services:**
- AWS S3
- MinIO
- Ceph RGW
- DigitalOcean Spaces
- Backblaze B2
- Any S3-compatible API

---

### `AzureBackend`

**Location:** `storage/azure.rs`

Azure Blob Storage backend.

```rust
pub struct AzureBackend { /* private */ }

impl AzureBackend {
    pub fn new(config: AzureConfig) -> Result<Self>
}
```

**Authentication:**
1. Explicit `account_key` in config
2. `AZURE_STORAGE_KEY` environment variable
3. DefaultAzureCredential chain (service principal, managed identity, CLI)

---

### `GcsBackend`

**Location:** `storage/gcs.rs`

Google Cloud Storage backend.

```rust
pub struct GcsBackend { /* private */ }

impl GcsBackend {
    pub fn new(config: GcsConfig) -> Result<Self>
}
```

**Authentication:**
1. Explicit `service_account_path` in config
2. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
3. Application Default Credentials (SDK, metadata service)

---

### `FilesystemBackend`

**Location:** `storage/filesystem.rs`

Local filesystem storage backend.

```rust
pub struct FilesystemBackend { /* private */ }

impl FilesystemBackend {
    pub fn new(base_path: PathBuf) -> Self
}
```

**Notes:**
- Automatically creates parent directories on `put`
- Normalizes keys to prevent path traversal attacks
- Returns `None` for `e_tag` in metadata

---

### `MemoryBackend`

**Location:** `storage/memory.rs`

In-memory storage backend for testing.

```rust
pub struct MemoryBackend { /* private */ }

impl MemoryBackend {
    pub fn new() -> Self
}

impl Default for MemoryBackend {
    fn default() -> Self
}
```

**Notes:**
- Data is lost when the backend is dropped
- Useful for unit tests
- Not suitable for production use

---

## Error Types

### `StorageError`

**Location:** `error.rs`

```rust
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Object not found: {0}")]
    NotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),
}
```

**Variants:**

| Variant | Description | Common Causes |
|---------|-------------|---------------|
| `NotFound` | Object doesn't exist | `get`, `delete`, `size`, `head` on missing key |
| `PermissionDenied` | Access denied | Invalid credentials, missing IAM permissions |
| `Backend` | Backend operation failed | Network error, service unavailable |
| `InvalidPath` | Invalid key/path | Path traversal attempt, invalid characters |

---

## Complete Example

```rust
use bytes::Bytes;
use kafka_backup_core::storage::{
    create_backend_from_config,
    ObjectMetadata,
    StorageBackend,
    StorageBackendConfig,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create backend from config
    let config = StorageBackendConfig::S3 {
        bucket: "kafka-backups".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: None,
        access_key: None,
        secret_key: None,
        prefix: Some("prod-cluster".to_string()),
        path_style: false,
        allow_http: false,
    };

    let backend: Arc<dyn StorageBackend> = create_backend_from_config(&config)?;

    // 2. Write data
    let manifest = r#"{"backup_id": "backup-001", "version": 1}"#;
    backend.put("backup-001/manifest.json", Bytes::from(manifest)).await?;

    // 3. Check existence
    assert!(backend.exists("backup-001/manifest.json").await?);

    // 4. Read data
    let data = backend.get("backup-001/manifest.json").await?;
    println!("Manifest: {}", String::from_utf8_lossy(&data));

    // 5. Get metadata
    let meta: ObjectMetadata = backend.head("backup-001/manifest.json").await?;
    println!("Size: {} bytes, Modified: {}", meta.size, meta.last_modified);

    // 6. List objects
    let keys = backend.list("backup-001/").await?;
    println!("Found {} objects", keys.len());

    // 7. Copy object
    backend.copy("backup-001/manifest.json", "backup-001/manifest.json.bak").await?;

    // 8. Delete object
    backend.delete("backup-001/manifest.json.bak").await?;

    Ok(())
}
```
