# Pluggable Storage Interface Guide

This guide covers the pluggable storage interface for Kafka Backup, providing support for multiple cloud storage providers and local storage options.

---

## Table of Contents

1. [Overview](#overview)
2. [Quickstart Guide](#quickstart-guide)
3. [API Reference](#api-reference)
4. [Configuration Reference](#configuration-reference)
5. [Deployment Guide](#deployment-guide)
6. [Troubleshooting](#troubleshooting)

---

## Overview

The Kafka Backup storage interface provides a unified API for storing backup data across multiple storage backends:

| Backend | Description | Use Case |
|---------|-------------|----------|
| **S3** | AWS S3 and S3-compatible services | Production cloud deployments |
| **Azure** | Azure Blob Storage | Azure-native environments |
| **GCS** | Google Cloud Storage | GCP-native environments |
| **Filesystem** | Local filesystem | Development, on-premise |
| **Memory** | In-memory storage | Testing only |

### Key Features

- **Unified API**: All backends implement the same `StorageBackend` trait
- **Zero code changes**: Switch backends with configuration only
- **Automatic credential chains**: Supports environment variables and cloud credential providers
- **Prefix support**: Organize backups by cluster, environment, or tenant
- **Circuit breaker integration**: Built-in fault tolerance for cloud backends

### Architecture

```
┌─────────────────────────────────────┐
│     Backup/Restore Engine           │
│  - Partition-level backup logic     │
│  - Segment writing + compression    │
│  - Offset tracking                  │
└─────────────────────┬───────────────┘
                      │
         Uses trait   │
                      ▼
┌─────────────────────────────────────┐
│      StorageBackend trait           │
│  put() | get() | list() | delete()  │
│  exists() | size() | head() | copy()│
└─────────────────────┬───────────────┘
                      │
         Implements   │
        ┌─────────────┼─────────────┬─────────────┐
        ▼             ▼             ▼             ▼
   ┌────────┐   ┌────────┐   ┌────────┐   ┌────────────┐
   │   S3   │   │ Azure  │   │  GCS   │   │ Filesystem │
   └────────┘   └────────┘   └────────┘   └────────────┘
```

---

## Quickstart Guide

### 1. Local Filesystem (Development)

The simplest way to get started:

```yaml
# backup-config.yaml
mode: backup
backup_id: "my-first-backup"

source:
  bootstrap_servers:
    - localhost:9092
  topics:
    include:
      - my-topic

storage:
  backend: filesystem
  path: /var/kafka-backups
```

Run the backup:

```bash
kafka-backup backup --config backup-config.yaml
```

### 2. AWS S3

```yaml
storage:
  backend: s3
  bucket: kafka-backups
  region: us-east-1
```

Set credentials via environment:

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

kafka-backup backup --config backup-config.yaml
```

### 3. MinIO (S3-Compatible)

```yaml
storage:
  backend: s3
  bucket: kafka-backups
  endpoint: http://minio.local:9000
  access_key: minioadmin
  secret_key: minioadmin
  path_style: true
  allow_http: true
```

### 4. Azure Blob Storage

```yaml
storage:
  backend: azure
  account_name: mystorageaccount
  container_name: kafka-backups
```

Using account key:

```bash
export AZURE_STORAGE_KEY=your-storage-account-key
kafka-backup backup --config backup-config.yaml
```

Or with Managed Identity (no environment variable needed):

```yaml
storage:
  backend: azure
  account_name: mystorageaccount
  container_name: kafka-backups
  # account_key omitted - uses DefaultAzureCredential
```

### 5. Google Cloud Storage

```yaml
storage:
  backend: gcs
  bucket: kafka-backups
```

Using service account:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
kafka-backup backup --config backup-config.yaml
```

### 6. URL-Based Configuration (CLI)

```bash
# S3
kafka-backup list --path "s3://my-bucket?region=us-west-2"

# Azure
kafka-backup list --path "azure://account.blob.core.windows.net/container"

# GCS
kafka-backup list --path "gcs://my-bucket"

# Filesystem
kafka-backup list --path "file:///var/kafka-backups"
```

### 7. Using in Rust Code

```rust
use kafka_backup_core::storage::{
    create_backend_from_config,
    StorageBackendConfig,
    StorageBackend,
};
use bytes::Bytes;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create backend from config
    let config = StorageBackendConfig::S3 {
        bucket: "kafka-backups".to_string(),
        region: Some("us-east-1".to_string()),
        endpoint: None,
        access_key: None,  // Uses AWS_ACCESS_KEY_ID env var
        secret_key: None,  // Uses AWS_SECRET_ACCESS_KEY env var
        prefix: Some("prod-cluster".to_string()),
        path_style: false,
        allow_http: false,
    };

    let backend = create_backend_from_config(&config)?;

    // Write data
    backend.put("backups/manifest.json", Bytes::from("{}")).await?;

    // Read data
    let data = backend.get("backups/manifest.json").await?;

    // List objects
    let keys = backend.list("backups/").await?;

    // Check existence
    if backend.exists("backups/manifest.json").await? {
        println!("Manifest exists!");
    }

    Ok(())
}
```

---

## API Reference

### StorageBackend Trait

The core trait implemented by all storage backends.

```rust
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Write data to a key (creates or overwrites)
    async fn put(&self, key: &str, data: Bytes) -> Result<()>;

    /// Read data from a key
    async fn get(&self, key: &str) -> Result<Bytes>;

    /// List keys with a given prefix
    async fn list(&self, prefix: &str) -> Result<Vec<String>>;

    /// Check if a key exists
    async fn exists(&self, key: &str) -> Result<bool>;

    /// Delete a key
    async fn delete(&self, key: &str) -> Result<()>;

    /// Get the size of an object in bytes
    async fn size(&self, key: &str) -> Result<u64>;

    /// Get object metadata (size, last modified, etag)
    async fn head(&self, key: &str) -> Result<ObjectMetadata>;

    /// Copy object from source to destination
    async fn copy(&self, src: &str, dest: &str) -> Result<()>;
}
```

#### Method Details

| Method | Description | Returns | Errors |
|--------|-------------|---------|--------|
| `put` | Write/overwrite data at key | `()` | `Backend` on write failure |
| `get` | Read data from key | `Bytes` | `NotFound` if key missing |
| `list` | List keys with prefix | `Vec<String>` | `Backend` on enumeration failure |
| `exists` | Check key existence | `bool` | `Backend` on check failure |
| `delete` | Remove key | `()` | `NotFound` if key missing |
| `size` | Get object size | `u64` | `NotFound` if key missing |
| `head` | Get object metadata | `ObjectMetadata` | `NotFound` if key missing |
| `copy` | Copy object | `()` | `NotFound` if source missing |

### ObjectMetadata

```rust
#[derive(Debug, Clone)]
pub struct ObjectMetadata {
    /// Size in bytes
    pub size: u64,

    /// Last modified timestamp (epoch milliseconds)
    pub last_modified: i64,

    /// ETag or content hash (cloud backends only)
    pub e_tag: Option<String>,
}
```

### Factory Functions

#### create_backend_from_config (Recommended)

```rust
pub fn create_backend_from_config(
    config: &StorageBackendConfig,
) -> Result<Arc<dyn StorageBackend>>
```

Creates a storage backend from the new tagged enum configuration. Supports all backend types.

**Example:**

```rust
let config = StorageBackendConfig::Memory;
let backend = create_backend_from_config(&config)?;
```

#### create_backend (Legacy)

```rust
pub fn create_backend(
    config: &crate::config::StorageConfig,
) -> Result<Arc<dyn StorageBackend>>
```

Creates a storage backend from the legacy flat configuration structure. Maintained for backward compatibility.

### StorageBackendConfig

Tagged enum for type-safe configuration.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend")]
pub enum StorageBackendConfig {
    S3 { /* fields */ },
    Azure { /* fields */ },
    Gcs { /* fields */ },
    Filesystem { path: PathBuf },
    Memory,
}

impl StorageBackendConfig {
    /// Parse from URL string
    pub fn from_url(url: &str) -> Result<Self>;

    /// Get configured prefix
    pub fn prefix(&self) -> Option<&str>;
}
```

### Backend-Specific Configuration Types

#### S3Config

```rust
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

#### AzureConfig

```rust
pub struct AzureConfig {
    pub account_name: String,
    pub container_name: String,
    pub account_key: Option<String>,
    pub prefix: Option<String>,
}
```

#### GcsConfig

```rust
pub struct GcsConfig {
    pub bucket: String,
    pub service_account_path: Option<String>,
    pub prefix: Option<String>,
}
```

### Error Types

```rust
pub enum StorageError {
    /// Object not found
    NotFound(String),

    /// Permission denied
    PermissionDenied(String),

    /// Backend error (connection, API, IO)
    Backend(String),

    /// Invalid path
    InvalidPath(String),
}
```

---

## Configuration Reference

### S3 / S3-Compatible

```yaml
storage:
  backend: s3

  # Required
  bucket: kafka-backups

  # Optional - AWS region (default: us-east-1)
  region: us-east-1

  # Optional - Custom endpoint for S3-compatible services
  endpoint: http://minio.local:9000

  # Optional - Credentials (falls back to AWS_ACCESS_KEY_ID)
  access_key: AKIAIOSFODNN7EXAMPLE

  # Optional - Credentials (falls back to AWS_SECRET_ACCESS_KEY)
  secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

  # Optional - Key prefix for all operations
  prefix: cluster-prod

  # Optional - Use path-style requests (required for MinIO/Ceph)
  path_style: false

  # Optional - Allow HTTP connections (insecure)
  allow_http: false
```

**Environment Variables:**
- `AWS_ACCESS_KEY_ID` - Access key (fallback)
- `AWS_SECRET_ACCESS_KEY` - Secret key (fallback)
- `AWS_REGION` - Region (SDK default)

### Azure Blob Storage

```yaml
storage:
  backend: azure

  # Required
  account_name: mystorageaccount
  container_name: kafka-backups

  # Optional - Account key (uses DefaultAzureCredential if omitted)
  account_key: your-account-key

  # Optional - Key prefix for all operations
  prefix: cluster-prod
```

**Environment Variables:**
- `AZURE_STORAGE_KEY` - Account key (fallback)
- `AZURE_TENANT_ID` - Tenant ID for service principal
- `AZURE_CLIENT_ID` - Client ID for service principal
- `AZURE_CLIENT_SECRET` - Client secret for service principal

**Credential Chain (when account_key omitted):**
1. Environment variables (service principal)
2. Managed Identity
3. Azure CLI credentials
4. Azure Developer CLI credentials

### Google Cloud Storage

```yaml
storage:
  backend: gcs

  # Required
  bucket: kafka-backups

  # Optional - Service account JSON path (uses ADC if omitted)
  service_account_path: /path/to/service-account.json

  # Optional - Key prefix for all operations
  prefix: cluster-prod
```

**Environment Variables:**
- `GOOGLE_APPLICATION_CREDENTIALS` - Service account JSON path (fallback)

**Credential Chain (when service_account_path omitted):**
1. `GOOGLE_APPLICATION_CREDENTIALS` environment variable
2. Google Cloud SDK default credentials
3. Compute Engine/GKE metadata service

### Local Filesystem

```yaml
storage:
  backend: filesystem

  # Required - Base path for storage
  path: /var/kafka-backups
```

### In-Memory (Testing)

```yaml
storage:
  backend: memory
```

### URL Formats

| Backend | URL Format |
|---------|------------|
| S3 | `s3://bucket?region=us-east-1&endpoint=http://host:9000` |
| Azure | `azure://account.blob.core.windows.net/container` |
| GCS | `gcs://bucket` or `gs://bucket` |
| Filesystem | `file:///path/to/storage` |
| Memory | `memory://` |

---

## Deployment Guide

### Docker Deployment

#### Dockerfile

```dockerfile
FROM rust:1.75 as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/kafka-backup /usr/local/bin/
ENTRYPOINT ["kafka-backup"]
```

#### Docker Compose with MinIO

```yaml
version: '3.8'
services:
  kafka-backup:
    build: .
    environment:
      - AWS_ACCESS_KEY_ID=minioadmin
      - AWS_SECRET_ACCESS_KEY=minioadmin
    volumes:
      - ./config:/config
    command: backup --config /config/backup.yaml
    depends_on:
      - minio
      - kafka

  minio:
    image: minio/minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data

volumes:
  minio-data:
```

### Kubernetes Deployment

#### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kafka-backup-config
data:
  backup.yaml: |
    mode: backup
    backup_id: "k8s-backup"

    source:
      bootstrap_servers:
        - kafka.kafka.svc.cluster.local:9092
      topics:
        include:
          - "*"
        exclude:
          - "__*"

    storage:
      backend: s3
      bucket: kafka-backups
      region: us-east-1
      prefix: k8s-cluster

    backup:
      compression: zstd
      continuous: true

    # Enable metrics for Prometheus scraping
    metrics:
      enabled: true
      port: 8080
      bind_address: "0.0.0.0"  # Required for K8s - allows Service routing
      path: "/metrics"
```

> **Important:** When running in Kubernetes, set `bind_address: "0.0.0.0"` to allow the Service to route traffic to the metrics endpoint. The default `127.0.0.1` only accepts localhost connections.

#### Secret (AWS Credentials)

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-backup-aws
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: AKIAIOSFODNN7EXAMPLE
  AWS_SECRET_ACCESS_KEY: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

#### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-backup
  labels:
    app: kafka-backup
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-backup
  template:
    metadata:
      labels:
        app: kafka-backup
    spec:
      containers:
        - name: kafka-backup
          image: ghcr.io/osodevops/kafka-backup:latest
          args:
            - backup
            - --config
            - /config/backup.yaml
          ports:
            - name: metrics
              containerPort: 8080
              protocol: TCP
          livenessProbe:
            httpGet:
              path: /health
              port: metrics
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /health
              port: metrics
            initialDelaySeconds: 5
            periodSeconds: 10
          envFrom:
            - secretRef:
                name: kafka-backup-aws
          volumeMounts:
            - name: config
              mountPath: /config
          resources:
            requests:
              memory: "256Mi"
              cpu: "100m"
            limits:
              memory: "1Gi"
              cpu: "500m"
      volumes:
        - name: config
          configMap:
            name: kafka-backup-config
```

#### Service (Metrics Endpoint)

Create a Service to expose the metrics endpoint for Prometheus scraping:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: kafka-backup-metrics
  labels:
    app: kafka-backup
spec:
  selector:
    app: kafka-backup
  ports:
    - name: metrics
      port: 8080
      targetPort: metrics
      protocol: TCP
  type: ClusterIP
```

#### ServiceMonitor (Prometheus Operator)

If using the Prometheus Operator, create a ServiceMonitor:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: kafka-backup
  labels:
    app: kafka-backup
    release: prometheus  # Match your Prometheus serviceMonitorSelector
spec:
  selector:
    matchLabels:
      app: kafka-backup
  endpoints:
    - port: metrics
      interval: 15s
      path: /metrics
```

#### CronJob (Scheduled Backups)

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: kafka-backup-scheduled
spec:
  schedule: "0 */6 * * *"  # Every 6 hours
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: kafka-backup
              image: kafka-backup:latest
              args:
                - backup
                - --config
                - /config/backup.yaml
              envFrom:
                - secretRef:
                    name: kafka-backup-aws
              volumeMounts:
                - name: config
                  mountPath: /config
          restartPolicy: OnFailure
          volumes:
            - name: config
              configMap:
                name: kafka-backup-config
```

### Azure Kubernetes Service (AKS) with Managed Identity

#### Enable Workload Identity

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kafka-backup
  annotations:
    azure.workload.identity/client-id: <MANAGED_IDENTITY_CLIENT_ID>
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-backup
spec:
  template:
    metadata:
      labels:
        azure.workload.identity/use: "true"
    spec:
      serviceAccountName: kafka-backup
      containers:
        - name: kafka-backup
          image: kafka-backup:latest
          # No credentials needed - uses Workload Identity
```

#### Storage Config (No account_key)

```yaml
storage:
  backend: azure
  account_name: mystorageaccount
  container_name: kafka-backups
  # account_key omitted - uses Managed Identity
```

### Google Kubernetes Engine (GKE) with Workload Identity

#### Service Account Binding

```bash
gcloud iam service-accounts add-iam-policy-binding \
  backup-sa@PROJECT.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:PROJECT.svc.id.goog[NAMESPACE/kafka-backup]"
```

#### Kubernetes Config

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: kafka-backup
  annotations:
    iam.gke.io/gcp-service-account: backup-sa@PROJECT.iam.gserviceaccount.com
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-backup
spec:
  template:
    spec:
      serviceAccountName: kafka-backup
      containers:
        - name: kafka-backup
          image: kafka-backup:latest
          # No credentials needed - uses Workload Identity
```

### Production Best Practices

#### 1. Use Managed Identity / Workload Identity

Avoid storing credentials in ConfigMaps or Secrets. Use cloud-native identity:
- AWS: IAM Roles for Service Accounts (IRSA)
- Azure: Workload Identity / Managed Identity
- GCP: Workload Identity

#### 2. Enable Encryption

```yaml
storage:
  backend: s3
  bucket: kafka-backups
  # Enable server-side encryption (configure in bucket policy)
```

#### 3. Configure Lifecycle Policies

Set up bucket lifecycle policies to automatically delete old backups:

**S3 Lifecycle Policy:**
```json
{
  "Rules": [{
    "ID": "DeleteOldBackups",
    "Status": "Enabled",
    "Filter": {"Prefix": "backups/"},
    "Expiration": {"Days": 30}
  }]
}
```

#### 4. Use Prefixes for Multi-Tenancy

```yaml
storage:
  backend: s3
  bucket: kafka-backups
  prefix: cluster-prod/team-a
```

#### 5. Monitor Storage Metrics

Enable bucket metrics and set up alerts for:
- Storage size growth
- Request errors (4xx, 5xx)
- Latency percentiles

#### 6. Test Restore Regularly

Schedule periodic restore tests to validate backup integrity:

```yaml
# restore-test.yaml
mode: restore
backup_id: "latest"

target:
  bootstrap_servers:
    - kafka-test.local:9092
  topics:
    include:
      - "test-restore-*"

storage:
  backend: s3
  bucket: kafka-backups

restore:
  topic_mapping:
    orders: test-restore-orders
    payments: test-restore-payments
```

---

## Troubleshooting

### Common Issues

#### S3: "Access Denied"

1. Check credentials are set:
   ```bash
   echo $AWS_ACCESS_KEY_ID
   echo $AWS_SECRET_ACCESS_KEY
   ```

2. Verify bucket permissions:
   ```bash
   aws s3 ls s3://your-bucket/
   ```

3. Check IAM policy includes required actions:
   - `s3:GetObject`
   - `s3:PutObject`
   - `s3:DeleteObject`
   - `s3:ListBucket`

#### S3: "Connection refused" (MinIO)

1. Ensure `path_style: true` is set
2. Ensure `allow_http: true` for HTTP endpoints
3. Verify endpoint URL is correct

#### Azure: "AuthenticationFailed"

1. Check account key:
   ```bash
   echo $AZURE_STORAGE_KEY
   ```

2. For Managed Identity, verify:
   - Identity is assigned to the resource
   - Identity has "Storage Blob Data Contributor" role

#### GCS: "Permission denied"

1. Check service account credentials:
   ```bash
   echo $GOOGLE_APPLICATION_CREDENTIALS
   cat $GOOGLE_APPLICATION_CREDENTIALS
   ```

2. Verify service account has required roles:
   - `roles/storage.objectAdmin`
   - `roles/storage.legacyBucketReader`

#### Filesystem: "Permission denied"

1. Check directory permissions:
   ```bash
   ls -la /var/kafka-backups
   ```

2. Ensure the user running kafka-backup has write access

### Debug Logging

Enable debug logging to see storage operations:

```bash
RUST_LOG=kafka_backup_core::storage=debug kafka-backup backup --config config.yaml
```

### Health Checks

Use the list command to verify storage connectivity:

```bash
kafka-backup list --path "s3://bucket/prefix"
```

---

## Migration Guide

### From Legacy Config to New Config

**Before (legacy):**
```yaml
storage:
  backend: s3
  bucket: kafka-backups
  region: us-east-1
  endpoint: null
  access_key: AKIAIOSFODNN7EXAMPLE
  secret_key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
  prefix: null
```

**After (new tagged enum):**
```yaml
storage:
  backend: s3
  bucket: kafka-backups
  region: us-east-1
  # endpoint, access_key, secret_key, prefix are all optional
  # and can be omitted if not needed
```

Both formats continue to work - the legacy format is maintained for backward compatibility.

### Adding a New Backend

To add a new storage backend:

1. Create `src/storage/mybackend.rs`
2. Implement `StorageBackend` trait
3. Add config variant to `StorageBackendConfig`
4. Add case to `create_backend_from_config`
5. Export from `mod.rs`

See existing backends (azure.rs, gcs.rs) as templates.
