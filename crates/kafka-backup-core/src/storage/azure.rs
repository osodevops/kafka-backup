//! Azure Blob Storage backend implementation.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use std::sync::Arc;
use tracing::{debug, info};

use super::{ObjectMetadata, StorageBackend};
use crate::error::StorageError;
use crate::{Error, Result};

/// Azure Blob Storage backend configuration
#[derive(Debug, Clone)]
pub struct AzureConfig {
    /// Azure storage account name
    pub account_name: String,
    /// Azure blob container name
    pub container_name: String,
    /// Storage account key (if None, uses DefaultAzureCredential chain)
    pub account_key: Option<String>,
    /// Key prefix for all operations
    pub prefix: Option<String>,
    /// Custom endpoint URL for sovereign clouds (Azure Government, Azure China)
    pub endpoint: Option<String>,
    /// Enable Workload Identity authentication (for AKS)
    pub use_workload_identity: Option<bool>,
    /// Azure AD client ID (for Workload Identity or service principal)
    pub client_id: Option<String>,
    /// Azure AD tenant ID (for Workload Identity or service principal)
    pub tenant_id: Option<String>,
    /// Client secret (for service principal authentication)
    pub client_secret: Option<String>,
    /// SAS token for shared access signature authentication
    pub sas_token: Option<String>,
}

/// Azure Blob Storage backend
pub struct AzureBackend {
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
}

impl AzureBackend {
    /// Create a new Azure Blob Storage backend
    ///
    /// Authentication methods (in order of precedence):
    /// 1. SAS token (`sas_token`)
    /// 2. Storage account key (`account_key`)
    /// 3. Service principal (`client_id` + `client_secret` + `tenant_id`)
    /// 4. Workload Identity (`use_workload_identity` or AZURE_FEDERATED_TOKEN_FILE env var)
    /// 5. DefaultAzureCredential chain (environment, managed identity, CLI)
    pub fn new(config: AzureConfig) -> Result<Self> {
        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(&config.account_name)
            .with_container_name(&config.container_name);

        // Custom endpoint for sovereign clouds (Azure Government, China, etc.)
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint.clone());
        }

        // Authentication configuration (order matters for precedence)
        if let Some(sas_token) = &config.sas_token {
            // SAS token authentication - parse query string into key-value pairs
            let pairs: Vec<(String, String)> = sas_token
                .trim_start_matches('?')
                .split('&')
                .filter_map(|pair| {
                    let mut parts = pair.splitn(2, '=');
                    match (parts.next(), parts.next()) {
                        (Some(k), Some(v)) => Some((k.to_string(), v.to_string())),
                        _ => None,
                    }
                })
                .collect();
            builder = builder.with_sas_authorization(pairs);
            debug!("Azure authentication: SAS token");
        } else if let Some(key) = &config.account_key {
            // Storage account key authentication
            builder = builder.with_access_key(key);
            debug!("Azure authentication: Account key");
        } else if config.client_secret.is_some() {
            // Service principal authentication
            if let Some(client_id) = &config.client_id {
                builder = builder.with_client_id(client_id);
            }
            if let Some(tenant_id) = &config.tenant_id {
                builder = builder.with_tenant_id(tenant_id);
            }
            if let Some(client_secret) = &config.client_secret {
                builder = builder.with_client_secret(client_secret);
            }
            debug!("Azure authentication: Service principal");
        } else if config.use_workload_identity.unwrap_or(false)
            || std::env::var("AZURE_FEDERATED_TOKEN_FILE").is_ok()
        {
            // Workload Identity - explicitly configure federated token authentication
            // Read from environment variables set by Azure Workload Identity webhook
            let client_id = std::env::var("AZURE_CLIENT_ID").ok();
            let tenant_id = std::env::var("AZURE_TENANT_ID").ok();
            let token_file = std::env::var("AZURE_FEDERATED_TOKEN_FILE").ok();

            if let Some(ref file) = token_file {
                builder = builder.with_federated_token_file(file);
            }
            if let Some(ref id) = client_id {
                builder = builder.with_client_id(id);
            }
            if let Some(ref id) = tenant_id {
                builder = builder.with_tenant_id(id);
            }
            builder = builder.with_use_azure_cli(false);

            debug!(
                "Azure authentication: Workload Identity (client_id={}, tenant_id={}, token_file={})",
                client_id.as_deref().unwrap_or("not set"),
                tenant_id.as_deref().unwrap_or("not set"),
                token_file.as_deref().unwrap_or("not set")
            );
        } else {
            // DefaultAzureCredential chain - environment, managed identity, CLI
            debug!("Azure authentication: DefaultAzureCredential chain");
        }

        let store = builder.build().map_err(|e| {
            Error::Storage(StorageError::Backend(format!(
                "Failed to create Azure client: {}",
                e
            )))
        })?;

        info!(
            "Created Azure backend for account: {}, container: {}, prefix: {:?}, endpoint: {:?}",
            config.account_name, config.container_name, config.prefix, config.endpoint
        );

        Ok(Self {
            store: Arc::new(store),
            prefix: config.prefix,
        })
    }

    /// Build the full path for a key
    fn full_path(&self, key: &str) -> Path {
        match &self.prefix {
            Some(prefix) => Path::from(format!("{}/{}", prefix.trim_end_matches('/'), key)),
            None => Path::from(key),
        }
    }

    /// Strip the prefix from a path to get the key
    fn strip_prefix(&self, path: &str) -> String {
        match &self.prefix {
            Some(p) => path
                .strip_prefix(&format!("{}/", p.trim_end_matches('/')))
                .unwrap_or(path)
                .to_string(),
            None => path.to_string(),
        }
    }
}

#[async_trait]
impl StorageBackend for AzureBackend {
    async fn put(&self, key: &str, data: Bytes) -> Result<()> {
        let path = self.full_path(key);
        debug!("Azure PUT: {}", path);

        self.store
            .put(&path, PutPayload::from_bytes(data))
            .await
            .map_err(|e| {
                Error::Storage(StorageError::Backend(format!("Azure PUT failed: {}", e)))
            })?;

        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Bytes> {
        let path = self.full_path(key);
        debug!("Azure GET: {}", path);

        let result = self.store.get(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                Error::Storage(StorageError::NotFound(key.to_string()))
            }
            _ => Error::Storage(StorageError::Backend(format!("Azure GET failed: {}", e))),
        })?;

        result.bytes().await.map_err(|e| {
            Error::Storage(StorageError::Backend(format!(
                "Failed to read Azure response: {}",
                e
            )))
        })
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>> {
        let full_prefix = self.full_path(prefix);
        debug!("Azure LIST: {}", full_prefix);

        let mut keys = Vec::new();
        let mut stream = self.store.list(Some(&full_prefix));

        use futures::StreamExt;
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    let key = meta.location.to_string();
                    keys.push(self.strip_prefix(&key));
                }
                Err(e) => {
                    return Err(Error::Storage(StorageError::Backend(format!(
                        "Azure LIST failed: {}",
                        e
                    ))));
                }
            }
        }

        Ok(keys)
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let path = self.full_path(key);
        debug!("Azure HEAD: {}", path);

        match self.store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(Error::Storage(StorageError::Backend(format!(
                "Azure HEAD failed: {}",
                e
            )))),
        }
    }

    async fn delete(&self, key: &str) -> Result<()> {
        let path = self.full_path(key);
        debug!("Azure DELETE: {}", path);

        self.store.delete(&path).await.map_err(|e| {
            Error::Storage(StorageError::Backend(format!("Azure DELETE failed: {}", e)))
        })?;

        Ok(())
    }

    async fn size(&self, key: &str) -> Result<u64> {
        let path = self.full_path(key);
        debug!("Azure HEAD (size): {}", path);

        let meta = self.store.head(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                Error::Storage(StorageError::NotFound(key.to_string()))
            }
            _ => Error::Storage(StorageError::Backend(format!("Azure HEAD failed: {}", e))),
        })?;

        Ok(meta.size as u64)
    }

    async fn head(&self, key: &str) -> Result<ObjectMetadata> {
        let path = self.full_path(key);
        debug!("Azure HEAD: {}", path);

        let meta = self.store.head(&path).await.map_err(|e| match e {
            object_store::Error::NotFound { .. } => {
                Error::Storage(StorageError::NotFound(key.to_string()))
            }
            _ => Error::Storage(StorageError::Backend(format!("Azure HEAD failed: {}", e))),
        })?;

        Ok(ObjectMetadata {
            size: meta.size as u64,
            last_modified: meta.last_modified.timestamp_millis(),
            e_tag: meta.e_tag.clone(),
        })
    }

    async fn copy(&self, src: &str, dest: &str) -> Result<()> {
        let src_path = self.full_path(src);
        let dest_path = self.full_path(dest);
        debug!("Azure COPY: {} -> {}", src_path, dest_path);

        self.store.copy(&src_path, &dest_path).await.map_err(|e| {
            Error::Storage(StorageError::Backend(format!("Azure COPY failed: {}", e)))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require actual Azure storage to run
    // They are ignored by default

    #[tokio::test]
    #[ignore]
    async fn test_azure_backend_basic() {
        let config = AzureConfig {
            account_name: std::env::var("AZURE_STORAGE_ACCOUNT").unwrap(),
            container_name: "test-backups".to_string(),
            account_key: std::env::var("AZURE_STORAGE_KEY").ok(),
            prefix: Some("test".to_string()),
            endpoint: None,
            use_workload_identity: None,
            client_id: None,
            tenant_id: None,
            client_secret: None,
            sas_token: None,
        };

        let backend = AzureBackend::new(config).unwrap();

        // Test put
        let data = Bytes::from("Hello, Azure!");
        backend.put("test-key", data.clone()).await.unwrap();

        // Test exists
        assert!(backend.exists("test-key").await.unwrap());

        // Test get
        let retrieved = backend.get("test-key").await.unwrap();
        assert_eq!(retrieved, data);

        // Test size
        let size = backend.size("test-key").await.unwrap();
        assert_eq!(size, data.len() as u64);

        // Test head
        let meta = backend.head("test-key").await.unwrap();
        assert_eq!(meta.size, data.len() as u64);

        // Test copy
        backend.copy("test-key", "test-key-copy").await.unwrap();
        assert!(backend.exists("test-key-copy").await.unwrap());

        // Test delete
        backend.delete("test-key").await.unwrap();
        backend.delete("test-key-copy").await.unwrap();
        assert!(!backend.exists("test-key").await.unwrap());
    }
}
