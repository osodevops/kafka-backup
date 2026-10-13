//! Azure Blob Storage backend implementation.

use async_trait::async_trait;
use bytes::Bytes;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::sync::Arc;
use tracing::{debug, warn};

use super::{ObjectMetadata, StorageBackend};
use crate::error::StorageError;
use crate::{Error, Result};

/// Account name of the Azure Storage emulator (Azurite).
pub const EMULATOR_ACCOUNT: &str = "devstoreaccount1";

/// Well-known, publicly documented account key of the Azure Storage
/// emulator (Azurite). Not a secret: every Azurite instance accepts it.
pub const EMULATOR_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// Azure Blob Storage backend configuration
#[derive(Debug, Clone)]
pub struct AzureConfig {
    /// Azure storage account name
    pub account_name: String,
    /// Azure blob container name
    pub container_name: String,
    /// Storage account key
    pub account_key: Option<String>,
    /// Key prefix for all operations
    pub prefix: Option<String>,
    /// Custom endpoint URL: sovereign clouds (Azure Government, Azure
    /// China), or the emulator (`http://127.0.0.1:10000/devstoreaccount1`)
    pub endpoint: Option<String>,
    /// Enable Workload Identity authentication (for AKS)
    pub use_workload_identity: Option<bool>,
    /// Azure AD client ID: the service principal / Workload Identity app,
    /// or the user-assigned managed identity to use with IMDS
    pub client_id: Option<String>,
    /// Azure AD tenant ID (for Workload Identity or service principal)
    pub tenant_id: Option<String>,
    /// Client secret (for service principal authentication)
    pub client_secret: Option<String>,
    /// SAS token for shared access signature authentication, as copied
    /// from the portal (percent-encoded values; leading `?` optional)
    pub sas_token: Option<String>,
    /// Talk to the Azure Storage emulator (Azurite): plain HTTP,
    /// path-style URLs, and the well-known emulator key when no
    /// `account_key` or `sas_token` is given. Without `endpoint`, the URL
    /// is `AZURITE_BLOB_STORAGE_URL` or `http://127.0.0.1:10000`.
    pub use_emulator: bool,
    /// Allow plain-HTTP endpoints. Implied by `use_emulator` and by an
    /// `endpoint` that starts with `http://`.
    pub allow_http: bool,
}

/// Resolve Workload Identity parameters: explicit config values take precedence
/// over the environment variables injected by the AKS Workload Identity webhook.
fn resolve_workload_identity(
    config: &AzureConfig,
    env: impl Fn(&str) -> Option<String>,
) -> (Option<String>, Option<String>, Option<String>) {
    let client_id = config.client_id.clone().or_else(|| env("AZURE_CLIENT_ID"));
    let tenant_id = config.tenant_id.clone().or_else(|| env("AZURE_TENANT_ID"));
    let token_file = env("AZURE_FEDERATED_TOKEN_FILE");
    (client_id, tenant_id, token_file)
}

/// How the backend authenticates.
#[derive(Clone, PartialEq, Eq)]
enum AzureAuth {
    /// SAS query pairs, percent-decoded once.
    Sas(Vec<(String, String)>),
    AccessKey(String),
    ServicePrincipal {
        client_id: Option<String>,
        tenant_id: Option<String>,
        client_secret: String,
    },
    WorkloadIdentity {
        client_id: Option<String>,
        tenant_id: Option<String>,
        token_file: Option<String>,
    },
    /// Managed identity through IMDS. `client_id` selects a user-assigned
    /// identity; `None` means the system-assigned one.
    ManagedIdentity {
        client_id: Option<String>,
    },
}

impl AzureAuth {
    fn kind(&self) -> &'static str {
        match self {
            Self::Sas(_) => "SAS token",
            Self::AccessKey(_) => "account key",
            Self::ServicePrincipal { .. } => "service principal",
            Self::WorkloadIdentity { .. } => "Workload Identity",
            Self::ManagedIdentity { client_id: Some(_) } => {
                "managed identity (user-assigned, IMDS)"
            }
            Self::ManagedIdentity { client_id: None } => "managed identity (IMDS)",
        }
    }
}

// Never print credentials.
impl std::fmt::Debug for AzureAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.kind())
    }
}

/// Everything `AzureBackend::new` hands to the object_store builder,
/// resolved from [`AzureConfig`] without side effects.
#[derive(Debug, Clone, PartialEq, Eq)]
struct AzureSettings {
    auth: AzureAuth,
    /// Explicit service URL.
    endpoint: Option<String>,
    /// object_store emulator mode (emulator without an explicit endpoint).
    emulator_default_url: bool,
    allow_http: bool,
}

/// Parse a SAS token as copied from the portal or `az` (values
/// percent-encoded, leading `?` optional) into query pairs, decoding each
/// value exactly once. object_store encodes them again when it builds
/// request URLs, so passing still-encoded values (`sig=...%2B...`) would
/// double-encode them and Azure would answer 403.
fn parse_sas_token(sas_token: &str) -> Result<Vec<(String, String)>> {
    object_store::azure::split_sas(sas_token.trim()).map_err(|e| {
        Error::Config(format!(
            "Invalid Azure sas_token (expected a query string such as \
             \"sv=...&sp=...&sig=...\"): {e}"
        ))
    })
}

/// Endpoint for the emulator: Azurite serves accounts path-style, so the
/// URL must end in the account name (`http://host:10000/devstoreaccount1`);
/// it is appended when the configured endpoint has no path.
fn emulator_endpoint(endpoint: &str, account: &str) -> Result<String> {
    let trimmed = endpoint.trim().trim_end_matches('/');
    let url = url::Url::parse(trimmed)
        .map_err(|e| Error::Config(format!("Invalid Azure endpoint {endpoint}: {e}")))?;
    if url.path().is_empty() || url.path() == "/" {
        Ok(format!("{trimmed}/{account}"))
    } else {
        Ok(trimmed.to_string())
    }
}

/// Resolve credentials and endpoint. Precedence:
///
/// 1. `sas_token`
/// 2. `account_key` (with `use_emulator` and neither 1 nor 2 set: the
///    well-known emulator key)
/// 3. Service principal: `client_secret` (+ `client_id`, `tenant_id`)
/// 4. Workload Identity: `use_workload_identity: true`, or
///    `AZURE_FEDERATED_TOKEN_FILE` set; YAML `client_id` / `tenant_id`
///    override the webhook-injected `AZURE_CLIENT_ID` / `AZURE_TENANT_ID`
/// 5. Managed identity via IMDS: YAML `client_id` selects a user-assigned
///    identity, otherwise the system-assigned identity is used
///
/// Nothing else is consulted: no Azure CLI and no other environment
/// variables. The emulator only accepts 1 and 2.
fn resolve_settings(
    config: &AzureConfig,
    env: impl Fn(&str) -> Option<String>,
) -> Result<AzureSettings> {
    let explicit_http =
        super::config::implied_allow_http(config.endpoint.as_deref(), config.allow_http);

    if config.use_emulator {
        let auth = if let Some(sas) = &config.sas_token {
            AzureAuth::Sas(parse_sas_token(sas)?)
        } else {
            AzureAuth::AccessKey(
                config
                    .account_key
                    .clone()
                    .unwrap_or_else(|| EMULATOR_ACCOUNT_KEY.to_string()),
            )
        };
        if config.client_secret.is_some() || config.use_workload_identity == Some(true) {
            warn!(
                "Azure use_emulator: true ignores client_secret / use_workload_identity; \
                 the emulator authenticates with a SAS token or account key"
            );
        }
        let endpoint = config
            .endpoint
            .as_deref()
            .map(|e| emulator_endpoint(e, &config.account_name))
            .transpose()?;
        return Ok(AzureSettings {
            auth,
            emulator_default_url: endpoint.is_none(),
            endpoint,
            allow_http: true,
        });
    }

    let auth = if let Some(sas) = &config.sas_token {
        AzureAuth::Sas(parse_sas_token(sas)?)
    } else if let Some(key) = &config.account_key {
        AzureAuth::AccessKey(key.clone())
    } else if let Some(client_secret) = &config.client_secret {
        if config.client_id.is_none() || config.tenant_id.is_none() {
            warn!(
                "Azure client_secret is set but client_id or tenant_id is missing; \
                 service-principal authentication needs all three"
            );
        }
        AzureAuth::ServicePrincipal {
            client_id: config.client_id.clone(),
            tenant_id: config.tenant_id.clone(),
            client_secret: client_secret.clone(),
        }
    } else if config.use_workload_identity.unwrap_or(false)
        || env("AZURE_FEDERATED_TOKEN_FILE").is_some()
    {
        let (client_id, tenant_id, token_file) = resolve_workload_identity(config, &env);
        if token_file.is_none() {
            warn!(
                "Azure use_workload_identity: true but AZURE_FEDERATED_TOKEN_FILE is not set; \
                 the AKS Workload Identity webhook injects it (pod label \
                 azure.workload.identity/use=true)"
            );
        }
        AzureAuth::WorkloadIdentity {
            client_id,
            tenant_id,
            token_file,
        }
    } else {
        AzureAuth::ManagedIdentity {
            client_id: config.client_id.clone(),
        }
    };

    Ok(AzureSettings {
        auth,
        endpoint: config.endpoint.clone(),
        emulator_default_url: false,
        allow_http: explicit_http,
    })
}

/// Azure Blob Storage backend
pub struct AzureBackend {
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
}

impl AzureBackend {
    /// Create a new Azure Blob Storage backend
    ///
    /// Authentication methods, in order of precedence:
    /// 1. SAS token (`sas_token`), as copied from the portal
    /// 2. Storage account key (`account_key`); with `use_emulator` and no
    ///    SAS token or key, the well-known emulator key
    /// 3. Service principal (`client_secret` + `client_id` + `tenant_id`)
    /// 4. Workload Identity (`use_workload_identity: true` or
    ///    `AZURE_FEDERATED_TOKEN_FILE` set); YAML `client_id` / `tenant_id`
    ///    override the webhook-injected env vars
    /// 5. Managed identity via IMDS; `client_id` selects a user-assigned
    ///    identity, otherwise the system-assigned identity is used
    ///
    /// No other source (Azure CLI, other environment variables) is used.
    pub fn new(config: AzureConfig) -> Result<Self> {
        let settings = resolve_settings(&config, |name| std::env::var(name).ok())?;

        let mut builder = MicrosoftAzureBuilder::new()
            .with_account(&config.account_name)
            .with_container_name(&config.container_name)
            .with_allow_http(settings.allow_http);
        if settings.emulator_default_url {
            builder = builder.with_use_emulator(true);
        }
        if let Some(endpoint) = &settings.endpoint {
            builder = builder.with_endpoint(endpoint.clone());
        }

        builder = match settings.auth.clone() {
            AzureAuth::Sas(pairs) => builder.with_sas_authorization(pairs),
            AzureAuth::AccessKey(key) => builder.with_access_key(key),
            AzureAuth::ServicePrincipal {
                client_id,
                tenant_id,
                client_secret,
            } => {
                if let Some(id) = client_id {
                    builder = builder.with_client_id(id);
                }
                if let Some(id) = tenant_id {
                    builder = builder.with_tenant_id(id);
                }
                builder.with_client_secret(client_secret)
            }
            AzureAuth::WorkloadIdentity {
                client_id,
                tenant_id,
                token_file,
            } => {
                debug!(
                    "Azure Workload Identity: client_id={}, tenant_id={}, token_file={}",
                    client_id.as_deref().unwrap_or("not set"),
                    tenant_id.as_deref().unwrap_or("not set"),
                    token_file.as_deref().unwrap_or("not set")
                );
                if let Some(file) = token_file {
                    builder = builder.with_federated_token_file(file);
                }
                if let Some(id) = client_id {
                    builder = builder.with_client_id(id);
                }
                if let Some(id) = tenant_id {
                    builder = builder.with_tenant_id(id);
                }
                builder.with_use_azure_cli(false)
            }
            // object_store falls through to IMDS when there is no key, SAS,
            // secret or federated token; a client id on its own selects the
            // user-assigned identity there.
            AzureAuth::ManagedIdentity { client_id } => match client_id {
                Some(id) => builder.with_client_id(id),
                None => builder,
            },
        };
        debug!("Azure authentication: {}", settings.auth.kind());

        let store = builder.build().map_err(|e| {
            Error::Storage(StorageError::Backend(format!(
                "Failed to create Azure client: {}",
                e
            )))
        })?;

        debug!(
            "Created Azure backend for account: {}, container: {}, prefix: {:?}, endpoint: {:?}, \
             emulator: {}",
            config.account_name,
            config.container_name,
            config.prefix,
            settings.endpoint,
            config.use_emulator
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
    fn backend_name(&self) -> &str {
        "azure"
    }

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
            use_emulator: false,
            allow_http: false,
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

    fn wi_config(client_id: Option<&str>, tenant_id: Option<&str>) -> AzureConfig {
        AzureConfig {
            account_name: "acct".to_string(),
            container_name: "backups".to_string(),
            account_key: None,
            prefix: None,
            endpoint: None,
            use_workload_identity: Some(true),
            client_id: client_id.map(str::to_string),
            tenant_id: tenant_id.map(str::to_string),
            client_secret: None,
            sas_token: None,
            use_emulator: false,
            allow_http: false,
        }
    }

    fn fake_env(name: &str) -> Option<String> {
        match name {
            "AZURE_CLIENT_ID" => Some("env-client".to_string()),
            "AZURE_TENANT_ID" => Some("env-tenant".to_string()),
            "AZURE_FEDERATED_TOKEN_FILE" => {
                Some("/var/run/secrets/azure/tokens/azure-identity-token".to_string())
            }
            _ => None,
        }
    }

    #[test]
    fn workload_identity_prefers_yaml_ids_over_env() {
        let (client_id, tenant_id, token_file) = resolve_workload_identity(
            &wi_config(Some("yaml-client"), Some("yaml-tenant")),
            fake_env,
        );
        assert_eq!(client_id.as_deref(), Some("yaml-client"));
        assert_eq!(tenant_id.as_deref(), Some("yaml-tenant"));
        assert_eq!(
            token_file.as_deref(),
            Some("/var/run/secrets/azure/tokens/azure-identity-token")
        );
    }

    fn base_config() -> AzureConfig {
        AzureConfig {
            use_workload_identity: None,
            ..wi_config(None, None)
        }
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    /// A SAS token as the portal / `az storage container generate-sas`
    /// prints it: `se` and `sig` percent-encoded.
    const PORTAL_SAS: &str = "?sv=2022-11-02&ss=b&srt=co&sp=rwdlacx\
        &se=2026-12-31T23%3A59%3A59Z&st=2026-01-01T00%3A00%3A00Z&spr=https\
        &sig=ab%2Bcd%2Fef%3D";

    #[test]
    fn sas_token_values_are_percent_decoded_once() {
        let pairs = parse_sas_token(PORTAL_SAS).unwrap();
        let get = |k: &str| {
            pairs
                .iter()
                .find(|(key, _)| key == k)
                .map(|(_, v)| v.as_str())
        };
        // The old hand split kept `ab%2Bcd%2Fef%3D`, which object_store
        // encoded again (`%252B`): every request was rejected with 403.
        assert_eq!(get("sig"), Some("ab+cd/ef="));
        assert_eq!(get("se"), Some("2026-12-31T23:59:59Z"));
        assert_eq!(get("sv"), Some("2022-11-02"));
        assert_eq!(pairs.len(), 8);

        // Leading `?` and surrounding whitespace are optional.
        assert_eq!(
            parse_sas_token(" sv=1&sig=x%3D ").unwrap(),
            vec![
                ("sv".to_string(), "1".to_string()),
                ("sig".to_string(), "x=".to_string())
            ]
        );
    }

    /// The bug itself, at the wire: a portal SAS must reach Azure with each
    /// value encoded exactly once. (Azurite tolerates double encoding, so
    /// the emulator tests cannot catch a regression; this capture server
    /// can.)
    #[tokio::test]
    async fn portal_sas_reaches_the_wire_encoded_once() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 1024];
            while !buf.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0, "client closed early");
                buf.extend_from_slice(&chunk[..n]);
            }
            socket
                .write_all(
                    b"HTTP/1.1 404 Not Found\r\nx-ms-error-code: BlobNotFound\r\n\
                      Content-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            let head = String::from_utf8(buf).unwrap();
            head.lines().next().unwrap().to_string()
        });

        let mut config = base_config();
        config.endpoint = Some(format!("http://127.0.0.1:{port}/acct"));
        config.sas_token = Some(PORTAL_SAS.to_string());
        let backend = AzureBackend::new(config).unwrap();
        let err = backend.get("k").await.unwrap_err();
        assert!(err.is_not_found(), "{err}");

        let request_line = server.await.unwrap();
        assert!(
            request_line.contains("sig=ab%2Bcd%2Fef%3D"),
            "sig encoded once: {request_line}"
        );
        assert!(
            request_line.contains("se=2026-12-31T23%3A59%3A59Z"),
            "{request_line}"
        );
        assert!(
            !request_line.contains("%25"),
            "double-encoded: {request_line}"
        );
    }

    #[test]
    fn malformed_sas_token_is_a_config_error() {
        let err = parse_sas_token("sv=2022-11-02&garbage").unwrap_err();
        assert!(matches!(err, Error::Config(_)), "{err}");
        assert!(err.to_string().contains("sas_token"), "{err}");
    }

    #[test]
    fn credential_precedence_matches_the_documented_order() {
        let mut c = base_config();
        c.sas_token = Some(PORTAL_SAS.to_string());
        c.account_key = Some("a2V5".to_string());
        c.client_secret = Some("secret".to_string());
        c.client_id = Some("app".to_string());
        c.tenant_id = Some("tenant".to_string());
        c.use_workload_identity = Some(true);
        let kind = |c: &AzureConfig| resolve_settings(c, fake_env).unwrap().auth.kind();

        assert_eq!(kind(&c), "SAS token");
        c.sas_token = None;
        assert_eq!(kind(&c), "account key");
        c.account_key = None;
        assert_eq!(kind(&c), "service principal");
        c.client_secret = None;
        assert_eq!(kind(&c), "Workload Identity");
        c.use_workload_identity = None;
        // The webhook's token file alone also selects Workload Identity.
        assert_eq!(kind(&c), "Workload Identity");
        assert_eq!(
            resolve_settings(&c, no_env).unwrap().auth.kind(),
            "managed identity (user-assigned, IMDS)"
        );
    }

    #[test]
    fn service_principal_carries_its_ids() {
        let mut c = base_config();
        c.client_secret = Some("secret".to_string());
        c.client_id = Some("app".to_string());
        c.tenant_id = Some("tenant".to_string());
        assert_eq!(
            resolve_settings(&c, no_env).unwrap().auth,
            AzureAuth::ServicePrincipal {
                client_id: Some("app".to_string()),
                tenant_id: Some("tenant".to_string()),
                client_secret: "secret".to_string(),
            }
        );
    }

    #[test]
    fn managed_identity_passes_the_yaml_client_id_for_a_user_assigned_identity() {
        let mut c = base_config();
        c.client_id = Some("uami-client-id".to_string());
        assert_eq!(
            resolve_settings(&c, no_env).unwrap().auth,
            AzureAuth::ManagedIdentity {
                client_id: Some("uami-client-id".to_string())
            }
        );
        // The backend builds (no network until the first request).
        AzureBackend::new(c).expect("UAMI config builds");
    }

    #[test]
    fn managed_identity_without_client_id_is_system_assigned() {
        let settings = resolve_settings(&base_config(), no_env).unwrap();
        assert_eq!(
            settings.auth,
            AzureAuth::ManagedIdentity { client_id: None }
        );
        assert!(!settings.allow_http);
        assert_eq!(settings.endpoint, None);
        // Env client ids are not picked up for IMDS (only for Workload Identity).
        let settings = resolve_settings(&base_config(), |n| {
            (n == "AZURE_CLIENT_ID").then(|| "env-id".to_string())
        })
        .unwrap();
        assert_eq!(
            settings.auth,
            AzureAuth::ManagedIdentity { client_id: None }
        );
    }

    #[test]
    fn debug_output_never_contains_credentials() {
        let mut c = base_config();
        c.account_key = Some("c2VjcmV0LWtleQ==".to_string());
        let printed = format!("{:?}", resolve_settings(&c, no_env).unwrap());
        assert!(!printed.contains("c2VjcmV0"), "{printed}");
        c.account_key = None;
        c.sas_token = Some(PORTAL_SAS.to_string());
        let printed = format!("{:?}", resolve_settings(&c, no_env).unwrap());
        assert!(!printed.contains("ab+cd"), "{printed}");
    }

    #[test]
    fn emulator_defaults_to_the_well_known_key_and_http() {
        let mut c = base_config();
        c.account_name = EMULATOR_ACCOUNT.to_string();
        c.use_emulator = true;
        let settings = resolve_settings(&c, fake_env).unwrap();
        assert_eq!(
            settings.auth,
            AzureAuth::AccessKey(EMULATOR_ACCOUNT_KEY.to_string())
        );
        assert!(settings.allow_http);
        assert!(settings.emulator_default_url);
        assert_eq!(settings.endpoint, None);
        AzureBackend::new(c).expect("emulator config builds");
    }

    #[test]
    fn emulator_endpoint_is_path_style_with_the_account() {
        let mut c = base_config();
        c.account_name = EMULATOR_ACCOUNT.to_string();
        c.use_emulator = true;
        for (given, expected) in [
            (
                "http://127.0.0.1:10000",
                "http://127.0.0.1:10000/devstoreaccount1",
            ),
            (
                "http://127.0.0.1:10000/",
                "http://127.0.0.1:10000/devstoreaccount1",
            ),
            (
                "http://azurite:10000/devstoreaccount1/",
                "http://azurite:10000/devstoreaccount1",
            ),
        ] {
            c.endpoint = Some(given.to_string());
            let settings = resolve_settings(&c, no_env).unwrap();
            assert_eq!(settings.endpoint.as_deref(), Some(expected), "{given}");
            assert!(!settings.emulator_default_url);
            assert!(settings.allow_http);
        }
        c.endpoint = Some("not a url".to_string());
        assert!(matches!(
            resolve_settings(&c, no_env),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn emulator_accepts_an_explicit_key_or_a_sas_token() {
        let mut c = base_config();
        c.use_emulator = true;
        c.account_key = Some("b3RoZXI=".to_string());
        assert_eq!(
            resolve_settings(&c, no_env).unwrap().auth,
            AzureAuth::AccessKey("b3RoZXI=".to_string())
        );
        c.sas_token = Some(PORTAL_SAS.to_string());
        assert_eq!(
            resolve_settings(&c, no_env).unwrap().auth.kind(),
            "SAS token"
        );
        // Entra ID credentials do not apply to the emulator.
        c.sas_token = None;
        c.account_key = None;
        c.client_secret = Some("s".to_string());
        assert_eq!(
            resolve_settings(&c, no_env).unwrap().auth,
            AzureAuth::AccessKey(EMULATOR_ACCOUNT_KEY.to_string())
        );
    }

    #[test]
    fn allow_http_is_explicit_or_implied_by_an_http_endpoint() {
        let mut c = base_config();
        c.account_key = Some("a2V5".to_string());
        c.endpoint = Some("https://acct.blob.core.usgovcloudapi.net".to_string());
        assert!(!resolve_settings(&c, no_env).unwrap().allow_http);
        c.endpoint = Some("http://10.0.0.5:10000/acct".to_string());
        let settings = resolve_settings(&c, no_env).unwrap();
        assert!(settings.allow_http);
        // Outside emulator mode the endpoint is used as given.
        assert_eq!(
            settings.endpoint.as_deref(),
            Some("http://10.0.0.5:10000/acct")
        );
        c.endpoint = None;
        c.allow_http = true;
        assert!(resolve_settings(&c, no_env).unwrap().allow_http);
    }

    #[test]
    fn workload_identity_falls_back_to_env() {
        let (client_id, tenant_id, _) = resolve_workload_identity(&wi_config(None, None), fake_env);
        assert_eq!(client_id.as_deref(), Some("env-client"));
        assert_eq!(tenant_id.as_deref(), Some("env-tenant"));
        let (client_id, _, token) = resolve_workload_identity(&wi_config(None, None), |_| None);
        assert!(client_id.is_none() && token.is_none());
    }
}
