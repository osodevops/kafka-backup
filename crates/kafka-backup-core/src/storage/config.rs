//! Storage configuration types.

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Storage backend configuration using tagged enum for type-safe configuration.
///
/// Supports multiple storage backends:
/// - S3 and S3-compatible (MinIO, Ceph RGW, etc.)
/// - Azure Blob Storage
/// - Google Cloud Storage
/// - Local filesystem
/// - In-memory (for testing)
///
/// `Debug` never prints credentials (keys, secrets, SAS tokens, or
/// credentials embedded in an endpoint URL); use [`Self::describe`] for a
/// human-readable location.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "backend")]
pub enum StorageBackendConfig {
    /// AWS S3 or S3-compatible storage (MinIO, Ceph RGW, DigitalOcean Spaces, etc.)
    #[serde(rename = "s3")]
    S3 {
        /// S3 bucket name
        bucket: String,
        /// AWS region (e.g., "us-east-1")
        #[serde(default)]
        region: Option<String>,
        /// Custom endpoint URL (for S3-compatible services like MinIO)
        #[serde(default)]
        endpoint: Option<String>,
        /// Access key ID (falls back to AWS_ACCESS_KEY_ID env var).
        /// `access_key_id` is accepted as an alias (issue #166).
        #[serde(default, alias = "access_key_id")]
        access_key: Option<String>,
        /// Secret access key (falls back to AWS_SECRET_ACCESS_KEY env var).
        /// `secret_access_key` is accepted as an alias (issue #166).
        #[serde(default, alias = "secret_access_key")]
        secret_key: Option<String>,
        /// Key prefix for all operations
        #[serde(default)]
        prefix: Option<String>,
        /// Use path-style requests (required for MinIO/Ceph RGW)
        #[serde(default)]
        path_style: bool,
        /// Allow HTTP (insecure) connections
        #[serde(default)]
        allow_http: bool,
    },

    /// Azure Blob Storage
    ///
    /// Authentication, in order of precedence:
    /// 1. SAS token (`sas_token`, as copied from the portal)
    /// 2. Storage account key (`account_key`); with `use_emulator` and no
    ///    SAS token or key, the well-known emulator key
    /// 3. Service principal (`client_secret` + `client_id` + `tenant_id`)
    /// 4. Workload Identity (`use_workload_identity: true` or
    ///    `AZURE_FEDERATED_TOKEN_FILE` set, for AKS)
    /// 5. Managed identity via IMDS; `client_id` selects a user-assigned
    ///    identity, otherwise the system-assigned identity is used
    ///
    /// No other source (Azure CLI, other environment variables) is used.
    #[serde(rename = "azure")]
    Azure {
        /// Azure storage account name
        account_name: String,
        /// Azure blob container name
        container_name: String,
        /// Storage account key
        #[serde(default)]
        account_key: Option<String>,
        /// Key prefix for all operations
        #[serde(default)]
        prefix: Option<String>,
        /// Custom endpoint URL for sovereign clouds (Azure Government, Azure China)
        /// Example: "https://mystorageaccount.blob.core.usgovcloudapi.net".
        /// For the emulator: "http://127.0.0.1:10000/devstoreaccount1".
        #[serde(default)]
        endpoint: Option<String>,
        /// Enable Workload Identity authentication (for AKS)
        /// When true, uses AZURE_FEDERATED_TOKEN_FILE, AZURE_CLIENT_ID, AZURE_TENANT_ID
        #[serde(default)]
        use_workload_identity: Option<bool>,
        /// Azure AD client ID: the service principal or Workload Identity
        /// app, or the user-assigned managed identity to use with IMDS
        #[serde(default)]
        client_id: Option<String>,
        /// Azure AD tenant ID (for Workload Identity or service principal)
        #[serde(default)]
        tenant_id: Option<String>,
        /// Client secret (for service principal authentication)
        #[serde(default)]
        client_secret: Option<String>,
        /// SAS token for shared access signature authentication, as copied
        /// from the portal (leading `?` optional)
        #[serde(default)]
        sas_token: Option<String>,
        /// Use the Azure Storage emulator (Azurite): plain HTTP,
        /// path-style URLs, the well-known emulator key unless
        /// `account_key` or `sas_token` is set. Without `endpoint`, the
        /// URL is `AZURITE_BLOB_STORAGE_URL` or `http://127.0.0.1:10000`.
        #[serde(default)]
        use_emulator: bool,
        /// Allow plain-HTTP endpoints. Implied by `use_emulator` and by an
        /// `endpoint` starting with `http://`.
        #[serde(default)]
        allow_http: bool,
    },

    /// Google Cloud Storage
    #[serde(rename = "gcs")]
    Gcs {
        /// GCS bucket name
        bucket: String,
        /// Path to service account JSON key file (if None, uses Application Default Credentials)
        #[serde(default)]
        service_account_path: Option<String>,
        /// Key prefix for all operations
        #[serde(default)]
        prefix: Option<String>,
    },

    /// Local filesystem storage
    #[serde(rename = "filesystem")]
    Filesystem {
        /// Base path for storage
        path: PathBuf,
    },

    /// In-memory storage (for testing)
    #[serde(rename = "memory")]
    Memory,
}

/// Plain-HTTP S3 endpoints (in-cluster MinIO / Ceph RGW) need `allow_http` or
/// the client refuses to build. An explicit `allow_http: true` always wins;
/// otherwise an `endpoint` starting with `http://` implies it (issue #166).
/// Shared by the YAML and `--path` URL code paths.
impl StorageBackendConfig {
    /// Credential-free description of the storage location, e.g.
    /// `s3://bucket/prefix`, `azure://account/container/prefix (endpoint
    /// https://...)`, `gs://bucket`, `file:///path`.
    ///
    /// Suitable for logs and for reports handed to third parties, such as
    /// validation evidence.
    pub fn describe(&self) -> String {
        fn with_prefix(base: String, prefix: Option<&str>) -> String {
            match prefix
                .map(|p| p.trim_matches('/'))
                .filter(|p| !p.is_empty())
            {
                Some(p) => format!("{base}/{p}"),
                None => base,
            }
        }
        fn with_endpoint(base: String, endpoint: Option<&str>) -> String {
            match endpoint {
                Some(e) => format!("{base} (endpoint {})", sanitize_endpoint(e)),
                None => base,
            }
        }

        match self {
            Self::S3 {
                bucket,
                endpoint,
                prefix,
                ..
            } => with_endpoint(
                with_prefix(format!("s3://{bucket}"), prefix.as_deref()),
                endpoint.as_deref(),
            ),
            Self::Azure {
                account_name,
                container_name,
                prefix,
                endpoint,
                ..
            } => with_endpoint(
                with_prefix(
                    format!("azure://{account_name}/{container_name}"),
                    prefix.as_deref(),
                ),
                endpoint.as_deref(),
            ),
            Self::Gcs { bucket, prefix, .. } => {
                with_prefix(format!("gs://{bucket}"), prefix.as_deref())
            }
            Self::Filesystem { path } => format!("file://{}", path.display()),
            Self::Memory => "memory://".to_string(),
        }
    }
}

/// An endpoint URL without userinfo, query or fragment, where credentials
/// (a password, a pasted SAS token) could hide.
fn sanitize_endpoint(endpoint: &str) -> String {
    match url::Url::parse(endpoint) {
        Ok(mut url) => {
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.set_query(None);
            url.set_fragment(None);
            url.as_str().trim_end_matches('/').to_string()
        }
        Err(_) => "<unparseable endpoint>".to_string(),
    }
}

/// Debug placeholder for a credential that is set.
struct Redacted;

impl std::fmt::Debug for Redacted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

fn redacted(value: &Option<String>) -> Option<Redacted> {
    value.as_ref().map(|_| Redacted)
}

// Never print credentials: storage configs reach logs and reports.
impl std::fmt::Debug for StorageBackendConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::S3 {
                bucket,
                region,
                endpoint,
                access_key,
                secret_key,
                prefix,
                path_style,
                allow_http,
            } => f
                .debug_struct("S3")
                .field("bucket", bucket)
                .field("region", region)
                .field("endpoint", &endpoint.as_deref().map(sanitize_endpoint))
                .field("access_key", &redacted(access_key))
                .field("secret_key", &redacted(secret_key))
                .field("prefix", prefix)
                .field("path_style", path_style)
                .field("allow_http", allow_http)
                .finish(),
            Self::Azure {
                account_name,
                container_name,
                account_key,
                prefix,
                endpoint,
                use_workload_identity,
                client_id,
                tenant_id,
                client_secret,
                sas_token,
                use_emulator,
                allow_http,
            } => f
                .debug_struct("Azure")
                .field("account_name", account_name)
                .field("container_name", container_name)
                .field("account_key", &redacted(account_key))
                .field("prefix", prefix)
                .field("endpoint", &endpoint.as_deref().map(sanitize_endpoint))
                .field("use_workload_identity", use_workload_identity)
                .field("client_id", client_id)
                .field("tenant_id", tenant_id)
                .field("client_secret", &redacted(client_secret))
                .field("sas_token", &redacted(sas_token))
                .field("use_emulator", use_emulator)
                .field("allow_http", allow_http)
                .finish(),
            Self::Gcs {
                bucket,
                service_account_path,
                prefix,
            } => f
                .debug_struct("Gcs")
                .field("bucket", bucket)
                .field("service_account_path", service_account_path)
                .field("prefix", prefix)
                .finish(),
            Self::Filesystem { path } => f.debug_struct("Filesystem").field("path", path).finish(),
            Self::Memory => f.write_str("Memory"),
        }
    }
}

pub(crate) fn implied_allow_http(endpoint: Option<&str>, explicit: bool) -> bool {
    explicit || endpoint.is_some_and(|e| e.starts_with("http://"))
}

/// The local directory a `file://` URL names (#221).
///
/// `Url::to_file_path` percent-decodes (`my%20backups` → `my backups`) and
/// maps `file:///C:/x` to `C:\x` on Windows. Only local URLs are accepted:
/// a host used to be dropped silently, so `file://tmp/backups` meant
/// `/backups`. A query or fragment, an empty or root path, and NUL bytes are
/// rejected rather than ignored.
fn file_url_to_path(parsed: &url::Url, raw: &str) -> crate::Result<PathBuf> {
    let err = |msg: String| crate::Error::Config(format!("Invalid file URL {raw}: {msg}"));

    // `localhost` is normalised to "no host" by the parser.
    if let Some(host) = parsed.host_str() {
        return Err(err(format!(
            "host \"{host}\" is not supported; use file:///{host}{} (three slashes) for \
             an absolute path, or pass a relative path without file://",
            parsed.path()
        )));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(err(
            "a file URL takes no query string or fragment; percent-encode '?' as %3F and \
             '#' as %23 in directory names"
                .to_string(),
        ));
    }
    let path = parsed
        .to_file_path()
        .map_err(|()| err("not a local path".to_string()))?;
    if path.parent().is_none() {
        return Err(err(
            "no directory given (it would mean the filesystem root)".to_string(),
        ));
    }
    if path.as_os_str().as_encoded_bytes().contains(&0) {
        return Err(err("the path contains a NUL byte (%00)".to_string()));
    }
    Ok(path)
}

impl StorageBackendConfig {
    /// Parse configuration from a URL string
    ///
    /// Supported URL formats:
    /// - `s3://bucket-name?region=us-east-1`
    /// - `azure://container@account.blob.core.windows.net`
    /// - `gcs://bucket-name`
    /// - `file:///path/to/data` (percent-decoded; host must be empty or
    ///   `localhost`)
    /// - `memory://`
    pub fn from_url(url: &str) -> crate::Result<Self> {
        let parsed = url::Url::parse(url)
            .map_err(|e| crate::Error::Config(format!("Invalid storage URL: {}", e)))?;

        match parsed.scheme() {
            "s3" | "s3a" => {
                let bucket = parsed.host_str().unwrap_or_default().to_string();
                let prefix = parsed.path().trim_matches('/');
                let prefix = if prefix.is_empty() {
                    None
                } else {
                    Some(prefix.to_string())
                };
                let region = parsed
                    .query_pairs()
                    .find(|(k, _)| k == "region")
                    .map(|(_, v)| v.to_string());
                let endpoint = parsed
                    .query_pairs()
                    .find(|(k, _)| k == "endpoint")
                    .map(|(_, v)| v.to_string());
                let path_style = parsed
                    .query_pairs()
                    .find(|(k, _)| k == "path_style")
                    .map(|(_, v)| v == "true")
                    .unwrap_or(false);
                // Plain-HTTP endpoints (in-cluster MinIO/Ceph RGW) need
                // allow_http or the S3 client refuses to build; an explicit
                // `allow_http=true` query param works too, and an
                // `endpoint=http://…` implies it (issue #166).
                let allow_http = parsed
                    .query_pairs()
                    .find(|(k, _)| k == "allow_http")
                    .map(|(_, v)| v == "true")
                    .unwrap_or_else(|| implied_allow_http(endpoint.as_deref(), false));

                Ok(Self::S3 {
                    bucket,
                    region,
                    endpoint,
                    access_key: std::env::var("AWS_ACCESS_KEY_ID").ok(),
                    secret_key: std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
                    prefix,
                    path_style,
                    allow_http,
                })
            }
            "azure" | "az" => {
                let host = parsed.host_str().unwrap_or_default();
                let account_name = host.split('.').next().unwrap_or(host).to_string();
                let container_name = parsed.path().trim_start_matches('/').to_string();

                // Check for Workload Identity environment
                let has_workload_identity = std::env::var("AZURE_FEDERATED_TOKEN_FILE").is_ok();

                Ok(Self::Azure {
                    account_name,
                    container_name,
                    account_key: std::env::var("AZURE_STORAGE_KEY")
                        .ok()
                        .or_else(|| std::env::var("AZURE_STORAGE_ACCOUNT_KEY").ok()),
                    prefix: None,
                    endpoint: None,
                    use_workload_identity: if has_workload_identity {
                        Some(true)
                    } else {
                        None
                    },
                    client_id: std::env::var("AZURE_CLIENT_ID").ok(),
                    tenant_id: std::env::var("AZURE_TENANT_ID").ok(),
                    client_secret: std::env::var("AZURE_CLIENT_SECRET").ok(),
                    sas_token: std::env::var("AZURE_STORAGE_SAS_TOKEN").ok(),
                    use_emulator: false,
                    allow_http: false,
                })
            }
            "gcs" | "gs" => {
                let bucket = parsed.host_str().unwrap_or_default().to_string();

                Ok(Self::Gcs {
                    bucket,
                    service_account_path: std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok(),
                    prefix: None,
                })
            }
            "file" => Ok(Self::Filesystem {
                path: file_url_to_path(&parsed, url)?,
            }),
            "memory" => Ok(Self::Memory),
            scheme => Err(crate::Error::Config(format!(
                "Unknown storage scheme: {}",
                scheme
            ))),
        }
    }

    /// Get the prefix for this storage configuration
    pub fn prefix(&self) -> Option<&str> {
        match self {
            Self::S3 { prefix, .. } => prefix.as_deref(),
            Self::Azure { prefix, .. } => prefix.as_deref(),
            Self::Gcs { prefix, .. } => prefix.as_deref(),
            Self::Filesystem { .. } => None,
            Self::Memory => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_url_parsing() {
        let config = StorageBackendConfig::from_url("s3://my-bucket?region=us-west-2").unwrap();
        match config {
            StorageBackendConfig::S3 { bucket, region, .. } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(region, Some("us-west-2".to_string()));
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_s3_url_parsing_preserves_path_prefix() {
        let config =
            StorageBackendConfig::from_url("s3://my-bucket/backups/prod/?region=us-west-2")
                .unwrap();
        match config {
            StorageBackendConfig::S3 {
                bucket,
                region,
                prefix,
                ..
            } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(region, Some("us-west-2".to_string()));
                assert_eq!(prefix.as_deref(), Some("backups/prod"));
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_filesystem_url_parsing() {
        let config = StorageBackendConfig::from_url("file:///var/kafka-backups").unwrap();
        match config {
            StorageBackendConfig::Filesystem { path } => {
                assert_eq!(path, PathBuf::from("/var/kafka-backups"));
            }
            _ => panic!("Expected Filesystem config"),
        }
    }

    fn file_path(url: &str) -> PathBuf {
        match StorageBackendConfig::from_url(url) {
            Ok(StorageBackendConfig::Filesystem { path }) => path,
            other => panic!("{url}: expected a filesystem config, got {other:?}"),
        }
    }

    fn file_error(url: &str) -> String {
        match StorageBackendConfig::from_url(url) {
            Err(e) => e.to_string(),
            Ok(config) => panic!("{url}: should be rejected, got {config:?}"),
        }
    }

    /// #221: `file://` URLs are percent-decoded like any URL.
    #[cfg(unix)]
    #[test]
    fn file_url_is_percent_decoded() {
        assert_eq!(
            file_path("file:///tmp/my%20backups"),
            PathBuf::from("/tmp/my backups")
        );
        assert_eq!(
            file_path("file:///data/caf%C3%A9/b%C3%BCro"),
            PathBuf::from("/data/café/büro")
        );
        // Non-UTF-8 bytes are legal in Unix paths.
        use std::os::unix::ffi::OsStrExt;
        assert_eq!(
            file_path("file:///tmp/%FF").as_os_str().as_bytes(),
            b"/tmp/\xFF"
        );
    }

    #[cfg(unix)]
    #[test]
    fn file_url_accepts_localhost_and_the_one_slash_form() {
        for url in [
            "file://localhost/var/kafka-backups",
            "file://LOCALHOST/var/kafka-backups",
            "file:/var/kafka-backups",
            "file:///var/kafka-backups/",
            "file:///var/x/../kafka-backups",
        ] {
            let path = file_path(url);
            assert_eq!(
                path.components().collect::<Vec<_>>(),
                PathBuf::from("/var/kafka-backups")
                    .components()
                    .collect::<Vec<_>>(),
                "{url}"
            );
        }
    }

    /// #221: a host used to be dropped silently, so `file://tmp/backups`
    /// (meaning /tmp/backups) became `/backups`, and a remote host read the
    /// local disk.
    #[test]
    fn file_url_rejects_a_host() {
        let err = file_error("file://tmp/backups");
        assert!(err.contains("host \"tmp\""), "{err}");
        assert!(
            err.contains("file:///tmp/backups"),
            "suggests three slashes: {err}"
        );
        let err = file_error("file://nfs-server/exports/kafka");
        assert!(err.contains("nfs-server"), "{err}");
    }

    #[test]
    fn file_url_rejects_query_and_fragment() {
        for url in [
            "file:///tmp/x?region=us-east-1",
            "file:///tmp/x#y",
            "file:///tmp/x?",
        ] {
            let err = file_error(url);
            assert!(
                err.contains("query") || err.contains("fragment"),
                "{url}: {err}"
            );
        }
        // An encoded `?` / `#` is part of the directory name.
        #[cfg(unix)]
        assert_eq!(
            file_path("file:///tmp/a%3Fb%23c"),
            PathBuf::from("/tmp/a?b#c")
        );
    }

    /// `file://` with no path (e.g. `file://$BACKUP_DIR` with the variable
    /// unset) must not mean the filesystem root.
    #[test]
    fn file_url_rejects_an_empty_or_root_path() {
        for url in [
            "file://",
            "file:///",
            "file://localhost",
            "file://localhost/",
        ] {
            let err = file_error(url);
            assert!(err.contains("no directory"), "{url}: {err}");
        }
    }

    #[test]
    fn file_url_rejects_nul_bytes() {
        let err = file_error("file:///tmp/a%00b");
        assert!(err.contains("NUL"), "{err}");
    }

    #[cfg(windows)]
    #[test]
    fn file_url_drive_letter_is_a_windows_path() {
        assert_eq!(
            file_path("file:///C:/backups"),
            PathBuf::from(r"C:\backups")
        );
    }

    #[test]
    fn test_memory_url_parsing() {
        let config = StorageBackendConfig::from_url("memory://").unwrap();
        assert!(matches!(config, StorageBackendConfig::Memory));
    }

    #[test]
    fn from_url_s3_http_endpoint_implies_allow_http() {
        let config = StorageBackendConfig::from_url(
            "s3://bucket/prefix?endpoint=http://minio:9000&path_style=true",
        )
        .unwrap();
        match config {
            StorageBackendConfig::S3 {
                allow_http,
                path_style,
                endpoint,
                ..
            } => {
                assert!(allow_http, "http:// endpoint must imply allow_http");
                assert!(path_style);
                assert_eq!(endpoint.as_deref(), Some("http://minio:9000"));
            }
            _ => panic!("expected S3"),
        }

        // Explicit override wins in both directions.
        let off = StorageBackendConfig::from_url(
            "s3://bucket?endpoint=http://minio:9000&allow_http=false",
        )
        .unwrap();
        match off {
            StorageBackendConfig::S3 { allow_http, .. } => assert!(!allow_http),
            _ => panic!("expected S3"),
        }
        let on = StorageBackendConfig::from_url("s3://bucket?allow_http=true").unwrap();
        match on {
            StorageBackendConfig::S3 { allow_http, .. } => assert!(allow_http),
            _ => panic!("expected S3"),
        }
        let https =
            StorageBackendConfig::from_url("s3://bucket?endpoint=https://s3.example").unwrap();
        match https {
            StorageBackendConfig::S3 { allow_http, .. } => assert!(!allow_http),
            _ => panic!("expected S3"),
        }
    }

    #[test]
    fn test_yaml_deserialization_s3() {
        let yaml = r#"
backend: s3
bucket: kafka-backups
region: us-east-1
endpoint: http://localhost:9000
access_key: minioadmin
secret_key: minioadmin
path_style: true
allow_http: true
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageBackendConfig::S3 {
                bucket,
                region,
                endpoint,
                path_style,
                allow_http,
                ..
            } => {
                assert_eq!(bucket, "kafka-backups");
                assert_eq!(region, Some("us-east-1".to_string()));
                assert_eq!(endpoint, Some("http://localhost:9000".to_string()));
                assert!(path_style);
                assert!(allow_http);
            }
            _ => panic!("Expected S3 config"),
        }
    }

    #[test]
    fn test_yaml_deserialization_azure() {
        let yaml = r#"
backend: azure
account_name: mystorageaccount
container_name: kafka-backups
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageBackendConfig::Azure {
                account_name,
                container_name,
                ..
            } => {
                assert_eq!(account_name, "mystorageaccount");
                assert_eq!(container_name, "kafka-backups");
            }
            _ => panic!("Expected Azure config"),
        }
    }

    #[test]
    fn test_yaml_deserialization_azure_emulator_flags() {
        let config: StorageBackendConfig = serde_yaml::from_str(
            r#"
backend: azure
account_name: devstoreaccount1
container_name: kafka-backups
endpoint: http://127.0.0.1:10000/devstoreaccount1
use_emulator: true
allow_http: true
"#,
        )
        .unwrap();
        match config {
            StorageBackendConfig::Azure {
                use_emulator,
                allow_http,
                endpoint,
                ..
            } => {
                assert!(use_emulator);
                assert!(allow_http);
                assert_eq!(
                    endpoint.as_deref(),
                    Some("http://127.0.0.1:10000/devstoreaccount1")
                );
            }
            _ => panic!("Expected Azure config"),
        }

        // Both default to false.
        let config: StorageBackendConfig =
            serde_yaml::from_str("backend: azure\naccount_name: a\ncontainer_name: c\n").unwrap();
        match config {
            StorageBackendConfig::Azure {
                use_emulator,
                allow_http,
                ..
            } => assert!(!use_emulator && !allow_http),
            _ => panic!("Expected Azure config"),
        }
    }

    #[test]
    fn test_yaml_deserialization_gcs() {
        let yaml = r#"
backend: gcs
bucket: kafka-backups
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageBackendConfig::Gcs { bucket, .. } => {
                assert_eq!(bucket, "kafka-backups");
            }
            _ => panic!("Expected GCS config"),
        }
    }

    #[test]
    fn test_yaml_deserialization_filesystem() {
        let yaml = r#"
backend: filesystem
path: /var/kafka-backups
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageBackendConfig::Filesystem { path } => {
                assert_eq!(path, PathBuf::from("/var/kafka-backups"));
            }
            _ => panic!("Expected Filesystem config"),
        }
    }

    #[test]
    fn test_yaml_deserialization_memory() {
        let yaml = r#"
backend: memory
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(config, StorageBackendConfig::Memory));
    }

    #[test]
    fn yaml_accepts_legacy_access_key_id_aliases() {
        let yaml = r#"
backend: s3
bucket: backups
access_key_id: AKIA
secret_access_key: shh
"#;
        let config: StorageBackendConfig = serde_yaml::from_str(yaml).unwrap();
        match config {
            StorageBackendConfig::S3 {
                access_key,
                secret_key,
                ..
            } => {
                assert_eq!(access_key.as_deref(), Some("AKIA"));
                assert_eq!(secret_key.as_deref(), Some("shh"));
            }
            _ => panic!("expected S3"),
        }
    }

    #[test]
    fn implied_allow_http_cases() {
        assert!(implied_allow_http(Some("http://minio:9000"), false));
        assert!(!implied_allow_http(Some("https://s3.example"), false));
        assert!(!implied_allow_http(None, false));
        assert!(implied_allow_http(None, true));
        assert!(implied_allow_http(Some("https://s3.example"), true));
    }

    const S3_SECRET: &str = "s3-secret-key-not-for-output";
    const S3_ACCESS: &str = "AKIAEXAMPLEACCESSID";
    const AZURE_KEY: &str = "c2VjcmV0LWFjY291bnQta2V5LW5vdC1mb3Itb3V0cHV0";
    const AZURE_SAS: &str = "sv=2022-11-02&sig=c2FzLXNpZ25hdHVyZQ%3D%3D";
    const AZURE_CLIENT_SECRET: &str = "client-secret-not-for-output";

    fn s3_with_credentials() -> StorageBackendConfig {
        StorageBackendConfig::S3 {
            bucket: "kafka-backups".to_string(),
            region: Some("eu-west-2".to_string()),
            endpoint: Some("http://minio:9000".to_string()),
            access_key: Some(S3_ACCESS.to_string()),
            secret_key: Some(S3_SECRET.to_string()),
            prefix: Some("prod/".to_string()),
            path_style: true,
            allow_http: true,
        }
    }

    fn azure_with_credentials() -> StorageBackendConfig {
        StorageBackendConfig::Azure {
            account_name: "kafkabackups".to_string(),
            container_name: "backups".to_string(),
            account_key: Some(AZURE_KEY.to_string()),
            prefix: Some("prod".to_string()),
            endpoint: None,
            use_workload_identity: None,
            client_id: Some("00000000-1111-2222-3333-444444444444".to_string()),
            tenant_id: Some("55555555-6666-7777-8888-999999999999".to_string()),
            client_secret: Some(AZURE_CLIENT_SECRET.to_string()),
            sas_token: Some(AZURE_SAS.to_string()),
            use_emulator: false,
            allow_http: false,
        }
    }

    /// `{:?}` of a storage config ended up in signed validation evidence
    /// reports (the CLI recorded `format!("{:?}", config.storage)` as the
    /// storage backend), so Debug must never print credentials.
    #[test]
    fn debug_output_never_contains_storage_credentials() {
        let printed = format!("{:?}", s3_with_credentials());
        assert!(!printed.contains(S3_SECRET), "{printed}");
        assert!(!printed.contains(S3_ACCESS), "{printed}");
        assert!(printed.contains("kafka-backups"), "{printed}");
        assert!(
            printed.contains("secret_key: Some(<redacted>)"),
            "{printed}"
        );

        let printed = format!("{:#?}", azure_with_credentials());
        for secret in [
            AZURE_KEY,
            AZURE_SAS,
            "c2FzLXNpZ25hdHVyZQ",
            AZURE_CLIENT_SECRET,
        ] {
            assert!(!printed.contains(secret), "{printed}");
        }
        assert!(printed.contains("kafkabackups"), "{printed}");
        assert!(
            printed.contains("00000000-1111-2222-3333-444444444444"),
            "{printed}"
        );

        let unset = StorageBackendConfig::S3 {
            bucket: "b".to_string(),
            region: None,
            endpoint: None,
            access_key: None,
            secret_key: None,
            prefix: None,
            path_style: false,
            allow_http: false,
        };
        assert!(format!("{unset:?}").contains("secret_key: None"));
    }

    #[test]
    fn describe_names_the_location_without_credentials() {
        assert_eq!(
            s3_with_credentials().describe(),
            "s3://kafka-backups/prod (endpoint http://minio:9000)"
        );
        assert_eq!(
            azure_with_credentials().describe(),
            "azure://kafkabackups/backups/prod"
        );
        assert_eq!(
            StorageBackendConfig::Gcs {
                bucket: "gcs-bucket".to_string(),
                service_account_path: Some("/secrets/sa.json".to_string()),
                prefix: None,
            }
            .describe(),
            "gs://gcs-bucket"
        );
        assert_eq!(
            StorageBackendConfig::Filesystem {
                path: PathBuf::from("/var/lib/kafka-backup")
            }
            .describe(),
            "file:///var/lib/kafka-backup"
        );
        assert_eq!(StorageBackendConfig::Memory.describe(), "memory://");

        for described in [
            s3_with_credentials().describe(),
            azure_with_credentials().describe(),
        ] {
            for secret in [
                S3_SECRET,
                S3_ACCESS,
                AZURE_KEY,
                AZURE_SAS,
                AZURE_CLIENT_SECRET,
            ] {
                assert!(!described.contains(secret), "{described}");
            }
        }
    }

    /// An endpoint URL can carry credentials of its own (userinfo, or a SAS
    /// token pasted into the query string); neither is printed.
    #[test]
    fn endpoint_credentials_are_never_printed() {
        let StorageBackendConfig::Azure {
            account_name,
            container_name,
            ..
        } = azure_with_credentials()
        else {
            unreachable!()
        };
        let config = StorageBackendConfig::Azure {
            account_name,
            container_name,
            account_key: None,
            prefix: None,
            endpoint: Some(format!(
                "https://user:pa55word@storage.example/devstoreaccount1?{AZURE_SAS}"
            )),
            use_workload_identity: Some(true),
            client_id: None,
            tenant_id: None,
            client_secret: None,
            sas_token: None,
            use_emulator: false,
            allow_http: false,
        };
        let described = config.describe();
        assert_eq!(
            described,
            "azure://kafkabackups/backups (endpoint https://storage.example/devstoreaccount1)"
        );
        let printed = format!("{config:?}");
        for output in [&described, &printed] {
            assert!(!output.contains("pa55word"), "{output}");
            assert!(!output.contains("c2FzLXNpZ25hdHVyZQ"), "{output}");
        }
    }
}
