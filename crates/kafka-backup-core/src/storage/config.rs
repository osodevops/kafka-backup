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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Supports multiple authentication methods:
    /// 1. Storage Account Key (explicit `account_key`)
    /// 2. Workload Identity (for AKS deployments)
    /// 3. DefaultAzureCredential chain (environment, managed identity, CLI)
    #[serde(rename = "azure")]
    Azure {
        /// Azure storage account name
        account_name: String,
        /// Azure blob container name
        container_name: String,
        /// Storage account key (if None, uses DefaultAzureCredential chain)
        #[serde(default)]
        account_key: Option<String>,
        /// Key prefix for all operations
        #[serde(default)]
        prefix: Option<String>,
        /// Custom endpoint URL for sovereign clouds (Azure Government, Azure China)
        /// Example: "https://mystorageaccount.blob.core.usgovcloudapi.net"
        #[serde(default)]
        endpoint: Option<String>,
        /// Enable Workload Identity authentication (for AKS)
        /// When true, uses AZURE_FEDERATED_TOKEN_FILE, AZURE_CLIENT_ID, AZURE_TENANT_ID
        #[serde(default)]
        use_workload_identity: Option<bool>,
        /// Azure AD client ID (for Workload Identity or service principal)
        #[serde(default)]
        client_id: Option<String>,
        /// Azure AD tenant ID (for Workload Identity or service principal)
        #[serde(default)]
        tenant_id: Option<String>,
        /// Client secret (for service principal authentication)
        #[serde(default)]
        client_secret: Option<String>,
        /// SAS token for shared access signature authentication
        #[serde(default)]
        sas_token: Option<String>,
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
}
