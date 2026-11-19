//! No token and no secret ever reaches the logs, even at TRACE with every
//! dependency's logging on: token fetches from every provider (success,
//! retry and failure), SASL payloads, broker rejections, registry requests,
//! and `Debug` / serialization of the configuration and the installed
//! factories.
//!
//! One test per binary: it installs a global subscriber.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use kafka_backup_core::kafka::SaslMechanismPlugin;
use kafka_backup_core::oauth::cache::{CacheSettings, TokenCache};
use kafka_backup_core::oauth::http::{ClientAuth, RetryPolicy};
use kafka_backup_core::oauth::providers::azure_imds::AzureManagedIdentityProvider;
use kafka_backup_core::oauth::providers::azure_workload::AzureWorkloadIdentityProvider;
use kafka_backup_core::oauth::providers::file::FileTokenProvider;
use kafka_backup_core::oauth::providers::oidc::{OidcClientCredentialsProvider, OidcSettings};
use kafka_backup_core::oauth::{OauthBearerPlugin, OauthTokenProvider, RegistryOauth};
use kafka_backup_core::Config;
use tracing_subscriber::fmt::MakeWriter;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ACCESS_TOKEN: &str =
    "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJrYWZrYS1iYWNrdXAifQ.TOKEN-SIGNATURE-7f3a";
const IMDS_TOKEN: &str = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJpbWRzIn0.IMDS-SIGNATURE-41c9";
const WORKLOAD_TOKEN: &str = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ3aSJ9.WORKLOAD-SIGNATURE-b07e";
const FEDERATED_TOKEN: &str = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJzYSJ9.FEDERATED-SIGNATURE-3d55";
const FILE_TOKEN: &str = "FILE-TOKEN-c2a917";
const CLIENT_SECRET: &str = "CLIENT-SECRET-9d2c41";
const STATIC_TOKEN: &str = "STATIC-TOKEN-55e1b0";

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Vec<u8>>>);

impl Write for Capture {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Capture {
    type Writer = Capture;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
        ..RetryPolicy::default()
    }
}

fn oidc(url: String) -> OidcClientCredentialsProvider {
    OidcClientCredentialsProvider::new(OidcSettings {
        token_endpoint_url: Some(url),
        client_id: "kafka-backup".into(),
        client_secret: zeroize::Zeroizing::new(CLIENT_SECRET.into()),
        client_auth: ClientAuth::Basic,
        scope: Some("kafka".into()),
        retry: fast_retry(),
        ..Default::default()
    })
    .unwrap()
}

/// Fetch through a cache and log the outcome the way the CLI does.
async fn fetch(name: &str, provider: Arc<dyn OauthTokenProvider>) -> TokenCache {
    let cache = TokenCache::new(name, provider, CacheSettings::default());
    match cache.get().await {
        Ok(token) => tracing::trace!(provider = name, token = ?token, "token fetched"),
        Err(err) => {
            tracing::error!(provider = name, error = %err, debug = ?err, "token fetch failed")
        }
    }
    cache
}

#[tokio::test]
async fn nothing_secret_is_logged_at_trace() {
    let capture = Capture::default();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_writer(capture.clone())
        .with_ansi(false)
        .init();

    let idp = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ok"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": ACCESS_TOKEN, "token_type": "Bearer", "expires_in": 3600
        })))
        .mount(&idp)
        .await;
    Mock::given(method("POST"))
        .and(path("/flaky"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(1)
        .mount(&idp)
        .await;
    Mock::given(method("POST"))
        .and(path("/flaky"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": ACCESS_TOKEN, "expires_in": 3600
        })))
        .mount(&idp)
        .await;
    Mock::given(method("POST"))
        .and(path("/denied"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "invalid_client",
            "error_description": format!("secret {CLIENT_SECRET} is wrong; token {ACCESS_TOKEN}"),
        })))
        .mount(&idp)
        .await;
    // A 200 that is not a token response but carries one in its body.
    Mock::given(method("POST"))
        .and(path("/garbled"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(format!("<html>your token is {ACCESS_TOKEN}</html>")),
        )
        .mount(&idp)
        .await;
    // Azure IMDS: success, and an error body that echoes a token.
    Mock::given(method("GET"))
        .and(path("/imds/ok"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": IMDS_TOKEN, "expires_in": "3600", "token_type": "Bearer"
        })))
        .mount(&idp)
        .await;
    Mock::given(method("GET"))
        .and(path("/imds/denied"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": "invalid_request",
            "error_description": format!("Identity not found; saw {IMDS_TOKEN}"),
        })))
        .mount(&idp)
        .await;
    // Azure workload identity: success, and a refusal that echoes the
    // federated assertion.
    Mock::given(method("POST"))
        .and(path("/tenant-ok/oauth2/v2.0/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": WORKLOAD_TOKEN, "token_type": "Bearer", "expires_in": 3600
        })))
        .mount(&idp)
        .await;
    Mock::given(method("POST"))
        .and(path("/tenant-denied/oauth2/v2.0/token"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "invalid_client",
            "error_description": format!("AADSTS70021: no matching federated identity for {FEDERATED_TOKEN}"),
        })))
        .mount(&idp)
        .await;
    let sr = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["s"])))
        .mount(&sr)
        .await;

    // ── oidc_client_credentials: success, retry, refusal, garbled 200 ──
    let ok = fetch("ok", Arc::new(oidc(format!("{}/ok", idp.uri())))).await;
    assert_eq!(ok.get().await.unwrap().value(), ACCESS_TOKEN);
    let flaky = fetch("flaky", Arc::new(oidc(format!("{}/flaky", idp.uri())))).await;
    flaky.get().await.unwrap();
    let denied = fetch("denied", Arc::new(oidc(format!("{}/denied", idp.uri())))).await;
    assert!(denied.get().await.is_err());
    fetch("garbled", Arc::new(oidc(format!("{}/garbled", idp.uri())))).await;
    fetch(
        "unreachable",
        Arc::new(oidc("http://127.0.0.1:1/token".into())),
    )
    .await;

    // ── azure_managed_identity ──
    for (name, endpoint) in [("imds-ok", "/imds/ok"), ("imds-denied", "/imds/denied")] {
        let provider = AzureManagedIdentityProvider::new(
            "api://kafka-app",
            Some("uami-client-id".into()),
            Some(format!("{}{endpoint}", idp.uri())),
            None,
            fast_retry(),
        )
        .unwrap();
        let cache = fetch(name, Arc::new(provider)).await;
        assert_eq!(cache.get().await.is_ok(), name == "imds-ok");
    }

    // ── azure_workload_identity: success, refusal, missing token file ──
    let dir = tempfile::tempdir().unwrap();
    let federated = dir.path().join("azure-identity-token");
    std::fs::write(&federated, FEDERATED_TOKEN).unwrap();
    for (name, tenant, file) in [
        ("wi-ok", "tenant-ok", federated.clone()),
        ("wi-denied", "tenant-denied", federated.clone()),
        ("wi-no-file", "tenant-ok", dir.path().join("missing")),
    ] {
        let provider = AzureWorkloadIdentityProvider::new(
            tenant,
            "wi-client",
            file,
            &format!("{}/", idp.uri()),
            "api://kafka-app/.default",
            fast_retry(),
        )
        .unwrap();
        let cache = fetch(name, Arc::new(provider)).await;
        assert_eq!(cache.get().await.is_ok(), name == "wi-ok");
    }

    // ── file: success, empty file, missing file, unreadable expiry ──
    let token_file = dir.path().join("token");
    std::fs::write(&token_file, format!("{FILE_TOKEN}\n")).unwrap();
    let empty_file = dir.path().join("empty");
    std::fs::write(&empty_file, "\n").unwrap();
    let bad_expiry = dir.path().join("expiry");
    std::fs::write(&bad_expiry, "not a time").unwrap();
    let file_ok = fetch(
        "file-ok",
        Arc::new(FileTokenProvider::new(&token_file, None)),
    )
    .await;
    assert_eq!(file_ok.get().await.unwrap().value(), FILE_TOKEN);
    fetch(
        "file-empty",
        Arc::new(FileTokenProvider::new(&empty_file, None)),
    )
    .await;
    fetch(
        "file-missing",
        Arc::new(FileTokenProvider::new(dir.path().join("nope"), None)),
    )
    .await;
    fetch(
        "file-bad-expiry",
        Arc::new(FileTokenProvider::new(&token_file, Some(bad_expiry))),
    )
    .await;

    // ── SASL payloads, re-auth, a broker rejection, a provider failure ──
    let plugin = OauthBearerPlugin::from_cache(ok.clone(), Default::default());
    let payload = plugin.initial_payload().await.unwrap();
    assert!(String::from_utf8_lossy(&payload).contains(ACCESS_TOKEN));
    plugin.reauth_payload().await.unwrap();
    plugin
        .continue_payload(br#"{"status":"invalid_token"}"#)
        .await
        .unwrap();
    tracing::warn!(error = %plugin.interpret_server_error(b"Authentication failed"), "rejected");
    let failing = OauthBearerPlugin::from_cache(denied.clone(), Default::default());
    tracing::error!(error = %failing.initial_payload().await.unwrap_err(), "handshake failed");
    tracing::trace!(plugin = ?plugin, cache = ?ok, "plugin state");

    // ── Configuration and install (Debug of everything, incl. factories) ──
    let yaml = format!(
        r#"
mode: restore
backup_id: b
source:
  bootstrap_servers: ["127.0.0.1:1"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth: {{ provider: idp, extensions: {{ logicalCluster: lkc-1 }} }}
target:
  bootstrap_servers: ["127.0.0.1:1"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth: {{ provider: fixed }}
storage: {{ backend: memory }}
oauth:
  providers:
    idp:
      type: oidc_client_credentials
      token_endpoint_url: {idp}/ok
      client_id: kafka-backup
      client_secret: {CLIENT_SECRET}
    fixed: {{ type: static, token: {STATIC_TOKEN} }}
"#,
        idp = idp.uri(),
    );
    let mut config = Config::from_yaml_with_warnings(&yaml).unwrap().0;
    let installed = kafka_backup_core::oauth::install(&mut config).unwrap();
    assert_eq!(installed.caches.len(), 2);
    for cache in &installed.caches {
        cache.get().await.unwrap();
    }
    tracing::trace!(config = ?config, installed = ?installed, "installed");
    tracing::trace!(yaml = %serde_yaml::to_string(&config).unwrap(), "re-serialised");
    tracing::trace!(json = %serde_json::to_string(&config.oauth).unwrap(), "re-serialised");

    // ── A Schema Registry request with the bearer token ──
    let registry = RegistryOauth::new(installed.caches[0].clone(), Some("lsrc-1"), None);
    let response = registry
        .apply(reqwest::Client::new().get(format!("{}/subjects", sr.uri())))
        .await
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    tracing::trace!(registry = ?registry, "registry auth");

    let logs = String::from_utf8(capture.0.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("OAuth token fetched"),
        "the capture works:\n{logs}"
    );
    assert!(
        logs.contains("invalid_client"),
        "errors are logged:\n{logs}"
    );
    for provider in [
        "imds-denied",
        "wi-denied",
        "wi-no-file",
        "file-empty",
        "file-missing",
        "file-bad-expiry",
        "garbled",
        "unreachable",
    ] {
        assert!(
            logs.lines()
                .any(|l| l.contains("token fetch failed") && l.contains(provider)),
            "the failure path of `{provider}` ran:\n{logs}"
        );
    }
    let basic =
        base64::engine::general_purpose::STANDARD.encode(format!("kafka-backup:{CLIENT_SECRET}"));
    for forbidden in [
        ACCESS_TOKEN,
        "TOKEN-SIGNATURE-7f3a",
        IMDS_TOKEN,
        "IMDS-SIGNATURE-41c9",
        WORKLOAD_TOKEN,
        "WORKLOAD-SIGNATURE-b07e",
        FEDERATED_TOKEN,
        "FEDERATED-SIGNATURE-3d55",
        FILE_TOKEN,
        CLIENT_SECRET,
        STATIC_TOKEN,
        basic.as_str(),
    ] {
        assert!(
            !logs.contains(forbidden),
            "`{forbidden}` appears in the logs:\n{}",
            logs.lines()
                .filter(|l| l.contains(forbidden))
                .collect::<Vec<_>>()
                .join("\n")
        );
    }
    // Nothing JWT-shaped at all, whatever its source.
    let jwt = regex::Regex::new(r"eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]{4,}\.").unwrap();
    assert!(
        !jwt.is_match(&logs),
        "a JWT-shaped string appears in the logs:\n{}",
        logs.lines()
            .filter(|l| jwt.is_match(l))
            .collect::<Vec<_>>()
            .join("\n")
    );
}
