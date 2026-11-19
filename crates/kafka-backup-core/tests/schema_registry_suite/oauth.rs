//! Schema Registry with OAuth: the bearer token comes from the token cache
//! shared with the Kafka clusters of the same provider, the Confluent Cloud
//! routing headers are sent, a 401 fetches a new token once, and
//! `oauth::install` does the wiring.

use std::time::Duration;

use kafka_backup_core::schema_registry::client::SchemaRegistryClient;
use kafka_backup_core::schema_registry::config::{SchemaRegistryAuth, SchemaRegistryConfig};
use kafka_backup_core::Config;
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

async fn token_endpoint(server: &MockServer, token: &str, expect: u64) {
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": token, "token_type": "Bearer", "expires_in": 3600
        })))
        .expect(expect)
        .mount(server)
        .await;
}

/// A config with an OIDC provider at `idp` and Schema Registry at `sr`.
fn config(idp: &MockServer, sr: &MockServer, auth: &str, extra: &str) -> Config {
    let yaml = format!(
        r#"
mode: backup
backup_id: b
source: {{ bootstrap_servers: ["127.0.0.1:1"] }}
storage: {{ backend: memory }}
oauth:
  providers:
    idp:
      type: oidc_client_credentials
      token_endpoint_url: {idp}/token
      client_id: sr-client
      client_secret: sr-secret
      allow_insecure_http: true
  cache: {{ min_refresh_interval: 0s }}
schema_registry:
  url: {sr}
  auth: {auth}
  connection: {{ max_retries: 2, retry_backoff_ms: 1 }}
{extra}"#,
        idp = idp.uri(),
        sr = sr.uri(),
    );
    let (config, ignored) = Config::from_yaml_with_warnings(&yaml).unwrap();
    assert!(ignored.is_empty(), "{ignored:?}");
    config
}

fn sr_config(config: &Config) -> SchemaRegistryConfig {
    config.schema_registry.clone().unwrap()
}

#[tokio::test]
async fn bearer_and_confluent_headers_with_a_shared_cache() {
    let idp = MockServer::start().await;
    let sr = MockServer::start().await;
    token_endpoint(&idp, "sr-token", 1).await;
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .and(header("authorization", "Bearer sr-token"))
        .and(header("target-sr-cluster", "lsrc-abc123"))
        // Raw value: wiremock's `header` matcher splits on commas.
        .and(|req: &Request| {
            req.headers
                .get("confluent-identity-pool-id")
                .and_then(|v| v.to_str().ok())
                == Some("pool-a,pool-b")
        })
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["s1"])))
        .expect(3)
        .mount(&sr)
        .await;

    let mut cfg = config(
        &idp,
        &sr,
        r#"{ type: oauth, provider: idp, target_sr_cluster: lsrc-abc123, identity_pool_id: "pool-a, pool-b" }"#,
        "",
    );
    let installed = kafka_backup_core::oauth::install(&mut cfg).unwrap();
    assert_eq!(installed.caches.len(), 1);

    // Two clients (as two control-plane passes would build) share one token.
    let a = SchemaRegistryClient::new(&sr_config(&cfg)).unwrap();
    let b = SchemaRegistryClient::new(&sr_config(&cfg)).unwrap();
    assert_eq!(a.list_subjects(false).await.unwrap(), vec!["s1"]);
    assert_eq!(a.list_subjects(false).await.unwrap(), vec!["s1"]);
    assert_eq!(b.list_subjects(false).await.unwrap(), vec!["s1"]);
}

/// The Schema Registry restore's writes (IMPORT mode, registrations with
/// original IDs) go through the same authenticated request path as the
/// backup's reads: bearer token plus the Confluent Cloud routing headers.
#[tokio::test]
async fn restore_writes_carry_the_bearer_and_confluent_headers() {
    let idp = MockServer::start().await;
    let sr = MockServer::start().await;
    token_endpoint(&idp, "restore-token", 1).await;
    Mock::given(method("PUT"))
        .and(path("/mode"))
        .and(header("authorization", "Bearer restore-token"))
        .and(header("target-sr-cluster", "lsrc-restore"))
        .and(header("confluent-identity-pool-id", "pool-r"))
        .and(body_string_contains("IMPORT"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"mode": "IMPORT"})),
        )
        .expect(1)
        .mount(&sr)
        .await;
    Mock::given(method("POST"))
        .and(path("/subjects/orders-value/versions"))
        .and(header("authorization", "Bearer restore-token"))
        .and(header("target-sr-cluster", "lsrc-restore"))
        .and(header("confluent-identity-pool-id", "pool-r"))
        .and(body_string_contains("\"id\":42"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"id": 42, "version": 1})),
        )
        .expect(1)
        .mount(&sr)
        .await;

    let mut cfg = config(
        &idp,
        &sr,
        "{ type: oauth, provider: idp, target_sr_cluster: lsrc-restore, identity_pool_id: pool-r }",
        "",
    );
    kafka_backup_core::oauth::install(&mut cfg).unwrap();
    let client = SchemaRegistryClient::new(&sr_config(&cfg)).unwrap();
    client.set_global_mode("IMPORT").await.unwrap();
    let response = client
        .register_schema(
            "orders-value",
            &kafka_backup_core::schema_registry::models::RegisterSchemaRequest {
                schema: r#""string""#.to_string(),
                schema_type: None,
                references: Vec::new(),
                id: Some(42),
                version: Some(1),
                metadata: None,
                rule_set: None,
            },
        )
        .await
        .unwrap();
    assert_eq!(response.id, 42);
}

#[tokio::test]
async fn identity_pool_header_only_when_configured() {
    let idp = MockServer::start().await;
    let sr = MockServer::start().await;
    token_endpoint(&idp, "t", 1).await;
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .and(header("target-sr-cluster", "lsrc-1"))
        .and(|req: &Request| !req.headers.contains_key("confluent-identity-pool-id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&sr)
        .await;
    let mut cfg = config(
        &idp,
        &sr,
        "{ type: oauth, provider: idp, target_sr_cluster: lsrc-1 }",
        "",
    );
    kafka_backup_core::oauth::install(&mut cfg).unwrap();
    SchemaRegistryClient::new(&sr_config(&cfg))
        .unwrap()
        .list_subjects(false)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_401_refreshes_the_token_once() {
    let idp = MockServer::start().await;
    let sr = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "revoked", "expires_in": 3600
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&idp)
        .await;
    token_endpoint(&idp, "fresh", 1).await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer revoked"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error_code": 401, "message": "Unauthorized"
        })))
        .expect(1)
        .mount(&sr)
        .await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer fresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["ok"])))
        .expect(1)
        .mount(&sr)
        .await;
    let mut cfg = config(&idp, &sr, "{ type: oauth, provider: idp }", "");
    kafka_backup_core::oauth::install(&mut cfg).unwrap();
    let client = SchemaRegistryClient::new(&sr_config(&cfg)).unwrap();
    // min_refresh_interval is 0 here, so the rejected token is dropped.
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert_eq!(client.list_subjects(false).await.unwrap(), vec!["ok"]);
}

#[tokio::test]
async fn a_provider_reference_needs_install() {
    let idp = MockServer::start().await;
    let sr = MockServer::start().await;
    let cfg = config(&idp, &sr, "{ type: oauth, provider: idp }", "");
    let err = SchemaRegistryClient::new(&sr_config(&cfg))
        .err()
        .unwrap()
        .to_string();
    assert!(
        err.contains("provider 'idp'") && err.contains("not set up"),
        "{err}"
    );
    match sr_config(&cfg).auth.unwrap() {
        SchemaRegistryAuth::OAuth(o) => assert!(o.token_source.is_none()),
        other => panic!("{other:?}"),
    }
}

#[tokio::test]
async fn token_endpoint_failure_is_an_auth_error() {
    let idp = MockServer::start().await;
    let sr = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": "unauthorized_client", "error_description": "client not allowed"
        })))
        .mount(&idp)
        .await;
    let mut cfg = config(&idp, &sr, "{ type: oauth, provider: idp }", "");
    kafka_backup_core::oauth::install(&mut cfg).unwrap();
    let err = SchemaRegistryClient::new(&sr_config(&cfg))
        .unwrap()
        .list_subjects(false)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("unauthorized_client") && err.contains("provider")
            || err.contains("OAuth token"),
        "{err}"
    );
    assert!(!err.contains("sr-secret"), "{err}");
}

#[tokio::test]
async fn uncertain_schema_registration_is_not_retried() {
    let idp = MockServer::start().await;
    let sr = MockServer::start().await;
    token_endpoint(&idp, "restore-token", 1).await;
    Mock::given(method("POST"))
        .and(path("/subjects/orders-value/versions"))
        .and(header("authorization", "Bearer restore-token"))
        .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
            "error_code": 503, "message": "response lost after registration"
        })))
        .expect(1)
        .mount(&sr)
        .await;
    let mut cfg = config(&idp, &sr, "{ type: oauth, provider: idp }", "");
    kafka_backup_core::oauth::install(&mut cfg).unwrap();
    let client = SchemaRegistryClient::new(&sr_config(&cfg)).unwrap();
    let result = client
        .register_schema(
            "orders-value",
            &kafka_backup_core::schema_registry::models::RegisterSchemaRequest {
                schema: r#""string""#.into(),
                schema_type: None,
                references: vec![],
                id: Some(42),
                version: Some(1),
                metadata: None,
                rule_set: None,
            },
        )
        .await;
    assert!(result.is_err());
    let writes = sr.received_requests().await.unwrap();
    assert_eq!(
        writes.len(),
        1,
        "uncertain registry writes must not be replayed"
    );
}

/// Issue #262: the registry client takes its tokens from the cache the
/// Kafka connections use, not from a second one.
#[tokio::test]
async fn the_registry_and_a_kafka_cluster_on_one_provider_share_one_token_cache() {
    let idp = MockServer::start().await;
    let sr = MockServer::start().await;
    // One token request serves the registry and the Kafka side.
    token_endpoint(&idp, "shared-token", 1).await;
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .and(header("authorization", "Bearer shared-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
        .expect(1)
        .mount(&sr)
        .await;

    let mut cfg = config(&idp, &sr, "{ type: oauth, provider: idp }", "");
    let source = cfg.source.as_mut().unwrap();
    source.security.security_protocol = kafka_backup_core::config::SecurityProtocol::SaslSsl;
    source.security.sasl_mechanism = Some(kafka_backup_core::config::SaslMechanism::OAuthBearer);
    source.security.oauth = Some(serde_yaml::from_str("provider: idp").unwrap());

    let installed = kafka_backup_core::oauth::install(&mut cfg).unwrap();
    assert_eq!(installed.caches.len(), 1, "one provider, one cache");
    assert_eq!(installed.uses.len(), 2, "{:?}", installed.uses);
    assert!(installed.uses.iter().any(|u| u.contains("Schema Registry")));
    assert!(cfg
        .source
        .as_ref()
        .unwrap()
        .security
        .sasl_mechanism_plugin_factory
        .is_some());

    // The Kafka side fetches the token (what a broker connection does)...
    let kafka_token = installed.caches[0].get().await.unwrap();
    assert_eq!(kafka_token.value(), "shared-token");
    // ...and the registry request reuses it.
    let client = SchemaRegistryClient::new(&sr_config(&cfg)).unwrap();
    assert!(client.list_subjects(false).await.unwrap().is_empty());
}

#[tokio::test]
async fn an_unknown_or_disabled_registry_provider_is_handled_at_install() {
    let idp = MockServer::start().await;
    let sr = MockServer::start().await;

    let mut cfg = config(&idp, &sr, "{ type: oauth, provider: nope }", "");
    let err = kafka_backup_core::oauth::install(&mut cfg)
        .unwrap_err()
        .to_string();
    assert!(err.contains("schema_registry.auth"), "{err}");
    assert!(err.contains("'nope'"), "{err}");
    assert!(err.contains("defined: idp"), "{err}");

    // A disabled section is left alone: no token cache is built for it.
    let mut cfg = config(
        &idp,
        &sr,
        "{ type: oauth, provider: nope }",
        "  enabled: false\n",
    );
    let installed = kafka_backup_core::oauth::install(&mut cfg).unwrap();
    assert!(installed.caches.is_empty() && !installed.is_active());
}
