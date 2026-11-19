//! Bearer authentication for a Schema Registry client (`RegistryOauth`):
//! the token comes from the cache the Kafka cluster uses, Confluent Cloud's
//! routing headers are sent only when configured, and a 401 makes the next
//! request fetch a new token. No Docker: wiremock stands in for the
//! identity provider and the registry.

use std::time::Duration;

use kafka_backup_core::oauth::registry_auth::{IDENTITY_POOL_HEADER, TARGET_SR_CLUSTER_HEADER};
use kafka_backup_core::oauth::{OauthConfig, ProviderRegistry, RegistryOauth};
use wiremock::matchers::{header, method, path};
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

fn oauth_config(idp: &MockServer, cache: &str) -> OauthConfig {
    serde_yaml::from_str(&format!(
        r#"
providers:
  idp:
    type: oidc_client_credentials
    token_endpoint_url: {}/token
    client_id: kafka-backup
    client_secret: registry-secret
cache: {cache}
"#,
        idp.uri()
    ))
    .unwrap()
}

#[tokio::test]
async fn bearer_and_confluent_headers_from_the_shared_cache() {
    let idp = MockServer::start().await;
    token_endpoint(&idp, "registry-token", 1).await;
    let sr = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/subjects"))
        .and(header("Authorization", "Bearer registry-token"))
        .and(header(TARGET_SR_CLUSTER_HEADER, "lsrc-abc12"))
        // wiremock's `header` matcher splits values on commas; compare the
        // raw header instead.
        .and(|req: &Request| {
            req.headers
                .get(IDENTITY_POOL_HEADER)
                .is_some_and(|v| v.as_bytes() == b"pool-a,pool-b")
        })
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["orders-value"])))
        .expect(3)
        .mount(&sr)
        .await;

    let config = oauth_config(&idp, "{}");
    let mut providers = ProviderRegistry::new(Some(&config)).unwrap();
    // The Kafka cluster and the registry ask for the same provider: one cache.
    let kafka_cache = providers.cache("idp", "source.security.oauth").unwrap();
    let registry_cache = providers.cache("idp", "schema_registry.auth").unwrap();
    assert!(kafka_cache.ptr_eq(&registry_cache));
    kafka_cache.get().await.unwrap();

    let auth = RegistryOauth::new(
        registry_cache,
        Some(" lsrc-abc12 "),
        Some(" pool-a , pool-b ,"),
    );
    let http = reqwest::Client::new();
    for _ in 0..3 {
        let response = auth
            .apply(http.get(format!("{}/subjects", sr.uri())))
            .await
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    assert_eq!(
        kafka_cache.fetch_count(),
        1,
        "one token for Kafka and the registry"
    );
}

#[tokio::test]
async fn routing_headers_are_sent_only_when_configured() {
    let idp = MockServer::start().await;
    token_endpoint(&idp, "plain-token", 1).await;
    let sr = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("Authorization", "Bearer plain-token"))
        .and(|req: &Request| {
            !req.headers.contains_key(TARGET_SR_CLUSTER_HEADER)
                && !req.headers.contains_key(IDENTITY_POOL_HEADER)
        })
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&sr)
        .await;

    let config = oauth_config(&idp, "{}");
    let cache = ProviderRegistry::new(Some(&config))
        .unwrap()
        .cache("idp", "schema_registry.auth")
        .unwrap();
    // Blank values count as unset.
    let auth = RegistryOauth::new(cache, Some("  "), Some(" , "));
    let response = auth
        .apply(reqwest::Client::new().get(sr.uri()))
        .await
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
}

#[tokio::test]
async fn a_401_drops_the_token_so_the_retry_fetches_a_new_one() {
    let idp = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "revoked-token", "expires_in": 3600
        })))
        .up_to_n_times(1)
        .mount(&idp)
        .await;
    token_endpoint(&idp, "fresh-token", 1).await;
    let sr = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("Authorization", "Bearer revoked-token"))
        .respond_with(ResponseTemplate::new(401))
        .expect(1)
        .mount(&sr)
        .await;
    Mock::given(method("GET"))
        .and(header("Authorization", "Bearer fresh-token"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&sr)
        .await;

    // `min_refresh_interval: 0s` lets a token fetched moments ago be dropped.
    let config = oauth_config(&idp, "{ min_refresh_interval: 0s }");
    let cache = ProviderRegistry::new(Some(&config))
        .unwrap()
        .cache("idp", "schema_registry.auth")
        .unwrap();
    let auth = RegistryOauth::new(cache, None, None);
    let http = reqwest::Client::new();

    let first = auth
        .apply(http.get(sr.uri()))
        .await
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(first.status(), 401);
    assert!(auth.invalidate(), "the rejected token is dropped");
    // The cache spaces provider calls a second apart.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let second = auth
        .apply(http.get(sr.uri()))
        .await
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(second.status(), 200);
    assert_eq!(auth.cache().fetch_count(), 2);
}

#[tokio::test]
async fn a_token_endpoint_failure_is_an_error_not_an_unauthenticated_request() {
    let idp = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "invalid_client", "error_description": "registry-secret is wrong"
        })))
        .mount(&idp)
        .await;
    let config = oauth_config(&idp, "{}");
    let cache = ProviderRegistry::new(Some(&config))
        .unwrap()
        .cache("idp", "schema_registry.auth")
        .unwrap();
    let auth = RegistryOauth::new(cache, None, None);
    let err = auth
        .apply(reqwest::Client::new().get("http://127.0.0.1:1/subjects"))
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("invalid_client"), "{err}");
    assert!(
        !err.contains("registry-secret"),
        "the secret is scrubbed: {err}"
    );
}
