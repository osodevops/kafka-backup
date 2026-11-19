//! OAuth token providers against wiremock: Entra-style client credentials
//! (basic and post), OIDC discovery, Azure IMDS, Azure workload identity,
//! retries, error mapping, and the shared cache in front of a real provider.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use base64::Engine;
use kafka_backup_core::oauth::cache::{CacheSettings, TokenCache};
use kafka_backup_core::oauth::http::{ClientAuth, RetryPolicy};
use kafka_backup_core::oauth::providers::azure_imds::AzureManagedIdentityProvider;
use kafka_backup_core::oauth::providers::azure_workload::AzureWorkloadIdentityProvider;
use kafka_backup_core::oauth::providers::oidc::{OidcClientCredentialsProvider, OidcSettings};
use kafka_backup_core::oauth::{OauthProviderError, OauthTokenProvider};
use wiremock::matchers::{body_string_contains, header, method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

const SECRET: &str = "s3cret/with+special=chars";

fn fast_retry() -> RetryPolicy {
    RetryPolicy {
        base_delay: Duration::from_millis(5),
        max_delay: Duration::from_millis(50),
        ..RetryPolicy::default()
    }
}

fn entra_token(token: &str) -> serde_json::Value {
    serde_json::json!({
        "token_type": "Bearer",
        "expires_in": 3599,
        "ext_expires_in": 3599,
        "access_token": token,
    })
}

fn oidc(server: &MockServer, auth: ClientAuth) -> OidcClientCredentialsProvider {
    OidcClientCredentialsProvider::new(OidcSettings {
        token_endpoint_url: Some(format!("{}/tenant-1/oauth2/v2.0/token", server.uri())),
        client_id: "app id".into(),
        client_secret: zeroize::Zeroizing::new(SECRET.into()),
        client_auth: auth,
        scope: Some("api://kafka-app/.default".into()),
        retry: fast_retry(),
        ..Default::default()
    })
    .unwrap()
}

fn no_authorization_header(req: &Request) -> bool {
    !req.headers.contains_key("authorization")
}

fn body(req: &Request) -> String {
    String::from_utf8_lossy(&req.body).into_owned()
}

#[tokio::test]
async fn entra_client_secret_basic() {
    let server = MockServer::start().await;
    // RFC 6749 §2.3.1: id and secret are form-urlencoded before Basic.
    let expected =
        base64::engine::general_purpose::STANDARD.encode("app+id:s3cret%2Fwith%2Bspecial%3Dchars");
    Mock::given(method("POST"))
        .and(path("/tenant-1/oauth2/v2.0/token"))
        .and(header(
            "authorization",
            format!("Basic {expected}").as_str(),
        ))
        .and(body_string_contains("grant_type=client_credentials"))
        .and(body_string_contains(
            "scope=api%3A%2F%2Fkafka-app%2F.default",
        ))
        .and(|req: &Request| !body(req).contains("client_secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(entra_token("tok-basic")))
        .expect(1)
        .mount(&server)
        .await;

    let before = SystemTime::now();
    let token = oidc(&server, ClientAuth::Basic).get_token().await.unwrap();
    assert_eq!(token.value(), "tok-basic");
    let exp = token.expires_at.unwrap();
    assert!(
        exp >= before + Duration::from_secs(3599)
            && exp <= SystemTime::now() + Duration::from_secs(3599)
    );
}

#[tokio::test]
async fn entra_client_secret_post() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/tenant-1/oauth2/v2.0/token"))
        .and(no_authorization_header)
        .and(body_string_contains("client_id=app+id"))
        .and(body_string_contains(
            "client_secret=s3cret%2Fwith%2Bspecial%3Dchars",
        ))
        .and(body_string_contains("grant_type=client_credentials"))
        .respond_with(ResponseTemplate::new(200).set_body_json(entra_token("tok-post")))
        .expect(1)
        .mount(&server)
        .await;
    let token = oidc(&server, ClientAuth::Post).get_token().await.unwrap();
    assert_eq!(token.value(), "tok-post");
}

#[tokio::test]
async fn audience_extra_params_and_string_expires_in() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("audience=api%3A%2F%2Fkafka"))
        .and(body_string_contains("resource=r1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "tok-aud", "token_type": "bearer", "expires_in": "120"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let provider = OidcClientCredentialsProvider::new(OidcSettings {
        token_endpoint_url: Some(format!("{}/token", server.uri())),
        client_id: "c".into(),
        client_secret: zeroize::Zeroizing::new("s".into()),
        audience: Some("api://kafka".into()),
        extra_params: BTreeMap::from([("resource".to_string(), "r1".to_string())]),
        retry: fast_retry(),
        ..Default::default()
    })
    .unwrap();
    let token = provider.get_token().await.unwrap();
    assert_eq!(token.value(), "tok-aud");
    assert!(token.expires_at.unwrap() <= SystemTime::now() + Duration::from_secs(120));
}

#[tokio::test]
async fn oidc_discovery_is_fetched_once() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/realms/kafka/.well-known/openid-configuration"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "issuer": format!("{}/realms/kafka", server.uri()),
            "token_endpoint": format!("{}/realms/kafka/protocol/openid-connect/token", server.uri()),
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/realms/kafka/protocol/openid-connect/token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(entra_token("tok-discovered")))
        .expect(2)
        .mount(&server)
        .await;
    let provider = OidcClientCredentialsProvider::new(OidcSettings {
        issuer: Some(format!("{}/realms/kafka/", server.uri())),
        client_id: "c".into(),
        client_secret: zeroize::Zeroizing::new("s".into()),
        retry: fast_retry(),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(
        provider.get_token().await.unwrap().value(),
        "tok-discovered"
    );
    assert_eq!(
        provider.get_token().await.unwrap().value(),
        "tok-discovered"
    );
}

#[tokio::test]
async fn discovery_failure_is_reported() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404).set_body_string("no such realm"))
        .mount(&server)
        .await;
    let provider = OidcClientCredentialsProvider::new(OidcSettings {
        issuer: Some(server.uri()),
        client_id: "c".into(),
        client_secret: zeroize::Zeroizing::new("s".into()),
        retry: fast_retry(),
        ..Default::default()
    })
    .unwrap();
    let err = provider.get_token().await.unwrap_err().to_string();
    assert!(err.contains("404") && err.contains("discovery"), "{err}");
}

#[tokio::test]
async fn client_error_fails_fast_without_echoing_the_secret() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "invalid_client",
            "error_description": format!("AADSTS7000215: Invalid client secret provided ({SECRET})."),
        })))
        .expect(1)
        .mount(&server)
        .await;
    let err = oidc(&server, ClientAuth::Basic)
        .get_token()
        .await
        .unwrap_err();
    match &err {
        OauthProviderError::Endpoint {
            status,
            error,
            description,
        } => {
            assert_eq!(*status, Some(401));
            assert_eq!(error, "invalid_client");
            assert!(description.as_deref().unwrap().contains("AADSTS7000215"));
        }
        other => panic!("{other:?}"),
    }
    assert!(!err.to_string().contains(SECRET), "{err}");
}

#[tokio::test]
async fn retries_429_and_5xx_then_succeeds() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(429).insert_header("Retry-After", "0"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503).set_body_string("busy"))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(entra_token("tok-after-retry")))
        .expect(1)
        .mount(&server)
        .await;
    let token = oidc(&server, ClientAuth::Basic).get_token().await.unwrap();
    assert_eq!(token.value(), "tok-after-retry");
}

#[tokio::test]
async fn persistent_5xx_gives_up_after_the_retry_budget() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(502).set_body_string("bad gateway"))
        .expect(4) // first attempt + 3 retries
        .mount(&server)
        .await;
    let err = oidc(&server, ClientAuth::Basic)
        .get_token()
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            OauthProviderError::Endpoint {
                status: Some(502),
                ..
            }
        ),
        "{err:?}"
    );
}

#[tokio::test]
async fn unparseable_success_never_echoes_the_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/json")
                .set_body_string(r#"{"token":"leaky-token-value"}"#),
        )
        .mount(&server)
        .await;
    let err = oidc(&server, ClientAuth::Basic)
        .get_token()
        .await
        .unwrap_err();
    assert!(
        matches!(err, OauthProviderError::InvalidResponse(_)),
        "{err:?}"
    );
    assert!(!format!("{err} {err:?}").contains("leaky-token-value"));
}

#[tokio::test]
async fn unreachable_endpoint_is_an_http_error() {
    let provider = OidcClientCredentialsProvider::new(OidcSettings {
        token_endpoint_url: Some("http://127.0.0.1:9/token".into()),
        client_id: "c".into(),
        client_secret: zeroize::Zeroizing::new("s".into()),
        retry: fast_retry(),
        ..Default::default()
    })
    .unwrap();
    assert!(matches!(
        provider.get_token().await,
        Err(OauthProviderError::Http(_))
    ));
}

// ── Azure IMDS ──────────────────────────────────────────────────────────

fn imds_body(token: &str, expires_on: u64) -> serde_json::Value {
    // IMDS sends every number as a string.
    serde_json::json!({
        "access_token": token,
        "client_id": "uami-client-id",
        "expires_in": "86399",
        "expires_on": expires_on.to_string(),
        "ext_expires_in": "86399",
        "not_before": (expires_on - 86399).to_string(),
        "resource": "api://kafka-app",
        "token_type": "Bearer",
    })
}

fn epoch_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[tokio::test]
async fn imds_user_assigned_identity_with_string_expiries() {
    let server = MockServer::start().await;
    let expires_on = epoch_now() + 3600;
    Mock::given(method("GET"))
        .and(path("/metadata/identity/oauth2/token"))
        .and(header("Metadata", "true"))
        .and(query_param("api-version", "2018-02-01"))
        .and(query_param("resource", "api://kafka-app"))
        .and(query_param("client_id", "uami-client-id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(imds_body("imds-token", expires_on)))
        .expect(1)
        .mount(&server)
        .await;
    let provider = AzureManagedIdentityProvider::new(
        "api://kafka-app",
        Some("uami-client-id".into()),
        Some(format!("{}/metadata/identity/oauth2/token", server.uri())),
        None,
        RetryPolicy {
            retry_not_found: true,
            ..fast_retry()
        },
    )
    .unwrap();
    let token = provider.get_token().await.unwrap();
    assert_eq!(token.value(), "imds-token");
    assert_eq!(
        token.expires_at,
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(expires_on)),
        "expires_on (absolute) wins over expires_in"
    );
}

#[tokio::test]
async fn imds_system_assigned_identity_sends_no_client_id_and_retries_404() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(|req: &Request| !req.url.query().unwrap_or("").contains("client_id"))
        .respond_with(ResponseTemplate::new(404).set_body_string("identity not yet assigned"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(|req: &Request| !req.url.query().unwrap_or("").contains("client_id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "system-token", "expires_in": "600", "token_type": "Bearer"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let provider = AzureManagedIdentityProvider::new(
        "api://kafka-app",
        None,
        Some(format!("{}/metadata/identity/oauth2/token", server.uri())),
        None,
        RetryPolicy {
            retry_not_found: true,
            ..fast_retry()
        },
    )
    .unwrap();
    let token = provider.get_token().await.unwrap();
    assert_eq!(token.value(), "system-token");
    assert!(token.expires_at.unwrap() <= SystemTime::now() + Duration::from_secs(600));
}

#[tokio::test]
async fn imds_error_body_is_mapped() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error": "invalid_request", "error_description": "Identity not found"
        })))
        .expect(1)
        .mount(&server)
        .await;
    let provider = AzureManagedIdentityProvider::new(
        "api://x",
        Some("unknown".into()),
        Some(server.uri()),
        None,
        RetryPolicy {
            retry_not_found: true,
            ..fast_retry()
        },
    )
    .unwrap();
    let err = provider.get_token().await.unwrap_err().to_string();
    assert!(
        err.contains("400")
            && err.contains("invalid_request")
            && err.contains("Identity not found"),
        "{err}"
    );
}

// ── Azure workload identity ─────────────────────────────────────────────

#[tokio::test]
async fn workload_identity_client_assertion_rereads_the_token_file() {
    let server = MockServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let token_file = dir.path().join("azure-identity-token");
    std::fs::write(&token_file, "federated-jwt-one\n").unwrap();

    for (assertion, issued) in [
        ("federated-jwt-one", "entra-1"),
        ("federated-jwt-two", "entra-2"),
    ] {
        Mock::given(method("POST"))
            .and(path("/tenant-9/oauth2/v2.0/token"))
            .and(no_authorization_header)
            .and(body_string_contains("grant_type=client_credentials"))
            .and(body_string_contains("client_id=wi-client"))
            .and(body_string_contains("scope=api%3A%2F%2Fkafka-app%2F.default"))
            .and(body_string_contains(
                "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer",
            ))
            .and(body_string_contains(format!("client_assertion={assertion}").as_str()))
            .and(|req: &Request| !body(req).contains("client_secret"))
            .respond_with(ResponseTemplate::new(200).set_body_json(entra_token(issued)))
            .expect(1)
            .mount(&server)
            .await;
    }

    let provider = AzureWorkloadIdentityProvider::new(
        "tenant-9",
        "wi-client",
        token_file.clone(),
        &format!("{}/", server.uri()),
        "api://kafka-app/.default",
        fast_retry(),
    )
    .unwrap();
    assert_eq!(
        provider.token_url(),
        format!("{}/tenant-9/oauth2/v2.0/token", server.uri())
    );
    assert_eq!(provider.get_token().await.unwrap().value(), "entra-1");
    // The kubelet rotates the projected token: the next fetch uses the new one.
    std::fs::write(&token_file, "federated-jwt-two").unwrap();
    assert_eq!(provider.get_token().await.unwrap().value(), "entra-2");
}

#[tokio::test]
async fn workload_identity_missing_token_file_is_clear() {
    let provider = AzureWorkloadIdentityProvider::new(
        "t",
        "c",
        "/nonexistent/azure-identity-token".into(),
        "https://login.microsoftonline.com/",
        "api://x/.default",
        fast_retry(),
    )
    .unwrap();
    let err = provider.get_token().await.unwrap_err().to_string();
    assert!(err.contains("federated token file"), "{err}");
}

// ── Cache in front of a real provider ───────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hundred_concurrent_handshakes_make_one_token_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(entra_token("shared-token"))
                .set_delay(Duration::from_millis(100)),
        )
        .expect(1)
        .mount(&server)
        .await;
    let cache = TokenCache::new(
        "entra",
        Arc::new(oidc(&server, ClientAuth::Basic)),
        CacheSettings::default(),
    );
    let tasks: Vec<_> = (0..100)
        .map(|_| {
            let cache = cache.clone();
            tokio::spawn(async move { cache.get_with_min_ttl(Duration::from_secs(60)).await })
        })
        .collect();
    for task in tasks {
        assert_eq!(task.await.unwrap().unwrap().value(), "shared-token");
    }
    assert_eq!(cache.fetch_count(), 1);
}

#[tokio::test]
async fn transport_errors_are_reported_once() {
    let provider = OidcClientCredentialsProvider::new(OidcSettings {
        token_endpoint_url: Some("http://127.0.0.1:9/token".into()),
        client_id: "c".into(),
        client_secret: zeroize::Zeroizing::new("s".into()),
        retry: fast_retry(),
        ..Default::default()
    })
    .unwrap();
    let err = provider.get_token().await.unwrap_err().to_string();
    assert_eq!(err.matches("token request failed").count(), 1, "{err}");
}
