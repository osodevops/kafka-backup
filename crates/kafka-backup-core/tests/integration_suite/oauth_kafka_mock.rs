//! OAUTHBEARER from configuration to the wire, with no Docker: a YAML
//! configuration is parsed, `oauth::install` wires its provider onto the
//! cluster, and `KafkaClient` authenticates against the in-process mock
//! broker with a token fetched from a wiremock identity provider.

use kafka_backup_core::kafka::KafkaClient;
use kafka_backup_core::{oauth, Config};
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::sasl_mock_broker::{Exchange, MockKafkaBroker};

fn config(broker: &str, idp: &MockServer, extensions: &str) -> Config {
    let yaml = format!(
        r#"
mode: backup
backup_id: oauth-mock
source:
  bootstrap_servers: ["{broker}"]
  security:
    security_protocol: SASL_PLAINTEXT
    sasl_mechanism: OAUTHBEARER
    oauth:
      provider: entra
      extensions: {extensions}
storage: {{ backend: memory }}
oauth:
  providers:
    entra:
      type: oidc_client_credentials
      token_endpoint_url: {idp}/tenant/oauth2/v2.0/token
      client_id: kafka-backup
      client_secret: mock-secret
      scope: api://kafka/.default
"#,
        idp = idp.uri()
    );
    let (config, warnings) = Config::from_yaml_with_warnings(&yaml).expect("valid config");
    assert!(warnings.is_empty(), "{warnings:?}");
    config
}

async fn token_endpoint(idp: &MockServer, token: &str) {
    Mock::given(method("POST"))
        .and(path("/tenant/oauth2/v2.0/token"))
        .and(body_string_contains("grant_type=client_credentials"))
        .and(body_string_contains("scope=api%3A%2F%2Fkafka%2F.default"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": token, "token_type": "Bearer", "expires_in": 3600
        })))
        .expect(1)
        .mount(idp)
        .await;
}

/// The client sends an RFC 7628 initial response carrying the identity
/// provider's token and the Confluent Cloud extensions of the binding.
#[tokio::test]
async fn configured_provider_authenticates_with_token_and_extensions() {
    let idp = MockServer::start().await;
    token_endpoint(&idp, "mock-access-token").await;
    let mock = MockKafkaBroker::start(vec![
        Exchange::HandshakeSuccess,
        Exchange::AuthenticateSuccess {
            auth_bytes: vec![],
            session_lifetime_ms: 0,
        },
    ])
    .await;

    let mut config = config(
        &mock.bootstrap(),
        &idp,
        r#"{ logicalCluster: lkc-abc12, identityPoolId: "pool-b1, pool-a2" }"#,
    );
    let installed = oauth::install(&mut config).expect("install");
    assert_eq!(
        installed.uses,
        vec!["source Kafka cluster (provider 'entra')"]
    );

    let client = KafkaClient::new(config.source.clone().unwrap());
    client.connect().await.expect("connect succeeds");

    let captured = mock.captured().await;
    assert_eq!(captured.handshake_mechanism.as_deref(), Some("OAUTHBEARER"));
    assert_eq!(captured.authenticate_payloads.len(), 1);
    assert_eq!(
        captured.authenticate_payloads[0],
        b"n,,\x01identityPoolId=pool-b1,pool-a2\x01logicalCluster=lkc-abc12\x01\
          auth=Bearer mock-access-token\x01\x01"
            .to_vec()
    );
    assert_eq!(installed.caches[0].fetch_count(), 1);
    assert_eq!(installed.caches[0].failure_count(), 0);
    mock.shutdown().await;
}

/// A token the broker refuses fails the connection with an error that names
/// the provider and what to check, and never contains the token.
#[tokio::test]
async fn a_rejected_token_names_the_provider_and_the_likely_causes() {
    let idp = MockServer::start().await;
    token_endpoint(&idp, "refused-access-token").await;
    let mock = MockKafkaBroker::start(vec![
        Exchange::HandshakeSuccess,
        Exchange::AuthenticateError {
            // SASL_AUTHENTICATION_FAILED
            error_code: 58,
            error_message: r#"{"status":"invalid_token"}"#.to_string(),
        },
    ])
    .await;

    let mut config = config(&mock.bootstrap(), &idp, "{}");
    oauth::install(&mut config).expect("install");
    let err = KafkaClient::new(config.source.clone().unwrap())
        .connect()
        .await
        .expect_err("the broker refused the token")
        .to_string();

    assert!(err.contains("invalid_token"), "{err}");
    assert!(err.contains("OAuth provider 'entra'"), "{err}");
    for cause in ["issuer", "audience", "identity pool"] {
        assert!(err.contains(cause), "names {cause}: {err}");
    }
    assert!(!err.contains("refused-access-token"), "{err}");
    mock.shutdown().await;
}

/// An identity provider that refuses the client fails the connection before
/// any SASL payload is sent, with the RFC 6749 error and no secret.
#[tokio::test]
async fn an_identity_provider_refusal_fails_before_authenticating() {
    let idp = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error": "invalid_client",
            "error_description": "client secret mock-secret is not valid",
        })))
        .mount(&idp)
        .await;
    let mock = MockKafkaBroker::start(vec![Exchange::HandshakeSuccess]).await;

    let mut config = config(&mock.bootstrap(), &idp, "{}");
    oauth::install(&mut config).expect("install");
    let err = KafkaClient::new(config.source.clone().unwrap())
        .connect()
        .await
        .expect_err("no token")
        .to_string();

    assert!(err.contains("invalid_client"), "{err}");
    assert!(err.contains("'entra'"), "{err}");
    assert!(!err.contains("mock-secret"), "{err}");
    assert!(mock.captured().await.authenticate_payloads.is_empty());
    mock.shutdown().await;
}
