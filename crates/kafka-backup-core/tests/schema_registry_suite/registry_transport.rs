//! Reads may retry transient transport failures. Writes must reconcile once.
use bytes::Bytes;
use kafka_backup_core::{
    apicurio_registry::client::ApicurioRegistryClient,
    config::{ApicurioRegistryConfig, SchemaRegistryConfig},
    schema_registry::client::SchemaRegistryClient,
};
use reqwest::Method;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use wiremock::{
    matchers::{method, path},
    Mock, MockServer, Request, ResponseTemplate,
};

#[tokio::test]
async fn registry_reads_retry_transient_failures_but_writes_are_never_repeated() {
    for timeout in [false, true] {
        for confluent in [false, true] {
            let server = MockServer::start().await;
            let attempts = Arc::new(AtomicUsize::new(0));
            let read_attempts = attempts.clone();
            let endpoint = if confluent {
                "/v1/metadata/version"
            } else {
                "/apis/registry/v3/system/info"
            };
            Mock::given(method("GET"))
                .and(path(endpoint))
                .respond_with(move |_: &Request| {
                    if read_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        if timeout {
                            ResponseTemplate::new(200)
                                .set_body_json(json!({"version":"3.3.3"}))
                                .set_delay(Duration::from_millis(300))
                        } else {
                            ResponseTemplate::new(503)
                                .set_body_json(json!({"error_code":50301,"message":"unavailable"}))
                        }
                    } else {
                        ResponseTemplate::new(200).set_body_json(json!({"version":"3.3.3"}))
                    }
                })
                .mount(&server)
                .await;
            let response = if timeout {
                ResponseTemplate::new(200)
                    .set_body_json(json!({"id":7}))
                    .set_delay(Duration::from_millis(300))
            } else {
                ResponseTemplate::new(503)
                    .set_body_json(json!({"error_code":50301,"message":"unavailable"}))
            };
            Mock::given(method("POST"))
                .respond_with(response)
                .mount(&server)
                .await;
            let config = json!({"url":server.uri(),"connection":{"timeout_ms":100,"max_retries":3,"retry_backoff_ms":1,"rate_limit_rps":10000}});
            if confluent {
                let config: SchemaRegistryConfig = serde_json::from_value(config).unwrap();
                let client = SchemaRegistryClient::new(&config).unwrap();
                assert_eq!(
                    client.server_version().await.unwrap().as_deref(),
                    Some("3.3.3")
                );
                assert!(client
                    .mutate(
                        Method::POST,
                        "/subjects/t/versions",
                        Some(&json!({"schema":"\"string\""}))
                    )
                    .await
                    .is_err());
            } else {
                let config: ApicurioRegistryConfig = serde_json::from_value(config).unwrap();
                let client = ApicurioRegistryClient::new(&config).unwrap();
                assert_eq!(
                    client.server_version().await.unwrap().as_deref(),
                    Some("3.3.3")
                );
                assert!(client
                    .mutate(
                        Method::POST,
                        "/groups/default/artifacts",
                        "application/json",
                        Bytes::from_static(b"{}"),
                        &[]
                    )
                    .await
                    .is_err());
            }
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
            let requests = server.received_requests().await.unwrap();
            assert_eq!(
                requests.iter().filter(|r| r.method == Method::POST).count(),
                1,
                "An uncertain write was blindly retried"
            );
        }
    }
}

#[tokio::test]
async fn read_connection_closed_before_response_retries_but_mutation_does_not() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    for confluent in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = attempts.clone();
        let server = tokio::spawn(async move {
            for number in 0..3 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0; 8192];
                let read = stream.read(&mut request).await.unwrap();
                assert!(
                    read > 0,
                    "client must send a request before losing its response"
                );
                observed.fetch_add(1, Ordering::SeqCst);
                if number == 1 {
                    let body = r#"{"version":"3.3.3"}"#;
                    let response = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{body}", body.len());
                    stream.write_all(response.as_bytes()).await.unwrap();
                }
                // The first read and the final mutation lose their response.
            }
        });
        let config = json!({"url":format!("http://{address}"),"connection":{"timeout_ms":1000,"max_retries":2,"retry_backoff_ms":1,"rate_limit_rps":10000}});
        if confluent {
            let config: SchemaRegistryConfig = serde_json::from_value(config).unwrap();
            let client = SchemaRegistryClient::new(&config).unwrap();
            assert_eq!(
                client.server_version().await.unwrap().as_deref(),
                Some("3.3.3")
            );
            assert!(client
                .mutate(
                    Method::POST,
                    "/subjects/t/versions",
                    Some(&json!({"schema":"\"string\""}))
                )
                .await
                .is_err());
        } else {
            let config: ApicurioRegistryConfig = serde_json::from_value(config).unwrap();
            let client = ApicurioRegistryClient::new(&config).unwrap();
            assert_eq!(
                client.server_version().await.unwrap().as_deref(),
                Some("3.3.3")
            );
            assert!(client
                .mutate(
                    Method::POST,
                    "/groups/default/artifacts",
                    "application/json",
                    Bytes::from_static(b"{}"),
                    &[]
                )
                .await
                .is_err());
        }
        server.await.unwrap();
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }
}
