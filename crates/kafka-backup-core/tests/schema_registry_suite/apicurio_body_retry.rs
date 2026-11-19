//! Transport failures after successful headers must retry reads, never writes.
use bytes::Bytes;
use kafka_backup_core::{
    apicurio_registry::client::ApicurioRegistryClient,
    config::{ApicurioConnectionConfig, ApicurioRegistryConfig},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn oracle(
    failures: usize,
    body: &'static str,
) -> (
    ApicurioRegistryClient,
    Arc<AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let requests = count.clone();
    let task = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = vec![0; 4096];
            let _ = stream.read(&mut request).await.unwrap();
            let attempt = requests.fetch_add(1, Ordering::SeqCst);
            let data = if attempt < failures { "{" } else { body };
            let length = if attempt < failures { 1000 } else { data.len() };
            stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n{data}").as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        }
    });
    let config = ApicurioRegistryConfig {
        enabled: true,
        url: format!("http://{address}"),
        api_version: Default::default(),
        auth: None,
        tls: None,
        backup: None,
        connection: Some(ApicurioConnectionConfig {
            timeout_ms: 500,
            max_retries: 2,
            retry_backoff_ms: 1,
            rate_limit_rps: 1000,
        }),
    };
    (ApicurioRegistryClient::new(&config).unwrap(), count, task)
}

#[tokio::test]
async fn interrupted_json_read_retries_and_decodes_complete_response() {
    let (client, count, server) = oracle(1, r#"{"version":"3.3.3"}"#).await;
    assert_eq!(
        client.server_version().await.unwrap().as_deref(),
        Some("3.3.3")
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
    server.abort();
}
#[tokio::test]
async fn interrupted_content_read_retries_with_bounded_failure() {
    let (client, count, server) = oracle(1, "schema-content").await;
    assert_eq!(
        client.get_content_by_id(1).await.unwrap(),
        Bytes::from_static(b"schema-content")
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
    server.abort();
    let (client, count, server) = oracle(usize::MAX, "unused").await;
    assert!(client.get_content_by_id(1).await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 3);
    server.abort();
}
#[tokio::test]
async fn malformed_complete_json_is_not_retried() {
    let (client, count, server) = oracle(0, "not-json").await;
    assert!(client.server_version().await.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    server.abort();
}
#[tokio::test]
async fn committed_mutation_with_interrupted_response_is_never_replayed() {
    let (client, count, server) = oracle(1, "{}").await;
    assert!(client
        .mutate(
            reqwest::Method::POST,
            "/admin/import",
            "application/zip",
            Bytes::from_static(b"archive"),
            &[]
        )
        .await
        .is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    server.abort();
}
