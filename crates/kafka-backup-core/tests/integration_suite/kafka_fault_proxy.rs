//! A Kafka-aware TCP proxy for fault injection against a real broker
//! (toxiproxy-style, in process).
//!
//! Every Metadata response is rewritten so the brokers' advertised
//! addresses point at the proxy, so all of a client's connections pass
//! through it. Chosen Produce responses can be dropped: the broker has
//! appended the batch, the proxy closes the client connection instead of
//! relaying the answer, and the client sees a lost connection — exactly the
//! case a retry would duplicate without an idempotent producer.
#![allow(dead_code)]

use std::collections::{HashSet, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{ApiKey, MetadataResponse, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

#[derive(Default)]
struct Control {
    /// Produce responses seen so far (across all connections).
    produce_responses: AtomicUsize,
    /// Indexes (0-based) of the Produce responses to drop.
    drop_produce_responses: Mutex<HashSet<usize>>,
    /// Produce responses dropped.
    dropped: AtomicUsize,
}

pub struct FaultProxy {
    pub addr: SocketAddr,
    control: Arc<Control>,
    handle: JoinHandle<()>,
}

impl FaultProxy {
    /// Proxy to the broker at `upstream` (`host:port`).
    pub async fn start(upstream: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().expect("proxy addr");
        let control = Arc::new(Control::default());
        let upstream = upstream.to_string();
        let c = control.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    return;
                };
                let upstream = upstream.clone();
                let control = c.clone();
                tokio::spawn(async move {
                    if let Ok(server) = TcpStream::connect(&upstream).await {
                        proxy_connection(client, server, addr, control).await;
                    }
                });
            }
        });
        Self {
            addr,
            control,
            handle,
        }
    }

    pub fn bootstrap(&self) -> String {
        self.addr.to_string()
    }

    /// Drop the responses of these Produce requests (0-based, in the order
    /// their responses arrive from the broker).
    pub fn drop_produce_responses(&self, indexes: impl IntoIterator<Item = usize>) {
        self.control
            .drop_produce_responses
            .lock()
            .unwrap()
            .extend(indexes);
    }

    /// Produce responses relayed or dropped so far.
    pub fn produce_responses(&self) -> usize {
        self.control.produce_responses.load(Ordering::SeqCst)
    }

    /// Produce responses dropped so far.
    pub fn dropped(&self) -> usize {
        self.control.dropped.load(Ordering::SeqCst)
    }
}

impl Drop for FaultProxy {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn read_frame(stream: &mut OwnedReadHalf) -> Option<Bytes> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await.ok()?;
    let mut frame = vec![0u8; i32::from_be_bytes(len) as usize];
    stream.read_exact(&mut frame).await.ok()?;
    Some(Bytes::from(frame))
}

async fn write_frame(stream: &mut OwnedWriteHalf, frame: &[u8]) -> bool {
    let mut buf = BytesMut::with_capacity(frame.len() + 4);
    buf.put_i32(frame.len() as i32);
    buf.extend_from_slice(frame);
    stream.write_all(&buf).await.is_ok()
}

async fn proxy_connection(
    client: TcpStream,
    server: TcpStream,
    proxy_addr: SocketAddr,
    control: Arc<Control>,
) {
    let (mut client_rx, client_tx) = client.into_split();
    let (mut server_rx, mut server_tx) = server.into_split();
    // (api key, version) of the requests awaiting a response, in order.
    let pending: Arc<Mutex<VecDeque<(i16, i16)>>> = Arc::default();
    let client_tx = Arc::new(tokio::sync::Mutex::new(Some(client_tx)));

    let p = pending.clone();
    let requests = tokio::spawn(async move {
        while let Some(frame) = read_frame(&mut client_rx).await {
            if frame.len() < 8 {
                return;
            }
            let api_key = i16::from_be_bytes([frame[0], frame[1]]);
            let version = i16::from_be_bytes([frame[2], frame[3]]);
            p.lock().unwrap().push_back((api_key, version));
            if !write_frame(&mut server_tx, &frame).await {
                return;
            }
        }
    });

    while let Some(frame) = read_frame(&mut server_rx).await {
        let Some((api_key, version)) = pending.lock().unwrap().pop_front() else {
            break;
        };
        let frame = match ApiKey::try_from(api_key) {
            Ok(ApiKey::Produce) => {
                let index = control.produce_responses.fetch_add(1, Ordering::SeqCst);
                if control
                    .drop_produce_responses
                    .lock()
                    .unwrap()
                    .remove(&index)
                {
                    control.dropped.fetch_add(1, Ordering::SeqCst);
                    // The broker appended the batch; the client never hears.
                    break;
                }
                frame
            }
            Ok(ApiKey::Metadata) => rewrite_metadata(frame, version, proxy_addr),
            _ => frame,
        };
        let mut guard = client_tx.lock().await;
        let Some(tx) = guard.as_mut() else { break };
        if !write_frame(tx, &frame).await {
            break;
        }
    }
    requests.abort();
    // Dropping both halves closes the client and the upstream connection.
    client_tx.lock().await.take();
}

/// Point every advertised broker at the proxy.
fn rewrite_metadata(frame: Bytes, version: i16, proxy: SocketAddr) -> Bytes {
    let header_version = ApiKey::Metadata.response_header_version(version);
    let mut buf = frame.clone();
    let Ok(header) = ResponseHeader::decode(&mut buf, header_version) else {
        return frame;
    };
    let Ok(mut response) = MetadataResponse::decode(&mut buf, version) else {
        return frame;
    };
    for broker in response.brokers.iter_mut() {
        broker.host = StrBytes::from_string(proxy.ip().to_string());
        broker.port = i32::from(proxy.port());
    }
    let mut out = BytesMut::new();
    header
        .encode(&mut out, header_version)
        .expect("encode header");
    response.encode(&mut out, version).expect("encode metadata");
    out.freeze()
}
