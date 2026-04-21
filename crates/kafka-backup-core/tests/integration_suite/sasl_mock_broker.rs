//! Minimal in-process Kafka-wire mock for SASL dispatch tests.
//!
//! Speaks just enough of the Kafka protocol for a client to drive
//! `SaslHandshake` + `SaslAuthenticate` rounds through it. Uses the
//! existing `kafka-protocol` dependency for frame encoding/decoding —
//! no new dev-deps.
//!
//! The mock accepts exactly one connection and processes a scripted
//! sequence of [`Exchange`]s against incoming requests. After the
//! script is exhausted it drops the socket, which the client observes
//! as a broken pipe on its next RPC — fine, because the tests
//! orchestrate what they expect.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use kafka_protocol::messages::{
    ApiKey, RequestHeader, ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse,
};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

/// Scripted exchange: how the mock answers the next incoming request.
#[derive(Debug, Clone)]
pub enum Exchange {
    /// Respond with `error_code = 0` to a `SaslHandshake`.
    HandshakeSuccess,
    /// Respond with `SaslAuthenticate` success carrying `auth_bytes`
    /// (bytes the plugin's `continue_payload` will see) and
    /// `session_lifetime_ms` (what the re-auth scheduler uses).
    AuthenticateSuccess {
        auth_bytes: Vec<u8>,
        session_lifetime_ms: i64,
    },
    /// Respond to a `SaslAuthenticate` with a broker-side error.
    AuthenticateError {
        error_code: i16,
        error_message: String,
    },
}

/// Per-connection record of what the client sent. Lets tests assert on
/// the request bodies — especially useful for verifying multi-round
/// dispatch forwards `continue_payload`'s return bytes correctly.
#[derive(Debug, Default, Clone)]
pub struct CapturedRequests {
    pub handshake_mechanism: Option<String>,
    pub authenticate_payloads: Vec<Vec<u8>>,
}

pub struct MockKafkaBroker {
    addr: SocketAddr,
    captured: Arc<Mutex<CapturedRequests>>,
    handle: JoinHandle<()>,
}

impl MockKafkaBroker {
    pub async fn start(script: Vec<Exchange>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock broker");
        let addr = listener.local_addr().expect("mock broker local_addr");
        let captured = Arc::new(Mutex::new(CapturedRequests::default()));
        let captured_clone = captured.clone();
        let handle = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                serve_connection(stream, script, captured_clone).await;
            }
        });
        Self {
            addr,
            captured,
            handle,
        }
    }

    pub fn bootstrap(&self) -> String {
        self.addr.to_string()
    }

    pub async fn captured(&self) -> CapturedRequests {
        self.captured.lock().await.clone()
    }

    pub async fn shutdown(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

async fn serve_connection(
    mut stream: TcpStream,
    script: Vec<Exchange>,
    captured: Arc<Mutex<CapturedRequests>>,
) {
    for exchange in script {
        let Some((api_key, api_version, correlation_id, body)) = read_request(&mut stream).await
        else {
            return;
        };

        match (exchange, api_key) {
            (Exchange::HandshakeSuccess, ApiKey::SaslHandshake) => {
                let mechanism = decode_handshake_body(&body, api_version);
                captured.lock().await.handshake_mechanism = Some(mechanism);
                let resp = SaslHandshakeResponse::default()
                    .with_error_code(0)
                    .with_mechanisms(vec![]);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            (
                Exchange::AuthenticateSuccess {
                    auth_bytes,
                    session_lifetime_ms,
                },
                ApiKey::SaslAuthenticate,
            ) => {
                let payload = decode_authenticate_body(&body, api_version);
                captured.lock().await.authenticate_payloads.push(payload);
                let resp = SaslAuthenticateResponse::default()
                    .with_error_code(0)
                    .with_auth_bytes(Bytes::from(auth_bytes))
                    .with_session_lifetime_ms(session_lifetime_ms);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            (
                Exchange::AuthenticateError {
                    error_code,
                    error_message,
                },
                ApiKey::SaslAuthenticate,
            ) => {
                let payload = decode_authenticate_body(&body, api_version);
                captured.lock().await.authenticate_payloads.push(payload);
                let resp = SaslAuthenticateResponse::default()
                    .with_error_code(error_code)
                    .with_error_message(Some(StrBytes::from_string(error_message)));
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            (exchange, got) => {
                panic!(
                    "mock broker: scripted {:?} but client sent api_key {:?}",
                    exchange, got
                );
            }
        }
    }
}

async fn read_request(stream: &mut TcpStream) -> Option<(ApiKey, i16, i32, Bytes)> {
    let mut len_buf = [0u8; 4];
    if stream.read_exact(&mut len_buf).await.is_err() {
        return None;
    }
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut frame = vec![0u8; len];
    if stream.read_exact(&mut frame).await.is_err() {
        return None;
    }

    // Peek api_key + version to choose the correct header version.
    if frame.len() < 4 {
        return None;
    }
    let api_key_raw = i16::from_be_bytes([frame[0], frame[1]]);
    let api_version = i16::from_be_bytes([frame[2], frame[3]]);
    let api_key = ApiKey::try_from(api_key_raw).expect("known api_key");
    let header_version = api_key.request_header_version(api_version);

    let mut frame_bytes = Bytes::from(frame);
    let header = RequestHeader::decode(&mut frame_bytes, header_version).expect("decode header");
    Some((api_key, api_version, header.correlation_id, frame_bytes))
}

fn decode_handshake_body(body: &Bytes, api_version: i16) -> String {
    let mut cursor = body.clone();
    let req = SaslHandshakeRequest::decode(&mut cursor, api_version).expect("decode handshake");
    req.mechanism.to_string()
}

fn decode_authenticate_body(body: &Bytes, api_version: i16) -> Vec<u8> {
    let mut cursor = body.clone();
    let req =
        SaslAuthenticateRequest::decode(&mut cursor, api_version).expect("decode authenticate");
    req.auth_bytes.to_vec()
}

async fn write_response<Resp: Encodable>(
    stream: &mut TcpStream,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    response: &Resp,
) {
    let header_version = api_key.response_header_version(api_version);
    let header = ResponseHeader::default().with_correlation_id(correlation_id);

    let mut buf = BytesMut::new();
    buf.put_i32(0);
    header
        .encode(&mut buf, header_version)
        .expect("encode response header");
    response
        .encode(&mut buf, api_version)
        .expect("encode response body");
    let len = (buf.len() - 4) as i32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());

    stream
        .write_all(&buf)
        .await
        .expect("write response to mock client");
}
