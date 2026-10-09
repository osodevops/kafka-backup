//! A recording stub of the S3 REST API: just enough PUT / GET / HEAD / DELETE /
//! DeleteObjects / ListObjectsV2 for object_store 0.14 to complete a request,
//! so tests can assert that `--path s3://...` really sends requests to the S3
//! endpoint with the right bucket and prefix. It is not an S3 implementation; S3 semantics
//! are covered by the MinIO round-trip test.
//!
//! It never answers 5xx: object_store retries those for up to ~3 minutes.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

#[derive(Debug, Clone)]
pub struct Req {
    pub method: String,
    /// Percent-decoded path, e.g. `/bucket/prefix/key`.
    pub path: String,
    /// Percent-decoded query string.
    pub query: String,
    /// Signed with SigV4 using the test access key.
    pub sigv4: bool,
}

#[derive(Default)]
struct State {
    /// `bucket/key` -> body
    objects: Mutex<BTreeMap<String, Vec<u8>>>,
    log: Mutex<Vec<Req>>,
    deny_list: AtomicBool,
    /// GET / HEAD of a key ending in one of these answers 403 AccessDenied.
    deny_get: Mutex<Vec<String>>,
}

pub struct StubS3 {
    pub addr: SocketAddr,
    state: Arc<State>,
}

/// Static test credentials, so object_store never falls back to IMDS.
pub const CREDS: [(&str, &str); 2] = [
    ("AWS_ACCESS_KEY_ID", "test"),
    ("AWS_SECRET_ACCESS_KEY", "test"),
];

impl StubS3 {
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(State::default());
        let shared = state.clone();
        thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let state = shared.clone();
                thread::spawn(move || {
                    let _ = handle(stream, &state);
                });
            }
        });
        Self { addr, state }
    }

    /// `s3://{bucket_and_prefix}` pointed at this stub.
    pub fn url(&self, bucket_and_prefix: &str) -> String {
        format!(
            "s3://{bucket_and_prefix}?endpoint=http://{}&region=us-east-1",
            self.addr
        )
    }

    /// Seed objects under `bucket/prefix/`.
    pub fn seed(&self, bucket_and_prefix: &str, objects: &super::Objects) {
        let mut store = self.state.objects.lock().unwrap();
        for (key, bytes) in objects {
            store.insert(format!("{bucket_and_prefix}/{key}"), bytes.clone());
        }
    }

    pub fn keys(&self) -> Vec<String> {
        self.state.objects.lock().unwrap().keys().cloned().collect()
    }

    pub fn requests(&self) -> Vec<Req> {
        self.state.log.lock().unwrap().clone()
    }

    /// `METHOD /path` for every request so far.
    pub fn request_lines(&self) -> Vec<String> {
        self.requests()
            .iter()
            .map(|r| format!("{} {}", r.method, r.path))
            .collect()
    }

    pub fn clear_requests(&self) {
        self.state.log.lock().unwrap().clear();
    }

    /// Answer ListObjectsV2 with 403 AccessDenied.
    pub fn deny_list(&self) {
        self.state.deny_list.store(true, Ordering::SeqCst);
    }

    /// Answer GET / HEAD of any key ending in `suffix` with 403 AccessDenied,
    /// whether or not the object exists (like S3 without s3:GetObject).
    pub fn deny_get(&self, suffix: &str) {
        self.state.deny_get.lock().unwrap().push(suffix.to_string());
    }
}

fn pct_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).ok();
            if let Some(b) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn query_param<'a>(query: &'a str, name: &str) -> Option<&'a str> {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == name)
        .map(|(_, v)| v)
}

const LAST_MODIFIED: &str = "Tue, 06 Oct 2026 00:00:00 GMT";

fn respond(stream: &mut TcpStream, status: &str, headers: &[(&str, String)], body: &[u8]) {
    let mut head = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
    for (k, v) in headers {
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    if !headers.iter().any(|(k, _)| *k == "Content-Length") {
        head.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body);
    let _ = stream.flush();
}

fn error_xml(code: &str) -> Vec<u8> {
    format!("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code><Message>{code}</Message></Error>").into_bytes()
}

fn handle(stream: TcpStream, state: &State) -> std::io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut stream = stream;

    let mut request_line = String::new();
    reader.read_line(&mut request_line)?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    let mut authorization = String::new();
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header)? == 0 {
            break;
        }
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        if let Some((name, value)) = header.split_once(':') {
            match name.trim().to_ascii_lowercase().as_str() {
                "content-length" => content_length = value.trim().parse().unwrap_or(0),
                "authorization" => authorization = value.trim().to_string(),
                _ => {}
            }
        }
    }
    let mut body = vec![0; content_length];
    reader.read_exact(&mut body)?;

    let (raw_path, raw_query) = target.split_once('?').unwrap_or((&target, ""));
    let path = pct_decode(raw_path);
    let query = pct_decode(raw_query);
    state.log.lock().unwrap().push(Req {
        method: method.clone(),
        path: path.clone(),
        query: query.clone(),
        sigv4: authorization.starts_with("AWS4-HMAC-SHA256 Credential=test/"),
    });

    let trimmed = path.trim_start_matches('/');
    let (bucket, key) = trimmed.split_once('/').unwrap_or((trimmed, ""));
    let object_key = format!("{bucket}/{key}");

    match (method.as_str(), key.is_empty()) {
        ("GET", true) if query_param(&query, "list-type") == Some("2") => {
            if state.deny_list.load(Ordering::SeqCst) {
                respond(
                    &mut stream,
                    "403 Forbidden",
                    &[],
                    &error_xml("AccessDenied"),
                );
                return Ok(());
            }
            let prefix = query_param(&query, "prefix").unwrap_or_default();
            let mut contents = String::new();
            for (k, v) in state.objects.lock().unwrap().iter() {
                let Some(k) = k.strip_prefix(&format!("{bucket}/")) else {
                    continue;
                };
                if k.starts_with(prefix) {
                    contents.push_str(&format!(
                        "<Contents><Key>{k}</Key><LastModified>2026-10-06T00:00:00.000Z</LastModified><ETag>\"e\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
                        v.len()
                    ));
                }
            }
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{bucket}</Name><Prefix>{prefix}</Prefix><IsTruncated>false</IsTruncated>{contents}</ListBucketResult>"
            );
            respond(
                &mut stream,
                "200 OK",
                &[("Content-Type", "application/xml".into())],
                xml.as_bytes(),
            );
        }
        ("PUT", false) => {
            let mut objects = state.objects.lock().unwrap();
            objects.insert(object_key, body);
            let etag = format!("\"e{}\"", objects.len());
            respond(&mut stream, "200 OK", &[("ETag", etag)], b"");
        }
        ("GET", false) | ("HEAD", false)
            if state
                .deny_get
                .lock()
                .unwrap()
                .iter()
                .any(|suffix| object_key.ends_with(suffix.as_str())) =>
        {
            let body = if method == "GET" {
                error_xml("AccessDenied")
            } else {
                Vec::new()
            };
            respond(&mut stream, "403 Forbidden", &[], &body);
        }
        ("GET", false) | ("HEAD", false) => {
            let found = state.objects.lock().unwrap().get(&object_key).cloned();
            match found {
                Some(data) => {
                    let headers = [
                        ("Content-Length", data.len().to_string()),
                        ("ETag", "\"e\"".to_string()),
                        ("Last-Modified", LAST_MODIFIED.to_string()),
                    ];
                    let body: &[u8] = if method == "GET" { &data } else { b"" };
                    respond(&mut stream, "200 OK", &headers, body);
                }
                None if method == "HEAD" => {
                    respond(
                        &mut stream,
                        "404 Not Found",
                        &[("Content-Length", "0".into())],
                        b"",
                    );
                }
                None => respond(&mut stream, "404 Not Found", &[], &error_xml("NoSuchKey")),
            }
        }
        ("DELETE", false) => {
            state.objects.lock().unwrap().remove(&object_key);
            respond(&mut stream, "204 No Content", &[], b"");
        }
        // DeleteObjects: object_store 0.14 deletes through this bulk API.
        ("POST", true)
            if query
                .split('&')
                .any(|kv| kv == "delete" || kv.starts_with("delete=")) =>
        {
            let body = String::from_utf8_lossy(&body);
            let mut deleted = String::new();
            let mut objects = state.objects.lock().unwrap();
            for chunk in body.split("<Key>").skip(1) {
                let key = chunk.split("</Key>").next().unwrap_or_default();
                objects.remove(&format!("{bucket}/{key}"));
                deleted.push_str(&format!("<Deleted><Key>{key}</Key></Deleted>"));
            }
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?><DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{deleted}</DeleteResult>"
            );
            respond(
                &mut stream,
                "200 OK",
                &[("Content-Type", "application/xml".into())],
                xml.as_bytes(),
            );
        }
        _ => respond(
            &mut stream,
            "400 Bad Request",
            &[],
            &error_xml("BadRequest"),
        ),
    }
    Ok(())
}
