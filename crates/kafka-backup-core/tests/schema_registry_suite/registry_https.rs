//! Real TLS handshakes for the shared registry transports, not compatibility evidence.
use kafka_backup_core::{
    apicurio_registry::client::ApicurioRegistryClient,
    config::{ApicurioRegistryConfig, SchemaRegistryConfig},
    schema_registry::client::SchemaRegistryClient,
};
use serde_json::json;
use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, Command, Stdio},
};

struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn openssl(dir: &Path, args: &[&str]) {
    let result = Command::new("openssl")
        .current_dir(dir)
        .args(args)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
}

#[test]
fn incomplete_mutual_tls_identity_is_a_configuration_error() {
    for (cert, key) in [
        (None, None),
        (Some("cert.pem"), None),
        (None, Some("key.pem")),
    ] {
        let value = json!({"url":"https://localhost", "auth":{"type":"mtls","client_cert":cert,"client_key":key}});
        // Confluent: both files are required keys, so the configuration
        // does not parse. Apicurio: the client refuses to be built.
        assert!(
            serde_json::from_value::<SchemaRegistryConfig>(value.clone()).is_err(),
            "Confluent silently omitted the selected client identity"
        );
        let ap: ApicurioRegistryConfig = serde_json::from_value(value).unwrap();
        assert!(
            ApicurioRegistryClient::new(&ap).is_err(),
            "Apicurio silently omitted the selected client identity"
        );
    }
}

#[tokio::test]
async fn both_registry_clients_authenticate_over_mutual_tls_and_reject_untrusted_peers() {
    let temp = tempfile::tempdir().unwrap();
    let dir = temp.path();
    openssl(
        dir,
        &[
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            "ca.key",
            "-out",
            "ca.pem",
            "-days",
            "1",
            "-subj",
            "/CN=registry-test-ca",
        ],
    );
    for (name, purpose) in [("server", "serverAuth"), ("client", "clientAuth")] {
        openssl(
            dir,
            &[
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                &format!("{name}.key"),
                "-out",
                &format!("{name}.csr"),
                "-subj",
                &format!("/CN={name}"),
            ],
        );
        std::fs::write(dir.join(format!("{name}.ext")), format!("basicConstraints=critical,CA:FALSE\nextendedKeyUsage={purpose}\nsubjectAltName=DNS:localhost\n")).unwrap();
        openssl(
            dir,
            &[
                "x509",
                "-req",
                "-in",
                &format!("{name}.csr"),
                "-CA",
                "ca.pem",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                &format!("{name}.pem"),
                "-days",
                "1",
                "-extfile",
                &format!("{name}.ext"),
            ],
        );
    }
    let script = r#"
import http.server,json,ssl
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body=json.dumps({'version':'3.3.3'}).encode()
        self.send_response(200);self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def log_message(self,*args): pass
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain('server.pem','server.key')
ctx.load_verify_locations('ca.pem');ctx.verify_mode=ssl.CERT_REQUIRED
server.socket=ctx.wrap_socket(server.socket,server_side=True)
print(server.server_port,flush=True);server.serve_forever()
"#;
    let mut child = Command::new("python3")
        .args(["-u", "-c", script])
        .current_dir(dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let _server = Server(child);
    let url = format!("https://localhost:{}", line.trim().parse::<u16>().unwrap());
    let value = json!({"url":url,"connection":{"max_retries":0,"timeout_ms":3000},"auth":{"type":"mtls","ca_cert":dir.join("ca.pem"),"client_cert":dir.join("client.pem"),"client_key":dir.join("client.key")}});
    let sr: SchemaRegistryConfig = serde_json::from_value(value.clone()).unwrap();
    let ap: ApicurioRegistryConfig = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(
        SchemaRegistryClient::new(&sr)
            .unwrap()
            .server_version()
            .await
            .unwrap()
            .as_deref(),
        Some("3.3.3")
    );
    assert_eq!(
        ApicurioRegistryClient::new(&ap)
            .unwrap()
            .server_version()
            .await
            .unwrap()
            .as_deref(),
        Some("3.3.3")
    );
    let mut untrusted = value.clone();
    untrusted["auth"].as_object_mut().unwrap().remove("ca_cert");
    let sr: SchemaRegistryConfig = serde_json::from_value(untrusted.clone()).unwrap();
    let ap: ApicurioRegistryConfig = serde_json::from_value(untrusted).unwrap();
    assert!(SchemaRegistryClient::new(&sr)
        .unwrap()
        .server_version()
        .await
        .is_err());
    assert!(ApicurioRegistryClient::new(&ap)
        .unwrap()
        .server_version()
        .await
        .is_err());
    let anonymous = json!({"url":url,"tls":{"ca_cert":dir.join("ca.pem")},"connection":{"max_retries":0,"timeout_ms":3000}});
    let sr: SchemaRegistryConfig = serde_json::from_value(anonymous.clone()).unwrap();
    let ap: ApicurioRegistryConfig = serde_json::from_value(anonymous).unwrap();
    assert!(SchemaRegistryClient::new(&sr)
        .unwrap()
        .server_version()
        .await
        .is_err());
    assert!(ApicurioRegistryClient::new(&ap)
        .unwrap()
        .server_version()
        .await
        .is_err());
}
