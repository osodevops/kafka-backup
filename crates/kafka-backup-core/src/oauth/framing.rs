//! RFC 7628 framing for OAUTHBEARER.
//!
//! §3.1 client initial response (CIR):
//!
//! ```text
//! n,a=<authzid>,<0x01>[kvpair<0x01>]*auth=Bearer <token><0x01><0x01>
//! ```
//!
//! The leading `n,a=<authzid>,` is a GS2 header. Most
//! token-based deployments leave `authzid` empty (the broker authenticates
//! the principal from the token itself), so this encoder emits `n,,` — the
//! canonical "no authorization identity, no channel binding" form.
//!
//! Extensions are sorted deterministically so the CIR is byte-stable across
//! invocations (matters only for tests, but it's cheap insurance).

use std::collections::BTreeMap;

/// Build an RFC 7628 client initial response payload.
///
/// The `token` bytes are copied verbatim after `auth=Bearer `. The provided
/// `extensions` are emitted in sorted order between the GS2 header and the
/// `auth=` field, each terminated by a `0x01` separator.
pub fn build_client_initial_response(
    token: &str,
    extensions: &BTreeMap<String, String>,
) -> Vec<u8> {
    // Conservative pre-allocation; keeps the hot path allocation-free for
    // typical tokens (~1 KB) plus a couple of small extensions.
    let mut buf = Vec::with_capacity(token.len() + 32);
    buf.extend_from_slice(b"n,,"); // GS2 header: no authzid, no channel binding
    buf.push(0x01);
    for (k, v) in extensions {
        buf.extend_from_slice(k.as_bytes());
        buf.push(b'=');
        buf.extend_from_slice(v.as_bytes());
        buf.push(0x01);
    }
    buf.extend_from_slice(b"auth=Bearer ");
    buf.extend_from_slice(token.as_bytes());
    buf.push(0x01);
    buf.push(0x01);
    buf
}

/// RFC 7628 §3.1 key/value separator (also the client's reply to a server
/// error challenge, §3.2.2).
pub const KVSEP: u8 = 0x01;

/// The JSON error a broker sends as a challenge when it rejects a token
/// (RFC 7628 §3.2.2), e.g. `{"status":"invalid_token"}`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct ServerError {
    pub status: String,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default, rename = "openid-configuration")]
    pub openid_configuration: Option<String>,
}

/// Parse a server error challenge. `None` if it is not one.
pub fn parse_server_error(bytes: &[u8]) -> Option<ServerError> {
    serde_json::from_slice(bytes).ok()
}

/// Check one SASL extension against RFC 7628 §3.1: the key is one or more
/// ASCII letters and not `auth`; the value has no control characters other
/// than HTAB, CR and LF (so never the 0x01 separator).
pub fn validate_extension(key: &str, value: &str) -> Result<(), String> {
    if key.is_empty() || !key.bytes().all(|b| b.is_ascii_alphabetic()) {
        return Err(format!(
            "SASL extension name `{key}` must be ASCII letters only (RFC 7628)"
        ));
    }
    if key == "auth" {
        return Err("`auth` is reserved and cannot be a SASL extension".into());
    }
    if let Some(bad) = value
        .chars()
        .find(|c| (c.is_control() && !matches!(c, '\t' | '\r' | '\n')) || !c.is_ascii())
    {
        return Err(format!(
            "SASL extension `{key}` has an invalid character {bad:?} (printable ASCII only)"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cir_with_one_extension() {
        let mut ext = BTreeMap::new();
        ext.insert("logicalCluster".to_string(), "lkc-abc123".to_string());
        let payload = build_client_initial_response("tok", &ext);
        assert_eq!(
            payload,
            b"n,,\x01logicalCluster=lkc-abc123\x01auth=Bearer tok\x01\x01"
        );
    }

    #[test]
    fn cir_with_confluent_extensions_and_pool_list() {
        let mut ext = BTreeMap::new();
        ext.insert("logicalCluster".to_string(), "lkc-1".to_string());
        ext.insert("identityPoolId".to_string(), "pool-a,pool-b".to_string());
        let payload = build_client_initial_response("tok", &ext);
        assert_eq!(
            payload,
            b"n,,\x01identityPoolId=pool-a,pool-b\x01logicalCluster=lkc-1\x01auth=Bearer tok\x01\x01"
        );
    }

    #[test]
    fn server_error_parsing() {
        let e = parse_server_error(br#"{"status":"invalid_token","scope":"kafka"}"#).unwrap();
        assert_eq!(e.status, "invalid_token");
        assert_eq!(e.scope.as_deref(), Some("kafka"));
        assert!(parse_server_error(b"").is_none());
        assert!(parse_server_error(b"not json").is_none());
        assert!(parse_server_error(br#"{"scope":"x"}"#).is_none());
    }

    #[test]
    fn extension_validation() {
        assert!(validate_extension("logicalCluster", "lkc-1").is_ok());
        assert!(validate_extension("identityPoolId", "pool-a,pool-b").is_ok());
        assert!(validate_extension("auth", "x").is_err());
        assert!(validate_extension("logical_cluster", "x").is_err());
        assert!(validate_extension("", "x").is_err());
        assert!(validate_extension("ok", "a\u{1}b").is_err());
        assert!(validate_extension("ok", "caf\u{e9}").is_err());
    }

    fn as_str(bytes: &[u8]) -> String {
        String::from_utf8(bytes.to_vec()).expect("utf8 payload")
    }

    #[test]
    fn cir_no_extensions_matches_rfc_example() {
        // RFC 7628 §3.1 worked example (without extensions):
        //   n,,<0x01>auth=Bearer vF9dft4qmT<0x01><0x01>
        let payload = build_client_initial_response("vF9dft4qmT", &BTreeMap::new());
        assert_eq!(
            payload,
            b"n,,\x01auth=Bearer vF9dft4qmT\x01\x01",
            "got: {}",
            as_str(&payload).replace('\x01', "<0x01>")
        );
    }

    #[test]
    fn cir_extensions_are_sorted_deterministically() {
        let mut ext = BTreeMap::new();
        ext.insert("zulu".to_string(), "last".to_string());
        ext.insert("alpha".to_string(), "first".to_string());
        ext.insert("mike".to_string(), "middle".to_string());

        let payload = build_client_initial_response("tok", &ext);
        let rendered = as_str(&payload);
        let alpha = rendered.find("alpha=first").unwrap();
        let mike = rendered.find("mike=middle").unwrap();
        let zulu = rendered.find("zulu=last").unwrap();
        assert!(
            alpha < mike && mike < zulu,
            "extensions not sorted: {rendered:?}"
        );
    }

    #[test]
    fn cir_ends_with_double_separator() {
        let payload = build_client_initial_response("t", &BTreeMap::new());
        let len = payload.len();
        assert_eq!(&payload[len - 2..], &[0x01, 0x01]);
    }

    #[test]
    fn cir_preserves_token_bytes_exactly() {
        let token = "eyJhbGciOiJub25lIn0.eyJzdWIiOiJ0ZXN0In0.";
        let payload = build_client_initial_response(token, &BTreeMap::new());
        let rendered = as_str(&payload);
        assert!(rendered.contains(&format!("auth=Bearer {token}")));
    }

    #[test]
    fn cir_user_agent_extension() {
        let mut ext = BTreeMap::new();
        ext.insert("User-Agent".to_string(), "kafka-backup/0.2".to_string());
        let payload = build_client_initial_response("tok", &ext);
        let rendered = as_str(&payload);
        assert!(rendered.contains("User-Agent=kafka-backup/0.2\x01auth=Bearer"));
    }
}
