//! Unverified reads of JWT claims.
//!
//! Tokens are opaque to Kafka Backup: the broker (and Confluent Cloud)
//! validate them. The only claim we ever look at is `exp`, and only when the
//! provider did not say when the token expires (file and static providers,
//! token responses without `expires_in`), so the cache knows when to
//! refresh. The signature is **not** checked; nothing here is a security
//! decision.

use std::time::{Duration, SystemTime};

use base64::Engine;

/// The `exp` claim of a JWT-shaped token, unverified. `None` when the token
/// is not a JWT, has no numeric `exp`, or cannot be decoded.
pub fn unverified_exp(token: &str) -> Option<SystemTime> {
    let claims = unverified_claims(token)?;
    let exp = match claims.get("exp")? {
        serde_json::Value::Number(n) => n.as_f64()?,
        serde_json::Value::String(s) => s.trim().parse::<f64>().ok()?,
        _ => return None,
    };
    if !exp.is_finite() || exp <= 0.0 {
        return None;
    }
    Some(SystemTime::UNIX_EPOCH + Duration::from_secs_f64(exp))
}

fn unverified_claims(token: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut parts = token.trim().split('.');
    let (_header, payload) = (parts.next()?, parts.next()?);
    // A JWS has three parts; a JWE (five) has an encrypted payload.
    if parts.count() != 1 {
        return None;
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    match serde_json::from_slice(&bytes).ok()? {
        serde_json::Value::Object(map) => Some(map),
        _ => None,
    }
}

/// Build an unsigned JWT with the given claims (tests only).
#[cfg(test)]
pub(crate) fn unsigned_jwt(claims: serde_json::Value) -> String {
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    format!(
        "{}.{}.sig",
        b64.encode(br#"{"alg":"none","typ":"JWT"}"#),
        b64.encode(claims.to_string())
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_numeric_and_string_exp() {
        let t = unsigned_jwt(serde_json::json!({"sub": "a", "exp": 1_900_000_000}));
        assert_eq!(
            unverified_exp(&t),
            Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1_900_000_000))
        );
        let t = unsigned_jwt(serde_json::json!({"exp": "1900000000"}));
        assert!(unverified_exp(&t).is_some());
    }

    #[test]
    fn non_jwts_have_no_exp() {
        assert_eq!(unverified_exp("opaque-token"), None);
        assert_eq!(unverified_exp("a.b"), None);
        assert_eq!(unverified_exp("a.!!!.c"), None);
        assert_eq!(unverified_exp("a.b.c.d.e"), None);
        let t = unsigned_jwt(serde_json::json!({"sub": "no-exp"}));
        assert_eq!(unverified_exp(&t), None);
        let t = unsigned_jwt(serde_json::json!({"exp": true}));
        assert_eq!(unverified_exp(&t), None);
    }
}
