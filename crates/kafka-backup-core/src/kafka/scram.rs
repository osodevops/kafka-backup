//! SCRAM-SHA-256/512 SASL authentication (RFC 5802).
//!
//! Implements the client side of the SCRAM protocol for Kafka SASL
//! authentication. Both SHA-256 and SHA-512 variants are supported.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use ring::rand::SecureRandom;
use ring::{digest, hmac, pbkdf2, rand};
use std::num::NonZeroU32;

use crate::config::SaslMechanism;

/// Algorithm-specific constants for SCRAM variants.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ScramAlgorithm {
    Sha256,
    Sha512,
}

impl ScramAlgorithm {
    /// The Kafka SASL mechanism name string.
    pub fn mechanism_name(&self) -> &'static str {
        match self {
            ScramAlgorithm::Sha256 => "SCRAM-SHA-256",
            ScramAlgorithm::Sha512 => "SCRAM-SHA-512",
        }
    }

    /// The ring PBKDF2 algorithm for key derivation.
    fn pbkdf2_algorithm(&self) -> pbkdf2::Algorithm {
        match self {
            ScramAlgorithm::Sha256 => pbkdf2::PBKDF2_HMAC_SHA256,
            ScramAlgorithm::Sha512 => pbkdf2::PBKDF2_HMAC_SHA512,
        }
    }

    /// The ring HMAC algorithm for signing.
    fn hmac_algorithm(&self) -> hmac::Algorithm {
        match self {
            ScramAlgorithm::Sha256 => hmac::HMAC_SHA256,
            ScramAlgorithm::Sha512 => hmac::HMAC_SHA512,
        }
    }

    /// The ring digest algorithm for hashing.
    fn digest_algorithm(&self) -> &'static digest::Algorithm {
        match self {
            ScramAlgorithm::Sha256 => &digest::SHA256,
            ScramAlgorithm::Sha512 => &digest::SHA512,
        }
    }

    /// Output length of the digest in bytes.
    fn digest_len(&self) -> usize {
        match self {
            ScramAlgorithm::Sha256 => digest::SHA256_OUTPUT_LEN,
            ScramAlgorithm::Sha512 => digest::SHA512_OUTPUT_LEN,
        }
    }

    /// Convert from SaslMechanism config enum.
    pub fn from_mechanism(mechanism: SaslMechanism) -> Option<Self> {
        match mechanism {
            SaslMechanism::ScramSha256 => Some(ScramAlgorithm::Sha256),
            SaslMechanism::ScramSha512 => Some(ScramAlgorithm::Sha512),
            _ => None,
        }
    }
}

/// Parse server-first message: `r=<nonce>,s=<salt>,i=<iterations>`
fn parse_server_first(msg: &str) -> Result<(String, Vec<u8>, NonZeroU32), crate::Error> {
    let mut nonce = None;
    let mut salt_b64 = None;
    let mut iterations = None;

    for part in msg.split(',') {
        if let Some(value) = part.strip_prefix("r=") {
            nonce = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("s=") {
            salt_b64 = Some(value.to_string());
        } else if let Some(value) = part.strip_prefix("i=") {
            iterations = Some(value.parse::<u32>().map_err(|e| {
                crate::Error::Authentication(format!("Invalid iteration count '{}': {}", value, e))
            })?);
        }
    }

    let nonce = nonce.ok_or_else(|| {
        crate::Error::Authentication("Missing nonce (r=) in server-first message".to_string())
    })?;

    let salt = BASE64
        .decode(
            salt_b64.ok_or_else(|| {
                crate::Error::Authentication(
                    "Missing salt (s=) in server-first message".to_string(),
                )
            })?,
        )
        .map_err(|e| crate::Error::Authentication(format!("Invalid base64 salt: {}", e)))?;

    let iter_count = iterations.ok_or_else(|| {
        crate::Error::Authentication(
            "Missing iterations (i=) in server-first message".to_string(),
        )
    })?;

    let iter_count = NonZeroU32::new(iter_count).ok_or_else(|| {
        crate::Error::Authentication("Iteration count must be > 0".to_string())
    })?;

    Ok((nonce, salt, iter_count))
}

/// Generate a cryptographically random nonce (base64-encoded).
fn generate_nonce() -> Result<String, crate::Error> {
    let rng = rand::SystemRandom::new();
    let mut nonce_bytes = [0u8; 24];
    rng.fill(&mut nonce_bytes).map_err(|_| {
        crate::Error::Authentication("Failed to generate random nonce".to_string())
    })?;
    Ok(BASE64.encode(nonce_bytes))
}

/// Derive the SaltedPassword using PBKDF2.
fn derive_salted_password(
    algorithm: &ScramAlgorithm,
    password: &str,
    salt: &[u8],
    iterations: NonZeroU32,
) -> Vec<u8> {
    let mut salted_password = vec![0u8; algorithm.digest_len()];
    pbkdf2::derive(
        algorithm.pbkdf2_algorithm(),
        iterations,
        salt,
        password.as_bytes(),
        &mut salted_password,
    );
    salted_password
}

/// Compute the client proof (ClientKey XOR ClientSignature).
fn compute_client_proof(
    algorithm: &ScramAlgorithm,
    password: &str,
    salt: &[u8],
    iterations: NonZeroU32,
    auth_message: &[u8],
) -> Vec<u8> {
    let salted_password = derive_salted_password(algorithm, password, salt, iterations);

    // ClientKey = HMAC(SaltedPassword, "Client Key")
    let salted_key = hmac::Key::new(algorithm.hmac_algorithm(), &salted_password);
    let client_key = hmac::sign(&salted_key, b"Client Key");

    // StoredKey = Hash(ClientKey)
    let stored_key = digest::digest(algorithm.digest_algorithm(), client_key.as_ref());

    // ClientSignature = HMAC(StoredKey, AuthMessage)
    let stored_key_hmac = hmac::Key::new(algorithm.hmac_algorithm(), stored_key.as_ref());
    let client_signature = hmac::sign(&stored_key_hmac, auth_message);

    // ClientProof = ClientKey XOR ClientSignature
    client_key
        .as_ref()
        .iter()
        .zip(client_signature.as_ref().iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

/// Compute the ServerKey HMAC key for server signature verification.
fn compute_server_key(
    algorithm: &ScramAlgorithm,
    password: &str,
    salt: &[u8],
    iterations: NonZeroU32,
) -> hmac::Key {
    let salted_password = derive_salted_password(algorithm, password, salt, iterations);
    let salted_key = hmac::Key::new(algorithm.hmac_algorithm(), &salted_password);
    let server_key = hmac::sign(&salted_key, b"Server Key");
    hmac::Key::new(algorithm.hmac_algorithm(), server_key.as_ref())
}

/// SCRAM client state machine.
///
/// Usage:
/// 1. `ScramClient::new(algorithm, username, password)`
/// 2. `client_first_message()` → send to server
/// 3. `process_server_first(response)` → send result to server
/// 4. `process_server_final(response)` → verify success
pub(crate) struct ScramClient {
    algorithm: ScramAlgorithm,
    password: String,
    client_nonce: String,
    /// `n=<username>,r=<client-nonce>` — needed for AuthMessage
    client_first_bare: String,
    /// Stored after processing server-first for server-final verification
    server_key: Option<hmac::Key>,
    auth_message: Option<String>,
}

impl ScramClient {
    /// Create a new SCRAM client for the given algorithm and credentials.
    pub fn new(
        algorithm: ScramAlgorithm,
        username: &str,
        password: &str,
    ) -> Result<Self, crate::Error> {
        let client_nonce = generate_nonce()?;
        // RFC 5802 SaslName: '=' → '=3D', ',' → '=2C'
        let safe_username = username.replace('=', "=3D").replace(',', "=2C");
        let client_first_bare = format!("n={},r={}", safe_username, client_nonce);

        Ok(Self {
            algorithm,
            password: password.to_string(),
            client_nonce,
            client_first_bare,
            server_key: None,
            auth_message: None,
        })
    }

    /// Produce the client-first message bytes.
    /// Format: `n,,n=<username>,r=<client-nonce>`
    pub fn client_first_message(&self) -> Vec<u8> {
        format!("n,,{}", self.client_first_bare).into_bytes()
    }

    /// Process the server-first message and produce the client-final message.
    /// Server-first format: `r=<server-nonce>,s=<salt-base64>,i=<iterations>`
    pub fn process_server_first(
        &mut self,
        server_first_bytes: &[u8],
    ) -> Result<Vec<u8>, crate::Error> {
        let server_first = std::str::from_utf8(server_first_bytes).map_err(|e| {
            crate::Error::Authentication(format!(
                "Server-first message is not valid UTF-8: {}",
                e
            ))
        })?;

        let (server_nonce, salt, iterations) = parse_server_first(server_first)?;

        // Verify server nonce starts with our client nonce
        if !server_nonce.starts_with(&self.client_nonce) {
            return Err(crate::Error::Authentication(
                "Server nonce does not start with client nonce".to_string(),
            ));
        }

        // "biws" is base64("n,,") — no channel binding
        let client_final_without_proof = format!("c=biws,r={}", server_nonce);

        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, server_first, client_final_without_proof
        );

        let client_proof = compute_client_proof(
            &self.algorithm,
            &self.password,
            &salt,
            iterations,
            auth_message.as_bytes(),
        );

        // Store state for server-final verification
        self.server_key = Some(compute_server_key(
            &self.algorithm,
            &self.password,
            &salt,
            iterations,
        ));
        self.auth_message = Some(auth_message);

        let proof_b64 = BASE64.encode(&client_proof);
        let client_final = format!("{},p={}", client_final_without_proof, proof_b64);

        Ok(client_final.into_bytes())
    }

    /// Verify the server-final message.
    /// Server-final format: `v=<server-signature-base64>` or `e=<error>`
    pub fn process_server_final(
        &self,
        server_final_bytes: &[u8],
    ) -> Result<(), crate::Error> {
        let server_final = std::str::from_utf8(server_final_bytes).map_err(|e| {
            crate::Error::Authentication(format!(
                "Server-final message is not valid UTF-8: {}",
                e
            ))
        })?;

        if let Some(error) = server_final.strip_prefix("e=") {
            return Err(crate::Error::Authentication(format!(
                "SCRAM server error: {}",
                error
            )));
        }

        let server_sig_b64 = server_final.strip_prefix("v=").ok_or_else(|| {
            crate::Error::Authentication(format!(
                "Invalid server-final message (expected 'v=...'): {}",
                server_final
            ))
        })?;

        let server_signature = BASE64.decode(server_sig_b64).map_err(|e| {
            crate::Error::Authentication(format!("Failed to decode server signature: {}", e))
        })?;

        let server_key = self.server_key.as_ref().ok_or_else(|| {
            crate::Error::Authentication(
                "process_server_first was not called".to_string(),
            )
        })?;

        let auth_message = self.auth_message.as_ref().ok_or_else(|| {
            crate::Error::Authentication(
                "process_server_first was not called".to_string(),
            )
        })?;

        // Verify using ring's constant-time HMAC verification
        hmac::verify(server_key, auth_message.as_bytes(), &server_signature)
            .map_err(|_| {
                crate::Error::Authentication(
                    "Server signature verification failed".to_string(),
                )
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_algorithm_mechanism_names() {
        assert_eq!(ScramAlgorithm::Sha256.mechanism_name(), "SCRAM-SHA-256");
        assert_eq!(ScramAlgorithm::Sha512.mechanism_name(), "SCRAM-SHA-512");
    }

    #[test]
    fn test_algorithm_from_mechanism() {
        assert!(matches!(
            ScramAlgorithm::from_mechanism(SaslMechanism::ScramSha256),
            Some(ScramAlgorithm::Sha256)
        ));
        assert!(matches!(
            ScramAlgorithm::from_mechanism(SaslMechanism::ScramSha512),
            Some(ScramAlgorithm::Sha512)
        ));
        assert!(ScramAlgorithm::from_mechanism(SaslMechanism::Plain).is_none());
    }

    #[test]
    fn test_parse_server_first_valid() {
        let msg = "r=abc123XYZ,s=QSXCR+Q6sek8bf92,i=4096";
        let (nonce, salt, iterations) = parse_server_first(msg).unwrap();
        assert_eq!(nonce, "abc123XYZ");
        assert!(!salt.is_empty());
        assert_eq!(iterations.get(), 4096);
    }

    #[test]
    fn test_parse_server_first_missing_nonce() {
        let msg = "s=QSXCR+Q6sek8bf92,i=4096";
        assert!(parse_server_first(msg).is_err());
    }

    #[test]
    fn test_parse_server_first_missing_salt() {
        let msg = "r=abc123,i=4096";
        assert!(parse_server_first(msg).is_err());
    }

    #[test]
    fn test_parse_server_first_missing_iterations() {
        let msg = "r=abc123,s=QSXCR+Q6sek8bf92";
        assert!(parse_server_first(msg).is_err());
    }

    #[test]
    fn test_parse_server_first_zero_iterations() {
        let msg = "r=abc,s=QSXCR+Q6sek8bf92,i=0";
        assert!(parse_server_first(msg).is_err());
    }

    #[test]
    fn test_parse_server_first_invalid_iterations() {
        let msg = "r=abc,s=QSXCR+Q6sek8bf92,i=notanumber";
        assert!(parse_server_first(msg).is_err());
    }

    #[test]
    fn test_client_first_message_format() {
        let client = ScramClient::new(ScramAlgorithm::Sha256, "user", "pencil").unwrap();
        let msg = String::from_utf8(client.client_first_message()).unwrap();
        assert!(msg.starts_with("n,,n=user,r="));
        // Nonce is base64 of 24 bytes = 32 chars
        let nonce = msg.strip_prefix("n,,n=user,r=").unwrap();
        assert_eq!(nonce.len(), 32);
    }

    #[test]
    fn test_username_escaping() {
        let client =
            ScramClient::new(ScramAlgorithm::Sha512, "us=er,name", "pass").unwrap();
        let msg = String::from_utf8(client.client_first_message()).unwrap();
        assert!(msg.contains("n=us=3Der=2Cname"));
    }

    #[test]
    fn test_nonce_validation_rejects_bad_server_nonce() {
        let mut client =
            ScramClient::new(ScramAlgorithm::Sha256, "user", "pass").unwrap();
        let bad_server_first = format!(
            "r=WRONG{},s={},i=4096",
            "extra",
            BASE64.encode(b"somesalt")
        );
        let result = client.process_server_first(bad_server_first.as_bytes());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("client nonce"));
    }

    #[test]
    fn test_server_error_response() {
        let client =
            ScramClient::new(ScramAlgorithm::Sha256, "user", "pass").unwrap();
        let result = client.process_server_final(b"e=invalid-encoding");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("invalid-encoding"));
    }

    /// Simulate both client and server sides of a SCRAM handshake
    /// to verify proof computation and server signature verification.
    fn full_scram_handshake_test(algorithm: ScramAlgorithm) {
        let password = "pencil";
        let salt = b"salty salt bytes!";
        let iterations = NonZeroU32::new(4096).unwrap();

        // --- Client: create client-first ---
        let mut client = ScramClient::new(algorithm, "user", password).unwrap();
        let client_first = String::from_utf8(client.client_first_message()).unwrap();
        let client_first_bare = client_first.strip_prefix("n,,").unwrap();

        let client_nonce = client_first_bare
            .split(',')
            .find(|p| p.starts_with("r="))
            .unwrap()
            .strip_prefix("r=")
            .unwrap();

        // --- Server: produce server-first ---
        let server_nonce = format!("{}ServerExtra", client_nonce);
        let salt_b64 = BASE64.encode(salt);
        let server_first = format!("r={},s={},i={}", server_nonce, salt_b64, iterations);

        // --- Client: process server-first, produce client-final ---
        let client_final_bytes =
            client.process_server_first(server_first.as_bytes()).unwrap();
        let client_final = String::from_utf8(client_final_bytes).unwrap();

        // Extract proof from client-final
        let proof_b64 = client_final
            .split(',')
            .find(|p| p.starts_with("p="))
            .unwrap()
            .strip_prefix("p=")
            .unwrap();
        let proof = BASE64.decode(proof_b64).unwrap();

        // --- Server: verify client proof ---
        let salted_password =
            derive_salted_password(&algorithm, password, salt, iterations);
        let salted_key =
            hmac::Key::new(algorithm.hmac_algorithm(), &salted_password);
        let client_key_tag = hmac::sign(&salted_key, b"Client Key");
        let stored_key =
            digest::digest(algorithm.digest_algorithm(), client_key_tag.as_ref());

        let client_final_without_proof = format!("c=biws,r={}", server_nonce);
        let auth_message = format!(
            "{},{},{}",
            client_first_bare, server_first, client_final_without_proof
        );

        let stored_key_hmac =
            hmac::Key::new(algorithm.hmac_algorithm(), stored_key.as_ref());
        let client_signature =
            hmac::sign(&stored_key_hmac, auth_message.as_bytes());

        // Reconstruct ClientKey = ClientProof XOR ClientSignature
        let reconstructed_client_key: Vec<u8> = proof
            .iter()
            .zip(client_signature.as_ref().iter())
            .map(|(a, b)| a ^ b)
            .collect();

        // Verify Hash(reconstructed) == StoredKey
        let reconstructed_stored_key =
            digest::digest(algorithm.digest_algorithm(), &reconstructed_client_key);
        assert_eq!(reconstructed_stored_key.as_ref(), stored_key.as_ref());

        // --- Server: produce server-final ---
        let server_key_tag = hmac::sign(&salted_key, b"Server Key");
        let server_key_hmac =
            hmac::Key::new(algorithm.hmac_algorithm(), server_key_tag.as_ref());
        let server_signature =
            hmac::sign(&server_key_hmac, auth_message.as_bytes());
        let server_final =
            format!("v={}", BASE64.encode(server_signature.as_ref()));

        // --- Client: verify server-final ---
        client
            .process_server_final(server_final.as_bytes())
            .expect("Server signature verification should succeed");
    }

    #[test]
    fn test_full_scram_handshake_sha256() {
        full_scram_handshake_test(ScramAlgorithm::Sha256);
    }

    #[test]
    fn test_full_scram_handshake_sha512() {
        full_scram_handshake_test(ScramAlgorithm::Sha512);
    }

    #[test]
    fn test_wrong_server_signature_rejected() {
        let algorithm = ScramAlgorithm::Sha256;
        let salt = b"test_salt_bytes!";
        let iterations = NonZeroU32::new(4096).unwrap();

        let mut client =
            ScramClient::new(algorithm, "user", "pencil").unwrap();
        let client_first = String::from_utf8(client.client_first_message()).unwrap();
        let client_nonce = client_first
            .strip_prefix("n,,n=user,r=")
            .unwrap();

        let server_nonce = format!("{}Server", client_nonce);
        let server_first = format!(
            "r={},s={},i={}",
            server_nonce,
            BASE64.encode(salt),
            iterations
        );

        let _ = client.process_server_first(server_first.as_bytes()).unwrap();

        // Wrong server signature
        let wrong_sig = BASE64.encode(b"this is definitely wrong signature!!");
        let server_final = format!("v={}", wrong_sig);
        let result = client.process_server_final(server_final.as_bytes());
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("verification failed"));
    }
}
