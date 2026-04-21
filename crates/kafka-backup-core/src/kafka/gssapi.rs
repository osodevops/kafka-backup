//! GSSAPI/Kerberos SASL authentication for Kafka (RFC 4752).
//!
//! This module delegates all Kerberos operations to the OS GSSAPI library
//! (MIT Kerberos / `libgssapi_krb5` on Linux, Heimdal on macOS/BSD).
//!
//! Kerberos credentials are resolved in the following order:
//!   1. `keytab_path` from the security config       → applied as `KRB5_KTNAME`
//!   2. `krb5_config_path` from the security config  → applied as `KRB5_CONFIG`
//!   3. OS defaults: the credential cache populated by `kinit`, the system
//!      `/etc/krb5.conf`, and any keytab referenced there.
//!
//! This module is always compiled, exactly like `scram.rs`. When
//! `sasl_mechanism` is not `GSSAPI` the code is never called; the linker
//! includes `libgssapi_krb5` but it stays dormant.
//!
//! The Kafka SASL/GSSAPI wire protocol follows RFC 4752 §3.1:
//!   Phase 1 — GSS context establishment (one or more SaslAuthenticate
//!              round-trips until `gss_init_sec_context` returns complete)
//!   Phase 2 — Security-layer negotiation (one final SaslAuthenticate
//!              round-trip; kafka-backup always selects "no security layer")
//!
//! # libgssapi version
//! Written against libgssapi 0.9.1. The key API points used are:
//!   - `OidSet::new()` / `OidSet::add()` to build the desired-mechs set
//!   - `Cred::acquire(name, time_req, usage, Option<&OidSet>)`
//!   - `ClientCtx::new(Option<Cred>, Name, CtxFlags, Option<&'static Oid>)`
//!   - `ClientCtx::step(Option<&[u8]>, Option<&[u8]>)` → `Result<Option<Buf>>`
//!   - `SecurityContext::wrap(bool, &[u8])` / `SecurityContext::unwrap(&[u8])`

use libgssapi::{
    context::{ClientCtx, CtxFlags, SecurityContext},
    credential::{Cred, CredUsage},
    name::Name,
    oid::{OidSet, GSS_MECH_KRB5, GSS_NT_HOSTBASED_SERVICE},
};
use tracing::debug;
use std::path::Path;


/// GSSAPI client state machine for a single broker connection.
///
/// Create a fresh `GssapiClient` for each connection (or reconnection).
/// GSS security contexts are bound to one TCP connection and must not be
/// reused after a reconnect.
///
/// # Typical call sequence (mirrors the logic in `client.rs`)
///
/// ```ignore
/// let mut gss = GssapiClient::new(
///     "kafka",                            // service_name
///     "broker1.corp.example.com",         // broker_host (no port)
///     Some("/etc/kafka/kafka.keytab"),    // keytab_path from config
///     Some("/etc/kafka/krb5.conf"),       // krb5_config_path from config
/// )?;
///
/// // Phase 1 — GSS context establishment (1+ round trips)
/// let mut token = gss.initial_token()?.expect("initial token is always Some");
/// loop {
///     // send token → SaslAuthenticate → receive server_token
///     match gss.step(&server_token)? {
///         Some(next_token) => token = next_token,
///         None => break, // context fully established, nothing more to send
///     }
/// }
///
/// // Phase 2 — security-layer negotiation (1 round trip)
/// // broker sends a GSSAPI-wrapped 4-byte proposal; we reply with layer=1
/// let reply = gss.handle_security_layer(&server_wrapped, "")?;
/// // send reply → SaslAuthenticate → done
/// ```
pub(crate) struct GssapiClient {
    ctx: ClientCtx,
}

impl GssapiClient {
    /// Create a new GSSAPI client targeting a specific Kafka broker.
    ///
    /// # Parameters
    ///
    /// - `service_name` — Kerberos service name; must match the broker's
    ///   `sasl.kerberos.service.name` property (Kafka default: `"kafka"`).
    ///
    /// - `broker_host` — Hostname of the broker, **without the port**.
    ///   Combined with `service_name` to form the GSS target principal
    ///   `service_name@broker_host`, which the library resolves to
    ///   `service_name/broker_host@REALM` via the host-based service OID
    ///   (RFC 2743 §4.1).
    ///
    /// - `keytab_path` — Optional path to a Kerberos keytab file.  When
    ///   `Some`, it is set as the `KRB5_CLIENT_KTNAME` environment variable before
    ///   credential acquisition, causing the library to use the keytab
    ///   instead of the default credential cache.  Corresponds to
    ///   `security.sasl_keytab_path` in `backup.yaml`.
    ///
    /// - `krb5_config_path` — Optional path to a `krb5.conf` file.  When
    ///   `Some`, it is set as the `KRB5_CONFIG` environment variable before
    ///   credential acquisition, overriding the system-wide `/etc/krb5.conf`.
    ///   Corresponds to `security.sasl_krb5_config_path` in `backup.yaml`.
    pub fn new(
        service_name: &str,
        broker_host: &str,
        keytab_path: Option<&Path>,
        krb5_config_path: Option<&Path>,
    ) -> Result<Self, crate::Error> {
        // Apply file-path overrides from the kafka-backup configuration before
        // any GSSAPI call.  These environment variables are the standard MIT
        // Kerberos mechanism for directing the library to specific files.
        //
        // Thread-safety note: `set_var` is not guaranteed safe when other
        // threads are reading the environment concurrently.  This is acceptable
        // here because:
        //   a) kafka-backup calls connect() from a single async task per
        //      KafkaClient instance, so there is no intra-client race.
        //   b) Both variables are only ever written, never deleted, so if two
        //      clients race at startup they write the same configured value.
        //   c) The Rust stdlib documents this limitation; we accept it for the
        //      same reasons other Rust GSSAPI integrations do when configuring
        //      Kerberos via environment variables.
        if let Some(path) = krb5_config_path {
            // SAFETY: see thread-safety note above
            unsafe { std::env::set_var("KRB5_CONFIG", path) };
            debug!("GSSAPI: KRB5_CONFIG={}", path.display());
        }
        if let Some(path) = keytab_path {
            // SAFETY: see thread-safety note above
            unsafe { std::env::set_var("KRB5_CLIENT_KTNAME", path) };
            debug!("GSSAPI: KRB5_CLIENT_KTNAME={}", path.display());
        }

        // Build the GSS target name as a host-based service name.
        // The format "service@host" is the RFC 2743 §4.1 host-based syntax.
        let target_str = format!("{}@{}", service_name, broker_host);
        let target_name =
            Name::new(target_str.as_bytes(), Some(&GSS_NT_HOSTBASED_SERVICE)).map_err(|e| {
                crate::Error::Authentication(format!(
                    "GSSAPI: failed to construct target name '{}': {}",
                    target_str, e
                ))
            })?;

        // Build an OidSet containing only the Kerberos 5 mechanism.
        // libgssapi 0.9.1 requires Cred::acquire to receive Option<&OidSet>
        // rather than a plain slice of OIDs — this is the correct API call.
        let desired_mechs = {
            let mut s = OidSet::new().map_err(|e| {
                crate::Error::Authentication(format!(
                    "GSSAPI: failed to create OidSet: {}",
                    e
                ))
            })?;
            s.add(&GSS_MECH_KRB5).map_err(|e| {
                crate::Error::Authentication(format!(
                    "GSSAPI: failed to add KRB5 to OidSet: {}",
                    e
                ))
            })?;
            s
        };

        // Acquire initiator credentials from the OS credential cache or keytab.
        // Passing `None` for the name lets the library choose the default
        // principal (the principal from `kinit`, or the first key in the keytab
        // when KRB5_CLIENT_KTNAME is set above).
        // Passing `None` for time_req requests the maximum available lifetime.
        let cred =
            Cred::acquire(None, None, CredUsage::Initiate, Some(&desired_mechs)).map_err(
                |e| {
                    crate::Error::Authentication(format!(
                        "GSSAPI: failed to acquire Kerberos credentials: {}. \
                         Check that either a valid ticket-granting ticket exists \
                         (run `kinit`) or that `sasl_keytab_path` in the security \
                         configuration points to a readable keytab file.",
                        e
                    ))
                },
            )?;

        // Request mutual authentication and sequence-number protection.
        // Both flags are mandatory for the Kafka SASL/GSSAPI profile (RFC 4752).
        // bitflags 2.0 (used by libgssapi 0.9.1) supports the | operator.
        let flags = CtxFlags::GSS_C_MUTUAL_FLAG | CtxFlags::GSS_C_SEQUENCE_FLAG;

        // ClientCtx::new stores parameters; it does not perform I/O or produce
        // tokens yet.  Tokens are generated lazily in step().
        // Signature in 0.9.1: new(cred: Option<Cred>, target: Name,
        //                         flags: CtxFlags, mech: Option<&'static Oid>)
        let ctx = ClientCtx::new(Some(cred), target_name, flags, Some(&GSS_MECH_KRB5));

        Ok(Self { ctx })
    }

    /// Produce the **initial** GSSAPI token, the first bytes sent to the broker
    /// in Phase 1.
    ///
    /// Returns `Ok(Some(token))`.  For Kerberos the initial token is never
    /// empty, so `None` here would indicate a library bug.
    pub fn initial_token(&mut self) -> Result<Option<Vec<u8>>, crate::Error> {
        self.step_inner(None)
    }

    /// Advance the GSS context with a token received from the broker.
    ///
    /// Returns:
    /// - `Ok(Some(token))` — send `token` to the broker; context not yet done
    /// - `Ok(None)`        — context is fully established; Phase 1 complete
    /// - `Err(_)`          — unrecoverable GSSAPI error
    pub fn step(&mut self, server_token: &[u8]) -> Result<Option<Vec<u8>>, crate::Error> {
        self.step_inner(Some(server_token))
    }

    // Internal: drive ClientCtx::step with optional input.
    // Signature in 0.9.1: step(&mut self, tok: Option<&[u8]>,
    //                           channel_bindings: Option<&[u8]>)
    //                    -> Result<Option<Buf>, Error>
    // Buf implements Deref<Target=[u8]>, so .to_vec() works correctly.
    fn step_inner(&mut self, input: Option<&[u8]>) -> Result<Option<Vec<u8>>, crate::Error> {
        match self.ctx.step(input, None) {
            Ok(Some(buf)) => Ok(Some(buf.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(crate::Error::Authentication(format!(
                "GSSAPI: context establishment step failed: {}",
                e
            ))),
        }
    }

    /// Handle the RFC 4752 §3.1 Phase 2 security-layer negotiation.
    ///
    /// After Phase 1 the broker sends a GSSAPI-wrapped 4-byte proposal:
    ///
    /// ```text
    /// byte  0    : bitmask of supported security layers
    ///                0x01 = no security layer  (always required)
    ///                0x02 = integrity only
    ///                0x04 = confidentiality + integrity
    /// bytes 1..3 : maximum message size the broker accepts (big-endian u24)
    /// ```
    ///
    /// This method unwraps the proposal, asserts that "no security layer"
    /// (0x01) is offered, and returns a GSSAPI-wrapped reply selecting it:
    ///
    /// ```text
    /// byte  0    : 0x01              (select no security layer)
    /// bytes 1..3 : 0x00 0x00 0x00   (max message size; unused for layer 1)
    /// bytes 4..  : authz_id as UTF-8 (empty = use the authenticated identity)
    /// ```
    ///
    /// The reply is wrapped without encryption (integrity-only, `conf_req=false`)
    /// and returned as bytes ready for a `SaslAuthenticateRequest`.
    ///
    /// `wrap` and `unwrap` are provided by the `SecurityContext` trait impl on
    /// `ClientCtx` in libgssapi 0.9.1 — no API changes there.
    pub fn handle_security_layer(
        &mut self,
        server_wrapped: &[u8],
        authz_id: &str,
    ) -> Result<Vec<u8>, crate::Error> {
        let unwrapped = self.ctx.unwrap(server_wrapped).map_err(|e| {
            crate::Error::Authentication(format!(
                "GSSAPI: failed to unwrap broker security-layer proposal: {}",
                e
            ))
        })?;

        if unwrapped.len() < 4 {
            return Err(crate::Error::Authentication(format!(
                "GSSAPI: broker security-layer message too short \
                 ({} bytes, expected ≥4)",
                unwrapped.len()
            )));
        }

        let server_layers = unwrapped[0];
        if server_layers & 0x01 == 0 {
            // kafka-backup does not implement per-message GSSAPI
            // integrity/confidentiality wrapping of Kafka protocol frames.
            // Only the authentication handshake itself uses GSSAPI wrapping.
            return Err(crate::Error::Authentication(format!(
                "GSSAPI: broker security-layer bitmask (0x{:02x}) does not \
                 include no-security-layer (0x01). kafka-backup requires \
                 security layer 1 (authentication only, no per-message wrap).",
                server_layers
            )));
        }

        // Build the client response: choose layer 1, size=0, optional authz_id.
        let mut response_plain = vec![0x01u8, 0x00, 0x00, 0x00];
        response_plain.extend_from_slice(authz_id.as_bytes());

        // Wrap without encryption (conf_req = false → integrity-only wrap).
        let wrapped = self.ctx.wrap(false, &response_plain).map_err(|e| {
            crate::Error::Authentication(format!(
                "GSSAPI: failed to wrap security-layer response: {}",
                e
            ))
        })?;

        debug!(
            "GSSAPI: Phase 2 complete (server_layers=0x{:02x}, authz_id={:?})",
            server_layers, authz_id
        );

        Ok(wrapped.to_vec())
    }
}
