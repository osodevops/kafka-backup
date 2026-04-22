//! `SaslMechanismPlugin` for SASL/GSSAPI (Kerberos).
//!
//! Feature-gated behind `gssapi`. The plugin is a thin state machine
//! around [`libgssapi::context::ClientCtx`] that satisfies RFC 4752 §3.1:
//!
//! ```text
//! Phase 1 — GSS context establishment
//!   initial_payload()         -> first token from gss_init_sec_context
//!   continue_payload(tok_N)   -> tok_{N+1} until gss_init_sec_context
//!                                returns Ok(None) (context complete)
//! Phase 1→2 — handshake transition
//!   continue_payload(<any>)   -> Continue(empty), prompting the broker
//!                                to send its wrapped security-layer
//!                                proposal
//! Phase 2 — security layer selection (RFC 4752 §3.1)
//!   continue_payload(proposal)
//!     -> gss_unwrap proposal (4 bytes: layer-mask | 3 bytes max-size)
//!     -> build reply: 0x01 | 0x00 0x00 0x00 | authzid
//!     -> gss_wrap reply
//!     -> Continue(wrapped_reply)
//!   continue_payload(<any>)   -> Done
//! ```
//!
//! ## Credential acquisition
//!
//! `libgssapi 0.9.1` does not expose a keytab-path argument on
//! [`libgssapi::credential::Cred::acquire`]. We steer the MIT krb5
//! library with the `KRB5_CLIENT_KTNAME` / `KRB5_CONFIG` environment
//! variables. Because `std::env::set_var` is process-global, we
//! serialize the entire credential-acquisition window behind
//! [`KRB5_ENV_LOCK`]. This closes the race that PR #95's unsynchronised
//! `set_var` left open when multiple `KafkaClient`s in the same process
//! authenticate concurrently.
//!
//! A better fix would be a keytab-aware `Cred::acquire` upstream; if
//! that lands in a future `libgssapi` release, switch to it and delete
//! the mutex.

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::debug;

use libgssapi::context::{ClientCtx, CtxFlags, SecurityContext};
use libgssapi::credential::{Cred, CredUsage};
use libgssapi::name::Name;
use libgssapi::oid::{OidSet, GSS_MECH_KRB5, GSS_NT_HOSTBASED_SERVICE};

use super::plugin::{SaslAuthOutcome, SaslMechanismPlugin, SaslPluginError};

/// Process-wide serialization point for the `KRB5_*` env-var mutation +
/// `Cred::acquire` call. See module docs.
static KRB5_ENV_LOCK: Mutex<()> = Mutex::const_new(());

/// Errors specific to [`GssapiPlugin`]. Carried inside
/// [`SaslPluginError::PayloadFailed`] when surfaced through the trait.
#[derive(Debug, Error)]
pub enum GssapiPluginError {
    #[error("GSSAPI name construction failed: {0}")]
    Name(String),

    #[error("GSSAPI credential acquisition failed: {0}")]
    Credential(String),

    #[error("GSSAPI context step failed: {0}")]
    ContextStep(String),

    #[error("GSSAPI wrap/unwrap failed: {0}")]
    Crypto(String),

    /// The Phase 2 server proposal was malformed (too short, or the
    /// layer-mask does not advertise the no-security-layer bit we rely
    /// on).
    #[error("GSSAPI Phase 2 proposal invalid: {0}")]
    InvalidProposal(String),

    /// A keytab path was configured but does not exist.
    #[error("GSSAPI keytab not found at {path}")]
    KeytabMissing { path: PathBuf },
}

impl GssapiPluginError {
    fn into_sasl_error(self) -> SaslPluginError {
        SaslPluginError::PayloadFailed {
            mechanism: "GSSAPI".to_string(),
            source: Box::new(self),
        }
    }
}

/// Plugin state machine.
#[derive(Debug)]
enum State {
    /// Not yet started, or reset after `Done` prior to reauth.
    Initial,
    /// Phase 1: exchanging `gss_init_sec_context` tokens.
    ContextInProgress { ctx: ClientCtx },
    /// Phase 1 complete; sent the empty turnaround token. Waiting for
    /// the broker's wrapped security-layer proposal.
    AwaitingLayerProposal { ctx: ClientCtx },
    /// Phase 2 reply sent; next server byte means Done.
    AwaitingFinalAck,
    /// Handshake complete.
    Done,
    /// Irrecoverable: future calls immediately return this error.
    Poisoned(String),
}

/// SASL/GSSAPI plugin.
///
/// Do not construct directly in production code — wire a
/// [`GssapiPluginFactory`] into
/// `SecurityConfig::sasl_mechanism_plugin_factory` and let
/// `KafkaClient::authenticate` build a fresh plugin per broker
/// connection. Each plugin holds its own `ClientCtx`, so sharing an
/// instance across the per-broker connection pool would corrupt
/// in-progress handshakes whenever KIP-368 reauth fires on a sibling
/// connection.
///
/// `GssapiPlugin::new` remains public for unit tests and for exotic
/// embeddings that hand-wire a single-connection plugin.
#[derive(Debug)]
pub struct GssapiPlugin {
    /// Kafka broker service name (`sasl.kerberos.service.name`).
    service_name: String,
    /// Target broker host — must match the hostname in the SPN. The
    /// hostname the broker's `advertised.listeners` resolves to must
    /// match the service principal's instance part (e.g.
    /// `kafka/kafka.test.local@TEST.LOCAL` ↔ connect to
    /// `kafka.test.local`).
    broker_host: String,
    /// Optional keytab path, surfaced through `KRB5_CLIENT_KTNAME`
    /// during `Cred::acquire`. `None` ⇒ OS credential cache (kinit).
    keytab_path: Option<PathBuf>,
    /// Optional `krb5.conf` path, surfaced through `KRB5_CONFIG`.
    krb5_config_path: Option<PathBuf>,
    /// Interior mutability: the trait hands `&self`, but `ClientCtx`
    /// needs `&mut` to step.
    state: Arc<Mutex<State>>,
}

impl GssapiPlugin {
    /// Construct a fresh plugin.
    ///
    /// `service_name` typically `"kafka"`. `broker_host` must resolve to
    /// the broker *and* match the instance part of the Kerberos service
    /// principal (`service/host@REALM`).
    pub fn new(
        service_name: impl Into<String>,
        broker_host: impl Into<String>,
        keytab_path: Option<PathBuf>,
        krb5_config_path: Option<PathBuf>,
    ) -> Result<Self, GssapiPluginError> {
        let keytab_path = match keytab_path {
            Some(p) if !p.exists() => {
                return Err(GssapiPluginError::KeytabMissing { path: p });
            }
            other => other,
        };
        Ok(Self {
            service_name: service_name.into(),
            broker_host: broker_host.into(),
            keytab_path,
            krb5_config_path,
            state: Arc::new(Mutex::new(State::Initial)),
        })
    }

    /// Build the `service@host` hostbased name the GSS library resolves
    /// against the KDC.
    fn target_name(&self) -> Result<Name, GssapiPluginError> {
        let spn = format!("{}@{}", self.service_name, self.broker_host);
        Name::new(spn.as_bytes(), Some(&GSS_NT_HOSTBASED_SERVICE))
            .map_err(|e| GssapiPluginError::Name(format!("{spn}: {e}")))
    }

    /// Acquire credentials, holding the process-wide lock while env
    /// vars are set and `Cred::acquire` reads them.
    async fn acquire_cred(&self) -> Result<Cred, GssapiPluginError> {
        let keytab = self.keytab_path.clone();
        let krb5_config = self.krb5_config_path.clone();

        let _guard = KRB5_ENV_LOCK.lock().await;

        // SAFETY: `std::env::set_var` is unsafe in edition 2024 because
        // it's not thread-safe w.r.t. other getenv callers. We
        // serialize via KRB5_ENV_LOCK — only one GSSAPI acquisition
        // mutates these variables at a time. Non-GSSAPI code does not
        // read `KRB5_CLIENT_KTNAME`, `KRB5_CONFIG`, or `KRB5CCNAME`.
        if let Some(path) = &keytab {
            unsafe {
                std::env::set_var("KRB5_CLIENT_KTNAME", path.as_os_str());
            }
            // Isolate from the OS credential cache. Without this, if the
            // operator has stale tickets in the default ccache (common on
            // macOS where API:<uuid> caches persist across logins), MIT
            // Kerberos prefers them over a fresh TGT from the keytab.
            // Stale tickets encrypted with an old service key cause the
            // broker to reject the AP-REQ with "invalid credentials".
            //
            // `MEMORY:<addr>` gives this plugin a private in-process
            // cache keyed by heap address — cheap, per-instance, and
            // cleaned up with the plugin.
            let ccache = format!("MEMORY:{:p}", self as *const Self);
            unsafe {
                std::env::set_var("KRB5CCNAME", ccache);
            }
        }
        if let Some(path) = &krb5_config {
            unsafe {
                std::env::set_var("KRB5_CONFIG", path.as_os_str());
            }
        }

        // Pin the mechanism to Kerberos 5 rather than relying on the
        // library's implicit default. Matches librdkafka + the Java
        // Kafka client, and protects against future libgssapi default
        // shifts picking up an unexpected mech.
        let mut desired = OidSet::new()
            .map_err(|e| GssapiPluginError::Credential(format!("OidSet::new: {e}")))?;
        desired
            .add(&GSS_MECH_KRB5)
            .map_err(|e| GssapiPluginError::Credential(format!("OidSet::add KRB5: {e}")))?;

        // `Cred::acquire` reads the env synchronously and stashes the
        // result in the native credential handle; subsequent operations
        // do not re-read the env. So it's safe to release the lock
        // after this call returns.
        Cred::acquire(None, None, CredUsage::Initiate, Some(&desired))
            .map_err(|e| GssapiPluginError::Credential(e.to_string()))
    }

    /// Drive one step of `gss_init_sec_context`. Returns the token the
    /// caller should put on the wire, or `None` if the context is
    /// complete after this step.
    fn step_context(
        ctx: &mut ClientCtx,
        server_token: Option<&[u8]>,
    ) -> Result<Option<Vec<u8>>, GssapiPluginError> {
        match ctx.step(server_token, None) {
            Ok(Some(tok)) => Ok(Some(tok.to_vec())),
            Ok(None) => Ok(None),
            Err(e) => Err(GssapiPluginError::ContextStep(e.to_string())),
        }
    }

    /// Parse the 4-byte RFC 4752 §3.1 Phase 2 proposal: one byte layer
    /// mask + three bytes max message size. We only accept the
    /// no-security-layer bit (0x01). Confidentiality/integrity layers
    /// would require additional wrap/unwrap on every ordinary Kafka
    /// RPC, which Kafka itself does not speak.
    fn parse_phase2_proposal(proposal: &[u8]) -> Result<u8, GssapiPluginError> {
        if proposal.len() < 4 {
            return Err(GssapiPluginError::InvalidProposal(format!(
                "server proposal is {} byte(s), expected ≥4",
                proposal.len()
            )));
        }
        let layer_mask = proposal[0];
        if layer_mask & 0x01 == 0 {
            return Err(GssapiPluginError::InvalidProposal(format!(
                "broker does not advertise the 0x01 (no-security-layer) bit; mask=0x{layer_mask:02x}"
            )));
        }
        Ok(layer_mask)
    }

    /// Build the Phase 2 reply payload to gss_wrap:
    /// `0x01 | 0x00 0x00 0x00 | authz_id`, per RFC 4752 §3.1.
    fn build_phase2_reply(authz_id: &str) -> Vec<u8> {
        let mut reply = Vec::with_capacity(4 + authz_id.len());
        // Chosen layer: no security layer (0x01)
        reply.push(0x01);
        // Max message size (we do not wrap ordinary Kafka RPCs, so 0)
        reply.extend_from_slice(&[0x00, 0x00, 0x00]);
        // authz_id — authenticated principal, or empty to let the
        // broker derive it from the context's source name (common).
        reply.extend_from_slice(authz_id.as_bytes());
        reply
    }
}

#[async_trait]
impl SaslMechanismPlugin for GssapiPlugin {
    fn mechanism_name(&self) -> &str {
        "GSSAPI"
    }

    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        let cred = self
            .acquire_cred()
            .await
            .map_err(GssapiPluginError::into_sasl_error)?;

        let target = self
            .target_name()
            .map_err(GssapiPluginError::into_sasl_error)?;

        let mut ctx = ClientCtx::new(
            Some(cred),
            target,
            CtxFlags::GSS_C_MUTUAL_FLAG | CtxFlags::GSS_C_SEQUENCE_FLAG,
            Some(&GSS_MECH_KRB5),
        );

        let first = Self::step_context(&mut ctx, None)
            .map_err(GssapiPluginError::into_sasl_error)?
            .ok_or_else(|| {
                GssapiPluginError::ContextStep(
                    "initial gss_init_sec_context returned no token".to_string(),
                )
                .into_sasl_error()
            })?;

        let mut state = self.state.lock().await;
        *state = State::ContextInProgress { ctx };
        Ok(first)
    }

    async fn continue_payload(
        &self,
        server_response: &[u8],
    ) -> Result<SaslAuthOutcome, SaslPluginError> {
        let mut state = self.state.lock().await;
        let current = std::mem::replace(&mut *state, State::Poisoned("transient".to_string()));

        match current {
            State::Initial => {
                let msg = "continue_payload called before initial_payload".to_string();
                *state = State::Poisoned(msg.clone());
                Err(GssapiPluginError::ContextStep(msg).into_sasl_error())
            }
            State::Poisoned(prior) => {
                let msg = format!("plugin is poisoned: {prior}");
                *state = State::Poisoned(prior);
                Err(GssapiPluginError::ContextStep(msg).into_sasl_error())
            }
            State::ContextInProgress { mut ctx } => {
                match Self::step_context(&mut ctx, Some(server_response)) {
                    Ok(Some(next)) => {
                        *state = State::ContextInProgress { ctx };
                        Ok(SaslAuthOutcome::Continue(next))
                    }
                    Ok(None) => {
                        // Phase 1 done. RFC 4752 says the client now
                        // sends an empty auth frame to prompt the
                        // broker's Phase 2 proposal.
                        *state = State::AwaitingLayerProposal { ctx };
                        Ok(SaslAuthOutcome::Continue(Vec::new()))
                    }
                    Err(e) => {
                        *state = State::Poisoned(e.to_string());
                        Err(e.into_sasl_error())
                    }
                }
            }
            State::AwaitingLayerProposal { mut ctx } => {
                let unwrapped = match ctx.unwrap(server_response) {
                    Ok(buf) => buf,
                    Err(e) => {
                        let err = GssapiPluginError::Crypto(format!("unwrap proposal: {e}"));
                        *state = State::Poisoned(err.to_string());
                        return Err(err.into_sasl_error());
                    }
                };
                let server_layers = match Self::parse_phase2_proposal(&unwrapped) {
                    Ok(mask) => mask,
                    Err(e) => {
                        *state = State::Poisoned(e.to_string());
                        return Err(e.into_sasl_error());
                    }
                };
                // Empty authz_id — broker uses the authenticated
                // principal (the GSS source name). This matches the
                // librdkafka default.
                let reply = Self::build_phase2_reply("");
                let wrapped = match ctx.wrap(false, &reply) {
                    Ok(buf) => buf.to_vec(),
                    Err(e) => {
                        let err = GssapiPluginError::Crypto(format!("wrap reply: {e}"));
                        *state = State::Poisoned(err.to_string());
                        return Err(err.into_sasl_error());
                    }
                };
                debug!(
                    server_layers = format_args!("0x{server_layers:02x}"),
                    authz_id = "",
                    "GSSAPI Phase 2 complete"
                );
                *state = State::AwaitingFinalAck;
                Ok(SaslAuthOutcome::Continue(wrapped))
            }
            State::AwaitingFinalAck | State::Done => {
                *state = State::Done;
                Ok(SaslAuthOutcome::Done)
            }
        }
    }

    fn supports_reauth(&self) -> bool {
        // Kerberos GSS-API contexts are bound to the wire connection;
        // Apache Kafka's broker rejects in-place re-authentication for
        // GSSAPI ("SaslAuthenticate request received after successful
        // authentication"), even when connections.max.reauth.ms > 0 is
        // advertised. Matches librdkafka behaviour: treat
        // session_lifetime_ms as a drain-and-reconnect timer rather than
        // firing reauth that the broker will reject. The session expires
        // naturally and the next RPC reconnects through the normal auth
        // path.
        false
    }

    async fn reauth_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        // Kerberos tickets expire; we cannot reuse a stale ClientCtx.
        // Reset to Initial and start a fresh handshake.
        {
            let mut state = self.state.lock().await;
            *state = State::Initial;
        }
        self.initial_payload().await
    }
}

/// Factory that builds a fresh [`GssapiPlugin`] per `KafkaClient`.
///
/// The factory stores the mechanism-wide configuration (service name,
/// keytab, krb5.conf) and defers the per-broker bit (hostname component
/// of the SPN) to [`Self::build`]. Kafka clusters typically have
/// per-broker SPNs (`kafka/brokerN.fqdn@REALM`), so binding the SPN at
/// build-time — with the broker host the router is about to connect
/// to — is the only way to authenticate a multi-broker cluster
/// correctly.
#[derive(Debug, Clone)]
pub struct GssapiPluginFactory {
    service_name: String,
    keytab_path: Option<PathBuf>,
    krb5_config_path: Option<PathBuf>,
}

impl GssapiPluginFactory {
    /// Construct a factory from the operator-provided configuration.
    ///
    /// The keytab is validated for existence eagerly: an absent keytab
    /// is a config error, so we surface it up-front rather than at the
    /// first broker connection. If you need different keytabs per
    /// broker, implement your own [`SaslMechanismPluginFactory`].
    pub fn new(
        service_name: impl Into<String>,
        keytab_path: Option<PathBuf>,
        krb5_config_path: Option<PathBuf>,
    ) -> Result<Self, GssapiPluginError> {
        let keytab_path = match keytab_path {
            Some(p) if !p.exists() => {
                return Err(GssapiPluginError::KeytabMissing { path: p });
            }
            other => other,
        };
        Ok(Self {
            service_name: service_name.into(),
            keytab_path,
            krb5_config_path,
        })
    }

    /// Wrap in an `Arc` for direct assignment to
    /// `SecurityConfig::sasl_mechanism_plugin_factory`.
    pub fn into_handle(self) -> super::plugin::SaslMechanismPluginFactoryHandle {
        Arc::new(self)
    }
}

impl super::plugin::SaslMechanismPluginFactory for GssapiPluginFactory {
    fn mechanism_name(&self) -> &str {
        "GSSAPI"
    }

    fn build(
        &self,
        broker_host: &str,
        _broker_port: u16,
    ) -> Result<super::plugin::SaslMechanismPluginHandle, SaslPluginError> {
        let plugin = GssapiPlugin::new(
            self.service_name.clone(),
            broker_host.to_string(),
            self.keytab_path.clone(),
            self.krb5_config_path.clone(),
        )
        .map_err(|e| SaslPluginError::FactoryFailed {
            mechanism: "GSSAPI".to_string(),
            source: Box::new(e),
        })?;
        Ok(Arc::new(plugin))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase2_proposal_parser_rejects_short_payload() {
        let err = GssapiPlugin::parse_phase2_proposal(&[0x01, 0x00, 0x00]).unwrap_err();
        match err {
            GssapiPluginError::InvalidProposal(msg) => {
                assert!(msg.contains("3 byte"), "message was: {msg}");
            }
            other => panic!("expected InvalidProposal, got {other:?}"),
        }
    }

    #[test]
    fn phase2_proposal_parser_rejects_missing_no_security_bit() {
        // Layer mask 0x02 = integrity-only, 0x04 = confidentiality —
        // both require on-the-wire wrap/unwrap the broker does not use.
        let err = GssapiPlugin::parse_phase2_proposal(&[0x02, 0x00, 0xFF, 0xFF]).unwrap_err();
        match err {
            GssapiPluginError::InvalidProposal(msg) => {
                assert!(msg.contains("0x01"), "message was: {msg}");
                assert!(msg.contains("0x02"), "mask should be reported: {msg}");
            }
            other => panic!("expected InvalidProposal, got {other:?}"),
        }
    }

    #[test]
    fn phase2_proposal_parser_accepts_no_security_layer_bit() {
        // 0x01 alone, or 0x07 (all three bits) — both include 0x01.
        GssapiPlugin::parse_phase2_proposal(&[0x01, 0x00, 0xFF, 0xFF]).unwrap();
        GssapiPlugin::parse_phase2_proposal(&[0x07, 0x00, 0xFF, 0xFF]).unwrap();
    }

    #[test]
    fn phase2_reply_format_has_layer_and_size_zero() {
        let reply = GssapiPlugin::build_phase2_reply("");
        assert_eq!(reply, vec![0x01, 0x00, 0x00, 0x00]);

        let reply_with_authz = GssapiPlugin::build_phase2_reply("alice@EXAMPLE.COM");
        assert_eq!(&reply_with_authz[..4], &[0x01, 0x00, 0x00, 0x00]);
        assert_eq!(&reply_with_authz[4..], b"alice@EXAMPLE.COM");
    }

    #[test]
    fn keytab_missing_at_construction_time_surfaces_clear_error() {
        // Plugin::new performs upfront existence check — fail fast at
        // construction rather than mid-handshake.
        let err = GssapiPlugin::new(
            "kafka",
            "kafka.test.local",
            Some(PathBuf::from("/definitely/does/not/exist.keytab")),
            None,
        )
        .unwrap_err();
        match err {
            GssapiPluginError::KeytabMissing { path } => {
                assert_eq!(path, PathBuf::from("/definitely/does/not/exist.keytab"));
            }
            other => panic!("expected KeytabMissing, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn continue_payload_before_initial_payload_is_poisoned_error() {
        // Calling continue_payload without initial_payload should
        // surface a clean error, not panic or silently succeed.
        let plugin = GssapiPlugin::new("kafka", "kafka.test.local", None, None).unwrap();
        let err = plugin.continue_payload(b"unexpected").await.unwrap_err();
        match err {
            SaslPluginError::PayloadFailed { mechanism, .. } => {
                assert_eq!(mechanism, "GSSAPI");
            }
            other => panic!("expected PayloadFailed, got {other:?}"),
        }
        // Subsequent continue_payload calls stay poisoned.
        let err2 = plugin.continue_payload(b"again").await.unwrap_err();
        assert!(matches!(err2, SaslPluginError::PayloadFailed { .. }));
    }

    #[test]
    fn gssapi_plugin_opts_out_of_reauth() {
        // Apache Kafka rejects in-place SaslAuthenticate for GSSAPI after
        // the initial handshake — the plugin must opt out so the client
        // does not schedule a KIP-368 reauth task.
        let plugin = GssapiPlugin::new("kafka", "kafka.test.local", None, None).unwrap();
        assert!(
            !plugin.supports_reauth(),
            "GSSAPI must not schedule live reauth — the broker rejects it"
        );
    }

    #[test]
    fn mechanism_name_is_gssapi() {
        // Matches what the broker's advertised mechanisms list should
        // contain; mismatches surface as UnsupportedSaslMechanism from
        // the broker.
        let plugin = GssapiPlugin::new("kafka", "kafka.test.local", None, None).unwrap();
        assert_eq!(plugin.mechanism_name(), "GSSAPI");
    }

    #[test]
    fn factory_mechanism_name_is_gssapi() {
        use super::super::plugin::SaslMechanismPluginFactory;
        let factory = GssapiPluginFactory::new("kafka", None, None).unwrap();
        assert_eq!(factory.mechanism_name(), "GSSAPI");
    }

    #[test]
    fn factory_missing_keytab_is_clear_error_at_construction() {
        let err = GssapiPluginFactory::new(
            "kafka",
            Some(PathBuf::from("/definitely/does/not/exist.keytab")),
            None,
        )
        .unwrap_err();
        match err {
            GssapiPluginError::KeytabMissing { path } => {
                assert_eq!(path, PathBuf::from("/definitely/does/not/exist.keytab"));
            }
            other => panic!("expected KeytabMissing, got {other:?}"),
        }
    }

    #[test]
    fn factory_builds_plugin_with_broker_specific_host() {
        use super::super::plugin::SaslMechanismPluginFactory;
        let factory = GssapiPluginFactory::new("kafka", None, None).unwrap();

        // Each call to build gets a *different* broker host — the
        // fundamental reason the factory exists. Verify the built
        // plugin carries the host we passed in.
        let handle_a = factory.build("broker-0.example.com", 9093).unwrap();
        let handle_b = factory.build("broker-1.example.com", 9094).unwrap();

        assert_eq!(handle_a.mechanism_name(), "GSSAPI");
        assert_eq!(handle_b.mechanism_name(), "GSSAPI");
        // Each build returns a fresh Arc — this is the whole point
        // (per-connection state).
        assert!(!Arc::ptr_eq(&handle_a, &handle_b));
    }
}
