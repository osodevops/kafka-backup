# Product Requirements Document: Pluggable SASL Mechanism Extension Point

**Document Version:** 2.2
**Date:** 2026-04-22
**Status:** Implemented (0.15.0, unreleased)
**Owners:** kafka-backup-core maintainers
**Supersedes:** v2.1 (2026-04-21)

---

## Changelog

- **v2.2 (2026-04-22):** Reshaped the extension point from a shared `Arc<dyn SaslMechanismPlugin>` to a per-connection factory (`SaslMechanismPluginFactory`). `SecurityConfig.sasl_mechanism_plugin` is gone; the field is now `sasl_mechanism_plugin_factory: Option<SaslMechanismPluginFactoryHandle>`. The factory is invoked once per `KafkaClient` with the broker endpoint from `bootstrap_servers[0]` (which `PartitionLeaderRouter` has already rewritten to the advertised per-broker `host:port` from `MetadataResponse`). Stateless mechanisms (PLAIN, OAUTHBEARER) wrap a single `Arc` in `SharedPluginFactory`; stateful mechanisms (GSSAPI) build a fresh plugin per call. Fixes one correctness bug and removes one latent risk: (1) **fixed** — non-bootstrap brokers authenticating with the bootstrap SPN on multi-broker Kerberized clusters; (2) **removed as a latent risk** — sharing a single `GssapiPlugin` `ClientCtx` across a pooled `connections_per_broker` was a concurrency hazard even if it hadn't produced a visible failure. The GSSAPI integration roundtrip now runs at the default `connections_per_broker: 4` with each pooled connection owning its own `GssapiPlugin`. The pool-isolation guarantee is now tested end-to-end (`pool_produces_distinct_plugin_per_kafkaclient`). Also adds a `SaslMechanismPlugin::supports_reauth()` capability flag (default `true`; `GssapiPlugin` overrides to `false`) — Apache Kafka rejects live `SaslAuthenticate` for GSSAPI after the initial handshake because Kerberos contexts are connection-bound, so the client no longer schedules a reauth task for GSSAPI and the broker-advertised `session_lifetime_ms` becomes a drain-and-reconnect window (matches librdkafka / JVM client behaviour). New `SaslPluginError::FactoryFailed` variant. No version bump — folds into unreleased 0.15.0.
- **v2.1 (2026-04-21):** Aligned spec with shipped 0.14.0 implementation. Mock SASL server is hand-rolled on top of `kafka-protocol = "0.17"` (`tests/integration_suite/sasl_mock_broker.rs`); `rsasl` dev-dep was NOT added — zero new dev-deps, strictly better than the v2.0 target. E2E fixture uses Confluent cp-kafka 7.7.0 with the bundled `OAuthBearerUnsecuredValidatorCallbackHandler` (`tests/sasl-oauth-test-infra/`), not Redpanda — Redpanda lacks a simple unsecured-JWS OAUTHBEARER path. All 14 unit + 4 integration tests ship. Known limitation: reauth task is not torn down on reconnect (follow-up #132).
- **v2.0 (2026-04-20):** Protocol detail hardened against KIP-368 and RFC 7628 authoritative sources; verified OSS code-path anchors (file:line); resolved open questions Q1/Q2/Q3; expanded test plan from 10 → 14 units and 1 → 6 integrations; added a pre-refactor SCRAM/PLAIN regression baseline commit (OSS currently has zero SASL tests); corrected `interpret_server_error` default to handle Kafka 3.5+ free-form error_message (not just RFC 7628 JSON); added Redpanda-backed E2E; added two risks (R6 non-JSON error, R7 plugin re-entry deadlock). Budget raised from 3 → 5 engineer-days.
- **v1.0 (2026-04-20):** Initial draft.

---

## Executive Summary

`kafka-backup-core` supports `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512` SASL mechanisms natively. Modern managed Kafka services — **AWS MSK with IAM**, **Confluent Cloud**, **Azure Event Hubs**, **Strimzi + Keycloak** — require **OAUTHBEARER** (RFC 7628) with service-specific token providers that each bring heavyweight, commercial-coupled dependencies (`aws-sigv4` for MSK IAM, full OIDC client flows, vendor SDKs).

Rather than bundle those dependencies into OSS, this PRD introduces a **minimal, dependency-free extension trait** — `SaslMechanismPlugin` — in OSS. Downstream crates (notably `kafka-backup-enterprise-core`) implement OAUTHBEARER and its token providers without adding a single new **runtime** dependency to the OSS build.

**OSS cost: ~650 LOC, zero new runtime dependencies.** Two dev-dependencies (`rsasl` for mock SASL server, `testcontainers-modules` Kafka feature) for tests only.

**Delivery: 5 engineer-days, one PR.** Unblocks enterprise OAUTHBEARER / MSK IAM integration; no behavioural change to existing PLAIN / SCRAM / mTLS / plaintext users.

---

## 1. Problem Statement

### 1.1 Current state (verified against `main` @ `d0e791e`, Apr 2026)

- `crates/kafka-backup-core/src/config.rs:272-276` defines `SaslMechanism::{Plain, ScramSha256, ScramSha512}`.
- `crates/kafka-backup-core/src/kafka/client.rs` hard-codes SASL dispatch on that enum. The dispatch is **duplicated across two entry points**:
  - Initial-connect: `authenticate()` at L225-247 calls `sasl_plain_auth()` (L249-284) or `sasl_scram_auth()` (L287-356).
  - Reconnect: `authenticate_raw()` at L462-484 calls `sasl_plain_auth_raw()` (L487-522) or `sasl_scram_auth_raw()` (L525-596).
  - Two `match security.sasl_mechanism` sites (L228, L465). Four auth methods total for two mechanisms.
- `SaslHandshake` is sent at **version 1**; `SaslAuthenticate` at **version 2** (hardcoded at L725-726). `SaslAuthenticate v1+` is the floor for KIP-368 support — we are already on the correct floor.
- **Zero SASL tests exist** anywhere in `crates/kafka-backup-core/tests/` or as inline unit tests. TLS integration tests (`tests/integration_suite/tls_tests.rs`, 216 lines) are the closest precedent and establish the `testcontainers` + docker-compose pattern we should follow.
- Connection state is `Arc<Mutex<Option<BrokerConnection>>>` at `client.rs:43` — a single mutex over the whole stream. `is_connection_error()` (L404) explicitly excludes `SaslHandshake`/`SaslAuthenticate` API keys from auto-reconnect (L384), so SASL errors don't trigger the generic retry loop — they propagate to the caller.

Adding a mechanism today means editing OSS code, adding its wire protocol, its token providers, and all transitive deps (AWS SDK, OIDC client, etc.).

### 1.2 Why not add OAUTHBEARER natively in OSS

1. **Dependency weight.** MSK IAM support requires `aws-sigv4`, `aws-credential-types`, `aws-config` — ~1.2 MB binary bloat and ~50 transitive crates (verified via Perplexity query against rust-rdkafka ecosystem, Apr 2026). OIDC support requires a full OAuth2 client. These benefit only users on managed cloud Kafka.
2. **Commercial fit.** Migration against managed cloud Kafka clusters is the paid use case (MSK migrator is enterprise-gated). Token-provider complexity should live in the enterprise layer where the investment is funded.
3. **Provider fragmentation.** MSK IAM, Confluent Cloud, Azure Event Hubs, Strimzi+Keycloak each require different token logic. Shipping one "correct" provider set in OSS is impossible; shipping several is bloat.
4. **Maintenance surface.** RFC 7628 framing, KIP-368 re-authentication, token caching, clock-skew handling — these are production-grade complexity that benefit the narrow set of managed-cloud users.

### 1.3 Why not fork `KafkaClient` in enterprise

- `KafkaClient` and `PartitionLeaderRouter` are the backbone of OSS: 2000+ LOC including retry logic, partition-leader routing, connection pooling, request/response framing. Duplicating this in enterprise doubles maintenance cost forever.
- Every future Kafka protocol improvement (e.g., KIP-951 `NOT_LEADER_OR_FOLLOWER` routing fix, KIP-714 client metrics) would require parallel merges into a fork.

---

## 2. Proposal: `SaslMechanismPlugin` Extension Trait

OSS exposes one new public trait. It lets external crates provide mechanism-specific SASL bytes without knowing anything about Kafka's SASL framing, version negotiation, or KIP-368 re-auth.

### 2.1 The trait

```rust
// crates/kafka-backup-core/src/kafka/sasl/plugin.rs  (NEW)

use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::Arc;

/// Pluggable SASL mechanism. Implementors supply mechanism-specific payload
/// bytes; kafka-backup-core handles Kafka SASL framing, handshake negotiation,
/// and KIP-368 session-lifetime re-authentication.
///
/// This trait is the extension point for SASL mechanisms whose implementation
/// would bring heavyweight or cloud-vendor-specific dependencies into OSS
/// (OAUTHBEARER with MSK IAM / OIDC tokens, GSSAPI with Kerberos, etc.).
///
/// See `kafka-backup-enterprise-core` for the canonical OAUTHBEARER
/// implementation of this trait.
///
/// # Contract
///
/// 1. `mechanism_name()` MUST return a stable string for the lifetime of the
///    plugin. It is sent verbatim in `SaslHandshakeRequest.mechanism`.
/// 2. `initial_payload()` is called once per authentication (including re-auth).
///    For single-round mechanisms like OAUTHBEARER, this is the full
///    authentication payload.
/// 3. `continue_payload()` defaults to `Ok(None)` (single-round). Override
///    only if the mechanism requires server-driven multi-round exchange.
/// 4. `interpret_server_error()` is invoked with the raw `error_message` or
///    `auth_bytes` payload from a failed handshake. The default implementation
///    attempts RFC 7628 JSON parse and falls back to UTF-8 string. Override
///    if the mechanism has a structured error format beyond RFC 7628.
/// 5. `reauth_payload()` defaults to `initial_payload()`. Override if the
///    re-authentication payload must differ (rare).
///
/// # Concurrency requirements
///
/// Plugin methods **MUST NOT** call back into the `KafkaClient` that is
/// driving them (e.g., to issue Metadata RPCs during a handshake). Doing so
/// will deadlock on the connection mutex. Plugins that require side-channel
/// data should fetch it via their own HTTP client or a separately-configured
/// `KafkaClient` instance.
#[async_trait]
pub trait SaslMechanismPlugin: Send + Sync + Debug {
    /// The SASL mechanism name as advertised on the wire (e.g., "OAUTHBEARER").
    fn mechanism_name(&self) -> &str;

    /// Produce the initial client SASL payload.
    ///
    /// - For OAUTHBEARER: RFC 7628 client initial response —
    ///   `n,a=<authzid>,\x01[kvpair\x01]*auth=Bearer <token>\x01\x01`.
    /// - For GSSAPI: GSS-API context-establishment token.
    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError>;

    /// Continue a multi-round mechanism. Called when the server returns a
    /// non-empty `SaslAuthenticateResponse.auth_bytes` alongside `error_code=0`.
    /// Return `Ok(Some(next_bytes))` to send another round, or `Ok(None)` to
    /// declare success.
    async fn continue_payload(
        &self,
        _server_bytes: &[u8],
    ) -> Result<Option<Vec<u8>>, SaslPluginError> {
        Ok(None)
    }

    /// Interpret a server error payload.
    ///
    /// This is invoked when `SaslAuthenticateResponse.error_code != 0` OR when
    /// the broker closes the connection mid-handshake with a populated
    /// `error_message`.
    ///
    /// The default implementation handles both common shapes:
    ///
    /// 1. **RFC 7628 JSON body** — `{"status": "invalid_token", "scope": "...",
    ///    "openid-configuration": "..."}`. If parse succeeds, returns a
    ///    `ServerRejected` with the parsed `status` and `scope` surfaced.
    /// 2. **Free-form UTF-8 text** — Apache Kafka 3.5+ brokers (including
    ///    Confluent Platform and Strimzi) return free-form error messages
    ///    rather than RFC 7628 JSON. The fallback wraps the lossy UTF-8 decode
    ///    in a `ServerRejected`.
    fn interpret_server_error(&self, error_bytes: &[u8]) -> SaslPluginError {
        // Try RFC 7628 JSON first.
        if let Ok(parsed) = serde_json::from_slice::<Rfc7628Error>(error_bytes) {
            return SaslPluginError::ServerRejected {
                mechanism: self.mechanism_name().to_string(),
                detail: format!(
                    "{} (scope={:?})",
                    parsed.status,
                    parsed.scope.as_deref().unwrap_or("<none>")
                ),
            };
        }
        // Fallback: treat as UTF-8 text (Kafka 3.5+ broker reality).
        SaslPluginError::ServerRejected {
            mechanism: self.mechanism_name().to_string(),
            detail: String::from_utf8_lossy(error_bytes).into_owned(),
        }
    }

    /// Re-authentication payload for KIP-368. Called by the core's re-auth
    /// scheduler before the session lifetime elapses. Typically the plugin
    /// refreshes an underlying token and returns fresh initial bytes.
    async fn reauth_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        self.initial_payload().await
    }
}

/// RFC 7628 §3.2.2 error response body.
#[derive(Debug, serde::Deserialize)]
struct Rfc7628Error {
    status: String,
    scope: Option<String>,
    #[serde(rename = "openid-configuration")]
    #[allow(dead_code)]
    openid_configuration: Option<String>,
}

/// Handle to a plugin. Client code holds `Arc<dyn SaslMechanismPlugin>`.
pub type SaslMechanismPluginHandle = Arc<dyn SaslMechanismPlugin>;

#[derive(Debug, thiserror::Error)]
pub enum SaslPluginError {
    #[error("plugin failed to produce payload: {0}")]
    PayloadProductionFailed(String),

    #[error("server rejected {mechanism}: {detail}")]
    ServerRejected { mechanism: String, detail: String },

    #[error("token or credential acquisition failed: {0}")]
    CredentialError(String),

    #[error("plugin internal error: {0}")]
    Internal(#[source] Box<dyn std::error::Error + Send + Sync>),
}
```

**Design note on `#[non_exhaustive]`:** Not applied to the trait. In Rust, `#[non_exhaustive]` has semantic effect only on structs, enums, and variants — *not* on traits. Adding new methods with default implementations is already backwards-compatible for downstream `impl` blocks without the attribute. (Open question Q1 from v1.0 resolved here.)

**Design note on broker mechanism advertisement:** The trait does not expose the broker's advertised mechanisms list. If a plugin's `mechanism_name()` is not advertised, `SaslHandshakeResponse.error_code` returns `UNSUPPORTED_SASL_MECHANISM` which the dispatcher surfaces naturally. (Open question Q2 from v1.0 resolved here.)

### 2.2 Config wiring

`SecurityConfig` gains one field. The field is a **factory handle**, not a plugin handle — see §2.2a for the factory trait shape and why.

```rust
// crates/kafka-backup-core/src/config.rs  (modified)

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SecurityConfig {
    // ...existing fields unchanged (security_protocol, sasl_mechanism,
    // sasl_username, sasl_password, ssl_ca_location,
    // ssl_certificate_location, ssl_key_location)...

    /// External SASL mechanism plugin factory. When set, overrides
    /// `sasl_mechanism` dispatch. Programmatic-only (not YAML-addressable);
    /// downstream crates inject factories at client-build time.
    ///
    /// Called once per `KafkaClient` with the broker endpoint from
    /// `bootstrap_servers[0]`. `PartitionLeaderRouter` has already rewritten
    /// that entry to the advertised per-broker `host:port` from the
    /// `MetadataResponse`, so the factory receives the correct endpoint for
    /// mechanism-specific per-broker state (e.g. GSSAPI SPN).
    ///
    /// See `SaslMechanismPluginFactory` for the extension contract.
    #[serde(skip)]
    pub sasl_mechanism_plugin_factory: Option<SaslMechanismPluginFactoryHandle>,
}
```

`#[serde(skip)]` is a novel pattern in this file (verified: zero prior uses in `config.rs`). It keeps the YAML config surface unchanged and prevents users from attempting to deserialize something into a trait object. The `Default` derive produces `None`, preserving legacy behaviour for every existing config file.

### 2.2a Factory trait

```rust
// crates/kafka-backup-core/src/kafka/sasl/plugin.rs  (added alongside SaslMechanismPlugin)

/// Factory that produces a `SaslMechanismPlugin` bound to a specific broker
/// endpoint. Called once per `KafkaClient::authenticate` with the endpoint
/// from `bootstrap_servers[0]`.
///
/// # Why a factory, not a shared `Arc<dyn SaslMechanismPlugin>`
///
/// Stateful mechanisms (notably GSSAPI) hold per-connection state — a GSS
/// context, a keytab-derived credential handle — that cannot be shared across
/// concurrent connections to different brokers. Two concrete bugs the factory
/// shape fixes:
///
/// 1. **Per-broker SPN (Kerberos).** `kafka/broker1.fqdn@REALM` differs from
///    `kafka/broker2.fqdn@REALM`. A single plugin constructed from
///    `bootstrap_servers[0]` authenticates every connection against the
///    bootstrap's SPN — wrong for non-bootstrap brokers. The factory binds
///    SPN at `.build()` time, after `PartitionLeaderRouter` has already
///    rewritten `bootstrap_servers` to the advertised per-broker host.
/// 2. **Per-connection context.** A shared GSSAPI plugin's KIP-368 reauth
///    resets its internal state machine while a sibling connection has an
///    in-flight handshake on the same plugin, corrupting the sibling.
///
/// Stateless mechanisms (PLAIN, OAUTHBEARER with static/cached tokens) can
/// wrap a single `Arc` via `SharedPluginFactory` — the factory's `build` just
/// clones the Arc.
pub trait SaslMechanismPluginFactory: Send + Sync + Debug {
    /// Advertised mechanism name. Must match the plugin returned by `build`.
    fn mechanism_name(&self) -> &str;

    /// Construct a plugin bound to this broker endpoint.
    ///
    /// `broker_host` / `broker_port` come from `KafkaConfig.bootstrap_servers[0]`
    /// at the time `KafkaClient::authenticate` runs. For clients produced by
    /// `PartitionLeaderRouter::get_broker_connection`, that entry is the
    /// advertised per-broker endpoint from `MetadataResponse`.
    fn build(
        &self,
        broker_host: &str,
        broker_port: u16,
    ) -> Result<SaslMechanismPluginHandle, SaslPluginError>;
}

pub type SaslMechanismPluginFactoryHandle = Arc<dyn SaslMechanismPluginFactory>;

/// Stateless-mechanism convenience wrapper. Wraps one `Arc` and returns it
/// unchanged from every `build` call. Used by OAUTHBEARER static-token,
/// PLAIN, and any mechanism whose plugin holds no per-connection state.
pub struct SharedPluginFactory { /* inner: SaslMechanismPluginHandle */ }

impl SharedPluginFactory {
    pub fn new(plugin: SaslMechanismPluginHandle) -> Self { /* ... */ }
    pub fn into_handle(self) -> SaslMechanismPluginFactoryHandle { /* ... */ }
}

// New error variant for factory failure:
pub enum SaslPluginError {
    // ...existing variants...
    #[error("factory failed to build {mechanism} plugin: {source}")]
    FactoryFailed {
        mechanism: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}
```

**Why `build` is sync, not async:** stateless factories return `Arc::clone` (trivial). `GssapiPluginFactory::build` does microseconds of work (clones `PathBuf`s + constructs an empty `GssapiPlugin::State`); no libgssapi calls happen until the handshake starts. Async plugins (token fetch, OIDC refresh) do their async work inside `initial_payload`, which is already async. An async `build` would force every caller to `.await` for no benefit.

**Why `Debug` supertrait:** `SaslMechanismPlugin` already requires `Debug`, and `SecurityConfig` derives `Debug` — the factory handle must be `Debug` for the derive to compile.

### 2.2b `supports_reauth()` capability flag

```rust
// crates/kafka-backup-core/src/kafka/sasl/plugin.rs  (added to SaslMechanismPlugin)

#[async_trait]
pub trait SaslMechanismPlugin: Send + Sync + Debug {
    // ...existing methods...

    /// Whether this mechanism supports KIP-368 live re-authentication on
    /// the existing connection. Default: `true` (PLAIN, SCRAM,
    /// OAUTHBEARER). Override to `false` when the broker cannot renew
    /// the plugin's authentication state in-place — e.g. `GssapiPlugin`.
    fn supports_reauth(&self) -> bool { true }
}
```

**Why this flag exists.** Apache Kafka does **not** support live KIP-368 re-authentication for the GSSAPI mechanism. Kerberos GSS-API contexts are bound to the wire connection; the broker rejects any in-place `SaslAuthenticate` after the initial handshake with:

```
SaslAuthenticate request received after successful authentication
```

even when `connections.max.reauth.ms > 0` is advertised in the broker's `SaslAuthenticateResponse.session_lifetime_ms`. librdkafka's behaviour (verified via their issue #3304 and source) is to treat `session_lifetime_ms` as a **drain-and-reconnect** timer for GSSAPI rather than a reauth timer: let the session expire naturally, then reconnect on next RPC.

**How the client uses it.** `KafkaClient::authenticate` (client.rs:~335) gates `spawn_reauth_task` on `plugin.supports_reauth()`:

```rust
SaslAuthOutcome::Done => {
    if plugin.supports_reauth() {
        super::sasl::reauth::spawn_reauth_task(
            Arc::downgrade(&self.connection),
            self.correlation_id.clone(),
            plugin.clone(),
            resp.session_lifetime_ms,
        );
    }
    return Ok(());
}
```

`GssapiPlugin` returns `false`. The client no longer schedules a reauth task for GSSAPI; the broker-advertised lifetime becomes a drain window, the session expires, and the next RPC reconnects through the normal auth path. This eliminates the `SaslAuthenticate request received after successful authentication` WARN storm that otherwise appeared every ~48 s per pooled connection.

**Default-true rationale.** PLAIN, SCRAM, and OAUTHBEARER all support live re-authentication on Apache Kafka; keeping the default as `true` preserves the existing scheduler behaviour without requiring downstream plugin authors to opt in. The scheduler's existing "lifetime > 0" early-return still handles brokers that do not advertise reauth at all.

### 2.3 Dispatch integration in `client.rs`

**Structural change:** collapse the four duplicate auth methods into a single unified dispatch. The duplication (C1/C2 in §1.1) is a pre-existing wart; this PR unifies it.

**Before (current):**

```rust
// Initial-connect path (L225)
fn authenticate(&mut self) -> Result<()> {
    match security.sasl_mechanism {
        Some(Plain) => self.sasl_plain_auth().await?,            // L249
        Some(ScramSha256) | Some(ScramSha512) => self.sasl_scram_auth(...).await?,  // L287
        None => { /* no-op */ }
    }
}

// Reconnect path (L462)
fn authenticate_raw(&mut self) -> Result<()> {
    match security.sasl_mechanism {
        Some(Plain) => self.sasl_plain_auth_raw().await?,        // L487
        Some(ScramSha256) | Some(ScramSha512) => self.sasl_scram_auth_raw(...).await?,  // L525
        None => { /* no-op */ }
    }
}
```

**After:**

```rust
/// Unified SASL authentication driver. Called from both initial-connect
/// and reconnect paths. Takes an explicit stream reference so it works in
/// both contexts (where `self.connection` is or isn't yet populated).
async fn authenticate_on(
    &self,
    stream: &mut ConnectionStream,
    security: &SecurityConfig,
) -> Result<AuthOutcome> {
    // Factory branch: takes priority if a factory is configured.
    // Build a fresh plugin bound to this connection's broker endpoint.
    if let Some(factory) = &security.sasl_mechanism_plugin_factory {
        let entry = self.config.bootstrap_servers.first()
            .ok_or_else(|| Error::Config("bootstrap_servers is empty".into()))?;
        let (host, port) = parse_broker_endpoint(entry);
        let plugin = factory.build(&host, port).map_err(|e| {
            Error::Authentication(format!(
                "SASL plugin factory failed for broker {host}:{port}: {e}"
            ))
        })?;
        return self.authenticate_with_plugin(stream, plugin.as_ref()).await;
    }
    // Legacy dispatch: PLAIN / SCRAM / no-auth.
    match security.sasl_mechanism {
        Some(SaslMechanism::Plain) => self.sasl_plain(stream, security).await,
        Some(SaslMechanism::ScramSha256) => self.sasl_scram(stream, security, Sha256).await,
        Some(SaslMechanism::ScramSha512) => self.sasl_scram(stream, security, Sha512).await,
        None => Ok(AuthOutcome::NoAuth),
    }
}

/// Wrapper: initial-connect path. Matches existing behaviour.
async fn authenticate(&mut self) -> Result<()> {
    let security = &self.config.security;
    let mut conn = self.connection.lock().await;
    let bc = conn.as_mut().ok_or(Error::NotConnected)?;
    let outcome = self.authenticate_on(&mut bc.stream, security).await?;
    self.handle_auth_outcome(outcome).await
}

/// Wrapper: reconnect path. Operates on a just-established stream that
/// isn't yet installed in `self.connection`.
async fn authenticate_raw(&self, stream: &mut ConnectionStream) -> Result<AuthOutcome> {
    let security = &self.config.security;
    self.authenticate_on(stream, security).await
}
```

`AuthOutcome` carries the `session_lifetime_ms` so the re-auth scheduler can be spawned by the caller. `handle_auth_outcome` spawns the scheduler when appropriate.

**`authenticate_with_plugin`** is a new private method (~130 LOC):

1. Sends `SaslHandshakeRequest(mechanism = plugin.mechanism_name())` at v1.
2. Reads `SaslHandshakeResponse`; returns `UnsupportedMechanism` error if `error_code != 0`.
3. Calls `plugin.initial_payload()` → sends as `SaslAuthenticateRequest` v2.
4. Loops on response:
   - If `error_code != 0` → `plugin.interpret_server_error(&response.error_message.as_bytes())` and return `Err`.
   - If `auth_bytes` non-empty with `error_code == 0` → `plugin.continue_payload(&auth_bytes)`; if `Some(bytes)`, send another round; if `None`, handshake complete.
   - If `auth_bytes` empty with `error_code == 0` → handshake complete.
5. Captures `session_lifetime_ms` from the final response.
6. Returns `AuthOutcome::Plugin { session_lifetime_ms, plugin: plugin.clone() }` so the caller spawns the re-auth scheduler.

### 2.4 KIP-368 re-authentication driver (OSS)

This is pure Kafka protocol — no auth-specific logic — so it belongs in OSS where every SASL mechanism (including SCRAM, not just plugins) benefits. For this PRD, the scheduler is wired only for plugin-driven auths; legacy PLAIN/SCRAM retain their "authenticate once" behaviour for backwards compatibility.

```rust
// crates/kafka-backup-core/src/kafka/sasl/reauth.rs  (NEW)

pub(crate) struct ReauthScheduler {
    connection: Weak<Mutex<Option<BrokerConnection>>>,
    plugin: Arc<dyn SaslMechanismPlugin>,
    session_lifetime: Duration,
    authenticated_at: Instant,
    reauth_fraction: f64,      // 0.8 default
    min_interval: Duration,    // 30s floor (prevents spam on misconfigured brokers)
    jitter: Duration,          // ±5s (thundering-herd mitigation)
}
```

**Behaviour (verified against KIP-368 spec):**

1. Spawned via `tokio::spawn` when `AuthOutcome::Plugin { session_lifetime_ms > 0 }` is returned.
2. Sleeps until `authenticated_at + max(session_lifetime * reauth_fraction, min_interval) ± jitter`.
3. On wake, upgrades `Weak` to `Arc`; if connection is dropped, task exits silently.
4. Acquires the connection mutex — the **same** `Mutex<Option<BrokerConnection>>` that serializes all Kafka RPC (`client.rs:43`, `client.rs:648`). This satisfies KIP-368's "client cannot queue up additional send requests beyond the one that triggers re-authentication" requirement automatically.
5. Calls `plugin.reauth_payload()`.
6. Sends `SaslHandshakeRequest` + `SaslAuthenticateRequest` on the live stream (KIP-368 uses the same API keys for re-auth as for initial auth).
7. Parses response; updates `authenticated_at`, reschedules.
8. On failure: marks connection unhealthy via the normal `is_connection_error()` path → next caller-initiated request triggers reconnect.
9. Plugin panic is caught via `tokio::task::spawn`'s unwind handling; on panic, same as (8).

**KIP-368 reference notes:**
- `session_lifetime_ms` lives on `SaslAuthenticateResponse` v1+; OSS already uses v2, so the field is always available.
- Value `0` means "no re-auth required"; scheduler is not spawned in that case.
- Value > 0 is the maximum milliseconds the broker will accept before it closes the connection.
- Re-auth uses the **same live socket**; no reconnect, no new TCP.

### 2.5 What OSS does NOT ship

Explicitly out of scope for this PRD:

- **No OAUTHBEARER wire protocol.** RFC 7628 framing (CIR construction, JSON error parsing beyond the fallback default) is plugin-side work.
- **No token types** (`OauthToken`, `OauthTokenProvider` trait).
- **No token cache, providers, or OIDC/MSK IAM wiring.**
- **No AWS runtime dependencies.** `aws-sigv4`, `aws-credential-types`, `aws-config` never appear in OSS runtime deps.
- **No new YAML fields.** `SaslMechanism` enum is unchanged; plugin selection is programmatic only.
- **No re-auth for legacy PLAIN/SCRAM.** Legacy mechanisms retain "authenticate once" behaviour. KIP-368 engagement requires the plugin path. This preserves 100% back-compat.

---

## 3. What Changes in `kafka-backup-core`

### 3.1 New files

```
crates/kafka-backup-core/src/kafka/sasl/
├── mod.rs                    (NEW — module root, re-exports)
├── plugin.rs                 (NEW — trait + error type ~200 LOC)
└── reauth.rs                 (NEW — KIP-368 scheduler ~180 LOC)

crates/kafka-backup-core/tests/
├── sasl_plugin_test.rs       (NEW — rsasl integration + Redpanda E2E)
└── fixtures/
    └── redpanda-oauth.yml    (NEW — Docker Compose for E2E)

crates/kafka-backup-core/examples/
└── custom_sasl_plugin.rs     (NEW — static-token OAUTHBEARER example ~80 LOC)
```

### 3.2 Modified files

| File | Change | Est. LOC |
|---|---|---|
| `src/config.rs` | Add `sasl_mechanism_plugin_factory` field with `#[serde(skip)]` | ~10 |
| `src/kafka/client.rs` | Unify duplicate SASL dispatch; add factory branch (calls `factory.build(host, port)` with `bootstrap_servers[0]`); spawn reauth scheduler | ~200 |
| `src/kafka/mod.rs` | `pub mod sasl;` | ~2 |
| `src/lib.rs` | Re-export `SaslMechanismPlugin`, `SaslMechanismPluginHandle`, `SaslPluginError` at crate root | ~4 |
| `tests/integration_suite_tests.rs` | Add 4 SCRAM/PLAIN baseline tests (commit 0) | ~250 |
| `Cargo.toml` | `[dev-dependencies]` add `rsasl = "2"`, `testcontainers-modules` Kafka feature | ~3 |
| `CHANGELOG.md` | `## Unreleased` / `### Added` entry | ~3 |
| `docs/PRD-sasl-mechanism-plugin.md` | This document | — |

**Total net new code: ~650 LOC** (trait + scheduler + dispatch refactor + tests + example).
**New runtime dependencies: ZERO.** (Uses existing `async-trait`, `thiserror`, `tokio`, `serde`/`serde_json`.)
**New dev-dependencies: TWO** (`rsasl`, `testcontainers-modules` Kafka feature — tests only, no production footprint).

### 3.3 Backwards compatibility

- Existing configs (PLAIN / SCRAM-* / mTLS / plaintext) are byte-for-byte unchanged in behaviour — verified by the new baseline integration tests landing in commit 0 *before* the refactor.
- `SaslMechanism` enum untouched. No new YAML fields.
- `#[serde(skip)]` on the new field means existing YAML deserialises into `sasl_mechanism_plugin: None`, preserving the legacy dispatch path.
- The refactor of `client.rs` SASL dispatch (collapsing the duplicated methods) is a pure internal cleanup — no behaviour change, proven by the baseline tests.
- No public API removed or renamed. Three new public items added: `SaslMechanismPlugin`, `SaslMechanismPluginHandle`, `SaslPluginError`.

---

## 4. Verification

### 4.1 Unit tests (`crates/kafka-backup-core/src/kafka/sasl/*.rs`)

14 cases, all `#[tokio::test]`, `tokio::time::pause()` where time-based:

| # | Test | File | Asserts |
|---|---|---|---|
| U1 | `plugin_trait_default_continue_payload_returns_none` | `plugin.rs` | Single-round mechanisms work with no override. |
| U2 | `plugin_trait_default_reauth_calls_initial` | `plugin.rs` | Default `reauth_payload` delegates to `initial_payload`. |
| U3 | `interpret_server_error_parses_rfc7628_json` | `plugin.rs` | `{"status":"invalid_token","scope":"kafka.topic"}` → `ServerRejected` with `invalid_token (scope="kafka.topic")`. |
| U4 | `interpret_server_error_falls_back_to_utf8_on_nonjson` | `plugin.rs` | `b"Authentication failed"` → `ServerRejected` with that text (Kafka 3.5+ reality). |
| U5 | `interpret_server_error_handles_empty_payload` | `plugin.rs` | Empty bytes → `ServerRejected` with empty detail; no panic. |
| U6 | `dispatch_single_round_success` | `client.rs` (test mod) | Mock stream: SaslHandshake + one SaslAuthenticate; `authenticate_with_plugin` returns `Ok(AuthOutcome::Plugin { .. })`. |
| U7 | `dispatch_multi_round_success` | `client.rs` (test mod) | Mock plugin returns `Some(bytes)` on first `continue_payload` then `None`; two SaslAuthenticate exchanges occur. |
| U8 | `dispatch_server_error_routes_to_interpret` | `client.rs` (test mod) | `error_code = SASL_AUTHENTICATION_FAILED (58)` → `interpret_server_error` called; returned error propagates. |
| U9 | `dispatch_plugin_none_preserves_legacy_scram` | `client.rs` (test mod) | `sasl_mechanism_plugin_factory = None`, SCRAM config → legacy `sasl_scram` path invoked (regression guard). |
| U10 | `reauth_fires_at_80_percent` | `reauth.rs` | `session_lifetime_ms = 10_000`; with paused time, `reauth_payload` called between t=7.5s and t=8.5s (accounts for ±5s jitter with lower bound respected). |
| U11 | `reauth_respects_min_interval_floor` | `reauth.rs` | `session_lifetime_ms = 1_000` (broker misconfigured) → scheduler waits 30s floor minimum, not 800 ms. |
| U12 | `reauth_exits_on_connection_drop` | `reauth.rs` | Drop the `Arc<Mutex<Option<BrokerConnection>>>`; scheduler task observes `Weak::upgrade()` = None on wake; exits within 1 tick. |
| U13 | `reauth_failure_marks_connection_unhealthy` | `reauth.rs` | Plugin `reauth_payload` returns `Err`; assert `BrokerConnection` flagged so next send triggers reconnect via existing path. |
| U14 | `security_config_serde_skips_plugin_factory_field` | `config.rs` (test mod) | `serde_yaml::to_string(&SecurityConfig { sasl_mechanism_plugin_factory: Some(factory), .. })` → output YAML has no `sasl_mechanism_plugin_factory` key. Roundtrip back → field is `None`. |
| U15 | `factory_receives_per_broker_endpoint` | `tests/integration_suite/sasl_plugin_mock_tests.rs` | A capturing factory asserts `build(host, port)` is invoked exactly once per `KafkaClient::authenticate` with the configured bootstrap endpoint. This is the regression gate for Bug 1 (per-broker SPN); when `PartitionLeaderRouter` rewrites `bootstrap_servers` to the advertised broker host, that host flows into the factory. |
| U16 | `shared_plugin_factory_build_returns_same_arc` | `plugin.rs` | `SharedPluginFactory::build(...)` returns an `Arc` pointer-equal to the wrapped plugin on every call, regardless of host/port — proves the stateless convenience wrapper is just an `Arc::clone`. |
| U17 | `gssapi_factory_builds_plugin_with_broker_specific_host` | `gssapi.rs` | `GssapiPluginFactory::build("broker-2.fqdn", 9094)` produces a `GssapiPlugin` whose internal hostname matches the argument — proves per-broker SPN binding at `.build()` time. |
| U18 | `default_supports_reauth_is_true` | `plugin.rs` | Default trait impl returns `true` so PLAIN/SCRAM/OAUTHBEARER keep scheduling KIP-368 reauth without overriding. |
| U19 | `gssapi_plugin_opts_out_of_reauth` | `gssapi.rs` | `GssapiPlugin::supports_reauth()` returns `false` — Apache Kafka rejects in-place reauth for GSSAPI, so the client must not schedule a reauth task. |
| U20 | `reauth_scheduler_not_spawned_when_plugin_opts_out` | `tests/integration_suite/sasl_plugin_mock_tests.rs` | End-to-end over `MockKafkaBroker`: plugin with `supports_reauth() = false` + `session_lifetime_ms = 60_000`; virtual time advances past the 80 % deadline; asserts `reauth_payload` never called and the mock sees exactly one `SaslAuthenticate` frame. |
| U21 | `pool_produces_distinct_plugin_per_kafkaclient` | `tests/integration_suite/sasl_plugin_mock_tests.rs` | N=3 separate `MockKafkaBroker`s + N `KafkaClient`s sharing one factory; asserts `build` is called once per client with the correct endpoint, and each returned plugin `Arc` is pointer-distinct. Regression gate for "pool-isolation latent risk removed". |

**Coverage target: ≥ 90% line coverage** on `src/kafka/sasl/*.rs` (measured via `cargo llvm-cov`).

### 4.2 Integration tests (`crates/kafka-backup-core/tests/`)

| # | Test | File | Mode | Shape |
|---|---|---|---|---|
| I1 | `sasl_plain_baseline` | `integration_suite_tests.rs` | CI | testcontainers Kafka, SASL_SSL + PLAIN. Connect → produce 100 messages → fetch → disconnect. |
| I2 | `sasl_scram_sha512_baseline` | `integration_suite_tests.rs` | CI | Same shape, SASL_SSL + SCRAM-SHA-512. |
| I3 | `sasl_scram_sha256_baseline` | `integration_suite_tests.rs` | CI | Same shape, SASL_SSL + SCRAM-SHA-256. |
| I4 | `sasl_plain_survives_broker_restart` | `integration_suite_tests.rs` | CI | SASL_SSL + PLAIN. Produce 10, restart broker container, produce 10 more. Reconnect + reauth via current (non-KIP-368) path. |
| I5 | `plugin_dispatch_via_mock_rsasl_server` | `sasl_plugin_test.rs` | CI | `rsasl` mock server, test-fixture plugin with 10 s session lifetime, `tokio::time::pause()` not used (real clock), assert 3 reauths, zero disconnects, all produces round-trip. |
| I6 | `plugin_dispatch_real_broker_oauthbearer` | `sasl_plugin_test.rs` | **`#[ignore]`** | Docker Redpanda configured for OAUTHBEARER unsecured-JWT mode + static-token plugin; produce 10 + fetch 10; assert one successful handshake + session_lifetime captured. README documents local-run command. |

**Why I1-I4 before the refactor:** OSS has zero SASL tests today. "Existing tests catch regressions" was a false claim in v1.0. These four tests land in **commit 0** so every subsequent commit has a binary regression check. Estimated ~250 LOC using the `tls_tests.rs` pattern (Apache Kafka + SASL_SSL listener + testcontainers).

**Mock server (shipped as a hand-rolled scripted mock, not `rsasl`):** `tests/integration_suite/sasl_mock_broker.rs` (~240 LOC) speaks Kafka wire frames via the existing `kafka-protocol = "0.17"` dev-dep. It accepts one connection, processes a scripted sequence of `Exchange`s (handshake/auth-success/auth-error) against incoming requests, and captures what the client sent for assertion. Outcome: zero new dev-deps added (better than v2.0's `rsasl` target). Tests that would have been I5 ship as `tests/integration_suite/sasl_plugin_mock_tests.rs` — `dispatch_single_round_success`, `dispatch_multi_round_success`, `dispatch_server_error_routes_to_interpret`, and `plugin_reauth_fires_at_80_percent_via_scheduler`.

**E2E broker (shipped as Confluent cp-kafka 7.7.0, not Redpanda):** Redpanda lacks a simple unsecured-JWS OAUTHBEARER validator; cp-kafka bundles Apache's `OAuthBearerUnsecuredValidatorCallbackHandler`, which is the canonical test-mode documented by Apache. The E2E fixture lives at `tests/sasl-oauth-test-infra/{docker-compose-oauth.yml,jaas.conf,README.md}` and runs on port 9097. Developers start it locally with `docker compose -f tests/sasl-oauth-test-infra/docker-compose-oauth.yml up -d` and run the `#[ignore]` test at `tests/integration_suite/sasl_oauth_tests.rs`. CI skips via `#[ignore]`.

### 4.3 Regression matrix ("don't break anything")

| Config | Pre-PR state | Post-PR state | Gated by |
|---|---|---|---|
| PLAINTEXT, no SASL | pass (existing `integration_suite_tests`) | must pass | existing suite |
| SSL, no SASL | pass (existing `tls_tests`) | must pass | existing `tls_tests` |
| SASL_PLAINTEXT + PLAIN | **untested** | pass | **I1 (new commit 0)** |
| SASL_SSL + PLAIN | **untested** | pass | I1 variant (new commit 0) |
| SASL_SSL + SCRAM-SHA-256 | **untested** | pass | **I3 (new commit 0)** |
| SASL_SSL + SCRAM-SHA-512 | **untested** | pass | **I2 (new commit 0)** |
| mTLS (SSL + SSL cert auth) | pass (existing `tls_tests`) | must pass | existing `tls_tests` |
| Plugin path, `plugin = None` | N/A | pass | U9 |
| Plugin path, plugin set | N/A | pass | U6-U8, I5, I6 |

No row regresses. The untested-today rows are covered by the new baseline.

### 4.4 Non-functional gates

Required for PR merge:

- `cargo clippy -p kafka-backup-core --all-targets -- -D warnings` — **zero warnings**.
- `cargo fmt --all -- --check` — clean.
- `cargo deny check` — no new advisories, no new licence issues.
- `cargo doc -p kafka-backup-core --no-deps --all-features` — `SaslMechanismPlugin` public-docs page renders with contract + example.
- `cargo test -p kafka-backup-core` — all tests pass; minimum 14 new unit tests + 4 new integration tests passing (I6 may be skipped via `#[ignore]` in CI).
- **Binary size check** — `cargo bloat --release -p kafka-backup-cli --crates | head -20` diff pasted to PR. **Target: zero new top-level crates, ≤ 50 KB binary delta.**
- **Dependency tree check** — `cargo tree -p kafka-backup-core --edges no-dev --depth 1 | wc -l` before and after. **Must match exactly** (runtime deps unchanged).

### 4.5 End-to-end verification recipe

Run as a single script before PR review:

```bash
# 0. Pre-flight: current OSS state green
cd /Users/sionsmith/development/kafka-backup
cargo test -p kafka-backup-core

# 1. After commit 0 (SCRAM/PLAIN baseline): 4 new integration tests pass
cargo test -p kafka-backup-core --test integration_suite_tests -- sasl_

# 2. After commit 1 (refactor): same 4 tests still pass — proves no behavioural drift
cargo test -p kafka-backup-core --test integration_suite_tests -- sasl_

# 3. After commit 2 (plugin trait): unit tests + rsasl integration pass
cargo test -p kafka-backup-core sasl::
cargo test -p kafka-backup-core --test sasl_plugin_test -- --skip real_broker

# 4. After commit 3 (reauth scheduler): scheduler tests pass (paused-time assertions)
cargo test -p kafka-backup-core sasl::reauth::

# 5. E2E: Redpanda + OAUTHBEARER static-token + example plugin
docker compose -f crates/kafka-backup-core/tests/fixtures/redpanda-oauth.yml up -d
cargo test -p kafka-backup-core --test sasl_plugin_test plugin_dispatch_real_broker_oauthbearer -- --ignored --nocapture
docker compose -f crates/kafka-backup-core/tests/fixtures/redpanda-oauth.yml down

# 6. Size + deps
cargo bloat --release -p kafka-backup-cli --crates | head -20
cargo tree -p kafka-backup-core --edges no-dev --depth 1 > /tmp/deps-after.txt
# Compare /tmp/deps-before.txt captured before the PR

# 7. Downstream: enterprise compiles against the new trait
cd /Users/sionsmith/development/kafka-backup-enterprise
cargo check -p kafka-backup-enterprise-core
```

**Step 7 is the canary**: if the enterprise crate (which is the real consumer) compiles cleanly against the new trait without requiring changes to the trait surface, the contract is stable. If it doesn't, the trait needs iteration before merge — better to find that here than after OSS ships a v0.14.0 API we have to deprecate.

### 4.6 Manual verification checklist (maintainer smoke before merge)

- [ ] `cargo clippy`, `cargo fmt --check`, `cargo deny check` all green.
- [ ] `cargo doc` produces a `SaslMechanismPlugin` public page with the contract block rendered.
- [ ] `examples/custom_sasl_plugin.rs` compiles. Example is referenced from the trait's rustdoc.
- [ ] Binary-size delta screenshotted in the PR description.
- [ ] `cargo tree` dependency list unchanged at runtime.
- [ ] Commit 0 lands before commit 1; every subsequent commit keeps I1-I4 green.
- [ ] Enterprise `cargo check` succeeds against the path-dep (via `git checkout` of both repos locally).

---

## 5. Rollout

### 5.1 Delivery shape

**One OSS PR. Six commits, stacked:**

| # | Commit | Est. LOC | Verifies |
|---|---|---|---|
| 0 | `test(kafka): SCRAM/PLAIN integration baseline` | ~250 | I1-I4 green; establishes the regression gate. |
| 1 | `refactor(kafka): unify SASL authenticate/authenticate_raw dispatch` | ~100 (net −50 after deduplication) | I1-I4 still green; no behaviour change. |
| 2 | `feat(kafka): add SaslMechanismPlugin extension trait` | ~200 | U1-U9 green; plugin branch routes correctly. |
| 3 | `feat(kafka): add KIP-368 re-authentication scheduler` | ~200 | U10-U13 green; I5 (rsasl mock reauth loop) green. |
| 4 | `test(kafka): SaslMechanismPlugin integration + Redpanda E2E` | ~200 | I5 and I6 green; coverage ≥ 90% on new modules. |
| 5 | `docs(kafka): custom SASL plugin example + CHANGELOG + v2 PRD` | ~150 | `cargo doc`, `cargo deny`, `cargo bloat` all green; example runs in CI. |

### 5.2 Versioning

- `kafka-backup-core` 0.14.0 shipped the initial `SaslMechanismPlugin` extension trait.
- `kafka-backup-core` 0.15.0 (unreleased, this PR) reshapes the extension point to `SaslMechanismPluginFactory` and adds in-tree GSSAPI under the `gssapi` Cargo feature. Because 0.15.0 is still in-flight on `feat/sasl-mechanism-plugin` and no external consumers exist, the factory revision folds into the same version — no 0.16.0 bump.
- Public items added across 0.14.0 + 0.15.0: `SaslMechanismPlugin`, `SaslMechanismPluginHandle`, `SaslPluginError`, `SaslMechanismPluginFactory`, `SaslMechanismPluginFactoryHandle`, `SharedPluginFactory`, `GssapiPluginFactory` (feature-gated), `SaslAuthOutcome`.
- Removed before first release: `SecurityConfig::sasl_mechanism_plugin` (replaced by `sasl_mechanism_plugin_factory`). Net zero breakage for consumers.
- `SaslMechanism` enum untouched. No new YAML fields.

### 5.3 Downstream consumers

`kafka-backup-enterprise-core` retargets `kafka-backup-core = "0.14"` (or path-dep during integration) and implements the OAUTHBEARER plugin internally. See `docs/msk-oauthbearer-enterprise-plan.md` in the enterprise repo for the full Phase 1 scope (~5 engineer-days) that unblocks live MSK IAM migrations.

**Wall-clock from this PRD approval to first live MSK migration:**
- OSS PR: 5 engineer-days (this PRD).
- Enterprise P1: 5 engineer-days (static token provider + MSK IAM provider + trait impl + wire-up).
- **Total: ~2 calendar weeks** assuming single engineer, no blockers.

### 5.4 Schedule (single engineer)

| Day | Work |
|---|---|
| 1 | Commit 0: 4 testcontainers-based SCRAM/PLAIN integration tests. Green CI. |
| 2 | Commit 1 (morning): unify dispatch, I1-I4 stay green. Commit 2 (afternoon): plugin trait + `SecurityConfig` field + dispatch branch. |
| 3 | Commit 3: reauth scheduler + U10-U13. |
| 4 | Commit 4: rsasl integration test + Redpanda fixture + I5/I6. |
| 5 | Commit 5: example + CHANGELOG + PRD cross-refs + size/dep checks + PR review prep. |

---

## 6. Risks

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | Commit 1 refactor introduces subtle behaviour change vs current duplicated dispatch | Medium | High | Commit 0 (baseline) lands first; commit 1 must keep I1-I4 green. Any delta forces commit 1 back to drawing board without polluting the plugin work. |
| R2 | `rsasl` crate abandoned / unmaintained between now and implementation | Low | Low | Verify last-publish + download count as first action of commit 4. Fallback: ~150-LOC hand-rolled mock SASL server. Cost: +0.5 engineer-day if triggered. |
| R3 | Re-auth scheduler races with in-flight produces, corrupting responses | Low | High | Uses existing `Arc<Mutex<Option<BrokerConnection>>>` at `client.rs:43`. This is the same mutex serializing all Kafka RPC, so re-auth is atomically interleaved. KIP-368's "client serializes" requirement is satisfied for free. Tested by U10 + I5. |
| R4 | Plugin panic poisons scheduler task | Low | Medium | `tokio::spawn`'d task's panic is isolated; on `JoinError`, mark connection unhealthy and let the reconnect path handle recovery. Same failure mode as a transient broker issue. |
| R5 | Kafka broker returns non-JSON `error_message` (Kafka 3.5+ reality, see §2.1 Q1 resolution) | Known | Low | `interpret_server_error` default explicitly handles both JSON (RFC 7628) and UTF-8 text. Tested by U3+U4. Documented in trait rustdoc. |
| R6 | Plugin calls back into `KafkaClient` during handshake → deadlock on connection mutex | Low | High | Trait contract documentation explicitly forbids re-entry. Debug-build assertion: `try_lock` in the scheduler with 5s timeout + panic with diagnostic message rather than hanging forever. Release builds retain blocking `lock`. |
| R7 | Binary-size creep from dev-deps (`rsasl`, `testcontainers-modules`) bleeds into production build | None | None | `[dev-dependencies]` only. `cargo bloat` diff in §4.5 catches any accidental promotion to `[dependencies]`. |
| R8 | Enterprise plugin written against trait before trait is stable, requires churn | Low | Medium | Land OSS first; enterprise P1 starts after OSS PR merges and v0.14.0 is published (or path-dep is verified via §4.5 step 7). Trait uses default methods for forward compat; no `#[non_exhaustive]` needed. |
| R9 | Broker advertises mechanism but rejects our plugin's initial payload with a vague error | Medium | Low | Wrap the handshake result with mechanism name + broker advertised list on error to aid debugging. Errors returned to caller always include `mechanism` field. |
| R10 | `#[serde(skip)]` on trait object causes serde compile errors on certain feature flag combinations | Low | Low | Field type is `Option<Arc<dyn Trait>>` with `Default` impl returning `None`. Verified compilable with current serde usage patterns. Proof-of-concept validation in commit 2. |

---

## 7. Alternatives Considered

### 7.1 Full OAUTHBEARER in OSS

Rejected after initial proposal. Drags `aws-sigv4` / `aws-credential-types` / `aws-config` into OSS as feature-gated deps. Even behind a feature flag, the maintenance burden of shipping, testing, and supporting four token providers (static, file, OIDC, MSK IAM) is too high for code whose only paying users are the MSK migrator and Confluent Cloud users.

### 7.2 Fork `KafkaClient` in enterprise

Rejected. `KafkaClient` + `PartitionLeaderRouter` are ~2000 LOC of battle-tested protocol code. Forking in enterprise doubles forever-maintenance, and every future OSS protocol improvement requires manual merging.

### 7.3 Raw `AsyncReadWrite` hook instead of plugin trait

Considered: let plugins drive the socket directly rather than return bytes. Rejected because it forces every plugin to know Kafka's request framing (correlation IDs, api versions, header format). The plugin trait as proposed gives plugins the minimum surface they need — SASL mechanism bytes — and keeps Kafka protocol details in OSS where they belong.

### 7.4 Standalone `kafka-backup-sasl` crate

Considered splitting the trait into its own published crate so plugins depend on something smaller than all of `kafka-backup-core`. Rejected: the trait is ~30 LOC; splitting it into a crate adds release coordination and version-skew risk. Plugins depending on `kafka-backup-core` directly is fine — they were going to anyway for protocol types.

### 7.5 Use librdkafka-style C callback signature

Considered mirroring librdkafka's `oauthbearer_token_refresh_cb`: single function pointer, no object state. Rejected: idiomatic Rust uses trait objects for this kind of polymorphism; C-style callbacks force `fn`-pointer + `*mut c_void` ergonomics that feel wrong in async Rust and preclude holding state (token cache, HTTP client) cleanly.

### 7.6 Pre-populate all four providers (static, file, OIDC, MSK IAM) in OSS behind a feature flag

Considered. Rejected: features are cumulative in workspace builds; a single dependent crate enabling `msk-iam` feature drags AWS SDKs into everyone's build. Feature flags don't provide the isolation we need; downstream-crate delegation does.

---

## 8. Open Questions

All resolved in v2.0.

1. **Q1:** Should the `SaslMechanismPlugin` trait be marked `#[non_exhaustive]`?
   **Resolved: NO.** `#[non_exhaustive]` has semantic effect only on structs, enums, and variants in Rust — not on traits. Default method implementations already provide forward-compat for downstream `impl` blocks.

2. **Q2:** Should the trait expose the broker's advertised SASL mechanisms so plugins can negotiate?
   **Resolved: NO.** Plugins implement a specific mechanism; mismatch with broker advertisement surfaces naturally via `SaslHandshakeResponse.error_code = UNSUPPORTED_SASL_MECHANISM`. Adding the list to the trait would inflate the surface and invite feature creep.

3. **Q3:** Should we ship a reference-plugin example?
   **Resolved: YES.** `examples/custom_sasl_plugin.rs` implements a static-token OAUTHBEARER plugin (~80 LOC). Compiles in CI via `cargo build --examples`. Referenced from the trait's rustdoc for documentation discoverability.

---

## 9. References

### KIPs
- **KIP-43** — Kafka SASL enhancements — https://cwiki.apache.org/confluence/display/KAFKA/KIP-43
- **KIP-84** — Support SASL/SCRAM mechanisms — https://cwiki.apache.org/confluence/display/KAFKA/KIP-84
- **KIP-152** — Improve diagnostics for SASL authentication failures (SaslHandshake v1) — https://cwiki.apache.org/confluence/display/KAFKA/KIP-152
- **KIP-255** — SASL Authentication via OAuth Bearer tokens — https://cwiki.apache.org/confluence/display/KAFKA/KIP-255
- **KIP-368** — Allow SASL Connections to Periodically Re-Authenticate — https://cwiki.apache.org/confluence/display/KAFKA/KIP-368

### RFCs
- **RFC 7628** — A Set of Simple Authentication and Security Layer (SASL) Mechanisms for OAuth — https://datatracker.ietf.org/doc/html/rfc7628
- **RFC 6749** — The OAuth 2.0 Authorization Framework (client-credentials flow for OIDC providers) — https://datatracker.ietf.org/doc/html/rfc6749

### Existing docs
- `kafka-backup-enterprise/docs/msk-oauthbearer-enterprise-plan.md` — full OAUTHBEARER implementation consuming this extension trait.
- `kafka-backup-core/src/kafka/client.rs` — current SASL dispatch implementation (file:line anchors in §1.1).

---

## 10. Appendix — Example Downstream Plugin (Sketch)

```rust
// Illustrative: this is what kafka-backup-enterprise-core implements.
// Included here purely to ground the trait contract.

use kafka_backup_core::{SaslMechanismPlugin, SaslPluginError};
use std::sync::Arc;
use std::collections::BTreeMap;

#[derive(Debug)]
pub struct OauthBearerPlugin {
    token_provider: Arc<dyn OauthTokenProvider>,
    extensions: BTreeMap<String, String>,
}

#[async_trait::async_trait]
impl SaslMechanismPlugin for OauthBearerPlugin {
    fn mechanism_name(&self) -> &str { "OAUTHBEARER" }

    async fn initial_payload(&self) -> Result<Vec<u8>, SaslPluginError> {
        let token = self.token_provider.get_token()
            .await
            .map_err(|e| SaslPluginError::CredentialError(e.to_string()))?;
        Ok(build_rfc7628_cir(token.value(), &self.extensions))
    }

    // interpret_server_error — use the default (handles both JSON and text).
    // reauth_payload — use the default (delegates to initial_payload, which
    // transparently refreshes via the token cache).
}

fn build_rfc7628_cir(token: &str, ext: &BTreeMap<String, String>) -> Vec<u8> {
    // n,a=,<0x01>[key=value<0x01>]*auth=Bearer <token><0x01><0x01>
    let mut bytes = b"n,a=,\x01".to_vec();
    for (k, v) in ext {
        bytes.extend_from_slice(k.as_bytes());
        bytes.push(b'=');
        bytes.extend_from_slice(v.as_bytes());
        bytes.push(0x01);
    }
    bytes.extend_from_slice(b"auth=Bearer ");
    bytes.extend_from_slice(token.as_bytes());
    bytes.push(0x01);
    bytes.push(0x01);
    bytes
}
```

The OAUTHBEARER-specific logic (RFC 7628 framing, token provider trait, token cache, MSK IAM sigv4 presigning, OIDC flow) all lives in `kafka-backup-enterprise-core`. None of it is visible to this OSS crate.

---

## 10.a In-tree GSSAPI (Kerberos) plugin — feature-gated

Unlike OAUTHBEARER/MSK IAM (which live in the enterprise crate), GSSAPI is
in-tree under a `gssapi` Cargo feature. Rationale: Kerberos is a legitimate
on-prem requirement, not cloud-vendor lock-in, and the reference impl proves
the trait scales beyond OAUTH-style mechanisms without altering the core
dispatch.

**Crate surface** (default build is unchanged):

```toml
# workspace root Cargo.toml
[workspace.dependencies]
libgssapi = { version = "0.9", optional = true }

# crates/kafka-backup-core/Cargo.toml
[features]
default = []
gssapi = ["dep:libgssapi"]

# crates/kafka-backup-cli/Cargo.toml
[features]
default = []
gssapi = ["kafka-backup-core/gssapi"]
```

**Build requirements** — the feature links MIT krb5 at build time:

- macOS: `brew install krb5` + export
  `PKG_CONFIG_PATH="$(brew --prefix krb5)/lib/pkgconfig:…"` (Apple's bundled
  Heimdal does not expose the symbols `libgssapi 0.9` links against).
- Debian/Ubuntu: `apt-get install libkrb5-dev`.
- Fedora/RHEL: `dnf install krb5-devel`.

**Config shape** (always present in `SaslMechanism`; runtime error if the
binary was compiled without `--features gssapi`):

```yaml
source:
  bootstrap_servers: ["kafka.prod.example.com:9094"]
  security_protocol: SASL_PLAINTEXT  # or SASL_SSL for over-TLS
  sasl_mechanism: GSSAPI
  sasl_kerberos_service_name: kafka
  sasl_keytab_path: /etc/kafka-backup/client.keytab
  sasl_krb5_config_path: /etc/kafka-backup/krb5.conf
```

**Handshake mapping** (`crates/kafka-backup-core/src/kafka/sasl/gssapi.rs`):

- `initial_payload()` → first `gss_init_sec_context` token (no server input).
- `continue_payload(server_bytes)` → steps the context machine. On
  `gss_init_sec_context` returning `None` (established), it emits an empty
  client token which prompts the broker's Phase 2 wrapped security-layer
  proposal. The next call unwraps that proposal, verifies the `0x01` no-layer
  bit, wraps a `{layer=0x01, max_size=0, authz_id=""}` reply, and returns it.
  Subsequent calls with any server bytes transition to `Done`.
- `interpret_server_error` → default (handles RFC 7628 JSON + free-form UTF-8;
  GSS minor-status strings are surfaced unchanged).
- `supports_reauth()` → **`false`** (§2.2b). Apache Kafka rejects in-place
  `SaslAuthenticate` for GSSAPI after the initial handshake, so the client
  does not schedule a KIP-368 reauth task; the broker-advertised
  `session_lifetime_ms` is treated as a drain-and-reconnect window.
- `reauth_payload()` → retained for symmetry and for brokers that might
  reach the plugin via a different control path in future: rebuilds a
  fresh `ClientCtx` (tickets expire — can't reuse stale context) and
  returns the new Phase 1 initial token. In practice, this method is not
  called because `supports_reauth()` is `false`.

**Factory surface.** `GssapiPluginFactory` is the public entry point and what
`kafka-backup-cli` installs. It eagerly validates the keytab at construction
time (same behaviour as pre-factory `GssapiPlugin::new`), and its `build(host, port)`
method creates a fresh `GssapiPlugin` bound to that broker's hostname each call.
This is what fixes the multi-broker SPN and connection-pool reauth correctness
bugs.

**Env-var serialisation** — `libgssapi 0.9.1` does not expose a keytab-aware
`Cred::acquire` API; paths are supplied via `KRB5_CLIENT_KTNAME` /
`KRB5_CONFIG`. The plugin holds a process-wide
`tokio::sync::Mutex<()>` across credential acquisition so parallel
`KafkaClient` instances do not race each other's env mutation. Once the
cred handle exists, it caches the paths internally — subsequent context
steps do not re-read the env.

**Operational caveats**:

1. Multi-broker Kerberos is **supported by design** as of 0.15.0 via the
   factory extension point (§2.2a). `GssapiPluginFactory::build` is invoked
   once per `KafkaClient`, and `PartitionLeaderRouter` rewrites
   `bootstrap_servers[0]` to the advertised per-broker `host:port` from
   `MetadataResponse` before cloning the config, so each connection
   authenticates against its own SPN (`kafka/brokerN.fqdn@REALM`).
   End-to-end proof against a real multi-broker Kerberized cluster is
   tracked as a follow-up (2-broker `docker-compose-gssapi.yml` fixture);
   the factory-dispatch contract and pool-isolation guarantee are covered
   today by the mock-broker `factory_receives_per_broker_endpoint` and
   `pool_produces_distinct_plugin_per_kafkaclient` tests.
2. **Live re-authentication is disabled for GSSAPI by design** (§2.2b).
   Apache Kafka rejects in-place `SaslAuthenticate` for Kerberos
   connections, so the client does not schedule a KIP-368 reauth task;
   the broker-advertised `session_lifetime_ms` acts as a
   drain-and-reconnect window, matching librdkafka / JVM-client
   behaviour. The session expires naturally and the next RPC reconnects
   through the normal auth path.
3. Default release binaries and the default Docker image do not include
   GSSAPI. Build your own with `--features gssapi` and matching runtime
   `libkrb5-3` install, or wait for a `Dockerfile.gssapi` variant.

---

## 11. Sign-off

Approval required from:
- [ ] `kafka-backup-core` maintainer (OSS): API shape, dispatch refactor, re-auth scheduler.
- [ ] `kafka-backup-enterprise-core` owner: trait contract is expressive enough for the enterprise P1 OAUTHBEARER impl.
- [ ] Security / release lead: zero new runtime deps, binary-size delta ≤ 50 KB, dev-dep choice of `rsasl` is acceptable.

Post-approval, implementation proceeds per §5.4 schedule.
