//! KIP-368 SASL re-authentication scheduler.
//!
//! When a broker advertises a non-zero `session_lifetime_ms` in the
//! terminal `SaslAuthenticateResponse`, the client has a limited window
//! to re-authenticate before the broker closes the connection. This
//! module implements that scheduler.
//!
//! ## Timing
//!
//! We reauth at 80 % of the advertised lifetime, minus a small negative
//! jitter (up to 5 s) to avoid a thundering herd when many connections
//! authenticate at the same instant. A 30 s floor prevents a
//! misconfigured broker from spinning the scheduler at sub-second
//! intervals. These are [`ReauthTiming::DEFAULT`]; tests inject shorter
//! values through `KafkaClient::with_reauth_timing`.
//!
//! ## Wire protocol
//!
//! Per KIP-368, re-authentication sends **only** `SaslAuthenticate`
//! frames; there is no fresh `SaslHandshake`. The mechanism is already
//! bound to the connection from the initial handshake.
//!
//! ## Lifecycle
//!
//! The task holds a `Weak` reference to the shared connection slot and
//! the generation of the connection it was spawned for. Every connect
//! and reconnect gives the new `BrokerConnection` a fresh generation from
//! a counter on the `KafkaClient`. After each sleep the task locks the
//! slot and exits if the client was dropped, the slot is empty, or the
//! slot now holds a different connection (generation mismatch), so a
//! task can never re-authenticate a connection it was not spawned for.
//! The connection also keeps the task's `AbortHandle` and aborts it when
//! it is dropped or replaced, so a reconnect retires the old task at once
//! rather than at its next wake-up: there is at most one task per live
//! connection.
//!
//! On re-auth failure (an I/O error, or a broker rejection such as
//! "Cannot change principals during re-authentication") the task clears
//! the slot, under the same lock and only if the generation still
//! matches. The next caller into `send_request` then sees `NotConnected`
//! and reconnects through the normal path, which performs a full SASL
//! handshake again.

use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::{ApiKey, SaslAuthenticateRequest, SaslAuthenticateResponse};
use tokio::sync::Mutex;
use tokio::task::AbortHandle;
use tracing::{debug, trace, warn};

use super::plugin::{SaslAuthOutcome, SaslMechanismPluginHandle};
use crate::kafka::client::{encode_kafka_request, send_rpc_on_stream, BrokerConnection};

/// `SaslAuthenticate` wire version. Must match the version used by the
/// initial handshake path in `KafkaClient::sasl_plugin_auth` (v2 per
/// KIP-368, which carries `session_lifetime_ms` on the response).
const SASL_AUTHENTICATE_VERSION: i16 = 2;

/// When to re-authenticate, relative to the broker-advertised session
/// lifetime. Production uses [`ReauthTiming::DEFAULT`]; tests shorten
/// the floor so they do not wait 30 s.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ReauthTiming {
    /// Fraction of `session_lifetime_ms` after which we re-authenticate.
    /// Matches librdkafka's `sasl.oauthbearer.token.refresh.ms` default
    /// ratio.
    pub(crate) fraction: f64,
    /// Lower bound on the computed delay. Guards against brokers that
    /// advertise a very short session lifetime and would otherwise make
    /// the scheduler fire constantly.
    pub(crate) min_interval: Duration,
    /// Maximum negative jitter subtracted from the computed delay, to
    /// spread simultaneous re-auths. Subtracted only (never added) so we
    /// never overshoot the broker's expiry.
    pub(crate) max_jitter: Duration,
}

impl ReauthTiming {
    /// 80 % of the lifetime, at least 30 s, minus up to 5 s of jitter.
    pub(crate) const DEFAULT: Self = Self {
        fraction: 0.8,
        min_interval: Duration::from_secs(30),
        max_jitter: Duration::from_secs(5),
    };

    /// Delay before the next re-auth for a broker-advertised
    /// `session_lifetime_ms`, or `None` when the broker advertised no
    /// lifetime (0 or negative), meaning re-auth is disabled.
    ///
    /// When `lifetime * fraction - jitter` falls below `min_interval`,
    /// the floor wins. On a pathologically short lifetime the floor may
    /// exceed the lifetime itself; that is intentional: the next RPC on
    /// the connection sees the broker-initiated close and reconnects
    /// through the normal path, which does a full re-handshake.
    pub(crate) fn delay(&self, session_lifetime_ms: i64) -> Option<Duration> {
        if session_lifetime_ms <= 0 {
            return None;
        }
        let lifetime = Duration::from_millis(session_lifetime_ms as u64);
        let scaled = lifetime.mul_f64(self.fraction);
        let after_jitter = scaled.saturating_sub(pseudo_jitter(self.max_jitter));
        Some(after_jitter.max(self.min_interval))
    }
}

impl Default for ReauthTiming {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Cheap, non-cryptographic jitter in `[0, max)`. Pulls from the system
/// clock's sub-second nanoseconds so we don't need a `rand` dependency.
/// This is good enough for spreading re-auth bursts; it is not a
/// security-sensitive signal.
fn pseudo_jitter(max: Duration) -> Duration {
    let max_nanos = max.as_nanos() as u64;
    if max_nanos == 0 {
        return Duration::ZERO;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    Duration::from_nanos((nanos as u64) % max_nanos)
}

/// What a re-auth task needs to know about the connection it serves.
pub(in crate::kafka) struct ReauthContext {
    /// The client's connection slot. `Weak`, so the task ends when the
    /// client is dropped.
    pub(in crate::kafka) connection: Weak<Mutex<Option<BrokerConnection>>>,
    /// Generation of the connection the task was spawned for.
    pub(in crate::kafka) generation: u64,
    /// The client's correlation id counter.
    pub(in crate::kafka) correlation_id: Arc<AtomicI32>,
    pub(in crate::kafka) timing: ReauthTiming,
    /// Number of live re-auth tasks for the client (diagnostics, tests).
    pub(in crate::kafka) live_tasks: Arc<AtomicUsize>,
}

/// Decrements the live-task count however the task ends (return or
/// abort).
struct LiveTaskGuard(Arc<AtomicUsize>);

impl Drop for LiveTaskGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How one re-auth attempt ended.
enum ReauthResult {
    /// Re-authenticated; the broker's new `session_lifetime_ms`.
    Reauthenticated(i64),
    /// The slot is empty or holds a newer connection: nothing to do.
    Stale,
}

/// Spawn a background task that re-authenticates the connection of
/// generation `ctx.generation` before the broker-advertised
/// `session_lifetime_ms` expires.
///
/// Returns the task's `AbortHandle` (the caller stores it on the
/// connection, which aborts the task when dropped), or `None` when the
/// broker advertised no lifetime and no task was spawned.
pub(in crate::kafka) fn spawn_reauth_task(
    ctx: ReauthContext,
    plugin: SaslMechanismPluginHandle,
    session_lifetime_ms: i64,
) -> Option<AbortHandle> {
    let Some(initial_delay) = ctx.timing.delay(session_lifetime_ms) else {
        trace!("Broker advertised session_lifetime_ms=0; reauth scheduler disabled");
        return None;
    };

    ctx.live_tasks.fetch_add(1, Ordering::SeqCst);
    let guard = LiveTaskGuard(ctx.live_tasks.clone());
    let handle = tokio::spawn(async move {
        let _guard = guard;
        let mut delay = initial_delay;
        loop {
            tokio::time::sleep(delay).await;

            let Some(conn_arc) = ctx.connection.upgrade() else {
                trace!("KafkaClient dropped; reauth task exiting");
                return;
            };

            match reauth_once(&conn_arc, ctx.generation, &ctx.correlation_id, &plugin).await {
                Ok(ReauthResult::Reauthenticated(next_lifetime_ms)) => {
                    match ctx.timing.delay(next_lifetime_ms) {
                        Some(next) => {
                            debug!(
                                "SASL reauth succeeded; next reauth in {}ms",
                                next.as_millis()
                            );
                            delay = next;
                        }
                        None => {
                            debug!("SASL reauth succeeded; broker disabled further reauth");
                            return;
                        }
                    }
                }
                Ok(ReauthResult::Stale) => {
                    trace!(
                        "Connection generation {} replaced or gone; reauth task exiting",
                        ctx.generation
                    );
                    return;
                }
                Err(e) => {
                    warn!(
                        "SASL reauth failed: {}. Connection will reconnect with a full \
                         SASL handshake on the next RPC.",
                        e
                    );
                    return;
                }
            }
        }
    });
    Some(handle.abort_handle())
}

/// `true` when `slot` holds the connection of `generation`.
fn is_current(slot: &Option<BrokerConnection>, generation: u64) -> bool {
    slot.as_ref().is_some_and(|c| c.generation == generation)
}

/// Perform one re-auth exchange on the connection of `generation`.
///
/// Returns `Stale` (without contacting the broker) if the slot is empty
/// or holds another connection. On failure the slot is cleared, only if
/// it still holds this connection and while still holding the lock, so a
/// newer connection is never dropped by an old task.
async fn reauth_once(
    connection: &Arc<Mutex<Option<BrokerConnection>>>,
    generation: u64,
    correlation_id: &AtomicI32,
    plugin: &SaslMechanismPluginHandle,
) -> Result<ReauthResult, String> {
    // Cheap check before asking the plugin for a (possibly fresh) token.
    if !is_current(&*connection.lock().await, generation) {
        return Ok(ReauthResult::Stale);
    }

    let payload = match plugin.reauth_payload().await {
        Ok(payload) => payload,
        Err(e) => {
            let mut slot = connection.lock().await;
            if !is_current(&slot, generation) {
                return Ok(ReauthResult::Stale);
            }
            *slot = None;
            return Err(format!("plugin reauth_payload failed: {}", e));
        }
    };

    let mut slot = connection.lock().await;
    if !is_current(&slot, generation) {
        return Ok(ReauthResult::Stale);
    }
    let result = reauth_exchange(
        slot.as_mut().expect("checked above"),
        correlation_id,
        plugin,
        payload,
    )
    .await;
    if result.is_err() {
        *slot = None;
    }
    result.map(ReauthResult::Reauthenticated)
}

/// The `SaslAuthenticate` rounds of one re-auth, on a locked connection.
async fn reauth_exchange(
    conn: &mut BrokerConnection,
    correlation_id: &AtomicI32,
    plugin: &SaslMechanismPluginHandle,
    mut payload: Vec<u8>,
) -> Result<i64, String> {
    loop {
        let req = SaslAuthenticateRequest::default().with_auth_bytes(Bytes::from(payload.clone()));
        let buf = encode_kafka_request(
            correlation_id,
            ApiKey::SaslAuthenticate,
            SASL_AUTHENTICATE_VERSION,
            &req,
        )
        .map_err(|e| format!("encode SaslAuthenticate: {}", e))?;

        let resp: SaslAuthenticateResponse = send_rpc_on_stream(
            &mut conn.stream,
            ApiKey::SaslAuthenticate,
            SASL_AUTHENTICATE_VERSION,
            &buf,
        )
        .await
        .map_err(|e| format!("SaslAuthenticate RPC: {}", e))?;

        if resp.error_code != 0 {
            let err_bytes: Vec<u8> = resp
                .error_message
                .as_ref()
                .map(|s| s.as_bytes().to_vec())
                .unwrap_or_default();
            return Err(plugin.interpret_server_error(&err_bytes).to_string());
        }

        match plugin
            .continue_payload(&resp.auth_bytes)
            .await
            .map_err(|e| format!("plugin continue_payload failed: {}", e))?
        {
            SaslAuthOutcome::Continue(next) => payload = next,
            SaslAuthOutcome::Done => return Ok(resp.session_lifetime_ms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compute_reauth_delay(session_lifetime_ms: i64) -> Option<Duration> {
        ReauthTiming::DEFAULT.delay(session_lifetime_ms)
    }

    #[test]
    fn compute_reauth_delay_zero_lifetime_returns_none() {
        assert_eq!(compute_reauth_delay(0), None);
    }

    #[test]
    fn compute_reauth_delay_negative_lifetime_returns_none() {
        assert_eq!(compute_reauth_delay(-1), None);
    }

    #[test]
    fn compute_reauth_delay_typical_hits_80_percent_minus_jitter() {
        // 100s lifetime → base 80s, minus up to 5s jitter → [75s, 80s]
        let delay = compute_reauth_delay(100_000).expect("should schedule");
        assert!(
            delay >= Duration::from_secs(75) && delay <= Duration::from_secs(80),
            "expected [75s, 80s], got {:?}",
            delay
        );
    }

    #[test]
    fn compute_reauth_delay_below_floor_clamps_to_min_interval() {
        // 10s lifetime → base 8s, below MIN_REAUTH_INTERVAL (30s). Floor wins.
        let delay = compute_reauth_delay(10_000).expect("should schedule");
        assert_eq!(delay, ReauthTiming::DEFAULT.min_interval);
    }

    #[test]
    fn compute_reauth_delay_just_above_floor_respects_jitter() {
        // 40s lifetime → base 32s, minus up to 5s → [27s, 32s]. Floor (30s)
        // should clamp the low end.
        let delay = compute_reauth_delay(40_000).expect("should schedule");
        assert!(
            delay >= ReauthTiming::DEFAULT.min_interval && delay <= Duration::from_secs(32),
            "expected [30s, 32s], got {:?}",
            delay
        );
    }

    #[test]
    fn pseudo_jitter_stays_within_bounds() {
        let max = ReauthTiming::DEFAULT.max_jitter;
        for _ in 0..100 {
            let j = pseudo_jitter(max);
            assert!(j < max, "jitter {:?} >= max {:?}", j, max);
        }
        assert_eq!(pseudo_jitter(Duration::ZERO), Duration::ZERO);
    }

    #[test]
    fn injected_timing_drives_the_delay() {
        let fast = ReauthTiming {
            fraction: 0.5,
            min_interval: Duration::from_millis(10),
            max_jitter: Duration::ZERO,
        };
        assert_eq!(fast.delay(400), Some(Duration::from_millis(200)));
        assert_eq!(fast.delay(10), Some(Duration::from_millis(10)));
        assert_eq!(fast.delay(0), None);
        assert_eq!(ReauthTiming::default(), ReauthTiming::DEFAULT);
    }
}
