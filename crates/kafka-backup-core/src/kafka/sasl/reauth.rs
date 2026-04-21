//! KIP-368 SASL re-authentication scheduler.
//!
//! When a broker advertises a non-zero `session_lifetime_ms` in the
//! terminal `SaslAuthenticateResponse`, the client has a limited window
//! to re-authenticate before the broker closes the connection. This
//! module implements that scheduler.
//!
//! ## Timing
//!
//! We reauth at `REAUTH_FRACTION` (80%) of the advertised lifetime, minus
//! a small negative jitter to avoid a thundering herd when many
//! connections authenticate at the same instant. A floor of
//! `MIN_REAUTH_INTERVAL` prevents a misconfigured broker from spinning
//! the scheduler at sub-second intervals.
//!
//! ## Wire protocol
//!
//! Per KIP-368, re-authentication sends **only** `SaslAuthenticate`
//! frames; there is no fresh `SaslHandshake`. The mechanism is already
//! bound to the connection from the initial handshake.
//!
//! ## Lifecycle
//!
//! The task holds a `Weak` reference to the shared connection. When the
//! owning `KafkaClient` is dropped, the upgrade fails and the task
//! exits. On re-auth failure we set the connection slot to `None`; the
//! next caller into `send_request` will reconnect through the normal
//! path, which triggers a full SASL handshake again.
//!
//! ## Known limitation (tracked follow-up)
//!
//! If a transport-level error triggers `reconnect()` while a reauth
//! task is still sleeping, the reconnect path spawns a second reauth
//! task without cancelling the first. The first wakes later, locks
//! the (new) connection, and does a redundant SaslAuthenticate — then
//! both tasks keep re-arming. The wire interactions stay correct
//! (KIP-368 permits SaslAuthenticate on an authenticated connection),
//! but the per-connection task count grows with each reconnect. A
//! generation counter on `BrokerConnection` would let stale tasks
//! detect the swap and exit — deferred to a follow-up.

use std::sync::atomic::AtomicI32;
use std::sync::{Arc, Weak};
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::{ApiKey, SaslAuthenticateRequest, SaslAuthenticateResponse};
use tokio::sync::Mutex;
use tracing::{debug, trace, warn};

use super::plugin::{SaslAuthOutcome, SaslMechanismPluginHandle};
use crate::kafka::client::{encode_kafka_request, send_rpc_on_stream, BrokerConnection};

/// Fraction of `session_lifetime_ms` after which we trigger re-auth.
/// Matches librdkafka's `sasl.oauthbearer.token.refresh.ms` default ratio.
const REAUTH_FRACTION: f64 = 0.8;

/// Lower bound on the computed re-auth delay. Guards against brokers
/// that advertise a very short session lifetime and would otherwise
/// cause the scheduler to fire constantly.
const MIN_REAUTH_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum negative jitter subtracted from the computed delay. Spreads
/// simultaneous re-auths across a ±5s window; subtracted only (never
/// added) so we never overshoot the broker's expiry.
const MAX_JITTER: Duration = Duration::from_secs(5);

/// `SaslAuthenticate` wire version. Must match the version used by the
/// initial handshake path in `KafkaClient::sasl_plugin_auth` (v2 per
/// KIP-368, which carries `session_lifetime_ms` on the response).
const SASL_AUTHENTICATE_VERSION: i16 = 2;

/// Compute when to fire the next re-auth given a broker-advertised
/// `session_lifetime_ms`. Returns `None` when the broker advertised no
/// lifetime (0 or negative), meaning re-auth is disabled for this
/// connection.
///
/// When `lifetime * REAUTH_FRACTION - jitter` falls below
/// `MIN_REAUTH_INTERVAL`, the floor wins. On a pathologically short
/// lifetime the floor may exceed the lifetime itself — that is
/// intentional: the next RPC on the connection will see the
/// broker-initiated close and reconnect through the normal path,
/// which does a full re-handshake.
pub fn compute_reauth_delay(session_lifetime_ms: i64) -> Option<Duration> {
    if session_lifetime_ms <= 0 {
        return None;
    }

    let lifetime = Duration::from_millis(session_lifetime_ms as u64);
    let scaled = lifetime.mul_f64(REAUTH_FRACTION);
    let jitter = pseudo_jitter();
    let after_jitter = scaled.saturating_sub(jitter);
    Some(after_jitter.max(MIN_REAUTH_INTERVAL))
}

/// Cheap, non-cryptographic jitter in `[0, MAX_JITTER)`. Pulls from the
/// system clock's sub-second nanoseconds so we don't need a `rand`
/// dependency. This is good enough for spreading re-auth bursts — it's
/// not a security-sensitive signal.
fn pseudo_jitter() -> Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let jitter_nanos = (nanos as u64) % (MAX_JITTER.as_nanos() as u64);
    Duration::from_nanos(jitter_nanos)
}

/// Spawn a background task that re-authenticates the given connection
/// before the broker-advertised `session_lifetime_ms` expires.
///
/// The task holds only a `Weak` reference to the connection, so it
/// self-terminates when the owning `KafkaClient` is dropped. On
/// re-auth failure the connection slot is cleared (`*slot = None`) so
/// the next RPC reconnects through the normal code path, which will
/// perform a fresh handshake.
pub(in crate::kafka) fn spawn_reauth_task(
    connection: Weak<Mutex<Option<BrokerConnection>>>,
    correlation_id: Arc<AtomicI32>,
    plugin: SaslMechanismPluginHandle,
    session_lifetime_ms: i64,
) {
    let Some(initial_delay) = compute_reauth_delay(session_lifetime_ms) else {
        trace!("Broker advertised session_lifetime_ms=0; reauth scheduler disabled");
        return;
    };

    tokio::spawn(async move {
        let mut delay = initial_delay;
        loop {
            tokio::time::sleep(delay).await;

            let Some(conn_arc) = connection.upgrade() else {
                trace!("KafkaClient dropped; reauth task exiting");
                return;
            };

            match reauth_once(&conn_arc, &correlation_id, &plugin).await {
                Ok(Some(next_lifetime_ms)) => match compute_reauth_delay(next_lifetime_ms) {
                    Some(next) => {
                        debug!("SASL reauth succeeded; next reauth in {}s", next.as_secs());
                        delay = next;
                    }
                    None => {
                        debug!("SASL reauth succeeded; broker disabled further reauth");
                        return;
                    }
                },
                Ok(None) => {
                    trace!("Connection gone before reauth; exiting");
                    return;
                }
                Err(e) => {
                    warn!(
                        "SASL reauth failed: {}. Connection will reconnect on next RPC.",
                        e
                    );
                    let mut slot = conn_arc.lock().await;
                    *slot = None;
                    return;
                }
            }
        }
    });
}

/// Perform one re-auth exchange on the already-established connection.
///
/// Returns:
/// - `Ok(Some(session_lifetime_ms))` on success; caller re-arms.
/// - `Ok(None)` if the connection slot was already empty (dropped
///   between sleep and lock — not an error, just nothing to do).
/// - `Err(..)` on wire failure or broker-side auth rejection.
async fn reauth_once(
    connection: &Arc<Mutex<Option<BrokerConnection>>>,
    correlation_id: &AtomicI32,
    plugin: &SaslMechanismPluginHandle,
) -> Result<Option<i64>, String> {
    let mut payload = plugin
        .reauth_payload()
        .await
        .map_err(|e| format!("plugin reauth_payload failed: {}", e))?;

    let mut slot = connection.lock().await;
    let Some(conn) = slot.as_mut() else {
        return Ok(None);
    };

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
            SaslAuthOutcome::Done => return Ok(Some(resp.session_lifetime_ms)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(delay, MIN_REAUTH_INTERVAL);
    }

    #[test]
    fn compute_reauth_delay_just_above_floor_respects_jitter() {
        // 40s lifetime → base 32s, minus up to 5s → [27s, 32s]. Floor (30s)
        // should clamp the low end.
        let delay = compute_reauth_delay(40_000).expect("should schedule");
        assert!(
            delay >= MIN_REAUTH_INTERVAL && delay <= Duration::from_secs(32),
            "expected [30s, 32s], got {:?}",
            delay
        );
    }

    #[test]
    fn pseudo_jitter_stays_within_bounds() {
        for _ in 0..100 {
            let j = pseudo_jitter();
            assert!(j < MAX_JITTER, "jitter {:?} >= MAX_JITTER", j);
        }
    }
}
