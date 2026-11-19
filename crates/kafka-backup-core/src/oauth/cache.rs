//! `TokenCache`: one per OAuth provider, shared by every consumer of that
//! provider (all Kafka connections of a cluster, Schema Registry, the DEK
//! Registry).
//!
//! ## Semantics
//!
//! - **Single flight.** At most one provider call runs at a time; callers
//!   that need a token while one is in flight wait for it (a tokio mutex,
//!   checked again after it is acquired). 100 concurrent cold `get()`s make
//!   exactly one provider call.
//! - **Valid** means `exp - clock_skew > now` (a token without a known
//!   expiry is always valid).
//! - **Refresh time** is
//!   `max(fetched + min_refresh_interval, min(fetched + refresh_fraction * lifetime, exp - refresh_before_expiry))`.
//!   A token past its refresh time but still valid is returned at once and a
//!   background refresh starts (if none is running).
//! - **Errors.** A failed background refresh keeps serving the current
//!   token and retries with exponential backoff from `min_refresh_interval`,
//!   never later than just before the token stops being valid. With no
//!   valid token, every caller waiting on the failed attempt gets the error
//!   and further attempts are limited to one per second; a token that is no
//!   longer "valid" but has not actually expired is still served as a last
//!   resort while the provider is failing.
//! - **Minimum TTL.** [`TokenCache::get_with_min_ttl`] (used for every SASL
//!   handshake, initial and KIP-368 re-authentication) refreshes a token
//!   whose remaining lifetime is below `min(min_ttl, 0.8 * lifetime)`, so the
//!   broker grants a session long enough to re-authenticate in. A token
//!   younger than `min(min_refresh_interval, lifetime / 5)` is never
//!   replaced for this reason.
//! - **Invalidation.** [`TokenCache::invalidate`] drops the current token
//!   after a broker or registry rejected it, unless it was fetched less than
//!   `min_refresh_interval` ago (then the rejection is about configuration,
//!   not expiry, and refetching would only load the identity provider).
//!
//! Tokens are opaque. When a provider gives no expiry, the JWT `exp` claim
//! is read without verification ([`super::jwt`]); failing that, the token is
//! refreshed every five minutes.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use tracing::{debug, info, warn};

use super::jwt;
use super::token::{OauthProviderError, OauthToken, OauthTokenProvider};

/// Source of wall-clock time, injectable for tests.
pub trait Clock: Send + Sync + fmt::Debug {
    fn now(&self) -> SystemTime;
}

/// The system clock.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Timing settings (YAML `oauth.cache`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CacheSettings {
    /// Refresh after this fraction of the token lifetime (default 0.8).
    pub refresh_fraction: f64,
    /// ... or this long before expiry, whichever is earlier (default 5m).
    pub refresh_before_expiry: Duration,
    /// A token is valid while `exp - clock_skew > now` (default 30s).
    pub clock_skew: Duration,
    /// Never refresh sooner than this after a fetch, and the first backoff
    /// step after a failed background refresh (default 30s).
    pub min_refresh_interval: Duration,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            refresh_fraction: 0.8,
            refresh_before_expiry: Duration::from_secs(300),
            clock_skew: Duration::from_secs(30),
            min_refresh_interval: Duration::from_secs(30),
        }
    }
}

/// Minimum remaining lifetime requested for SASL handshake tokens.
pub const HANDSHAKE_MIN_TTL: Duration = Duration::from_secs(60);

/// Refresh cadence for tokens whose expiry is unknown.
const UNKNOWN_EXPIRY_REFRESH: Duration = Duration::from_secs(300);

/// Spacing of attempts while there is no valid token and the provider fails.
const COLD_RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// How long a handshake waits for a refresh when it already holds a valid
/// token (it uses that token if the identity provider is slower).
const INLINE_REFRESH_BUDGET: Duration = Duration::from_secs(5);

/// Longest background-refresh backoff, as a power of two of the base.
const MAX_BACKOFF_DOUBLINGS: u32 = 6;

/// A shared, single-flight token cache in front of one provider.
#[derive(Clone)]
pub struct TokenCache {
    inner: Arc<Inner>,
}

struct Inner {
    name: String,
    provider: Arc<dyn OauthTokenProvider>,
    settings: CacheSettings,
    clock: Arc<dyn Clock>,
    state: Mutex<State>,
    refresh_lock: tokio::sync::Mutex<()>,
    fetches: AtomicU64,
    failures: AtomicU64,
}

#[derive(Default)]
struct State {
    current: Option<Cached>,
    background_running: bool,
    next_background_attempt: Option<SystemTime>,
    background_failures: u32,
    /// The last failed attempt made while no valid token was available.
    last_failure: Option<(SystemTime, String)>,
}

#[derive(Clone)]
struct Cached {
    token: Arc<OauthToken>,
    fetched_at: SystemTime,
    expires_at: Option<SystemTime>,
    refresh_at: SystemTime,
}

impl Cached {
    fn new(token: OauthToken, fetched_at: SystemTime, settings: &CacheSettings) -> Self {
        let expires_at = token
            .expires_at
            .or_else(|| jwt::unverified_exp(token.value()));
        Self {
            refresh_at: refresh_at(fetched_at, expires_at, settings),
            token: Arc::new(token),
            fetched_at,
            expires_at,
        }
    }

    fn lifetime(&self) -> Option<Duration> {
        self.expires_at
            .map(|exp| exp.duration_since(self.fetched_at).unwrap_or_default())
    }

    fn remaining(&self, now: SystemTime) -> Option<Duration> {
        self.expires_at
            .map(|exp| exp.duration_since(now).unwrap_or_default())
    }

    fn age(&self, now: SystemTime) -> Duration {
        now.duration_since(self.fetched_at).unwrap_or_default()
    }

    /// `exp - skew > now`.
    fn is_valid(&self, now: SystemTime, skew: Duration) -> bool {
        self.expires_at.is_none_or(|exp| now + skew < exp)
    }

    fn not_expired(&self, now: SystemTime) -> bool {
        self.expires_at.is_none_or(|exp| now < exp)
    }

    fn meets_min_ttl(&self, now: SystemTime, min_ttl: Duration, settings: &CacheSettings) -> bool {
        if min_ttl.is_zero() {
            return true;
        }
        let (Some(remaining), Some(lifetime)) = (self.remaining(now), self.lifetime()) else {
            return true;
        };
        if remaining >= min_ttl.min(lifetime.mul_f64(0.8)) {
            return true;
        }
        self.age(now) < settings.min_refresh_interval.min(lifetime / 5)
    }
}

/// When a token fetched at `fetched_at` should be refreshed.
pub(crate) fn refresh_at(
    fetched_at: SystemTime,
    expires_at: Option<SystemTime>,
    settings: &CacheSettings,
) -> SystemTime {
    let floor = fetched_at + settings.min_refresh_interval;
    let Some(exp) = expires_at else {
        return fetched_at + settings.min_refresh_interval.max(UNKNOWN_EXPIRY_REFRESH);
    };
    let lifetime = exp.duration_since(fetched_at).unwrap_or_default();
    let by_fraction = fetched_at + lifetime.mul_f64(settings.refresh_fraction);
    let by_margin = exp
        .checked_sub(settings.refresh_before_expiry)
        .unwrap_or(fetched_at)
        .max(fetched_at);
    floor.max(by_fraction.min(by_margin))
}

fn secs_until(later: SystemTime, now: SystemTime) -> u64 {
    later.duration_since(now).unwrap_or_default().as_secs()
}

impl fmt::Debug for TokenCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenCache")
            .field("provider", &self.inner.name)
            .field("kind", &self.inner.provider.kind())
            .field("settings", &self.inner.settings)
            .finish()
    }
}

/// Clears `background_running` however the background task ends.
struct BackgroundGuard(TokenCache);

impl Drop for BackgroundGuard {
    fn drop(&mut self) {
        self.0.lock_state().background_running = false;
    }
}

impl TokenCache {
    /// A cache named `name` (the provider name, used in logs and metrics).
    pub fn new(
        name: impl Into<String>,
        provider: Arc<dyn OauthTokenProvider>,
        settings: CacheSettings,
    ) -> Self {
        Self::with_clock(name, provider, settings, Arc::new(SystemClock))
    }

    /// [`Self::new`] with an injected clock (tests).
    pub fn with_clock(
        name: impl Into<String>,
        provider: Arc<dyn OauthTokenProvider>,
        settings: CacheSettings,
        clock: Arc<dyn Clock>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                name: name.into(),
                provider,
                settings,
                clock,
                state: Mutex::new(State::default()),
                refresh_lock: tokio::sync::Mutex::new(()),
                fetches: AtomicU64::new(0),
                failures: AtomicU64::new(0),
            }),
        }
    }

    /// The provider name this cache serves.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// The provider kind (`oidc_client_credentials`, `file`, ...).
    pub fn provider_kind(&self) -> &'static str {
        self.inner.provider.kind()
    }

    /// Provider calls made so far (successful or not).
    pub fn fetch_count(&self) -> u64 {
        self.inner.fetches.load(Ordering::Relaxed)
    }

    /// Provider calls that failed so far.
    pub fn failure_count(&self) -> u64 {
        self.inner.failures.load(Ordering::Relaxed)
    }

    /// True when both handles share one cache.
    pub fn ptr_eq(&self, other: &TokenCache) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    fn lock_state(&self) -> MutexGuard<'_, State> {
        // A panic while holding this lock leaves plain data behind; keep going.
        self.inner
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// A valid token, fetching one if needed.
    pub async fn get(&self) -> Result<Arc<OauthToken>, OauthProviderError> {
        self.get_with_min_ttl(Duration::ZERO).await
    }

    /// A valid token with at least `min(min_ttl, 0.8 * lifetime)` left.
    pub async fn get_with_min_ttl(
        &self,
        min_ttl: Duration,
    ) -> Result<Arc<OauthToken>, OauthProviderError> {
        let settings = self.inner.settings;
        let now = self.inner.clock.now();
        {
            let mut state = self.lock_state();
            if let Some(cached) = state.current.clone() {
                if cached.is_valid(now, settings.clock_skew)
                    && cached.meets_min_ttl(now, min_ttl, &settings)
                {
                    if now >= cached.refresh_at {
                        self.maybe_spawn_background(&mut state, now);
                    }
                    return Ok(cached.token);
                }
            }
        }
        self.refresh_in_line(min_ttl).await
    }

    /// Drop the current token after a broker or registry rejected it.
    /// Returns true if it was dropped (see the module docs).
    pub fn invalidate(&self) -> bool {
        let now = self.inner.clock.now();
        let mut state = self.lock_state();
        match &state.current {
            Some(cached) if cached.age(now) >= self.inner.settings.min_refresh_interval => {
                state.current = None;
                info!(
                    provider = %self.inner.name,
                    "OAuth token was rejected; it will be fetched again"
                );
                true
            }
            Some(_) => {
                debug!(
                    provider = %self.inner.name,
                    "OAuth token was rejected but was fetched moments ago; keeping it \
                     (check the token's audience, issuer and permissions)"
                );
                false
            }
            None => false,
        }
    }

    async fn refresh_in_line(
        &self,
        min_ttl: Duration,
    ) -> Result<Arc<OauthToken>, OauthProviderError> {
        let settings = self.inner.settings;
        let _flight = self.inner.refresh_lock.lock().await;
        let now = self.inner.clock.now();
        let fallback = {
            let state = self.lock_state();
            if let Some(cached) = &state.current {
                if cached.is_valid(now, settings.clock_skew)
                    && cached.meets_min_ttl(now, min_ttl, &settings)
                {
                    return Ok(cached.token.clone());
                }
            }
            // Never call the provider more than once a second: a token that
            // came back already inside the skew window is served as is.
            if let Some(cached) = state
                .current
                .as_ref()
                .filter(|c| c.age(now) < COLD_RETRY_INTERVAL && c.not_expired(now))
            {
                return Ok(cached.token.clone());
            }
            if let Some((failed_at, message)) = &state.last_failure {
                if now < *failed_at + COLD_RETRY_INTERVAL {
                    if let Some(cached) = state.current.as_ref().filter(|c| c.not_expired(now)) {
                        return Ok(cached.token.clone());
                    }
                    return Err(OauthProviderError::RecentFailure(message.clone()));
                }
            }
            // A still-valid token while refreshes are backing off after
            // failures: use it rather than wait on the identity provider.
            if let Some(cached) = state.current.as_ref().filter(|c| {
                c.is_valid(now, settings.clock_skew)
                    && state.next_background_attempt.is_some_and(|at| now < at)
            }) {
                return Ok(cached.token.clone());
            }
            state.current.clone()
        };

        // With a valid token in hand, a slow identity provider must not
        // hold up the handshake: give the refresh a short budget.
        let has_valid_fallback = fallback
            .as_ref()
            .is_some_and(|c| c.is_valid(now, settings.clock_skew));
        let result = if has_valid_fallback {
            match tokio::time::timeout(INLINE_REFRESH_BUDGET, self.fetch()).await {
                Ok(result) => result,
                Err(_) => Err(OauthProviderError::Http(format!(
                    "no answer from the identity provider within {}s",
                    INLINE_REFRESH_BUDGET.as_secs()
                ))),
            }
        } else {
            self.fetch().await
        };

        match result {
            Ok(cached) => {
                let token = cached.token.clone();
                let mut state = self.lock_state();
                state.current = Some(cached);
                state.last_failure = None;
                state.background_failures = 0;
                state.next_background_attempt = None;
                Ok(token)
            }
            Err(e) => {
                let failed_at = self.inner.clock.now();
                {
                    let mut state = self.lock_state();
                    state.last_failure = Some((failed_at, e.to_string()));
                    if has_valid_fallback {
                        self.schedule_retry(&mut state, failed_at);
                    }
                }
                if let Some(cached) = fallback.filter(|c| c.not_expired(failed_at)) {
                    warn!(
                        provider = %self.inner.name,
                        error = %e,
                        expires_in_secs = cached.remaining(failed_at).map(|d| d.as_secs()),
                        "OAuth token refresh failed; still using the current token until it expires"
                    );
                    return Ok(cached.token);
                }
                Err(e)
            }
        }
    }

    fn maybe_spawn_background(&self, state: &mut State, now: SystemTime) {
        if state.background_running {
            return;
        }
        if state.next_background_attempt.is_some_and(|at| now < at) {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        state.background_running = true;
        let cache = self.clone();
        runtime.spawn(async move {
            let guard = BackgroundGuard(cache.clone());
            cache.background_refresh().await;
            drop(guard);
        });
    }

    async fn background_refresh(&self) {
        let _flight = self.inner.refresh_lock.lock().await;
        let now = self.inner.clock.now();
        let due = self
            .lock_state()
            .current
            .as_ref()
            .is_none_or(|c| now >= c.refresh_at);
        if !due {
            return; // refreshed in line while this task waited
        }
        let result = self.fetch().await;
        let mut state = self.lock_state();
        match result {
            Ok(cached) => {
                state.current = Some(cached);
                state.background_failures = 0;
                state.next_background_attempt = None;
                state.last_failure = None;
            }
            Err(e) => {
                let next = self.schedule_retry(&mut state, now);
                warn!(
                    provider = %self.inner.name,
                    error = %e,
                    retry_in_secs = secs_until(next, now),
                    "Background OAuth token refresh failed; the current token is still valid and stays in use"
                );
            }
        }
    }

    /// Record a failed refresh made while a valid token is held: exponential
    /// backoff from `min_refresh_interval`, but always another attempt just
    /// before the current token stops being valid. Returns the next attempt.
    fn schedule_retry(&self, state: &mut State, now: SystemTime) -> SystemTime {
        let settings = self.inner.settings;
        state.background_failures = state.background_failures.saturating_add(1);
        let doublings = (state.background_failures - 1).min(MAX_BACKOFF_DOUBLINGS);
        let base = settings.min_refresh_interval.max(Duration::from_secs(1));
        let mut next = now + base * 2u32.pow(doublings);
        if let Some(latest) = state
            .current
            .as_ref()
            .and_then(|c| c.expires_at)
            .and_then(|exp| exp.checked_sub(settings.clock_skew + Duration::from_secs(1)))
        {
            next = next.min(latest.max(now));
        }
        state.next_background_attempt = Some(next);
        next
    }

    async fn fetch(&self) -> Result<Cached, OauthProviderError> {
        let settings = self.inner.settings;
        self.inner.fetches.fetch_add(1, Ordering::Relaxed);
        let started = self.inner.clock.now();
        let result = match self.inner.provider.get_token().await {
            Ok(token) if token.value().is_empty() => Err(OauthProviderError::InvalidToken(
                "the provider returned an empty token".into(),
            )),
            Ok(token) => {
                let cached = Cached::new(token, started, &settings);
                let now = self.inner.clock.now();
                if cached.expires_at.is_some_and(|exp| exp <= now) {
                    Err(OauthProviderError::InvalidToken(
                        "the provider returned a token that has already expired".into(),
                    ))
                } else {
                    debug!(
                        provider = %self.inner.name,
                        kind = self.inner.provider.kind(),
                        expires_in_secs = cached.remaining(now).map(|d| d.as_secs()),
                        refresh_in_secs = secs_until(cached.refresh_at, now),
                        "OAuth token fetched"
                    );
                    Ok(cached)
                }
            }
            Err(e) => Err(e),
        };
        if result.is_err() {
            self.inner.failures.fetch_add(1, Ordering::Relaxed);
        }
        result
    }
}

/// A clock tests move by hand.
#[cfg(test)]
#[derive(Debug)]
pub struct ManualClock(Mutex<SystemTime>);

#[cfg(test)]
impl ManualClock {
    pub fn new(start: SystemTime) -> Self {
        Self(Mutex::new(start))
    }

    pub fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += by;
    }
}

#[cfg(test)]
impl Clock for ManualClock {
    fn now(&self) -> SystemTime {
        *self.0.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    fn t0() -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(1_800_000_000)
    }

    /// Mints `tok-<n>` tokens valid for `lifetime` from the shared clock.
    #[derive(Debug)]
    struct TestProvider {
        clock: Arc<ManualClock>,
        lifetime: Option<Duration>,
        calls: AtomicUsize,
        failing: AtomicBool,
        delay: Duration,
    }

    impl TestProvider {
        fn new(clock: Arc<ManualClock>, lifetime: Option<Duration>) -> Arc<Self> {
            Arc::new(Self {
                clock,
                lifetime,
                calls: AtomicUsize::new(0),
                failing: AtomicBool::new(false),
                delay: Duration::ZERO,
            })
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn fail(&self, failing: bool) {
            self.failing.store(failing, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl OauthTokenProvider for TestProvider {
        async fn get_token(&self) -> Result<OauthToken, OauthProviderError> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            if !self.delay.is_zero() {
                tokio::time::sleep(self.delay).await;
            }
            if self.failing.load(Ordering::SeqCst) {
                return Err(OauthProviderError::Endpoint {
                    status: Some(503),
                    error: "temporarily_unavailable".into(),
                    description: None,
                });
            }
            Ok(OauthToken::new(
                format!("tok-{n}"),
                self.lifetime.map(|l| self.clock.now() + l),
                None,
                HashMap::new(),
            ))
        }

        fn kind(&self) -> &'static str {
            "test"
        }
    }

    fn cache(provider: Arc<TestProvider>) -> TokenCache {
        let clock = provider.clock.clone();
        TokenCache::with_clock("p", provider, CacheSettings::default(), clock)
    }

    /// Let spawned background refreshes run.
    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[test]
    fn refresh_schedule() {
        let s = CacheSettings::default();
        let at = |lifetime: u64| {
            refresh_at(t0(), Some(t0() + Duration::from_secs(lifetime)), &s)
                .duration_since(t0())
                .unwrap()
                .as_secs()
        };
        assert_eq!(at(3600), 2880, "80 % of an hour");
        assert_eq!(at(600), 300, "five minutes before expiry wins");
        assert_eq!(at(90), 30, "min_refresh_interval floor");
        assert_eq!(at(60), 30, "min_refresh_interval floor");
        assert_eq!(
            refresh_at(t0(), None, &s).duration_since(t0()).unwrap(),
            Duration::from_secs(300)
        );
    }

    #[tokio::test]
    async fn hundred_concurrent_gets_make_one_provider_call() {
        let clock = Arc::new(ManualClock::new(t0()));
        let provider = Arc::new(TestProvider {
            delay: Duration::from_millis(50),
            ..Arc::try_unwrap(TestProvider::new(clock, Some(Duration::from_secs(3600)))).unwrap()
        });
        let cache = cache(provider.clone());
        let handles: Vec<_> = (0..100)
            .map(|_| {
                let cache = cache.clone();
                tokio::spawn(async move { cache.get().await.map(|t| t.value().to_string()) })
            })
            .collect();
        for handle in handles {
            assert_eq!(handle.await.unwrap().unwrap(), "tok-1");
        }
        assert_eq!(provider.calls(), 1);
        assert_eq!(cache.fetch_count(), 1);
    }

    #[tokio::test]
    async fn fresh_token_is_reused_then_refreshed_in_the_background() {
        let clock = Arc::new(ManualClock::new(t0()));
        let provider = TestProvider::new(clock.clone(), Some(Duration::from_secs(3600)));
        let cache = cache(provider.clone());
        assert_eq!(cache.get().await.unwrap().value(), "tok-1");
        clock.advance(Duration::from_secs(2000));
        assert_eq!(cache.get().await.unwrap().value(), "tok-1");
        assert_eq!(provider.calls(), 1);

        // Past the refresh time (2880 s): the current token comes back at
        // once and a background refresh replaces it.
        clock.advance(Duration::from_secs(900));
        assert_eq!(cache.get().await.unwrap().value(), "tok-1");
        settle().await;
        assert_eq!(provider.calls(), 2);
        assert_eq!(cache.get().await.unwrap().value(), "tok-2");
    }

    #[tokio::test]
    async fn stale_but_valid_token_is_served_while_refresh_fails_with_backoff() {
        let clock = Arc::new(ManualClock::new(t0()));
        let provider = TestProvider::new(clock.clone(), Some(Duration::from_secs(3600)));
        let cache = cache(provider.clone());
        cache.get().await.unwrap();

        provider.fail(true);
        clock.advance(Duration::from_secs(2900)); // past refresh_at, still valid
        assert_eq!(cache.get().await.unwrap().value(), "tok-1");
        settle().await;
        assert_eq!(provider.calls(), 2, "one background attempt");

        // Within the 30 s backoff: no further attempts.
        for _ in 0..5 {
            clock.advance(Duration::from_secs(5));
            assert_eq!(cache.get().await.unwrap().value(), "tok-1");
            settle().await;
        }
        assert_eq!(provider.calls(), 2);

        // After 30 s: one more attempt; then the backoff doubles to 60 s.
        clock.advance(Duration::from_secs(6));
        cache.get().await.unwrap();
        settle().await;
        assert_eq!(provider.calls(), 3);
        clock.advance(Duration::from_secs(45));
        cache.get().await.unwrap();
        settle().await;
        assert_eq!(provider.calls(), 3, "60 s backoff not over yet");
        clock.advance(Duration::from_secs(20));
        cache.get().await.unwrap();
        settle().await;
        assert_eq!(provider.calls(), 4);

        // Recovery.
        provider.fail(false);
        clock.advance(Duration::from_secs(130));
        cache.get().await.unwrap();
        settle().await;
        assert_eq!(cache.get().await.unwrap().value(), "tok-5");
    }

    #[tokio::test]
    async fn backoff_never_passes_the_end_of_validity() {
        let clock = Arc::new(ManualClock::new(t0()));
        // 600 s token: refresh at 300 s, valid until 570 s.
        let provider = TestProvider::new(clock.clone(), Some(Duration::from_secs(600)));
        let cache = cache(provider.clone());
        cache.get().await.unwrap();
        provider.fail(true);
        // Fail at 300, 330, 390, 510 (backoff 30, 60, 120); the next step
        // (240 s) would be 750 s, past validity (570 s): it is capped at
        // exp - skew - 1 s = 569 s.
        for (advance, expected_calls) in [(300, 2), (30, 3), (60, 4), (120, 5)] {
            clock.advance(Duration::from_secs(advance));
            cache.get().await.unwrap();
            settle().await;
            assert_eq!(provider.calls(), expected_calls);
        }
        clock.advance(Duration::from_secs(58)); // 568 s
        cache.get().await.unwrap();
        settle().await;
        assert_eq!(provider.calls(), 5);
        clock.advance(Duration::from_secs(1)); // 569 s
        cache.get().await.unwrap();
        settle().await;
        assert_eq!(provider.calls(), 6);
    }

    #[tokio::test]
    async fn cold_failure_reaches_every_waiter_and_retries_once_per_second() {
        let clock = Arc::new(ManualClock::new(t0()));
        let provider = Arc::new(TestProvider {
            delay: Duration::from_millis(20),
            ..Arc::try_unwrap(TestProvider::new(
                clock.clone(),
                Some(Duration::from_secs(60)),
            ))
            .unwrap()
        });
        provider.fail(true);
        let cache = cache(provider.clone());
        let handles: Vec<_> = (0..10)
            .map(|_| {
                let cache = cache.clone();
                tokio::spawn(async move { cache.get().await })
            })
            .collect();
        for handle in handles {
            assert!(handle.await.unwrap().is_err());
        }
        assert_eq!(provider.calls(), 1);

        let err = cache.get().await.unwrap_err();
        assert!(matches!(err, OauthProviderError::RecentFailure(ref m) if m.contains("503")));
        assert_eq!(provider.calls(), 1, "rate-limited");

        clock.advance(Duration::from_secs(1));
        provider.fail(false);
        assert_eq!(cache.get().await.unwrap().value(), "tok-2");
    }

    #[tokio::test]
    async fn min_ttl_refreshes_short_tokens_but_not_young_ones() {
        let clock = Arc::new(ManualClock::new(t0()));
        let provider = TestProvider::new(clock.clone(), Some(Duration::from_secs(60)));
        let cache = cache(provider.clone());
        assert_eq!(cache.get().await.unwrap().value(), "tok-1");

        // 5 s old, 55 s left: wanted min(60, 48) = 48 s, so it qualifies.
        clock.advance(Duration::from_secs(5));
        assert_eq!(
            cache
                .get_with_min_ttl(HANDSHAKE_MIN_TTL)
                .await
                .unwrap()
                .value(),
            "tok-1"
        );

        // 20 s old, 40 s left: a plain get keeps it, a handshake refreshes.
        clock.advance(Duration::from_secs(15));
        assert_eq!(cache.get().await.unwrap().value(), "tok-1");
        assert_eq!(
            cache
                .get_with_min_ttl(HANDSHAKE_MIN_TTL)
                .await
                .unwrap()
                .value(),
            "tok-2"
        );
        assert_eq!(provider.calls(), 2);

        // Young tokens are never replaced for min_ttl (age guard 12 s).
        clock.advance(Duration::from_secs(11));
        assert_eq!(
            cache
                .get_with_min_ttl(Duration::from_secs(600))
                .await
                .unwrap()
                .value(),
            "tok-2"
        );
        assert_eq!(provider.calls(), 2);
    }

    #[tokio::test]
    async fn min_ttl_refresh_failure_still_serves_the_valid_token() {
        let clock = Arc::new(ManualClock::new(t0()));
        let provider = TestProvider::new(clock.clone(), Some(Duration::from_secs(60)));
        let cache = cache(provider.clone());
        cache.get().await.unwrap();
        provider.fail(true);
        clock.advance(Duration::from_secs(20));
        assert_eq!(
            cache
                .get_with_min_ttl(HANDSHAKE_MIN_TTL)
                .await
                .unwrap()
                .value(),
            "tok-1"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_provider_does_not_hold_up_a_handshake_with_a_valid_token() {
        let clock = Arc::new(ManualClock::new(t0()));
        let provider = TestProvider::new(clock.clone(), Some(Duration::from_secs(60)));
        let cache = cache(provider.clone());
        cache.get().await.unwrap();
        clock.advance(Duration::from_secs(20)); // valid, below the 48 s min TTL

        #[derive(Debug)]
        struct Hang;
        #[async_trait]
        impl OauthTokenProvider for Hang {
            async fn get_token(&self) -> Result<OauthToken, OauthProviderError> {
                std::future::pending().await
            }
            fn kind(&self) -> &'static str {
                "hang"
            }
        }
        // Same state, hanging provider.
        let hanging =
            TokenCache::with_clock("h", Arc::new(Hang), CacheSettings::default(), clock.clone());
        hanging.lock_state().current = cache.lock_state().current.clone();

        let started = tokio::time::Instant::now();
        let token = hanging.get_with_min_ttl(HANDSHAKE_MIN_TTL).await.unwrap();
        assert_eq!(token.value(), "tok-1");
        assert_eq!(started.elapsed(), INLINE_REFRESH_BUDGET);
        // Backing off now: the next handshake does not wait at all.
        let started = tokio::time::Instant::now();
        hanging.get_with_min_ttl(HANDSHAKE_MIN_TTL).await.unwrap();
        assert!(started.elapsed() < Duration::from_millis(1));
        assert_eq!(hanging.fetch_count(), 1);
    }

    #[tokio::test]
    async fn last_resort_serves_an_unexpired_token_past_the_skew() {
        let clock = Arc::new(ManualClock::new(t0()));
        let provider = TestProvider::new(clock.clone(), Some(Duration::from_secs(60)));
        let cache = cache(provider.clone());
        cache.get().await.unwrap();
        provider.fail(true);
        clock.advance(Duration::from_secs(45)); // past exp - 30 s, before exp
        assert_eq!(cache.get().await.unwrap().value(), "tok-1");
        clock.advance(Duration::from_secs(16)); // expired
        assert!(cache.get().await.is_err());
    }

    #[tokio::test]
    async fn invalidate_drops_only_tokens_older_than_min_refresh_interval() {
        let clock = Arc::new(ManualClock::new(t0()));
        let provider = TestProvider::new(clock.clone(), Some(Duration::from_secs(3600)));
        let cache = cache(provider.clone());
        cache.get().await.unwrap();
        clock.advance(Duration::from_secs(10));
        assert!(!cache.invalidate());
        assert_eq!(cache.get().await.unwrap().value(), "tok-1");
        clock.advance(Duration::from_secs(25));
        assert!(cache.invalidate());
        assert_eq!(cache.get().await.unwrap().value(), "tok-2");
        assert!(!cache.invalidate(), "the new token is fresh");
    }

    #[tokio::test]
    async fn expired_and_empty_tokens_are_errors() {
        #[derive(Debug)]
        struct Bad(&'static str, Option<SystemTime>);
        #[async_trait]
        impl OauthTokenProvider for Bad {
            async fn get_token(&self) -> Result<OauthToken, OauthProviderError> {
                Ok(OauthToken::new(self.0, self.1, None, HashMap::new()))
            }
            fn kind(&self) -> &'static str {
                "bad"
            }
        }
        let clock = Arc::new(ManualClock::new(t0()));
        let expired = TokenCache::with_clock(
            "x",
            Arc::new(Bad("t", Some(t0() - Duration::from_secs(1)))),
            CacheSettings::default(),
            clock.clone(),
        );
        assert!(matches!(
            expired.get().await,
            Err(OauthProviderError::InvalidToken(ref m)) if m.contains("expired")
        ));
        let empty = TokenCache::with_clock(
            "y",
            Arc::new(Bad("", None)),
            CacheSettings::default(),
            clock,
        );
        assert!(matches!(
            empty.get().await,
            Err(OauthProviderError::InvalidToken(_))
        ));
        assert_eq!(empty.failure_count(), 1);
    }

    #[tokio::test]
    async fn jwt_exp_is_used_when_the_provider_gives_no_expiry() {
        #[derive(Debug)]
        struct Jwt(String);
        #[async_trait]
        impl OauthTokenProvider for Jwt {
            async fn get_token(&self) -> Result<OauthToken, OauthProviderError> {
                Ok(OauthToken::new(self.0.clone(), None, None, HashMap::new()))
            }
            fn kind(&self) -> &'static str {
                "jwt"
            }
        }
        let clock = Arc::new(ManualClock::new(t0()));
        let exp = t0() + Duration::from_secs(120);
        let token = jwt::unsigned_jwt(serde_json::json!({
            "exp": exp.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs()
        }));
        let provider = Arc::new(Jwt(token));
        let cache = TokenCache::with_clock("j", provider, CacheSettings::default(), clock.clone());
        cache.get().await.unwrap();
        clock.advance(Duration::from_secs(91)); // past exp - skew
                                                // Not yet expired: the last-resort path re-fetches (same JWT) and,
                                                // as it is past the skew, the fresh copy is still served.
        let _ = cache.get().await;
        assert_eq!(cache.fetch_count(), 2);
        // ... and at most once a second.
        let _ = cache.get().await;
        assert_eq!(cache.fetch_count(), 2);
        clock.advance(Duration::from_secs(1));
        let _ = cache.get().await;
        assert_eq!(cache.fetch_count(), 3);
    }
}
