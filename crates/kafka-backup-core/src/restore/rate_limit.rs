//! Restore-wide produce rate limits.
//!
//! `rate_limit_records_per_sec` and `rate_limit_bytes_per_sec` each become
//! one token bucket shared by every partition (and every repartition
//! writer) of a restore, so the configured figure is the restore's total
//! rate, not a per-partition rate.

use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::config::RestoreOptions;

/// A token bucket that refills at `rate` tokens per second up to one
/// second's worth. [`TokenBucket::acquire`] takes its tokens at once and,
/// when that leaves the bucket in debt, sleeps until the debt is repaid:
/// concurrent callers queue behind each other's debt, so the total rate
/// holds however many tasks share the bucket and however large each request
/// is (a request larger than the burst simply waits longer).
#[derive(Debug)]
pub(crate) struct TokenBucket {
    rate: f64,
    capacity: f64,
    state: Mutex<BucketState>,
}

#[derive(Debug)]
struct BucketState {
    tokens: f64,
    refilled_at: Instant,
}

impl TokenBucket {
    /// A bucket refilling at `rate_per_sec` (> 0) tokens per second. It
    /// starts empty, so the first request is paced like every other one.
    pub(crate) fn new(rate_per_sec: u64) -> Self {
        let rate = rate_per_sec.max(1) as f64;
        Self {
            rate,
            capacity: rate,
            state: Mutex::new(BucketState {
                tokens: 0.0,
                refilled_at: Instant::now(),
            }),
        }
    }

    /// Take `amount` tokens, sleeping as long as needed to stay within the
    /// rate.
    pub(crate) async fn acquire(&self, amount: u64) {
        if amount == 0 {
            return;
        }
        let wait = {
            let mut state = self.state.lock().await;
            let now = Instant::now();
            let elapsed = now.duration_since(state.refilled_at).as_secs_f64();
            state.tokens = (state.tokens + elapsed * self.rate).min(self.capacity);
            state.refilled_at = now;
            state.tokens -= amount as f64;
            if state.tokens >= 0.0 {
                Duration::ZERO
            } else {
                Duration::from_secs_f64(-state.tokens / self.rate)
            }
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

/// The record and byte buckets of one restore.
#[derive(Debug, Default)]
pub(crate) struct RestoreRateLimiter {
    records: Option<TokenBucket>,
    bytes: Option<TokenBucket>,
}

impl RestoreRateLimiter {
    /// Buckets for the limits set in `options`; `None` when neither is set.
    pub(crate) fn from_options(options: &RestoreOptions) -> Option<Self> {
        let records = options.rate_limit_records_per_sec.map(TokenBucket::new);
        let bytes = options.rate_limit_bytes_per_sec.map(TokenBucket::new);
        (records.is_some() || bytes.is_some()).then_some(Self { records, bytes })
    }

    /// Wait until a batch of `records` records and `bytes` bytes may be
    /// produced.
    pub(crate) async fn acquire(&self, records: u64, bytes: u64) {
        if let Some(bucket) = &self.records {
            bucket.acquire(records).await;
        }
        if let Some(bucket) = &self.bytes {
            bucket.acquire(bytes).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[tokio::test(start_paused = true)]
    async fn single_caller_is_paced_at_the_rate() {
        let bucket = TokenBucket::new(100);
        let start = Instant::now();
        for _ in 0..10 {
            bucket.acquire(50).await;
        }
        // 500 tokens at 100/s from an empty bucket: 5 seconds.
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(4_990) && elapsed <= Duration::from_millis(5_010),
            "{elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_rate_is_shared_by_concurrent_callers() {
        let bucket = Arc::new(TokenBucket::new(1_000));
        let start = Instant::now();
        let tasks: Vec<_> = (0..8)
            .map(|_| {
                let bucket = Arc::clone(&bucket);
                tokio::spawn(async move {
                    for _ in 0..5 {
                        bucket.acquire(100).await;
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        // 8 partitions x 500 tokens = 4000 tokens at 1000/s: 4 seconds in
        // total, not the 0.5 s a per-partition limit would give.
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(3_990) && elapsed <= Duration::from_millis(4_010),
            "{elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_request_larger_than_the_burst_waits_proportionally() {
        let bucket = TokenBucket::new(10);
        let start = Instant::now();
        bucket.acquire(35).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(3_490) && elapsed <= Duration::from_millis(3_510),
            "{elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn idle_time_refills_at_most_one_second_of_burst() {
        let bucket = TokenBucket::new(100);
        tokio::time::sleep(Duration::from_secs(60)).await;
        let start = Instant::now();
        // 100 tokens of burst are available after the idle minute ...
        bucket.acquire(100).await;
        assert!(start.elapsed() < Duration::from_millis(10));
        // ... and no more: the next 100 take a second.
        bucket.acquire(100).await;
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(990) && elapsed <= Duration::from_millis(1_010),
            "{elapsed:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn limiter_applies_both_record_and_byte_limits() {
        let options = RestoreOptions {
            rate_limit_records_per_sec: Some(1_000),
            rate_limit_bytes_per_sec: Some(10_000),
            ..RestoreOptions::default()
        };
        let limiter = RestoreRateLimiter::from_options(&options).expect("limits set");
        let start = Instant::now();
        // 100 records but 50_000 bytes: the byte limit dominates (5 s).
        for _ in 0..10 {
            limiter.acquire(10, 5_000).await;
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(4_990) && elapsed <= Duration::from_millis(5_010),
            "{elapsed:?}"
        );

        assert!(RestoreRateLimiter::from_options(&RestoreOptions::default()).is_none());
    }
}
