//! Restore-wide produce rate limits.
//!
//! `rate_limit_records_per_sec` and `rate_limit_bytes_per_sec` each become
//! one token bucket shared by every partition (and every repartition
//! writer) of a restore, so the configured figure is the restore's total
//! rate, not a per-partition rate. See [`crate::rate_limit`].

use crate::config::RestoreOptions;

pub(crate) use crate::rate_limit::RateLimiter as RestoreRateLimiter;

/// The restore's limiter; `None` when no limit is set.
pub(crate) fn limiter_for(options: &RestoreOptions) -> Option<RestoreRateLimiter> {
    RestoreRateLimiter::from_limits(
        options.rate_limit_records_per_sec,
        options.rate_limit_bytes_per_sec,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_options_set_the_limits() {
        let options = RestoreOptions {
            rate_limit_records_per_sec: Some(1_000),
            rate_limit_bytes_per_sec: Some(10_000),
            ..RestoreOptions::default()
        };
        assert!(limiter_for(&options).is_some());
        assert!(limiter_for(&RestoreOptions::default()).is_none());
    }
}
