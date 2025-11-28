//! Circuit breaker pattern for fault tolerance.
//!
//! Implements a circuit breaker that helps prevent cascading failures
//! by temporarily blocking operations when a service is failing.

use parking_lot::Mutex;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Circuit breaker state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Circuit is closed, operations proceed normally
    Closed,
    /// Circuit is open, operations are blocked
    Open,
    /// Circuit is half-open, allowing a test request
    HalfOpen,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit
    pub failure_threshold: u32,
    /// Duration to wait before attempting to close the circuit
    pub reset_timeout: Duration,
    /// Number of successes required in half-open state to close
    pub success_threshold: u32,
    /// Name for logging
    pub name: String,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            reset_timeout: Duration::from_secs(30),
            success_threshold: 2,
            name: "circuit".to_string(),
        }
    }
}

/// Circuit breaker for managing failure states
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    state: Mutex<CircuitBreakerState>,
}

struct CircuitBreakerState {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure: Option<Instant>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(config: CircuitBreakerConfig) -> Self {
        info!("Created circuit breaker: {}", config.name);
        Self {
            config,
            state: Mutex::new(CircuitBreakerState {
                state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                last_failure: None,
            }),
        }
    }

    /// Get the current circuit state
    pub fn state(&self) -> CircuitState {
        let mut state = self.state.lock();
        self.maybe_transition_to_half_open(&mut state);
        state.state
    }

    /// Check if the circuit allows the operation
    pub fn is_allowed(&self) -> bool {
        let mut state = self.state.lock();
        self.maybe_transition_to_half_open(&mut state);

        match state.state {
            CircuitState::Closed => true,
            CircuitState::Open => false,
            CircuitState::HalfOpen => true, // Allow test request
        }
    }

    /// Record a successful operation
    pub fn record_success(&self) {
        let mut state = self.state.lock();

        match state.state {
            CircuitState::Closed => {
                // Reset failure count on success
                state.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                state.success_count += 1;
                debug!(
                    "[{}] Success in half-open state ({}/{})",
                    self.config.name, state.success_count, self.config.success_threshold
                );

                if state.success_count >= self.config.success_threshold {
                    info!("[{}] Circuit closing after successful test", self.config.name);
                    state.state = CircuitState::Closed;
                    state.failure_count = 0;
                    state.success_count = 0;
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but reset anyway
                state.state = CircuitState::Closed;
                state.failure_count = 0;
            }
        }
    }

    /// Record a failed operation
    pub fn record_failure(&self) {
        let mut state = self.state.lock();

        state.failure_count += 1;
        state.last_failure = Some(Instant::now());

        match state.state {
            CircuitState::Closed => {
                debug!(
                    "[{}] Failure recorded ({}/{})",
                    self.config.name, state.failure_count, self.config.failure_threshold
                );

                if state.failure_count >= self.config.failure_threshold {
                    warn!(
                        "[{}] Circuit opening after {} failures",
                        self.config.name, state.failure_count
                    );
                    state.state = CircuitState::Open;
                }
            }
            CircuitState::HalfOpen => {
                warn!("[{}] Test request failed, circuit reopening", self.config.name);
                state.state = CircuitState::Open;
                state.success_count = 0;
            }
            CircuitState::Open => {
                // Already open, just update failure count
            }
        }
    }

    /// Reset the circuit breaker to closed state
    pub fn reset(&self) {
        let mut state = self.state.lock();
        info!("[{}] Circuit manually reset", self.config.name);
        state.state = CircuitState::Closed;
        state.failure_count = 0;
        state.success_count = 0;
        state.last_failure = None;
    }

    /// Check if we should transition from open to half-open
    fn maybe_transition_to_half_open(&self, state: &mut CircuitBreakerState) {
        if state.state == CircuitState::Open {
            if let Some(last_failure) = state.last_failure {
                if last_failure.elapsed() >= self.config.reset_timeout {
                    info!(
                        "[{}] Circuit transitioning to half-open after {:?}",
                        self.config.name, self.config.reset_timeout
                    );
                    state.state = CircuitState::HalfOpen;
                    state.success_count = 0;
                }
            }
        }
    }

    /// Execute a fallible operation with circuit breaker protection
    pub async fn call<F, T, E>(&self, operation: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: std::future::Future<Output = Result<T, E>>,
    {
        if !self.is_allowed() {
            return Err(CircuitBreakerError::Open);
        }

        match operation.await {
            Ok(result) => {
                self.record_success();
                Ok(result)
            }
            Err(e) => {
                self.record_failure();
                Err(CircuitBreakerError::Inner(e))
            }
        }
    }
}

/// Error type for circuit breaker operations
#[derive(Debug)]
pub enum CircuitBreakerError<E> {
    /// The circuit is open and blocking operations
    Open,
    /// The underlying operation failed
    Inner(E),
}

impl<E: std::fmt::Display> std::fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CircuitBreakerError::Open => write!(f, "Circuit breaker is open"),
            CircuitBreakerError::Inner(e) => write!(f, "{}", e),
        }
    }
}

impl<E: std::error::Error> std::error::Error for CircuitBreakerError<E> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_circuit_breaker_closed() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        });

        assert_eq!(cb.state(), CircuitState::Closed);
        assert!(cb.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_opens_after_failures() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        });

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);

        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);
        assert!(!cb.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_success_resets_failures() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 3,
            ..Default::default()
        });

        cb.record_failure();
        cb.record_failure();
        cb.record_success();

        // Should be back to 0 failures
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_half_open_transition() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 2,
            reset_timeout: Duration::from_millis(10),
            success_threshold: 1,
            ..Default::default()
        });

        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for reset timeout
        std::thread::sleep(Duration::from_millis(20));

        assert_eq!(cb.state(), CircuitState::HalfOpen);
        assert!(cb.is_allowed());
    }

    #[test]
    fn test_circuit_breaker_closes_after_successes() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 2,
            reset_timeout: Duration::from_millis(10),
            success_threshold: 2,
            ..Default::default()
        });

        // Open the circuit
        cb.record_failure();
        cb.record_failure();
        assert_eq!(cb.state(), CircuitState::Open);

        // Wait for reset timeout
        std::thread::sleep(Duration::from_millis(20));
        assert_eq!(cb.state(), CircuitState::HalfOpen);

        // Record successes
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::HalfOpen);
        cb.record_success();
        assert_eq!(cb.state(), CircuitState::Closed);
    }

    #[tokio::test]
    async fn test_circuit_breaker_call() {
        let cb = CircuitBreaker::new(CircuitBreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        });

        // Successful call
        let result: Result<i32, CircuitBreakerError<&str>> =
            cb.call(async { Ok::<_, &str>(42) }).await;
        assert!(result.is_ok());

        // Failed calls
        let _ = cb.call(async { Err::<i32, _>("error1") }).await;
        let _ = cb.call(async { Err::<i32, _>("error2") }).await;

        // Circuit should be open now
        let result: Result<i32, CircuitBreakerError<&str>> =
            cb.call(async { Ok::<_, &str>(42) }).await;
        assert!(matches!(result, Err(CircuitBreakerError::Open)));
    }
}
