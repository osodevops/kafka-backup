//! Chaos test suite entry point.
//!
//! These tests simulate failure scenarios and require:
//! - Docker for Kafka containers
//! - Multi-broker cluster support (for some tests)
//! - Network manipulation capabilities (for some tests)
//!
//! Run with: `cargo test --test chaos_tests`
//! Run ignored tests: `cargo test --test chaos_tests -- --ignored`
//!
//! Note: Most chaos tests are marked as `#[ignore]` because they
//! require specific infrastructure that may not be available in CI.

mod chaos_suite;
