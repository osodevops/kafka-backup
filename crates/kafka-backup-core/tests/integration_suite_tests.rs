//! Integration test suite entry point (new tests).
//!
//! These tests require Docker and use Testcontainers to run real Kafka brokers.
//! Tests include PITR accuracy, offset semantics, and bulk operations.
//!
//! Run with: `cargo test --test integration_suite_tests`
//! Run ignored tests: `cargo test --test integration_suite_tests -- --ignored`

mod integration_suite;
