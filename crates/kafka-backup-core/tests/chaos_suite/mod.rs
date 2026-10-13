//! Chaos tests for kafka-backup-core.
//!
//! Each test drives the real engine against a Kafka container and injects
//! faults through the core's `failpoints` feature (the `fail` crate), then
//! checks invariants on what is in storage.

#[path = "../integration_suite/common.rs"]
mod common;

pub mod checkpoint_durability;
