//! Integration tests for kafka-backup-core.
//!
//! These tests require Docker and use Testcontainers to spin up
//! real Kafka brokers for testing against actual broker behavior.
//!
//! Test categories:
//! - PITR Accuracy: Point-in-time recovery precision tests
//! - Offset Semantics: Consumer group offset handling tests
//! - Bulk Offset Reset: Parallel offset reset operations

pub mod common;
pub mod offset_semantics;
pub mod pitr_accuracy;
