//! Integration tests for kafka-backup-core.
//!
//! These tests require Docker and use Testcontainers to spin up
//! real Kafka brokers for testing against actual broker behavior.
//!
//! Test categories:
//! - PITR Accuracy: Point-in-time recovery precision tests
//! - Offset Semantics: Consumer group offset handling tests
//! - TLS Security: SSL/TLS certificate handling tests
//! - Snapshot Backup: stop_at_current_offsets feature tests

pub mod common;
pub mod offset_semantics;
pub mod pitr_accuracy;
pub mod snapshot_backup;
pub mod tls_tests;
