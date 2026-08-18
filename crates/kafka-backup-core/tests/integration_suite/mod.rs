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
//! - Performance Issues: Issue #29 constant lag and throughput tests
//! - Issue #144: OFFSET_OUT_OF_RANGE recovery and data-gap recording
//! - Issue #146: connection-error classification and reconnect (no Docker)
//! - Issue #148: consumer group offsets applied in Phase 3 after restore

pub mod common;
pub mod issue_144_offset_out_of_range;
pub mod issue_146_connection_errors;
pub mod issue_148_offset_reset_after_restore;
pub mod issue_56_missing_topic;
pub mod issue_67_fixes;
pub mod offset_semantics;
pub mod performance_issues;
pub mod pitr_accuracy;
pub mod sasl_gssapi_tests;
pub mod sasl_mock_broker;
pub mod sasl_oauth_tests;
pub mod sasl_plugin_mock_tests;
pub mod sasl_test_fixtures;
pub mod sasl_tests;
pub mod snapshot_backup;
pub mod tls_tests;
pub mod validation;
