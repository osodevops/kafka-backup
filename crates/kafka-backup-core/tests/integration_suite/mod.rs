//! Integration tests for kafka-backup-core.
//!
//! These tests require Docker and use Testcontainers to spin up
//! real Kafka brokers for testing against actual broker behavior.
//!
//! Test categories:
//! - Azure: Azure Blob backend against Azurite (account key, SAS, backup/restore)
//! - PITR Accuracy: Point-in-time recovery precision tests
//! - Offset Semantics: Consumer group offset handling tests
//! - TLS Security: SSL/TLS certificate handling tests
//! - Snapshot Backup: stop_at_current_offsets feature tests
//! - Performance Issues: Issue #29 constant lag and throughput tests
//! - Issue #144: OFFSET_OUT_OF_RANGE recovery and data-gap recording
//! - Issue #146: connection-error classification and reconnect (no Docker)
//! - Issue #148: consumer group offsets applied in Phase 3 after restore
//! - Issue #154: default offset headers are documented, logged, and strippable on restore
//! - Issue #155: null header values survive backup and restore
//! - Issue #167: backup.on_missing_topic warn/fail behaviour
//! - Issue #197: circuit-breaker config + scoped connection eviction
//! - Offset mapping across gaps: groups resume on the next unconsumed record
//! - Restore produce path (mock broker, no Docker): idempotent produce with
//!   byte-identical retries, the fallback, byte-capped batches, the
//!   max.message.bytes pre-check
//! - Restore checkpoint (mock broker): written at start, only acknowledged
//!   work, kill-and-resume with zero duplicates, another-writer detection,
//!   remote copy, sibling cancellation, repartitioned segment completion
//! - Idempotent restore against Kafka: produce responses lost through a
//!   fault proxy leave zero duplicates
//! - Read-committed backup of transactional topics (aborted records, markers,
//!   the last stable offset), with a test-support transactional producer
//! - strimzi-backup-operator#57: snapshot progress gauges describe the current run

pub mod azure_azurite;
pub mod common;
pub mod group_coordinator_mock;
pub mod issue_144_offset_out_of_range;
pub mod issue_146_connection_errors;
pub mod issue_148_offset_reset_after_restore;
pub mod issue_154_offset_headers;
pub mod issue_155_null_header_value;
pub mod issue_167_on_missing_topic;
pub mod issue_169_retention;
pub mod issue_197_broker_pause;
pub mod issue_197_circuit_breaker;
pub mod issue_224_group_coordinator;
pub mod issue_56_missing_topic;
pub mod issue_57_incremental_snapshot_target;
pub mod issue_67_fixes;
pub mod kafka_fault_proxy;
pub mod offset_mapping_gaps;
pub mod offset_semantics;
pub mod performance_issues;
pub mod pitr_accuracy;
pub mod preflight_e2e;
pub mod produce_mock_broker;
pub mod read_committed_backup;
pub mod restore_checkpoint_resume;
pub mod restore_idempotent_docker;
pub mod restore_produce_path;
pub mod sasl_gssapi_tests;
pub mod sasl_mock_broker;
pub mod sasl_oauth_tests;
pub mod sasl_plugin_mock_tests;
pub mod sasl_test_fixtures;
pub mod sasl_tests;
pub mod snapshot_backup;
pub mod tls_tests;
pub mod transactional_producer;
pub mod validation;
