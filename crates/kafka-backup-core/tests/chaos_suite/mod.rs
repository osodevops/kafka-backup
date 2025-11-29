//! Chaos engineering tests for kafka-backup-core.
//!
//! These tests simulate failure scenarios to verify resilience:
//! - Broker failures during backup/restore
//! - Network partitions during offset operations
//! - Partition leader elections
//!
//! These tests are slower and more resource-intensive than regular
//! integration tests. They should be run in a dedicated CI job.

pub mod broker_failures;
pub mod network_partitions;
pub mod partition_leadership;
