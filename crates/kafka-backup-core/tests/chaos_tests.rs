//! Chaos suite entry point: crash-consistency of backup checkpoints.
//!
//! Needs Docker (testcontainers Kafka) and the `failpoints` feature:
//!
//! ```bash
//! cargo test -p kafka-backup-core --features failpoints --test chaos_tests \
//!     -- --ignored --nocapture
//! # nightly depth:
//! KAFKA_BACKUP_CHAOS_SEEDS=100 cargo test -p kafka-backup-core --features failpoints \
//!     --test chaos_tests -- --ignored --nocapture
//! # replay one seed:
//! KAFKA_BACKUP_CHAOS_SEED_BASE=1234 KAFKA_BACKUP_CHAOS_SEEDS=1 cargo test ...
//! ```
//!
//! Without the feature this file compiles to an empty test binary.
#![cfg(feature = "failpoints")]

mod chaos_suite;
