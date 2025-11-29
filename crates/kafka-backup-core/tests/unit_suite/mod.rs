//! Unit tests for kafka-backup-core.
//!
//! These tests focus on pure functions and data structures without I/O.
//! They run quickly and don't require Docker or external services.

pub mod backup;
pub mod compression;
pub mod helpers;
pub mod offset_mapping;
pub mod restore;
