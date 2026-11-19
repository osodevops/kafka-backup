//! Schema Registry backup and restore against mocked registries (wiremock).
//!
//! No Docker: the backup tests mock single endpoints, the restore tests use
//! a stateful fake registry that models the rules the restore depends on
//! (IMPORT mode, ID and version uniqueness, soft deletes, contexts).
//!
//! Run with: `cargo test --test schema_registry_tests`
//! The real-registry tests are in `schema_registry_real.rs`.

mod schema_registry_suite;
