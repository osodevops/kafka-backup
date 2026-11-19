//! Confluent Schema Registry backup and restore with original schema IDs.
//!
//! Records written with the Confluent serializers start with a magic byte
//! and a 4-byte schema ID. A restore reproduces those bytes exactly, so the
//! target registry must hold the same schemas under the same IDs. This
//! module snapshots a registry next to the topic data and restores it before
//! the records.
//!
//! - [`config`]: the top-level `schema_registry` configuration section.
//! - [`client`]: the REST client (basic, mTLS and OAuth authentication,
//!   rate limit, retries of reads).
//! - [`backup`]: [`SchemaRegistryBackup`], one snapshot pass.
//! - [`snapshot`]: the completeness marker and the object checksums.
//! - [`restore`]: [`SchemaRegistryRestore`]: plan, execute under a mode
//!   guard, verify, report.
//! - [`run`]: what the commands call: [`run::snapshot`] for a backup and
//!   [`run::restore_before_records`] for a restore.
//! - [`context`], [`ordering`], [`models`]: contexts (`:.ctx:subject`),
//!   registration order and the stored / reported types.

pub mod backup;
pub mod client;
pub mod config;
pub mod context;
pub mod error;
pub mod models;
pub mod ordering;
pub mod restore;
pub mod run;
pub mod snapshot;

pub use backup::SchemaRegistryBackup;
pub use client::SchemaRegistryClient;
pub use config::{
    SchemaBackupOptions, SchemaConnectionConfig, SchemaRegistryAuth, SchemaRegistryConfig,
    SchemaRegistryOAuth, SchemaRegistryTls, SchemaRestoreOptions, SchemaRestoreStrategy,
    VersionSelection,
};
pub use error::SchemaRegistryError;
pub use models::{SchemaRegistryManifest, SchemaRestoreAction, SchemaRestoreReport};
pub use restore::{report_key, SchemaRegistryRestore};
