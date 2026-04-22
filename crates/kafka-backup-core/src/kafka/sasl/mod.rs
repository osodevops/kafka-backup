//! Pluggable SASL mechanism support.
//!
//! The OSS client speaks `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`
//! natively (see `client::authenticate`). Modern managed-cloud Kafka
//! services also need `OAUTHBEARER` (RFC 7628) with service-specific
//! token providers (AWS MSK IAM, Confluent Cloud, Azure Event Hubs,
//! Strimzi + Keycloak). Rather than bundling heavyweight vendor SDKs
//! into the OSS build, the [`SaslMechanismPlugin`] trait lets downstream
//! crates implement those mechanisms without modifying this crate.
//!
//! See `docs/PRD-sasl-mechanism-plugin.md` for the full design.

pub mod plugin;
pub(crate) mod reauth;

#[cfg(feature = "gssapi")]
pub mod gssapi;

#[cfg(feature = "gssapi")]
pub use gssapi::{GssapiPlugin, GssapiPluginError, GssapiPluginFactory};

pub use plugin::{
    SaslAuthOutcome, SaslMechanismPlugin, SaslMechanismPluginFactory,
    SaslMechanismPluginFactoryHandle, SaslMechanismPluginHandle, SaslPluginError,
    SharedPluginFactory,
};
