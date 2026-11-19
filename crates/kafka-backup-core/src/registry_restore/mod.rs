//! Registry restore planning, execution, verification and wire transformations.
pub mod engine;
pub mod snapshot;
pub mod wire;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Confluent,
    Apicurio,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IdPolicy {
    #[default]
    Preserve,
    Rewrite,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CoordinateMapping {
    pub source_group: String,
    pub source_name: String,
    pub target_group: String,
    pub target_name: String,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestoreConfig {
    pub source: Option<Provider>,
    pub destination: Option<Provider>,
    #[serde(default)]
    pub id_policy: IdPolicy,
    #[serde(default)]
    pub coordinates: Vec<CoordinateMapping>,
    /// Exact source topic names; raw key and value profiles must be explicit.
    #[serde(default)]
    pub serializers: BTreeMap<String, wire::TopicProfile>,
    #[serde(default)]
    pub restore_global_settings: bool,
    #[serde(default)]
    pub dry_run: bool,
}
