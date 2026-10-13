//! Restoring captured topic configuration into a target that takes only part
//! of it (Kafka 4.x, Confluent Cloud).
//!
//! For each topic the restore applies configuration to:
//! 1. keys the target's DescribeConfigs does not list, or lists read-only,
//!    are left out ([`prefilter`]);
//! 2. with a `target_profile`, values outside its documented limits are
//!    clamped, or rejected under `unsupported_config_policy: fail`
//!    ([`apply_profile`]);
//! 3. the rest is applied in one IncrementalAlterConfigs request; for every
//!    topic the broker rejects (it applies each topic atomically), each key
//!    is probed on its own with `validate_only` and the accepted ones are
//!    applied ([`apply_topic_configs`]).
//!
//! Skipped keys and adjusted values are logged and listed in the restore
//! report. Under `unsupported_config_policy: fail` nothing is changed when
//! any key would be skipped: the restore fails with the full list.

use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use tracing::{info, warn};

use crate::config::{TargetClusterType, TargetProfile, UnsupportedConfigPolicy};
use crate::kafka::{
    ConfigChange, ConfigChangeResult, ConfigEntry, ConfigOp, ConfigResourceType,
    PartitionLeaderRouter,
};
use crate::manifest::{AdjustedTopicConfig, SkippedTopicConfig, TopicConfigSkipReason};
use crate::{Error, Result};

/// Replication factor of topics a `confluent_cloud` restore creates when
/// `default_replication_factor` is unset: the only one Confluent Cloud uses.
pub(crate) const CONFLUENT_CLOUD_REPLICATION_FACTOR: i16 = 3;

/// Replication factor for topics the restore creates.
pub(crate) fn default_replication_factor(
    configured: Option<i16>,
    profile: Option<TargetProfile>,
) -> i16 {
    configured.unwrap_or(match profile {
        Some(TargetProfile::ConfluentCloud) => CONFLUENT_CLOUD_REPLICATION_FACTOR,
        None => -1,
    })
}

/// A numeric range a target profile allows for one topic configuration key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ConfigLimit {
    name: &'static str,
    min: Option<i64>,
    max: Option<i64>,
}

/// Confluent Cloud topic configuration limits, from "Configuration Reference
/// for Topics in Confluent Cloud"
/// (<https://docs.confluent.io/cloud/current/topics/manage.html>, last
/// published 25 September 2026). Limits only: whether a cluster accepts a
/// key at all (`flush.ms`, `preallocate`, `compression.type` and
/// `message.downconversion.enable` are not editable there) is probed.
///
/// Where a limit depends on the cluster type and none is configured, the
/// most permissive one applies, so nothing is clamped below what the target
/// may accept; the broker rejects the rest.
fn confluent_cloud_limits(cluster: Option<TargetClusterType>) -> [ConfigLimit; 6] {
    use TargetClusterType::{Basic, Dedicated, Enterprise, Standard};
    // 8 MiB on Basic and Standard, 20 MiB on Enterprise and Dedicated.
    let max_message_bytes = match cluster {
        Some(Basic | Standard) => 8_388_608,
        Some(Enterprise | Dedicated) | None => 20_971_520,
    };
    // 6 hours on Dedicated, 7 days on Basic, Standard and Enterprise.
    let min_max_compaction_lag_ms = match cluster {
        Some(Basic | Standard | Enterprise) => 604_800_000,
        Some(Dedicated) | None => 21_600_000,
    };
    [
        ConfigLimit {
            name: "min.insync.replicas",
            min: Some(1),
            max: Some(2),
        },
        ConfigLimit {
            name: "segment.bytes",
            min: Some(52_428_800),
            max: Some(1_073_741_824),
        },
        // Values down to 10 minutes are accepted, but a 4-hour floor applies.
        ConfigLimit {
            name: "segment.ms",
            min: Some(14_400_000),
            max: None,
        },
        ConfigLimit {
            name: "max.message.bytes",
            min: None,
            max: Some(max_message_bytes),
        },
        ConfigLimit {
            name: "max.compaction.lag.ms",
            min: Some(min_max_compaction_lag_ms),
            max: None,
        },
        ConfigLimit {
            name: "delete.retention.ms",
            min: None,
            max: Some(60_566_400_000),
        },
    ]
}

fn profile_limits(
    profile: Option<TargetProfile>,
    cluster: Option<TargetClusterType>,
) -> Vec<ConfigLimit> {
    match profile {
        Some(TargetProfile::ConfluentCloud) => confluent_cloud_limits(cluster).to_vec(),
        None => Vec::new(),
    }
}

fn describe_limit(limit: &ConfigLimit) -> String {
    match (limit.min, limit.max) {
        (Some(min), Some(max)) => format!("Confluent Cloud allows {min} to {max}"),
        (Some(min), None) => format!("Confluent Cloud allows at least {min}"),
        (None, Some(max)) => format!("Confluent Cloud allows at most {max}"),
        (None, None) => "Confluent Cloud".to_string(),
    }
}

/// Split `desired` into the keys the target lists as writable and the keys
/// to skip (not listed, or read-only).
pub(crate) fn prefilter(
    topic: &str,
    desired: BTreeMap<String, String>,
    target: &[ConfigEntry],
) -> (BTreeMap<String, String>, Vec<SkippedTopicConfig>) {
    let reported: HashMap<&str, &ConfigEntry> =
        target.iter().map(|e| (e.name.as_str(), e)).collect();
    let mut kept = BTreeMap::new();
    let mut skipped = Vec::new();
    for (name, value) in desired {
        let reason = match reported.get(name.as_str()) {
            None => Some(TopicConfigSkipReason::NotReportedByTarget),
            Some(entry) if entry.read_only => Some(TopicConfigSkipReason::ReadOnly),
            Some(_) => None,
        };
        match reason {
            Some(reason) => skipped.push(SkippedTopicConfig {
                topic: topic.to_string(),
                name,
                value,
                reason,
                message: None,
            }),
            None => {
                kept.insert(name, value);
            }
        }
    }
    (kept, skipped)
}

/// Clamp the values in `desired` that fall outside the profile's limits and
/// return what changed (the caller rejects them instead under `fail`, see
/// [`limits_error`]).
pub(crate) fn apply_profile(
    topic: &str,
    desired: &mut BTreeMap<String, String>,
    profile: Option<TargetProfile>,
    cluster: Option<TargetClusterType>,
) -> Vec<AdjustedTopicConfig> {
    let mut adjusted = Vec::new();
    for limit in profile_limits(profile, cluster) {
        let Some(value) = desired.get(limit.name) else {
            continue;
        };
        // A non-numeric value is the broker's to reject.
        let Ok(number) = value.trim().parse::<i64>() else {
            continue;
        };
        let clamped = number
            .max(limit.min.unwrap_or(i64::MIN))
            .min(limit.max.unwrap_or(i64::MAX));
        if clamped != number {
            adjusted.push(AdjustedTopicConfig {
                topic: topic.to_string(),
                name: limit.name.to_string(),
                value: value.clone(),
                applied: clamped.to_string(),
                limit: describe_limit(&limit),
            });
        }
    }
    for a in &adjusted {
        desired.insert(a.name.clone(), a.applied.clone());
    }
    adjusted
}

/// The error for values outside the profile limits under
/// `unsupported_config_policy: fail`.
pub(crate) fn limits_error(adjusted: &[AdjustedTopicConfig]) -> Error {
    let listed: Vec<String> = adjusted
        .iter()
        .map(|a| format!("{}: {}={} ({})", a.topic, a.name, a.value, a.limit))
        .collect();
    Error::Config(format!(
        "topic configuration outside the target_profile limits \
         (unsupported_config_policy: fail; nothing was changed): {}",
        listed.join("; ")
    ))
}

/// What a restore did not restore as captured.
#[derive(Debug, Default)]
pub(crate) struct TopicConfigOutcome {
    pub skipped: Vec<SkippedTopicConfig>,
    pub adjusted: Vec<AdjustedTopicConfig>,
}

impl TopicConfigOutcome {
    /// One WARN line per skipped key and per adjusted value.
    pub(crate) fn log(&self) {
        for s in &self.skipped {
            warn!(topic = %s.topic, "topic config skipped: {}", describe_skipped(s));
        }
        for a in &self.adjusted {
            warn!(
                topic = %a.topic,
                "topic config {}={} clamped to {} ({})",
                a.name, a.value, a.applied, a.limit
            );
        }
    }
}

/// The IncrementalAlterConfigs calls the restore makes, behind a trait so
/// the probing logic can be tested without a broker.
#[async_trait]
pub(crate) trait TopicConfigAdmin: Send + Sync {
    async fn alter_topic_configs(
        &self,
        changes: &[ConfigChange],
        validate_only: bool,
    ) -> Result<Vec<ConfigChangeResult>>;
}

#[async_trait]
impl TopicConfigAdmin for PartitionLeaderRouter {
    async fn alter_topic_configs(
        &self,
        changes: &[ConfigChange],
        validate_only: bool,
    ) -> Result<Vec<ConfigChangeResult>> {
        self.incremental_alter_configs_per_resource(changes, validate_only)
            .await
    }
}

fn set_ops(topic: &str, configs: &BTreeMap<String, String>) -> ConfigChange {
    ConfigChange {
        resource_type: ConfigResourceType::Topic,
        resource_name: topic.to_string(),
        ops: configs
            .iter()
            .map(|(name, value)| ConfigOp::Set {
                name: name.clone(),
                value: value.clone(),
            })
            .collect(),
    }
}

fn rejected(topic: &str, name: &str, value: &str, message: String) -> SkippedTopicConfig {
    SkippedTopicConfig {
        topic: topic.to_string(),
        name: name.to_string(),
        value: value.to_string(),
        reason: TopicConfigSkipReason::Rejected,
        message: Some(message),
    }
}

/// Probe each key of the failed topics on its own (`validate_only`), one
/// request per key position across topics. Returns the accepted keys per
/// topic and the rejected ones.
async fn probe_keys(
    admin: &dyn TopicConfigAdmin,
    failed: &[(String, BTreeMap<String, String>)],
) -> Result<(
    Vec<(String, BTreeMap<String, String>)>,
    Vec<SkippedTopicConfig>,
)> {
    let keys: Vec<Vec<(&String, &String)>> =
        failed.iter().map(|(_, cfg)| cfg.iter().collect()).collect();
    let mut accepted: Vec<BTreeMap<String, String>> = vec![BTreeMap::new(); failed.len()];
    let mut skipped = Vec::new();
    let rounds = keys.iter().map(Vec::len).max().unwrap_or(0);
    for round in 0..rounds {
        let mut probed = Vec::new();
        let mut changes = Vec::new();
        for (i, (topic, _)) in failed.iter().enumerate() {
            if let Some((name, value)) = keys[i].get(round) {
                probed.push((i, *name, *value));
                changes.push(ConfigChange {
                    resource_type: ConfigResourceType::Topic,
                    resource_name: topic.clone(),
                    ops: vec![ConfigOp::Set {
                        name: (*name).clone(),
                        value: (*value).clone(),
                    }],
                });
            }
        }
        let results = admin.alter_topic_configs(&changes, true).await?;
        for ((i, name, value), result) in probed.into_iter().zip(results) {
            let topic = &failed[i].0;
            if result.is_success() {
                accepted[i].insert(name.clone(), value.clone());
            } else {
                skipped.push(rejected(topic, name, value, result.describe_error()));
            }
        }
    }
    let accepted = failed
        .iter()
        .zip(accepted)
        .map(|((topic, _), cfg)| (topic.clone(), cfg))
        .collect();
    Ok((accepted, skipped))
}

/// Apply each topic's configuration; for a topic the broker rejects, probe
/// its keys one by one and apply the accepted ones. Returns the rejected
/// keys. Under `fail`, everything is validated first and nothing is changed
/// when a key is rejected.
pub(crate) async fn apply_topic_configs(
    admin: &dyn TopicConfigAdmin,
    plans: Vec<(String, BTreeMap<String, String>)>,
    policy: UnsupportedConfigPolicy,
) -> Result<Vec<SkippedTopicConfig>> {
    let plans: Vec<_> = plans.into_iter().filter(|(_, c)| !c.is_empty()).collect();
    if plans.is_empty() {
        return Ok(Vec::new());
    }
    let validate_first = policy == UnsupportedConfigPolicy::Fail;
    let changes: Vec<ConfigChange> = plans.iter().map(|(t, c)| set_ops(t, c)).collect();
    let results = admin.alter_topic_configs(&changes, validate_first).await?;

    let mut failed = Vec::new();
    for ((topic, configs), result) in plans.iter().zip(&results) {
        if !result.is_success() {
            info!(
                topic = %topic,
                "target rejected the topic configuration ({}); probing its {} key(s) one by one",
                result.describe_error(),
                configs.len()
            );
            failed.push((topic.clone(), configs.clone()));
        }
    }

    let (accepted, mut skipped) = if failed.is_empty() {
        (Vec::new(), Vec::new())
    } else {
        probe_keys(admin, &failed).await?
    };

    if validate_first {
        if !skipped.is_empty() {
            return Err(fail_error(&skipped));
        }
        // Everything validated: apply for real.
        let results = admin.alter_topic_configs(&changes, false).await?;
        if let Some(bad) = results.iter().find(|r| !r.is_success()) {
            return Err(Error::Config(format!(
                "applying the topic configuration of {} failed after it validated: {}",
                bad.resource_name,
                bad.describe_error()
            )));
        }
        return Ok(Vec::new());
    }

    // Apply what each failed topic accepted key by key.
    let retry: Vec<_> = accepted
        .into_iter()
        .filter(|(_, c)| !c.is_empty())
        .collect();
    if !retry.is_empty() {
        let changes: Vec<ConfigChange> = retry.iter().map(|(t, c)| set_ops(t, c)).collect();
        let results = admin.alter_topic_configs(&changes, false).await?;
        for ((topic, configs), result) in retry.iter().zip(results) {
            if !result.is_success() {
                let message = format!(
                    "rejected together with the other accepted keys: {}",
                    result.describe_error()
                );
                skipped.extend(
                    configs
                        .iter()
                        .map(|(name, value)| rejected(topic, name, value, message.clone())),
                );
            }
        }
    }
    Ok(skipped)
}

/// The error for `unsupported_config_policy: fail`.
pub(crate) fn fail_error(skipped: &[SkippedTopicConfig]) -> Error {
    let listed: Vec<String> = skipped.iter().map(describe_skipped).collect();
    Error::Config(format!(
        "the target does not take this topic configuration (unsupported_config_policy: fail; \
         nothing was changed): {}",
        listed.join("; ")
    ))
}

fn describe_skipped(s: &SkippedTopicConfig) -> String {
    let why = match s.reason {
        TopicConfigSkipReason::NotReportedByTarget => "not reported by the target".to_string(),
        TopicConfigSkipReason::ReadOnly => "read-only on the target".to_string(),
        TopicConfigSkipReason::Rejected => format!(
            "rejected: {}",
            s.message.as_deref().unwrap_or("<no message>")
        ),
    };
    format!("{}: {}={} ({why})", s.topic, s.name, s.value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn entry(name: &str, read_only: bool) -> ConfigEntry {
        ConfigEntry {
            name: name.to_string(),
            value: Some("x".to_string()),
            read_only,
            config_source: 5,
            is_default: true,
            is_sensitive: false,
        }
    }

    fn configs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Kafka 4.x topic configs: no message.format.version /
    /// message.downconversion.enable.
    fn kafka4_target() -> Vec<ConfigEntry> {
        [
            "cleanup.policy",
            "flush.ms",
            "preallocate",
            "retention.ms",
            "segment.bytes",
            "min.insync.replicas",
        ]
        .iter()
        .map(|n| entry(n, false))
        .chain([entry("confluent.tier.enable", true)])
        .collect()
    }

    #[test]
    fn prefilter_keeps_reported_writable_keys_only() {
        let desired = configs(&[
            ("cleanup.policy", "compact"),
            ("flush.ms", "1000"),
            ("message.format.version", "2.8-IV1"),
            ("message.downconversion.enable", "true"),
            ("confluent.tier.enable", "true"),
            ("confluent.value.schema.validation", "true"),
        ]);
        let (kept, skipped) = prefilter("t", desired, &kafka4_target());
        assert_eq!(
            kept,
            configs(&[("cleanup.policy", "compact"), ("flush.ms", "1000")])
        );
        let reasons: Vec<_> = skipped
            .iter()
            .map(|s| (s.name.as_str(), s.reason))
            .collect();
        assert_eq!(
            reasons,
            [
                ("confluent.tier.enable", TopicConfigSkipReason::ReadOnly),
                (
                    "confluent.value.schema.validation",
                    TopicConfigSkipReason::NotReportedByTarget
                ),
                (
                    "message.downconversion.enable",
                    TopicConfigSkipReason::NotReportedByTarget
                ),
                (
                    "message.format.version",
                    TopicConfigSkipReason::NotReportedByTarget
                ),
            ]
        );
        assert!(skipped
            .iter()
            .all(|s| s.topic == "t" && s.message.is_none()));
    }

    #[test]
    fn confluent_cloud_profile_clamps_out_of_range_values() {
        let mut desired = configs(&[
            ("min.insync.replicas", "3"),
            ("segment.bytes", "1048576"),
            ("segment.ms", "600000"),
            ("max.message.bytes", "33554432"),
            ("max.compaction.lag.ms", "3600000"),
            ("delete.retention.ms", "99999999999"),
            ("retention.ms", "-1"),
            ("cleanup.policy", "compact"),
        ]);
        let adjusted = apply_profile(
            "orders",
            &mut desired,
            Some(TargetProfile::ConfluentCloud),
            Some(TargetClusterType::Standard),
        );
        let applied: Vec<_> = adjusted
            .iter()
            .map(|a| (a.name.as_str(), a.applied.as_str()))
            .collect();
        assert_eq!(
            applied,
            [
                ("min.insync.replicas", "2"),
                ("segment.bytes", "52428800"),
                ("segment.ms", "14400000"),
                ("max.message.bytes", "8388608"),
                ("max.compaction.lag.ms", "604800000"),
                ("delete.retention.ms", "60566400000"),
            ]
        );
        assert_eq!(desired["min.insync.replicas"], "2");
        assert_eq!(desired["max.message.bytes"], "8388608");
        assert_eq!(desired["retention.ms"], "-1", "no limit: unchanged");
        assert_eq!(adjusted[0].value, "3");
        assert_eq!(adjusted[0].limit, "Confluent Cloud allows 1 to 2");
    }

    #[test]
    fn cluster_type_selects_the_limit_and_unset_is_most_permissive() {
        let clamp = |cluster, bytes: &str| {
            let mut desired = configs(&[("max.message.bytes", bytes)]);
            apply_profile(
                "t",
                &mut desired,
                Some(TargetProfile::ConfluentCloud),
                cluster,
            );
            desired["max.message.bytes"].clone()
        };
        assert_eq!(clamp(Some(TargetClusterType::Basic), "10485760"), "8388608");
        assert_eq!(
            clamp(Some(TargetClusterType::Dedicated), "10485760"),
            "10485760"
        );
        assert_eq!(clamp(None, "10485760"), "10485760");
        assert_eq!(clamp(None, "52428800"), "20971520");
    }

    #[test]
    fn profile_leaves_values_alone_without_a_profile_or_when_not_numeric() {
        let mut desired = configs(&[("min.insync.replicas", "3"), ("segment.ms", "soon")]);
        let before = desired.clone();
        let adjusted = apply_profile("t", &mut desired, None, None);
        assert!(adjusted.is_empty());
        assert_eq!(desired, before);

        let mut desired = configs(&[("segment.ms", "soon")]);
        let adjusted = apply_profile("t", &mut desired, Some(TargetProfile::ConfluentCloud), None);
        assert!(adjusted.is_empty());
    }

    #[test]
    fn limits_error_names_the_value_and_the_limit() {
        let mut desired = configs(&[("min.insync.replicas", "3")]);
        let adjusted = apply_profile(
            "orders",
            &mut desired,
            Some(TargetProfile::ConfluentCloud),
            None,
        );
        let err = limits_error(&adjusted).to_string();
        assert!(
            err.contains("orders: min.insync.replicas=3 (Confluent Cloud allows 1 to 2)"),
            "{err}"
        );
        assert!(err.contains("nothing was changed"), "{err}");
    }

    #[test]
    fn replication_factor_default_follows_the_profile() {
        assert_eq!(default_replication_factor(None, None), -1);
        assert_eq!(
            default_replication_factor(None, Some(TargetProfile::ConfluentCloud)),
            3
        );
        assert_eq!(
            default_replication_factor(Some(2), Some(TargetProfile::ConfluentCloud)),
            2
        );
    }

    /// One recorded call: `validate_only` and, per topic, the keys sent.
    type Call = (bool, Vec<(String, Vec<String>)>);

    /// A broker that rejects some keys: a request fails for a topic when any
    /// of its keys is rejected (atomically, nothing applied). Records every
    /// call and what was applied.
    #[derive(Default)]
    struct FakeAdmin {
        rejects: Vec<(&'static str, &'static str)>,
        calls: Mutex<Vec<Call>>,
        applied: Mutex<BTreeMap<String, BTreeMap<String, String>>>,
    }

    impl FakeAdmin {
        fn rejecting(rejects: &[(&'static str, &'static str)]) -> Self {
            Self {
                rejects: rejects.to_vec(),
                ..Self::default()
            }
        }

        fn applied(&self) -> BTreeMap<String, BTreeMap<String, String>> {
            self.applied.lock().unwrap().clone()
        }

        fn calls(&self) -> Vec<Call> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl TopicConfigAdmin for FakeAdmin {
        async fn alter_topic_configs(
            &self,
            changes: &[ConfigChange],
            validate_only: bool,
        ) -> Result<Vec<ConfigChangeResult>> {
            self.calls.lock().unwrap().push((
                validate_only,
                changes
                    .iter()
                    .map(|c| {
                        (
                            c.resource_name.clone(),
                            c.ops
                                .iter()
                                .map(|op| match op {
                                    ConfigOp::Set { name, .. } | ConfigOp::Delete { name } => {
                                        name.clone()
                                    }
                                })
                                .collect(),
                        )
                    })
                    .collect(),
            ));
            Ok(changes
                .iter()
                .map(|change| {
                    let bad = change.ops.iter().find_map(|op| match op {
                        ConfigOp::Set { name, .. } => self
                            .rejects
                            .iter()
                            .find(|(key, _)| key == name)
                            .map(|(key, msg)| format!("{key}: {msg}")),
                        ConfigOp::Delete { .. } => None,
                    });
                    if bad.is_none() && !validate_only {
                        let mut applied = self.applied.lock().unwrap();
                        let topic = applied.entry(change.resource_name.clone()).or_default();
                        for op in &change.ops {
                            if let ConfigOp::Set { name, value } = op {
                                topic.insert(name.clone(), value.clone());
                            }
                        }
                    }
                    ConfigChangeResult {
                        resource_type: change.resource_type,
                        resource_name: change.resource_name.clone(),
                        error_code: if bad.is_some() { 44 } else { 0 },
                        error_message: bad,
                    }
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn accepted_configs_are_applied_in_one_request() {
        let admin = FakeAdmin::default();
        let skipped = apply_topic_configs(
            &admin,
            vec![
                ("a".into(), configs(&[("flush.ms", "1")])),
                ("b".into(), configs(&[("retention.ms", "2")])),
                ("empty".into(), BTreeMap::new()),
            ],
            UnsupportedConfigPolicy::Skip,
        )
        .await
        .unwrap();
        assert!(skipped.is_empty());
        assert_eq!(admin.calls().len(), 1);
        assert_eq!(admin.applied()["a"], configs(&[("flush.ms", "1")]));
        assert_eq!(admin.applied()["b"], configs(&[("retention.ms", "2")]));
    }

    #[tokio::test]
    async fn rejected_keys_are_probed_one_by_one_and_the_rest_applied() {
        let admin = FakeAdmin::rejecting(&[
            ("flush.ms", "Topic config flush.ms is not editable"),
            ("preallocate", "not editable"),
        ]);
        let skipped = apply_topic_configs(
            &admin,
            vec![
                (
                    "orders".into(),
                    configs(&[
                        ("cleanup.policy", "compact"),
                        ("flush.ms", "1000"),
                        ("preallocate", "true"),
                        ("retention.ms", "-1"),
                    ]),
                ),
                ("payments".into(), configs(&[("retention.ms", "1000")])),
                ("logs".into(), configs(&[("flush.ms", "5")])),
            ],
            UnsupportedConfigPolicy::Skip,
        )
        .await
        .unwrap();

        let applied = admin.applied();
        assert_eq!(
            applied["orders"],
            configs(&[("cleanup.policy", "compact"), ("retention.ms", "-1")])
        );
        assert_eq!(applied["payments"], configs(&[("retention.ms", "1000")]));
        assert!(!applied.contains_key("logs"), "nothing left to apply");

        let mut rejected: Vec<_> = skipped
            .iter()
            .map(|s| (s.topic.as_str(), s.name.as_str(), s.reason))
            .collect();
        rejected.sort_by_key(|(topic, name, _)| (*topic, *name));
        assert_eq!(
            rejected,
            [
                ("logs", "flush.ms", TopicConfigSkipReason::Rejected),
                ("orders", "flush.ms", TopicConfigSkipReason::Rejected),
                ("orders", "preallocate", TopicConfigSkipReason::Rejected),
            ]
        );
        let message = skipped[0].message.as_deref().unwrap();
        assert!(
            message.starts_with("error_code=44 (PolicyViolation): "),
            "{message}"
        );

        // Bulk apply, then one validate-only probe per key position across the
        // failed topics (orders has 4 keys, logs 1), then the retry.
        let calls = admin.calls();
        assert_eq!(calls.len(), 1 + 4 + 1, "{calls:?}");
        assert!(!calls[0].0);
        assert!(calls[1..5].iter().all(|(validate_only, _)| *validate_only));
        assert_eq!(
            calls[1].1,
            [
                ("orders".to_string(), vec!["cleanup.policy".to_string()]),
                ("logs".to_string(), vec!["flush.ms".to_string()]),
            ]
        );
        assert_eq!(
            calls[5],
            (
                false,
                vec![(
                    "orders".to_string(),
                    vec!["cleanup.policy".to_string(), "retention.ms".to_string()]
                )]
            )
        );
    }

    #[tokio::test]
    async fn fail_policy_changes_nothing_and_lists_every_rejected_key() {
        let admin = FakeAdmin::rejecting(&[("flush.ms", "not editable")]);
        let err = apply_topic_configs(
            &admin,
            vec![
                (
                    "orders".into(),
                    configs(&[("cleanup.policy", "compact"), ("flush.ms", "1000")]),
                ),
                ("payments".into(), configs(&[("retention.ms", "1000")])),
            ],
            UnsupportedConfigPolicy::Fail,
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(err.contains("nothing was changed"), "{err}");
        assert!(
            err.contains("orders: flush.ms=1000 (rejected: error_code=44"),
            "{err}"
        );
        assert!(!err.contains("cleanup.policy"), "{err}");
        assert!(admin.applied().is_empty(), "validate-only calls only");
        assert!(admin
            .calls()
            .iter()
            .all(|(validate_only, _)| *validate_only));
    }

    #[tokio::test]
    async fn fail_policy_applies_when_everything_validates() {
        let admin = FakeAdmin::default();
        let skipped = apply_topic_configs(
            &admin,
            vec![("orders".into(), configs(&[("retention.ms", "1")]))],
            UnsupportedConfigPolicy::Fail,
        )
        .await
        .unwrap();
        assert!(skipped.is_empty());
        let calls = admin.calls();
        assert_eq!(calls.len(), 2);
        assert!(calls[0].0, "validated first");
        assert!(!calls[1].0, "then applied");
        assert_eq!(admin.applied()["orders"], configs(&[("retention.ms", "1")]));
    }

    #[test]
    fn fail_error_lists_reasons() {
        let skipped = vec![
            SkippedTopicConfig {
                topic: "t".into(),
                name: "message.format.version".into(),
                value: "2.8".into(),
                reason: TopicConfigSkipReason::NotReportedByTarget,
                message: None,
            },
            SkippedTopicConfig {
                topic: "t".into(),
                name: "confluent.tier.enable".into(),
                value: "true".into(),
                reason: TopicConfigSkipReason::ReadOnly,
                message: None,
            },
        ];
        let err = fail_error(&skipped).to_string();
        assert!(
            err.contains("t: message.format.version=2.8 (not reported by the target)"),
            "{err}"
        );
        assert!(
            err.contains("t: confluent.tier.enable=true (read-only on the target)"),
            "{err}"
        );
    }
}
