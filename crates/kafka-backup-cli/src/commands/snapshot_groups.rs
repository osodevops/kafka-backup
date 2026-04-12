//! `snapshot-groups` command — snapshot consumer group offsets for backed-up topics.
//!
//! Queries every broker for consumer groups (KRaft-safe), fetches their committed
//! offsets, filters to groups that have offsets on backed-up topics, and saves the
//! result to `{backup_id}/consumer-groups-snapshot.json` in the configured storage
//! backend.
//!
//! The snapshot can be loaded automatically at restore time via
//! `auto_consumer_groups: true` in the restore configuration.

use anyhow::{anyhow, Result};
use bytes::Bytes;
use serde::Serialize;
use std::collections::HashMap;
use tracing::{debug, info, warn};

use kafka_backup_core::{
    config::Mode,
    kafka::{consumer_groups::fetch_offsets, PartitionLeaderRouter},
    manifest::BackupManifest,
    storage::create_backend,
    Config,
};

#[derive(Serialize)]
struct GroupEntry {
    group_id: String,
    /// topic -> partition_id (string) -> committed offset
    offsets: HashMap<String, HashMap<String, i64>>,
}

#[derive(Serialize)]
struct ConsumerGroupsSnapshot {
    snapshot_time: i64,
    groups: Vec<GroupEntry>,
}

pub async fn run(config_path: &str) -> Result<()> {
    info!("Loading configuration from: {}", config_path);

    let config_content = tokio::fs::read_to_string(config_path).await?;
    let config_content = super::config::expand_env_vars(&config_content);
    let config: Config = serde_yaml::from_str(&config_content)?;

    if config.mode != Mode::Backup {
        anyhow::bail!(
            "snapshot-groups requires a backup config (mode: backup), got {:?}",
            config.mode
        );
    }

    let source = config
        .source
        .as_ref()
        .ok_or_else(|| anyhow!("source configuration required for snapshot-groups"))?;

    let backup_id = &config.backup_id;

    // Connect to source cluster via partition-leader router so we can query all brokers
    info!("Connecting to source Kafka cluster...");
    let router = PartitionLeaderRouter::new(source.clone()).await?;

    // Load manifest to know which topics are backed up
    let storage = create_backend(&config.storage)?;
    let manifest_key = format!("{}/manifest.json", backup_id);
    let manifest_data = storage
        .get(&manifest_key)
        .await
        .map_err(|e| anyhow!("Failed to load manifest from {}: {}", manifest_key, e))?;
    let manifest: BackupManifest = serde_json::from_slice(&manifest_data)
        .map_err(|e| anyhow!("Failed to parse manifest: {}", e))?;

    let backed_topics: std::collections::HashSet<String> =
        manifest.topics.iter().map(|t| t.name.clone()).collect();

    info!("Manifest loaded: {} backed-up topics", backed_topics.len());

    // List all consumer groups across ALL brokers (KRaft: each broker is coordinator
    // only for a subset of groups — must query all brokers to get the full list)
    let all_groups = router.list_groups_all_brokers().await?;
    info!(
        "Found {} consumer groups across all brokers",
        all_groups.len()
    );

    // Use bootstrap client for OffsetFetch (broker routes to correct coordinator)
    let bootstrap_client = router.bootstrap_client();

    let mut snapshot_groups: Vec<GroupEntry> = Vec::new();

    for group in &all_groups {
        let group_id = &group.group_id;

        let committed = match fetch_offsets(bootstrap_client, group_id, None).await {
            Ok(c) => c,
            Err(e) => {
                warn!("Failed to fetch offsets for group {}: {}", group_id, e);
                continue;
            }
        };

        if committed.is_empty() {
            debug!("Group {} has no committed offsets, skipping", group_id);
            continue;
        }

        let mut offsets_by_topic: HashMap<String, HashMap<String, i64>> = HashMap::new();
        for co in &committed {
            if backed_topics.contains(&co.topic) && co.offset >= 0 {
                offsets_by_topic
                    .entry(co.topic.clone())
                    .or_default()
                    .insert(co.partition.to_string(), co.offset);
            }
        }

        if offsets_by_topic.is_empty() {
            debug!(
                "Group {} has no offsets on backed-up topics, skipping",
                group_id
            );
            continue;
        }

        info!(
            "Group {}: offsets on {} backed-up topics",
            group_id,
            offsets_by_topic.len()
        );
        snapshot_groups.push(GroupEntry {
            group_id: group_id.clone(),
            offsets: offsets_by_topic,
        });
    }

    info!(
        "Saving snapshot for {} consumer groups",
        snapshot_groups.len()
    );

    let snapshot = ConsumerGroupsSnapshot {
        snapshot_time: chrono::Utc::now().timestamp_millis(),
        groups: snapshot_groups,
    };

    let snapshot_json = serde_json::to_string_pretty(&snapshot)?;
    let snapshot_key = format!("{}/consumer-groups-snapshot.json", backup_id);
    storage
        .put(&snapshot_key, Bytes::from(snapshot_json))
        .await
        .map_err(|e| anyhow!("Failed to save snapshot to {}: {}", snapshot_key, e))?;

    info!("Consumer groups snapshot saved to {}", snapshot_key);

    Ok(())
}
