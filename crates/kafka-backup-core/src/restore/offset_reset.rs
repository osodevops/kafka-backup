//! Offset reset plan generation and execution (Phase 3 of three-phase restore).
//!
//! This module implements the final phase of the three-phase offset remapping system:
//! - Generate offset reset plans from offset mappings
//! - Execute offset commits to consumer groups
//! - Generate shell scripts for manual offset reset
//! - Support dry-run mode for validation

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info, warn};

use crate::kafka::{commit_offsets, fetch_offsets, KafkaClient};
#[cfg(test)]
use crate::manifest::ConsumerGroupOffset;
use crate::manifest::{ConsumerGroupOffsets, OffsetMapping};
use crate::Result;

/// Offset reset plan for consumer groups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetResetPlan {
    /// Consumer groups to reset
    pub groups: Vec<GroupResetPlan>,

    /// Generation timestamp
    pub generated_at: i64,

    /// Strategy used to generate the plan
    pub strategy: String,

    /// Whether this is a dry-run plan
    pub dry_run: bool,

    /// Source backup ID
    pub backup_id: Option<String>,

    /// Source cluster ID
    pub source_cluster_id: Option<String>,

    /// Target cluster bootstrap servers
    pub target_bootstrap_servers: Vec<String>,
}

/// Reset plan for a single consumer group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupResetPlan {
    /// Consumer group ID
    pub group_id: String,

    /// Partition reset plans
    pub partitions: Vec<PartitionResetPlan>,

    /// Total partitions to reset
    pub partition_count: usize,

    /// Whether all partitions have valid target offsets
    pub complete: bool,
}

/// Reset plan for a single partition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionResetPlan {
    /// Topic name
    pub topic: String,

    /// Partition ID
    pub partition: i32,

    /// Source cluster committed offset
    pub source_offset: i64,

    /// Target cluster offset to reset to
    pub target_offset: i64,

    /// Record timestamp at this offset
    pub timestamp: i64,

    /// Optional commit metadata
    pub metadata: Option<String>,
}

/// Result of executing an offset reset plan
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OffsetResetReport {
    /// Execution timestamp
    pub executed_at: i64,

    /// Groups that were reset
    pub groups_reset: Vec<GroupResetResult>,

    /// Total partitions reset
    pub partitions_reset: u64,

    /// Total errors encountered
    pub errors: Vec<String>,

    /// Whether the reset was successful
    pub success: bool,

    /// Duration in milliseconds
    pub duration_ms: u64,
}

/// Result of resetting a single consumer group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupResetResult {
    /// Consumer group ID
    pub group_id: String,

    /// Partitions successfully reset
    pub partitions_reset: u64,

    /// Partitions that failed to reset
    pub partitions_failed: u64,

    /// Error messages for failed partitions
    pub errors: Vec<String>,
}

/// Strategy for executing offset reset
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OffsetResetStrategy {
    /// Generate plan only, require manual review and execution
    #[default]
    Manual,
    /// Automatically apply offset resets
    Auto,
    /// Generate plan but don't apply (dry-run)
    DryRun,
}

/// Offset reset executor
pub struct OffsetResetExecutor {
    /// Target Kafka client
    client: Option<KafkaClient>,

    /// Bootstrap servers for shell script generation
    bootstrap_servers: Vec<String>,
}

impl OffsetResetExecutor {
    /// Create a new executor with a Kafka client
    pub fn new(client: KafkaClient, bootstrap_servers: Vec<String>) -> Self {
        Self {
            client: Some(client),
            bootstrap_servers,
        }
    }

    /// Create a new executor without a client (for plan generation only)
    pub fn new_offline(bootstrap_servers: Vec<String>) -> Self {
        Self {
            client: None,
            bootstrap_servers,
        }
    }

    /// Generate an offset reset plan from an offset mapping
    pub async fn generate_plan(
        &self,
        offset_mapping: &OffsetMapping,
        group_ids: &[String],
        strategy: OffsetResetStrategy,
    ) -> Result<OffsetResetPlan> {
        let mut groups = Vec::new();

        for group_id in group_ids {
            let group_plan = self.generate_group_plan(offset_mapping, group_id).await?;
            groups.push(group_plan);
        }

        let plan = OffsetResetPlan {
            groups,
            generated_at: chrono::Utc::now().timestamp_millis(),
            strategy: format!("{:?}", strategy),
            dry_run: strategy == OffsetResetStrategy::DryRun,
            backup_id: None,
            source_cluster_id: offset_mapping.source_cluster_id.clone(),
            target_bootstrap_servers: self.bootstrap_servers.clone(),
        };

        info!(
            "Generated offset reset plan for {} groups with {} total partitions",
            plan.groups.len(),
            plan.groups.iter().map(|g| g.partition_count).sum::<usize>()
        );

        Ok(plan)
    }

    /// Generate plan for a single consumer group
    async fn generate_group_plan(
        &self,
        offset_mapping: &OffsetMapping,
        group_id: &str,
    ) -> Result<GroupResetPlan> {
        let mut partitions = Vec::new();
        let mut complete = true;

        // Check if we have consumer group offsets in the mapping
        if let Some(group_offsets) = offset_mapping.consumer_groups.get(group_id) {
            for (topic, topic_offsets) in &group_offsets.offsets {
                for (partition, offset_info) in topic_offsets {
                    if let Some(target_offset) = offset_info.target_offset {
                        partitions.push(PartitionResetPlan {
                            topic: topic.clone(),
                            partition: *partition,
                            source_offset: offset_info.source_offset,
                            target_offset,
                            timestamp: offset_info.timestamp,
                            metadata: offset_info.metadata.clone(),
                        });
                    } else {
                        // Try to calculate target offset from detailed mapping
                        if let Some(target) = offset_mapping.lookup_target_offset(
                            topic,
                            *partition,
                            offset_info.source_offset,
                        ) {
                            partitions.push(PartitionResetPlan {
                                topic: topic.clone(),
                                partition: *partition,
                                source_offset: offset_info.source_offset,
                                target_offset: target,
                                timestamp: offset_info.timestamp,
                                metadata: offset_info.metadata.clone(),
                            });
                        } else {
                            warn!(
                                "No target offset mapping for {}:{}:{} - skipping",
                                group_id, topic, partition
                            );
                            complete = false;
                        }
                    }
                }
            }
        } else if let Some(client) = &self.client {
            // Fetch current offsets from source cluster and calculate target offsets
            let committed = fetch_offsets(client, group_id, None).await?;

            for offset in committed {
                if offset.error_code == 0 {
                    if let Some(target) = offset_mapping.lookup_target_offset(
                        &offset.topic,
                        offset.partition,
                        offset.offset,
                    ) {
                        partitions.push(PartitionResetPlan {
                            topic: offset.topic,
                            partition: offset.partition,
                            source_offset: offset.offset,
                            target_offset: target,
                            timestamp: chrono::Utc::now().timestamp_millis(),
                            metadata: offset.metadata,
                        });
                    } else {
                        warn!(
                            "No target offset mapping for {}:{}:{} - skipping",
                            group_id, offset.topic, offset.partition
                        );
                        complete = false;
                    }
                }
            }
        }

        let partition_count = partitions.len();

        Ok(GroupResetPlan {
            group_id: group_id.to_string(),
            partitions,
            partition_count,
            complete,
        })
    }

    /// Execute an offset reset plan
    pub async fn execute_plan(&self, plan: &OffsetResetPlan) -> Result<OffsetResetReport> {
        let start_time = std::time::Instant::now();
        let client = self.client.as_ref().ok_or_else(|| {
            crate::Error::Config("Kafka client required to execute offset reset".to_string())
        })?;

        let mut groups_reset = Vec::new();
        let mut total_errors = Vec::new();
        let mut total_partitions_reset = 0u64;

        for group_plan in &plan.groups {
            let result = self.execute_group_reset(client, group_plan).await;

            match result {
                Ok(group_result) => {
                    total_partitions_reset += group_result.partitions_reset;
                    if !group_result.errors.is_empty() {
                        total_errors.extend(group_result.errors.clone());
                    }
                    groups_reset.push(group_result);
                }
                Err(e) => {
                    let error_msg = format!("Failed to reset group {}: {}", group_plan.group_id, e);
                    total_errors.push(error_msg.clone());
                    groups_reset.push(GroupResetResult {
                        group_id: group_plan.group_id.clone(),
                        partitions_reset: 0,
                        partitions_failed: group_plan.partition_count as u64,
                        errors: vec![error_msg],
                    });
                }
            }
        }

        let duration_ms = start_time.elapsed().as_millis() as u64;

        let report = OffsetResetReport {
            executed_at: chrono::Utc::now().timestamp_millis(),
            groups_reset,
            partitions_reset: total_partitions_reset,
            errors: total_errors.clone(),
            success: total_errors.is_empty(),
            duration_ms,
        };

        if report.success {
            info!(
                "Offset reset completed successfully: {} partitions reset in {}ms",
                report.partitions_reset, report.duration_ms
            );
        } else {
            warn!(
                "Offset reset completed with errors: {} partitions reset, {} errors in {}ms",
                report.partitions_reset,
                report.errors.len(),
                report.duration_ms
            );
        }

        Ok(report)
    }

    /// Execute offset reset for a single group
    async fn execute_group_reset(
        &self,
        client: &KafkaClient,
        plan: &GroupResetPlan,
    ) -> Result<GroupResetResult> {
        let offsets: Vec<_> = plan
            .partitions
            .iter()
            .map(|p| {
                (
                    p.topic.clone(),
                    p.partition,
                    p.target_offset,
                    p.metadata.clone(),
                )
            })
            .collect();

        let results = commit_offsets(client, &plan.group_id, &offsets).await?;

        let mut partitions_reset = 0u64;
        let mut partitions_failed = 0u64;
        let mut errors = Vec::new();

        for (topic, partition, error_code) in results {
            if error_code == 0 {
                partitions_reset += 1;
                debug!(
                    "Reset offset for {}:{}:{} successfully",
                    plan.group_id, topic, partition
                );
            } else {
                partitions_failed += 1;
                errors.push(format!(
                    "{}:{}:{} - error code {}",
                    plan.group_id, topic, partition, error_code
                ));
            }
        }

        Ok(GroupResetResult {
            group_id: plan.group_id.clone(),
            partitions_reset,
            partitions_failed,
            errors,
        })
    }

    /// Generate a shell script for manual offset reset using kafka-consumer-groups CLI
    pub fn generate_shell_script(&self, plan: &OffsetResetPlan) -> String {
        let mut script = String::new();

        script.push_str("#!/bin/bash\n");
        script.push_str("# Offset Reset Script\n");
        script.push_str(&format!(
            "# Generated at: {}\n",
            chrono::Utc::now().to_rfc3339()
        ));
        script.push_str(&format!("# Strategy: {}\n", plan.strategy));
        if let Some(ref backup_id) = plan.backup_id {
            script.push_str(&format!("# Backup ID: {}\n", backup_id));
        }
        script.push_str("#\n");
        script.push_str("# WARNING: This script will reset consumer group offsets.\n");
        script.push_str("# Review the commands below before executing.\n");
        script.push_str("#\n");
        script.push_str("set -e\n\n");

        let bootstrap = if plan.target_bootstrap_servers.is_empty() {
            "localhost:9092".to_string()
        } else {
            plan.target_bootstrap_servers.join(",")
        };

        for group in &plan.groups {
            script.push_str(&format!("\n# Consumer Group: {}\n", group.group_id));
            script.push_str(&format!(
                "# Partitions to reset: {} (complete: {})\n",
                group.partition_count, group.complete
            ));

            // Group by topic for cleaner output
            let mut by_topic: HashMap<&str, Vec<&PartitionResetPlan>> = HashMap::new();
            for partition in &group.partitions {
                by_topic
                    .entry(&partition.topic)
                    .or_default()
                    .push(partition);
            }

            for (topic, partitions) in by_topic {
                script.push_str(&format!("\n# Topic: {}\n", topic));

                for p in partitions {
                    script.push_str(&format!(
                        "# Partition {}: source_offset={} -> target_offset={} (timestamp={})\n",
                        p.partition, p.source_offset, p.target_offset, p.timestamp
                    ));

                    script.push_str(&format!(
                        "kafka-consumer-groups.sh --bootstrap-server {} \\\n",
                        bootstrap
                    ));
                    script.push_str(&format!("  --group {} \\\n", group.group_id));
                    script.push_str(&format!("  --topic {}:{} \\\n", p.topic, p.partition));
                    script.push_str(&format!(
                        "  --reset-offsets --to-offset {} --execute\n\n",
                        p.target_offset
                    ));
                }
            }
        }

        script.push_str("echo \"Offset reset complete!\"\n");

        script
    }

    /// Generate a JSON report of the reset plan
    pub fn generate_json_report(&self, plan: &OffsetResetPlan) -> Result<String> {
        Ok(serde_json::to_string_pretty(plan)?)
    }

    /// Generate a YAML report of the reset plan
    pub fn generate_yaml_report(&self, plan: &OffsetResetPlan) -> Result<String> {
        Ok(serde_yaml::to_string(plan)?)
    }

    /// Generate a CSV report of the reset plan
    pub fn generate_csv_report(&self, plan: &OffsetResetPlan) -> String {
        let mut csv = String::new();
        csv.push_str("group_id,topic,partition,source_offset,target_offset,timestamp\n");

        for group in &plan.groups {
            for p in &group.partitions {
                csv.push_str(&format!(
                    "{},{},{},{},{},{}\n",
                    group.group_id,
                    p.topic,
                    p.partition,
                    p.source_offset,
                    p.target_offset,
                    p.timestamp
                ));
            }
        }

        csv
    }
}

/// Builder for creating offset reset plans from consumer group offsets
pub struct OffsetResetPlanBuilder {
    groups: HashMap<String, ConsumerGroupOffsets>,
    offset_mapping: OffsetMapping,
    backup_id: Option<String>,
    bootstrap_servers: Vec<String>,
}

impl OffsetResetPlanBuilder {
    /// Create a new builder from an offset mapping
    pub fn new(offset_mapping: OffsetMapping) -> Self {
        Self {
            groups: HashMap::new(),
            offset_mapping,
            backup_id: None,
            bootstrap_servers: Vec::new(),
        }
    }

    /// Set the backup ID
    pub fn with_backup_id(mut self, backup_id: String) -> Self {
        self.backup_id = Some(backup_id);
        self
    }

    /// Set the target bootstrap servers
    pub fn with_bootstrap_servers(mut self, servers: Vec<String>) -> Self {
        self.bootstrap_servers = servers;
        self
    }

    /// Add a consumer group to the plan
    pub fn add_consumer_group(mut self, group: ConsumerGroupOffsets) -> Self {
        self.groups.insert(group.group_id.clone(), group);
        self
    }

    /// Build the offset reset plan
    pub fn build(self, strategy: OffsetResetStrategy) -> OffsetResetPlan {
        let mut groups = Vec::new();

        for (group_id, group_offsets) in self.groups {
            let mut partitions = Vec::new();
            let mut complete = true;

            for (topic, topic_offsets) in group_offsets.offsets {
                for (partition, offset_info) in topic_offsets {
                    let target_offset = offset_info.target_offset.or_else(|| {
                        self.offset_mapping.lookup_target_offset(
                            &topic,
                            partition,
                            offset_info.source_offset,
                        )
                    });

                    if let Some(target) = target_offset {
                        partitions.push(PartitionResetPlan {
                            topic: topic.clone(),
                            partition,
                            source_offset: offset_info.source_offset,
                            target_offset: target,
                            timestamp: offset_info.timestamp,
                            metadata: offset_info.metadata,
                        });
                    } else {
                        complete = false;
                    }
                }
            }

            let partition_count = partitions.len();
            groups.push(GroupResetPlan {
                group_id,
                partitions,
                partition_count,
                complete,
            });
        }

        OffsetResetPlan {
            groups,
            generated_at: chrono::Utc::now().timestamp_millis(),
            strategy: format!("{:?}", strategy),
            dry_run: strategy == OffsetResetStrategy::DryRun,
            backup_id: self.backup_id,
            source_cluster_id: self.offset_mapping.source_cluster_id,
            target_bootstrap_servers: self.bootstrap_servers,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_reset_plan_builder() {
        let mapping = OffsetMapping::new();
        let builder = OffsetResetPlanBuilder::new(mapping)
            .with_backup_id("test-backup".to_string())
            .with_bootstrap_servers(vec!["localhost:9092".to_string()]);

        let plan = builder.build(OffsetResetStrategy::DryRun);

        assert!(plan.dry_run);
        assert_eq!(plan.backup_id, Some("test-backup".to_string()));
        assert_eq!(plan.target_bootstrap_servers, vec!["localhost:9092"]);
    }

    #[test]
    fn test_generate_csv_report() {
        let plan = OffsetResetPlan {
            groups: vec![GroupResetPlan {
                group_id: "test-group".to_string(),
                partitions: vec![PartitionResetPlan {
                    topic: "test-topic".to_string(),
                    partition: 0,
                    source_offset: 100,
                    target_offset: 200,
                    timestamp: 1000,
                    metadata: None,
                }],
                partition_count: 1,
                complete: true,
            }],
            generated_at: 0,
            strategy: "DryRun".to_string(),
            dry_run: true,
            backup_id: None,
            source_cluster_id: None,
            target_bootstrap_servers: vec![],
        };

        let executor = OffsetResetExecutor::new_offline(vec!["localhost:9092".to_string()]);
        let csv = executor.generate_csv_report(&plan);

        assert!(csv.contains("test-group"));
        assert!(csv.contains("test-topic"));
        assert!(csv.contains("100"));
        assert!(csv.contains("200"));
    }

    #[test]
    fn test_generate_shell_script() {
        let plan = OffsetResetPlan {
            groups: vec![GroupResetPlan {
                group_id: "my-consumer-group".to_string(),
                partitions: vec![
                    PartitionResetPlan {
                        topic: "orders".to_string(),
                        partition: 0,
                        source_offset: 100,
                        target_offset: 5100,
                        timestamp: 1700000000000,
                        metadata: None,
                    },
                    PartitionResetPlan {
                        topic: "orders".to_string(),
                        partition: 1,
                        source_offset: 200,
                        target_offset: 5200,
                        timestamp: 1700000001000,
                        metadata: None,
                    },
                ],
                partition_count: 2,
                complete: true,
            }],
            generated_at: 0,
            strategy: "Auto".to_string(),
            dry_run: false,
            backup_id: Some("prod-backup-2024".to_string()),
            source_cluster_id: None,
            target_bootstrap_servers: vec!["kafka-1:9092".to_string(), "kafka-2:9092".to_string()],
        };

        let executor = OffsetResetExecutor::new_offline(vec![
            "kafka-1:9092".to_string(),
            "kafka-2:9092".to_string(),
        ]);
        let script = executor.generate_shell_script(&plan);

        // Verify script structure
        assert!(script.starts_with("#!/bin/bash"));
        assert!(script.contains("kafka-consumer-groups.sh"));
        assert!(script.contains("--bootstrap-server kafka-1:9092,kafka-2:9092"));
        assert!(script.contains("--group my-consumer-group"));
        assert!(script.contains("--topic orders:0"));
        assert!(script.contains("--topic orders:1"));
        assert!(script.contains("--to-offset 5100"));
        assert!(script.contains("--to-offset 5200"));
        assert!(script.contains("# Backup ID: prod-backup-2024"));
    }

    #[test]
    fn test_offset_reset_plan_with_mapping_lookup() {
        let mut mapping = OffsetMapping::new();

        // Add detailed mappings for orders topic
        mapping.add_detailed("orders", 0, 100, 5100, 1700000000000);
        mapping.add_detailed("orders", 0, 101, 5101, 1700000001000);
        mapping.add_detailed("orders", 0, 102, 5102, 1700000002000);

        // Create consumer group offsets
        let mut group_offsets = ConsumerGroupOffsets::new("order-processor");
        group_offsets.add_offset(
            "orders",
            0,
            ConsumerGroupOffset {
                source_offset: 101,
                target_offset: None, // Should be looked up from mapping
                timestamp: 1700000001000,
                metadata: None,
            },
        );

        let builder = OffsetResetPlanBuilder::new(mapping)
            .with_backup_id("test-backup".to_string())
            .with_bootstrap_servers(vec!["localhost:9092".to_string()])
            .add_consumer_group(group_offsets);

        let plan = builder.build(OffsetResetStrategy::Auto);

        assert_eq!(plan.groups.len(), 1);
        assert_eq!(plan.groups[0].group_id, "order-processor");
        assert_eq!(plan.groups[0].partitions.len(), 1);
        assert_eq!(plan.groups[0].partitions[0].source_offset, 101);
        assert_eq!(plan.groups[0].partitions[0].target_offset, 5101);
        assert!(plan.groups[0].complete);
    }

    #[test]
    fn test_offset_reset_plan_incomplete_mapping() {
        let mapping = OffsetMapping::new(); // Empty mapping

        let mut group_offsets = ConsumerGroupOffsets::new("order-processor");
        group_offsets.add_offset(
            "orders",
            0,
            ConsumerGroupOffset {
                source_offset: 101,
                target_offset: None,
                timestamp: 1700000001000,
                metadata: None,
            },
        );

        let builder = OffsetResetPlanBuilder::new(mapping).add_consumer_group(group_offsets);

        let plan = builder.build(OffsetResetStrategy::Manual);

        assert_eq!(plan.groups.len(), 1);
        assert_eq!(plan.groups[0].partitions.len(), 0); // No mapping found
        assert!(!plan.groups[0].complete);
    }

    #[test]
    fn test_offset_reset_strategy_debug() {
        // Test that strategy enum variants can be formatted
        assert_eq!(format!("{:?}", OffsetResetStrategy::Auto), "Auto");
        assert_eq!(format!("{:?}", OffsetResetStrategy::Manual), "Manual");
        assert_eq!(format!("{:?}", OffsetResetStrategy::DryRun), "DryRun");
    }

    #[test]
    fn test_offset_reset_strategy_default() {
        let strategy: OffsetResetStrategy = Default::default();
        assert_eq!(strategy, OffsetResetStrategy::Manual);
    }
}
