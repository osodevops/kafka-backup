//! The produce half of a restore, shared by the 1:1 partition path
//! (`engine.rs`) and the repartitioning writers (`repartition.rs`): batch
//! planning by record count and encoded bytes, the target's
//! `max.message.bytes` pre-check, the restore-wide rate limit, and sending.

use std::ops::Range;
use std::sync::Arc;

use crate::error::KafkaError;
use crate::kafka::produce::{
    batch_bytes_within_max_message_bytes, find_oversized_record, plan_batches, records_encoded_size,
};
use crate::manifest::BackupRecord;
use crate::Result;

use super::producer::{ProducedBatch, RestoreProducer};
use super::rate_limit::RestoreRateLimiter;

/// Where a record came from, for error messages.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RecordOrigin<'a> {
    pub source_topic: &'a str,
    pub source_partition: i32,
}

/// Per-topic produce settings and shared state.
#[derive(Clone)]
pub(crate) struct ProducePath {
    producer: Arc<RestoreProducer>,
    rate_limiter: Option<Arc<RestoreRateLimiter>>,
    /// The target topic's `max.message.bytes`, when it could be read.
    max_message_bytes: Option<usize>,
    max_batch_records: usize,
    /// Uncompressed byte cap of a batch: `produce_max_batch_bytes`, lowered
    /// to fit `max.message.bytes` after compression.
    max_batch_bytes: usize,
}

impl ProducePath {
    pub(crate) fn new(
        producer: Arc<RestoreProducer>,
        rate_limiter: Option<Arc<RestoreRateLimiter>>,
        max_message_bytes: Option<usize>,
        options: &crate::config::RestoreOptions,
    ) -> Self {
        Self {
            producer,
            rate_limiter,
            max_message_bytes,
            max_batch_records: options.produce_batch_size,
            max_batch_bytes: effective_batch_bytes(
                options.produce_max_batch_bytes,
                max_message_bytes,
            ),
        }
    }

    /// Records per batch at most.
    pub(crate) fn max_batch_records(&self) -> usize {
        self.max_batch_records
    }

    /// Uncompressed bytes per batch at most.
    pub(crate) fn max_batch_bytes(&self) -> usize {
        self.max_batch_bytes
    }

    /// Consecutive batches of `records` within the record and byte caps.
    pub(crate) fn plan(&self, records: &[BackupRecord]) -> Vec<Range<usize>> {
        plan_batches(records, self.max_batch_records, self.max_batch_bytes)
    }

    /// Fail before anything of `records` is produced when one of them is
    /// larger than the target topic's `max.message.bytes` even alone in a
    /// compressed batch. The error names the record's source offset.
    pub(crate) fn check_record_sizes(
        &self,
        records: &[BackupRecord],
        origin: RecordOrigin<'_>,
        target_topic: &str,
        target_partition: i32,
    ) -> Result<()> {
        let Some(max_message_bytes) = self.max_message_bytes else {
            return Ok(());
        };
        if let Some((index, batch_bytes)) = find_oversized_record(records, max_message_bytes)? {
            return Err(KafkaError::RecordTooLarge {
                source_topic: origin.source_topic.to_string(),
                source_partition: origin.source_partition,
                source_offset: records[index].offset,
                target_topic: target_topic.to_string(),
                target_partition,
                batch_bytes,
                max_message_bytes,
            }
            .into());
        }
        Ok(())
    }

    /// Wait for the restore-wide rate limit, then produce `batch` to
    /// `topic`/`partition` with the restore producer (idempotent unless it
    /// fell back). The result lists `(base offset, record count)` per
    /// produced sub-batch, in order.
    pub(crate) async fn produce(
        &self,
        topic: &str,
        partition: i32,
        batch: &[BackupRecord],
    ) -> Result<ProducedBatch> {
        if let Some(limiter) = &self.rate_limiter {
            limiter
                .acquire(batch.len() as u64, records_encoded_size(batch) as u64)
                .await;
        }
        self.producer.produce(topic, partition, batch).await
    }
}

/// The batch byte cap: `produce_max_batch_bytes`, lowered so a compressed
/// batch always fits the target topic's `max.message.bytes`.
pub(crate) fn effective_batch_bytes(
    produce_max_batch_bytes: usize,
    max_message_bytes: Option<usize>,
) -> usize {
    match max_message_bytes {
        Some(max) => produce_max_batch_bytes.min(batch_bytes_within_max_message_bytes(max)),
        None => produce_max_batch_bytes,
    }
}

/// Parse `max.message.bytes` from a topic's DescribeConfigs entries.
pub(crate) fn max_message_bytes_from_configs(
    entries: &[crate::kafka::ConfigEntry],
) -> Option<usize> {
    entries
        .iter()
        .find(|e| e.name == "max.message.bytes")
        .and_then(|e| e.value.as_deref())
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::ConfigEntry;

    fn entry(name: &str, value: Option<&str>) -> ConfigEntry {
        ConfigEntry {
            name: name.to_string(),
            value: value.map(str::to_string),
            read_only: false,
            config_source: 5,
            is_default: true,
            is_sensitive: false,
        }
    }

    #[test]
    fn batch_cap_is_the_smaller_of_the_setting_and_the_topic_limit() {
        assert_eq!(effective_batch_bytes(1_048_576, None), 1_048_576);
        // Confluent Cloud default topic limit (~2 MiB): the 1 MiB setting wins.
        assert_eq!(effective_batch_bytes(1_048_576, Some(2_097_164)), 1_048_576);
        // A topic limited to 512 KiB lowers the cap below 512 KiB.
        let capped = effective_batch_bytes(1_048_576, Some(524_288));
        assert!(capped < 524_288 && capped > 500_000, "{capped}");
    }

    #[test]
    fn max_message_bytes_is_read_from_describe_configs() {
        let entries = vec![
            entry("cleanup.policy", Some("delete")),
            entry("max.message.bytes", Some("8388608")),
        ];
        assert_eq!(max_message_bytes_from_configs(&entries), Some(8_388_608));
        assert_eq!(
            max_message_bytes_from_configs(&[entry("max.message.bytes", None)]),
            None
        );
        assert_eq!(
            max_message_bytes_from_configs(&[entry("max.message.bytes", Some("junk"))]),
            None
        );
        assert_eq!(max_message_bytes_from_configs(&[]), None);
    }
}
