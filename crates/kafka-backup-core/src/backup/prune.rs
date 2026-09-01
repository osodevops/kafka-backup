//! Retention for backup sets: plan and execute the deletion of aged or
//! oversized segments from an incremental backup, recording every removed
//! range in the manifest (issue #169).
//!
//! Bucket lifecycle rules must NOT be used on an incremental backup set —
//! they delete segments the manifest still references and never expire the
//! manifest itself. This module is the safe replacement: manifest first,
//! object deletion second, and a [`PrunedRange`] left behind so
//! `describe`/`validate`/`restore` can explain the hole.

use std::collections::HashMap;

use tracing::{debug, warn};

use crate::manifest::{BackupManifest, PruneReason, PrunedRange, SegmentMetadata};
use crate::offset_store::OffsetStore;
use crate::storage::StorageBackend;
use crate::{Error, Result};

/// What to prune.
#[derive(Debug, Clone, Default)]
pub struct PruneCriteria {
    /// Prune segments whose newest record (or upload time, when recorded) is
    /// older than this (epoch milliseconds).
    pub older_than: Option<i64>,
    /// After the age pass, keep pruning oldest-first until the set's total
    /// compressed size fits under this many bytes.
    pub max_total_bytes: Option<u64>,
    /// Never prune a partition below this many newest segments (minimum 1).
    pub keep_segments: usize,
}

/// One partition's slice of a [`PrunePlan`].
#[derive(Debug, Clone)]
pub struct PartitionPrunePlan {
    pub topic: String,
    pub partition: i32,
    /// Segments to delete, oldest first (a prefix of the partition's list).
    pub segments: Vec<SegmentMetadata>,
    /// The manifest entry recording the removal.
    pub range: PrunedRange,
}

/// A concrete, explainable prune decision.
#[derive(Debug, Clone, Default)]
pub struct PrunePlan {
    pub backup_id: String,
    pub partitions: Vec<PartitionPrunePlan>,
    /// Total segments to delete.
    pub segments: u64,
    /// Total compressed bytes to delete.
    pub bytes: u64,
    /// Partitions where pruning stopped early to protect the resume position.
    pub protected_partitions: Vec<(String, i32)>,
}

/// The effective age of a segment for retention purposes: upload time when
/// recorded (0.21+), else the newest record timestamp.
fn segment_age_basis(segment: &SegmentMetadata) -> i64 {
    if segment.uploaded_at > 0 {
        segment.uploaded_at
    } else {
        segment.end_timestamp
    }
}

/// Resume positions (`last_offset + 1`) per (topic, partition) from the
/// backup's offset store; a segment containing or after the resume position
/// is never pruned.
pub async fn resume_positions(
    store: &dyn OffsetStore,
    backup_id: &str,
) -> Result<HashMap<(String, i32), i64>> {
    let offsets = store.get_all_offsets(backup_id).await?;
    Ok(offsets
        .into_iter()
        .map(|o| ((o.topic, o.partition), o.last_offset + 1))
        .collect())
}

/// Plan a prune of `manifest` under `criteria`.
///
/// Per partition (segments sorted by `start_offset`), only a *prefix* of the
/// segment list is ever pruned — no holes mid-partition. A segment is
/// prunable when:
/// - it is older than `older_than` (when set),
/// - it is not among the newest `keep_segments` segments,
/// - its `end_offset` is below the partition's resume position (when known;
///   with no offset store the newest segment is always protected).
///
/// `max_total_bytes` then continues pruning oldest-first across the whole
/// set until the total fits, under the same protections.
pub fn plan_prune(
    manifest: &BackupManifest,
    resume: &HashMap<(String, i32), i64>,
    criteria: &PruneCriteria,
    now_ms: i64,
    reason: PruneReason,
) -> PrunePlan {
    let keep_segments = criteria.keep_segments.max(1);
    let mut plan = PrunePlan {
        backup_id: manifest.backup_id.clone(),
        ..Default::default()
    };

    // Per partition: the age-pruned prefix, plus the contiguous tail of
    // further prunable segments the size pass may extend into.
    let mut prefixes: Vec<(String, i32, Vec<SegmentMetadata>)> = Vec::new();
    let mut size_candidates: Vec<(String, i32, SegmentMetadata)> = Vec::new();
    let mut total_compressed: u64 = 0;

    for topic in &manifest.topics {
        for partition in &topic.partitions {
            let mut segments = partition.segments.clone();
            segments.sort_by_key(|s| s.start_offset);
            total_compressed += segments.iter().map(|s| s.compressed_size).sum::<u64>();
            let resume_offset = resume
                .get(&(topic.name.clone(), partition.partition_id))
                .copied();
            let max_prunable = segments.len().saturating_sub(keep_segments);

            let safe = |segment: &SegmentMetadata| match resume_offset {
                Some(resume) => segment.end_offset < resume,
                // Without an offset store there is no resume position to
                // protect; keep_segments >= 1 already protects the newest.
                None => true,
            };

            let mut prefix: Vec<SegmentMetadata> = Vec::new();
            let mut still_prunable = true;
            for (idx, segment) in segments.iter().enumerate() {
                if idx >= max_prunable || !safe(segment) {
                    if idx < max_prunable && !safe(segment) {
                        plan.protected_partitions
                            .push((topic.name.clone(), partition.partition_id));
                    }
                    still_prunable = false;
                    break;
                }
                let old_enough = matches!(criteria.older_than, Some(cutoff) if segment_age_basis(segment) < cutoff);
                if old_enough {
                    prefix.push(segment.clone());
                } else {
                    // Age prefix ends here; anything further is only
                    // reachable by the size pass, contiguously.
                    break;
                }
            }
            if still_prunable {
                for (idx, segment) in segments.iter().enumerate().skip(prefix.len()) {
                    if idx >= max_prunable || !safe(segment) {
                        break;
                    }
                    size_candidates.push((
                        topic.name.clone(),
                        partition.partition_id,
                        segment.clone(),
                    ));
                }
            }
            prefixes.push((topic.name.clone(), partition.partition_id, prefix));
        }
    }

    // Size cap: keep pruning globally oldest-first, extending each
    // partition's prefix contiguously, until the total fits.
    if let Some(cap) = criteria.max_total_bytes {
        let age_pruned: u64 = prefixes
            .iter()
            .flat_map(|(_, _, p)| p)
            .map(|s| s.compressed_size)
            .sum();
        let mut remaining_total = total_compressed.saturating_sub(age_pruned);
        size_candidates.sort_by_key(|(_, _, s)| segment_age_basis(s));
        for (topic, partition, segment) in size_candidates {
            if remaining_total <= cap {
                break;
            }
            let Some((_, _, prefix)) = prefixes
                .iter_mut()
                .find(|(t, p, _)| *t == topic && *p == partition)
            else {
                continue;
            };
            // Contiguity: only extend with the segment directly after the
            // current prefix (candidates were collected in offset order).
            let expected_next = prefix.last().map(|s| s.end_offset);
            let contiguous = match expected_next {
                Some(prev_end) => segment.start_offset > prev_end,
                None => true,
            };
            // Candidates for a partition arrive in order, so the first
            // unconsumed one is always the contiguous next; later ones only
            // apply once the earlier ones were taken.
            if !contiguous {
                continue;
            }
            remaining_total = remaining_total.saturating_sub(segment.compressed_size);
            prefix.push(segment);
        }
    }

    // Materialise partition plans.
    for (topic, partition, segments) in prefixes {
        if segments.is_empty() {
            continue;
        }
        let bytes: u64 = segments.iter().map(|s| s.compressed_size).sum();
        let range = PrunedRange {
            start_offset: segments.first().map(|s| s.start_offset).unwrap_or(0),
            end_offset: segments.last().map(|s| s.end_offset).unwrap_or(0),
            segments: segments.len() as u32,
            bytes,
            pruned_at: now_ms,
            cutoff_timestamp: criteria.older_than.unwrap_or(0),
            reason,
        };
        plan.segments += segments.len() as u64;
        plan.bytes += bytes;
        plan.partitions.push(PartitionPrunePlan {
            topic,
            partition,
            segments,
            range,
        });
    }
    plan
}

/// Remove the planned segments from `manifest` and record the pruned ranges.
pub fn apply_plan_to_manifest(manifest: &mut BackupManifest, plan: &PrunePlan) {
    for part_plan in &plan.partitions {
        let Some(topic) = manifest
            .topics
            .iter_mut()
            .find(|t| t.name == part_plan.topic)
        else {
            continue;
        };
        let Some(partition) = topic
            .partitions
            .iter_mut()
            .find(|p| p.partition_id == part_plan.partition)
        else {
            continue;
        };
        let doomed: std::collections::HashSet<&str> =
            part_plan.segments.iter().map(|s| s.key.as_str()).collect();
        partition
            .segments
            .retain(|s| !doomed.contains(s.key.as_str()));
        partition.add_pruned(part_plan.range.clone());
    }
}

/// Delete the planned segment objects, tolerating already-missing keys —
/// the manifest was rewritten first, so a partial failure leaves harmless
/// orphans that a retry or `--sweep-orphans` removes.
pub async fn delete_planned_segments(storage: &dyn StorageBackend, plan: &PrunePlan) {
    for part_plan in &plan.partitions {
        for segment in &part_plan.segments {
            match storage.delete(&segment.key).await {
                Ok(()) => debug!("Pruned segment {}", segment.key),
                Err(e) => warn!(
                    "Could not delete pruned segment {} (continuing; the manifest no longer \
                     references it): {}",
                    segment.key, e
                ),
            }
        }
    }
}

/// Execute `plan` against a stored manifest: rewrite `{backup_id}/manifest.json`
/// directly (never via the merge path — a union merge would resurrect the
/// removed entries), then delete the objects.
pub async fn execute_prune(
    storage: &dyn StorageBackend,
    mut manifest: BackupManifest,
    plan: &PrunePlan,
) -> Result<BackupManifest> {
    if plan.segments == 0 {
        return Ok(manifest);
    }
    apply_plan_to_manifest(&mut manifest, plan);
    let key = format!("{}/manifest.json", manifest.backup_id);
    let json = serde_json::to_string_pretty(&manifest)
        .map_err(|e| Error::Config(format!("serialize manifest: {e}")))?;
    storage.put(&key, bytes::Bytes::from(json)).await?;
    delete_planned_segments(storage, plan).await;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::PartitionBackup;

    fn seg(start: i64, end: i64, uploaded_at: i64, bytes: u64) -> SegmentMetadata {
        SegmentMetadata {
            key: format!("b/topics/t/partition=0/segment-{:020}.bin.zst", start),
            start_offset: start,
            end_offset: end,
            start_timestamp: start * 1000,
            end_timestamp: end * 1000,
            record_count: end - start + 1,
            uncompressed_size: bytes * 4,
            compressed_size: bytes,
            sha256: String::new(),
            uploaded_at,
        }
    }

    fn manifest(segments: Vec<SegmentMetadata>) -> BackupManifest {
        let mut m = BackupManifest::new("b".to_string());
        let topic = m.get_or_create_topic("t");
        topic.original_partition_count = Some(1);
        topic.partitions = vec![PartitionBackup {
            partition_id: 0,
            segments,
            gaps: Vec::new(),
            pruned: Vec::new(),
        }];
        m
    }

    fn resume(offset: i64) -> HashMap<(String, i32), i64> {
        HashMap::from([(("t".to_string(), 0), offset)])
    }

    const DAY_MS: i64 = 86_400_000;

    #[test]
    fn plan_prunes_only_an_aged_prefix_and_respects_keep_segments() {
        let now = 100 * DAY_MS;
        let m = manifest(vec![
            seg(0, 9, 10 * DAY_MS, 100),
            seg(10, 19, 20 * DAY_MS, 100),
            seg(20, 29, 95 * DAY_MS, 100), // too new
            seg(30, 39, 96 * DAY_MS, 100),
        ]);
        let plan = plan_prune(
            &m,
            &resume(1_000),
            &PruneCriteria {
                older_than: Some(now - 30 * DAY_MS),
                max_total_bytes: None,
                keep_segments: 1,
            },
            now,
            PruneReason::Manual,
        );
        assert_eq!(plan.segments, 2);
        assert_eq!(plan.bytes, 200);
        let range = &plan.partitions[0].range;
        assert_eq!((range.start_offset, range.end_offset), (0, 19));
    }

    #[test]
    fn plan_never_prunes_at_or_past_the_resume_position() {
        let now = 100 * DAY_MS;
        let m = manifest(vec![
            seg(0, 9, 10 * DAY_MS, 100),
            seg(10, 19, 11 * DAY_MS, 100),
            seg(20, 29, 12 * DAY_MS, 100),
        ]);
        // Resume inside the second segment: only the first is safe.
        let plan = plan_prune(
            &m,
            &resume(15),
            &PruneCriteria {
                older_than: Some(now),
                max_total_bytes: None,
                keep_segments: 1,
            },
            now,
            PruneReason::Retention,
        );
        assert_eq!(plan.segments, 1);
        assert_eq!(plan.partitions[0].range.end_offset, 9);
        assert_eq!(plan.protected_partitions, vec![("t".to_string(), 0)]);
    }

    #[test]
    fn plan_without_offsets_db_still_keeps_the_newest_segment() {
        let now = 100 * DAY_MS;
        let m = manifest(vec![seg(0, 9, DAY_MS, 100), seg(10, 19, DAY_MS, 100)]);
        let plan = plan_prune(
            &m,
            &HashMap::new(),
            &PruneCriteria {
                older_than: Some(now),
                max_total_bytes: None,
                keep_segments: 1,
            },
            now,
            PruneReason::Manual,
        );
        assert_eq!(plan.segments, 1);
    }

    #[test]
    fn size_cap_extends_the_prefix_oldest_first() {
        let now = 100 * DAY_MS;
        let m = manifest(vec![
            seg(0, 9, 10 * DAY_MS, 100),
            seg(10, 19, 20 * DAY_MS, 100),
            seg(20, 29, 30 * DAY_MS, 100),
            seg(30, 39, 90 * DAY_MS, 100),
        ]);
        // No age cutoff; cap forces two oldest out (400 total, cap 200,
        // newest protected by keep_segments).
        let plan = plan_prune(
            &m,
            &resume(1_000),
            &PruneCriteria {
                older_than: None,
                max_total_bytes: Some(200),
                keep_segments: 1,
            },
            now,
            PruneReason::Retention,
        );
        assert_eq!(plan.segments, 2);
        assert_eq!(plan.partitions[0].range.end_offset, 19);
    }

    #[test]
    fn old_segments_without_uploaded_at_fall_back_to_record_time() {
        let now = 100 * DAY_MS;
        let m = manifest(vec![seg(0, 9, 0, 100), seg(10, 19, 0, 100)]);
        // end_timestamp = end_offset * 1000 — ancient; both eligible, newest kept.
        let plan = plan_prune(
            &m,
            &resume(1_000),
            &PruneCriteria {
                older_than: Some(now),
                max_total_bytes: None,
                keep_segments: 1,
            },
            now,
            PruneReason::Manual,
        );
        assert_eq!(plan.segments, 1);
    }

    #[tokio::test]
    async fn execute_writes_manifest_before_deleting_and_tolerates_missing_objects() {
        use crate::storage::{MemoryBackend, StorageBackend};
        let storage = MemoryBackend::new();
        let m = manifest(vec![
            seg(0, 9, 1, 100),
            seg(10, 19, 2, 100),
            seg(20, 29, 3, 100),
        ]);
        // Only the first segment's object exists; the second is already gone.
        storage
            .put(
                &m.topics[0].partitions[0].segments[0].key,
                bytes::Bytes::from("x"),
            )
            .await
            .unwrap();
        let plan = plan_prune(
            &m,
            &resume(1_000),
            &PruneCriteria {
                older_than: Some(i64::MAX),
                max_total_bytes: None,
                keep_segments: 1,
            },
            4,
            PruneReason::Manual,
        );
        assert_eq!(plan.segments, 2);
        let updated = execute_prune(&storage, m, &plan).await.unwrap();
        assert_eq!(updated.topics[0].partitions[0].segments.len(), 1);
        assert_eq!(updated.topics[0].partitions[0].pruned.len(), 1);
        // Manifest in storage matches.
        let stored = storage.get("b/manifest.json").await.unwrap();
        let parsed: BackupManifest = serde_json::from_slice(&stored).unwrap();
        assert_eq!(parsed.total_pruned(), 1);
        assert_eq!(parsed.total_segments(), 1);
        // The deleted object is gone.
        assert!(!storage
            .exists(&plan.partitions[0].segments[0].key)
            .await
            .unwrap());
    }
}
