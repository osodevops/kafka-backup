//! Names of the record headers kafka-backup itself adds to records.
//!
//! Two places inject headers:
//!
//! * **Backup** (`backup.include_offset_headers`, default `true`) appends
//!   [`X_ORIGINAL_OFFSET`] and [`X_ORIGINAL_TIMESTAMP`] — and
//!   [`X_SOURCE_CLUSTER`] when `backup.source_cluster_id` is set — to every
//!   record *as it is archived*. These are Phase 1 of the three-phase restore
//!   and make header-based consumer offset recovery possible.
//! * **Restore** (`restore.include_original_offset_header`, or
//!   `consumer_group_strategy: header-based`) appends [`X_ORIGINAL_OFFSET`],
//!   [`X_ORIGINAL_TIMESTAMP`] and [`X_SOURCE_PARTITION`] to every record *as
//!   it is produced* to the target cluster.
//!
//! `restore.strip_offset_headers` removes all of [`ALL`] from archived
//! records before they are produced, for restores that must be
//! header-for-header identical to the source.

/// Source offset of the record, as a little-endian `i64`.
pub const X_ORIGINAL_OFFSET: &str = "x-original-offset";
/// Source timestamp of the record (epoch millis), as a little-endian `i64`.
pub const X_ORIGINAL_TIMESTAMP: &str = "x-original-timestamp";
/// `backup.source_cluster_id`, as UTF-8 (backup side only).
pub const X_SOURCE_CLUSTER: &str = "x-source-cluster";
/// Source partition of the record, as a little-endian `i32` (restore side only).
pub const X_SOURCE_PARTITION: &str = "x-source-partition";

/// Every header name kafka-backup may add to a record.
pub const ALL: [&str; 4] = [
    X_ORIGINAL_OFFSET,
    X_ORIGINAL_TIMESTAMP,
    X_SOURCE_CLUSTER,
    X_SOURCE_PARTITION,
];

/// Whether `key` is one of the headers kafka-backup adds.
pub fn is_offset_header(key: &str) -> bool {
    ALL.contains(&key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognises_only_kafka_backup_headers() {
        for key in ALL {
            assert!(is_offset_header(key), "{key}");
        }
        assert!(!is_offset_header("x-original-offset-v2"));
        assert!(!is_offset_header("X-Original-Offset"));
        assert!(!is_offset_header("trace-id"));
        assert!(!is_offset_header(""));
    }
}
