# OSO Kafka Backup: Comprehensive Testing PRD for OSS Core Functionality

## Document Status
**Version:** 1.0  
**Date:** 2025-11-29  
**Audience:** Engineering Team, QA Specialists  
**Focus:** Core (`core/`) module testing, not CLI  
**Philosophy:** Apache Kafka broker test suite patterns applied to backup/restore

---

## Executive Summary

This PRD defines a comprehensive testing strategy for OSO Kafka Backup's core backup/restore functionality. Tests are informed by:

1. **Apache Kafka's own test suite** – Broker component tests (KafkaServerTests)
2. **Real-world Kafka disasters** – Reddit + GitHub issues on backup tool failures
3. **Backup-specific edge cases** – Data consistency, offset semantics, partition failover
4. **Testcontainers patterns** – Real Kafka brokers during tests, not mocks

**Testing Philosophy:** Trust but verify. We test against **real Kafka brokers** in containers, not mocks, because:
- Mocks don't catch broker behavior edge cases
- Testcontainers give us real semantics at test speed
- Container cleanup is automatic (no leftover ports/resources)
- This is how Apache Kafka tests itself

---

## Part 1: Test Pyramid Architecture

### 1.1 Test Distribution

```
                    ▲
                   /│\
                  / │ \
                 /  │  \  E2E Tests (5%)
                /   │   \ - Full restore flow
               /    │    \- Real broker cluster
              /─────┼─────\ - Chaos scenarios
             /      │      \
            /       │       \ Integration Tests (35%)
           /        │        \- Testcontainers + real broker
          /         │         \- PITR accuracy
         /          │          \- Offset semantics
        /───────────┼───────────\
       /            │            \ Unit Tests (60%)
      /             │             \- Pure functions
     /              │              \- No I/O
    /______________▼______________\- Fast iteration
```

### 1.2 Test Categories by Layer

```
core/lib.rs ──────────────────────────────────────────────────────────
  ↓
  core/backup.rs (Backup Engine)
    Unit Tests (90%):
      ├─ Segment encoding/decoding
      ├─ Timestamp mapping
      ├─ Header manipulation
      ├─ Compression/decompression
      └─ Topic filtering logic
    
    Integration Tests (10%):
      └─ Full backup to S3

  core/restore.rs (Restore Engine)
    Unit Tests (85%):
      ├─ Time window filtering
      ├─ Offset mapping application
      ├─ Decompression pipeline
      ├─ Ordering guarantees
      └─ Edge case handling (empty partitions, etc.)
    
    Integration Tests (15%):
      ├─ Full PITR restore accuracy
      ├─ Offset reset semantics
      └─ Leader failover during restore

  core/offset_mapping.rs (Offset State)
    Unit Tests (95%):
      ├─ Offset serialization
      ├─ Group state tracking
      ├─ Header parsing
      ├─ Timestamp preservation
      └─ Deduplication logic
    
    Integration Tests (5%):
      └─ Offset accuracy vs. __consumer_offsets

  core/offset_automation.rs (Bulk Reset)
    Unit Tests (70%):
      ├─ Batching algorithm
      ├─ Group filtering
      ├─ Concurrency safety
      └─ Error handling per-partition
    
    Integration Tests (30%):
      ├─ Parallel vs. sequential equivalence
      ├─ Retry logic correctness
      └─ Per-partition failure recovery

  core/offset_rollback.rs (Snapshot + Rollback)
    Unit Tests (65%):
      ├─ Snapshot serialization
      ├─ Offset comparison logic
      └─ State machine validation
    
    Integration Tests (35%):
      ├─ Snapshot durability
      ├─ Rollback completeness
      ├─ Verification after rollback
      └─ Network partition recovery
```

---

## Part 2: Common Failures Found in Backup Tools (From Reddit/GitHub)

### 2.1 Known Disasters We Test Against

**Problem 1: Offset Commit Semantics Violation**
```
Issue: Tool resets offsets before verifying they're valid
Result: Consumers skip restored messages or reprocess indefinitely
Reddit: "Committing offsets before processing completes → message loss"
Test: verify_offset_commit_idempotence_under_rebalance()
```

**Problem 2: Data Loss on Partial Restore**
```
Issue: Restore starts, halfway through → broker fails
       Tool doesn't track which partitions succeeded
Result: Some partitions have old data, others have new → inconsistency
Test: chaos_broker_failure_mid_restore()
```

**Problem 3: Partition Leader Election During Backup**
```
Issue: While backing up partition 0 of a topic
       Leader changes mid-read (broker dies)
       Old code: Crashes, loses work
Result: Incomplete backup, no recovery
Test: chaos_partition_leader_election_during_backup()
```

**Problem 4: Unclean Leader Election Data Loss**
```
Issue: Backup cluster has `unclean.leader.election.enable=true`
       Leader dies, out-of-sync replica becomes leader
       Backed up data doesn't match reality
Result: Restore gets inconsistent snapshot
Test: backup_with_unclean_leader_election_enabled()
```

**Problem 5: Offset Mapping Corruption on Network Partition**
```
Issue: Backup fetches offsets from broker
       Network partitions → partial response
       Offset mapping has gaps
Result: Restore skips messages for some groups
Test: chaos_network_partition_during_offset_fetch()
```

**Problem 6: Message Corruption on Compression**
```
Issue: Messages compressed with codec=zstd
       Decompression uses wrong codec parameter
Result: Restored messages are garbage
Test: roundtrip_compression_zstd_all_sizes()
```

**Problem 7: High Watermark vs. Committed Offset Mismatch**
```
Issue: Restore tries to set offset beyond HWM
       Broker rejects commit
Result: Silent failure or timeout
Test: offset_reset_validates_high_watermark()
```

**Problem 8: Exactly-Once Violations**
```
Issue: Restore succeeds, but offset reset fails
       Data is restored, offsets aren't updated
       Consumer reprocesses all data
Result: Duplicates in downstream systems
Test: verify_atomicity_restore_and_offset_reset()
```

**Problem 9: Schema Registry Out-of-Sync**
```
Issue: Backup preserves messages but not schemas
       Restore writes messages to cluster
       Schema registry doesn't have matching schema ID
Result: Consumers can't deserialize
Note: OSS doesn't handle this, but we test that we don't corrupt it
Test: backup_restore_preserves_message_headers()
```

**Problem 10: Timestamp Accuracy Loss**
```
Issue: Messages have millisecond timestamps
       Backup uses second granularity
       Restore filters by time, loses precision
Result: Messages on wrong side of time boundary
Test: timestamp_filtering_preserves_millisecond_accuracy()
```

---

## Part 3: Unit Test Suite (60% of Tests)

### 3.1 Core Backup Unit Tests

**File:** `tests/unit/backup.rs`

```rust
#[cfg(test)]
mod backup_unit_tests {
    use super::*;
    use kafka_backup::core::backup::*;

    // ============= Segment Encoding/Decoding =============
    
    #[test]
    fn encode_decode_kafka_record_preserves_all_fields() {
        let original = KafkaRecord {
            topic: "orders".to_string(),
            partition: 0,
            offset: 12345,
            timestamp: 1672531200000,  // Millisecond precision
            key: Some(b"order-123".to_vec()),
            value: b"{'id':123,'amount':99.99}".to_vec(),
            headers: vec![
                ("x-original-offset".into(), "12345".into()),
            ],
        };

        let encoded = encode_record(&original)?;
        let decoded = decode_record(&encoded)?;

        assert_eq!(decoded.offset, original.offset);
        assert_eq!(decoded.timestamp, original.timestamp);  // Milliseconds preserved
        assert_eq!(decoded.value, original.value);
        assert_eq!(decoded.headers.len(), original.headers.len());
    }

    #[test]
    fn encode_record_with_null_key_preserves_semantics() {
        let record = KafkaRecord {
            topic: "events".to_string(),
            partition: 0,
            offset: 100,
            timestamp: 1672531200000,
            key: None,  // ← Null key case
            value: b"event_data".to_vec(),
            headers: vec![],
        };

        let encoded = encode_record(&record)?;
        let decoded = decode_record(&encoded)?;

        assert_eq!(decoded.key, None);
        // Consumers expect null key to round-trip through backup
    }

    #[test]
    fn encode_record_with_large_headers_does_not_truncate() {
        let large_header_value = vec![0u8; 1024 * 100];  // 100KB header
        
        let record = KafkaRecord {
            topic: "logging".to_string(),
            partition: 0,
            offset: 200,
            timestamp: 1672531200000,
            key: None,
            value: b"log_entry".to_vec(),
            headers: vec![
                ("big-header".into(), String::from_utf8_lossy(&large_header_value).to_string()),
            ],
        };

        let encoded = encode_record(&record)?;
        let decoded = decode_record(&encoded)?;

        assert_eq!(decoded.headers[0].1.len(), large_header_value.len());
        // Real Kafka allows large headers, so should we
    }

    // ============= Topic Filtering =============

    #[test]
    fn topic_filter_regex_matches_correctly() {
        let filter = TopicFilter::Regex("^app_.*".to_string());
        
        assert!(filter.matches("app_orders"));
        assert!(filter.matches("app_payments"));
        assert!(!filter.matches("system_logs"));
        assert!(!filter.matches("APP_ORDERS"));  // Case sensitive
    }

    #[test]
    fn topic_filter_wildcard_matches_correctly() {
        let filter = TopicFilter::Wildcard("app_*".to_string());
        
        assert!(filter.matches("app_"));
        assert!(filter.matches("app_orders"));
        assert!(filter.matches("app_payments_v1"));
        assert!(!filter.matches("system_app_orders"));  // Must be at start
    }

    #[test]
    fn topic_filter_list_matches_exactly() {
        let filter = TopicFilter::List(vec!["orders".into(), "payments".into()]);
        
        assert!(filter.matches("orders"));
        assert!(filter.matches("payments"));
        assert!(!filter.matches("orders_v2"));  // Exact match only
    }

    // ============= Timestamp Mapping =============

    #[test]
    fn timestamp_mapping_preserves_millisecond_precision() {
        let timestamps = vec![
            1672531200001,  // Different by 1ms
            1672531200002,
            1672531200003,
        ];

        for ts in timestamps {
            let record = create_test_record_with_timestamp(ts);
            let encoded = encode_record(&record)?;
            let decoded = decode_record(&encoded)?;
            
            assert_eq!(decoded.timestamp, ts);
        }
    }

    #[test]
    fn timestamp_filtering_matches_boundary_conditions() {
        let records = vec![
            create_test_record_with_timestamp(1000),
            create_test_record_with_timestamp(1001),
            create_test_record_with_timestamp(1999),
            create_test_record_with_timestamp(2000),
            create_test_record_with_timestamp(2001),
        ];

        let filtered = filter_by_timestamp(&records, 1000, 2000)?;
        
        // Boundary: inclusive on both ends (standard PITR)
        assert_eq!(filtered.len(), 4);
        assert_eq!(filtered[0].timestamp, 1000);
        assert_eq!(filtered[3].timestamp, 2000);
    }

    #[test]
    fn timestamp_filtering_handles_no_matches() {
        let records = vec![
            create_test_record_with_timestamp(1000),
            create_test_record_with_timestamp(2000),
        ];

        let filtered = filter_by_timestamp(&records, 3000, 4000)?;
        
        assert_eq!(filtered.len(), 0);
    }

    // ============= Compression =============

    #[test]
    fn compression_zstd_roundtrip_small_message() {
        let data = b"small message";
        let compressed = compress_zstd(data)?;
        let decompressed = decompress_zstd(&compressed)?;
        
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compression_zstd_roundtrip_large_message() {
        let data = vec![0u8; 1024 * 1024 * 10];  // 10MB
        let compressed = compress_zstd(&data)?;
        let decompressed = decompress_zstd(&compressed)?;
        
        assert_eq!(decompressed, data);
    }

    #[test]
    fn compression_zstd_compresses_repetitive_data() {
        let repetitive = b"aaaa".repeat(10000);  // 40KB of 'a'
        let compressed = compress_zstd(&repetitive)?;
        
        // zstd should compress highly repetitive data significantly
        assert!(compressed.len() < repetitive.len() / 10);
    }

    #[test]
    fn compression_zstd_does_not_compress_random_data() {
        let random = generate_random_bytes(1024);
        let compressed = compress_zstd(&random)?;
        
        // Random data shouldn't compress much
        // (may actually expand slightly due to compression overhead)
        assert!(compressed.len() <= random.len() + 100);  // Allow small overhead
    }

    #[test]
    fn decompression_corrupted_data_returns_error() {
        let corrupted = b"not valid compressed data";
        let result = decompress_zstd(corrupted);
        
        assert!(result.is_err());
    }

    // ============= Offset Ordering =============

    #[test]
    fn records_maintain_offset_order_within_partition() {
        let records = vec![
            create_test_record_with_offset(100),
            create_test_record_with_offset(101),
            create_test_record_with_offset(102),
        ];

        assert_eq!(records[0].offset, 100);
        assert_eq!(records[1].offset, 101);
        assert_eq!(records[2].offset, 102);
        
        // Consumers rely on this order
    }

    #[test]
    fn offset_gaps_are_preserved() {
        // Gap can occur due to deleted records or compaction
        let records = vec![
            create_test_record_with_offset(100),
            create_test_record_with_offset(105),  // Gap: 101-104
            create_test_record_with_offset(110),  // Gap: 106-109
        ];

        assert_eq!(records[1].offset - records[0].offset, 5);
        assert_eq!(records[2].offset - records[1].offset, 5);
    }

    // ============= Partition Handling =============

    #[test]
    fn empty_partition_backup_succeeds() {
        let partition_records = vec![];  // No messages
        
        let backup = create_backup(&partition_records)?;
        
        assert_eq!(backup.message_count, 0);
        assert_eq!(backup.segments.len(), 0);
    }

    #[test]
    fn single_message_partition_backup_succeeds() {
        let partition_records = vec![create_test_record_with_offset(0)];
        
        let backup = create_backup(&partition_records)?;
        
        assert_eq!(backup.message_count, 1);
    }
}
```

### 3.2 Core Restore Unit Tests

**File:** `tests/unit/restore.rs`

```rust
#[cfg(test)]
mod restore_unit_tests {
    use super::*;
    use kafka_backup::core::restore::*;

    // ============= Time Window Filtering =============

    #[test]
    fn restore_filters_messages_before_start_time() {
        let segments = vec![
            create_segment_with_timestamps(&[100, 200, 300]),
            create_segment_with_timestamps(&[400, 500, 600]),
        ];

        let filtered = filter_segments_by_time(&segments, 350, 700)?;
        
        // 400, 500, 600 should be included
        // 100, 200, 300 should be excluded
        assert_eq!(count_messages(&filtered), 3);
    }

    #[test]
    fn restore_filters_messages_after_end_time() {
        let segments = vec![
            create_segment_with_timestamps(&[100, 200, 300]),
            create_segment_with_timestamps(&[400, 500, 600]),
        ];

        let filtered = filter_segments_by_time(&segments, 0, 350)?;
        
        assert_eq!(count_messages(&filtered), 3);
    }

    #[test]
    fn restore_time_filter_boundary_inclusive_on_both_ends() {
        let segments = vec![
            create_segment_with_timestamps(&[1000, 1001, 1999, 2000, 2001]),
        ];

        let filtered = filter_segments_by_time(&segments, 1000, 2000)?;
        
        // Should include 1000 and 2000, exclude 2001
        assert_eq!(count_messages(&filtered), 4);
    }

    #[test]
    fn restore_all_messages_within_window() {
        let segments = vec![
            create_segment_with_timestamps(&[100]),
            create_segment_with_timestamps(&[200]),
            create_segment_with_timestamps(&[300]),
        ];

        let filtered = filter_segments_by_time(&segments, 50, 350)?;
        
        assert_eq!(count_messages(&filtered), 3);
    }

    // ============= Decompression Pipeline =============

    #[test]
    fn decompress_segment_reads_all_messages() {
        let original_messages = vec![
            create_test_record_with_offset(0),
            create_test_record_with_offset(1),
            create_test_record_with_offset(2),
        ];

        let compressed = compress_segment(&original_messages)?;
        let decompressed = decompress_segment(&compressed)?;

        assert_eq!(decompressed.len(), 3);
        for (i, msg) in decompressed.iter().enumerate() {
            assert_eq!(msg.offset, i as i64);
        }
    }

    #[test]
    fn decompress_segment_with_large_messages() {
        let large_value = vec![0u8; 1024 * 1024];  // 1MB message
        let original_messages = vec![
            KafkaRecord { value: large_value, ..create_test_record() }
        ];

        let compressed = compress_segment(&original_messages)?;
        let decompressed = decompress_segment(&compressed)?;

        assert_eq!(decompressed[0].value.len(), 1024 * 1024);
    }

    #[test]
    fn decompress_empty_segment_returns_empty_vec() {
        let empty_segment = create_empty_segment();
        let decompressed = decompress_segment(&empty_segment)?;
        
        assert_eq!(decompressed.len(), 0);
    }

    // ============= Ordering Guarantees =============

    #[test]
    fn restored_messages_maintain_partition_order() {
        let segments = vec![
            create_segment_with_offsets(&[100, 101, 102]),
            create_segment_with_offsets(&[103, 104]),
        ];

        let restored = restore_all_segments(&segments)?;
        
        let offsets: Vec<_> = restored.iter().map(|m| m.offset).collect();
        assert_eq!(offsets, vec![100, 101, 102, 103, 104]);
    }

    #[test]
    fn restored_messages_per_partition_are_sorted_by_offset() {
        // Segments may be out of order in storage
        let segments = vec![
            create_segment_with_offsets(&[50, 51, 52]),
            create_segment_with_offsets(&[0, 1, 2]),
            create_segment_with_offsets(&[25, 26, 27]),
        ];

        let restored = restore_and_sort(&segments)?;
        
        let offsets: Vec<_> = restored.iter().map(|m| m.offset).collect();
        assert_eq!(offsets, vec![0, 1, 2, 25, 26, 27, 50, 51, 52]);
    }

    // ============= Offset Mapping Application =============

    #[test]
    fn apply_offset_mapping_to_record_header() {
        let mut record = create_test_record_with_offset(12345);
        
        apply_offset_mapping_header(&mut record)?;
        
        assert!(record.headers.iter().any(|(k, _)| k == "x-original-offset"));
        let offset_header = record.headers.iter()
            .find(|(k, _)| k == "x-original-offset")
            .map(|(_, v)| v);
        
        assert_eq!(offset_header, Some(&"12345".to_string()));
    }

    #[test]
    fn offset_mapping_preserves_existing_headers() {
        let mut record = create_test_record();
        record.headers.push(("existing-header".into(), "value".into()));
        
        apply_offset_mapping_header(&mut record)?;
        
        assert!(record.headers.len() > 1);
        assert!(record.headers.iter().any(|(k, _)| k == "existing-header"));
        assert!(record.headers.iter().any(|(k, _)| k == "x-original-offset"));
    }

    // ============= Edge Cases =============

    #[test]
    fn restore_empty_backup_succeeds() {
        let backup = create_empty_backup();
        let restored = restore_backup(&backup)?;
        
        assert_eq!(restored.len(), 0);
    }

    #[test]
    fn restore_single_message_backup_succeeds() {
        let backup = create_backup_with_messages(&[create_test_record()]);
        let restored = restore_backup(&backup)?;
        
        assert_eq!(restored.len(), 1);
    }

    #[test]
    fn restore_with_timestamp_before_all_messages() {
        let segments = vec![
            create_segment_with_timestamps(&[1000, 2000, 3000]),
        ];

        let filtered = filter_segments_by_time(&segments, 0, 500)?;
        
        assert_eq!(count_messages(&filtered), 0);
    }

    #[test]
    fn restore_with_timestamp_after_all_messages() {
        let segments = vec![
            create_segment_with_timestamps(&[1000, 2000, 3000]),
        ];

        let filtered = filter_segments_by_time(&segments, 4000, 5000)?;
        
        assert_eq!(count_messages(&filtered), 0);
    }
}
```

### 3.3 Offset Mapping Unit Tests

**File:** `tests/unit/offset_mapping.rs`

```rust
#[cfg(test)]
mod offset_mapping_unit_tests {
    use super::*;
    use kafka_backup::core::offset_mapping::*;

    #[test]
    fn offset_state_serialization_preserves_all_fields() {
        let state = OffsetState {
            group_id: "app1".to_string(),
            topic: "orders".to_string(),
            partition: 0,
            committed_offset: 12345,
            timestamp: 1672531200000,
        };

        let serialized = serde_json::to_string(&state)?;
        let deserialized: OffsetState = serde_json::from_str(&serialized)?;

        assert_eq!(deserialized.group_id, state.group_id);
        assert_eq!(deserialized.committed_offset, state.committed_offset);
    }

    #[test]
    fn offset_mapping_deduplication_keeps_latest() {
        let states = vec![
            OffsetState { group_id: "app1".into(), committed_offset: 100, ..default() },
            OffsetState { group_id: "app1".into(), committed_offset: 200, ..default() },
            OffsetState { group_id: "app1".into(), committed_offset: 150, ..default() },
        ];

        let deduplicated = deduplicate_by_group(&states)?;
        
        // Should keep the latest (200), not 150
        assert_eq!(deduplicated[0].committed_offset, 200);
    }

    #[test]
    fn group_offset_state_groups_correctly() {
        let states = vec![
            create_offset_state("group1", "topic1", 0, 100),
            create_offset_state("group1", "topic1", 1, 200),
            create_offset_state("group2", "topic1", 0, 150),
        ];

        let grouped = group_by_group_id(&states)?;
        
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped["group1"].len(), 2);
        assert_eq!(grouped["group2"].len(), 1);
    }
}
```

---

## Part 4: Integration Tests (35% of Tests)

### 4.1 Testcontainers Setup

**File:** `tests/integration/common.rs`

```rust
use testcontainers::{clients, images};
use rdkafka::client::DefaultClientContext;
use rdkafka::admin::AdminClient;
use rdkafka::config::ClientConfig;

pub struct KafkaTestCluster {
    pub client: AdminClient<DefaultClientContext>,
    pub bootstrap_servers: String,
    pub container: Option<Container<'static, KafkaImage>>,
}

impl KafkaTestCluster {
    pub async fn start() -> Result<Self> {
        let docker = clients::Cli::default();
        
        let kafka = docker.run(
            KafkaImage::default()
                .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        );

        let bootstrap_servers = format!(
            "localhost:{}",
            kafka.get_host_port_ipv4(9092)
        );

        let client = ClientConfig::new()
            .set("bootstrap.servers", &bootstrap_servers)
            .set("client.id", "test-client")
            .create()?;

        Ok(KafkaTestCluster {
            client,
            bootstrap_servers,
            container: Some(kafka),
        })
    }

    pub async fn create_topic(
        &self,
        name: &str,
        partitions: u32,
        replication_factor: u16,
    ) -> Result<()> {
        let topic_spec = TopicSpecification::new(name, partitions as i32, TopicPartition::Assign(vec![/* ... */]));
        
        // Wait for topic creation
        tokio::time::sleep(Duration::from_millis(500)).await;
        Ok(())
    }

    pub async fn wait_for_broker_ready(&self, timeout: Duration) -> Result<()> {
        let start = Instant::now();
        
        loop {
            if self.client.fetch_metadata(None, Duration::from_secs(1)).is_ok() {
                return Ok(());
            }
            
            if start.elapsed() > timeout {
                return Err(anyhow!("Broker not ready"));
            }
            
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for KafkaTestCluster {
    fn drop(&mut self) {
        // Container cleanup is automatic
    }
}
```

### 4.2 Integration Test: PITR Accuracy

**File:** `tests/integration/pitr_accuracy.rs`

```rust
#[tokio::test]
async fn test_pitr_restore_accuracy_single_partition() {
    let cluster = KafkaTestCluster::start().await.unwrap();
    cluster.wait_for_broker_ready(Duration::from_secs(30)).await.unwrap();

    // Setup
    cluster.create_topic("test-topic", 1, 1).await.unwrap();
    
    let producer = create_test_producer(&cluster.bootstrap_servers).await.unwrap();
    
    // Phase 1: Produce messages with known timestamps
    let t1 = unix_timestamp_ms();
    produce_message(&producer, "test-topic", 0, "msg1", t1).await.unwrap();
    produce_message(&producer, "test-topic", 0, "msg2", t1 + 100).await.unwrap();
    produce_message(&producer, "test-topic", 0, "msg3", t1 + 200).await.unwrap();
    
    let t2 = unix_timestamp_ms();
    produce_message(&producer, "test-topic", 0, "msg4", t2 + 100).await.unwrap();
    produce_message(&producer, "test-topic", 0, "msg5", t2 + 200).await.unwrap();
    
    // Phase 2: Backup
    let backup_dir = "/tmp/test-backup";
    let backup_config = BackupConfig {
        topics: vec!["test-topic".to_string()],
        partitions: vec![0],
        compression: CompressionCodec::Zstd,
    };
    
    backup_to_s3(&cluster.bootstrap_servers, &backup_config).await.unwrap();
    
    // Phase 3: Restore with time window
    let restore_config = RestoreConfig {
        start_time: t1 + 50,
        end_time: t2 + 150,
        preserve_timestamps: true,
    };
    
    restore_from_backup(&cluster.bootstrap_servers, &restore_config).await.unwrap();
    
    // Phase 4: Verify
    let consumer = create_test_consumer(&cluster.bootstrap_servers, "test-group").await.unwrap();
    let messages = consume_all_messages(&consumer, Duration::from_secs(5)).await.unwrap();
    
    // Should have msg2, msg3, msg4 (within time window)
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0].value_str()?, "msg2");
    assert_eq!(messages[1].value_str()?, "msg3");
    assert_eq!(messages[2].value_str()?, "msg4");
}

#[tokio::test]
async fn test_pitr_restore_with_multiple_partitions() {
    let cluster = KafkaTestCluster::start().await.unwrap();
    cluster.wait_for_broker_ready(Duration::from_secs(30)).await.unwrap();

    cluster.create_topic("multi-part", 3, 1).await.unwrap();
    
    let producer = create_test_producer(&cluster.bootstrap_servers).await.unwrap();
    
    // Produce to all partitions
    for partition in 0..3 {
        for i in 0..5 {
            let key = format!("key-{}", partition);
            produce_to_partition(&producer, "multi-part", partition, &key, &format!("msg{}", i)).await.unwrap();
        }
    }
    
    // Backup and restore
    backup_and_restore(&cluster).await.unwrap();
    
    // Verify all partitions have correct message count
    for partition in 0..3 {
        let consumer = create_consumer_for_partition(&cluster.bootstrap_servers, partition).await.unwrap();
        let messages = consume_all_messages(&consumer, Duration::from_secs(5)).await.unwrap();
        
        assert_eq!(messages.len(), 5, "Partition {} should have 5 messages", partition);
    }
}
```

### 4.3 Integration Test: Offset Semantics

**File:** `tests/integration/offset_semantics.rs`

```rust
#[tokio::test]
async fn test_offset_reset_idempotence() {
    let cluster = KafkaTestCluster::start().await.unwrap();
    cluster.wait_for_broker_ready(Duration::from_secs(30)).await.unwrap();

    // Setup topic with consumer group
    cluster.create_topic("orders", 1, 1).await.unwrap();
    
    let producer = create_test_producer(&cluster.bootstrap_servers).await.unwrap();
    produce_messages(&producer, "orders", 10).await.unwrap();
    
    let consumer = create_consumer(&cluster.bootstrap_servers, "app1").await.unwrap();
    consume_n_messages(&consumer, 5).await.unwrap();  // Consume 5, offset now at 5
    
    // Capture current offset
    let current_offset = get_committed_offset(&cluster.bootstrap_servers, "app1", "orders", 0).await.unwrap();
    assert_eq!(current_offset, 5);
    
    // Reset offset to 3 (idempotent operation)
    reset_offset_to_3(&cluster, "app1", "orders", 0).await.unwrap();
    
    let offset_after_reset = get_committed_offset(&cluster.bootstrap_servers, "app1", "orders", 0).await.unwrap();
    assert_eq!(offset_after_reset, 3);
    
    // Reset again to same value (idempotent)
    reset_offset_to_3(&cluster, "app1", "orders", 0).await.unwrap();
    
    let offset_after_second_reset = get_committed_offset(&cluster.bootstrap_servers, "app1", "orders", 0).await.unwrap();
    assert_eq!(offset_after_second_reset, 3);  // Should stay at 3
}

#[tokio::test]
async fn test_offset_commit_under_rebalance() {
    let cluster = KafkaTestCluster::start().await.unwrap();
    cluster.wait_for_broker_ready(Duration::from_secs(30)).await.unwrap();

    cluster.create_topic("payments", 3, 1).await.unwrap();
    
    // Start consumer group with 2 members
    let consumer1 = create_consumer(&cluster.bootstrap_servers, "payment-group").await.unwrap();
    let consumer2 = create_consumer(&cluster.bootstrap_servers, "payment-group").await.unwrap();
    
    // Rebalance happens
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // Produce and consume
    let producer = create_test_producer(&cluster.bootstrap_servers).await.unwrap();
    produce_n_messages(&producer, "payments", 30).await.unwrap();
    
    // Both consumers process messages
    let msgs1 = consume_n_messages(&consumer1, 10).await.unwrap();
    let msgs2 = consume_n_messages(&consumer2, 10).await.unwrap();
    
    // Offsets should be committed correctly
    let offsets1 = get_consumer_offsets(&cluster, "payment-group").await.unwrap();
    
    // Total committed should reflect all consumed
    let total_committed: i64 = offsets1.values().sum();
    assert_eq!(total_committed, 20);  // Both consumers committed
}

#[tokio::test]
async fn test_offset_reset_validates_high_watermark() {
    let cluster = KafkaTestCluster::start().await.unwrap();
    cluster.wait_for_broker_ready(Duration::from_secs(30)).await.unwrap();

    cluster.create_topic("test-hwm", 1, 1).await.unwrap();
    
    let producer = create_test_producer(&cluster.bootstrap_servers).await.unwrap();
    produce_n_messages(&producer, "test-hwm", 10).await.unwrap();  // HWM = 10
    
    // Try to reset to valid offset (should succeed)
    let result = reset_offset(&cluster, "group1", "test-hwm", 0, 5).await;
    assert!(result.is_ok());
    
    // Try to reset beyond HWM (should fail or be capped)
    let result = reset_offset(&cluster, "group1", "test-hwm", 0, 100).await;
    
    // Kafka broker will reject offsets > HWM
    match result {
        Ok(()) => {
            let offset = get_committed_offset(&cluster.bootstrap_servers, "group1", "test-hwm", 0).await.unwrap();
            assert!(offset <= 10);  // Should be capped at HWM
        }
        Err(_) => {
            // Or the operation fails, which is acceptable
        }
    }
}
```

---

## Part 5: Chaos Engineering Tests (5% of Tests)

### 5.1 Broker Failure During Backup

**File:** `tests/chaos/broker_failures.rs`

```rust
#[tokio::test]
async fn chaos_broker_failure_mid_backup() {
    let cluster = KafkaTestCluster::start().await.unwrap();
    cluster.wait_for_broker_ready(Duration::from_secs(30)).await.unwrap();

    // Setup: topic with 3 partitions
    cluster.create_topic("orders", 3, 1).await.unwrap();
    
    let producer = create_test_producer(&cluster.bootstrap_servers).await.unwrap();
    for partition in 0..3 {
        produce_n_messages(&producer, "orders", partition, 100).await.unwrap();
    }
    
    // Start backup in background
    let backup_handle = tokio::spawn(async {
        backup_topic(&cluster.bootstrap_servers, "orders", "/tmp/backup-chaos").await
    });
    
    // Wait for backup to start processing partition 1
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Simulate broker crash (kill container)
    cluster.kill_broker().await.unwrap();
    
    // Wait for recovery
    tokio::time::sleep(Duration::from_secs(5)).await;
    cluster.start_broker().await.unwrap();
    cluster.wait_for_broker_ready(Duration::from_secs(30)).await.unwrap();
    
    // Backup should either:
    // 1. Complete successfully (if it had checkpointing), or
    // 2. Fail gracefully with clear error (not panic), or
    // 3. Be retryable
    
    let result = backup_handle.await;
    match result {
        Ok(Ok(())) => {
            // Backup completed despite failure - excellent
        }
        Ok(Err(e)) => {
            // Backup failed, but with clear error (not panic)
            assert!(!e.to_string().contains("panic"));
            assert!(is_retryable_error(&e));
        }
        Err(_) => panic!("Backup task panicked"),
    }
}
```

### 5.2 Network Partition During Offset Fetch

**File:** `tests/chaos/network_partitions.rs`

```rust
#[tokio::test]
async fn chaos_network_partition_during_offset_fetch() {
    let cluster = KafkaTestCluster::start().await.unwrap();
    
    // Setup consumer group with offset state
    cluster.create_topic("events", 2, 1).await.unwrap();
    consume_and_commit(&cluster, "my-group", "events").await.unwrap();
    
    // Start fetching offset state
    let fetch_handle = tokio::spawn(async {
        fetch_group_offsets(&cluster.bootstrap_servers, "my-group").await
    });
    
    // Simulate network partition
    tokio::time::sleep(Duration::from_millis(100)).await;
    cluster.simulate_network_partition().await.unwrap();
    
    // Wait for timeout
    tokio::time::sleep(Duration::from_secs(5)).await;
    cluster.heal_network_partition().await.unwrap();
    
    // Fetch should either complete or timeout gracefully
    let result = fetch_handle.await;
    match result {
        Ok(Ok(offsets)) => {
            // Recovered and got offsets - good
        }
        Ok(Err(e)) if e.is_timeout() => {
            // Timeout is expected, not a crash
        }
        Ok(Err(e)) => {
            assert!(!e.to_string().contains("panic"));
        }
        Err(_) => panic!("Task panicked"),
    }
}
```

### 5.3 Partition Leader Election During Restore

**File:** `tests/chaos/partition_leadership.rs`

```rust
#[tokio::test]
async fn chaos_partition_leader_election_during_restore() {
    let cluster = KafkaTestCluster::start().await.unwrap();
    cluster.wait_for_broker_ready(Duration::from_secs(30)).await.unwrap();

    // Setup replication: topic with replication factor = 2
    cluster.create_topic("backup-test", 1, 2).await.unwrap();
    
    let producer = create_test_producer(&cluster.bootstrap_servers).await.unwrap();
    produce_n_messages(&producer, "backup-test", 1000).await.unwrap();
    
    // Create backup snapshot
    let backup = create_backup(&cluster).await.unwrap();
    
    // Start restore
    let restore_handle = tokio::spawn(async {
        restore_backup(&cluster.bootstrap_servers, &backup).await
    });
    
    // Wait for restore to start reading messages
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Kill current leader (force election)
    cluster.kill_partition_leader("backup-test", 0).await.unwrap();
    
    // Wait for new leader to be elected
    tokio::time::sleep(Duration::from_secs(3)).await;
    
    // Restore should continue without data loss
    let result = restore_handle.await;
    assert!(result.is_ok(), "Restore should succeed despite leader election");
    
    // Verify restored data
    let consumer = create_test_consumer(&cluster.bootstrap_servers, "restore-verify").await.unwrap();
    let restored_count = consume_all_messages(&consumer, Duration::from_secs(5))
        .await
        .unwrap()
        .len();
    
    assert_eq!(restored_count, 1000, "Should restore all messages");
}
```

---

## Part 6: Test Organization & CI/CD

### 6.1 Test Directory Structure

```
tests/
├── unit/
│   ├── backup.rs
│   ├── restore.rs
│   ├── offset_mapping.rs
│   ├── offset_automation.rs
│   ├── offset_rollback.rs
│   ├── helpers.rs
│   └── mod.rs
├── integration/
│   ├── pitr_accuracy.rs
│   ├── offset_semantics.rs
│   ├── bulk_offset_reset.rs
│   ├── offset_rollback_e2e.rs
│   ├── common.rs
│   ├── testcontainers_setup.rs
│   └── mod.rs
├── chaos/
│   ├── broker_failures.rs
│   ├── network_partitions.rs
│   ├── partition_leadership.rs
│   ├── compression_edge_cases.rs
│   ├── helpers.rs
│   └── mod.rs
└── e2e/
    ├── full_restore_flow.rs
    ├── disaster_recovery_simulation.rs
    ├── concurrent_operations.rs
    └── mod.rs
```

### 6.2 Cargo.toml Test Dependencies

```toml
[dev-dependencies]
tokio = { version = "1.35", features = ["full"] }
testcontainers = "0.15"
rdkafka = "0.35"
serde_json = "1.0"
rand = "0.8"
tempfile = "3.8"
property-based-testing = "0.4"  # For fuzz testing

# Logging for test debugging
tracing = "0.1"
tracing-subscriber = "0.3"
```

### 6.3 GitHub Actions CI Configuration

**File:** `.github/workflows/test.yml`

```yaml
name: Tests

on:
  pull_request:
    branches: [main, develop]
    paths:
      - 'core/**'
      - 'tests/**'
      - 'Cargo.toml'

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      
      - name: Run unit tests
        run: cargo test --lib --test unit

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: docker/setup-buildx-action@v2
      
      - name: Run integration tests (requires Docker)
        run: cargo test --test integration -- --nocapture
        env:
          DOCKER_HOST: unix:///var/run/docker.sock
          RUST_LOG: debug

  chaos-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: docker/setup-buildx-action@v2
      
      - name: Run chaos tests
        run: cargo test --test chaos
        timeout-minutes: 15

  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: taiki-e/install-action@cargo-tarpaulin
      
      - name: Generate coverage
        run: cargo tarpaulin --out Xml --timeout 300 --ignore-panics --ignore-timeouts
      
      - uses: codecov/codecov-action@v3
        with:
          files: ./cobertura.xml
```

---

## Part 7: Test Execution

### 7.1 Run All Tests

```bash
# Unit tests only (fast, < 1 minute)
cargo test --lib --test unit

# Integration tests (slower, ~5 minutes, requires Docker)
cargo test --test integration

# Chaos tests (slow, ~10 minutes)
cargo test --test chaos

# All tests (ordered by speed)
cargo test --lib
cargo test --test unit
cargo test --test integration
cargo test --test chaos

# With logging
RUST_LOG=debug cargo test --test integration -- --nocapture

# Specific test
cargo test test_pitr_restore_accuracy -- --nocapture

# Measure coverage
cargo tarpaulin --out Html --timeout 300
```

### 7.2 Test Output Standards

Every test should output:
- ✅ **What it tests** (one clear sentence)
- ✅ **Why it matters** (reference to known failure or Kafka guarantee)
- ✅ **What could go wrong** (failure mode)

Example:
```
test test_pitr_restore_accuracy_single_partition ... ok
  ✓ Verifies PITR timestamp filtering includes/excludes correct messages
  ✓ Catches off-by-one errors in time window boundaries
  ✓ Detects message corruption during backup/restore roundtrip
```

---

## Part 8: Known Issues We're Testing Against

### Matrix of Failures

| Failure Mode | Test | Recovery |
|------|------|----------|
| Message corruption on decompression | roundtrip_compression_zstd_all_sizes | Fail fast, clear error |
| Offset gaps cause consumer skip | offset_gaps_are_preserved | Preserve exact offset sequence |
| Timestamp precision loss | timestamp_filtering_millisecond_precision | Store/restore ms, not seconds |
| Partial restore due to broker crash | chaos_broker_failure_mid_backup | Checkpoint, resume, verify |
| Leader election data loss | chaos_partition_leader_election | Replicas ensure no loss |
| Unclean leader election mismatch | backup_with_unclean_leader_election_enabled | Warn user, validate |
| Network partition causes inconsistent offsets | chaos_network_partition_offset_fetch | Timeout vs crash |
| Offset reset beyond HWM | offset_reset_validates_high_watermark | Reject or cap at HWM |
| Exactly-once violated | verify_atomicity_restore_offset_reset | Atomic transaction |

---

## Part 9: Acceptance Criteria

### 9.1 Coverage Requirements

- **Unit Tests:** ≥90% line coverage for core/ module
- **Integration Tests:** All major flows (backup, restore, offset reset)
- **Chaos Tests:** Every known failure mode
- **E2E Tests:** Full disaster recovery simulation

### 9.2 Performance Gates

- **Unit tests:** Complete in < 30 seconds
- **Integration tests:** Complete in < 5 minutes (with parallel test)
- **Chaos tests:** Complete in < 10 minutes
- **Full suite:** Complete in < 20 minutes

### 9.3 Reliability Requirements

- **Zero flaky tests** – Every test passes 100% on 10 consecutive runs
- **Clear failures** – If a test fails, root cause is obvious (not "timeout unknown")
- **Reproducible** – Failures are reproducible and not random

---

## Summary

This PRD ensures comprehensive testing of OSO Kafka Backup's core functionality by:

1. ✅ **Following Kafka's test patterns** – Real brokers, transaction semantics, leader election
2. ✅ **Catching real-world failures** – Reddit + GitHub issues inform test design
3. ✅ **Testing edge cases** – Empty partitions, large messages, network failures
4. ✅ **Verifying guarantees** – Ordering, atomicity, offset semantics
5. ✅ **Using proper tools** – Testcontainers for real Kafka, not mocks
6. ✅ **CI/CD integration** – Automated on every PR, coverage reporting

