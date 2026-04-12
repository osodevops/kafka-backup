//! Minimal produce test to debug error code 87 (INVALID_RECORD).
//! Run with: cargo test -p kafka-backup-core --test produce_debug -- --nocapture --ignored

use bytes::{Bytes, BytesMut};
use indexmap::IndexMap;
use kafka_protocol::messages::{
    produce_request::{PartitionProduceData, TopicProduceData},
    ApiKey, ProduceRequest, ProduceResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};

use kafka_backup_core::config::{ConnectionConfig, KafkaConfig, SecurityConfig, TopicSelection};
use kafka_backup_core::kafka::KafkaClient;

fn test_config() -> KafkaConfig {
    KafkaConfig {
        bootstrap_servers: vec!["localhost:9092".to_string()],
        security: SecurityConfig::default(),
        topics: TopicSelection::default(),
        connection: ConnectionConfig::default(),
    }
}

/// Test 1: Produce with our current record construction (NO_ constants)
#[tokio::test]
#[ignore = "requires Docker Kafka on localhost:9092"]
async fn test_produce_with_no_constants() {
    let client = KafkaClient::new(test_config());
    client.connect().await.expect("Failed to connect");

    let records = [Record {
        transactional: false,
        control: false,
        partition_leader_epoch: NO_PARTITION_LEADER_EPOCH, // -1
        producer_id: NO_PRODUCER_ID,                       // -1
        producer_epoch: NO_PRODUCER_EPOCH,                 // -1
        timestamp_type: TimestampType::Creation,
        offset: 0,
        sequence: NO_SEQUENCE, // -1
        timestamp: chrono::Utc::now().timestamp_millis(),
        key: Some(Bytes::from("test-key")),
        value: Some(Bytes::from("test-value")),
        headers: IndexMap::new(),
    }];

    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("encode failed");

    let records_bytes = buf.freeze();
    println!("Record batch bytes (hex): {:02x?}", records_bytes.as_ref());
    println!("Record batch length: {} bytes", records_bytes.len());

    // Produce using API v8
    let result = produce_raw(&client, "orders", 0, records_bytes.clone(), 8).await;
    println!("Produce v8 with NO_ constants: {:?}", result);

    // Also try API v9
    let result = produce_raw(&client, "orders", 0, records_bytes, 9).await;
    println!("Produce v9 with NO_ constants: {:?}", result);
}

/// Test 2: Produce with partition_leader_epoch=0 and sequence=offset (matching crate test)
#[tokio::test]
#[ignore = "requires Docker Kafka on localhost:9092"]
async fn test_produce_with_zero_epoch() {
    let client = KafkaClient::new(test_config());
    client.connect().await.expect("Failed to connect");

    let now = chrono::Utc::now().timestamp_millis();
    let records = [
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0, // <-- 0 instead of -1
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: 0, // <-- 0 instead of -1
            timestamp: now,
            key: Some(Bytes::from("key0")),
            value: Some(Bytes::from("value0")),
            headers: IndexMap::new(),
        },
        Record {
            transactional: false,
            control: false,
            partition_leader_epoch: 0,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 1,
            sequence: 1,
            timestamp: now,
            key: Some(Bytes::from("key1")),
            value: Some(Bytes::from("value1")),
            headers: IndexMap::new(),
        },
    ];

    let mut buf = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf,
        records.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .expect("encode failed");

    let records_bytes = buf.freeze();
    println!("Record batch bytes (hex): {:02x?}", records_bytes.as_ref());
    println!("Record batch length: {} bytes", records_bytes.len());

    let result = produce_raw(&client, "orders", 0, records_bytes.clone(), 8).await;
    println!("Produce v8 with epoch=0: {:?}", result);

    let result = produce_raw(&client, "orders", 0, records_bytes, 9).await;
    println!("Produce v9 with epoch=0: {:?}", result);
}

/// Test 3: Produce using our BackupRecord -> Record conversion (same as produce.rs)
#[tokio::test]
#[ignore = "requires Docker Kafka on localhost:9092"]
async fn test_produce_via_client_method() {
    let client = KafkaClient::new(test_config());
    client.connect().await.expect("Failed to connect");

    let records = vec![kafka_backup_core::manifest::BackupRecord {
        key: Some(b"test-key".to_vec()),
        value: Some(b"test-value".to_vec()),
        headers: vec![],
        timestamp: chrono::Utc::now().timestamp_millis(),
        offset: 0,
    }];

    let result = client.produce("orders", 0, records, -1, 30_000).await;
    println!("Produce via client.produce(): {:?}", result);
}

/// Test 4: Batch produce - isolate whether separate batches per record causes error 87
#[tokio::test]
#[ignore = "requires Docker Kafka on localhost:9092"]
async fn test_batch_sequence_fix() {
    let client = KafkaClient::new(test_config());
    client.connect().await.expect("Failed to connect");

    let now = chrono::Utc::now().timestamp_millis();

    // Test A: Multiple records with sequence=-1 (each becomes its own batch)
    println!("--- Test A: sequence=-1 for all (separate batches) ---");
    let records_a: Vec<Record> = (0..5)
        .map(|i| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: NO_SEQUENCE, // -1 for all
            timestamp: now + i as i64,
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Some(Bytes::from(format!("val-{}", i))),
            headers: IndexMap::new(),
        })
        .collect();

    let mut buf_a = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf_a,
        records_a.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    println!("  Encoded {} bytes for 5 records", buf_a.len());
    let result_a = produce_raw(&client, "orders", 0, buf_a.freeze(), 8).await;
    println!(
        "  Result: {:?}",
        result_a.as_ref().map(|_| "OK").map_err(|e| e.clone())
    );

    // Test B: Multiple records with sequence=offset (all in one batch)
    println!("\n--- Test B: sequence=offset (single batch) ---");
    let records_b: Vec<Record> = (0..5)
        .map(|i| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: i, // matches offset
            timestamp: now + i as i64,
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Some(Bytes::from(format!("val-{}", i))),
            headers: IndexMap::new(),
        })
        .collect();

    let mut buf_b = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf_b,
        records_b.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    println!("  Encoded {} bytes for 5 records", buf_b.len());
    let result_b = produce_raw(&client, "orders", 0, buf_b.freeze(), 8).await;
    println!(
        "  Result: {:?}",
        result_b.as_ref().map(|_| "OK").map_err(|e| e.clone())
    );

    // Test C: All records with offset=0, sequence=-1 (all in one batch since condition matches)
    println!("\n--- Test C: offset=0 for all, sequence=-1 (single batch) ---");
    let records_c: Vec<Record> = (0..5)
        .map(|i| Record {
            transactional: false,
            control: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: 0, // all zero
            sequence: NO_SEQUENCE,
            timestamp: now + i as i64,
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Some(Bytes::from(format!("val-{}", i))),
            headers: IndexMap::new(),
        })
        .collect();

    let mut buf_c = BytesMut::new();
    RecordBatchEncoder::encode(
        &mut buf_c,
        records_c.iter(),
        &RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        },
    )
    .unwrap();
    println!("  Encoded {} bytes for 5 records", buf_c.len());
    let result_c = produce_raw(&client, "orders", 0, buf_c.freeze(), 8).await;
    println!(
        "  Result: {:?}",
        result_c.as_ref().map(|_| "OK").map_err(|e| e.clone())
    );
}

/// Test 5: Read records from actual backup segment and produce them
#[tokio::test]
#[ignore = "requires Docker Kafka on localhost:9092"]
async fn test_produce_from_backup_segment() {
    use kafka_backup_core::manifest::RecordHeader;
    use kafka_backup_core::segment::SegmentReader;

    let client = KafkaClient::new(test_config());
    client.connect().await.expect("Failed to connect");

    // Read a segment file from the backup
    let segment_path =
        "/tmp/repart-test-backup/repart-test/topics/orders/partition=0/segment-000000.bin.zst";
    let data = std::fs::read(segment_path).expect("Failed to read segment file");
    println!("Segment file size: {} bytes", data.len());

    let mut reader = SegmentReader::open(Bytes::from(data)).expect("Failed to open segment");
    let binary_records = reader.read_all().expect("Failed to read records");
    println!("Read {} records from segment", binary_records.len());

    // Convert to BackupRecord (same as restore engine)
    let backup_records: Vec<kafka_backup_core::manifest::BackupRecord> = binary_records
        .into_iter()
        .map(|br| kafka_backup_core::manifest::BackupRecord {
            key: br.key.map(|b| b.to_vec()),
            value: br.value.map(|b| b.to_vec()),
            headers: br
                .headers
                .into_iter()
                .map(|(k, v)| RecordHeader {
                    key: k,
                    value: v.map(|b| b.to_vec()).unwrap_or_default(),
                })
                .collect(),
            timestamp: br.timestamp,
            offset: br.offset,
        })
        .collect();

    // Print first record for debugging
    if let Some(first) = backup_records.first() {
        println!(
            "First record: offset={}, timestamp={}, key={:?}, value_len={:?}, headers={}",
            first.offset,
            first.timestamp,
            first
                .key
                .as_ref()
                .map(|k| String::from_utf8_lossy(k).to_string()),
            first.value.as_ref().map(|v| v.len()),
            first.headers.len(),
        );
    }

    // Test 4a: produce single record from backup
    println!("\n--- Test 4a: Single record from backup ---");
    let single = vec![backup_records[0].clone()];
    let result = client.produce("orders", 0, single, -1, 30_000).await;
    println!("Single record produce: {:?}", result);

    // Test 4b: produce batch of 10 records from backup
    println!("\n--- Test 4b: Batch of 10 records from backup ---");
    let batch: Vec<_> = backup_records.iter().take(10).cloned().collect();
    let result = client.produce("orders", 0, batch, -1, 30_000).await;
    println!("Batch of 10 produce: {:?}", result);

    // Test 4c: produce ALL records from segment (like restore does)
    println!(
        "\n--- Test 4c: All {} records from backup ---",
        backup_records.len()
    );
    let result = client
        .produce("orders", 0, backup_records.clone(), -1, 30_000)
        .await;
    println!("All records produce: {:?}", result);

    // Test 4d: produce to the target topic orders-restored
    println!("\n--- Test 4d: Produce to orders-restored ---");
    let single = vec![backup_records[0].clone()];
    let result = client
        .produce("orders-restored", 0, single, -1, 30_000)
        .await;
    println!("Produce to orders-restored: {:?}", result);
}

async fn produce_raw(
    client: &KafkaClient,
    topic: &str,
    partition: i32,
    records: Bytes,
    _api_version: i16,
) -> Result<ProduceResponse, String> {
    let partition_data = PartitionProduceData::default()
        .with_index(partition)
        .with_records(Some(records));

    let topic_data = TopicProduceData::default()
        .with_name(TopicName(StrBytes::from_string(topic.to_string())))
        .with_partition_data(vec![partition_data]);

    let request = ProduceRequest::default()
        .with_acks(-1)
        .with_timeout_ms(30000)
        .with_topic_data(vec![topic_data]);

    let response: ProduceResponse = client
        .send_request(ApiKey::Produce, request)
        .await
        .map_err(|e| format!("send failed: {}", e))?;

    for topic_response in &response.responses {
        for partition_response in &topic_response.partition_responses {
            println!(
                "  Partition {}: error_code={}, base_offset={}, error_msg={:?}",
                partition_response.index,
                partition_response.error_code,
                partition_response.base_offset,
                partition_response.error_message,
            );
        }
    }

    Ok(response)
}
