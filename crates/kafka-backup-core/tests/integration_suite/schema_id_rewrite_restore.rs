//! `restore.rewrite_schema_ids` on both restore paths.
//!
//! The 1:1 partition path rewrote Confluent schema IDs, the repartition
//! (fan-out) path did not: a repartitioned topic reached the target with the
//! source registry's IDs. These tests run in-process with no Docker, against
//! a one-broker mock that decodes every produced record batch, and check the
//! bytes that were produced: rewritten IDs, the topic allow-list
//! (`schema_id_rewrite_topics`), the per-topic counts in the report, and
//! `schema_id_rewrite_strict` (on by default, as 0.24 always failed on an
//! unmapped ID).

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    ApiKey, BrokerId, MetadataResponse, ProduceRequest, ProduceResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, StrBytes};
use kafka_protocol::records::RecordBatchDecoder;
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use kafka_backup_core::config::{
    CompressionType, Config, ConnectionConfig, KafkaConfig, Mode, RepartitioningStrategy,
    RestoreOptions, SecurityConfig, TopicRepartitioning, TopicSelection,
};
use kafka_backup_core::manifest::{BackupManifest, BackupRecord, RecordHeader, RestoreReport};
use kafka_backup_core::metrics::PerformanceMetrics;
use kafka_backup_core::restore::transform::{RecordTransform, RecordTransformHandle};
use kafka_backup_core::restore::RestoreEngine;
use kafka_backup_core::segment::{BinaryRecord, SegmentWriter, SegmentWriterConfig};
use kafka_backup_core::storage::{create_backend, StorageBackendConfig};

use super::sasl_mock_broker::{read_request, write_response};

const TOPIC: &str = "schema-id-topic";
const PARTITIONS: i32 = 2;
const RECORDS_PER_PARTITION: i64 = 10;
/// Source registry IDs in the backup and the target IDs they map to.
const VALUE_ID: i32 = 7;
const KEY_ID: i32 = 8;
const UNMAPPED_ID: i32 = 9;
const MAPPING: [(i32, i32); 2] = [(VALUE_ID, 42), (KEY_ID, 43)];
/// Per partition: every value and every even key are framed.
const FRAMED_PAYLOADS: u64 = (PARTITIONS as u64) * (RECORDS_PER_PARTITION as u64 * 3 / 2);

#[derive(Debug, Clone)]
struct Produced {
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    /// As kafka-protocol decodes them (a map: one value per key).
    headers: HashMap<String, Option<Vec<u8>>>,
}

/// One broker leading every partition of `TOPIC`; records every produced
/// record (decoded from the request's record batches).
struct CapturingBroker {
    addr: std::net::SocketAddr,
    produced: Arc<Mutex<Vec<Produced>>>,
    handle: JoinHandle<()>,
}

impl CapturingBroker {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let produced = Arc::new(Mutex::new(Vec::new()));
        let next_offset = Arc::new(Mutex::new(HashMap::<i32, i64>::new()));
        let produced_c = produced.clone();
        let handle = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                tokio::spawn(serve(stream, addr, produced_c.clone(), next_offset.clone()));
            }
        });
        Self {
            addr,
            produced,
            handle,
        }
    }

    async fn produced(&self) -> Vec<Produced> {
        self.produced.lock().await.clone()
    }

    async fn shutdown(self) {
        self.handle.abort();
        let _ = self.handle.await;
    }
}

async fn serve(
    mut stream: TcpStream,
    addr: std::net::SocketAddr,
    produced: Arc<Mutex<Vec<Produced>>>,
    next_offset: Arc<Mutex<HashMap<i32, i64>>>,
) {
    while let Some((api_key, api_version, correlation_id, body)) = read_request(&mut stream).await {
        match api_key {
            ApiKey::Metadata => {
                let resp = metadata_response(addr);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            ApiKey::Produce => {
                let mut body = body;
                let request = ProduceRequest::decode(&mut body, api_version).expect("produce");
                let mut topics = Vec::new();
                for topic in request.topic_data {
                    let mut partitions = Vec::new();
                    for partition in topic.partition_data {
                        let mut records = partition.records.expect("records");
                        let set = RecordBatchDecoder::decode(&mut records).expect("batch");
                        let count = set.records.len() as i64;
                        produced
                            .lock()
                            .await
                            .extend(set.records.into_iter().map(|r| {
                                Produced {
                                    key: r.key.map(|k| k.to_vec()),
                                    value: r.value.map(|v| v.to_vec()),
                                    headers: r
                                        .headers
                                        .into_iter()
                                        .map(|(k, v)| (k.to_string(), v.map(|v| v.to_vec())))
                                        .collect(),
                                }
                            }));
                        let mut offsets = next_offset.lock().await;
                        let base = offsets.entry(partition.index).or_insert(0);
                        partitions.push(
                            PartitionProduceResponse::default()
                                .with_index(partition.index)
                                .with_error_code(0)
                                .with_base_offset(*base),
                        );
                        *base += count;
                    }
                    topics.push(
                        TopicProduceResponse::default()
                            .with_name(topic.name)
                            .with_partition_responses(partitions),
                    );
                }
                let resp = ProduceResponse::default().with_responses(topics);
                write_response(&mut stream, api_key, api_version, correlation_id, &resp).await;
            }
            other => panic!("schema-id mock broker: unexpected request {other:?}"),
        }
    }
}

fn metadata_response(addr: std::net::SocketAddr) -> MetadataResponse {
    let partitions = (0..PARTITIONS)
        .map(|p| {
            MetadataResponsePartition::default()
                .with_error_code(0)
                .with_partition_index(p)
                .with_leader_id(BrokerId(1))
                .with_replica_nodes(vec![BrokerId(1)])
                .with_isr_nodes(vec![BrokerId(1)])
        })
        .collect();
    MetadataResponse::default()
        .with_brokers(vec![MetadataResponseBroker::default()
            .with_node_id(BrokerId(1))
            .with_host(StrBytes::from_string(addr.ip().to_string()))
            .with_port(i32::from(addr.port()))])
        .with_controller_id(BrokerId(1))
        .with_topics(vec![MetadataResponseTopic::default()
            .with_error_code(0)
            .with_name(Some(TopicName(StrBytes::from_static_str(TOPIC))))
            .with_is_internal(false)
            .with_partitions(partitions)])
}

/// Confluent wire format: magic byte 0, 4-byte big-endian schema ID, payload.
fn framed(id: i32, payload: &str) -> Vec<u8> {
    let mut bytes = vec![0];
    bytes.extend_from_slice(&id.to_be_bytes());
    bytes.extend_from_slice(payload.as_bytes());
    bytes
}

fn schema_id(payload: &[u8]) -> Option<i32> {
    (payload.len() >= 5 && payload[0] == 0)
        .then(|| i32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]))
}

/// Partition `p`, offset `i`: value framed with `VALUE_ID`; key framed with
/// `KEY_ID` for even `i`, plain text for odd `i`. With `unmapped`, partition
/// 0 gets one more record whose value carries `UNMAPPED_ID`.
async fn write_backup(root: &std::path::Path, backup_id: &str, unmapped: bool) {
    let storage = create_backend(&StorageBackendConfig::Filesystem {
        path: root.to_path_buf(),
    })
    .unwrap();
    let mut manifest = BackupManifest::new(backup_id.to_string());
    manifest.compression = "none".to_string();
    for partition in 0..PARTITIONS {
        let config = SegmentWriterConfig {
            compression: CompressionType::None,
            max_segment_bytes: 1024 * 1024,
            ..Default::default()
        };
        let mut writer =
            SegmentWriter::new(config, storage.clone(), Arc::new(PerformanceMetrics::new()));
        let extra = i64::from(unmapped && partition == 0);
        for i in 0..RECORDS_PER_PARTITION + extra {
            let (key, value) = if i == RECORDS_PER_PARTITION {
                (b"unmapped".to_vec(), framed(UNMAPPED_ID, "lost"))
            } else if i % 2 == 0 {
                (
                    framed(KEY_ID, &format!("k{i}")),
                    framed(VALUE_ID, &format!("v{i}")),
                )
            } else {
                (
                    format!("plain-key-{i}").into_bytes(),
                    framed(VALUE_ID, &format!("v{i}")),
                )
            };
            writer
                .add_record(BinaryRecord {
                    timestamp: 1_700_000_000_000 + i,
                    offset: i,
                    key: Some(Bytes::from(key)),
                    value: Some(Bytes::from(value)),
                    headers: vec![],
                })
                .unwrap();
        }
        let key = format!(
            "{backup_id}/topics/{TOPIC}/partition={partition}/segment-{:020}.bin",
            0
        );
        let meta = writer.flush(&key).await.unwrap().expect("segment");
        manifest
            .get_or_create_topic(TOPIC)
            .get_or_create_partition(partition)
            .add_segment(meta);
    }
    storage
        .put(
            &format!("{backup_id}/manifest.json"),
            Bytes::from(serde_json::to_vec_pretty(&manifest).unwrap()),
        )
        .await
        .unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Path {
    OneToOne,
    Repartition,
}

fn restore_config(
    bootstrap: &str,
    root: &std::path::Path,
    backup_id: &str,
    path: Path,
    tweak: impl FnOnce(&mut RestoreOptions),
) -> Config {
    let mut restore = RestoreOptions {
        restore_topic_configs: false,
        create_topics: false,
        include_original_offset_header: false,
        produce_batch_size: 4,
        produce_acks: 1,
        // acks other than -1 need idempotence off (0.25)
        idempotent_producer: false,
        rewrite_schema_ids: true,
        schema_id_mapping: MAPPING.into_iter().collect(),
        ..Default::default()
    };
    if let Path::Repartition = path {
        restore.repartitioning = HashMap::from([(
            TOPIC.to_string(),
            TopicRepartitioning {
                strategy: RepartitioningStrategy::Murmur2,
                target_partitions: PARTITIONS,
            },
        )]);
    }
    tweak(&mut restore);
    Config {
        mode: Mode::Restore,
        backup_id: backup_id.to_string(),
        source: None,
        target: Some(KafkaConfig {
            bootstrap_servers: vec![bootstrap.to_string()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: vec![TOPIC.to_string()],
                exclude: vec![],
            },
            connection: ConnectionConfig {
                connections_per_broker: 1,
                ..Default::default()
            },
        }),
        storage: StorageBackendConfig::Filesystem {
            path: root.to_path_buf(),
        },
        backup: None,
        restore: Some(restore),
        offset_storage: None,
        metrics: None,
    }
}

async fn restore(
    path: Path,
    unmapped: bool,
    tweak: impl FnOnce(&mut RestoreOptions),
) -> (kafka_backup_core::Result<RestoreReport>, Vec<Produced>) {
    let broker = CapturingBroker::start().await;
    let tmp = TempDir::new().unwrap();
    let backup_id = format!("schema-ids-{path:?}").to_lowercase();
    write_backup(tmp.path(), &backup_id, unmapped).await;
    let config = restore_config(
        &broker.addr.to_string(),
        tmp.path(),
        &backup_id,
        path,
        tweak,
    );
    let result = RestoreEngine::new(config).expect("engine").run().await;
    let produced = broker.produced().await;
    broker.shutdown().await;
    (result, produced)
}

fn assert_rewritten(produced: &[Produced], path: Path) {
    let expected_records = (PARTITIONS as i64 * RECORDS_PER_PARTITION) as usize;
    assert_eq!(produced.len(), expected_records, "{path:?}");
    for record in produced {
        let value = record.value.as_deref().unwrap();
        assert_eq!(schema_id(value), Some(42), "{path:?}: value {value:?}");
        let key = record.key.as_deref().unwrap();
        if key.starts_with(b"plain-key-") {
            continue;
        }
        assert_eq!(schema_id(key), Some(43), "{path:?}: key {key:?}");
    }
    // The payload after the 5-byte prefix is untouched.
    assert!(produced
        .iter()
        .any(|r| r.value.as_deref().unwrap()[5..] == *b"v0"));
}

fn topic_counts(report: &RestoreReport) -> (u64, u64) {
    let topic = report
        .topics_restored
        .iter()
        .find(|t| t.source_topic == TOPIC)
        .expect("topic report");
    (topic.schema_ids_rewritten, topic.schema_ids_unmapped)
}

#[tokio::test]
async fn one_to_one_restore_rewrites_schema_ids_and_counts_them() {
    let (result, produced) = restore(Path::OneToOne, false, |_| {}).await;
    let report = result.expect("restore");
    assert_rewritten(&produced, Path::OneToOne);
    assert_eq!(topic_counts(&report), (FRAMED_PAYLOADS, 0));
}

#[tokio::test]
async fn repartitioned_restore_rewrites_schema_ids_and_counts_them() {
    let (result, produced) = restore(Path::Repartition, false, |_| {}).await;
    let report = result.expect("restore");
    assert_rewritten(&produced, Path::Repartition);
    assert_eq!(topic_counts(&report), (FRAMED_PAYLOADS, 0));
}

#[tokio::test]
async fn topic_allow_list_scopes_the_rewrite_on_both_paths() {
    for path in [Path::OneToOne, Path::Repartition] {
        let (result, produced) = restore(path, false, |r| {
            r.schema_id_rewrite_topics = vec!["other-*".to_string()];
        })
        .await;
        let report = result.expect("restore");
        assert_eq!(topic_counts(&report), (0, 0), "{path:?}");
        assert!(
            produced
                .iter()
                .all(|r| schema_id(r.value.as_deref().unwrap()) == Some(VALUE_ID)),
            "{path:?}: a topic outside the allow-list keeps its source IDs"
        );

        let (result, produced) = restore(path, false, |r| {
            r.schema_id_rewrite_topics = vec!["schema-id-*".to_string()];
        })
        .await;
        result.expect("restore");
        assert_rewritten(&produced, path);
    }
}

#[tokio::test]
async fn unmapped_ids_are_left_and_counted_when_not_strict() {
    for path in [Path::OneToOne, Path::Repartition] {
        let (result, produced) = restore(path, true, |r| {
            r.schema_id_rewrite_strict = false;
        })
        .await;
        let report = result.expect("restore");
        assert_eq!(topic_counts(&report), (FRAMED_PAYLOADS, 1), "{path:?}");
        let lost = produced
            .iter()
            .find(|r| r.key.as_deref() == Some(&b"unmapped"[..]))
            .expect("the unmapped record is produced");
        assert_eq!(schema_id(lost.value.as_deref().unwrap()), Some(UNMAPPED_ID));
    }
}

#[tokio::test]
async fn an_unmapped_id_fails_the_restore_by_default_on_both_paths() {
    for path in [Path::OneToOne, Path::Repartition] {
        let (result, produced) = restore(path, true, |_| {}).await;
        let err = result.expect_err("strict restore fails").to_string();
        for needle in [
            "schema ID 9",
            TOPIC,
            "partition 0",
            &format!("offset {RECORDS_PER_PARTITION}"),
            "schema_id_mapping",
        ] {
            assert!(
                err.contains(needle),
                "{path:?}: {needle:?} missing in: {err}"
            );
        }
        assert!(
            produced
                .iter()
                .all(|r| r.key.as_deref() != Some(&b"unmapped"[..])),
            "{path:?}: the unmapped record must not be produced"
        );
    }
}

/// A record transform, as Kafka Backup Enterprise's registry restore uses.
/// It must see the record as archived: the source registry's value ID (the
/// schema-ID rewrite runs after it) and no kafka-backup offset headers (they
/// are added after it). It tags each record, and fails at `fail_at`.
struct CheckingTransform {
    fail_at: Option<i64>,
}

impl RecordTransform for CheckingTransform {
    fn name(&self) -> &str {
        "checking"
    }
    fn fingerprint(&self) -> &str {
        "checking-v1"
    }
    fn transform(
        &self,
        source_topic: &str,
        record: &mut BackupRecord,
    ) -> kafka_backup_core::Result<()> {
        let fail = |why: String| Err(kafka_backup_core::Error::Config(why));
        if self.fail_at == Some(record.offset) {
            return fail(format!("test transform rejects offset {}", record.offset));
        }
        let value_id = record.value.as_deref().and_then(schema_id);
        if value_id != Some(VALUE_ID) {
            return fail(format!("transform saw value schema ID {value_id:?}"));
        }
        if record.headers.iter().any(|h| h.key.starts_with("x-")) {
            return fail(format!("transform saw headers {:?}", record.headers));
        }
        record.headers.push(RecordHeader {
            key: "transformed-by".to_string(),
            value: Some(source_topic.as_bytes().to_vec()),
        });
        Ok(())
    }
}

#[tokio::test]
async fn transform_then_schema_id_rewrite_then_offset_headers_on_both_paths() {
    for path in [Path::OneToOne, Path::Repartition] {
        let (result, produced) = restore(path, false, |r| {
            r.include_original_offset_header = true;
            r.record_transform = Some(RecordTransformHandle::new(Arc::new(CheckingTransform {
                fail_at: None,
            })));
        })
        .await;
        let report = result.expect("restore");
        assert_rewritten(&produced, path);
        assert_eq!(topic_counts(&report), (FRAMED_PAYLOADS, 0), "{path:?}");
        for record in &produced {
            assert_eq!(
                record.headers.get("transformed-by"),
                Some(&Some(TOPIC.as_bytes().to_vec())),
                "{path:?}: {record:?}"
            );
            let offset = record
                .headers
                .get("x-original-offset")
                .cloned()
                .flatten()
                .map(|v| i64::from_le_bytes(v.try_into().unwrap()));
            assert!(
                offset.is_some_and(|o| (0..RECORDS_PER_PARTITION).contains(&o)),
                "{path:?}: offset header {offset:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_failing_transform_stops_the_restore_before_its_segment_is_produced() {
    for path in [Path::OneToOne, Path::Repartition] {
        let (result, produced) = restore(path, false, |r| {
            r.record_transform = Some(RecordTransformHandle::new(Arc::new(CheckingTransform {
                fail_at: Some(5),
            })));
        })
        .await;
        let err = result.expect_err("the transform fails").to_string();
        assert!(
            err.contains("test transform rejects offset 5"),
            "{path:?}: {err}"
        );
        // Every partition holds offset 5 in its only segment: the transform
        // fails before any record of it is routed or produced.
        assert!(produced.is_empty(), "{path:?}: produced {}", produced.len());
    }
}

/// The same check against a real broker: back up a two-partition topic of
/// framed records, restore it 1:1 and repartitioned (into 3 partitions), and
/// read every restored record back.
#[tokio::test]
#[ignore = "requires Docker"]
async fn schema_ids_are_rewritten_on_both_paths_against_kafka() {
    use super::common::{create_temp_storage, KafkaTestCluster};
    use kafka_backup_core::backup::BackupEngine;
    use kafka_backup_core::config::BackupOptions;
    use kafka_backup_core::kafka::TopicToCreate;
    use kafka_backup_core::manifest::BackupRecord;
    use std::time::Duration;

    const SOURCE: &str = "schema-ids-src";
    let cluster = KafkaTestCluster::start().await.expect("start Kafka");
    cluster
        .wait_for_ready(Duration::from_secs(30))
        .await
        .expect("Kafka ready");
    let bs = cluster.bootstrap_servers.clone();
    let client = cluster.create_client();
    client.connect().await.expect("connect");
    let created = client
        .create_topics(
            vec![TopicToCreate {
                name: SOURCE.to_string(),
                num_partitions: PARTITIONS,
                replication_factor: 1,
            }],
            30_000,
        )
        .await
        .expect("create topic");
    assert!(created.iter().all(|r| r.is_success_or_exists()));
    tokio::time::sleep(Duration::from_secs(2)).await;
    for partition in 0..PARTITIONS {
        let records = (0..RECORDS_PER_PARTITION)
            .map(|i| BackupRecord {
                key: Some(if i % 2 == 0 {
                    framed(KEY_ID, &format!("k{partition}-{i}"))
                } else {
                    format!("plain-key-{partition}-{i}").into_bytes()
                }),
                value: Some(framed(VALUE_ID, &format!("v{i}"))),
                headers: vec![],
                timestamp: 1_700_000_000_000 + i,
                offset: i,
            })
            .collect();
        client
            .produce(SOURCE, partition, records, -1, 30_000)
            .await
            .expect("produce");
    }

    let storage = create_temp_storage();
    let backup_id = "schema-ids-docker";
    let backup = Config {
        mode: Mode::Backup,
        backup_id: backup_id.to_string(),
        source: Some(KafkaConfig {
            bootstrap_servers: vec![bs.clone()],
            security: SecurityConfig::default(),
            topics: TopicSelection {
                include: vec![SOURCE.to_string()],
                exclude: vec![],
            },
            connection: Default::default(),
        }),
        target: None,
        storage: StorageBackendConfig::Filesystem {
            path: storage.path().to_path_buf(),
        },
        backup: Some(BackupOptions {
            stop_at_current_offsets: true,
            continuous: false,
            compression: CompressionType::Zstd,
            ..Default::default()
        }),
        restore: None,
        offset_storage: None,
        metrics: None,
    };
    tokio::time::timeout(
        Duration::from_secs(60),
        BackupEngine::new(backup)
            .await
            .expect("backup engine")
            .run(),
    )
    .await
    .expect("backup timed out")
    .expect("backup");

    for (path, target, target_partitions) in [
        (Path::OneToOne, "schema-ids-1to1", PARTITIONS),
        (Path::Repartition, "schema-ids-repart", 3),
    ] {
        let mut restore = RestoreOptions {
            topic_mapping: HashMap::from([(SOURCE.to_string(), target.to_string())]),
            create_topics: true,
            rewrite_schema_ids: true,
            schema_id_mapping: MAPPING.into_iter().collect(),
            schema_id_rewrite_strict: true,
            ..Default::default()
        };
        if let Path::Repartition = path {
            restore.repartitioning = HashMap::from([(
                target.to_string(),
                TopicRepartitioning {
                    strategy: RepartitioningStrategy::Murmur2,
                    target_partitions,
                },
            )]);
        }
        let config = Config {
            mode: Mode::Restore,
            backup_id: backup_id.to_string(),
            source: None,
            target: Some(KafkaConfig {
                bootstrap_servers: vec![bs.clone()],
                security: SecurityConfig::default(),
                topics: TopicSelection::default(),
                connection: Default::default(),
            }),
            storage: StorageBackendConfig::Filesystem {
                path: storage.path().to_path_buf(),
            },
            backup: None,
            restore: Some(restore),
            offset_storage: None,
            metrics: None,
        };
        let report = tokio::time::timeout(
            Duration::from_secs(60),
            RestoreEngine::new(config).expect("restore engine").run(),
        )
        .await
        .expect("restore timed out")
        .unwrap_or_else(|e| panic!("{path:?} restore: {e}"));
        assert_eq!(
            report.topics_restored[0].schema_ids_rewritten, FRAMED_PAYLOADS,
            "{path:?}"
        );

        let mut restored = Vec::new();
        for partition in 0..target_partitions {
            let mut offset = 0;
            loop {
                let resp = client
                    .fetch(
                        target,
                        partition,
                        offset,
                        8 * 1024 * 1024,
                        kafka_backup_core::IsolationLevel::ReadUncommitted,
                    )
                    .await
                    .expect("fetch");
                if resp.records.is_empty() {
                    break;
                }
                offset = resp.next_offset;
                restored.extend(resp.records.into_iter().map(|r| Produced {
                    key: r.key,
                    value: r.value,
                    headers: r.headers.into_iter().map(|h| (h.key, h.value)).collect(),
                }));
            }
        }
        assert_rewritten(&restored, path);
    }
}
