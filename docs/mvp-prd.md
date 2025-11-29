OSO Kafka Backup & Restore – PRD (MVP)
1. Product Summary
OSO Kafka Backup & Restore is a Rust-based, deployment-agnostic tool for backing up and restoring Kafka and Kafka‑compatible clusters.
It connects as a Kafka client (using the Kafka protocol) to read events, writes them to a pluggable storage backend (S3-compatible object storage or filesystem), and can restore events (with optional time-window filtering) into any target cluster, including Confluent Cloud and on‑prem Kafka.

Scope of this PRD: core backup/restore engine + minimal CLI, no UI/operator yet.

2. Goals & Non‑Goals
2.1 Goals (MVP)
Back up selected Kafka topics from a source cluster to durable storage.

Restore backed-up topics into a target cluster (may be same or different).

Support time‑windowed restore (basic point‑in‑time restore).

Handle internal and system topics as regular topics where allowed.

Run in multiple environments: bare metal, VM, Docker, Kubernetes pod.

Configuration‑driven behavior via YAML config and CLI flags.

2.2 Non‑Goals (MVP)
No GUI / web control plane.

No Kubernetes Operator or CRDs (can be added later).

No advanced offset mapping automation beyond optional header-based original offset tracking.

No plugin system (masking/transforms) in MVP – design only for future extension.

3. Core Use Cases
Backup from self‑managed Kafka / MSK / Redpanda to S3-compatible storage

Backup from Confluent Cloud (as a client) to S3-compatible storage.

Restore from backup into:

the same cluster (disaster recovery), or

another cluster (migration, environment cloning).

Time‑bounded restore (e.g. “restore last 24h”, “restore until timestamp T”).

4. Architecture Overview
4.1 High-Level Components
Core Engine (Library crate: kafka-backup-core)

No Kubernetes dependencies.

Responsibilities:

Connect to source cluster with kafka-protocol-rs.

Discover topics/partitions via Metadata API.

Fetch records and write them to storage with segment metadata.

Restore records from storage to target cluster via Produce API.

CLI Tool (Binary crate: kafka-backup-cli)

Thin wrapper around the core engine.

Subcommands:

backup – run a backup with given config.

restore – restore from a backup.

list – list available backups in storage (simple manifest inspection).

Storage Backend Abstraction

Built on top of object_store crate or s3‑style client.

Implementations:

S3-compatible (Ceph RGW, SeaweedFS, etc.).

Local filesystem / PVC path.

Backup Data Format & Metadata

Per topic/partition segments:

Compressed batches of records (zstd or lz4).

Side metadata capturing:

topic, partition

start_offset, end_offset

start_timestamp, end_timestamp

Per backup manifest:

List of segments and their metadata.

Optional information about:

Cluster ID, source brokers

Included topics

Created_at, retention policy

4.2 Data Flow – Backup
Load config (source cluster, topics, storage config).

Connect to source brokers using kafka-protocol-rs.

Fetch Metadata to resolve partitions for selected topics.

For each topic/partition:

Start at configured starting offset (default: earliest).

Loop:

Fetch records (Fetch API).

Group them into batches/segments:

By max size (e.g. 128MB) and/or time window.

Compress and write the segment object to storage.

Record segment metadata in manifest.

4.3 Data Flow – Restore
Load config (target cluster, backup location, topics, time window).

Load manifest for chosen backup ID.

Filter segments by:

Topic/partition selection.

Overlapping time window (start_ts/end_ts).

For each selected segment:

Read from storage, decompress.

Stream records:

Filter by record timestamp to enforce precise time window.

Optionally include original offset as a header.

Produce to target topic/partition using kafka-protocol-rs.

5. Functional Requirements
5.1 Configuration
Config file (YAML) fields:

text
# Example: backup config
mode: backup
backup_id: "backup-2025-11-26-001"

source:
  bootstrap_servers:
    - broker-1:9092
    - broker-2:9092
  security:
    sasl_mechanism: plain  # or none
    sasl_username: "user"
    sasl_password: "pass"
    security_protocol: "SASL_SSL"  # or PLAINTEXT/TLS
  topics:
    include:
      - orders
      - payments
    exclude:
      - __consumer_offsets  # optional

storage:
  backend: "s3"   # s3 | ceph | filesystem
  endpoint: "https://object-store.example.com"
  bucket: "kafka-backups"
  access_key: "XXXX"
  secret_key: "YYYY"
  prefix: "cluster-a"

backup:
  segment_max_bytes: 134217728   # 128MB
  segment_max_interval_ms: 60000
  compression: "zstd"            # zstd | lz4 | none
Similar config for restore mode, with:

restore.time_window_start / restore.time_window_end.

restore.topic_mapping (optional: map orders → orders_clone, etc.).

5.2 Backup Mode
MUST:

Connect and authenticate to the source Kafka cluster.

List and validate configured topics (fail fast if missing).

For each topic/partition, stream records indefinitely until:

Interrupted, or

End offset reached (for one-shot backup).

Write segments to storage with:

Key structure:

text
{prefix}/{backup_id}/topics/{topic}/partition={partition}/segment-{sequence}.zst
{prefix}/{backup_id}/topics/{topic}/partition={partition}/segments.json  # optional index
Maintain a backup manifest at:

text
{prefix}/{backup_id}/manifest.json
SHOULD:

Support both one-shot “run until current end offset” and continuous mode.

Optionally store original offset as a header field in segment records for later mapping.

5.3 Restore Mode
MUST:

Load manifest for selected backup_id.

Apply topic and partition filters from config.

If a time window is specified:

Pre-filter segments using segment-level start/end timestamps.

Intra-segment filter per record at replay time.

Produce records to the target cluster respecting:

Original partition by default.

Optional remapping: topic name and/or partition remap.

Optionally inject a header with original offset and timestamp.

SHOULD:

Support parallel restore across multiple partitions using Tokio tasks.

Allow rate limiting / throttle parameters (max records/sec or MB/sec).

5.4 Topic Coverage (Basics)
MVP behavior:

Treat all non-internal topics as normal (as per include/exclude patterns).

Internal topics:

Default: exclude __consumer_offsets, __transaction_state.

Allow opt‑in to include them in backup (read-only).

Streams/Connect topics (*-changelog, *-repartition, connect-*) are just normal topics and can be included by pattern.

Offset mapping/automation beyond header-based tracking is explicitly out of scope for MVP.

6. Technical Design – Libraries and Stack
6.1 Language & Runtime
Rust, edition 2021+.

Async runtime: Tokio.

6.2 Kafka Protocol & Client
kafka-protocol crate (kafka_protocol-rs):

Used to construct and parse raw protocol messages (Metadata, Fetch, Produce, etc.).

Direct control over replica/client semantics (we will use it in “client mode” for MVP).

For MVP, behave like a consumer/producer:

Use Fetch API with replica_id = -1 (client semantics).

Use Produce API to write records during restore.

6.3 Storage
Primary abstraction: object_store crate (if suitable) or custom thin abstraction:

S3-compatible backend (Ceph RGW, SeaweedFS, etc.).

Local filesystem backend for dev/small deployments.

Compression:

zstd crate for high-ratio compression.

lz4_flex or lz4 crate for lower CPU, higher speed.

6.4 Serialization & Metadata
serde + serde_json or serde_yaml for config/manifest.

chrono or time crate for timestamps.

6.5 CLI & Logging
clap for CLI argument parsing.

tracing + tracing-subscriber for structured logs.

7. Data Structures (MVP-Level)
7.1 Manifest
rust
#[derive(Serialize, Deserialize)]
pub struct BackupManifest {
    pub backup_id: String,
    pub created_at: i64,  // epoch millis
    pub source_cluster_id: Option<String>,
    pub topics: Vec<TopicBackup>,
}

#[derive(Serialize, Deserialize)]
pub struct TopicBackup {
    pub name: String,
    pub partitions: Vec<PartitionBackup>,
}

#[derive(Serialize, Deserialize)]
pub struct PartitionBackup {
    pub partition_id: i32,
    pub segments: Vec<SegmentMetadata>,
}

#[derive(Serialize, Deserialize)]
pub struct SegmentMetadata {
    pub key: String,              // object_store key
    pub start_offset: i64,
    pub end_offset: i64,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub record_count: i64,
}
7.2 Internal Record Structure in Segments
rust
#[derive(Serialize, Deserialize)]
pub struct BackupRecord {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
    pub timestamp: i64,
    pub offset: i64,
}
8. Non‑Functional Requirements
8.1 Performance
Target: sustain tens of MB/s per partition on modest hardware (exact benchmarks later).

Use async I/O with concurrent partition processing.

Batching:

Fetch in reasonably large batches (configurable max bytes).

Write segments in multi-MB chunks to storage.

8.2 Reliability
On intermittent failure:

Resume backup from last committed segment/offset when restarted.

Ensure idempotent writes where possible (e.g. skip existing segment keys).

8.3 Security
Support TLS / SASL for Kafka connections.

Support HTTPS and credentials for S3-compatible storage.

Do not log sensitive credentials.

9. Minimal Milestones
Milestone 1 – Core Engine & CLI, Local FS
kafka-backup-core:

Connect to Kafka, fetch metadata, fetch records for topics.

Write segments to local filesystem with compression and manifest.

Restore from filesystem to a local Kafka.

kafka-backup-cli:

backup and restore commands.

Milestone 2 – S3-Compatible Storage
Implement S3 backend using object_store / S3 client.

Configurable endpoint, credentials, bucket, prefix.

Milestone 3 – Time-Windowed Restore
Segment-level and per-record timestamp filtering.

CLI options for --from-timestamp, --to-timestamp.
