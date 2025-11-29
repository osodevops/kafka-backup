# OSO Kafka Backup & Restore – Product Requirements Document (v2)

**With High-Performance Optimizations & Production Resilience**

---

## Table of Contents

1. [Product Summary](#1-product-summary)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Core Use Cases](#3-core-use-cases)
4. [Architecture Overview](#4-architecture-overview)
5. [Functional Requirements](#5-functional-requirements)
6. [High-Performance Design](#6-high-performance-design)
7. [Resilience & Recovery](#7-resilience--recovery)
8. [Technical Stack](#8-technical-stack)
9. [Data Structures](#9-data-structures)
10. [Non-Functional Requirements](#10-non-functional-requirements)
11. [Milestones](#11-milestones)

---

## 1. Product Summary

OSO Kafka Backup & Restore is a **high-performance, Rust-based, deployment-agnostic** tool for backing up and restoring Kafka and Kafka-compatible clusters.

It connects as a **Kafka client** using the Kafka protocol to read events, writes them to pluggable storage backends (S3-compatible object storage or filesystem) with intelligent batching and compression, and restores events with optional time-window filtering into any target cluster.

**Key Differentiators:**
- **10-20x faster than naive implementations** through mini-batching, zero-copy patterns, and efficient serialization
- **Resilient to process failure** via external offset state storage (SQLite + S3/Ceph sync)
- **Deployment agnostic**: CLI binary, Docker, Kubernetes, bare metal VM – same codebase everywhere
- **Production-grade**: Point-in-time restore, internal topics support, consumer offset handling, observability built-in

---

## 2. Goals & Non-Goals

### 2.1 Goals (MVP v2)

**Performance:**
- Back up selected Kafka topics at **100+ MB/s throughput** on commodity hardware
- Maintain **<100ms p99 latency** on checkpoint operations
- Achieve **3-5x compression ratio** for typical Kafka event streams
- Support **memory-efficient streaming** (<500MB resident for multi-partition backup)

**Functionality:**
- Backup selected Kafka topics to durable storage with intelligent batching
- Restore backed-up topics into target cluster with time-windowed point-in-time restore
- Handle internal and system topics (`__consumer_offsets`, Streams changelog, Connect offsets)
- Resume from failure by tracking offsets externally (SQLite + S3 sync)

**Operations:**
- Run in multiple environments: bare metal, VM, Docker, Kubernetes pod
- Configuration-driven behavior via YAML config and CLI flags
- Observability: metrics collection, health checks, error handling with circuit breakers
- Graceful shutdown and signal handling

### 2.2 Non-Goals (MVP v2)

- No GUI / web control plane (can be added later)
- No Kubernetes Operator or CRDs (can be added later)
- No plugin system for data masking/transforms (design for future extension)
- No automated offset mapping automation beyond header-based tracking

---

## 3. Core Use Cases

1. **Backup from self-managed Kafka / MSK / Redpanda to S3-compatible storage** with high throughput
2. **Backup from Confluent Cloud** (as a client) to S3-compatible storage
3. **Restore from backup** into:
   - Same cluster (disaster recovery)
   - Another cluster (migration, environment cloning)
4. **Time-bounded restore** (e.g. "restore last 24h", "restore until timestamp T")
5. **Resume backup after process failure** without data loss or duplication

---

## 4. Architecture Overview

### 4.1 High-Level Components

#### **Core Engine (Library crate: `kafka-backup-core`)**
- No Kubernetes dependencies
- Responsibilities:
  - Connect to source cluster with `kafka-protocol-rs`
  - Discover topics/partitions via Metadata API
  - Fetch records in parallel across partitions
  - Write to storage with mini-batching, compression, and zero-copy patterns
  - Restore records from storage to target cluster via Produce API

#### **CLI Tool (Binary crate: `kafka-backup-cli`)**
- Thin wrapper around the core engine
- Subcommands: `backup`, `restore`, `list`, `status`, `validate`
- Configuration via YAML files and CLI flags

#### **Offset State Storage (SQLite + S3 Sync)**
- In-memory SQLite database with periodic S3 sync
- Persists per-partition offset state
- Survives process/pod restart
- No external database dependency

#### **Storage Backend Abstraction**
- Built on `object_store` crate
- S3-compatible backend (Ceph RGW, SeaweedFS, etc.)
- Local filesystem backend for dev/small deployments

#### **Performance Optimization Layer**
- Segment writer with mini-batching
- Compression (zstd or lz4)
- Zero-copy record handling
- Async I/O with Tokio
- Metrics collection & profiling

### 4.2 Data Flow – Backup (High-Performance)

```
1. Load config (source, topics, storage)
2. Connect to brokers using kafka-protocol-rs
3. Fetch metadata to resolve partitions
4. For EACH PARTITION (in parallel):
   a. Load last offset from SQLite offset store
   b. Create SegmentWriter with 128MB batch buffer
   c. FETCH LOOP:
      - Fetch up to 100K records (async, non-blocking)
      - Add records to in-memory buffer (zero-copy references)
      - When buffer hits threshold OR timer expires:
        * Serialize records directly into Vec (single pass)
        * Compress with zstd (level 3)
        * Write segment to S3 (single I/O operation)
        * Record segment metadata locally
      - Every 5s checkpoint:
        * Write offsets to SQLite
        * Sync SQLite to S3
5. On graceful shutdown:
   a. Flush remaining buffered records
   b. Final checkpoint to durable storage
```

**Performance characteristics:**
- **Throughput**: 100+ MB/s per partition (achievable on 10GbE)
- **Latency**: <5ms between record fetch and buffer (no copy overhead)
- **I/O operations**: 1 S3 write per 128MB segment (vs 1 write per record)
- **Memory**: ~500MB for 4 concurrent partitions (batches + buffers only)

### 4.3 Data Flow – Restore (Time-Windowed)

```
1. Load config (target cluster, backup ID, time window)
2. Load manifest for backup
3. Filter segments by:
   - Topic/partition selection
   - Overlapping timestamp range (segment-level prefilter)
4. For EACH SELECTED SEGMENT (parallel):
   a. Stream from S3 (don't download entire segment)
   b. Decompress on-the-fly (zstd)
   c. For each record:
      - Check timestamp against window (intra-segment filter)
      - Optionally inject original offset as header
      - Batch produce to target (50KB batches)
   d. Produce batch to target partition
5. Report completion with record counts & latencies
```

---

## 5. Functional Requirements

### 5.1 Configuration (YAML-Driven)

```yaml
# backup-config.yaml
mode: backup
backup_id: "backup-2025-11-27-prod"

source:
  bootstrap_servers:
    - broker-1:9092
    - broker-2:9092
  security:
    sasl_mechanism: plain
    sasl_username: "backup-user"
    sasl_password: "${KAFKA_PASSWORD}"
    security_protocol: "SASL_SSL"
  topics:
    include:
      - orders
      - payments
      - user-events
    exclude:
      - __consumer_offsets  # Handle separately

storage:
  backend: "s3"  # s3 | ceph | filesystem
  endpoint: "https://s3.example.com"
  bucket: "kafka-backups"
  access_key: "${S3_ACCESS_KEY}"
  secret_key: "${S3_SECRET_KEY}"
  prefix: "cluster-prod-us-east"

backup:
  # Performance tuning
  segment_max_bytes: 134217728      # 128MB
  segment_max_interval_ms: 5000     # 5 seconds
  fetch_max_bytes: 52428800         # 50MB per fetch
  fetch_timeout_ms: 500
  
  compression: "zstd"               # zstd | lz4 | none
  compression_level: 3              # 1-22 for zstd
  
  # Resilience
  checkpoint_interval_secs: 5
  checkpoint_on_shutdown: true

offset_storage:
  backend: "sqlite"                 # sqlite | s3
  db_path: "./offsets.db"           # Local SQLite file
  s3_key: "backups/offset-state/offsets.db"  # Where to sync
  sync_interval_secs: 30
```

### 5.2 Backup Mode (Performance-Optimized)

**MUST:**
- Connect and authenticate to source Kafka cluster
- Validate configured topics exist (fail fast)
- For each topic/partition, stream records with:
  - **Mini-batching**: Accumulate up to 128MB or 5 seconds
  - **Compression**: zstd level 3 (42x faster than f64 with good ratio)
  - **Zero-copy serialization**: Direct buffer writes, no intermediate copies
  - **Async I/O**: Non-blocking fetch + write operations
- Write segments to storage with structure:
  ```
  {prefix}/{backup_id}/topics/{topic}/partition={partition}/segment-{sequence}.zst
  ```
- Maintain backup manifest at:
  ```
  {prefix}/{backup_id}/manifest.json
  ```
- Store offset state in SQLite locally + periodic S3 sync
- Track metrics: throughput, compression ratio, checkpoint latency

**SHOULD:**
- Support both one-shot and continuous backup modes
- Optionally store original offset as header during backup
- Parallel partition processing (Tokio concurrency)
- Circuit breaker for transient S3 failures
- Dead letter queue for malformed records

### 5.3 Restore Mode (Time-Windowed)

**MUST:**
- Load manifest for selected backup_id
- Apply topic/partition filters from config
- If time window specified:
  - Pre-filter segments using segment-level start/end timestamps
  - Intra-segment filter per record at replay time
- Produce records to target cluster respecting:
  - Original partition by default
  - Optional partition remapping (topic-A/0 → topic-B-copy/0)
- Optionally inject headers with original offset and timestamp
- Report statistics: records restored, latencies, any errors

**SHOULD:**
- Parallel restore across multiple partitions (Tokio tasks)
- Rate limiting / throttle parameters (max MB/sec)
- Resumable restore (track which segments completed)

### 5.4 Topic Coverage (Basics)

**Standard topics:**
- Default behavior: include/exclude by pattern
- Examples: `orders`, `payments`, `user-*`

**Internal topics:**
- Default: exclude `__consumer_offsets`, `__transaction_state`
- Allow opt-in to include them in backup (read-only)
- Streams/Connect topics (`*-changelog`, `*-repartition`, `connect-*`) are normal topics

**Offset handling:**
- Where `__consumer_offsets` is readable: back it up like any other topic
- Where not readable: store original offset in message headers
- Restore documentation guides consumer group rebalancing

---

## 6. High-Performance Design

### 6.1 Mini-Batching Strategy

```rust
pub struct SegmentWriter {
    buffer: Vec<u8>,
    current_size: usize,
    max_bytes: usize,
    max_interval: Duration,
    last_flush: Instant,
}

impl SegmentWriter {
    pub async fn add_record(&mut self, record: &BackupRecord) -> anyhow::Result<()> {
        // Serialize directly into buffer (zero-copy reference)
        self.serialize_into_buffer(record)?;
        
        // Flush conditions
        if self.should_flush() {
            self.flush().await?;
        }
        
        Ok(())
    }
    
    fn should_flush(&self) -> bool {
        self.current_size >= self.max_bytes || 
        self.last_flush.elapsed() >= self.max_interval
    }
    
    async fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        
        // Single compression pass
        let compressed = zstd::encode_all(&self.buffer[..], 3)?;
        
        // Single S3 write
        self.storage.put(&segment_key, compressed.into()).await?;
        
        self.buffer.clear();
        self.current_size = 0;
        self.last_flush = Instant::now();
        
        Ok(())
    }
}
```

**Impact:**
- **I/O reduction**: 1 write per 128MB vs 1 write per record (1000x fewer syscalls)
- **Throughput**: 100+ MB/s achievable
- **CPU**: Single serialization pass per batch

### 6.2 Zero-Copy Patterns

```rust
// Avoid copies: work with references
pub struct BackupRecord<'a> {
    key: &'a [u8],
    value: &'a [u8],
    offset: i64,
    timestamp: i64,
}

pub async fn serialize_batch<'a>(
    records: &'a [BackupRecord<'a>],
    buffer: &mut Vec<u8>,
) {
    for record in records {
        // Direct buffer writes, no intermediate Vec creation
        buffer.extend_from_slice(&(record.key.len() as u32).to_le_bytes());
        buffer.extend_from_slice(record.key);
        buffer.extend_from_slice(&(record.value.len() as u32).to_le_bytes());
        buffer.extend_from_slice(record.value);
        buffer.extend_from_slice(&record.offset.to_le_bytes());
        buffer.extend_from_slice(&record.timestamp.to_le_bytes());
    }
}
```

**Impact:**
- **Memory usage**: ~50% reduction
- **GC pressure**: Fewer allocations
- **Cache efficiency**: Data stays hot

### 6.3 Compression Strategy

```rust
// Balance speed vs compression
const ZSTD_LEVEL: i32 = 3;  // 42x faster than f64, acceptable compression

pub async fn compress_segment(buffer: &[u8]) -> anyhow::Result<Vec<u8>> {
    zstd::encode_all(buffer, ZSTD_LEVEL)
}
```

**Rationale:**
- **Level 3**: ~3-5x compression ratio for typical Kafka data
- **Speed**: <1ms for 128MB segment
- **Trade-off**: Good enough for most workloads

### 6.4 Async I/O Architecture

```rust
pub async fn backup_loop(
    mut consumer: KafkaConsumer,
    storage: Arc<dyn ObjectStore>,
) {
    let mut writer = SegmentWriter::new(128 * 1024 * 1024);
    let mut checkpoint_timer = interval(Duration::from_secs(5));
    
    loop {
        tokio::select! {
            // Non-blocking fetch
            msg = consumer.poll(Duration::from_millis(100)) => {
                for record in msg? {
                    writer.add_record(&record)?;
                    
                    if writer.should_flush() {
                        writer.flush(&storage).await?;
                    }
                }
            }
            // Periodic checkpoint
            _ = checkpoint_timer.tick() => {
                writer.flush(&storage).await?;
                offset_store.checkpoint(&backup_id, &offsets).await?;
            }
        }
    }
}
```

**Benefits:**
- **Parallelism**: Handle 4 partitions concurrently
- **I/O efficiency**: While uploading segment 1, fetching segment 2
- **CPU**: No threads blocked

### 6.5 Partition-Level Parallelism

```rust
pub async fn run(&self) -> anyhow::Result<()> {
    let mut tasks = vec![];
    
    // Spawn task per partition
    for (topic, partitions) in &self.topics {
        for partition in partitions {
            let task = self.replicate_partition(
                topic.clone(),
                *partition,
            );
            tasks.push(task);
        }
    }
    
    // Wait for all partitions
    futures::future::join_all(tasks).await
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?;
    
    Ok(())
}
```

**Throughput scaling:**
- 1 partition: 25 MB/s
- 4 partitions: 100 MB/s (linear scaling)
- 10 partitions: 250+ MB/s

### 6.6 Metrics & Profiling

```rust
pub struct PerformanceMetrics {
    records_processed: usize,
    bytes_written: usize,
    compression_ratio: f64,
    checkpoint_latencies: Vec<u64>,
    segment_write_latencies: Vec<u64>,
}

impl PerformanceMetrics {
    pub fn report(&self) {
        let avg_checkpoint = percentile_mean(&self.checkpoint_latencies);
        let p99_checkpoint = percentile_99(&self.checkpoint_latencies);
        
        println!("Throughput: {:.2} MB/s", self.throughput());
        println!("Compression: {:.2}x", self.compression_ratio);
        println!("Checkpoint latency: avg={}ms p99={}ms", 
            avg_checkpoint, p99_checkpoint);
    }
}
```

---

## 7. Resilience & Recovery

### 7.1 Offset State Storage (External)

**Design principle:** Offset state MUST survive process death

```
┌─────────────────────────────────┐
│   Backup Process (Rust)         │
│  - In-memory offset tracking    │
│  - Writes to SQLite every 5s    │
└──────────┬──────────────────────┘
           │
           ▼
┌─────────────────────────────────┐
│   SQLite (Local)                │
│  - offsets.db                   │
│  - Atomic transactions          │
└──────────┬──────────────────────┘
           │
           ▼ (Every 30s)
┌─────────────────────────────────┐
│   S3/Ceph Storage               │
│  - backups/{id}/state/offsets.db│
│  - Source of truth              │
└─────────────────────────────────┘
```

### 7.2 Recovery After Failure

```
1. Process dies (pod restart, VM crash, etc.)
2. New process starts with same config
3. Load SQLite from S3: `backups/{id}/state/offsets.db`
4. Read last_offset for each (topic, partition)
5. Resume fetching from last_offset + 1
6. Continue as if never interrupted

Result: No data loss or duplication
```

### 7.3 Checkpoint Semantics

```rust
pub async fn checkpoint(
    &self,
    backup_id: &str,
    offsets: &OffsetMap,
) -> anyhow::Result<()> {
    // 1. Write to local SQLite (fast, atomic)
    sqlx::query(
        "INSERT OR REPLACE INTO offsets (backup_id, topic, partition, last_offset) 
         VALUES (?, ?, ?, ?)"
    )
    .bind(backup_id)
    .bind(topic)
    .bind(partition)
    .bind(offset)
    .execute(&self.pool)
    .await?;
    
    // 2. Every Nth checkpoint, sync to S3 (durable)
    if checkpoint_count % 6 == 0 {  // Every 30s
        let db_bytes = tokio::fs::read("./offsets.db").await?;
        self.storage
            .put(&s3_key, db_bytes.into())
            .await?;
    }
    
    Ok(())
}
```

**Properties:**
- **At-least-once delivery**: May re-process small overlap, but never skip
- **Durable**: Survives process death
- **Low latency**: Local writes are instant, S3 sync is periodic

### 7.4 Circuit Breakers & Error Handling

```rust
pub struct CircuitBreaker {
    failure_count: AtomicUsize,
    failure_threshold: usize,
    recovery_timeout: Duration,
    last_failure: Mutex<Instant>,
}

impl CircuitBreaker {
    pub async fn execute<F>(&self, f: F) -> anyhow::Result<()>
    where
        F: Fn() -> BoxFuture<'static, anyhow::Result<()>>,
    {
        // Check if circuit is open
        if self.is_open() {
            return Err(anyhow::anyhow!("Circuit breaker open"));
        }
        
        // Execute with retry
        match f().await {
            Ok(_) => {
                self.reset();
                Ok(())
            }
            Err(e) => {
                self.record_failure();
                if self.failure_count.load(Ordering::SeqCst) >= self.failure_threshold {
                    tracing::warn!("Circuit breaker opening after {} failures", self.failure_threshold);
                }
                Err(e)
            }
        }
    }
}
```

---

## 8. Technical Stack

### 8.1 Core Dependencies

| Component | Crate | Version | Purpose |
|-----------|-------|---------|---------|
| **Kafka Protocol** | `kafka-protocol` | Latest | Raw protocol control |
| **Async Runtime** | `tokio` | 1.x | Non-blocking I/O |
| **Storage** | `object_store` | 0.11+ | S3/Ceph/filesystem abstraction |
| **Compression** | `zstd` | 0.13+ | Fast compression |
| **SQL** | `sqlx` | 0.7+ | SQLite offset storage |
| **Serialization** | `serde` + `bincode`/`json` | Latest | Record encoding |
| **CLI** | `clap` | 4.x | Command-line interface |
| **Logging** | `tracing` + `tracing-subscriber` | Latest | Structured logs |

### 8.2 Architecture Crates

```
kafka-backup/
├── kafka-backup-core/           # Main engine (no K8s deps)
│   ├── src/
│   │   ├── client/              # kafka-protocol-rs wrapper
│   │   ├── storage/             # object_store abstraction
│   │   ├── backup/              # Backup engine logic
│   │   │   ├── segment_writer.rs  # Mini-batching + compression
│   │   │   └── backup_engine.rs
│   │   ├── restore/             # Restore engine logic
│   │   ├── offset_store/        # SQLite + S3 sync
│   │   └── metrics.rs           # Performance tracking
│   └── Cargo.toml
│
├── kafka-backup-cli/            # Binary wrapper
│   ├── src/main.rs
│   └── Cargo.toml
│
└── Cargo.workspace.toml
```

---

## 9. Data Structures

### 9.1 Backup Manifest

```rust
#[derive(Serialize, Deserialize)]
pub struct BackupManifest {
    pub backup_id: String,
    pub created_at: i64,           // epoch millis
    pub source_cluster_id: Option<String>,
    pub source_bootstrap_servers: Vec<String>,
    pub topics: Vec<TopicBackup>,
    pub retention_policy: RetentionPolicy,
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
    pub record_count: i64,
}

#[derive(Serialize, Deserialize)]
pub struct SegmentMetadata {
    pub key: String,              // s3 key
    pub start_offset: i64,
    pub end_offset: i64,
    pub start_timestamp: i64,
    pub end_timestamp: i64,
    pub record_count: i64,
    pub compressed_bytes: u64,
    pub uncompressed_bytes: u64,
}
```

### 9.2 Offset State (SQLite Schema)

```sql
CREATE TABLE offsets (
    backup_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    last_offset INTEGER NOT NULL,
    checkpoint_ts INTEGER,
    PRIMARY KEY (backup_id, topic, partition)
);

CREATE TABLE backup_jobs (
    backup_id TEXT PRIMARY KEY,
    source_cluster_id TEXT,
    status TEXT,              -- running, paused, failed, completed
    created_at INTEGER,
    last_heartbeat INTEGER,
    last_checkpoint INTEGER
);
```

### 9.3 Wire Format (Segments)

```
Binary layout inside compressed segment:
┌────────────────────────────────────────┐
│ SegmentHeader                          │
│ - version: u8                          │
│ - created_at: i64                      │
│ - record_count: u32                    │
├────────────────────────────────────────┤
│ Record 1                               │
│ - key_len: u32                         │
│ - key: [u8; key_len]                   │
│ - value_len: u32                       │
│ - value: [u8; value_len]               │
│ - offset: i64                          │
│ - timestamp: i64                       │
│ - header_count: u16                    │
├────────────────────────────────────────┤
│ Record 2, 3, ... N                     │
└────────────────────────────────────────┘
```

---

## 10. Non-Functional Requirements

### 10.1 Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| **Throughput** | 100+ MB/s | Per partition, achievable on 10GbE |
| **Latency (p99)** | <100ms | Checkpoint latency |
| **Memory** | <500MB | Resident set for 4 partitions |
| **CPU** | <50% single core | Per partition with good batching |
| **Compression ratio** | 3-5x | Typical Kafka data with zstd-3 |

### 10.2 Reliability

- **At-least-once semantics**: May re-process small overlap after failure
- **Offset durability**: Offsets persisted to S3 every 30s
- **Data integrity**: All records checksummed during restore
- **Circuit breaker**: 5 failures → temporary backoff, auto-recovery

### 10.3 Security

- **TLS/SASL**: Full Kafka security support
- **Credential management**: Environment variables or secure vaults
- **S3 credentials**: AWS SDK standard credential chain
- **Audit logging**: All backup/restore operations logged

### 10.4 Observability

- **Metrics**: Prometheus-compatible output (throughput, latency, compression ratio)
- **Structured logs**: tracing with timestamps, levels, spans
- **Health checks**: /health endpoint (if HTTP server added)
- **Error reporting**: Detailed error messages with context

---

## 11. Milestones

### Milestone 1: Core Engine & CLI (Weeks 1-3)

**Deliverables:**
- `kafka-backup-core` library with basic backup/restore
- SQLite offset storage with S3 sync
- `kafka-backup-cli` with config parsing
- Local filesystem storage backend
- Basic metrics collection

**Success criteria:**
- Backup 1 topic to local disk
- Restore from local disk
- Survive process restart
- 50+ MB/s throughput on single partition

### Milestone 2: Performance Optimization (Weeks 4-5)

**Deliverables:**
- Mini-batching with configurable segment size
- Compression (zstd) integration
- Partition-level parallelism
- Async I/O optimization
- Performance profiling

**Success criteria:**
- 100+ MB/s throughput with 4 partitions
- <100ms p99 checkpoint latency
- 3-5x compression ratio
- <500MB memory footprint

### Milestone 3: S3-Compatible Storage (Weeks 6-7)

**Deliverables:**
- S3/Ceph backend via `object_store`
- Manifest generation
- Time-windowed restore
- Offset header injection

**Success criteria:**
- Backup to S3/Ceph
- Point-in-time restore with 1-minute precision
- Multi-cluster restore scenarios

### Milestone 4: Internal Topics & Resilience (Weeks 8-9)

**Deliverables:**
- `__consumer_offsets` backup support
- Streams/Connect internal topic handling
- Circuit breaker for transient failures
- Health monitoring
- Comprehensive error handling

**Success criteria:**
- Full backup including consumer offsets
- Graceful failure handling
- Clear error messages

### Milestone 5: Production Hardening (Weeks 10-11)

**Deliverables:**
- Load testing (1TB+ datasets)
- Distributed tracing
- Production deployment docs
- Docker + Kubernetes examples
- Security audit

**Success criteria:**
- Tested up to 1TB backups
- Documented deployment patterns
- Security best practices documented

---

## Appendix: Design Rationales

### Why Rust?

- **Performance**: 10-20x throughput vs Python/Java
- **Memory safety**: Catch bugs at compile time
- **Async**: True non-blocking I/O (no thread pool overhead)
- **Single binary**: No runtime dependencies

### Why kafka-protocol-rs?

- **Full protocol control**: Not limited by client abstraction layers
- **Flexible**: Can implement observer replicas in future
- **Low overhead**: Direct TCP control

### Why SQLite + S3 Sync?

- **No external database**: Single binary deployment
- **Durable**: S3 provides long-term persistence
- **Fast**: Local SQLite reads instant
- **Scalable**: Supports many backup jobs

### Why zstd Level 3?

- **Speed**: 42x faster than f64 floating-point operations
- **Compression**: 3-5x typical ratio for Kafka data
- **Standard**: Industry standard (used by Kafka, Linux, etc.)

---

## Glossary

| Term | Definition |
|------|-----------|
| **Segment** | Batch of records (up to 128MB) written as single object |
| **Checkpoint** | Persisting current offsets to durable storage |
| **Manifest** | JSON metadata file listing all segments and offsets |
| **Point-in-Time Restore** | Restore data from specific time window |
| **Zero-Copy** | Working with references instead of copying data |
| **Mini-Batching** | Accumulating records before write (10x I/O reduction) |
| **Circuit Breaker** | Temporary backoff after repeated failures |
