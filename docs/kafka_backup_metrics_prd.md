# Product Requirements Document: Kafka Backup Metrics & Monitoring

**Version:** 1.0  
**Date:** January 2026  
**Status:** Draft  
**Owner:** Engineering Team

---

## Executive Summary

kafka-backup operates as a sophisticated Kafka consumer that reads messages from source topics and persists them to cloud storage backends (S3, Azure Blob, GCS). Enterprise teams require deep visibility into backup and restore operations to:

- **Detect failures early** – Identify lag spikes, connectivity issues, and data loss before they impact production
- **Measure operational efficiency** – Track throughput, compression ratios, and storage utilization
- **Optimize resource allocation** – Right-size clusters based on actual consumption patterns
- **Meet compliance requirements** – Audit backup completeness and recovery time objective (RTO) compliance

This PRD defines comprehensive metrics exposure for both CLI and long-running deployments, enabling teams to integrate kafka-backup with standard monitoring stacks (Prometheus, Grafana, Datadog, etc.).

---

## Problem Statement

### Current Gaps

1. **Consumer Lag Blindness** – Teams cannot measure how far behind the backup consumer is from the high watermark
2. **No Operational Insights** – CLI provides summary output only; no time-series data for trending
3. **Storage I/O Opaque** – Write latencies, error rates, and throughput to storage backends are invisible
4. **Error Tracking Limited** – Failure modes (network, auth, codec) not surfaced as metrics
5. **Restore Observability Minimal** – No way to track restore progress, ETA, or latency per partition

### Competitive Analysis

**Kannika (Primary Competitor)**
- Provides Kubernetes-native backup status (Paused/Streaming/Error)
- Exposes deployment health conditions
- **Does NOT expose** detailed lag, throughput, or storage metrics via standard monitoring interfaces
- **Advantage for kafka-backup:** Can differentiate with rich metrics ecosystem

**Kafka Native Metrics**
- Consumer lag via JMX (records-lag, records-lag-max, records-consumed-rate)
- These exist at broker level; kafka-backup must expose at backup job level

**Confluent Cloud & AWS MSK**
- Both expose consumer lag as first-class metric
- Both support Prometheus-compatible scraping
- kafka-backup should match or exceed this capability

---

## Objectives & Key Results (OKRs)

### OKR 1: Enable Production Monitoring
- **Objective:** Enterprise teams can monitor kafka-backup with standard observability stacks
- **KR1:** Support Prometheus text format + OpenMetrics exposition
- **KR2:** ≥15 key metrics covering lag, throughput, latency, errors
- **KR3:** Zero additional operational overhead vs. current deployment

### OKR 2: Operational Troubleshooting
- **Objective:** Teams can diagnose failures in <5 minutes from metric signals
- **KR1:** Per-partition lag granularity for hot-spot detection
- **KR2:** Storage backend latency histograms (p50, p95, p99)
- **KR3:** Error rate + error type classification (transient vs. permanent)

### OKR 3: CLI Usability
- **Objective:** CLI reports remain accessible while metrics ship externally
- **KR1:** CLI output unchanged for backward compatibility
- **KR2:** Optional `--expose-metrics` flag for local collection (if needed for CI/CD)
- **KR3:** Metrics file export option for air-gapped environments

---

## Scope

### In Scope

1. **Metrics Exposure** (long-running deployments)
   - HTTP `/metrics` endpoint (default port 8080)
   - Prometheus text format (OpenMetrics compliant)
   - Per-topic, per-partition, per-backup granularity where relevant

2. **Metrics Categories**
   - Consumer group lag (records, bytes, time-based)
   - Throughput (records/sec, bytes/sec)
   - Operation timing (backup, restore, offset reset)
   - Storage I/O (latency, bytes read/written, errors)
   - Compression metrics (ratio, algorithm)
   - Error tracking (by type, by partition)
   - Operator reconciliation (if Kubernetes operator used)

3. **Rust Implementation**
   - Use `prometheus-client` crate (OpenMetrics compliant)
   - Minimal runtime overhead (<1% CPU, <10MB memory for metrics registry)
   - Thread-safe, async-compatible

4. **CLI Integration**
   - Optional metrics collection during operation
   - Summary statistics in final report (no new requirements)
   - Optional export to JSON/Prometheus format for CI/CD pipelines

5. **Documentation**
   - Metrics reference (all metrics, labels, types, buckets)
   - Grafana dashboard examples (JSON exports)
   - Alert rule examples (Prometheus recording rules, alert rules)
   - Integration guides (Datadog, New Relic, Splunk)

### Out of Scope

1. **Custom collectors** – Custom business metrics beyond backup/restore operations
2. **Distributed tracing** – OpenTelemetry traces (future phase)
3. **Metrics push** – Push-based collectors (pull via Prometheus scrape only in MVP)
4. **Real-time dashboards** – Built-in UI (external integration with Grafana/Datadog)
5. **Historical querying** – No embedded time-series database; rely on Prometheus retention

---

## Detailed Metric Specifications

### 1. Consumer Lag Metrics

**Purpose:** Track how far behind the backup consumer is from the high watermark, per partition.

| Metric Name | Type | Unit | Labels | Description | Alerting Threshold |
|---|---|---|---|---|---|
| `kafka_backup_lag_records` | Gauge | records | `topic`, `partition`, `backup_id` | Number of records the backup consumer is behind | >100K sustained >5min |
| `kafka_backup_lag_bytes` | Gauge | bytes | `topic`, `partition`, `backup_id` | Approximate bytes behind (estimated from avg record size) | >1GB sustained >5min |
| `kafka_backup_lag_seconds` | Gauge | seconds | `topic`, `partition`, `backup_id` | Time lag based on message timestamps (requires message parsing) | >3600s for critical topics |
| `kafka_backup_lag_records_max` | Gauge | records | `backup_id` | Maximum lag across all partitions | >500K sustained >10min |

**Implementation Notes:**
- Update every poll cycle (default: 500ms)
- Calculate as: `high_watermark_offset - consumer_committed_offset`
- Bytes estimated via: `lag_records * avg_record_size` (rolling average over 1 hour)
- Time lag requires message timestamp parsing; optional if latency budget exceeded

**Queries (PromQL Examples):**
```promql
# Alert on high lag
kafka_backup_lag_records{topic="production-events"} > 100000

# Lag trend over 1 hour
rate(kafka_backup_lag_records[1h])

# Slowest partition
topk(5, kafka_backup_lag_records)
```

---

### 2. Throughput Metrics

**Purpose:** Measure how fast kafka-backup is consuming and processing data.

| Metric Name | Type | Unit | Labels | Description | Target |
|---|---|---|---|---|---|
| `kafka_backup_throughput_records_per_sec` | Gauge | records/sec | `backup_id`, `topic` | Records consumed per second | Stable, >1K for typical workloads |
| `kafka_backup_throughput_bytes_per_sec` | Gauge | bytes/sec | `backup_id`, `topic` | Bytes consumed per second | Topic-dependent |
| `kafka_backup_fetch_rate` | Gauge | fetches/sec | `backup_id` | Number of fetch requests to broker per second | 1–10 typical |
| `kafka_backup_records_total` | Counter | records | `backup_id` | Cumulative records backed up (for detecting resets) | Always increasing |
| `kafka_backup_bytes_total` | Counter | bytes | `backup_id` | Cumulative bytes backed up | Always increasing |

**Implementation Notes:**
- Compute records/sec from message count in fetch batches
- Bytes/sec includes payload only (no protocol overhead)
- Fetch rate = number of `KafkaConsumer::poll()` calls per second
- Counters never decrease; reset only on process restart

**Queries (PromQL Examples):**
```promql
# Current throughput
rate(kafka_backup_records_total[1m])

# Average over last hour
avg_over_time(kafka_backup_throughput_records_per_sec[1h])

# Detect slow consumers
kafka_backup_throughput_records_per_sec < 500
```

---

### 3. Operation Duration Metrics

**Purpose:** Measure time taken for backup, restore, and offset reset operations.

| Metric Name | Type | Labels | Buckets | Description |
|---|---|---|---|---|
| `kafka_backup_duration_seconds` | Histogram | `backup_id`, `status` (success/failure) | 1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600 | Backup job duration (seconds) |
| `kafka_restore_duration_seconds` | Histogram | `backup_id`, `status` | 1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600 | Restore job duration (seconds) |
| `kafka_offset_reset_duration_seconds` | Histogram | `consumer_group`, `status` | 0.5, 1, 2.5, 5, 10, 30, 60, 120 | Offset reset duration per consumer group |

**Implementation Notes:**
- Start timer on operation initiation
- Stop timer on completion (success or failure)
- Status label = "success" or "failure"
- Buckets chosen for typical Kafka operations (seconds to hours)

**Queries (PromQL Examples):**
```promql
# P95 backup duration
histogram_quantile(0.95, rate(kafka_backup_duration_seconds_bucket[1h]))

# Success rate
rate(kafka_backup_duration_seconds_bucket{status="success"}[1h]) / rate(kafka_backup_duration_seconds_bucket[1h])

# Alert on slow backups
histogram_quantile(0.95, rate(kafka_backup_duration_seconds_bucket[5m])) > 300
```

---

### 4. Storage I/O Metrics

**Purpose:** Track latency, throughput, and errors for cloud storage operations.

| Metric Name | Type | Labels | Buckets | Description |
|---|---|---|---|---|
| `kafka_backup_storage_write_latency_seconds` | Histogram | `backend` (s3, azure, gcs), `operation` (segment, manifest, checkpoint) | 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 | Write latency by backend and operation |
| `kafka_backup_storage_read_latency_seconds` | Histogram | `backend`, `operation` | 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10 | Read latency for restore operations |
| `kafka_backup_storage_write_bytes_total` | Counter | `backend`, `backup_id` | — | Cumulative bytes written to storage |
| `kafka_backup_storage_read_bytes_total` | Counter | `backend`, `backup_id` | — | Cumulative bytes read from storage |
| `kafka_backup_storage_errors_total` | Counter | `backend`, `error_type` (timeout, auth, not_found, quota) | — | Storage operation failures by type |

**Implementation Notes:**
- Measure wall-clock time from request submission to response received
- Distinguish segment writes (main data) from metadata writes
- Error types: timeout, auth/permission, not_found, quota_exceeded, unknown
- Per-backend metrics allow easy cost/performance analysis

**Queries (PromQL Examples):**
```promql
# P99 write latency
histogram_quantile(0.99, rate(kafka_backup_storage_write_latency_seconds_bucket{operation="segment"}[5m]))

# Bytes/sec to S3
rate(kafka_backup_storage_write_bytes_total{backend="s3"}[1m])

# Error rate
rate(kafka_backup_storage_errors_total[1m])
```

---

### 5. Compression Metrics

**Purpose:** Understand data reduction via compression.

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `kafka_backup_compression_ratio` | Gauge | `algorithm` (zstd, lz4, snappy, none), `backup_id` | Ratio of uncompressed to compressed size (e.g., 4.5 = 4.5x reduction) |
| `kafka_backup_compressed_bytes_total` | Counter | `algorithm`, `backup_id` | Cumulative bytes after compression |
| `kafka_backup_uncompressed_bytes_total` | Counter | `algorithm`, `backup_id` | Cumulative bytes before compression |

**Implementation Notes:**
- Ratio = uncompressed_size / compressed_size
- 1.0 = no compression; >1.0 = data reduction
- Update counters after each segment flush

---

### 6. Error & Health Metrics

**Purpose:** Track failures, retries, and operational health.

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `kafka_backup_errors_total` | Counter | `backup_id`, `error_type` (broker_connection, consumer_timeout, deserialization, storage_io) | Cumulative errors by category |
| `kafka_backup_retries_total` | Counter | `backup_id`, `operation` (fetch, commit, write) | Cumulative retries |
| `kafka_backup_last_successful_commit` | Gauge | `backup_id`, `topic`, `partition` | Unix timestamp of last successful offset commit |
| `kafka_backup_consumer_rebalance_total` | Counter | `backup_id` | Number of consumer group rebalances |

**Implementation Notes:**
- Error types: broker_connection, consumer_timeout, deserialization, storage_io, codec, offset_invalid
- Track time since last commit to detect stalled backups
- High rebalance counts = cluster instability or config issues

---

### 7. Restore Operation Metrics

**Purpose:** Monitor restore-specific operations.

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `kafka_restore_progress_percent` | Gauge | `restore_id`, `backup_id` | Restore progress as percentage (0–100) |
| `kafka_restore_eta_seconds` | Gauge | `restore_id` | Estimated seconds until restore completes |
| `kafka_restore_throughput_records_per_sec` | Gauge | `restore_id` | Records written to target cluster per second |
| `kafka_restore_records_total` | Counter | `restore_id` | Cumulative records restored |

**Implementation Notes:**
- ETA calculated from: `remaining_records / current_throughput`
- Progress: `records_restored / total_records_in_backup * 100`
- Throughput = records written to broker per second

---

### 8. Operator-Specific Metrics (Kubernetes)

**Purpose:** For kubernetes-deployed operators.

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `kafka_backup_operator_reconcile_duration_seconds` | Histogram | `kind` (Backup, Restore, OffsetReset) | Reconciliation loop duration |
| `kafka_backup_operator_reconciliation_errors_total` | Counter | `kind` | Failed reconciliations |
| `kafka_backup_operator_active_backups` | Gauge | — | Number of active backup processes |
| `kafka_backup_operator_last_reconcile_timestamp` | Gauge | `kind` | Unix timestamp of last reconciliation |

---

## Metrics Exposure Method

### HTTP Endpoint

```
GET /metrics
```

**Response Format:** Prometheus text format (OpenMetrics 1.0 compliant)

**Example Response:**
```
# HELP kafka_backup_lag_records Number of records behind the high watermark
# TYPE kafka_backup_lag_records gauge
kafka_backup_lag_records{topic="orders",partition="0",backup_id="main"} 42500
kafka_backup_lag_records{topic="orders",partition="1",backup_id="main"} 38200
# HELP kafka_backup_throughput_records_per_sec Records consumed per second
# TYPE kafka_backup_throughput_records_per_sec gauge
kafka_backup_throughput_records_per_sec{backup_id="main",topic="orders"} 12500
# HELP kafka_backup_duration_seconds_bucket Backup operation duration
# TYPE kafka_backup_duration_seconds histogram
kafka_backup_duration_seconds_bucket{backup_id="main",status="success",le="1"} 0
kafka_backup_duration_seconds_bucket{backup_id="main",status="success",le="5"} 2
kafka_backup_duration_seconds_bucket{backup_id="main",status="success",le="60"} 150
kafka_backup_duration_seconds_count{backup_id="main",status="success"} 155
kafka_backup_duration_seconds_sum{backup_id="main",status="success"} 8942.5
# EOF
```

**Configuration:**
```yaml
# config.yaml
metrics:
  enabled: true
  port: 8080
  path: /metrics
  update_interval_ms: 500  # How often metrics are recalculated
```

**Default Behavior:**
- Metrics endpoint enabled by default
- Localhost-only binding for security (use reverse proxy for remote access)
- 500ms update interval (configurable)
- Zero performance impact when not scraped

### CLI Integration

**CLI remains unchanged** – No breaking changes to command interface.

**Optional Future Enhancement (out of MVP scope):**
```bash
# Export metrics to JSON after operation
kafka-backup backup --metrics-output=metrics.json topics.yaml

# Expose metrics on local HTTP endpoint during operation
kafka-backup backup --expose-metrics=:8080 topics.yaml
```

---

## Implementation Plan

### Phase 1: Core Metrics (MVP – 6 weeks)

1. **Lag Metrics** (kafka_backup_lag_records, kafka_backup_lag_records_max)
2. **Throughput Metrics** (kafka_backup_throughput_records_per_sec, _bytes_per_sec)
3. **Storage I/O Latency** (write/read latencies by backend)
4. **Errors & Retries** (error_total, retries_total)
5. **HTTP Metrics Endpoint** (port 8080, /metrics)

**Deliverables:**
- Metrics registry initialized in main CLI/server loop
- Labels properly attached (topic, partition, backup_id, backend)
- Prometheus text format output
- Documentation & examples
- Metrics reference page (kafkabackup.com/reference/metrics)

### Phase 2: Advanced Metrics (4 weeks post-MVP)

1. **Restore Operation Tracking** (progress, ETA, throughput)
2. **Offset Reset Metrics** (duration, error rates)
3. **Compression Analytics** (ratio, bytes_total)
4. **Consumer Lag by Time** (timestamp-based lag if feasible)

### Phase 3: Ecosystem Integration (4 weeks post-Phase 2)

1. **Grafana Dashboard** (JSON export, importable template)
2. **Alert Rules** (Prometheus recording rules & alert rules)
3. **Integration Guides** (Datadog, New Relic, Splunk connectors)
4. **Operator Metrics** (if Kubernetes operator exists)

---

## Metrics Library Choice: prometheus-client

### Why prometheus-client?

1. **Official** – Maintained under Prometheus GitHub org
2. **OpenMetrics Compliant** – Enforces spec, no custom extensions
3. **Modern Rust** – Async-friendly, zero-copy serialization
4. **Exemplar Support** – Optional trace correlation for histograms
5. **Active Maintenance** – Regular updates, security patches

### Alternative Considered: metrics-rs

- More abstraction, allows swapping backends
- Less enforcement of standards
- **Decision:** prometheus-client for strict compliance; revisit if multi-backend needed

### Cargo Dependency

```toml
[dependencies]
prometheus-client = "0.21"  # Latest as of Jan 2026
tokio = { version = "1.0", features = ["full"] }
hyper = "1.0"  # For HTTP endpoint
```

---

## Rust Implementation Patterns

### 1. Registry Setup

```rust
use prometheus_client::registry::Registry;
use prometheus_client::metrics::{Counter, Gauge, Histogram};

pub struct MetricsRegistry {
    pub registry: Registry,
    pub lag_records: Gauge,
    pub throughput_rps: Gauge,
    pub storage_write_latency: Histogram,
    pub backup_errors_total: Counter,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        let registry = Registry::new();
        
        let lag_records = Gauge::new(
            "kafka_backup_lag_records",
            "Records behind high watermark",
        );
        registry.register("kafka_backup_lag_records", lag_records.clone());
        
        // Initialize other metrics...
        
        Self { registry, lag_records, /* ... */ }
    }
}
```

### 2. Recording a Gauge with Labels

```rust
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::encoding::EncodeMetric;

// In consumer loop
let lag = hwm - committed_offset;
self.metrics.lag_records
    .get_or_create(&Labels {
        topic: topic.clone(),
        partition,
        backup_id: self.backup_id.clone(),
    })
    .set(lag as f64);
```

### 3. Histogram Observation

```rust
use std::time::Instant;

let start = Instant::now();

// Perform storage write...
let latency = start.elapsed().as_secs_f64();

self.metrics.storage_write_latency
    .get_or_create(&Labels {
        backend: "s3".to_string(),
        operation: "segment".to_string(),
    })
    .observe(latency);
```

### 4. HTTP Endpoint (using hyper)

```rust
use hyper::{Body, Request, Response, Server, StatusCode};
use prometheus_client::encoding::text::encode;

async fn metrics_handler(registry: &Registry) -> Response<Body> {
    let mut buffer = vec![];
    encode(&mut buffer, registry).unwrap();
    
    Response::builder()
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(Body::from(buffer))
        .unwrap()
}

// In main
let metrics_registry = Arc::new(MetricsRegistry::new());
let service = make_service_fn(|_| {
    let registry = Arc::clone(&metrics_registry);
    async move {
        Ok::<_, Infallible>(service_fn(move |_req| {
            metrics_handler(&registry)
        }))
    }
});

let addr = ([127, 0, 0, 1], 8080).into();
Server::bind(&addr).serve(service).await.unwrap();
```

### 5. Thread-Safe Shared Metrics

```rust
use std::sync::Arc;

pub struct BackupState {
    metrics: Arc<MetricsRegistry>,
    // ... other fields
}

impl BackupState {
    pub fn new(metrics: Arc<MetricsRegistry>) -> Self {
        Self { metrics, /* ... */ }
    }
    
    pub fn record_lag(&self, topic: &str, partition: u32, lag: u64) {
        self.metrics.lag_records
            .get_or_create(&Labels {
                topic: topic.to_string(),
                partition,
                backup_id: "main".to_string(),
            })
            .set(lag as f64);
    }
}
```

---

## Monitoring & Alerting Examples

### Prometheus Recording Rules

```yaml
groups:
  - name: kafka_backup_recording_rules
    interval: 30s
    rules:
      - record: kafka_backup:lag_records:max
        expr: max(kafka_backup_lag_records) by (backup_id)
      
      - record: kafka_backup:throughput:5m_avg
        expr: avg_over_time(kafka_backup_throughput_records_per_sec[5m])
      
      - record: kafka_backup:error_rate:5m
        expr: rate(kafka_backup_errors_total[5m])
      
      - record: kafka_backup:storage:p99_write_latency
        expr: histogram_quantile(0.99, rate(kafka_backup_storage_write_latency_seconds_bucket[5m]))
```

### Prometheus Alert Rules

```yaml
groups:
  - name: kafka_backup_alerts
    rules:
      - alert: KafkaBackupHighLag
        expr: kafka_backup_lag_records > 100000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Kafka backup {{ $labels.backup_id }} is lagging"
          description: "{{ $labels.topic }}-{{ $labels.partition }} is {{ $value }} records behind"
      
      - alert: KafkaBackupError
        expr: rate(kafka_backup_errors_total[5m]) > 0.1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka backup errors detected"
          description: "Error rate: {{ $value }} errors/sec"
      
      - alert: StorageWriteLatencyHigh
        expr: histogram_quantile(0.95, rate(kafka_backup_storage_write_latency_seconds_bucket[5m])) > 5
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Storage write latency high ({{ $labels.backend }})"
          description: "P95 latency: {{ $value }}s"
      
      - alert: KafkaBackupNotProgressing
        expr: rate(kafka_backup_records_total[10m]) < 100
        for: 15m
        labels:
          severity: critical
        annotations:
          summary: "Backup {{ $labels.backup_id }} not progressing"
          description: "Less than 100 records/min being backed up"
```

### Grafana Dashboard (JSON snippet)

```json
{
  "dashboard": {
    "title": "Kafka Backup Monitoring",
    "panels": [
      {
        "title": "Consumer Lag (Records)",
        "targets": [
          {
            "expr": "kafka_backup_lag_records",
            "legendFormat": "{{ topic }}-{{ partition }}"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Throughput (Records/sec)",
        "targets": [
          {
            "expr": "kafka_backup_throughput_records_per_sec",
            "legendFormat": "{{ backup_id }}"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Storage Write Latency (p99)",
        "targets": [
          {
            "expr": "histogram_quantile(0.99, rate(kafka_backup_storage_write_latency_seconds_bucket[5m]))",
            "legendFormat": "{{ backend }}"
          }
        ],
        "type": "graph"
      }
    ]
  }
}
```

---

## Testing Strategy

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_lag_metric_updates() {
        let metrics = MetricsRegistry::new();
        metrics.lag_records
            .get_or_create(&Labels { topic: "test".into(), partition: 0, backup_id: "1".into() })
            .set(1000.0);
        
        let mut buffer = vec![];
        encode(&mut buffer, &metrics.registry).unwrap();
        let output = String::from_utf8(buffer).unwrap();
        
        assert!(output.contains("kafka_backup_lag_records{"));
        assert!(output.contains("1000"));
    }
}
```

### Integration Tests

1. **Live Kafka Cluster** – Back up N records, verify metrics match
2. **Storage Latency** – Inject S3 delays, verify histogram buckets
3. **Error Scenarios** – Simulate auth failures, network timeouts, verify counters
4. **Long-Running** – 24h continuous backup, verify metrics accuracy

### Performance Testing

- Metrics overhead: <1% CPU, <10MB memory for registry
- Scrape endpoint latency: <100ms for 1000+ time series
- Per-label cardinality: <100K unique label combinations

---

## Documentation Deliverables

1. **Metrics Reference** (kafkabackup.com/reference/metrics)
   - All metrics, types, labels, buckets
   - Example queries
   - Alert rules

2. **Integration Guides**
   - Prometheus + Grafana setup
   - Datadog agent config
   - New Relic custom metrics
   - Splunk HEC integration

3. **Architecture Document**
   - Metrics registry design
   - Label cardinality limits
   - Performance characteristics

4. **Example Dashboards**
   - Grafana dashboard JSON (importable)
   - CloudWatch dashboard (if AWS-focused)

---

## Success Criteria

### MVP (Phase 1)

- [ ] All core metrics emitted and tested
- [ ] HTTP /metrics endpoint working
- [ ] OpenMetrics format compliance verified
- [ ] <5 minute integration time for Prometheus scrape
- [ ] Documentation complete
- [ ] Backwards compatible (no CLI changes)

### Phase 2 & 3

- [ ] Restore & offset reset metrics validated
- [ ] Grafana dashboard importable and functional
- [ ] Alert rules tested in production
- [ ] <10% feature request volume for new metrics

---

## Open Questions / Decisions

1. **Metrics cardinality limit?** Propose: max 10K unique label combinations (configurable)
2. **Purge metrics on restart?** Yes – Prometheus handles counter resets
3. **Multi-backup per instance?** Yes – metrics scoped by backup_id label
4. **Sensitive data in labels?** No – topics/partitions only, no payload content
5. **Support metrics push?** Future phase – MVP is pull-only (Prometheus scrape)

---

## Timeline

| Phase | Duration | Deliverables |
|-------|----------|--------------|
| **1 (MVP)** | 6 weeks | Core metrics, HTTP endpoint, documentation |
| **2 (Advanced)** | 4 weeks | Restore/offset reset metrics, compression |
| **3 (Integration)** | 4 weeks | Grafana dashboard, alert rules, guides |
| **Total** | 14 weeks | Production-ready monitoring ecosystem |

---

## Appendix A: Related Links

- [Prometheus Metric Types](https://prometheus.io/docs/concepts/metric_types/)
- [OpenMetrics Specification](https://openmetrics.io/)
- [prometheus-client Rust Crate](https://docs.rs/prometheus-client/)
- [Kafka Consumer Lag Best Practices](https://last9.io/blog/fixing-kafka-consumer-lag/)
- [OSO Kafka Backup Existing Metrics](https://kafkabackup.com/reference/metrics/)

---

## Appendix B: Glossary

- **Consumer Lag:** Difference between high watermark and committed offset
- **High Watermark (HWM):** Latest message offset in a partition
- **Throughput:** Records or bytes processed per unit time
- **Histogram:** Distribution of values; supports quantile calculations
- **OpenMetrics:** Standard metrics format; superset of Prometheus format
- **Reconciliation:** Kubernetes operator term for syncing desired vs. actual state
- **RTO:** Recovery Time Objective; max acceptable downtime
- **Exemplars:** Optional trace IDs attached to metrics (advanced)

---

**Document Control**

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-01-17 | Engineering | Initial PRD |

