# Kafka Backup Solutions Comparison

This document provides a detailed comparison of OSO Kafka Backup against other Kafka backup and replication solutions in the ecosystem.

## Solutions Compared

| Solution | Type | Maintainer |
|----------|------|------------|
| **OSO Kafka Backup** | Open source backup tool | OSO DevOps |
| **itadventurer/kafka-backup** | Open source backup tool | Community (unmaintained) |
| **Kannika Armory** | Commercial backup platform | Kannika |
| **Confluent Replicator / Cluster Linking** | Commercial replication | Confluent |
| **MirrorMaker 2** | Open source replication | Apache Kafka |

---

## Detailed Feature Comparison

### Point-in-Time Restore (PITR)

| Solution | Support | Details |
|----------|---------|---------|
| **OSO Kafka Backup** | ✅ Yes | Millisecond-precision time window restore per topic/partition. Specify exact start and end timestamps to restore only the data you need. |
| **itadventurer/kafka-backup** | ❌ No | Restores full topic from backup directory only. No ability to filter by timestamp or restore partial data. |
| **Kannika Armory** | ✅ Yes | Point-in-time restore with filters, but requires proprietary SaaS/UI to operate. |
| **Confluent Replicator** | ❌ No | Continuous replication only. Failover limited to "latest" or "earliest" offsets. |
| **MirrorMaker 2** | ❌ No | DR via replication only. No explicit PITR to an arbitrary timestamp. |

### Cloud Storage Backup

| Solution | Support | Details |
|----------|---------|---------|
| **OSO Kafka Backup** | ✅ Yes | S3, Azure Blob Storage, and GCS as primary backup targets. Native cloud integration. |
| **itadventurer/kafka-backup** | ❌ No | Filesystem only (local volumes). No direct object storage support. Cloud backup requires external tooling to sync filesystem. |
| **Kannika Armory** | ⚠️ Partial | Pluggable storage backends but typically uses K8s Persistent Volumes or enterprise storage. Not primarily cloud-native. |
| **Confluent Replicator** | ❌ No | No direct Kafka-to-object-storage backup. Focused entirely on cluster-to-cluster replication. |
| **MirrorMaker 2** | ❌ No | Replicates topics between Kafka clusters only. Not designed for backup storage. |

### Consumer Offset Recovery

| Solution | Support | Details |
|----------|---------|---------|
| **OSO Kafka Backup** | ✅ Yes | Multi-strategy offset capture and restore including snapshots, bulk reset, header-based mapping, and timestamp-based recovery. Solves the offset discontinuity problem. |
| **itadventurer/kafka-backup** | ⚠️ Partial | Backs up and restores consumer group offsets, but relies on offset sidecar files. Less flexible than multi-strategy approaches. |
| **Kannika Armory** | ✅ Yes | Restores data and supports environment cloning with schema/ID mapping. Full offset support. |
| **Confluent Replicator** | ⚠️ Limited | Can reset or sync offsets for failover scenarios, but does not support full historical snapshots or complex recovery scenarios. |
| **MirrorMaker 2** | ⚠️ Limited | Offset sync exists but is known to be fragile. Can drift or miss updates, leading to data loss or reprocessing. |

### Air-Gapped DR (Cold Backup)

| Solution | Support | Details |
|----------|---------|---------|
| **OSO Kafka Backup** | ✅ Yes | Backups live in object storage, completely independent of any Kafka cluster. True air-gapped disaster recovery. |
| **itadventurer/kafka-backup** | ⚠️ Partial | Local filesystem backup. DR capability depends entirely on how that storage is managed and replicated externally. |
| **Kannika Armory** | ✅ Yes | Supports "cold backup" and air-gapping, but as a commercial product with associated licensing costs. |
| **Confluent Replicator** | ❌ No | Keeps a hot secondary cluster in sync. Both clusters must be "live" - not true cold backup. |
| **MirrorMaker 2** | ❌ No | Hot secondary cluster architecture. Replication failure modes can still result in message loss. |

### Platform Dependencies

| Solution | Dependency | Details |
|----------|------------|---------|
| **OSO Kafka Backup** | ✅ None | Single binary with no external dependencies. No Kafka Connect, no external frameworks required. Just configure and run. |
| **itadventurer/kafka-backup** | ❌ Kafka Connect | Built as a Kafka Connect connector. Requires deploying and managing a Connect cluster. |
| **Kannika Armory** | ❌ K8s Platform | Runs as its own platform with K8s controllers, UI, and APIs. Significant operational overhead. |
| **Confluent Replicator** | ❌ Confluent Platform | Requires full Confluent Platform or Confluent Cloud control plane. Vendor lock-in. |
| **MirrorMaker 2** | ❌ MM2 Framework | Requires MM2 framework plus Connect-style configuration. Complex setup and tuning. |

### Operational Simplicity

| Solution | Rating | Details |
|----------|--------|---------|
| **OSO Kafka Backup** | 🟢 High | Simple config file + CLI. Works with any Kafka deployment: self-hosted, Amazon MSK, Confluent Cloud, Aiven, etc. |
| **itadventurer/kafka-backup** | 🟡 Medium | Need to build/deploy connector JAR and manage Connect tasks. Java ecosystem knowledge required. |
| **Kannika Armory** | 🟡 Medium/Low | Powerful but requires learning the product, CRDs, GraphQL/REST APIs. Steep learning curve. |
| **Confluent Replicator** | 🟡 Medium | Well-integrated with Confluent but opinionated and platform-specific. Limited flexibility. |
| **MirrorMaker 2** | 🔴 Low | Complex configurations. Well-known for tricky DR and offset synchronization behavior. Difficult to debug. |

### Backup vs Replication Use Case

| Solution | Primary Use Case | Details |
|----------|------------------|---------|
| **OSO Kafka Backup** | ✅ Backup & Restore | Purpose-built for backup and restore with PITR, offset recovery, and rollback capabilities. |
| **itadventurer/kafka-backup** | ✅ Backup & Restore | Purpose-built for backup and restore but filesystem-centric. Limited cloud integration. |
| **Kannika Armory** | ✅ Backup & Restore | Purpose-built commercial backup/restore platform with enterprise features. |
| **Confluent Replicator** | ❌ Replication | Designed for replication and DR between clusters. Not suitable for long-term backup retention. |
| **MirrorMaker 2** | ❌ Replication | Designed for replication and DR between clusters. Not a backup tool. |

### Licensing & Availability

| Solution | License | Details |
|----------|---------|---------|
| **OSO Kafka Backup** | ✅ MIT | Fully open source. Focused on OSS-friendly infrastructure teams. No vendor lock-in. |
| **itadventurer/kafka-backup** | ⚠️ MIT | Open source but unmaintained with low activity. Risk of abandonment. |
| **Kannika Armory** | ❌ Commercial | Closed-source commercial product / SaaS. Requires paid license. |
| **Confluent Replicator** | ❌ Commercial | Commercial license tied to Confluent Platform. Significant cost. |
| **MirrorMaker 2** | ✅ Apache 2.0 | Open source but part of broader Kafka ecosystem. Not a standalone backup tool. |

---

## Summary

### Why Choose OSO Kafka Backup?

**OSO Kafka Backup is the only solution that combines:**

1. **Millisecond-precision PITR** - Restore to any point in time, not just "latest" or "earliest"
2. **Cloud-native cold backups** - S3, Azure, GCS as first-class citizens
3. **Automated consumer offset recovery** - Multiple strategies to solve the offset discontinuity problem
4. **Zero platform dependencies** - Single binary, no Kafka Connect, no K8s operators
5. **True open source** - MIT license, actively maintained, community-focused

### When to Consider Alternatives

| If you need... | Consider |
|----------------|----------|
| Real-time active-active replication | MirrorMaker 2 or Confluent Cluster Linking |
| Enterprise support and SLAs | Kannika Armory or Confluent |
| Already running Kafka Connect | itadventurer/kafka-backup (if filesystem backup is sufficient) |
| Confluent Platform integration | Confluent Replicator |

### Decision Matrix

```
Need PITR?
├── Yes → Need cloud storage?
│         ├── Yes → Need open source?
│         │         ├── Yes → OSO Kafka Backup ✓
│         │         └── No  → Kannika Armory
│         └── No  → itadventurer/kafka-backup (if maintained)
└── No  → Need replication?
          ├── Yes → Confluent Replicator or MirrorMaker 2
          └── No  → Simple filesystem backup may suffice
```

---

## Additional Resources

- [OSO Kafka Backup Documentation](../README.md)
- [Quick Start Guide](quickstart.md)
- [Configuration Reference](configuration.md)
- [Restore Guide](restore_guide.md)
- [Offset Recovery Deep Dive](Offset_Remapping_Deep_Dive.md)
