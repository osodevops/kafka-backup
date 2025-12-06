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
| **Lenses K2K** | Commercial replication | Lenses.io |

---

## Detailed Feature Comparison

### Point-in-Time Restore (PITR)

| Solution | Support | Details                                                                                                                                                                                                            |
|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **OSO Kafka Backup** | ✅ Yes | Millisecond-precision time window restore per topic/partition. Specify exact start and end timestamps to restore only the data you need.                                                                           |
| **itadventurer/kafka-backup** | ❌ No | Restores full topic from backup directory only. No ability to filter by timestamp or restore partial data.                                                                                                         |
| **Kannika Armory** | ✅ Yes | Millisecond-precision time window restore per topic/partition. Specify exact start and end timestamps to restore only the data you need. Configurable with UI, API and K8s CRD's. |
| **Confluent Replicator** | ❌ No | Continuous replication only. Failover limited to "latest" or "earliest" offsets.                                                                                                                                   |
| **MirrorMaker 2** | ❌ No | DR via replication only. No explicit PITR to an arbitrary timestamp.                                                                                                                                               |
| **Lenses K2K** | ❌ No | Designed for continuous cluster-to-cluster replication. No explicit PITR to an arbitrary timestamp.                                                                                                                |

### Cloud Storage Backup

| Solution | Support | Details                                                                                                                       |
|----------|--------|-------------------------------------------------------------------------------------------------------------------------------|
| **OSO Kafka Backup** | ✅ Yes | S3, Azure Blob Storage, and GCS as primary backup targets. Native cloud integration.                                          |
| **itadventurer/kafka-backup** | ❌ No | Filesystem only (local volumes). No direct object storage support. Cloud backup requires external tooling to sync filesystem. |
| **Kannika Armory** | ✅ Yes | Pluggable storage backends. Native support for S3, Azure Blob Storage, GCS and Kubernetes CSI driver backed disks.            |
| **Confluent Replicator** | ❌ No | No direct Kafka-to-object-storage backup. Focused entirely on cluster-to-cluster replication.                                 |
| **MirrorMaker 2** | ❌ No | Replicates topics between Kafka clusters only. Not designed for backup storage.                                               |
| **Lenses K2K** | ❌ No | Targets another Kafka cluster. Backup to S3 is via separate Stream Reactor connectors, not K2K itself.                        |

### Consumer Offset Recovery

| Solution | Support | Details |
|----------|---------|---------|
| **OSO Kafka Backup** | ✅ Yes | Multi-strategy offset capture and restore including snapshots, bulk reset, header-based mapping, and timestamp-based recovery. Solves the offset discontinuity problem. |
| **itadventurer/kafka-backup** | ⚠️ Partial | Backs up and restores consumer group offsets, but relies on offset sidecar files. Less flexible than multi-strategy approaches. |
| **Kannika Armory** | ✅ Yes | Restores data and supports environment cloning with schema/ID mapping. Full offset support. |
| **Confluent Replicator** | ⚠️ Limited | Can reset or sync offsets for failover scenarios, but does not support full historical snapshots or complex recovery scenarios. |
| **MirrorMaker 2** | ⚠️ Limited | Offset sync exists but is known to be fragile. Can drift or miss updates, leading to data loss or reprocessing. |
| **Lenses K2K** | ⚠️ Limited | Can replicate `__consumer_offsets` topic as-is. No offset remapping when source/target offsets differ, so not a full "restore with translation" capability. |

### Air-Gapped DR (Cold Backup)

| Solution | Support | Details |
|----------|---------|---------|
| **OSO Kafka Backup** | ✅ Yes | Backups live in object storage, completely independent of any Kafka cluster. True air-gapped disaster recovery. |
| **itadventurer/kafka-backup** | ⚠️ Partial | Local filesystem backup. DR capability depends entirely on how that storage is managed and replicated externally. |
| **Kannika Armory** | ✅ Yes | Supports "cold backup" and air-gapping, but as a commercial product with associated licensing costs. |
| **Confluent Replicator** | ❌ No | Keeps a hot secondary cluster in sync. Both clusters must be "live" - not true cold backup. |
| **MirrorMaker 2** | ❌ No | Hot secondary cluster architecture. Replication failure modes can still result in message loss. |
| **Lenses K2K** | ❌ No | Focuses on active-active / active-passive replication between clusters. DR is via continuously running target cluster, not offline cold storage. |

### Platform Dependencies

| Solution | Dependency | Details |
|----------|------------|---------|
| **OSO Kafka Backup** | ✅ None | Single binary with no external dependencies. No Kafka Connect, no external frameworks required. Just configure and run. |
| **itadventurer/kafka-backup** | ❌ Kafka Connect | Built as a Kafka Connect connector. Requires deploying and managing a Connect cluster. |
| **Kannika Armory** | ❌ K8s Platform | Runs as its own platform with K8s controllers, UI, and APIs. Significant operational overhead. |
| **Confluent Replicator** | ❌ Confluent Platform | Requires full Confluent Platform or Confluent Cloud control plane. Vendor lock-in. |
| **MirrorMaker 2** | ❌ MM2 Framework | Requires MM2 framework plus Connect-style configuration. Complex setup and tuning. |
| **Lenses K2K** | ⚠️ Lenses Platform | Runs as a Kubernetes-native replication service with its own control plane. No Connect required, but you operate the Lenses platform or K2K app and YAML pipelines. |

### Operational Simplicity

| Solution | Rating | Details |
|----------|--------|---------|
| **OSO Kafka Backup** | 🟢 High | Simple config file + CLI. Works with any Kafka deployment: self-hosted, Amazon MSK, Confluent Cloud, Aiven, etc. |
| **itadventurer/kafka-backup** | 🟡 Medium | Need to build/deploy connector JAR and manage Connect tasks. Java ecosystem knowledge required. |
| **Kannika Armory** | 🟡 Medium/Low | Powerful but requires learning the product, CRDs, GraphQL/REST APIs. Steep learning curve. |
| **Confluent Replicator** | 🟡 Medium | Well-integrated with Confluent but opinionated and platform-specific. Limited flexibility. |
| **MirrorMaker 2** | 🔴 Low | Complex configurations. Well-known for tricky DR and offset synchronization behavior. Difficult to debug. |
| **Lenses K2K** | 🟡 Medium | Strong tooling and UI, but requires learning K2K concepts (pipelines, YAML, scaling, schema replication) and running the Lenses-compatible stack. |

### Backup vs Replication Use Case

| Solution | Primary Use Case | Details |
|----------|------------------|---------|
| **OSO Kafka Backup** | ✅ Backup & Restore | Purpose-built for backup and restore with PITR, offset recovery, and rollback capabilities. |
| **itadventurer/kafka-backup** | ✅ Backup & Restore | Purpose-built for backup and restore but filesystem-centric. Limited cloud integration. |
| **Kannika Armory** | ✅ Backup & Restore | Purpose-built commercial backup/restore platform with enterprise features. |
| **Confluent Replicator** | ❌ Replication | Designed for replication and DR between clusters. Not suitable for long-term backup retention. |
| **MirrorMaker 2** | ❌ Replication | Designed for replication and DR between clusters. Not a backup tool. |
| **Lenses K2K** | ❌ Replication | Designed for replication, DR, and data sharing between Kafka clusters (with filtering/masking). Not a long-term cold backup system. |

### Licensing & Availability

| Solution | License | Details |
|----------|---------|---------|
| **OSO Kafka Backup** | ✅ MIT | Fully open source. Focused on OSS-friendly infrastructure teams. No vendor lock-in. |
| **itadventurer/kafka-backup** | ⚠️ MIT | Open source but unmaintained with low activity. Risk of abandonment. |
| **Kannika Armory** | ❌ Commercial | Closed-source commercial product / SaaS. Requires paid license. |
| **Confluent Replicator** | ❌ Commercial | Commercial license tied to Confluent Platform. Significant cost. |
| **MirrorMaker 2** | ✅ Apache 2.0 | Open source but part of broader Kafka ecosystem. Not a standalone backup tool. |
| **Lenses K2K** | ❌ Commercial | Commercial (enterprise-oriented), part of Lenses platform. Free tier exists but with restricted replication features. |

---

## Summary

### Why Choose OSO Kafka Backup?

**OSO Kafka Backup is the only option that combines millisecond-precision PITR, cloud-native cold backups, and automated consumer offset recovery in a single, OSS-friendly binary**, rather than a replication platform or commercial control plane.

Competing tools either:
- Only do filesystem backups
- Are full commercial platforms you have to buy and operate (Kannika, Lenses K2K, Confluent)
- Are replication-focused tools that never give you a truly air-gapped, object-storage-based backup you can keep independent of any running Kafka cluster

This makes OSO Kafka Backup the highest-leverage choice for teams that need real Kafka disaster recovery and rollback, without adopting a new proprietary replication product or running extra clusters just to feel "safe."

### Key Differentiators

1. **Millisecond-precision PITR** - Restore to any point in time, not just "latest" or "earliest"
2. **Cloud-native cold backups** - S3, Azure, GCS as first-class citizens
3. **Automated consumer offset recovery** - Multiple strategies to solve the offset discontinuity problem
4. **Zero platform dependencies** - Single binary, no Kafka Connect, no K8s operators
5. **True open source** - MIT license, actively maintained, community-focused

### When to Consider Alternatives

| If you need... | Consider |
|----------------|----------|
| Real-time active-active replication | MirrorMaker 2, Confluent Cluster Linking, or Lenses K2K |
| Enterprise support and SLAs | Kannika Armory or Confluent |
| Already running Kafka Connect | itadventurer/kafka-backup (if filesystem backup is sufficient) |
| Confluent Platform integration | Confluent Replicator |
| Data masking/filtering during replication | Lenses K2K |

### Decision Matrix

```
Need PITR?
├── Yes → Need cloud storage?
│         ├── Yes → Need open source?
│         │         ├── Yes → OSO Kafka Backup ✓
│         │         └── No  → Kannika Armory
│         └── No  → itadventurer/kafka-backup (if maintained)
└── No  → Need replication?
          ├── Yes → Need data masking/filtering?
          │         ├── Yes → Lenses K2K
          │         └── No  → Confluent Replicator or MirrorMaker 2
          └── No  → Simple filesystem backup may suffice
```

---

## Additional Resources

- [OSO Kafka Backup Documentation](../README.md)
- [Quick Start Guide](quickstart.md)
- [Configuration Reference](configuration.md)
- [Restore Guide](restore_guide.md)
- [Offset Recovery Deep Dive](Offset_Remapping_Deep_Dive.md)
