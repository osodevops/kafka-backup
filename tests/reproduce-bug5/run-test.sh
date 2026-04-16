#!/usr/bin/env bash
# Reproduce & verify fix for Bug 5: OffsetFetch NOT_COORDINATOR in consumer group snapshot
#
# Bug: snapshot_consumer_groups() listed groups from all brokers correctly via
# list_groups_all_brokers(), but then called fetch_offsets(bootstrap_client, ...)
# for EVERY group through the single bootstrap broker. Groups coordinated by
# other brokers returned NOT_COORDINATOR (error 16), which fetch_offsets() did
# NOT check — it returned Ok(vec![]), silently dropping the group.
#
# On a 3-broker cluster, only ~1/3 of groups appeared in the snapshot.
#
# Fix: Add fetch_group_offsets_all_coordinators() to PartitionLeaderRouter
# which lists groups on each broker and fetches offsets from the same broker
# (the coordinator), ensuring complete coverage.
#
# This test:
#   1. Creates 12 consumer groups across a 3-broker cluster
#   2. Runs backup with consumer_group_snapshot
#   3. Verifies ALL groups appear in the snapshot (not just those on broker 1)
#
# Requires: Docker (for 3-broker KRaft cluster)
# Usage: ./run-test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
BOOTSTRAP="localhost:29092"
WORK_DIR=$(mktemp -d)
PASS=0
FAIL=0

trap cleanup EXIT
cleanup() {
    echo ""
    echo "=== Cleanup ==="
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    rm -rf "$WORK_DIR"
}

assert_equals() {
    local label="$1" actual="$2" expected="$3"
    if [ "$actual" = "$expected" ]; then
        echo "  PASS: $label (=$actual)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label (expected=$expected, got=$actual)"
        FAIL=$((FAIL + 1))
    fi
}

assert_ge() {
    local label="$1" actual="$2" minimum="$3"
    if [ "$actual" -ge "$minimum" ] 2>/dev/null; then
        echo "  PASS: $label ($actual >= $minimum)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label ($actual < $minimum)"
        FAIL=$((FAIL + 1))
    fi
}

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  Bug 5 test: OffsetFetch coordinator routing (3-broker)        ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Build
echo "=== Building kafka-backup CLI ==="
cargo build -p kafka-backup-cli --release 2>&1 | tail -3
CLI="$PROJECT_ROOT/target/release/kafka-backup"

# Start 3-broker cluster
echo ""
echo "=== Starting 3-broker KRaft cluster ==="
docker compose -f "$COMPOSE_FILE" up -d
for i in $(seq 1 45); do
    docker exec pr86-kafka1 /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka1:19092 --list 2>/dev/null && break
    [ "$i" -eq 45 ] && { echo "ERROR: Cluster not ready"; exit 1; }
    sleep 2
done
sleep 5
echo "Cluster is ready."

# Create topic
echo ""
echo "=== Creating topic ==="
docker exec pr86-kafka1 /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists --bootstrap-server kafka1:19092 \
    --partitions 6 --replication-factor 3 --topic orders 2>&1

# Produce data
for i in $(seq 1 300); do echo "key-$i:value-$i"; done | \
    docker exec -i pr86-kafka1 /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka1:19092 --topic orders \
    --property parse.key=true --property key.separator=: 2>&1
echo "300 records produced."

# Create 12 consumer groups — hash distribution across 3 coordinators
echo ""
echo "=== Creating 12 consumer groups ==="
GROUPS=(
    "app-service-alpha"
    "app-service-beta"
    "app-service-gamma"
    "app-workers-main"
    "app-workers-retry"
    "app-analytics-v1"
    "app-analytics-v2"
    "app-audit-trail"
    "app-notifications"
    "app-search-indexer"
    "app-cache-warmer"
    "app-email-sender"
)

for group in "${GROUPS[@]}"; do
    docker exec pr86-kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka1:19092 --group "$group" --topic orders \
        --from-beginning --timeout-ms 8000 > /dev/null 2>&1 || true
    echo "  Created: $group"
done

TOTAL_GROUPS=${#GROUPS[@]}

# Show __consumer_offsets leaders (coordinators)
echo ""
echo "=== __consumer_offsets partition leaders (coordinators) ==="
docker exec pr86-kafka1 /opt/kafka/bin/kafka-topics.sh \
    --describe --bootstrap-server kafka1:19092 --topic __consumer_offsets 2>/dev/null

# Run backup with consumer_group_snapshot
echo ""
echo "=== Running backup with consumer_group_snapshot ==="
echo "  Bootstrap: $BOOTSTRAP (broker 1)"
echo "  Before fix: only groups coordinated by broker 1 would appear"
echo ""

BACKUP_ID="bug5-test"
STORAGE_DIR="$WORK_DIR/storage"
mkdir -p "$STORAGE_DIR"

cat > "$WORK_DIR/backup.yaml" <<EOF
mode: backup
backup_id: "$BACKUP_ID"
source:
  bootstrap_servers: ["$BOOTSTRAP"]
  topics:
    include: ["orders"]
storage:
  backend: filesystem
  path: "$STORAGE_DIR"
backup:
  compression: zstd
  segment_max_bytes: 1048576
  stop_at_current_offsets: true
  consumer_group_snapshot: true
EOF

RUST_LOG=info $CLI backup --config "$WORK_DIR/backup.yaml" 2>&1 | \
    grep -E "(Consumer group snapshot|Backup completed)"

# Analyze snapshot
echo ""
echo "=== Snapshot analysis ==="
SNAPSHOT="$STORAGE_DIR/$BACKUP_ID/consumer-groups-snapshot.json"

SNAPSHOT_COUNT=$(python3 -c "import json; print(len(json.load(open('$SNAPSHOT')).get('groups', [])))")

python3 -c "
import json
s = json.load(open('$SNAPSHOT'))
groups = s.get('groups', [])
print(f'Groups in snapshot: {len(groups)} / $TOTAL_GROUPS created')
snapshot_ids = {g['group_id'] for g in groups}
all_ids = set('''${GROUPS[*]}'''.split())
missing = all_ids - snapshot_ids
if missing:
    print(f'MISSING groups: {sorted(missing)}')
else:
    print('All groups present!')
for g in sorted(groups, key=lambda x: x['group_id']):
    print(f'  + {g[\"group_id\"]}')
"

# Assertions
echo ""
echo "=== Assertions ==="
assert_equals \
    "All $TOTAL_GROUPS groups in snapshot" \
    "$SNAPSHOT_COUNT" \
    "$TOTAL_GROUPS"

assert_ge \
    "Snapshot has groups from multiple coordinators (not just bootstrap)" \
    "$SNAPSHOT_COUNT" \
    "8"

# Summary
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Results: $PASS passed, $FAIL failed"
echo "  Before fix: ~1/$((TOTAL_GROUPS / 3)) groups (bootstrap coordinator only)"
echo "  After fix:  $SNAPSHOT_COUNT/$TOTAL_GROUPS groups (all coordinators)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$FAIL" -gt 0 ]; then
    echo "  SOME TESTS FAILED"
    exit 1
else
    echo "  ALL TESTS PASSED"
    exit 0
fi
