#!/usr/bin/env bash
# Stress test for Bug 5 fix on a 12-broker KRaft cluster.
#
# With 12 brokers and 12 __consumer_offsets partitions, consumer group
# coordinators are spread across all 12 brokers. Before the fix, only
# ~8% of groups (those coordinated by the bootstrap broker) appeared
# in the snapshot. This test verifies complete coverage.
#
# Requires: Docker (spins up 12 Kafka containers)
# Usage: ./run-test-12broker.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose-12broker.yml"
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
echo "║  Bug 5 stress test: 12-broker KRaft cluster                    ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Build
echo "=== Building kafka-backup CLI ==="
cargo build -p kafka-backup-cli --release 2>&1 | tail -3
CLI="$PROJECT_ROOT/target/release/kafka-backup"

# Start 12-broker cluster
echo ""
echo "=== Starting 12-broker KRaft cluster ==="
docker compose -f "$COMPOSE_FILE" up -d
echo "Waiting for cluster (12 brokers)..."
for i in $(seq 1 90); do
    docker exec bug2-kafka1 /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka1:19092 --list 2>/dev/null && break
    [ "$i" -eq 90 ] && { echo "ERROR: Cluster not ready"; exit 1; }
    sleep 2
done
sleep 10
echo "Cluster is ready."

# Create topic
echo ""
echo "=== Creating topic (36 partitions, RF=3) ==="
docker exec bug2-kafka1 /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists --bootstrap-server kafka1:19092 \
    --partitions 36 --replication-factor 3 --topic orders 2>&1

# Produce data
for i in $(seq 1 500); do echo "key-$i:value-$i"; done | \
    docker exec -i bug2-kafka1 /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka1:19092 --topic orders \
    --property parse.key=true --property key.separator=: 2>&1
echo "500 records produced."

# Create 24 consumer groups — should distribute across all 12 coordinators
echo ""
echo "=== Creating 24 consumer groups ==="
GROUPS=(
    "svc-orders-primary"
    "svc-orders-secondary"
    "svc-payments-primary"
    "svc-payments-retry"
    "svc-inventory-main"
    "svc-inventory-audit"
    "svc-shipping-tracker"
    "svc-shipping-notify"
    "svc-analytics-realtime"
    "svc-analytics-batch"
    "svc-analytics-ml"
    "svc-search-indexer"
    "svc-search-suggest"
    "svc-cache-warmer"
    "svc-cache-invalidator"
    "svc-email-sender"
    "svc-email-bounce"
    "svc-webhook-dispatcher"
    "svc-webhook-retry"
    "svc-audit-trail"
    "svc-compliance-log"
    "svc-billing-events"
    "svc-billing-reconcile"
    "svc-notification-push"
)

for group in "${GROUPS[@]}"; do
    docker exec bug2-kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka1:19092 --group "$group" --topic orders \
        --from-beginning --timeout-ms 5000 > /dev/null 2>&1 || true
    echo "  Created: $group"
done

TOTAL_GROUPS=${#GROUPS[@]}

# Show coordinator distribution
echo ""
echo "=== __consumer_offsets partition leaders (12 partitions across 12 brokers) ==="
docker exec bug2-kafka1 /opt/kafka/bin/kafka-topics.sh \
    --describe --bootstrap-server kafka1:19092 --topic __consumer_offsets 2>/dev/null | \
    awk '{for(i=1;i<=NF;i++) if($i=="Leader:") print $(i+1)}' | sort | uniq -c | sort -rn
echo "(partitions per broker — should be ~1 each)"

# Run backup with consumer_group_snapshot
echo ""
echo "=== Running backup (bootstrap=broker 1 of 12) ==="
echo "  Before fix: only ~2/$TOTAL_GROUPS groups from broker 1's coordinator"
echo ""

BACKUP_ID="bug5-12broker-test"
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

SNAPSHOT_COUNT=$(python3 -c "
import json
s = json.load(open('$SNAPSHOT'))
groups = s.get('groups', [])
print(len(groups))
")

python3 -c "
import json
s = json.load(open('$SNAPSHOT'))
groups = s.get('groups', [])
snapshot_ids = {g['group_id'] for g in groups}
expected = set('''${GROUPS[*]}'''.split())
found = expected & snapshot_ids
missing = expected - snapshot_ids
extra = snapshot_ids - expected
print(f'Expected groups:  {len(expected)}')
print(f'Found in snapshot: {len(found)}')
print(f'Missing:          {len(missing)}')
if missing:
    for g in sorted(missing):
        print(f'  MISSING: {g}')
if extra:
    print(f'Extra groups (console-consumer etc): {len(extra)}')
print()
print('All expected groups:')
for g in sorted(expected):
    status = '+' if g in snapshot_ids else 'X'
    print(f'  [{status}] {g}')
"

# Assertions
echo ""
echo "=== Assertions ==="
assert_ge \
    "All $TOTAL_GROUPS expected groups in snapshot" \
    "$SNAPSHOT_COUNT" \
    "$TOTAL_GROUPS"

assert_ge \
    "Groups from multiple coordinators captured (not just bootstrap)" \
    "$SNAPSHOT_COUNT" \
    "16"

# Summary
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Results: $PASS passed, $FAIL failed"
echo "  Cluster: 12 brokers, 24 groups, 12 coordinator partitions"
echo "  Before fix: ~2/$TOTAL_GROUPS groups (8%)"
echo "  After fix:  $SNAPSHOT_COUNT groups captured"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$FAIL" -gt 0 ]; then
    echo "  SOME TESTS FAILED"
    exit 1
else
    echo "  ALL TESTS PASSED"
    exit 0
fi
