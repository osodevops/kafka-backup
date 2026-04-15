#!/usr/bin/env bash
# Reproduce & verify fix for Bug 4: Empty snapshot overwrites valid one
#
# Bug: snapshot_consumer_groups() unconditionally overwrites the snapshot file
# in storage, even when the group list is empty. This destroys valid snapshot
# data when kafka-backup restarts before consumer apps have reconnected.
#
# Fix: skip storage.put() when snapshot_groups is empty.
#
# This test:
#   1. Creates consumer groups and runs backup → snapshot populated
#   2. Deletes consumer groups (simulates restart before apps reconnect)
#   3. Runs backup again → snapshot should NOT be overwritten with empty list
#
# Requires: Docker
# Usage: ./run-test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
BOOTSTRAP="localhost:19092"
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

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  Bug 4 test: Empty snapshot must not overwrite valid one       ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# Build
echo "=== Building kafka-backup CLI ==="
cargo build -p kafka-backup-cli --release 2>&1 | tail -3
CLI="$PROJECT_ROOT/target/release/kafka-backup"

# Start Kafka
echo ""
echo "=== Starting Kafka ==="
docker compose -f "$COMPOSE_FILE" up -d
for i in $(seq 1 30); do
    docker exec bug1-kafka /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:19094 --list 2>/dev/null && break
    [ "$i" -eq 30 ] && { echo "ERROR: Kafka not ready"; exit 1; }
    sleep 1
done
echo "Kafka is ready."

# Create topic and data
echo ""
echo "=== Creating topic and data ==="
docker exec bug1-kafka /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists --bootstrap-server kafka:19094 \
    --partitions 3 --replication-factor 1 --topic orders 2>&1

for i in $(seq 1 100); do echo "key-$i:value-$i"; done | \
    docker exec -i bug1-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:19094 --topic orders \
    --property parse.key=true --property key.separator=: 2>&1
echo "100 records produced."

# Create consumer groups
echo ""
echo "=== Creating consumer groups ==="
for group in app-orders app-analytics app-audit; do
    docker exec bug1-kafka /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server kafka:19094 --group "$group" --topic orders \
        --from-beginning --timeout-ms 10000 > /dev/null 2>&1 || true
    echo "  Created: $group"
done

# Step 1: Backup with snapshot → should capture all 3 groups
echo ""
echo "=== Step 1: Backup with consumer_group_snapshot (groups exist) ==="
BACKUP_ID="bug4-test"
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

$CLI backup --config "$WORK_DIR/backup.yaml" 2>&1 | grep -q "Consumer groups snapshot saved" && \
    echo "Backup complete, snapshot saved."

SNAPSHOT="$STORAGE_DIR/$BACKUP_ID/consumer-groups-snapshot.json"
GROUPS_BEFORE=$(python3 -c "import json; print(len(json.load(open('$SNAPSHOT')).get('groups', [])))")
echo "Groups in snapshot: $GROUPS_BEFORE"

# Step 2: Delete all consumer groups
echo ""
echo "=== Step 2: Deleting consumer groups (simulates restart scenario) ==="
for group in app-orders app-analytics app-audit; do
    docker exec bug1-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
        --bootstrap-server kafka:19094 --delete --group "$group" 2>/dev/null || true
    echo "  Deleted: $group"
done

# Step 3: Re-run backup → empty snapshot should NOT overwrite
echo ""
echo "=== Step 3: Re-run backup (no consumer groups exist) ==="
# Produce a bit more data so backup has something to do
for i in $(seq 101 105); do echo "key-$i:value-$i"; done | \
    docker exec -i bug1-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:19094 --topic orders \
    --property parse.key=true --property key.separator=: 2>&1

$CLI backup --config "$WORK_DIR/backup.yaml" 2>&1 | tail -5

GROUPS_AFTER=$(python3 -c "import json; print(len(json.load(open('$SNAPSHOT')).get('groups', [])))")
echo "Groups in snapshot after step 3: $GROUPS_AFTER"

# Assertions
echo ""
echo "=== Assertions ==="
assert_equals \
    "Snapshot had groups after step 1" \
    "$GROUPS_BEFORE" \
    "3"

assert_equals \
    "Snapshot preserved after step 3 (empty result did not overwrite)" \
    "$GROUPS_AFTER" \
    "3"

# Summary
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Results: $PASS passed, $FAIL failed"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$FAIL" -gt 0 ]; then
    echo "  SOME TESTS FAILED"
    exit 1
else
    echo "  ALL TESTS PASSED"
    exit 0
fi
