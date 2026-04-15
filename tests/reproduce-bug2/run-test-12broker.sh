#!/usr/bin/env bash
# Stress test for Bug 2 fix on a 12-broker KRaft cluster.
#
# With 12 brokers and 36 partitions per topic (RF=3), leadership is spread
# across all 12 brokers. Before the fix, only ~8% of partitions (those led
# by the bootstrap broker) would pass validation. This test verifies that
# PartitionLeaderRouter correctly routes ListOffsets to all 12 leaders.
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

assert_contains() {
    local label="$1" output="$2" pattern="$3"
    if echo "$output" | grep -q "$pattern"; then
        echo "  PASS: $label"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label (expected to find: $pattern)"
        echo "  Actual output:"
        echo "$output" | grep -E "(PASS|FAIL|Overall)" | head -5
        FAIL=$((FAIL + 1))
    fi
}

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  Bug 2 stress test: 12-broker KRaft cluster                    ║"
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
echo "Waiting for cluster (12 brokers may take a moment)..."
for i in $(seq 1 90); do
    docker exec bug2-kafka1 /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka1:19092 --list 2>/dev/null && break
    [ "$i" -eq 90 ] && { echo "ERROR: Cluster not ready"; exit 1; }
    sleep 2
done
# Extra wait for all 12 brokers to join
sleep 10
echo "Cluster is ready."

# Create topics with 36 partitions (3 per broker), RF=3
echo ""
echo "=== Creating topics (36 partitions, RF=3) ==="
for topic in orders payments events inventory users sessions; do
    docker exec bug2-kafka1 /opt/kafka/bin/kafka-topics.sh \
        --create --if-not-exists --bootstrap-server kafka1:19092 \
        --partitions 36 --replication-factor 3 --topic "$topic" 2>&1
done

# Show leader distribution for one topic
echo ""
echo "=== Leader distribution for 'orders' (expect all 12 brokers) ==="
docker exec bug2-kafka1 /opt/kafka/bin/kafka-topics.sh \
    --describe --bootstrap-server kafka1:19092 --topic orders 2>/dev/null | \
    awk '{for(i=1;i<=NF;i++) if($i=="Leader:") print $(i+1)}' | sort | uniq -c | sort -rn
echo "(count of partitions per broker leader)"

# Produce data
echo ""
echo "=== Producing data (500 records per topic, 6 topics) ==="
for topic in orders payments events inventory users sessions; do
    for i in $(seq 1 500); do echo "key-$i:value-$i"; done | \
        docker exec -i bug2-kafka1 /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka1:19092 --topic "$topic" \
        --property parse.key=true --property key.separator=: 2>&1
    echo "  $topic: 500 records"
done

# Backup
echo ""
echo "=== Running backup ==="
BACKUP_ID="bug2-12broker-test"
STORAGE_DIR="$WORK_DIR/storage"
mkdir -p "$STORAGE_DIR"

cat > "$WORK_DIR/backup.yaml" <<EOF
mode: backup
backup_id: "$BACKUP_ID"
source:
  bootstrap_servers: ["$BOOTSTRAP"]
  topics:
    include: ["orders", "payments", "events", "inventory", "users", "sessions"]
storage:
  backend: filesystem
  path: "$STORAGE_DIR"
backup:
  compression: zstd
  segment_max_bytes: 1048576
  stop_at_current_offsets: true
EOF

$CLI backup --config "$WORK_DIR/backup.yaml" 2>&1 | grep -q "Backup completed" && \
    echo "Backup complete." || { echo "ERROR: Backup failed"; exit 1; }

# Signing keys
SIGNING_KEY_DIR=$(mktemp -d)
openssl ecparam -genkey -name prime256v1 -noout -out "$SIGNING_KEY_DIR/private.pem" 2>/dev/null
openssl ec -in "$SIGNING_KEY_DIR/private.pem" -pubout -out "$SIGNING_KEY_DIR/public.pem" 2>/dev/null
openssl pkcs8 -topk8 -nocrypt -in "$SIGNING_KEY_DIR/private.pem" -out "$SIGNING_KEY_DIR/private-pkcs8.pem" 2>/dev/null

# Validation
echo ""
echo "=== Running validation (bootstrap=$BOOTSTRAP = broker 1 of 12) ==="
echo "(Before fix: only ~3/36 partitions per topic would pass = 8%)"
echo ""

cat > "$WORK_DIR/validation.yaml" <<EOF
backup_id: "$BACKUP_ID"
storage:
  backend: filesystem
  path: "$STORAGE_DIR"
target:
  bootstrap_servers: ["$BOOTSTRAP"]
checks:
  message_count:
    enabled: true
    mode: exact
  offset_range:
    enabled: true
  consumer_group_offsets:
    enabled: false
evidence:
  formats: [json]
  signing:
    enabled: true
    private_key_path: "$SIGNING_KEY_DIR/private-pkcs8.pem"
  storage:
    prefix: "evidence/"
    retention_days: 1
EOF

OUTPUT=$(RUST_LOG=info $CLI validation run --config "$WORK_DIR/validation.yaml" 2>&1 || true)
echo "$OUTPUT" | grep -E "(PASS|FAIL|Overall|Checks:)"

rm -rf "$SIGNING_KEY_DIR"

# Assertions
echo ""
echo "=== Assertions ==="
assert_contains \
    "MessageCountCheck passes on 12-broker cluster" \
    "$OUTPUT" \
    "PASSED.*MessageCountCheck"

assert_contains \
    "OffsetRangeCheck passes on 12-broker cluster" \
    "$OUTPUT" \
    "PASSED.*OffsetRangeCheck"

assert_contains \
    "All 3000 records accounted for (6 topics x 500)" \
    "$OUTPUT" \
    "3000 messages expected, 3000 restored"

assert_contains \
    "All 216 partitions pass (6 topics x 36 partitions)" \
    "$OUTPUT" \
    "216 partitions checked; 216 passed"

# Summary
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Results: $PASS passed, $FAIL failed"
echo "  Cluster: 12 brokers, 6 topics, 216 partitions total"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

if [ "$FAIL" -gt 0 ]; then
    echo "  SOME TESTS FAILED"
    exit 1
else
    echo "  ALL TESTS PASSED"
    exit 0
fi
