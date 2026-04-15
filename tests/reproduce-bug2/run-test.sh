#!/usr/bin/env bash
# Reproduce & verify fix for Bug 2: Validation NOT_LEADER_FOR_PARTITION
#
# Bug: ValidationContext held a single KafkaClient (one TCP connection to the
# bootstrap broker). ListOffsets requests for partitions whose leader is on a
# different broker received error code 6 (NOT_LEADER_FOR_PARTITION), causing
# MessageCountCheck and OffsetRangeCheck to fail on multi-broker clusters.
#
# On a 3-broker cluster with balanced leadership, ~67% of partitions fail.
#
# Fix: Replace Arc<KafkaClient> with Arc<PartitionLeaderRouter> in
# ValidationContext. PartitionLeaderRouter routes ListOffsets to the correct
# partition leader with automatic retry on NOT_LEADER_FOR_PARTITION.
#
# This test verifies:
#   1. Backup succeeds on a 3-broker cluster
#   2. Validation MessageCountCheck passes (all partitions counted)
#   3. Validation OffsetRangeCheck passes (all partitions verified)
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

assert_contains() {
    local label="$1" output="$2" pattern="$3"
    if echo "$output" | grep -q "$pattern"; then
        echo "  PASS: $label"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $label (expected to find: $pattern)"
        FAIL=$((FAIL + 1))
    fi
}

assert_not_contains() {
    local label="$1" output="$2" pattern="$3"
    if echo "$output" | grep -q "$pattern"; then
        echo "  FAIL: $label (should NOT contain: $pattern)"
        FAIL=$((FAIL + 1))
    else
        echo "  PASS: $label"
        PASS=$((PASS + 1))
    fi
}

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  Bug 2 test: Validation NOT_LEADER_FOR_PARTITION (3-broker)    ║"
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
echo "Cluster is ready."

# Create topics with RF=3, 6 partitions → leaders distributed across 3 brokers
echo ""
echo "=== Creating topics (RF=3, 6 partitions each) ==="
for topic in orders payments events; do
    docker exec pr86-kafka1 /opt/kafka/bin/kafka-topics.sh \
        --create --if-not-exists --bootstrap-server kafka1:19092 \
        --partitions 6 --replication-factor 3 --topic "$topic" 2>&1
done

echo ""
echo "=== Partition leader distribution ==="
for topic in orders payments events; do
    docker exec pr86-kafka1 /opt/kafka/bin/kafka-topics.sh \
        --describe --bootstrap-server kafka1:19092 --topic "$topic" 2>/dev/null
done

# Produce data
echo ""
echo "=== Producing data ==="
for topic in orders payments events; do
    for i in $(seq 1 200); do echo "key-$i:value-$i"; done | \
        docker exec -i pr86-kafka1 /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka1:19092 --topic "$topic" \
        --property parse.key=true --property key.separator=: 2>&1
    echo "  $topic: 200 records"
done

# Backup
echo ""
echo "=== Running backup ==="
BACKUP_ID="bug2-test"
STORAGE_DIR="$WORK_DIR/storage"
mkdir -p "$STORAGE_DIR"

cat > "$WORK_DIR/backup.yaml" <<EOF
mode: backup
backup_id: "$BACKUP_ID"
source:
  bootstrap_servers: ["$BOOTSTRAP"]
  topics:
    include: ["orders", "payments", "events"]
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

# Generate signing keys
SIGNING_KEY_DIR=$(mktemp -d)
openssl ecparam -genkey -name prime256v1 -noout -out "$SIGNING_KEY_DIR/private.pem" 2>/dev/null
openssl ec -in "$SIGNING_KEY_DIR/private.pem" -pubout -out "$SIGNING_KEY_DIR/public.pem" 2>/dev/null
openssl pkcs8 -topk8 -nocrypt -in "$SIGNING_KEY_DIR/private.pem" -out "$SIGNING_KEY_DIR/private-pkcs8.pem" 2>/dev/null

# Run validation
echo ""
echo "=== Running validation (bootstrap=$BOOTSTRAP = broker 1) ==="
echo "(Before fix: ~67% of partitions would fail with NOT_LEADER_FOR_PARTITION)"
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
    "MessageCountCheck passes" \
    "$OUTPUT" \
    "PASSED.*MessageCountCheck"

assert_contains \
    "OffsetRangeCheck passes" \
    "$OUTPUT" \
    "PASSED.*OffsetRangeCheck"

assert_not_contains \
    "No NOT_LEADER_FOR_PARTITION failures" \
    "$OUTPUT" \
    "FAILED.*MessageCountCheck"

assert_contains \
    "All 600 records accounted for" \
    "$OUTPUT" \
    "600 messages expected, 600 restored"

assert_contains \
    "All 18 partitions pass offset check" \
    "$OUTPUT" \
    "18 partitions checked; 18 passed"

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
