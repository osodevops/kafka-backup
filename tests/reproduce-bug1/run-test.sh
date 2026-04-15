#!/usr/bin/env bash
# Reproduce & verify fix for Bug 1: auto_consumer_groups Phase 3 silently skipped
#
# Bug: When auto_consumer_groups=true, the three-phase restore's Phase 3
# (offset reset) was always skipped because:
#
#   1a. Config validation rejected reset_consumer_offsets=true with empty
#       consumer_groups, even when auto_consumer_groups=true would resolve
#       them at runtime from the snapshot.
#
#   1b. ThreePhaseRestore reads consumer_groups from a fresh config clone
#       AFTER the restore engine already injected groups from the snapshot
#       into its own local copy. The engine's mutation never propagated back.
#
# Fix:
#   - config.rs: skip validation when auto_consumer_groups=true
#   - manifest.rs: add resolved_consumer_groups to RestoreReport
#   - engine.rs: populate resolved_consumer_groups after restore
#   - three_phase.rs: use resolved_consumer_groups to drive Phase 3
#
# This test verifies:
#   1. Config with auto_consumer_groups=true + reset_consumer_offsets=true passes validation
#   2. Phase 3 actually runs and resets consumer group offsets
#   3. No regression when using explicit consumer_groups
#
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
echo "║  Bug 1 test: auto_consumer_groups Phase 3 offset reset        ║"
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

# Setup: topic, data, consumer group
echo ""
echo "=== Setup: topic, data, consumer group ==="
docker exec bug1-kafka /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists --bootstrap-server kafka:19094 \
    --partitions 3 --replication-factor 1 --topic orders 2>&1

for i in $(seq 1 100); do echo "key-$i:value-$i"; done | \
    docker exec -i bug1-kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:19094 --topic orders \
    --property parse.key=true --property key.separator=: 2>&1

docker exec bug1-kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:19094 --group test-app --topic orders \
    --from-beginning --timeout-ms 10000 > /dev/null 2>&1 || true
echo "Created topic 'orders' with 100 records, consumer group 'test-app'."

# Backup with consumer_group_snapshot
echo ""
echo "=== Backup with consumer_group_snapshot ==="
BACKUP_ID="bug1-test"
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
    echo "Backup complete, snapshot saved." || { echo "ERROR: Backup failed"; exit 1; }

# ── Test 1: auto_consumer_groups=true + reset=true should pass config validation ──
echo ""
echo "=== Test 1: config validation accepts auto_consumer_groups + reset ==="
cat > "$WORK_DIR/restore-test1.yaml" <<EOF
mode: restore
backup_id: "$BACKUP_ID"
target:
  bootstrap_servers: ["$BOOTSTRAP"]
storage:
  backend: filesystem
  path: "$STORAGE_DIR"
restore:
  auto_consumer_groups: true
  reset_consumer_offsets: true
  consumer_groups: []
  consumer_group_strategy: header-based
  create_topics: true
EOF

OUTPUT1=$(RUST_LOG=info $CLI three-phase-restore --config "$WORK_DIR/restore-test1.yaml" 2>&1 || true)
assert_not_contains \
    "Config does not reject auto_consumer_groups + reset_consumer_offsets" \
    "$OUTPUT1" \
    "consumer_groups must be specified"
assert_contains \
    "Phase 3 runs with auto-loaded groups" \
    "$OUTPUT1" \
    "Phase 3: Generating and applying offset reset plan"
assert_contains \
    "Offset reset succeeds" \
    "$OUTPUT1" \
    "Offset reset completed successfully"

# ── Test 2: auto_consumer_groups=true + reset=false still triggers Phase 3 ──
echo ""
echo "=== Test 2: auto_consumer_groups=true, reset=false still runs Phase 3 ==="
cat > "$WORK_DIR/restore-test2.yaml" <<EOF
mode: restore
backup_id: "$BACKUP_ID"
target:
  bootstrap_servers: ["$BOOTSTRAP"]
storage:
  backend: filesystem
  path: "$STORAGE_DIR"
restore:
  auto_consumer_groups: true
  reset_consumer_offsets: false
  consumer_groups: []
  consumer_group_strategy: header-based
  create_topics: true
EOF

OUTPUT2=$(RUST_LOG=info $CLI three-phase-restore --config "$WORK_DIR/restore-test2.yaml" 2>&1 || true)
assert_contains \
    "Phase 3 runs even with reset=false when auto groups found" \
    "$OUTPUT2" \
    "Phase 3: Generating and applying offset reset plan"
assert_contains \
    "Groups loaded from snapshot" \
    "$OUTPUT2" \
    "auto_consumer_groups: loaded"

# ── Test 3: explicit groups still work (no regression) ──
echo ""
echo "=== Test 3: explicit consumer_groups still works ==="
cat > "$WORK_DIR/restore-test3.yaml" <<EOF
mode: restore
backup_id: "$BACKUP_ID"
target:
  bootstrap_servers: ["$BOOTSTRAP"]
storage:
  backend: filesystem
  path: "$STORAGE_DIR"
restore:
  auto_consumer_groups: false
  reset_consumer_offsets: true
  consumer_groups: ["test-app"]
  consumer_group_strategy: header-based
  create_topics: true
EOF

OUTPUT3=$(RUST_LOG=info $CLI three-phase-restore --config "$WORK_DIR/restore-test3.yaml" 2>&1 || true)
assert_contains \
    "Phase 3 runs with explicit groups" \
    "$OUTPUT3" \
    "Phase 3: Generating and applying offset reset plan"

# ── Test 4: no groups, no auto → Phase 3 skipped (no regression) ──
echo ""
echo "=== Test 4: no groups, no auto → Phase 3 skipped ==="
cat > "$WORK_DIR/restore-test4.yaml" <<EOF
mode: restore
backup_id: "$BACKUP_ID"
target:
  bootstrap_servers: ["$BOOTSTRAP"]
storage:
  backend: filesystem
  path: "$STORAGE_DIR"
restore:
  auto_consumer_groups: false
  reset_consumer_offsets: false
  consumer_groups: []
  consumer_group_strategy: header-based
  create_topics: true
EOF

OUTPUT4=$(RUST_LOG=info $CLI three-phase-restore --config "$WORK_DIR/restore-test4.yaml" 2>&1 || true)
assert_contains \
    "Phase 3 correctly skipped when no groups" \
    "$OUTPUT4" \
    "PHASE 3: OFFSET RESET (skipped)"

# ── Summary ──
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
