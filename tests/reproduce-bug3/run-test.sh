#!/usr/bin/env bash
# Reproduce & verify fix for Bug 3: Schema migration missing after offset DB load
#
# Bug: load_from_storage() downloads offsets.db from object storage and replaces
# the SQLite pool, but does NOT call initialize_schema() on the new database.
# If the stored DB was created by an older version (missing the backup_jobs
# table), the next query crashes: "no such table: backup_jobs"
#
# Fix: call self.initialize_schema().await? after pool replacement in
# load_from_storage(). All DDL uses CREATE TABLE IF NOT EXISTS, so it's
# idempotent on up-to-date databases.
#
# This test:
#   1. Creates an old-format SQLite DB (offsets table only, no backup_jobs)
#   2. Places it in the storage path
#   3. Runs backup (which loads the old DB from storage)
#   4. Verifies backup does NOT crash (schema migration ran)
#
# Requires: Docker, sqlite3
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
echo "║  Bug 3 test: Schema migration after loading offset DB          ║"
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

if ! command -v sqlite3 &>/dev/null; then
    echo "ERROR: sqlite3 CLI required. Install with: brew install sqlite3"
    exit 1
fi

# Build
echo "=== Building kafka-backup CLI ==="
cargo build -p kafka-backup-cli --release 2>&1 | tail -3
CLI="$PROJECT_ROOT/target/release/kafka-backup"

# Start Kafka
echo ""
echo "=== Starting Kafka ==="
docker compose -f "$COMPOSE_FILE" up -d
for i in $(seq 1 30); do
    docker exec pr86-kafka-single /opt/kafka/bin/kafka-topics.sh \
        --bootstrap-server kafka:19094 --list 2>/dev/null && break
    [ "$i" -eq 30 ] && { echo "ERROR: Kafka not ready"; exit 1; }
    sleep 1
done
echo "Kafka is ready."

# Create topic and data
echo ""
echo "=== Creating topic and data ==="
docker exec pr86-kafka-single /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists --bootstrap-server kafka:19094 \
    --partitions 3 --replication-factor 1 --topic orders 2>&1
for i in $(seq 1 50); do echo "key-$i:value-$i"; done | \
    docker exec -i pr86-kafka-single /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:19094 --topic orders \
    --property parse.key=true --property key.separator=: 2>&1
echo "50 records produced."

# Create old-format offsets.db (missing backup_jobs table)
echo ""
echo "=== Creating old-format offsets.db (no backup_jobs table) ==="
BACKUP_ID="bug3-test"
STORAGE_DIR="$WORK_DIR/storage"
mkdir -p "$STORAGE_DIR/$BACKUP_ID"

OLD_DB="$STORAGE_DIR/$BACKUP_ID/offsets.db"
sqlite3 "$OLD_DB" <<'SQL'
CREATE TABLE offsets (
    backup_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    last_offset INTEGER NOT NULL,
    checkpoint_ts INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000),
    PRIMARY KEY (backup_id, topic, partition)
);
CREATE INDEX idx_offsets_backup ON offsets(backup_id);
INSERT INTO offsets (backup_id, topic, partition, last_offset)
VALUES ('bug3-test', 'orders', 0, 5);
SQL

echo "Tables in old DB:"
sqlite3 "$OLD_DB" ".tables"
echo "(backup_jobs is MISSING — simulates DB from older version)"

# Clean stale temp DB from previous runs (the engine defaults to /tmp/{backup_id}-offsets.db)
rm -f "/tmp/${BACKUP_ID}-offsets.db"

# Run backup in continuous mode (triggers offset store load from storage)
echo ""
echo "=== Running backup (loads old DB → schema migration should run) ==="

LOCAL_DB="$WORK_DIR/local-offsets.db"
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
offset_storage:
  db_path: "$LOCAL_DB"
backup:
  compression: zstd
  segment_max_bytes: 1048576
  continuous: true
  checkpoint_interval_secs: 5
EOF

# Run backup for a few seconds then kill it — we just need to see if it crashes
LOG_FILE="$WORK_DIR/backup.log"
RUST_LOG=info $CLI backup --config "$WORK_DIR/backup.yaml" > "$LOG_FILE" 2>&1 &
PID=$!
sleep 10
kill $PID 2>/dev/null
wait $PID 2>/dev/null || true
OUTPUT=$(cat "$LOG_FILE")

echo ""
echo "=== Assertions ==="
assert_contains \
    "Old DB was loaded from storage" \
    "$OUTPUT" \
    "Loaded offset database from storage"

assert_not_contains \
    "No 'no such table: backup_jobs' crash" \
    "$OUTPUT" \
    "no such table: backup_jobs"

assert_contains \
    "Backup started successfully (schema migration worked)" \
    "$OUTPUT" \
    "Connected to Kafka cluster"

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
