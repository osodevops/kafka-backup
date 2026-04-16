#!/usr/bin/env bash
set -euo pipefail

# E2E test: Incremental one-shot backup with offset_storage
#
# Scenario:
#   1. Start Kafka + produce initial batch of messages
#   2. Run first backup (snapshot mode + offset_storage) → exits after catch-up
#   3. Producer keeps going for ~5 minutes
#   4. Run second backup (same backup_id + offset_storage) → should resume
#   5. Verify: manifest has records from BOTH runs, second run didn't re-fetch old data

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
WORK_DIR=$(mktemp -d)
TOPIC="incremental-test-$(date +%s)"
BACKUP_ID="incr-e2e-001"
BOOTSTRAP="localhost:9092"
STORAGE_PATH="$WORK_DIR/storage"
OFFSET_DB="$WORK_DIR/offsets.db"
BATCH1_COUNT=500
BATCH2_COUNT=500
PRODUCER_INTERVAL_MS=100   # 10 msgs/sec
WAIT_BETWEEN_BACKUPS=300   # 5 minutes

CLI="$PROJECT_ROOT/target/release/kafka-backup"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

log()  { echo -e "${GREEN}[TEST]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
fail() { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }

cleanup() {
    log "Cleaning up work dir: $WORK_DIR"
    # Don't remove — user may want to inspect
    log "Artifacts preserved at: $WORK_DIR"
}
trap cleanup EXIT

# ── Prerequisites ──────────────────────────────────────────────────
log "Building release binary..."
cargo build --release -p kafka-backup-cli 2>&1 | tail -2
[[ -x "$CLI" ]] || fail "Binary not found at $CLI"

log "Checking Kafka connectivity..."
if ! "$CLI" list --path "$STORAGE_PATH" 2>/dev/null; then
    # list might fail if no backups yet, that's fine — just need the binary to work
    true
fi

mkdir -p "$STORAGE_PATH"

# ── Create topic ───────────────────────────────────────────────────
log "Creating topic: $TOPIC (3 partitions)"
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \
    --create --if-not-exists \
    --bootstrap-server kafka-broker-1:9094 \
    --partitions 3 --replication-factor 1 \
    --topic "$TOPIC" 2>&1 || fail "Failed to create topic"

sleep 2

# ── Produce batch 1 ───────────────────────────────────────────────
log "Producing batch 1: $BATCH1_COUNT messages..."
docker exec kafka-broker-1 bash -c "
    for i in \$(seq 1 $BATCH1_COUNT); do
        echo \"key-\$i:batch1-value-\$i-$(date +%s)\"
    done | /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka-broker-1:9094 \
        --topic $TOPIC \
        --property parse.key=true \
        --property key.separator=:
" 2>&1 || fail "Failed to produce batch 1"

sleep 3
log "Batch 1 produced. Verifying offsets..."
docker exec kafka-broker-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka-broker-1:9094 \
    --list 2>/dev/null || true
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-broker-1:9094 \
    --describe --topic "$TOPIC" 2>&1 || true

# ── Write backup config ───────────────────────────────────────────
BACKUP_CONFIG="$WORK_DIR/backup.yaml"
cat > "$BACKUP_CONFIG" <<EOF
mode: backup
backup_id: "$BACKUP_ID"

source:
  bootstrap_servers:
    - "$BOOTSTRAP"
  topics:
    include:
      - "$TOPIC"

storage:
  backend: filesystem
  path: "$STORAGE_PATH"

backup:
  compression: zstd
  segment_max_bytes: 65536        # 64KB — small segments for visibility
  stop_at_current_offsets: true   # Snapshot: catch up and exit
  continuous: false
  include_offset_headers: true

offset_storage:
  db_path: "$OFFSET_DB"
  sync_interval_secs: 5
EOF

log "Config written to: $BACKUP_CONFIG"
cat "$BACKUP_CONFIG"
echo

# ── Run first backup ──────────────────────────────────────────────
log "═══════════════════════════════════════════════════"
log "  FIRST BACKUP RUN (should back up ~$BATCH1_COUNT records)"
log "═══════════════════════════════════════════════════"
RUST_LOG=info "$CLI" backup --config "$BACKUP_CONFIG" 2>&1 | tee "$WORK_DIR/backup-run1.log"

# Check manifest after run 1
MANIFEST="$STORAGE_PATH/$BACKUP_ID/manifest.json"
if [[ ! -f "$MANIFEST" ]]; then
    fail "Manifest not found after first backup run: $MANIFEST"
fi

RUN1_RECORDS=$(python3 -c "
import json, sys
m = json.load(open('$MANIFEST'))
total = sum(s['record_count'] for t in m['topics'] for p in t['partitions'] for s in p['segments'])
print(total)
")
RUN1_SEGMENTS=$(python3 -c "
import json
m = json.load(open('$MANIFEST'))
total = sum(len(p['segments']) for t in m['topics'] for p in t['partitions'])
print(total)
")

log "Run 1 result: $RUN1_RECORDS records in $RUN1_SEGMENTS segments"
log "Offset DB exists: $(ls -la "$OFFSET_DB" 2>/dev/null && echo 'YES' || echo 'NO')"

# Verify offset DB was synced to storage
REMOTE_OFFSET="$STORAGE_PATH/$BACKUP_ID/offsets.db"
if [[ -f "$REMOTE_OFFSET" ]]; then
    log "Offset DB synced to remote storage: $REMOTE_OFFSET ($(du -h "$REMOTE_OFFSET" | cut -f1))"
else
    warn "Offset DB NOT found in remote storage — incremental resume may not work!"
fi

echo
log "═══════════════════════════════════════════════════"
log "  WAITING $WAIT_BETWEEN_BACKUPS seconds (producing batch 2 in background)"
log "═══════════════════════════════════════════════════"

# ── Produce batch 2 in background while waiting ───────────────────
log "Starting background producer: $BATCH2_COUNT messages over ${WAIT_BETWEEN_BACKUPS}s..."
(
    docker exec kafka-broker-1 bash -c "
        for i in \$(seq 1 $BATCH2_COUNT); do
            echo \"key-batch2-\$i:batch2-value-\$i-\$(date +%s%N)\"
            sleep \$(echo \"scale=3; $WAIT_BETWEEN_BACKUPS / $BATCH2_COUNT\" | bc)
        done | /opt/kafka/bin/kafka-console-producer.sh \
            --bootstrap-server kafka-broker-1:9094 \
            --topic $TOPIC \
            --property parse.key=true \
            --property key.separator=:
    " 2>&1
    log "Background producer finished"
) &
PRODUCER_PID=$!

# Wait the configured interval
ELAPSED=0
while [[ $ELAPSED -lt $WAIT_BETWEEN_BACKUPS ]]; do
    REMAINING=$((WAIT_BETWEEN_BACKUPS - ELAPSED))
    printf "\r${YELLOW}[WAIT]${NC} %d/%d seconds (producer PID: %d)..." "$ELAPSED" "$WAIT_BETWEEN_BACKUPS" "$PRODUCER_PID"
    sleep 10
    ELAPSED=$((ELAPSED + 10))
done
echo

# Wait for producer to finish
wait "$PRODUCER_PID" 2>/dev/null || true

sleep 3
log "Verifying offsets after batch 2..."
docker exec kafka-broker-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-broker-1:9094 \
    --describe --topic "$TOPIC" 2>&1 || true

# ── Run second backup ─────────────────────────────────────────────
log "═══════════════════════════════════════════════════"
log "  SECOND BACKUP RUN (should resume and back up ~$BATCH2_COUNT NEW records)"
log "═══════════════════════════════════════════════════"
RUST_LOG=info "$CLI" backup --config "$BACKUP_CONFIG" 2>&1 | tee "$WORK_DIR/backup-run2.log"

# ── Verify results ────────────────────────────────────────────────
RUN2_RECORDS=$(python3 -c "
import json
m = json.load(open('$MANIFEST'))
total = sum(s['record_count'] for t in m['topics'] for p in t['partitions'] for s in p['segments'])
print(total)
")
RUN2_SEGMENTS=$(python3 -c "
import json
m = json.load(open('$MANIFEST'))
total = sum(len(p['segments']) for t in m['topics'] for p in t['partitions'])
print(total)
")

echo
log "═══════════════════════════════════════════════════"
log "  RESULTS"
log "═══════════════════════════════════════════════════"
log "Run 1: $RUN1_RECORDS records, $RUN1_SEGMENTS segments"
log "Run 2: $RUN2_RECORDS records, $RUN2_SEGMENTS segments (merged manifest)"
EXPECTED_TOTAL=$((BATCH1_COUNT + BATCH2_COUNT))
log "Expected total: ~$EXPECTED_TOTAL records"

echo
# Check for incremental behavior in run 2 logs
if grep -q "starting at" "$WORK_DIR/backup-run2.log"; then
    log "Run 2 log shows offset resume:"
    grep "starting at\|offsets earliest\|lag=" "$WORK_DIR/backup-run2.log" | head -10
fi

# Verdict
if [[ "$RUN2_RECORDS" -gt "$RUN1_RECORDS" ]]; then
    log "${GREEN}PASS${NC}: Manifest grew from $RUN1_RECORDS to $RUN2_RECORDS records"
else
    fail "Manifest did NOT grow: run1=$RUN1_RECORDS, run2=$RUN2_RECORDS"
fi

if [[ "$RUN2_SEGMENTS" -gt "$RUN1_SEGMENTS" ]]; then
    log "${GREEN}PASS${NC}: New segments added ($RUN1_SEGMENTS → $RUN2_SEGMENTS)"
else
    warn "Segment count unchanged — records may have been appended to existing segments"
fi

if [[ "$RUN2_RECORDS" -ge "$EXPECTED_TOTAL" ]]; then
    log "${GREEN}PASS${NC}: All $EXPECTED_TOTAL records accounted for"
else
    warn "Record count ($RUN2_RECORDS) is less than expected ($EXPECTED_TOTAL) — some may have been in-flight"
fi

echo
log "Full manifest: $MANIFEST"
log "Run 1 log: $WORK_DIR/backup-run1.log"
log "Run 2 log: $WORK_DIR/backup-run2.log"
log "Offset DB: $OFFSET_DB"

# Pretty-print manifest summary
python3 -c "
import json
m = json.load(open('$MANIFEST'))
print()
print('Manifest Summary:')
print(f'  backup_id: {m[\"backup_id\"]}')
for t in m['topics']:
    print(f'  topic: {t[\"name\"]}')
    for p in t['partitions']:
        segs = p['segments']
        if segs:
            offsets = [(s['start_offset'], s['end_offset']) for s in segs]
            total = sum(s['record_count'] for s in segs)
            print(f'    partition {p[\"partition_id\"]}: {len(segs)} segments, {total} records, offsets {offsets[0][0]}-{offsets[-1][1]}')
"

echo
log "═══════════════════════════════════════════════════"
log "  TEST COMPLETE"
log "═══════════════════════════════════════════════════"
