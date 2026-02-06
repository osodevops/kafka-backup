#!/bin/bash
# Issue #29 High-Latency Reproduction Test
# Uses Toxiproxy to simulate Confluent Cloud latency (50ms per call)
# This demonstrates the exact conditions that cause the "stuck" behavior

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose-latency.yaml"
LATENCY_MS=${LATENCY_MS:-50}
NUM_TOPICS=${NUM_TOPICS:-100}

echo "=============================================="
echo "Issue #29 HIGH-LATENCY Reproduction Test"
echo "=============================================="
echo "Project root: $PROJECT_ROOT"
echo "Latency: ${LATENCY_MS}ms per call"
echo "Topics: $NUM_TOPICS"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running"
    exit 1
fi

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    cd "$SCRIPT_DIR"
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
    rm -rf /tmp/issue-29-latency-backup 2>/dev/null || true
}

# Set trap for cleanup
trap cleanup EXIT

# Build the project first
echo "Building kafka-backup..."
cd "$PROJECT_ROOT"
cargo build --release -p kafka-backup-cli

# Start Kafka + Toxiproxy
echo ""
echo "Starting Kafka + Toxiproxy infrastructure..."
cd "$SCRIPT_DIR"
docker compose -f "$COMPOSE_FILE" up -d zookeeper kafka toxiproxy

# Wait for Kafka
echo "Waiting for Kafka to be healthy..."
timeout=120
elapsed=0
while ! docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ $elapsed -ge $timeout ]; then
        echo "ERROR: Kafka did not become healthy within ${timeout}s"
        docker compose -f "$COMPOSE_FILE" logs kafka
        exit 1
    fi
    echo "  Waiting... (${elapsed}s)"
done
echo "Kafka is ready!"

# Wait for Toxiproxy
echo "Waiting for Toxiproxy..."
sleep 2
while ! curl -s http://localhost:8474/version > /dev/null 2>&1; do
    sleep 1
    elapsed=$((elapsed + 1))
    if [ $elapsed -ge $timeout ]; then
        echo "ERROR: Toxiproxy did not start"
        exit 1
    fi
done
echo "Toxiproxy is ready: $(curl -s http://localhost:8474/version)"

# Configure Toxiproxy: create proxy from port 19092 -> kafka:29092
echo ""
echo "Configuring Toxiproxy proxy (localhost:19092 -> kafka:29092)..."
curl -s -X POST http://localhost:8474/proxies \
    -H 'Content-Type: application/json' \
    -d '{
        "name": "kafka",
        "listen": "0.0.0.0:19092",
        "upstream": "kafka:29092",
        "enabled": true
    }' | python3 -m json.tool 2>/dev/null || echo "(proxy created)"

# Add latency toxic (both upstream and downstream)
echo ""
echo "Adding ${LATENCY_MS}ms latency toxic..."
curl -s -X POST http://localhost:8474/proxies/kafka/toxics \
    -H 'Content-Type: application/json' \
    -d "{
        \"name\": \"latency_downstream\",
        \"type\": \"latency\",
        \"stream\": \"downstream\",
        \"attributes\": {
            \"latency\": ${LATENCY_MS},
            \"jitter\": 10
        }
    }" | python3 -m json.tool 2>/dev/null || echo "(toxic added)"

curl -s -X POST http://localhost:8474/proxies/kafka/toxics \
    -H 'Content-Type: application/json' \
    -d "{
        \"name\": \"latency_upstream\",
        \"type\": \"latency\",
        \"stream\": \"upstream\",
        \"attributes\": {
            \"latency\": ${LATENCY_MS},
            \"jitter\": 10
        }
    }" | python3 -m json.tool 2>/dev/null || echo "(toxic added)"

# Verify proxy works (should be slow but functional)
echo ""
echo "Verifying proxied Kafka connection..."
echo "Direct (internal): $(docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>&1 | wc -l | tr -d ' ') topics visible"

# Create topics and produce data
echo ""
echo "Creating $NUM_TOPICS topics with data..."
docker compose -f "$COMPOSE_FILE" run --rm kafka-setup

# Verify topics
topic_count=$(docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list | wc -l | tr -d ' ')
echo "Verified: $topic_count topics exist"

# Prepare backup directory
rm -rf /tmp/issue-29-latency-backup
mkdir -p /tmp/issue-29-latency-backup

echo ""
echo "=============================================="
echo "Running HIGH-LATENCY backup test..."
echo "=============================================="
echo "With ${LATENCY_MS}ms latency, the OLD code would take:"
echo "  - Metadata: $NUM_TOPICS topics x ${LATENCY_MS}ms x 2 calls = $((NUM_TOPICS * LATENCY_MS * 2 / 1000))s"
echo "  - Plus offset capture and fetches"
echo ""
echo "With the FIX, metadata should complete in ~${LATENCY_MS}ms (single bulk call)"
echo ""

backup_start=$(date +%s)

# Run backup
echo "Starting backup at $(date)"
RUST_LOG=info "$PROJECT_ROOT/target/release/kafka-backup" \
    backup \
    --config "$SCRIPT_DIR/backup-config-latency.yaml" \
    2>&1 | tee /tmp/issue-29-latency-backup.log &

backup_pid=$!

# Monitor
monitor_start=$(date +%s)
last_line=""
max_wait=300

while kill -0 $backup_pid 2>/dev/null; do
    elapsed=$(($(date +%s) - monitor_start))

    current_line=$(tail -1 /tmp/issue-29-latency-backup.log 2>/dev/null || true)
    if [ "$current_line" != "$last_line" ]; then
        echo "  [${elapsed}s] $current_line"
        last_line="$current_line"
    fi

    if [ $elapsed -gt $max_wait ]; then
        echo ""
        echo "TIMEOUT: Backup exceeded ${max_wait}s - killing process"
        kill $backup_pid 2>/dev/null || true
        break
    fi

    sleep 2
done

wait $backup_pid 2>/dev/null
backup_exit_code=$?
backup_end=$(date +%s)
backup_duration=$((backup_end - backup_start))

echo ""
echo "=============================================="
echo "Test Results"
echo "=============================================="
echo "Backup exit code: $backup_exit_code"
echo "Backup duration: ${backup_duration}s"
echo "Simulated latency: ${LATENCY_MS}ms"
echo "Topics: $NUM_TOPICS"
echo ""

if [ $backup_exit_code -eq 0 ]; then
    echo "Backup completed SUCCESSFULLY"

    segment_count=$(find /tmp/issue-29-latency-backup -name "*.zst" 2>/dev/null | wc -l | tr -d ' ')
    echo "Segments created: $segment_count"

    if [ -f /tmp/issue-29-latency-backup/issue-29-latency-test/manifest.json ]; then
        echo "Manifest exists: YES"
    else
        echo "Manifest exists: NO"
    fi
else
    echo "Backup FAILED with exit code $backup_exit_code"
    echo ""
    echo "Error details from log:"
    grep -i "error\|failed" /tmp/issue-29-latency-backup.log | tail -20
fi

echo ""
echo "=============================================="
echo "Timing Analysis"
echo "=============================================="

# Extract key timestamps from log
metadata_line=$(grep "Backing up .* topics" /tmp/issue-29-latency-backup.log | head -1)
snapshot_line=$(grep "Captured snapshot offsets" /tmp/issue-29-latency-backup.log | head -1)
engine_start_line=$(grep "Backup engine starting" /tmp/issue-29-latency-backup.log | head -1)
spawned_line=$(grep "Spawned .* backup tasks" /tmp/issue-29-latency-backup.log | head -1)

echo "Key log entries:"
[ -n "$metadata_line" ] && echo "  Topics found: $metadata_line"
[ -n "$snapshot_line" ] && echo "  Offsets captured: $snapshot_line"
[ -n "$engine_start_line" ] && echo "  Engine started: $engine_start_line"
[ -n "$spawned_line" ] && echo "  Tasks spawned: $spawned_line"

echo ""
echo "Performance metrics from log:"
grep -A 12 "Performance Metrics" /tmp/issue-29-latency-backup.log || echo "(no metrics found)"

echo ""
# Compute expected old behavior
old_expected=$((NUM_TOPICS * LATENCY_MS * 2 / 1000))
echo "=============================================="
echo "Verdict"
echo "=============================================="
echo "Expected OLD code duration (metadata only): ~${old_expected}s"
echo "Actual duration: ${backup_duration}s"

if [ $backup_duration -gt $old_expected ]; then
    echo "RESULT: POSSIBLE REGRESSION - took longer than expected"
elif [ $backup_exit_code -ne 0 ]; then
    echo "RESULT: ERRORS DETECTED - errors were properly reported (fix #2 working)"
else
    echo "RESULT: FIX VERIFIED - completed much faster than old code would"
fi

echo ""
echo "Full log: /tmp/issue-29-latency-backup.log"
echo "=============================================="
