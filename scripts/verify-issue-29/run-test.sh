#!/bin/bash
# Issue #29 Regression Test Script
# Verifies that backup with 3000 topics completes in reasonable time

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

echo "=============================================="
echo "Issue #29 Regression Test"
echo "=============================================="
echo "Project root: $PROJECT_ROOT"
echo "Script dir: $SCRIPT_DIR"
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
    docker compose down -v 2>/dev/null || true
    rm -rf /tmp/issue-29-backup 2>/dev/null || true
}

# Set trap for cleanup
trap cleanup EXIT

# Build the project first
echo "Building kafka-backup..."
cd "$PROJECT_ROOT"
cargo build --release -p kafka-backup-cli

# Start Kafka with Docker Compose
echo ""
echo "Starting Kafka infrastructure..."
cd "$SCRIPT_DIR"
docker compose up -d zookeeper kafka

# Wait for Kafka to be healthy
echo "Waiting for Kafka to be healthy..."
timeout=120
elapsed=0
while ! docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ $elapsed -ge $timeout ]; then
        echo "ERROR: Kafka did not become healthy within ${timeout}s"
        docker compose logs kafka
        exit 1
    fi
    echo "  Waiting... (${elapsed}s)"
done
echo "Kafka is ready!"

# Create topics and produce data
echo ""
echo "Creating 3000 topics with data..."
docker compose run --rm kafka-setup

# Verify topics were created
topic_count=$(docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list | wc -l | tr -d ' ')
echo "Verified: $topic_count topics exist"

if [ "$topic_count" -lt 3000 ]; then
    echo "WARNING: Expected 3000 topics but found $topic_count"
fi

# Prepare backup directory
rm -rf /tmp/issue-29-backup
mkdir -p /tmp/issue-29-backup

# Run the backup with timing
echo ""
echo "=============================================="
echo "Running backup test..."
echo "=============================================="
echo "Expected behavior (BUG): Backup takes very long or appears stuck"
echo "Expected behavior (FIXED): Backup completes in seconds"
echo ""

backup_start=$(date +%s)

# Run backup (macOS doesn't have timeout, so we'll use a simple background approach)
# Use RUST_LOG to show progress
echo "Starting backup at $(date)"
RUST_LOG=info "$PROJECT_ROOT/target/release/kafka-backup" \
    backup \
    --config "$SCRIPT_DIR/backup-config.yaml" \
    2>&1 | tee /tmp/issue-29-backup.log &

backup_pid=$!

# Monitor progress with timestamps
monitor_start=$(date +%s)
last_line=""
max_wait=300  # 5 minutes max

while kill -0 $backup_pid 2>/dev/null; do
    elapsed=$(($(date +%s) - monitor_start))

    # Show latest log line if it changed
    current_line=$(tail -1 /tmp/issue-29-backup.log 2>/dev/null || true)
    if [ "$current_line" != "$last_line" ]; then
        echo "  [${elapsed}s] $current_line"
        last_line="$current_line"
    fi

    # Check for stuck behavior (> 60s without progress)
    if [ $elapsed -gt 60 ] && [ $((elapsed % 30)) -eq 0 ]; then
        echo ""
        echo "WARNING: Backup has been running for over ${elapsed} seconds"
        echo "This may indicate the bug is present"
    fi

    # Timeout after max_wait seconds
    if [ $elapsed -gt $max_wait ]; then
        echo ""
        echo "TIMEOUT: Backup exceeded ${max_wait}s - killing process"
        kill $backup_pid 2>/dev/null || true
        break
    fi

    sleep 2
done

# Wait for backup to finish and get exit code
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
echo ""

# Check results
if [ $backup_exit_code -eq 0 ]; then
    echo "Backup completed successfully"

    # Count backed up files
    segment_count=$(find /tmp/issue-29-backup -name "*.zst" 2>/dev/null | wc -l | tr -d ' ')
    echo "Segments created: $segment_count"

    if [ -f /tmp/issue-29-backup/issue-29-test/manifest.json ]; then
        echo "Manifest exists: YES"
    else
        echo "Manifest exists: NO"
    fi
else
    echo "Backup FAILED with exit code $backup_exit_code"
fi

echo ""
echo "=============================================="
echo "Analysis"
echo "=============================================="

if [ $backup_duration -gt 120 ]; then
    echo "RESULT: BUG CONFIRMED"
    echo "Backup took ${backup_duration}s (> 120s)"
    echo "This indicates the per-topic metadata call issue is present"
elif [ $backup_duration -gt 30 ]; then
    echo "RESULT: POSSIBLE BUG"
    echo "Backup took ${backup_duration}s (30-120s)"
    echo "Performance is degraded but may be acceptable"
else
    echo "RESULT: OK"
    echo "Backup completed in ${backup_duration}s (< 30s)"
    echo "Performance appears acceptable"
fi

echo ""
echo "Full log available at: /tmp/issue-29-backup.log"
echo "=============================================="
