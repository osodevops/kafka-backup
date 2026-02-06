#!/bin/bash
# Stress Test: 3000 topics with concurrent producers + 3 backup modes
#
# Tests snapshot, one-shot, and continuous backup modes under realistic
# load with 3000 topics, ~1000 records pre-seeded per topic, and ~500 rec/sec
# concurrent production during backup.
#
# Usage: ./run-stress-test.sh [--skip-seed] [--mode snapshot|oneshot|continuous]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yaml"

NUM_TOPICS=${NUM_TOPICS:-3000}
RECORDS_PER_TOPIC=${RECORDS_PER_TOPIC:-1000}
CONTINUOUS_DURATION=${CONTINUOUS_DURATION:-60}
BACKUP_TIMEOUT=${BACKUP_TIMEOUT:-300}
BACKUP_DIR="/tmp/stress-test-backup"
LOG_DIR="/tmp/stress-test-logs"

SKIP_SEED=false
RUN_MODE="all"  # all, snapshot, oneshot, continuous

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --skip-seed)
            SKIP_SEED=true
            shift
            ;;
        --mode)
            RUN_MODE="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--skip-seed] [--mode snapshot|oneshot|continuous]"
            exit 1
            ;;
    esac
done

echo "=============================================="
echo "Kafka Backup Stress Test"
echo "=============================================="
echo "Project root: $PROJECT_ROOT"
echo "Topics: $NUM_TOPICS"
echo "Records per topic: $RECORDS_PER_TOPIC"
echo "Continuous duration: ${CONTINUOUS_DURATION}s"
echo "Backup timeout: ${BACKUP_TIMEOUT}s"
echo "Skip seed: $SKIP_SEED"
echo "Run mode: $RUN_MODE"
echo "=============================================="
echo ""

# Check prerequisites
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running"
    exit 1
fi

# Cleanup function
cleanup() {
    echo ""
    echo "Cleaning up..."
    "$SCRIPT_DIR/run-continuous-producers.sh" stop 2>/dev/null || true
    cd "$SCRIPT_DIR"
    docker compose -f "$COMPOSE_FILE" down -v 2>/dev/null || true
}

trap cleanup EXIT

# Prepare directories
rm -rf "$BACKUP_DIR" "$LOG_DIR"
mkdir -p "$BACKUP_DIR" "$LOG_DIR"

# ============================================================
# Step 1: Build
# ============================================================
echo "=============================================="
echo "Step 1: Building kafka-backup"
echo "=============================================="
cd "$PROJECT_ROOT"
cargo build --release -p kafka-backup-cli
echo "Build complete."
echo ""

# ============================================================
# Step 2: Start Kafka infrastructure
# ============================================================
echo "=============================================="
echo "Step 2: Starting Kafka infrastructure"
echo "=============================================="
cd "$SCRIPT_DIR"
docker compose -f "$COMPOSE_FILE" up -d zookeeper kafka

echo "Waiting for Kafka to be healthy..."
timeout_secs=120
elapsed=0
while ! docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list > /dev/null 2>&1; do
    sleep 2
    elapsed=$((elapsed + 2))
    if [ $elapsed -ge $timeout_secs ]; then
        echo "ERROR: Kafka did not become healthy within ${timeout_secs}s"
        docker compose -f "$COMPOSE_FILE" logs kafka
        exit 1
    fi
    echo "  Waiting... (${elapsed}s)"
done
echo "Kafka is ready!"
echo ""

# ============================================================
# Step 3: Create 3000 topics
# ============================================================
echo "=============================================="
echo "Step 3: Creating $NUM_TOPICS topics"
echo "=============================================="
docker compose -f "$COMPOSE_FILE" run --rm \
    -e NUM_TOPICS=$NUM_TOPICS \
    kafka-setup

# Verify topics
topic_count=$(docker compose -f "$COMPOSE_FILE" exec -T kafka kafka-topics --bootstrap-server localhost:29092 --list | grep -c "stress-test-" || echo "0")
echo "Verified: $topic_count stress-test topics exist"

if [ "$topic_count" -lt "$NUM_TOPICS" ]; then
    echo "WARNING: Expected $NUM_TOPICS topics but found $topic_count"
fi
echo ""

# ============================================================
# Step 4: Seed data
# ============================================================
if [ "$SKIP_SEED" = false ]; then
    echo "=============================================="
    echo "Step 4: Seeding data (~$RECORDS_PER_TOPIC records/topic)"
    echo "=============================================="
    seed_start=$(date +%s)
    bash "$SCRIPT_DIR/seed-data.sh"
    seed_end=$(date +%s)
    echo "Seeding took $((seed_end - seed_start))s"
    echo ""
else
    echo "=============================================="
    echo "Step 4: Skipping data seeding (--skip-seed)"
    echo "=============================================="
    echo ""
fi

# ============================================================
# Helper: Run a single backup test
# ============================================================
run_backup_test() {
    local mode=$1
    local config_file=$2
    local log_file="$LOG_DIR/backup-${mode}.log"
    local backup_subdir="$BACKUP_DIR/${mode}"

    echo "=============================================="
    echo "Test: $mode mode"
    echo "=============================================="

    # Clean backup directory for this mode
    rm -rf "$backup_subdir"
    mkdir -p "$backup_subdir"

    # Start continuous producers
    echo "Starting continuous producers..."
    bash "$SCRIPT_DIR/run-continuous-producers.sh" start
    sleep 3  # Let producers warm up

    # Run backup
    echo "Starting $mode backup at $(date)"
    local backup_start=$(date +%s)

    if [ "$mode" = "continuous" ]; then
        # Continuous mode: run for CONTINUOUS_DURATION seconds then kill
        RUST_LOG=info "$PROJECT_ROOT/target/release/kafka-backup" \
            backup \
            --config "$config_file" \
            2>&1 | tee "$log_file" &

        local backup_pid=$!

        echo "  Running continuous backup for ${CONTINUOUS_DURATION}s..."
        local continuous_elapsed=0
        while [ $continuous_elapsed -lt $CONTINUOUS_DURATION ] && kill -0 $backup_pid 2>/dev/null; do
            sleep 5
            continuous_elapsed=$((continuous_elapsed + 5))
            local current_line=$(tail -1 "$log_file" 2>/dev/null || true)
            echo "  [${continuous_elapsed}s/${CONTINUOUS_DURATION}s] $current_line"
        done

        # Kill the continuous backup
        if kill -0 $backup_pid 2>/dev/null; then
            echo "  Stopping continuous backup after ${CONTINUOUS_DURATION}s..."
            kill $backup_pid 2>/dev/null || true
            wait $backup_pid 2>/dev/null || true
        fi
        local backup_exit_code=0  # Expected to be killed
    else
        # Snapshot/one-shot: run to completion with timeout
        RUST_LOG=info "$PROJECT_ROOT/target/release/kafka-backup" \
            backup \
            --config "$config_file" \
            2>&1 | tee "$log_file" &

        local backup_pid=$!

        # Monitor with timeout
        local monitor_elapsed=0
        local last_line=""
        while kill -0 $backup_pid 2>/dev/null; do
            sleep 5
            monitor_elapsed=$(($(date +%s) - backup_start))

            local current_line=$(tail -1 "$log_file" 2>/dev/null || true)
            if [ "$current_line" != "$last_line" ]; then
                echo "  [${monitor_elapsed}s] $current_line"
                last_line="$current_line"
            fi

            if [ $monitor_elapsed -gt $BACKUP_TIMEOUT ]; then
                echo ""
                echo "  TIMEOUT: Backup exceeded ${BACKUP_TIMEOUT}s - killing"
                kill $backup_pid 2>/dev/null || true
                break
            fi
        done

        wait $backup_pid 2>/dev/null
        local backup_exit_code=$?
    fi

    local backup_end=$(date +%s)
    local backup_duration=$((backup_end - backup_start))

    # Stop producers
    echo "Stopping continuous producers..."
    bash "$SCRIPT_DIR/run-continuous-producers.sh" stop
    sleep 2

    # Report timing
    echo ""
    echo "  $mode backup completed in ${backup_duration}s (exit code: $backup_exit_code)"

    # Validate
    echo ""
    bash "$SCRIPT_DIR/validate-results.sh" "$mode" "$backup_subdir" "$log_file" "$NUM_TOPICS" || true

    # Store result for summary
    echo "${mode}|${backup_duration}|${backup_exit_code}" >> "$LOG_DIR/results.csv"
    echo ""
}

# ============================================================
# Step 5: Run backup tests
# ============================================================
> "$LOG_DIR/results.csv"

if [ "$RUN_MODE" = "all" ] || [ "$RUN_MODE" = "snapshot" ]; then
    run_backup_test "snapshot" "$SCRIPT_DIR/backup-config-snapshot.yaml"
fi

if [ "$RUN_MODE" = "all" ] || [ "$RUN_MODE" = "oneshot" ]; then
    run_backup_test "oneshot" "$SCRIPT_DIR/backup-config-oneshot.yaml"
fi

if [ "$RUN_MODE" = "all" ] || [ "$RUN_MODE" = "continuous" ]; then
    run_backup_test "continuous" "$SCRIPT_DIR/backup-config-continuous.yaml"
fi

# ============================================================
# Summary
# ============================================================
echo ""
echo "=============================================="
echo "STRESS TEST SUMMARY"
echo "=============================================="
echo ""
printf "%-15s %-15s %-12s %-10s\n" "Mode" "Duration" "Exit Code" "Segments"
printf "%-15s %-15s %-12s %-10s\n" "----" "--------" "---------" "--------"

while IFS='|' read -r mode duration exit_code; do
    backup_id="stress-test-${mode}"
    segment_count=$(find "$BACKUP_DIR/${mode}/${backup_id}" -name "*.zst" 2>/dev/null | wc -l | tr -d ' ')
    printf "%-15s %-15s %-12s %-10s\n" "$mode" "${duration}s" "$exit_code" "$segment_count"
done < "$LOG_DIR/results.csv"

echo ""
echo "Logs:    $LOG_DIR/"
echo "Backups: $BACKUP_DIR/"
echo ""

# Overall pass/fail
failures=0
while IFS='|' read -r mode duration exit_code; do
    if [ "$mode" != "continuous" ] && [ "$exit_code" != "0" ]; then
        failures=$((failures + 1))
    fi
done < "$LOG_DIR/results.csv"

if [ $failures -eq 0 ]; then
    echo "OVERALL: PASS"
    exit 0
else
    echo "OVERALL: FAIL ($failures mode(s) had non-zero exit codes)"
    exit 1
fi
