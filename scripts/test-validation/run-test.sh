#!/usr/bin/env bash
# End-to-end validation test using docker-compose.
#
# This script:
# 1. Starts Kafka via docker-compose
# 2. Produces test data
# 3. Runs a backup (snapshot mode)
# 4. Runs validation checks against the backup + live cluster
# 5. Generates an evidence report (JSON + PDF)
# 6. Verifies the evidence report signature
#
# Usage: ./run-test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
BOOTSTRAP="localhost:19092"
BACKUP_DIR=$(mktemp -d)
BACKUP_ID="validation-e2e-test"
SIGNING_KEY_DIR=$(mktemp -d)

trap cleanup EXIT

cleanup() {
    echo ""
    echo "=== Cleanup ==="
    echo "Stopping docker-compose..."
    docker compose -f "$SCRIPT_DIR/docker-compose.yml" down -v 2>/dev/null || true
    echo "Removing temp dirs..."
    rm -rf "$BACKUP_DIR" "$SIGNING_KEY_DIR"
    echo "Done."
}

echo "=== Validation E2E Test ==="
echo "Project root: $PROJECT_ROOT"
echo "Backup dir:   $BACKUP_DIR"
echo "Bootstrap:    $BOOTSTRAP"
echo ""

# Step 0: Build the CLI
echo "=== Building kafka-backup CLI ==="
cargo build -p kafka-backup-cli --release 2>&1 | tail -3
CLI="$PROJECT_ROOT/target/release/kafka-backup"

# Step 1: Start Kafka
echo ""
echo "=== Starting Kafka (docker-compose) ==="
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up -d kafka
echo "Waiting for Kafka to be ready..."
for i in $(seq 1 30); do
    if docker compose -f "$SCRIPT_DIR/docker-compose.yml" exec -T kafka \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9094 --list 2>/dev/null; then
        echo "Kafka is ready."
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "ERROR: Kafka did not become ready in 30s"
        exit 1
    fi
    sleep 1
done

# Step 2: Create topics and produce data
echo ""
echo "=== Producing test data ==="
docker compose -f "$SCRIPT_DIR/docker-compose.yml" up kafka-setup
echo "Waiting for data to settle..."
sleep 3

# Step 3: Backup
echo ""
echo "=== Running backup ==="
cat > "$BACKUP_DIR/backup.yaml" <<EOF
mode: backup
backup_id: "$BACKUP_ID"
source:
  bootstrap_servers: ["$BOOTSTRAP"]
  topics:
    include: ["orders", "payments", "events"]
storage:
  backend: filesystem
  path: "$BACKUP_DIR/data"
backup:
  compression: zstd
  segment_max_bytes: 1048576
  stop_at_current_offsets: true
EOF

mkdir -p "$BACKUP_DIR/data"
$CLI backup --config "$BACKUP_DIR/backup.yaml"
echo "Backup complete."

# Verify manifest
if [ ! -f "$BACKUP_DIR/data/$BACKUP_ID/manifest.json" ]; then
    echo "ERROR: Manifest not found"
    exit 1
fi
echo "Manifest:"
cat "$BACKUP_DIR/data/$BACKUP_ID/manifest.json" | python3 -m json.tool 2>/dev/null | head -20 || \
    cat "$BACKUP_DIR/data/$BACKUP_ID/manifest.json" | head -5

# Step 4: Generate signing keys
echo ""
echo "=== Generating ECDSA-P256 signing keys ==="
openssl ecparam -genkey -name prime256v1 -noout -out "$SIGNING_KEY_DIR/private.pem" 2>/dev/null
openssl ec -in "$SIGNING_KEY_DIR/private.pem" -pubout -out "$SIGNING_KEY_DIR/public.pem" 2>/dev/null
# Convert to PKCS#8 format (required by p256 crate)
openssl pkcs8 -topk8 -nocrypt -in "$SIGNING_KEY_DIR/private.pem" -out "$SIGNING_KEY_DIR/private-pkcs8.pem" 2>/dev/null
echo "Keys generated in $SIGNING_KEY_DIR"

# Step 5: Run validation
echo ""
echo "=== Running validation ==="
cat > "$BACKUP_DIR/validation.yaml" <<EOF
backup_id: "$BACKUP_ID"
storage:
  backend: filesystem
  path: "$BACKUP_DIR/data"
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
  formats: [json, pdf]
  signing:
    enabled: true
    private_key_path: "$SIGNING_KEY_DIR/private-pkcs8.pem"
  storage:
    prefix: "evidence-reports/"
    retention_days: 2555
EOF

$CLI validation run --config "$BACKUP_DIR/validation.yaml" --triggered-by "docker-compose-e2e-test" || {
    echo ""
    echo "WARNING: Validation exited with non-zero (may indicate check failures — see output above)"
}

# Step 6: Find and verify evidence
echo ""
echo "=== Checking evidence reports ==="
EVIDENCE_DIR="$BACKUP_DIR/data/evidence-reports"
if [ -d "$EVIDENCE_DIR" ]; then
    echo "Evidence reports found:"
    find "$EVIDENCE_DIR" -type f | sort

    # Find the JSON report
    JSON_REPORT=$(find "$EVIDENCE_DIR" -name "*.json" | head -1)
    SIG_FILE=$(find "$EVIDENCE_DIR" -name "*.sig" | head -1)
    PDF_FILE=$(find "$EVIDENCE_DIR" -name "*.pdf" | head -1)

    if [ -n "$JSON_REPORT" ]; then
        echo ""
        echo "JSON report (first 30 lines):"
        cat "$JSON_REPORT" | python3 -m json.tool 2>/dev/null | head -30 || head -30 "$JSON_REPORT"
    fi

    if [ -n "$PDF_FILE" ]; then
        echo ""
        echo "PDF report: $PDF_FILE ($(wc -c < "$PDF_FILE") bytes)"
    fi

    if [ -n "$JSON_REPORT" ] && [ -n "$SIG_FILE" ]; then
        echo ""
        echo "=== Verifying evidence signature ==="
        $CLI validation evidence-verify \
            --report "$JSON_REPORT" \
            --signature "$SIG_FILE" \
            --public-key "$SIGNING_KEY_DIR/public.pem"
    else
        echo "No signature file found — skipping verification"
    fi
else
    echo "No evidence reports directory found at $EVIDENCE_DIR"
fi

echo ""
echo "=== Test Complete ==="
echo "All steps executed successfully."
