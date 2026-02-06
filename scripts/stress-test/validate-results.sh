#!/bin/bash
# Validate stress test backup results for a given mode
# Usage: ./validate-results.sh <mode> <backup_dir> <log_file> <expected_topics> [max_duration_secs]

set -e

MODE=${1:?Usage: validate-results.sh <mode> <backup_dir> <log_file> <expected_topics> [max_duration_secs]}
BACKUP_DIR=${2:?Missing backup_dir}
LOG_FILE=${3:?Missing log_file}
EXPECTED_TOPICS=${4:?Missing expected_topics}
MAX_DURATION=${5:-120}

echo "----------------------------------------------"
echo "Validating: $MODE mode"
echo "----------------------------------------------"

PASS=true
DETAILS=""

# 1. Check backup directory exists
if [ ! -d "$BACKUP_DIR" ]; then
    DETAILS="$DETAILS\n  [FAIL] Backup directory does not exist: $BACKUP_DIR"
    PASS=false
else
    DETAILS="$DETAILS\n  [OK]   Backup directory exists"
fi

# 2. Count segment files
segment_count=0
if [ -d "$BACKUP_DIR" ]; then
    segment_count=$(find "$BACKUP_DIR" -name "*.zst" 2>/dev/null | wc -l | tr -d ' ')
fi

if [ "$segment_count" -gt 0 ]; then
    DETAILS="$DETAILS\n  [OK]   Segment files: $segment_count"
else
    DETAILS="$DETAILS\n  [FAIL] No segment files found"
    PASS=false
fi

# 3. Check manifest exists
backup_id="stress-test-${MODE}"
manifest_path="$BACKUP_DIR/${backup_id}/manifest.json"
if [ -f "$manifest_path" ]; then
    DETAILS="$DETAILS\n  [OK]   Manifest exists: $manifest_path"

    # 4. Count topics in manifest
    if command -v python3 > /dev/null 2>&1; then
        manifest_topics=$(python3 -c "
import json
with open('$manifest_path') as f:
    m = json.load(f)
    topics = m.get('topics', [])
    print(len(topics))
" 2>/dev/null || echo "0")
        if [ "$manifest_topics" -ge "$EXPECTED_TOPICS" ]; then
            DETAILS="$DETAILS\n  [OK]   Manifest topics: $manifest_topics (expected >= $EXPECTED_TOPICS)"
        else
            DETAILS="$DETAILS\n  [WARN] Manifest topics: $manifest_topics (expected >= $EXPECTED_TOPICS)"
        fi
    fi
else
    DETAILS="$DETAILS\n  [FAIL] Manifest not found: $manifest_path"
    PASS=false
fi

# 5. Check log file for errors
error_count=0
if [ -f "$LOG_FILE" ]; then
    error_count=$(grep -ci "ERROR\|FAIL\|panic" "$LOG_FILE" 2>/dev/null || echo "0")

    if [ "$error_count" -eq 0 ]; then
        DETAILS="$DETAILS\n  [OK]   No errors in log"
    else
        DETAILS="$DETAILS\n  [WARN] $error_count error lines in log"
        # Show first few errors
        DETAILS="$DETAILS\n         First errors:"
        while IFS= read -r line; do
            DETAILS="$DETAILS\n           $line"
        done < <(grep -i "ERROR\|FAIL\|panic" "$LOG_FILE" 2>/dev/null | head -5)
    fi
else
    DETAILS="$DETAILS\n  [WARN] Log file not found: $LOG_FILE"
fi

# 6. Check backed-up topic directories
if [ -d "$BACKUP_DIR/${backup_id}/topics" ]; then
    backed_up_topics=$(find "$BACKUP_DIR/${backup_id}/topics" -mindepth 1 -maxdepth 1 -type d 2>/dev/null | wc -l | tr -d ' ')
    if [ "$backed_up_topics" -ge "$EXPECTED_TOPICS" ]; then
        DETAILS="$DETAILS\n  [OK]   Backed up topic dirs: $backed_up_topics (expected >= $EXPECTED_TOPICS)"
    else
        DETAILS="$DETAILS\n  [WARN] Backed up topic dirs: $backed_up_topics (expected >= $EXPECTED_TOPICS)"
    fi
else
    DETAILS="$DETAILS\n  [WARN] No topics directory found"
fi

# 7. For continuous mode: check if multiple cycles completed
if [ "$MODE" = "continuous" ] && [ -f "$LOG_FILE" ]; then
    cycle_count=$(grep -c "Backup cycle\|Starting backup pass\|Completed pass\|Backing up .* topics" "$LOG_FILE" 2>/dev/null || echo "0")
    if [ "$cycle_count" -ge 2 ]; then
        DETAILS="$DETAILS\n  [OK]   Backup cycles detected: $cycle_count"
    else
        DETAILS="$DETAILS\n  [WARN] Only $cycle_count backup cycle(s) detected (expected >= 2)"
    fi
fi

# Print results
echo -e "$DETAILS"
echo ""

if [ "$PASS" = true ]; then
    echo "  Result: PASS"
else
    echo "  Result: FAIL"
fi

echo "----------------------------------------------"

# Return exit code
if [ "$PASS" = true ]; then
    exit 0
else
    exit 1
fi
