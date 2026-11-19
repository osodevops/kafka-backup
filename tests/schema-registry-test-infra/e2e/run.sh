#!/usr/bin/env bash
# Schema Registry end-to-end test with independent clients (issue #262,
# verification items 4, 5 and 7).
#
# Records are produced with the official Confluent serializers (Avro incl.
# top-level `bytes`, JSON Schema, Protobuf with nested messages; tombstones,
# null keys, headers; uncompressed and lz4 / zstd / gzip topics), backed up
# with the kafka-backup CLI together with the registry, restored into a
# second Kafka cluster and an empty registry, and read back with the
# official deserializers pointed only at the target registry.
#
#   A  1:1 restore, preserve_ids            partition order, IDs unchanged
#   B  repartitioning restore               per-key order, IDs unchanged
#   C  into_existing + rewrite_ids          records carry the new IDs      (7.0+)
#   D  SIGTERM mid-restore, then rerun      modes back, no duplicates      (7.0+)
#
# Needs the compose stack of this directory (docker-compose.yml) to be up.
# SR_VERSION tells the script what the registry can do (default 8.3.1).
# KAFKA_BACKUP_BIN overrides the binary (default: target/debug/kafka-backup,
# built if missing). Prints one JSON line per scenario and a summary.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
COMPOSE=(docker compose -f "$HERE/../docker-compose.yml")
SR_VERSION="${SR_VERSION:-8.3.1}"
SR_MAJOR="${SR_VERSION%%.*}"
BIN="${KAFKA_BACKUP_BIN:-$ROOT/target/debug/kafka-backup}"
if [[ ! -x "$BIN" ]]; then
  (cd "$ROOT" && cargo build -p kafka-backup-cli)
fi
docker build -q -t kbsr-e2e-clients:local "$HERE" >/dev/null

WORK="$(mktemp -d "${TMPDIR:-/tmp}/kbsr-e2e.XXXXXX")"
chmod 777 "$WORK"
# KBSR_KEEP=1 keeps the working directory (configs, logs, storage).
if [[ -z "${KBSR_KEEP:-}" ]]; then trap 'rm -rf "$WORK"' EXIT; else echo "work dir: $WORK" >&2; fi
RESULTS=()
FAILED=0

clients() { docker run --rm --network kbsr_default -e PYTHONWARNINGS=ignore -v "$WORK:/work" kbsr-e2e-clients:local "$@"; }
SRC_KAFKA=kafka-source:9094 TGT_KAFKA=kafka-target:9094
SRC_SR=http://sr-source:8081 TGT_SR=http://sr-target:8081 SHARED_SR=http://sr-shared:8081
TOPICS=(kbsr-avro kbsr-avro-bytes kbsr-json kbsr-proto)

record() { # scenario status detail
  RESULTS+=("{\"scenario\":\"$1\",\"schema_registry\":\"$SR_VERSION\",\"status\":\"$2\",\"detail\":$3}")
  echo "{\"scenario\":\"$1\",\"schema_registry\":\"$SR_VERSION\",\"status\":\"$2\",\"detail\":$3}"
  [[ "$2" == PASS || "$2" == SKIP ]] || FAILED=1
}

delete_topics() { # service
  local list
  list="$("${COMPOSE[@]}" exec -T "$1" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --list | grep '^kbsr-' || true)"
  for t in $list; do
    "${COMPOSE[@]}" exec -T "$1" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --delete --topic "$t" >/dev/null
  done
  for _ in $(seq 1 30); do
    "${COMPOSE[@]}" exec -T "$1" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --list | grep -q '^kbsr-' || return 0
    sleep 1
  done
  echo "topics on $1 were not deleted" >&2; return 1
}

reset_target() {
  delete_topics kafka-target
  clients wipe --registry "$TGT_SR"
}

restore_config() { # file, registry-restore yaml, restore yaml, registry port (default 28082)
  cat >"$1" <<YAML
mode: restore
backup_id: kbsr-e2e
target:
  bootstrap_servers: ["localhost:29192"]
  topics:
    include: ["kbsr-*"]
storage:
  backend: filesystem
  path: $WORK/storage
metrics:
  enabled: false
restore:
  create_topics: true
  strip_offset_headers: true
$3
schema_registry:
  url: http://localhost:${4:-28082}
$2
YAML
}

# --- source data ------------------------------------------------------------
delete_topics kafka-source
clients wipe --registry "$SRC_SR"
reset_target
clients produce --bootstrap "$SRC_KAFKA" --registry "$SRC_SR" --expected /work/expected.json

cat >"$WORK/backup.yaml" <<YAML
mode: backup
backup_id: kbsr-e2e
source:
  bootstrap_servers: ["localhost:29092"]
  topics:
    include: ["kbsr-*"]
storage:
  backend: filesystem
  path: $WORK/storage
metrics:
  enabled: false
backup:
  stop_at_current_offsets: true
schema_registry:
  url: http://localhost:28081
  backup:
    fail_on_partial: true
YAML
"$BIN" backup --config "$WORK/backup.yaml" >"$WORK/backup.log" 2>&1 || { cat "$WORK/backup.log"; exit 1; }
grep -q "Schema Registry backup:" "$WORK/backup.log" || { cat "$WORK/backup.log"; echo "no registry snapshot" >&2; exit 1; }
test -f "$WORK/storage/kbsr-e2e/schema-registry/_manifest.json"

# --- A: 1:1 restore into a fresh cluster and an empty registry ---------------
restore_config "$WORK/restore-a.yaml" "" ""
if "$BIN" validate-restore --config "$WORK/restore-a.yaml" >"$WORK/validate-a.log" 2>&1 \
  && "$BIN" restore --config "$WORK/restore-a.yaml" >"$WORK/restore-a.log" 2>&1 \
  && out="$(clients verify --bootstrap "$TGT_KAFKA" --registry "$TGT_SR" --expected /work/expected.json --order partition --ids same)"; then
  # The verifier must be able to fail: one changed value has to be caught.
  python3 - "$WORK/expected.json" "$WORK/tampered.json" <<'PY'
import json, sys
records = json.load(open(sys.argv[1]))
next(r for r in records if r["topic"] == "kbsr-proto" and not r["tombstone"])["value"]["shipment_id"] = "tampered"
json.dump(records, open(sys.argv[2], "w"))
PY
  if clients verify --bootstrap "$TGT_KAFKA" --registry "$TGT_SR" --expected /work/tampered.json --order partition --ids same >/dev/null 2>&1; then
    record "A-preserve-ids" FAIL '"the verifier accepted a tampered expectation"'
  else
    record "A-preserve-ids" PASS "$out"
  fi
else
  tail -30 "$WORK/validate-a.log" "$WORK/restore-a.log" 2>/dev/null >&2 || true
  record "A-preserve-ids" FAIL "${out:-\"restore or verification failed\"}"
fi

# --- B: repartitioning restore ----------------------------------------------
reset_target
REPART="  repartitioning:"
for t in "${TOPICS[@]}"; do REPART+=$'\n'"    $t:"$'\n'"      target_partitions: 4"; done
restore_config "$WORK/restore-b.yaml" "" "$REPART"
out=""
if "$BIN" restore --config "$WORK/restore-b.yaml" >"$WORK/restore-b.log" 2>&1 \
  && out="$(clients verify --bootstrap "$TGT_KAFKA" --registry "$TGT_SR" --expected /work/expected.json --order key --partitions 4 --ids same)"; then
  record "B-repartitioning" PASS "$out"
else
  tail -30 "$WORK/restore-b.log" >&2 || true
  record "B-repartitioning" FAIL "${out:-\"restore or verification failed\"}"
fi

# --- C: into_existing + rewrite_ids into a populated registry ----------------
# The shared registry never held this backup; its own schemas hold every ID
# the backup's records carry.
delete_topics kafka-target
clients wipe --registry "$SHARED_SR"
REWRITE=$'  restore:\n    strategy: into_existing\n    rewrite_ids: true'
restore_config "$WORK/restore-c.yaml" "$REWRITE" "" 28083
if (( SR_MAJOR >= 7 )); then
  # The target's own schemas hold every ID the backup uses.
  clients occupy --registry "$SHARED_SR" --expected /work/expected.json >/dev/null
  out=""
  if "$BIN" restore --config "$WORK/restore-c.yaml" >"$WORK/restore-c.log" 2>&1 \
    && grep -q "Schema IDs: .* changed" "$WORK/restore-c.log" \
    && out="$(clients verify --bootstrap "$TGT_KAFKA" --registry "$SHARED_SR" --expected /work/expected.json --order partition --ids rewritten)"; then
    record "C-rewrite-ids" PASS "$out"
  else
    tail -30 "$WORK/restore-c.log" >&2 || true
    record "C-rewrite-ids" FAIL "${out:-\"restore or verification failed\"}"
  fi
else
  # Confluent Platform 6.2 cannot delete mode overrides: refused before any
  # change, and no record is produced.
  clients occupy --registry "$SHARED_SR" --expected /work/expected.json >/dev/null
  if "$BIN" restore --config "$WORK/restore-c.yaml" >"$WORK/restore-c.log" 2>&1; then
    record "C-rewrite-ids" FAIL '"into_existing was accepted on a registry that cannot delete mode overrides"'
  elif grep -q "cannot delete mode overrides" "$WORK/restore-c.log" \
    && grep -q "The Kafka records were not restored" "$WORK/restore-c.log" \
    && ! "${COMPOSE[@]}" exec -T kafka-target /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --list | grep -q '^kbsr-'; then
    record "C-rewrite-ids" PASS '"refused before any change, as expected for Confluent Platform 6.2"'
  else
    tail -30 "$WORK/restore-c.log" >&2 || true
    record "C-rewrite-ids" FAIL '"unexpected failure"'
  fi
fi

# --- D: SIGTERM during a registry restore, then rerun with into_existing -----
delete_topics kafka-target
if (( SR_MAJOR >= 7 )); then
  clients wipe --registry "$SRC_SR"
  clients wipe --registry "$TGT_SR"
  clients seed --registry "$SRC_SR" --count 12
  sed -e 's/backup_id: kbsr-e2e/backup_id: kbsr-kill/' "$WORK/backup.yaml" >"$WORK/backup-d.yaml"
  "$BIN" backup --schema-only --config "$WORK/backup-d.yaml" >"$WORK/backup-d.log" 2>&1
  cat >"$WORK/restore-d.yaml" <<YAML
mode: restore
backup_id: kbsr-kill
storage:
  backend: filesystem
  path: $WORK/storage
schema_registry:
  url: http://localhost:28082
  connection:
    rate_limit_rps: 4
YAML
  "$BIN" restore --schema-only --config "$WORK/restore-d.yaml" >"$WORK/restore-d.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 100); do
    n="$(curl -s 'http://localhost:28082/subjects?deleted=true' | tr ',' '\n' | grep -c fixture || true)"
    (( n >= 3 )) && break
    sleep 0.2
  done
  kill -TERM "$PID"
  set +e; wait "$PID"; CODE=$?; set -e
  clients registry --registry "$TGT_SR" >"$WORK/partial.json"
  PARTIAL="$(python3 -c "
import json,sys
s=json.load(open('$WORK/partial.json'))
modes=[m['mode'] for m in s['subjects'].values() if m['mode']]
print(json.dumps({'exit_code': $CODE, 'subjects_after_kill': len(s['subjects']), 'global_mode': s['mode'], 'subject_modes_left': modes}))
ok = $CODE != 0 and 0 < len(s['subjects']) < 12 and s['mode'] == 'READWRITE' and not modes
sys.exit(0 if ok else 1)")" && KILLED=1 || KILLED=0
  REPORT="$WORK/storage/kbsr-kill/restore/schema-registry-report.json"
  grep -q '"cancelled": true' "$REPORT" && grep -q '"modes_rolled_back": true' "$REPORT" || KILLED=0

  sed -e 's/rate_limit_rps: 4/rate_limit_rps: 200/' "$WORK/restore-d.yaml" >"$WORK/restore-d2.yaml"
  printf '  restore:\n    strategy: into_existing\n' >>"$WORK/restore-d2.yaml"
  RESUMED=0
  if "$BIN" restore --schema-only --config "$WORK/restore-d2.yaml" >"$WORK/restore-d2.log" 2>&1; then
    clients registry --registry "$SRC_SR" >"$WORK/source-registry.json"
    clients registry --registry "$TGT_SR" >"$WORK/target-registry.json"
    cmp -s "$WORK/source-registry.json" "$WORK/target-registry.json" && RESUMED=1
  fi
  if (( KILLED == 1 && RESUMED == 1 )); then
    record "D-sigterm-and-resume" PASS "$PARTIAL"
  else
    tail -20 "$WORK/restore-d.log" "$WORK/restore-d2.log" >&2 || true
    record "D-sigterm-and-resume" FAIL "${PARTIAL:-\"no partial state\"}"
  fi
else
  record "D-sigterm-and-resume" SKIP '"needs subject-level IMPORT (Schema Registry 7.0+)"'
fi

echo "summary: $(IFS=,; echo "[${RESULTS[*]}]")"
exit "$FAILED"
