#!/usr/bin/env bash
# Run the real-registry tests and the end-to-end test for every supported
# Confluent Schema Registry version (issue #262, verification item 6).
#
#   tests/schema-registry-test-infra/matrix.sh                # all versions
#   tests/schema-registry-test-infra/matrix.sh 7.9.2 8.3.1    # some
#
# Each version gets a fresh stack (volumes removed). Results: one line per
# version, the e2e JSON lines in $OUT (default: /tmp/kbsr-matrix).
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
VERSIONS=("$@")
(( ${#VERSIONS[@]} )) || VERSIONS=(6.2.0 7.6.0 7.9.2 8.1.1 8.3.1)
OUT="${OUT:-/tmp/kbsr-matrix}"
mkdir -p "$OUT"

# Build once, and run copies, so the matrix is not disturbed by other builds.
cd "$ROOT"
cargo build -p kafka-backup-cli || exit 1
TEST_BIN="$(cargo test -p kafka-backup-core --test schema_registry_real --no-run --message-format=json 2>/dev/null \
  | python3 -c 'import sys,json
for line in sys.stdin:
    try: m=json.loads(line)
    except ValueError: continue
    if m.get("executable") and m.get("target",{}).get("name")=="schema_registry_real": print(m["executable"])' | tail -1)"
[[ -x "$TEST_BIN" ]] || { echo "cannot build the schema_registry_real tests" >&2; exit 1; }
cp "$TEST_BIN" "$OUT/schema_registry_real"
cp "$ROOT/target/debug/kafka-backup" "$OUT/kafka-backup"

STATUS=0
for version in "${VERSIONS[@]}"; do
  export SR_VERSION="$version"
  # A stored ruleSet needs the rule-set extension, which 8.x ships.
  if [[ "${version%%.*}" -ge 8 ]]; then
    export SR_RESOURCE_EXTENSION=io.confluent.kafka.schemaregistry.rulehandler.RuleSetResourceExtension
  else
    export SR_RESOURCE_EXTENSION=
  fi
  docker compose -f "$HERE/docker-compose.yml" down -v >/dev/null 2>&1
  if ! docker compose -f "$HERE/docker-compose.yml" up -d --wait >"$OUT/up-$version.log" 2>&1; then
    echo "$version: stack did not start (see $OUT/up-$version.log)"; STATUS=1; continue
  fi
  "$OUT/schema_registry_real" --ignored --test-threads=1 >"$OUT/real-$version.log" 2>&1; REAL=$?
  KAFKA_BACKUP_BIN="$OUT/kafka-backup" "$HERE/e2e/run.sh" >"$OUT/e2e-$version.log" 2>&1; E2E=$?
  grep '^{"scenario' "$OUT/e2e-$version.log" >"$OUT/e2e-$version.jsonl"
  echo "$version: real-registry tests $([[ $REAL == 0 ]] && echo PASS || echo FAIL) ($(grep -c '\.\.\. ok' "$OUT/real-$version.log") ok, $(grep -c '\.\.\. FAILED' "$OUT/real-$version.log") failed); e2e $([[ $E2E == 0 ]] && echo PASS || echo FAIL) ($(sed -E 's/.*"scenario":"([^"]+)".*"status":"([^"]+)".*/\1=\2/' "$OUT/e2e-$version.jsonl" | tr '\n' ' '))"
  (( REAL == 0 && E2E == 0 )) || STATUS=1
done
docker compose -f "$HERE/docker-compose.yml" down -v >/dev/null 2>&1
exit "$STATUS"
