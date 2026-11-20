#!/usr/bin/env bash
# Schema Registry acceptance test against Confluent Cloud (issue #262,
# scenarios 8-11). It is run by hand: it needs three non-production
# environments and costs a little while they exist.
#
#   8   back up an environment's registry and topics     API key; OAuth if oauth.env exists
#   9   restore both into an empty environment            official deserializers, target registry only
#   10  into_existing into a populated environment        dry run, strict refusal, rewrite_ids
#   11  version limit                                     max_schema_versions refuses before any change
#
# Each environment needs a Kafka cluster, the environment's Schema Registry
# and API keys for both. KBSR_CC_DIR holds one file per environment,
# `source.env`, `target.env` and `shared.env` (keep them private):
#
#   BOOTSTRAP=pkc-xxxxx.eu-west-1.aws.confluent.cloud:9092
#   SR_URL=https://psrc-xxxxx.eu-west-1.aws.confluent.cloud
#   LSRC=lsrc-xxxxx
#   KAFKA_KEY=...   KAFKA_SECRET=...   SR_KEY=...   SR_SECRET=...
#
# Optional `oauth.env` for the OAuth half of scenario 8 (an identity provider
# and an identity pool with DeveloperRead on the source registry's subjects):
#
#   OAUTH_TOKEN_URL=...  OAUTH_CLIENT_ID=...  OAUTH_CLIENT_SECRET=...
#   OAUTH_SCOPE=...      OAUTH_IDENTITY_POOL=pool-xxxxx
#
# THE REGISTRIES OF ALL THREE ENVIRONMENTS ARE WIPED and their `kbsr-*`
# topics deleted. Set KBSR_CC_CONFIRM=wipe to acknowledge that.
# KAFKA_BACKUP_BIN overrides the binary (default: target/debug/kafka-backup).
# Logs, configs (no secrets: they reference the environment) and the restore
# reports are left in KBSR_CC_OUT (default: a new temporary directory).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
DIR="${KBSR_CC_DIR:?set KBSR_CC_DIR to the directory with source.env, target.env and shared.env}"
[[ "${KBSR_CC_CONFIRM:-}" == wipe ]] || { echo "this wipes the three registries; set KBSR_CC_CONFIRM=wipe" >&2; exit 2; }
BIN="${KAFKA_BACKUP_BIN:-$ROOT/target/debug/kafka-backup}"
[[ -x "$BIN" ]] || (cd "$ROOT" && cargo build -p kafka-backup-cli)
docker build -q -t kbsr-e2e-clients:local "$HERE/e2e" >/dev/null

OUT="${KBSR_CC_OUT:-$(mktemp -d "${TMPDIR:-/tmp}/kbsr-ccloud.XXXXXX")}"
mkdir -p "$OUT"; chmod 777 "$OUT"; OUT="$(cd "$OUT" && pwd)"
rm -rf "$OUT/storage"
echo "output: $OUT" >&2
RESULTS=() FAILED=0

# Every environment's settings as <PREFIX>_<NAME>, for the kafka-backup
# configs (which reference them as ${SRC_SR_KEY} and so on).
load() { # role prefix
  local k v
  while IFS='=' read -r k v; do
    [[ -z "$k" || "$k" == \#* ]] || export "${2}_$k=$v"
  done <"$DIR/$1.env"
}
load source SRC; load target TGT; load shared SHR
OAUTH=0; [[ -f "$DIR/oauth.env" ]] && { load oauth OA; OAUTH=1; }

clients() { # role, clients.py arguments; @BOOTSTRAP and @SR stand for the role's endpoints
  local role="$1" p; shift
  case "$role" in source) p=SRC ;; target) p=TGT ;; shared) p=SHR ;; esac
  local b="${p}_BOOTSTRAP" s="${p}_SR_URL" kk="${p}_KAFKA_KEY" ks="${p}_KAFKA_SECRET" sk="${p}_SR_KEY" ss="${p}_SR_SECRET"
  local args=() a
  for a in "$@"; do a="${a//@BOOTSTRAP/${!b}}"; a="${a//@SR/${!s}}"; args+=("$a"); done
  KBSR_KAFKA_API_KEY="${!kk}" KBSR_KAFKA_API_SECRET="${!ks}" KBSR_SR_API_KEY="${!sk}" KBSR_SR_API_SECRET="${!ss}" \
    docker run --rm -e KBSR_KAFKA_API_KEY -e KBSR_KAFKA_API_SECRET -e KBSR_SR_API_KEY -e KBSR_SR_API_SECRET \
    -e KBSR_REPLICATION_FACTOR=3 -e PYTHONWARNINGS=ignore -v "$OUT:/work" kbsr-e2e-clients:local "${args[@]}" 2>>"$OUT/clients.log"
}

record() { # scenario status detail(json)
  RESULTS+=("{\"scenario\":\"$1\",\"status\":\"$2\",\"detail\":$3}")
  echo "{\"scenario\":\"$1\",\"status\":\"$2\",\"detail\":$3}"
  [[ "$2" == PASS || "$2" == SKIP ]] || FAILED=1
}

state() { clients "$1" registry --registry @SR >"$OUT/$2"; }

restore_config() { # file prefix extra-registry-restore-yaml
  cat >"$1" <<YAML
mode: restore
backup_id: kbsr-ccloud
target:
  bootstrap_servers: ["\${${2}_BOOTSTRAP}"]
  topics:
    include: ["kbsr-*"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: \${${2}_KAFKA_KEY}
    sasl_password: \${${2}_KAFKA_SECRET}
storage:
  backend: filesystem
  path: $OUT/storage
metrics:
  enabled: false
restore:
  create_topics: true
  strip_offset_headers: true
schema_registry:
  url: \${${2}_SR_URL}
  auth:
    type: basic
    username: \${${2}_SR_KEY}
    password: \${${2}_SR_SECRET}
$3
YAML
}

# --- clean start -------------------------------------------------------------
for role in source target shared; do
  clients "$role" delete-topics --bootstrap @BOOTSTRAP >/dev/null
  clients "$role" wipe --registry @SR
done

# --- source data: records from the official serializers, a subject-level
# compatibility and a soft-deleted subject --------------------------------------
clients source produce --bootstrap @BOOTSTRAP --registry @SR --expected /work/expected.json
sr() { curl -sS -u "$SRC_SR_KEY:$SRC_SR_SECRET" -H 'Content-Type: application/vnd.schemaregistry.v1+json' "$@" >/dev/null; }
sr -X PUT "$SRC_SR_URL/config/kbsr-json-value" -d '{"compatibility":"FULL"}'
sr -X POST "$SRC_SR_URL/subjects/kbsr-retired-value/versions" \
  -d '{"schema":"{\"type\":\"record\",\"name\":\"Retired\",\"namespace\":\"kbsr\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}"}'
sr -X DELETE "$SRC_SR_URL/subjects/kbsr-retired-value"
state source source-registry.json

# --- 8: backup ---------------------------------------------------------------
cat >"$OUT/backup.yaml" <<YAML
mode: backup
backup_id: kbsr-ccloud
source:
  bootstrap_servers: ["\${SRC_BOOTSTRAP}"]
  topics:
    include: ["kbsr-*"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: PLAIN
    sasl_username: \${SRC_KAFKA_KEY}
    sasl_password: \${SRC_KAFKA_SECRET}
storage:
  backend: filesystem
  path: $OUT/storage
metrics:
  enabled: false
backup:
  stop_at_current_offsets: true
schema_registry:
  url: \${SRC_SR_URL}
  auth:
    type: basic
    username: \${SRC_SR_KEY}
    password: \${SRC_SR_SECRET}
  backup:
    fail_on_partial: true
YAML
SNAP="$OUT/storage/kbsr-ccloud/schema-registry"
if "$BIN" backup --config "$OUT/backup.yaml" >"$OUT/backup.log" 2>&1 \
  && grep -q '"complete":true' "$SNAP/_status.json" && test -f "$SNAP/_manifest.json"; then
  record "8-backup-api-key" PASS "\"$(grep -o 'Schema Registry backup: [^>]*' "$OUT/backup.log" | head -1 | sed 's/ -$//')\""
else
  tail -20 "$OUT/backup.log" >&2
  record "8-backup-api-key" FAIL '"backup failed"'
  echo "summary: $(IFS=,; echo "[${RESULTS[*]}]")"; exit 1
fi

if (( OAUTH )); then
  cat >"$OUT/backup-oauth.yaml" <<YAML
mode: backup
backup_id: kbsr-ccloud-oauth
storage:
  backend: filesystem
  path: $OUT/storage
oauth:
  providers:
    idp:
      type: oidc_client_credentials
      token_endpoint_url: \${OA_OAUTH_TOKEN_URL}
      client_id: \${OA_OAUTH_CLIENT_ID}
      client_secret: \${OA_OAUTH_CLIENT_SECRET}
      scope: \${OA_OAUTH_SCOPE}
schema_registry:
  url: \${SRC_SR_URL}
  auth:
    type: oauth
    provider: idp
    target_sr_cluster: \${SRC_LSRC}
    identity_pool_id: \${OA_OAUTH_IDENTITY_POOL}
  backup:
    fail_on_partial: true
YAML
  if "$BIN" backup --schema-only --config "$OUT/backup-oauth.yaml" >"$OUT/backup-oauth.log" 2>&1 \
    && diff -r -x _manifest.json -x _status.json "$SNAP" "$OUT/storage/kbsr-ccloud-oauth/schema-registry" >/dev/null; then
    record "8-backup-oauth" PASS '"same snapshot as the API key backup"'
  else
    tail -20 "$OUT/backup-oauth.log" >&2
    record "8-backup-oauth" FAIL '"backup failed or the snapshot differs"'
  fi
else
  record "8-backup-oauth" SKIP '"no oauth.env"'
fi

# --- 9: restore into an empty environment --------------------------------------
restore_config "$OUT/restore-9.yaml" TGT ""
out=""
if "$BIN" validate-restore --config "$OUT/restore-9.yaml" >"$OUT/validate-9.log" 2>&1 \
  && "$BIN" restore --config "$OUT/restore-9.yaml" >"$OUT/restore-9.log" 2>&1 \
  && cp "$OUT/storage/kbsr-ccloud/restore/schema-registry-report.json" "$OUT/report-9.json" \
  && out="$(clients target verify --bootstrap @BOOTSTRAP --registry @SR --expected /work/expected.json --order partition --ids same)" \
  && state target target-registry.json && cmp -s "$OUT/source-registry.json" "$OUT/target-registry.json" \
  && "$BIN" restore --schema-only --config "$OUT/restore-9.yaml" >"$OUT/restore-9-again.log" 2>&1 \
  && grep -q "registered=0 skipped=7" "$OUT/restore-9-again.log"; then
  record "9-restore-into-empty-environment" PASS "$out"
else
  tail -20 "$OUT/validate-9.log" "$OUT/restore-9.log" >&2 || true
  record "9-restore-into-empty-environment" FAIL "${out:-\"restore, verification or the second run failed\"}"
fi

# --- 10: into_existing into a populated environment ----------------------------
# The environment's own schemas hold every ID the backup's records carry.
clients shared occupy --registry @SR --expected /work/expected.json >/dev/null
state shared shared-before.json
restore_config "$OUT/restore-10-strict.yaml" SHR $'  restore:\n    strategy: into_existing'
restore_config "$OUT/restore-10-rewrite.yaml" SHR $'  restore:\n    strategy: into_existing\n    rewrite_ids: true'
unchanged() { state shared "$1" && cmp -s "$OUT/shared-before.json" "$OUT/$1"; }

if ! "$BIN" validate-restore --schema-only --config "$OUT/restore-10-strict.yaml" >"$OUT/validate-10-strict.log" 2>&1 \
  && grep -q "6 in conflict" "$OUT/validate-10-strict.log" && unchanged shared-after-dry-run.json; then
  record "10-dry-run-reports-conflicts" PASS '"6 conflicts reported, registry unchanged"'
else
  record "10-dry-run-reports-conflicts" FAIL '"the dry run passed, reported other conflicts or changed the registry"'
fi

if ! "$BIN" restore --config "$OUT/restore-10-strict.yaml" >"$OUT/restore-10-strict.log" 2>&1 \
  && grep -q "nothing was changed" "$OUT/restore-10-strict.log" && unchanged shared-after-strict.json \
  && [[ "$(clients shared delete-topics --bootstrap @BOOTSTRAP)" == '{"deleted": []}' ]]; then
  record "10-strict-conflict-refused" PASS '"refused, registry unchanged, no topic created"'
else
  record "10-strict-conflict-refused" FAIL '"the strict restore was not refused cleanly"'
fi

out=""
if "$BIN" validate-restore --schema-only --config "$OUT/restore-10-rewrite.yaml" >"$OUT/validate-10-rewrite.log" 2>&1 \
  && unchanged shared-after-dry-run-2.json \
  && "$BIN" restore --config "$OUT/restore-10-rewrite.yaml" >"$OUT/restore-10-rewrite.log" 2>&1 \
  && cp "$OUT/storage/kbsr-ccloud/restore/schema-registry-report.json" "$OUT/report-10.json" \
  && out="$(clients shared verify --bootstrap @BOOTSTRAP --registry @SR --expected /work/expected.json --order partition --ids rewritten)"; then
  record "10-rewrite-ids" PASS "$out"
else
  tail -20 "$OUT/restore-10-rewrite.log" >&2 || true
  record "10-rewrite-ids" FAIL "${out:-\"restore or verification failed\"}"
fi

# --- 11: version limit ---------------------------------------------------------
# The shared registry now holds 13 versions; restoring three subjects under
# new names adds 4, over a limit of 15.
state shared shared-before.json
restore_config "$OUT/restore-11.yaml" SHR $'  restore:\n    strategy: into_existing\n    rewrite_ids: true\n    max_schema_versions: 15\n    subject_mapping:\n      kbsr-avro-value: kbsr2-avro-value\n      kbsr-json-value: kbsr2-json-value\n      kbsr-proto-value: kbsr2-proto-value'
if ! "$BIN" restore --schema-only --config "$OUT/restore-11.yaml" >"$OUT/restore-11.log" 2>&1 \
  && grep -q "Schema Registry capacity exceeded" "$OUT/restore-11.log" && unchanged shared-after-limit.json; then
  record "11-version-limit-refused" PASS "\"$(grep -o 'the target holds [^;]*' "$OUT/restore-11.log" | head -1)\""
else
  record "11-version-limit-refused" FAIL '"the restore was not refused before changing the registry"'
fi

rm -rf "$OUT/storage"
echo "summary: $(IFS=,; echo "[${RESULTS[*]}]")" | tee "$OUT/summary.json"
exit "$FAILED"
