#!/usr/bin/env bash
# Release-binary smoke test for SASL/GSSAPI end-to-end.
#
# Drives the built `kafka-backup` binary through backup + restore
# against the Kerberized fixture at docker-compose-gssapi.yml, using
# the checked-in YAML configs. Catches regressions in:
#   - Arg/YAML parsing (clap + serde)
#   - Feature-gate wiring (`--features gssapi`)
#   - `populate_sasl_plugin` → runtime plugin handoff
#   - BackupEngine/RestoreEngine under a real SASL session
#
# This is complementary to `sasl_gssapi_backup_restore_roundtrip` in
# `crates/kafka-backup-core/tests/integration_suite/sasl_gssapi_tests.rs`
# — the Rust test drives the engines via their library API; this
# script drives the fully-built CLI binary the way an operator would.
#
# Usage (from repo root):
#   bash tests/sasl-gssapi-test-infra/run-cli-smoke.sh
#
# Prereqs:
#   - Fixture up: `docker compose -f tests/sasl-gssapi-test-infra/docker-compose-gssapi.yml up -d --wait`
#   - `/etc/hosts` has `127.0.0.1 kafka.test.local kdc.test.local`
#   - macOS: `brew install krb5`
#   - Linux: `apt-get install libkrb5-dev` / `dnf install krb5-devel`

set -euo pipefail

# Resolve repo root from this script's location so the script works
# regardless of the caller's cwd.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

# YAML configs use paths relative to repo root — the CLI inherits our cwd.
BACKUP_CFG="config/gssapi-backup.yaml"
RESTORE_CFG="config/gssapi-restore.yaml"
BACKUP_ID="gssapi-smoke"
STORAGE_DIR="/tmp/gssapi-backup-smoke"
SOURCE_TOPIC="smoke-topic"
RESTORED_TOPIC="smoke-topic-restored"
BROKER_CONTAINER="kafka-gssapi-test"
RECORD_COUNT=50

log() { printf '[cli-smoke] %s\n' "$*"; }
fail() { printf '[cli-smoke][FAIL] %s\n' "$*" >&2; exit 1; }

cleanup() {
    local code=$?
    log "cleanup (exit=$code)"
    # Best-effort: delete topics on the broker so reruns start clean.
    docker exec "$BROKER_CONTAINER" kafka-topics \
        --bootstrap-server localhost:9092 \
        --delete --topic "$SOURCE_TOPIC" 2>/dev/null || true
    docker exec "$BROKER_CONTAINER" kafka-topics \
        --bootstrap-server localhost:9092 \
        --delete --topic "$RESTORED_TOPIC" 2>/dev/null || true
    rm -rf "$STORAGE_DIR"
    exit "$code"
}
trap cleanup EXIT

# --- Preflight ---------------------------------------------------------
log "preflight"

grep -q 'kafka.test.local' /etc/hosts \
    || fail "/etc/hosts missing kafka.test.local (see tests/sasl-gssapi-test-infra/README.md)"

docker inspect -f '{{.State.Running}}' "$BROKER_CONTAINER" 2>/dev/null | grep -q true \
    || fail "fixture not running — start with: docker compose -f tests/sasl-gssapi-test-infra/docker-compose-gssapi.yml up -d --wait"

[[ -f tests/sasl-gssapi-test-infra/keytabs/client.keytab ]] \
    || fail "client.keytab missing — KDC init may have failed; check: docker compose logs kdc"

# krb5 headers for libgssapi-sys. Skip PKG_CONFIG export on Linux
# where the system pkg-config already finds krb5-config.
if command -v brew >/dev/null 2>&1; then
    KRB5_PREFIX="$(brew --prefix krb5 2>/dev/null || true)"
    if [[ -n "$KRB5_PREFIX" ]]; then
        export PKG_CONFIG_PATH="$KRB5_PREFIX/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
    fi
fi

# --- Build -------------------------------------------------------------
log "build --release --features gssapi"
cargo build --release --features gssapi -p kafka-backup-cli >/dev/null

BINARY="$REPO_ROOT/target/release/kafka-backup"
[[ -x "$BINARY" ]] || fail "built binary not executable: $BINARY"

# --- Seed source + destination topics ---------------------------------
# Both topics are created up front. Auto-create is enabled on the
# broker, but the restore's first produce RPC races topic auto-create
# and sees `Partition 0 not available` on the freshly-created topic
# before metadata propagates. Pre-creating avoids the race entirely.
log "creating source topic $SOURCE_TOPIC"
docker exec "$BROKER_CONTAINER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$SOURCE_TOPIC" \
    --partitions 1 --replication-factor 1 >/dev/null

log "creating destination topic $RESTORED_TOPIC"
docker exec "$BROKER_CONTAINER" kafka-topics \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$RESTORED_TOPIC" \
    --partitions 1 --replication-factor 1 >/dev/null

log "producing $RECORD_COUNT records"
# Internal PLAINTEXT listener means no SASL config needed for the
# producer/consumer CLIs inside the container — keeps the smoke
# script focused on the external binary's GSSAPI path, not JAAS
# wiring for the Java CLIs.
for i in $(seq 1 "$RECORD_COUNT"); do
    printf 'key-%d:value-%d\n' "$i" "$i"
done | docker exec -i "$BROKER_CONTAINER" kafka-console-producer \
    --bootstrap-server localhost:9092 \
    --topic "$SOURCE_TOPIC" \
    --property parse.key=true \
    --property key.separator=: >/dev/null

# --- Backup ------------------------------------------------------------
rm -rf "$STORAGE_DIR"
log "backup via release binary ($BACKUP_CFG)"
"$BINARY" backup --config "$BACKUP_CFG"

MANIFEST="$STORAGE_DIR/$BACKUP_ID/manifest.json"
[[ -f "$MANIFEST" ]] || fail "manifest not emitted at $MANIFEST"
log "manifest present: $MANIFEST"

# --- Restore -----------------------------------------------------------
log "restore via release binary ($RESTORE_CFG)"
"$BINARY" restore --config "$RESTORE_CFG"

# --- Verify ------------------------------------------------------------
log "verifying restored topic record count"
# --timeout-ms 10000 with --max-messages matching our produce count —
# consumer exits as soon as it's seen N messages or the timeout fires.
RESTORED=$(docker exec "$BROKER_CONTAINER" kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "$RESTORED_TOPIC" \
    --from-beginning \
    --timeout-ms 10000 \
    --max-messages "$RECORD_COUNT" 2>/dev/null | wc -l | tr -d ' ')

[[ "$RESTORED" == "$RECORD_COUNT" ]] \
    || fail "restored record count mismatch: got $RESTORED, expected $RECORD_COUNT"

log "PASS: restored $RESTORED / $RECORD_COUNT records via GSSAPI"
