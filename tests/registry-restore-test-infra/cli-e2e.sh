#!/usr/bin/env bash
# Apicurio Registry backup and restore through the kafka-backup CLI against a
# real registry, for one Apicurio version:
#
#   tests/registry-restore-test-infra/cli-e2e.sh              # 3.3.3
#   tests/registry-restore-test-infra/cli-e2e.sh 2.6.13.Final # API v2
#
# Seeds a source registry, runs `backup --schema-only`, `validate-restore
# --schema-only` and `restore --schema-only` into an empty registry, and
# compares the two registries version by version (global ID, content ID,
# content, state). Then reruns the restore, which must change nothing.
# KAFKA_BACKUP_BIN overrides the binary (default: target/debug/kafka-backup).
set -euo pipefail

VERSION="${1:-3.3.3}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BIN="${KAFKA_BACKUP_BIN:-$ROOT/target/debug/kafka-backup}"
[[ -x "$BIN" ]] || (cd "$ROOT" && cargo build -p kafka-backup-cli)
case "$VERSION" in
  2.*) API=v2; export APICURIO_IMAGE="quay.io/apicurio/apicurio-registry-mem:$VERSION" ;;
  *)   API=v3; export APICURIO_IMAGE="quay.io/apicurio/apicurio-registry:$VERSION" ;;
esac
SOURCE=http://localhost:29485 TARGET=http://localhost:29486
COMPOSE=(docker compose -f "$HERE/docker-compose.apicurio-backup.yml")
WORK="$(mktemp -d "${TMPDIR:-/tmp}/kb-apicurio-cli.XXXXXX")"
cleanup() { "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true; [[ -n "${KB_KEEP:-}" ]] || rm -rf "$WORK"; }
trap cleanup EXIT
"${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d >/dev/null 2>&1
for url in "$SOURCE" "$TARGET"; do
  deadline=$((SECONDS+180))
  until curl -fsS "$url/apis/registry/$API/system/info" >/dev/null 2>&1; do
    ((SECONDS<deadline)) || { echo "registry at $url did not start" >&2; exit 1; }
    sleep 2
  done
done

# --- source fixture ----------------------------------------------------------
if [[ "$API" == v3 ]]; then
  "$HERE/seed-apicurio.sh" "$SOURCE" >/dev/null
else
  v2() { curl -fsS -X POST "$SOURCE/apis/registry/v2/groups/$1/artifacts$5" -H 'Content-Type: application/json' \
           -H "X-Registry-ArtifactType: $2" -H "X-Registry-ArtifactId: $3" --data "$4" >/dev/null; }
  v2 payments AVRO PaymentEvent '{"type":"record","name":"PaymentEvent","namespace":"com.example","fields":[{"name":"id","type":"string"}]}' ""
  curl -fsS -X POST "$SOURCE/apis/registry/v2/groups/payments/artifacts/PaymentEvent/versions" -H 'Content-Type: application/json' \
    --data '{"type":"record","name":"PaymentEvent","namespace":"com.example","fields":[{"name":"id","type":"string"},{"name":"note","type":["null","string"],"default":null}]}' >/dev/null
  v2 orders JSON OrderCreated '{"type":"object","properties":{"order_id":{"type":"string"}}}' ""
  v2 orders PROTOBUF Shipment 'syntax = "proto3"; package acme; message Shipment { string id = 1; }' ""
  curl -fsS -X POST "$SOURCE/apis/registry/v2/groups/payments/artifacts/PaymentEvent/rules" -H 'Content-Type: application/json' \
    --data '{"type":"VALIDITY","config":"FULL"}' >/dev/null
fi

config() { # file mode url
  cat >"$1" <<YAML
mode: $2
backup_id: apicurio-cli
storage:
  backend: filesystem
  path: $WORK/storage
apicurio_registry:
  url: $3
  api_version: $API
YAML
}
config "$WORK/backup.yaml" backup "$SOURCE"
config "$WORK/restore.yaml" restore "$TARGET"
# Apply the snapshot's global rules too (the default keeps the target's), so
# the comparison below covers them. It needs the empty target this test has.
printf 'registry_restore:\n  restore_global_settings: true\nrestore:\n  checkpoint_state: %s/checkpoint.json\n' "$WORK" >>"$WORK/restore.yaml"

state() { # url -> canonical JSON of every version
  python3 - "$1" "$API" <<'PY'
import json, sys, urllib.parse, urllib.request
base, api = sys.argv[1] + "/apis/registry/" + sys.argv[2], sys.argv[2]
def get(path, raw=False):
    with urllib.request.urlopen(base + path) as r:
        body = r.read()
    return body if raw else json.loads(body)
q = urllib.parse.quote
out = []
if api == "v3":
    groups = [g["groupId"] for g in get("/groups?limit=1000")["groups"]]
    for g in sorted(set(groups) | {"default"}):
        try: arts = get(f"/groups/{q(g)}/artifacts?limit=1000")["artifacts"]
        except Exception: continue
        for a in sorted(x["artifactId"] for x in arts):
            for v in get(f"/groups/{q(g)}/artifacts/{q(a)}/versions?limit=1000")["versions"]:
                content = get(f"/groups/{q(g)}/artifacts/{q(a)}/versions/{q(v['version'])}/content", raw=True)
                out.append([g, a, v["version"], v["globalId"], v["contentId"], v.get("state"), v.get("artifactType"), content.decode()])
            out.append([g, a, "rules", sorted(get(f"/groups/{q(g)}/artifacts/{q(a)}/rules"))])
        try: out.append([g, "group-rules", sorted(get(f"/groups/{q(g)}/rules"))])
        except Exception: pass
    out.append(["global-rules", sorted(get("/admin/rules"))])
else:
    for item in sorted(get("/search/artifacts?limit=1000")["artifacts"], key=lambda x: (x.get("groupId") or "default", x["id"])):
        g, a = item.get("groupId") or "default", item["id"]
        for v in get(f"/groups/{q(g)}/artifacts/{q(a)}/versions?limit=1000")["versions"]:
            content = get(f"/groups/{q(g)}/artifacts/{q(a)}/versions/{q(v['version'])}", raw=True)
            out.append([g, a, v["version"], v["globalId"], v["contentId"], v.get("state"), v.get("type"), content.decode()])
        out.append([g, a, "rules", sorted(get(f"/groups/{q(g)}/artifacts/{q(a)}/rules"))])
    out.append(["global-rules", sorted(get("/admin/rules"))])
print(json.dumps(out, sort_keys=True))
PY
}

fail() { echo "{\"apicurio\":\"$VERSION\",\"api\":\"$API\",\"status\":\"FAIL\",\"detail\":\"$1\"}"; tail -25 "$WORK"/*.log >&2 || true; exit 1; }

"$BIN" backup --schema-only --config "$WORK/backup.yaml" >"$WORK/backup.log" 2>&1 || fail "backup --schema-only failed"
grep -q "Apicurio Registry backup:" "$WORK/backup.log" || fail "no Apicurio snapshot line"
test -f "$WORK/storage/apicurio-cli/apicurio-registry/_manifest.json" || fail "no manifest"
grep -q '"complete":true' "$WORK/storage/apicurio-cli/apicurio-registry/_status.json" || fail "snapshot not marked complete"

"$BIN" validate-restore --schema-only --format json --config "$WORK/restore.yaml" >"$WORK/validate.json" 2>"$WORK/validate.log" || fail "validate-restore failed"
python3 -c "
import json,sys
v=json.load(open('$WORK/validate.json'))
plan=v['registry_restore'][0]
sys.exit(0 if v['valid'] and plan['destination']=='apicurio' and plan['registrations'] and not plan['conflicts'] else 1)" || fail "unexpected validation plan"
BEFORE="$(state "$TARGET")"

"$BIN" restore --schema-only --config "$WORK/restore.yaml" >"$WORK/restore.log" 2>&1 || fail "restore --schema-only failed"
state "$SOURCE" >"$WORK/source.json"; state "$TARGET" >"$WORK/target.json"
[[ "$BEFORE" != "$(cat "$WORK/target.json")" ]] || fail "the restore changed nothing"
cmp -s "$WORK/source.json" "$WORK/target.json" || fail "source and target registries differ after the restore"

# A rerun reconciles from the journal and changes nothing.
"$BIN" restore --schema-only --config "$WORK/restore.yaml" >"$WORK/restore-2.log" 2>&1 || fail "the second restore failed"
state "$TARGET" >"$WORK/target-2.json"
cmp -s "$WORK/target.json" "$WORK/target-2.json" || fail "the second restore changed the target"

VERSIONS="$(python3 -c "import json; print(sum(1 for r in json.load(open('$WORK/target.json')) if len(r)==8))")"
echo "{\"apicurio\":\"$VERSION\",\"api\":\"$API\",\"status\":\"PASS\",\"versions_compared\":$VERSIONS}"
