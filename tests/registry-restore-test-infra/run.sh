#!/usr/bin/env bash
# Registry restore through the provider-neutral engine against real
# registries: Apicurio Registry (API v2 and v3) and Confluent Schema Registry.
#
# Native ID-preserving restore for both registries, then records produced
# with the released Confluent and Apicurio Java serializers, restored through
# the engine's record transform, and read back with the released
# deserializers pointed only at the destination registry: both directions
# between the two registries, same-registry ID allocation, 1:1 and
# repartitioned, with every supported framing.
#
#   tests/registry-restore-test-infra/run.sh
#   REGISTRY_APICURIO_SOURCE_VERSION=2.6.13.Final tests/registry-restore-test-infra/run.sh
#
# Versions come from REGISTRY_{CONFLUENT,APICURIO}_{SOURCE,TARGET}_VERSION
# (defaults: Confluent 8.3.1, Apicurio 3.3.3); matrix.py lists the pairs CI
# runs. Needs Docker, Java 17+ and Maven. Never touches an existing stack:
# this owns one Compose project and removes it on exit. Logs and the resolved
# image digests are kept in REGISTRY_EVIDENCE_DIR.
set -euo pipefail
REGISTRY_REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$REGISTRY_REPO_ROOT"
source "$REGISTRY_REPO_ROOT/tests/registry-restore-test-infra/lib/registry-maven.sh"
export REGISTRY_CONFLUENT_SOURCE_VERSION="${REGISTRY_CONFLUENT_SOURCE_VERSION:-8.3.1}"
export REGISTRY_CONFLUENT_TARGET_VERSION="${REGISTRY_CONFLUENT_TARGET_VERSION:-$REGISTRY_CONFLUENT_SOURCE_VERSION}"
export REGISTRY_APICURIO_SOURCE_VERSION="${REGISTRY_APICURIO_SOURCE_VERSION:-3.3.3}"
export REGISTRY_APICURIO_TARGET_VERSION="${REGISTRY_APICURIO_TARGET_VERSION:-$REGISTRY_APICURIO_SOURCE_VERSION}"
api_for() { case "$1" in 2.*) printf v2;; *) printf v3;; esac; }
image_for() { case "$1" in 2.*) printf 'quay.io/apicurio/apicurio-registry-mem:%s' "$1";; *) printf 'quay.io/apicurio/apicurio-registry:%s' "$1";; esac; }
export REGISTRY_APICURIO_SOURCE_API="$(api_for "$REGISTRY_APICURIO_SOURCE_VERSION")"
export REGISTRY_APICURIO_TARGET_API="$(api_for "$REGISTRY_APICURIO_TARGET_VERSION")"
export REGISTRY_APICURIO_SOURCE_IMAGE="$(image_for "$REGISTRY_APICURIO_SOURCE_VERSION")"
export REGISTRY_APICURIO_TARGET_IMAGE="$(image_for "$REGISTRY_APICURIO_TARGET_VERSION")"
export REGISTRY_APICURIO_API="$REGISTRY_APICURIO_SOURCE_API"
export REGISTRY_COMPOSE_FILE="$REGISTRY_REPO_ROOT/tests/registry-restore-test-infra/docker-compose.yml"
export REGISTRY_COMPOSE_PROJECT="${REGISTRY_COMPOSE_PROJECT:-registry-restore-test}"
export REGISTRY_EVIDENCE_DIR="${REGISTRY_EVIDENCE_DIR:-$REGISTRY_REPO_ROOT/target/registry-compatibility}"
mkdir -p "$REGISTRY_EVIDENCE_DIR"
compose() { docker compose -p "$REGISTRY_COMPOSE_PROJECT" -f "$REGISTRY_COMPOSE_FILE" "$@"; }
cleanup() {
  local registry_result=$?
  compose logs > "$REGISTRY_EVIDENCE_DIR/services.log" 2>&1 || true
  compose ps -a --format json > "$REGISTRY_EVIDENCE_DIR/service-state.json" 2>&1 || true
  local registry_ids
  registry_ids="$(compose ps -aq)"
  if [[ -n "$registry_ids" ]]; then
    # Capture state/resource evidence without registry credentials or env vars.
    while IFS= read -r registry_id; do
      docker inspect --format '{{json .State}}' "$registry_id" >> "$REGISTRY_EVIDENCE_DIR/container-state.jsonl" 2>&1 || true
      docker stats --no-stream --format '{{json .}}' "$registry_id" >> "$REGISTRY_EVIDENCE_DIR/container-resources.jsonl" 2>&1 || true
    done <<< "$registry_ids"
  fi
  compose down -v --remove-orphans >/dev/null || true
  return "$registry_result"
}
trap cleanup EXIT
compose up -d
export REGISTRY_KAFKA="localhost:${REGISTRY_KAFKA_PORT:-29492}"
export REGISTRY_CONFLUENT_SOURCE="http://localhost:${REGISTRY_CF_SOURCE_PORT:-29481}"
export REGISTRY_CONFLUENT_TARGET="http://localhost:${REGISTRY_CF_TARGET_PORT:-29482}"
export REGISTRY_APICURIO_SOURCE="http://localhost:${REGISTRY_AP_SOURCE_PORT:-29483}"
export REGISTRY_APICURIO_TARGET="http://localhost:${REGISTRY_AP_TARGET_PORT:-29484}"
wait_url() { local registry_deadline=$((SECONDS+180)); until curl -fsS "$1" >/dev/null 2>&1; do if ((SECONDS>=registry_deadline)); then printf 'Registry startup timeout: %s\n' "$1" >&2; return 1; fi; sleep 2; done; }
wait_url "$REGISTRY_CONFLUENT_SOURCE/subjects"
wait_url "$REGISTRY_CONFLUENT_TARGET/subjects"
wait_url "$REGISTRY_APICURIO_SOURCE/apis/registry/$REGISTRY_APICURIO_SOURCE_API/system/info"
wait_url "$REGISTRY_APICURIO_TARGET/apis/registry/$REGISTRY_APICURIO_TARGET_API/system/info"
python3 - <<'PY'
import datetime,json,os,pathlib,subprocess
ids=subprocess.check_output(['docker','compose','-p',os.environ['REGISTRY_COMPOSE_PROJECT'],'-f',os.environ['REGISTRY_COMPOSE_FILE'],'images','-q'],text=True).splitlines()
images=[]
for image in sorted(set(ids)):
 data=json.loads(subprocess.check_output(['docker','image','inspect',image],text=True))[0]
 images.append({k:data.get(k) for k in ['Id','RepoTags','RepoDigests','Architecture']})
data={'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'images':images,'versions':{k:v for k,v in os.environ.items() if k.startswith('REGISTRY_') and (k.endswith('_VERSION') or k.endswith('_API'))},'snappy_java_version':'1.1.10.7'}
pathlib.Path(os.environ['REGISTRY_EVIDENCE_DIR'],'evidence.json').write_text(json.dumps(data,indent=2)+'\n')
PY
# Exact source fixtures for schema-only contracts.
curl -fsS -X POST "$REGISTRY_CONFLUENT_SOURCE/subjects/restore-fixture-bytes/versions" -H 'Content-Type: application/json' --data '{"schema":"\"bytes\""}' >/dev/null
if [[ "$REGISTRY_APICURIO_SOURCE_API" == v2 ]]; then
  curl -fsS -X POST "$REGISTRY_APICURIO_SOURCE/apis/registry/v2/groups/default/artifacts" -H 'Content-Type: application/json' -H 'X-Registry-ArtifactType: AVRO' -H 'X-Registry-ArtifactId: fixture' --data '"string"' >/dev/null
else
  curl -fsS -X POST "$REGISTRY_APICURIO_SOURCE/apis/registry/v3/groups/default/artifacts" -H 'Content-Type: application/json' --data '{"artifactId":"fixture","artifactType":"AVRO","firstVersion":{"version":"1","content":{"content":"\"string\"","contentType":"application/json"}}}' >/dev/null
fi
run_case() { cargo test -p kafka-backup-core --test registry_restore_real "$1" -- --ignored --nocapture > "$REGISTRY_EVIDENCE_DIR/$2.log" 2>&1 || { cat "$REGISTRY_EVIDENCE_DIR/$2.log"; return 1; }; }
if python3 - <<'PYCODE'
import os,sys
v=lambda name:tuple(int(p) for p in os.environ[name].split('.')[:2])
sys.exit(0 if v('REGISTRY_CONFLUENT_SOURCE_VERSION')>=(8,1) and v('REGISTRY_CONFLUENT_TARGET_VERSION')<(8,1) else 1)
PYCODE
then export REGISTRY_EXPECT_GUID_REJECTION=1; fi
if [[ "$REGISTRY_CONFLUENT_TARGET_VERSION" == 6.2.0 ]]; then export REGISTRY_EXPECT_MODE_REJECTION=1; fi
run_case confluent_preserve_real confluent-preserve
unset REGISTRY_EXPECT_GUID_REJECTION REGISTRY_EXPECT_MODE_REJECTION
if [[ "$REGISTRY_APICURIO_SOURCE_API" != "$REGISTRY_APICURIO_TARGET_API" ]]; then export REGISTRY_EXPECT_ARCHIVE_REJECTION=1; fi
run_case apicurio_preserve_real apicurio-preserve
unset REGISTRY_EXPECT_ARCHIVE_REJECTION
# Build separate pinned SDK classpaths for each API generation. A single
# Maven classpath cannot load both v2 and v3 clients at once.
# The dependency plugin is named with its full coordinates and version: the
# short `dependency:` prefix needs a metadata lookup that can fail on its own.
build_oracle() {
  local registry_side="$1" registry_ap_version="$2" registry_cf_version="$3" registry_provider="$4"
  local registry_oracle_dir="$REGISTRY_EVIDENCE_DIR/oracle-$registry_side"
  mkdir -p "$registry_oracle_dir"
  cp -R tests/registry-serdes-oracle/src "$registry_oracle_dir/"
  cp tests/registry-serdes-oracle/pom.xml "$registry_oracle_dir/"
  local registry_maven_args=(-P"oracle-$registry_provider")
  if [[ "$registry_ap_version" == 2.* ]]; then registry_maven_args+=(-Dapicurio.v2=true); fi
  registry_maven "$REGISTRY_EVIDENCE_DIR/sdk-$registry_side.log" -q -f "$registry_oracle_dir/pom.xml" "${registry_maven_args[@]}" -Dapicurio.version="$registry_ap_version" -Dconfluent.version="$registry_cf_version" package org.apache.maven.plugins:maven-dependency-plugin:3.8.1:build-classpath -Dmdep.outputFile=classpath.txt -Dmdep.regenerateFile=true || return 1
  registry_maven "$REGISTRY_EVIDENCE_DIR/sdk-tree-$registry_side.log" -q -f "$registry_oracle_dir/pom.xml" "${registry_maven_args[@]}" -Dapicurio.version="$registry_ap_version" -Dconfluent.version="$registry_cf_version" org.apache.maven.plugins:maven-dependency-plugin:3.8.1:tree -DoutputFile="$REGISTRY_EVIDENCE_DIR/dependencies-$registry_side.txt" || return 1
  printf '%s:%s' "$registry_oracle_dir/target/classes" "$(cat "$registry_oracle_dir/classpath.txt")"
}
REGISTRY_ORACLE_CF_SOURCE_CLASSPATH="$(build_oracle source-confluent "$REGISTRY_APICURIO_SOURCE_VERSION" "$REGISTRY_CONFLUENT_SOURCE_VERSION" confluent)"
REGISTRY_ORACLE_AP_SOURCE_CLASSPATH="$(build_oracle source-apicurio "$REGISTRY_APICURIO_SOURCE_VERSION" "$REGISTRY_CONFLUENT_SOURCE_VERSION" apicurio)"
REGISTRY_ORACLE_CF_TARGET_CLASSPATH="$(build_oracle target-confluent "$REGISTRY_APICURIO_TARGET_VERSION" "$REGISTRY_CONFLUENT_TARGET_VERSION" confluent)"
REGISTRY_ORACLE_AP_TARGET_CLASSPATH="$(build_oracle target-apicurio "$REGISTRY_APICURIO_TARGET_VERSION" "$REGISTRY_CONFLUENT_TARGET_VERSION" apicurio)"
for registry_direction in confluent_to_apicurio apicurio_to_confluent; do
  export REGISTRY_CROSS_DIRECTION="$registry_direction"
  if [[ "$registry_direction" == confluent_to_apicurio ]]; then
    export REGISTRY_ORACLE_SOURCE_CLASSPATH="$REGISTRY_ORACLE_CF_SOURCE_CLASSPATH" REGISTRY_ORACLE_TARGET_CLASSPATH="$REGISTRY_ORACLE_AP_TARGET_CLASSPATH"
  else
    export REGISTRY_ORACLE_SOURCE_CLASSPATH="$REGISTRY_ORACLE_AP_SOURCE_CLASSPATH" REGISTRY_ORACLE_TARGET_CLASSPATH="$REGISTRY_ORACLE_CF_TARGET_CLASSPATH"
  fi
  export REGISTRY_COMPRESSION=gzip
  run_case independent_avro_sdk_cross_provider_real "$registry_direction"
  export REGISTRY_REPARTITION=1 REGISTRY_COMPRESSION=snappy
  run_case independent_avro_sdk_cross_provider_real "$registry_direction-repartition"
  unset REGISTRY_REPARTITION
  # Values use native headers (GUIDs where the SDK supports them), while keys
  # independently use numeric/global 4-byte prefixes. Older CF SDKs retain
  # numeric prefixes; unsupported GUID preservation is tested above.
  export REGISTRY_ORACLE_SERIALIZED_KEYS=true
  cf_header_mode() { if [[ "$1" == 8.* ]]; then printf headers; else printf prefix4; fi; }
  if [[ "$registry_direction" == confluent_to_apicurio ]]; then
    export REGISTRY_ORACLE_SOURCE_FRAMING="$(cf_header_mode "$REGISTRY_CONFLUENT_SOURCE_VERSION")" REGISTRY_ORACLE_TARGET_FRAMING=headers
  else
    export REGISTRY_ORACLE_SOURCE_FRAMING=headers REGISTRY_ORACLE_TARGET_FRAMING="$(cf_header_mode "$REGISTRY_CONFLUENT_TARGET_VERSION")"
  fi
  run_case independent_avro_sdk_cross_provider_real "$registry_direction-headers"
  export REGISTRY_ORACLE_ID_KIND=contentId REGISTRY_REPARTITION=1
  run_case independent_avro_sdk_cross_provider_real "$registry_direction-content-headers-repartition"
  unset REGISTRY_REPARTITION REGISTRY_ORACLE_ID_KIND
  export REGISTRY_ORACLE_SOURCE_FRAMING=prefix8 REGISTRY_ORACLE_TARGET_FRAMING=prefix8
  run_case independent_avro_sdk_cross_provider_real "$registry_direction-prefix8"
  unset REGISTRY_ORACLE_SOURCE_FRAMING REGISTRY_ORACLE_TARGET_FRAMING REGISTRY_ORACLE_SERIALIZED_KEYS
  # The released Confluent SDK is also the independent oracle for Apicurio's
  # ccompat API. These fixture images expose native content IDs in that API.
  export REGISTRY_ORACLE_SOURCE_FRAMING=compatible REGISTRY_ORACLE_TARGET_FRAMING=compatible REGISTRY_ORACLE_ID_KIND=contentId
  export REGISTRY_ORACLE_SOURCE_CLASSPATH="$REGISTRY_ORACLE_CF_SOURCE_CLASSPATH" REGISTRY_ORACLE_TARGET_CLASSPATH="$REGISTRY_ORACLE_CF_TARGET_CLASSPATH"
  run_case independent_avro_sdk_cross_provider_real "$registry_direction-compatible"
  unset REGISTRY_ORACLE_SOURCE_FRAMING REGISTRY_ORACLE_TARGET_FRAMING REGISTRY_ORACLE_ID_KIND
 done
export REGISTRY_CROSS_DIRECTION=confluent_to_apicurio REGISTRY_EXPECT_NATIVE_NESTED_REJECTION=1
export REGISTRY_ORACLE_SOURCE_CLASSPATH="$REGISTRY_ORACLE_CF_SOURCE_CLASSPATH" REGISTRY_ORACLE_TARGET_CLASSPATH="$REGISTRY_ORACLE_AP_TARGET_CLASSPATH"
run_case independent_avro_sdk_cross_provider_real native-apicurio-nested-protobuf-rejection
unset REGISTRY_EXPECT_NATIVE_NESTED_REJECTION
if [[ "${REGISTRY_MATRIX_SOURCE_PROVIDER:-}" == "${REGISTRY_MATRIX_TARGET_PROVIDER:-}" && -n "${REGISTRY_MATRIX_SOURCE_PROVIDER:-}" ]]; then
  export REGISTRY_SAME_PROVIDER="$REGISTRY_MATRIX_SOURCE_PROVIDER" REGISTRY_CROSS_DIRECTION=unused
  if [[ "$REGISTRY_SAME_PROVIDER" == confluent ]]; then
    export REGISTRY_ORACLE_SOURCE_CLASSPATH="$REGISTRY_ORACLE_CF_SOURCE_CLASSPATH" REGISTRY_ORACLE_TARGET_CLASSPATH="$REGISTRY_ORACLE_CF_TARGET_CLASSPATH"
  else
    export REGISTRY_ORACLE_SOURCE_CLASSPATH="$REGISTRY_ORACLE_AP_SOURCE_CLASSPATH" REGISTRY_ORACLE_TARGET_CLASSPATH="$REGISTRY_ORACLE_AP_TARGET_CLASSPATH"
  fi
  run_case independent_avro_sdk_cross_provider_real same-provider-rewrite
  export REGISTRY_REPARTITION=1
  run_case independent_avro_sdk_cross_provider_real same-provider-repartition
  unset REGISTRY_SAME_PROVIDER REGISTRY_REPARTITION
fi
printf 'Registry compatibility contracts passed. Evidence: %s\n' "$REGISTRY_EVIDENCE_DIR"
