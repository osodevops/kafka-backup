#!/usr/bin/env bash
# Seed an Apicurio Registry (API v3) with the fixture the backup tests in
# crates/kafka-backup-core/tests/apicurio_registry_real.rs expect: three
# groups, Avro / Protobuf / JSON Schema artifacts, several versions,
# references, and rules at the global, group and artifact level.
#
#   tests/registry-restore-test-infra/seed-apicurio.sh http://localhost:8085
set -euo pipefail
REGISTRY="${1:-http://localhost:8085}"
until curl -sf "$REGISTRY/apis/registry/v3/system/info" >/dev/null 2>&1; do sleep 2; done

echo 'Creating groups...'
curl -sf -X POST "$REGISTRY"/apis/registry/v3/groups \
  -H 'Content-Type: application/json' \
  -d '{"groupId": "payments", "description": "Payment domain schemas"}'
echo ''
curl -sf -X POST "$REGISTRY"/apis/registry/v3/groups \
  -H 'Content-Type: application/json' \
  -d '{"groupId": "orders", "description": "Order domain schemas"}'
echo ''
curl -sf -X POST "$REGISTRY"/apis/registry/v3/groups \
  -H 'Content-Type: application/json' \
  -d '{"groupId": "common", "description": "Shared types"}'
echo ''

echo 'Registering common/Address (Avro)...'
curl -sf -X POST "$REGISTRY"/apis/registry/v3/groups/common/artifacts \
  -H 'Content-Type: application/json' \
  -d '{"artifactId": "Address", "artifactType": "AVRO", "firstVersion": {"version": "1", "content": {"content": "{\"type\":\"record\",\"name\":\"Address\",\"namespace\":\"com.example.common\",\"fields\":[{\"name\":\"street\",\"type\":\"string\"},{\"name\":\"city\",\"type\":\"string\"},{\"name\":\"country\",\"type\":\"string\"}]}", "contentType": "application/json"}}}'
echo ''

echo 'Registering payments/PaymentEvent v1 (Avro)...'
curl -sf -X POST "$REGISTRY"/apis/registry/v3/groups/payments/artifacts \
  -H 'Content-Type: application/json' \
  -d '{"artifactId": "PaymentEvent", "artifactType": "AVRO", "firstVersion": {"version": "1", "content": {"content": "{\"type\":\"record\",\"name\":\"PaymentEvent\",\"namespace\":\"com.example.payments\",\"fields\":[{\"name\":\"payment_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\"}]}", "contentType": "application/json"}}}'
echo ''

echo 'Registering payments/PaymentEvent v2 (Avro, added timestamp)...'
curl -sf -X POST "$REGISTRY"/apis/registry/v3/groups/payments/artifacts/PaymentEvent/versions \
  -H 'Content-Type: application/json' \
  -d '{"version": "2", "content": {"content": "{\"type\":\"record\",\"name\":\"PaymentEvent\",\"namespace\":\"com.example.payments\",\"fields\":[{\"name\":\"payment_id\",\"type\":\"string\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"currency\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":[\"null\",\"long\"],\"default\":null}]}", "contentType": "application/json"}}'
echo ''

echo 'Registering orders/OrderCreated (Protobuf)...'
curl -sf -X POST "$REGISTRY"/apis/registry/v3/groups/orders/artifacts \
  -H 'Content-Type: application/json' \
  -d '{"artifactId": "OrderCreated", "artifactType": "PROTOBUF", "firstVersion": {"version": "1", "content": {"content": "syntax = \"proto3\";\npackage com.example.orders;\nmessage OrderCreated {\n  string order_id = 1;\n  string customer_id = 2;\n  double amount = 3;\n}", "contentType": "application/x-protobuf"}}}'
echo ''

echo 'Registering orders/OrderEvent (JSON Schema)...'
curl -sf -X POST "$REGISTRY"/apis/registry/v3/groups/orders/artifacts \
  -H 'Content-Type: application/json' \
  -d '{"artifactId": "OrderEvent", "artifactType": "JSON", "firstVersion": {"version": "1", "content": {"content": "{\"$$schema\":\"https://json-schema.org/draft/2020-12/schema\",\"type\":\"object\",\"properties\":{\"order_id\":{\"type\":\"string\"},\"status\":{\"type\":\"string\"}},\"required\":[\"order_id\"]}", "contentType": "application/json"}}}'
echo ''

echo 'Setting global COMPATIBILITY rule to BACKWARD...'
curl -sf -X POST "$REGISTRY"/apis/registry/v3/admin/rules \
  -H 'Content-Type: application/json' \
  -d '{"ruleType": "COMPATIBILITY", "config": "BACKWARD"}'
echo ''

echo 'Setting payments group VALIDITY rule to FULL...'
curl -sf -X POST "$REGISTRY"/apis/registry/v3/groups/payments/rules \
  -H 'Content-Type: application/json' \
  -d '{"ruleType": "VALIDITY", "config": "FULL"}'
echo ''

echo 'Apicurio test data registered.'
