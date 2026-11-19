"""Independent producers and consumers for the Schema Registry end-to-end test.

Everything here uses the official Confluent client (confluent-kafka) and its
serializers; nothing is shared with kafka-backup.

  produce   write the data set to the source cluster through the source
            registry and record what was sent (expected.json)
  verify    read the target cluster with deserializers that know only the
            target registry and compare with expected.json
  wipe      hard-delete everything in a registry
  occupy    give the schema IDs the records carry to other schemas in a registry
  seed      register a plain multi-version fixture (no topics)
  registry  print a registry's subjects, versions, IDs and modes as JSON
"""
import argparse
import json
import struct
import sys
import time
from collections import defaultdict
from urllib.parse import quote

import requests
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.schema_registry.protobuf import ProtobufDeserializer, ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from google.protobuf import descriptor_pb2, descriptor_pool, json_format, message_factory

HEADERS = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

ORDER_V1 = json.dumps({
    "type": "record", "name": "Order", "namespace": "kbsr.e2e",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "amount_minor", "type": "long"},
        {"name": "address", "type": {"type": "record", "name": "Address", "fields": [
            {"name": "street", "type": "string"}, {"name": "city", "type": "string"}]}},
    ],
})
ORDER_V2 = json.dumps({
    "type": "record", "name": "Order", "namespace": "kbsr.e2e",
    "fields": [
        {"name": "order_id", "type": "string"},
        {"name": "amount_minor", "type": "long"},
        {"name": "address", "type": {"type": "record", "name": "Address", "fields": [
            {"name": "street", "type": "string"}, {"name": "city", "type": "string"}]}},
        {"name": "status", "type": ["null", "string"], "default": None},
    ],
})
USER = json.dumps({
    "$schema": "http://json-schema.org/draft-07/schema#", "title": "User", "type": "object",
    "properties": {"user_id": {"type": "string"}, "email": {"type": "string"},
                   "tags": {"type": "array", "items": {"type": "string"}}},
    "required": ["user_id", "email"],
})

# Topic -> partitions and producer compression (a compressed source topic).
TOPICS = {
    "kbsr-avro": {"partitions": 3, "compression": "none"},
    "kbsr-avro-bytes": {"partitions": 1, "compression": "lz4"},
    "kbsr-json": {"partitions": 2, "compression": "zstd"},
    "kbsr-proto": {"partitions": 2, "compression": "gzip"},
}


def shipment_class():
    """Protobuf messages built at run time: `Shipment` is the second message
    of the file (message index [1]) and holds nested `Parcel` messages."""
    f = descriptor_pb2.FileDescriptorProto(name="kbsr/e2e.proto", package="kbsr.e2e", syntax="proto3")
    key = f.message_type.add(name="ShipmentKey")
    key.field.add(name="shipment_id", number=1, type=9, label=1)
    shipment = f.message_type.add(name="Shipment")
    parcel = shipment.nested_type.add(name="Parcel")
    parcel.field.add(name="sku", number=1, type=9, label=1)
    parcel.field.add(name="grams", number=2, type=3, label=1)
    dims = parcel.nested_type.add(name="Dimensions")
    dims.field.add(name="width_mm", number=1, type=5, label=1)
    dims.field.add(name="height_mm", number=2, type=5, label=1)
    parcel.field.add(name="dimensions", number=3, type=11, label=1,
                     type_name=".kbsr.e2e.Shipment.Parcel.Dimensions")
    shipment.field.add(name="shipment_id", number=1, type=9, label=1)
    shipment.field.add(name="parcels", number=2, type=11, label=3, type_name=".kbsr.e2e.Shipment.Parcel")
    shipment.field.add(name="express", number=3, type=8, label=1)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(f)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("kbsr.e2e.Shipment"))


def hexs(b):
    return None if b is None else b.hex()


def canonical(value):
    if isinstance(value, bytes):
        return {"bytes": value.hex()}
    return value


def wire_id(payload):
    """The schema ID of a Confluent wire-format payload, or None."""
    if payload is None or len(payload) < 5 or payload[0] != 0:
        return None
    return struct.unpack(">I", payload[1:5])[0]


# --------------------------------------------------------------------------
# produce
# --------------------------------------------------------------------------

def data_set(Shipment):
    """(topic, key, value, headers, kind). kind picks the value serializer."""
    records = []
    for i in range(60):
        key = None if i % 7 == 0 else f"order-{i % 9}"
        headers = [("trace-id", f"t-{i}".encode()), ("empty", b""), ("trace-id", b"dup")] if i % 3 == 0 else []
        if i % 11 == 10 and key is not None:
            records.append(("kbsr-avro", key, None, headers, "avro-v1"))  # tombstone
            continue
        value = {"order_id": f"o-{i}", "amount_minor": i * 100 - 50,
                 "address": {"street": f"{i} Main St", "city": "Zürich" if i % 2 else "Kraków"}}
        if i < 30:
            records.append(("kbsr-avro", key, value, headers, "avro-v1"))
        else:
            value["status"] = None if i % 4 == 0 else "paid"
            records.append(("kbsr-avro", key, value, headers, "avro-v2"))
    for i in range(25):
        payload = bytes([i, 0, 255, i * 3 % 256]) * (i + 1)
        records.append(("kbsr-avro-bytes", f"blob-{i}", payload, [("n", str(i).encode())], "avro-bytes"))
    records.append(("kbsr-avro-bytes", "blob-empty", b"", [], "avro-bytes"))
    for i in range(40):
        key = None if i % 10 == 0 else f"user-{i % 6}"
        if i == 33:
            records.append(("kbsr-json", key, None, [], "json"))
            continue
        value = {"user_id": f"u-{i}", "email": f"u{i}@example.com", "tags": [f"t{j}" for j in range(i % 4)]}
        records.append(("kbsr-json", key, value, [("source", b"e2e")] if i % 2 else [], "json"))
    for i in range(40):
        key = f"shipment-{i % 5}"
        if i == 17:
            records.append(("kbsr-proto", key, None, [], "proto"))
            continue
        msg = Shipment(shipment_id=f"s-{i}", express=i % 2 == 0)
        for j in range(i % 3 + 1):
            p = msg.parcels.add(sku=f"sku-{i}-{j}", grams=1000 * j + i)
            p.dimensions.width_mm = 10 * j
            p.dimensions.height_mm = i
        records.append(("kbsr-proto", key, msg, [("codec", b"gzip")], "proto"))
    return records


def cmd_produce(a):
    admin = AdminClient({"bootstrap.servers": a.bootstrap})
    existing = set(admin.list_topics(timeout=20).topics)
    new = [NewTopic(t, num_partitions=c["partitions"], replication_factor=1)
           for t, c in TOPICS.items() if t not in existing]
    for future in admin.create_topics(new).values():
        future.result(30)
    time.sleep(1)

    sr = SchemaRegistryClient({"url": a.registry})
    Shipment = shipment_class()
    no_auto = {"auto.register.schemas": True}
    serializers = {
        "avro-v1": AvroSerializer(sr, ORDER_V1, conf=no_auto),
        "avro-v2": AvroSerializer(sr, ORDER_V2, conf=no_auto),
        "avro-bytes": AvroSerializer(sr, '"bytes"', conf=no_auto),
        "json": JSONSerializer(USER, sr, conf=no_auto),
        "proto": ProtobufSerializer(Shipment, sr, {"use.deprecated.format": False}),
    }
    # The blob topic's keys go through the registry too (Avro "string").
    key_serializer = AvroSerializer(sr, '"string"', conf=no_auto)

    producers = {t: Producer({"bootstrap.servers": a.bootstrap, "compression.type": c["compression"],
                              "enable.idempotence": True, "linger.ms": 5}) for t, c in TOPICS.items()}
    expected, errors = [], []

    def delivered(entry):
        def callback(err, msg):
            if err is not None:
                errors.append(str(err))
                return
            entry["partition"], entry["offset"] = msg.partition(), msg.offset()
            entry["value_schema_id"] = wire_id(msg.value())
            entry["key_schema_id"] = wire_id(msg.key()) if entry["topic"] == "kbsr-avro-bytes" else None
        return callback

    for topic, key, value, headers, kind in data_set(Shipment):
        if topic == "kbsr-avro-bytes":
            raw_key = key_serializer(key, SerializationContext(topic, MessageField.KEY))
        else:
            raw_key = None if key is None else key.encode()
        raw_value = None if value is None else serializers[kind](value, SerializationContext(topic, MessageField.VALUE))
        logical = json_format.MessageToDict(value, preserving_proto_field_name=True) if kind == "proto" and value is not None else canonical(value)
        entry = {"topic": topic, "key": key, "value": logical, "tombstone": value is None,
                 "headers": [[k, hexs(v)] for k, v in headers]}
        expected.append(entry)
        producers[topic].produce(topic, key=raw_key, value=raw_value, headers=headers or None,
                                 on_delivery=delivered(entry))
        producers[topic].poll(0)
    for p in producers.values():
        p.flush(60)
    if errors or any("offset" not in e for e in expected):
        print(f"produce failed: {errors[:3]}", file=sys.stderr)
        return 1
    json.dump(expected, open(a.expected, "w"), indent=1, ensure_ascii=False)
    ids = sorted({e["value_schema_id"] for e in expected if e["value_schema_id"] is not None})
    print(f"produced {len(expected)} records to {len(TOPICS)} topics; value schema IDs {ids}")
    return 0


# --------------------------------------------------------------------------
# verify
# --------------------------------------------------------------------------

def consume_all(bootstrap, topics):
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": f"kbsr-verify-{time.time_ns()}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest", "isolation.level": "read_committed"})
    meta = c.list_topics(timeout=20)
    parts = []
    for t in topics:
        if t not in meta.topics or meta.topics[t].error is not None:
            raise SystemExit(f"topic {t} is missing in the target cluster")
        parts += [TopicPartition(t, p) for p in meta.topics[t].partitions]
    ends = {}
    for tp in parts:
        low, high = c.get_watermark_offsets(tp, timeout=20)
        tp.offset = low
        ends[(tp.topic, tp.partition)] = high
    c.assign(parts)
    out, done, deadline = [], set(k for k, v in ends.items() if v == 0), time.time() + 120
    while len(done) < len(ends) and time.time() < deadline:
        msg = c.poll(1.0)
        if msg is None:
            for tp in c.position(parts):
                if tp.offset >= ends[(tp.topic, tp.partition)]:
                    done.add((tp.topic, tp.partition))
            continue
        if msg.error():
            raise SystemExit(f"consume error: {msg.error()}")
        out.append(msg)
        if msg.offset() + 1 >= ends[(msg.topic(), msg.partition())]:
            done.add((msg.topic(), msg.partition()))
    c.close()
    if len(done) < len(ends):
        raise SystemExit("timed out reading the target topics")
    return out, {t: len(meta.topics[t].partitions) for t in topics}


def cmd_verify(a):
    expected = json.load(open(a.expected))
    mapping = dict(m.split("=", 1) for m in a.topic_map)
    target_topics = [mapping.get(t, t) for t in TOPICS]
    back = {v: k for k, v in mapping.items()}

    # Deserializers know the target registry only.
    sr = SchemaRegistryClient({"url": a.registry})
    Shipment = shipment_class()
    avro, json_d = AvroDeserializer(sr), JSONDeserializer(None, schema_registry_client=sr)
    proto = ProtobufDeserializer(Shipment, {"use.deprecated.format": False})
    key_avro = AvroDeserializer(sr)

    messages, partitions = consume_all(a.bootstrap, target_topics)
    actual, failures = [], []
    for m in messages:
        topic = back.get(m.topic(), m.topic())
        ctx = SerializationContext(m.topic(), MessageField.VALUE)
        try:
            if m.value() is None:
                value = None
            elif topic == "kbsr-json":
                value = json_d(m.value(), ctx)
            elif topic == "kbsr-proto":
                value = json_format.MessageToDict(proto(m.value(), ctx), preserving_proto_field_name=True)
            else:
                value = canonical(avro(m.value(), ctx))
            if topic == "kbsr-avro-bytes":
                key = key_avro(m.key(), SerializationContext(m.topic(), MessageField.KEY))
            else:
                key = None if m.key() is None else m.key().decode()
        except Exception as e:  # noqa: BLE001 - every decode failure is a finding
            failures.append(f"{m.topic()}[{m.partition()}]@{m.offset()}: {type(e).__name__}: {e}")
            continue
        headers = [[k, hexs(v)] for k, v in (m.headers() or [])]
        actual.append({"topic": topic, "partition": m.partition(), "offset": m.offset(), "key": key,
                       "value": value, "tombstone": m.value() is None, "headers": headers,
                       "value_schema_id": wire_id(m.value()),
                       "key_schema_id": wire_id(m.key()) if topic == "kbsr-avro-bytes" else None})

    def logical(e):
        return json.dumps([e["key"], e["value"], e["tombstone"], e["headers"]], sort_keys=True, ensure_ascii=False)

    def by(entries, field):
        groups = defaultdict(list)
        for e in sorted(entries, key=lambda e: (e["topic"], e["partition"], e["offset"])):
            groups[(e["topic"], field(e))].append(logical(e))
        return groups

    if len(actual) + len(failures) != len(expected):
        failures.append(f"{len(expected)} records produced, {len(actual) + len(failures)} restored")
    if a.order == "partition":
        # 1:1 restore: every partition holds the same records in the same order.
        want, got = by(expected, lambda e: e["partition"]), by(actual, lambda e: e["partition"])
    else:
        # Repartitioned: order holds per key; keyless records compare as a set.
        want, got = by(expected, lambda e: e["key"]), by(actual, lambda e: e["key"])
        for groups in (want, got):
            for k in [k for k in groups if k[1] is None]:
                groups[k] = sorted(groups[k])
        for t in TOPICS:
            if partitions[mapping.get(t, t)] != a.partitions:
                failures.append(f"{mapping.get(t, t)} has {partitions[mapping.get(t, t)]} partitions, expected {a.partitions}")
    for k in sorted(set(want) | set(got), key=str):
        if want.get(k) != got.get(k):
            failures.append(f"{k}: {len(want.get(k, []))} expected, {len(got.get(k, []))} restored, "
                            f"first difference at index "
                            f"{next((i for i, (x, y) in enumerate(zip(want.get(k, []), got.get(k, []))) if x != y), 'length')}")

    # Schema IDs carried by the restored records.
    source_ids = sorted({e[f] for e in expected for f in ("value_schema_id", "key_schema_id") if e[f] is not None})
    target_ids = sorted({e[f] for e in actual for f in ("value_schema_id", "key_schema_id") if e[f] is not None})
    if a.ids == "same" and source_ids != target_ids:
        failures.append(f"schema IDs changed: produced {source_ids}, restored {target_ids}")
    if a.ids == "rewritten":
        kept = sorted(set(source_ids) & set(target_ids))
        if kept:
            failures.append(f"schema IDs {kept} were expected to be rewritten (produced {source_ids}, restored {target_ids})")
    result = {"records": len(actual), "order": a.order, "source_schema_ids": source_ids,
              "restored_schema_ids": target_ids, "failures": failures[:20], "passed": not failures}
    print(json.dumps(result, ensure_ascii=False))
    return 0 if not failures else 1


# --------------------------------------------------------------------------
# registry helpers
# --------------------------------------------------------------------------

def call(registry, method, path, body=None, ok=(200,)):
    r = requests.request(method, registry + path, headers=HEADERS, data=None if body is None else json.dumps(body), timeout=30)
    if r.status_code not in ok:
        raise SystemExit(f"{method} {registry}{path}: {r.status_code} {r.text}")
    return r.json() if r.text else None


def enc(subject):
    return quote(subject, safe="")


def cmd_wipe(a):
    call(a.registry, "PUT", "/mode?force=true", {"mode": "READWRITE"})
    for _ in range(10):
        subjects = call(a.registry, "GET", "/subjects?deleted=true")
        if not subjects:
            break
        for s in subjects:
            requests.put(f"{a.registry}/mode/{enc(s)}?force=true", headers=HEADERS, data=json.dumps({"mode": "READWRITE"}), timeout=30)
            requests.delete(f"{a.registry}/subjects/{enc(s)}", timeout=30)
            requests.delete(f"{a.registry}/subjects/{enc(s)}?permanent=true", timeout=30)
            requests.delete(f"{a.registry}/mode/{enc(s)}", timeout=30)
            requests.delete(f"{a.registry}/config/{enc(s)}", timeout=30)
    call(a.registry, "PUT", "/config", {"compatibility": "BACKWARD"})
    left = call(a.registry, "GET", "/subjects?deleted=true")
    if left:
        raise SystemExit(f"{a.registry} not wiped: {left}")
    return 0


def cmd_occupy(a):
    """Give every schema ID the backup's records carry to a schema of the
    target's own, so each restored schema conflicts on its ID."""
    expected = json.load(open(a.expected))
    ids = sorted({e[f] for e in expected for f in ("value_schema_id", "key_schema_id") if e[f] is not None})
    call(a.registry, "PUT", "/mode?force=true", {"mode": "IMPORT"})
    for i in ids:
        schema = json.dumps({"type": "record", "name": f"Occupant{i}", "namespace": "kbsr.target",
                             "fields": [{"name": "n", "type": "int"}]})
        call(a.registry, "POST", f"/subjects/{enc(f'occupant-{i}-value')}/versions",
             {"schema": schema, "id": i, "version": 1})
    call(a.registry, "PUT", "/mode?force=true", {"mode": "READWRITE"})
    print(json.dumps({"occupied_ids": ids}))
    return 0


def cmd_seed(a):
    """A registry-only fixture: `count` subjects with three versions each."""
    for i in range(a.count):
        subject = f"fixture-{i:02d}-value"
        call(a.registry, "PUT", f"/config/{enc(subject)}", {"compatibility": "NONE"})
        for v in range(3):
            schema = json.dumps({"type": "record", "name": f"Fixture{i}", "namespace": "kbsr.fixture",
                                 "fields": [{"name": f"f{j}", "type": "string"} for j in range(v + 1)]})
            call(a.registry, "POST", f"/subjects/{enc(subject)}/versions", {"schema": schema})
    return 0


def registry_state(registry):
    state = {"mode": call(registry, "GET", "/mode")["mode"], "subjects": {}}
    for s in sorted(call(registry, "GET", "/subjects?deleted=true")):
        versions = {}
        for v in call(registry, "GET", f"/subjects/{enc(s)}/versions?deleted=true"):
            sv = call(registry, "GET", f"/subjects/{enc(s)}/versions/{v}?deleted=true")
            versions[str(v)] = {"id": sv["id"], "schema": sv["schema"], "type": sv.get("schemaType", "AVRO")}
        mode = requests.get(f"{registry}/mode/{enc(s)}", timeout=30)
        state["subjects"][s] = {
            "versions": versions,
            "live": requests.get(f"{registry}/subjects/{enc(s)}/versions", timeout=30).json()
            if requests.get(f"{registry}/subjects/{enc(s)}/versions", timeout=30).status_code == 200 else [],
            "mode": mode.json().get("mode") if mode.status_code == 200 else None,
        }
    return state


def cmd_registry(a):
    print(json.dumps(registry_state(a.registry), sort_keys=True))
    return 0


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("produce")
    s.add_argument("--bootstrap", required=True)
    s.add_argument("--registry", required=True)
    s.add_argument("--expected", required=True)
    s = sub.add_parser("verify")
    s.add_argument("--bootstrap", required=True)
    s.add_argument("--registry", required=True)
    s.add_argument("--expected", required=True)
    s.add_argument("--order", choices=["partition", "key"], default="partition")
    s.add_argument("--partitions", type=int, default=0)
    s.add_argument("--ids", choices=["same", "rewritten", "any"], default="same")
    s.add_argument("--topic-map", action="append", default=[])
    for name in ("wipe", "registry"):
        s = sub.add_parser(name)
        s.add_argument("--registry", required=True)
    s = sub.add_parser("occupy")
    s.add_argument("--registry", required=True)
    s.add_argument("--expected", required=True)
    s = sub.add_parser("seed")
    s.add_argument("--registry", required=True)
    s.add_argument("--count", type=int, required=True)
    a = p.parse_args()
    return {"produce": cmd_produce, "verify": cmd_verify, "wipe": cmd_wipe, "occupy": cmd_occupy,
            "seed": cmd_seed, "registry": cmd_registry}[a.cmd](a)


if __name__ == "__main__":
    sys.exit(main())
