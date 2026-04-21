# SASL Integration Test Infrastructure

Docker Compose stack for testing kafka-backup-core's SASL auth paths
(`PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`).

## What it gives you

- One Kafka broker on `localhost:9095` with a `SASL_PLAINTEXT` listener
  accepting all three mechanisms.
- Test credentials:
  - **PLAIN:** `alice` / `alice-secret`
  - **SCRAM-SHA-256:** `bob` / `bob-secret`
  - **SCRAM-SHA-512:** `carol` / `carol-secret`
- A one-shot `scram-init` service that seeds the SCRAM users into Kafka's
  metadata after the broker is up (SCRAM credentials can't live in JAAS).

## Bring up

```bash
cd tests/sasl-test-infra
docker-compose -f docker-compose-sasl.yml up -d

# Wait for the init container to finish seeding SCRAM users:
docker logs -f kafka-sasl-scram-init
# Look for: "SCRAM users seeded."
```

## Run the tests

From the repository root:

```bash
cargo test -p kafka-backup-core \
    --test integration_suite_tests \
    -- --ignored sasl_
```

## Tear down

```bash
docker-compose -f docker-compose-sasl.yml down -v
```

## Notes

- Uses `SASL_PLAINTEXT` (not `SASL_SSL`) to keep the fixture small — mTLS
  is already covered by `tls-test-infra`. SASL on top of TLS shares the
  same SASL framing path.
- Inter-broker traffic uses a separate `INTERNAL` PLAINTEXT listener so
  the broker bootstraps without needing its own SASL creds.
- The reconnect test uses `docker restart kafka-sasl-test` to force the
  client through the `authenticate_raw()` path.
