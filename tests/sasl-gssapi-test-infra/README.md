# GSSAPI (Kerberos) test infrastructure

Companion fixture for the `#[ignore]` E2E tests in
`crates/kafka-backup-core/tests/integration_suite/sasl_gssapi_tests.rs`.

## What it is

A three-container stack that boots a self-contained MIT Kerberos realm
(`TEST.LOCAL`) plus an Apache Kafka 7.7.0 broker configured for
`SASL_PLAINTEXT + GSSAPI`. The KDC container creates the service and
client principals on first start, exports keytabs into a shared volume,
and the Kafka broker mounts that volume read-only to authenticate
clients against the realm.

Exercises the full `GssapiPlugin` dispatch path end-to-end:

1. Phase 1 multi-round `gss_init_sec_context` against a real broker.
2. Phase 1→2 turnaround (client sends empty, broker replies with wrapped
   security-layer proposal).
3. Phase 2 wrap/unwrap with layer = 0x01 (no security layer, no size).
4. Post-auth `Metadata` RPC on the authenticated session.
5. KIP-368 reauth trigger (broker advertises 60s window; client
   schedules ~48s).

## Host prerequisites

**macOS.** Install MIT krb5 (Apple's bundled Heimdal does not expose the
symbols `libgssapi 0.9` links against):

```bash
brew install krb5
export PKG_CONFIG_PATH="$(brew --prefix krb5)/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
```

**Linux.** Install krb5 development headers:

```bash
# Debian/Ubuntu
sudo apt-get install libkrb5-dev
# Fedora/RHEL
sudo dnf install krb5-devel
```

**Host /etc/hosts.** The GSSAPI service principal is
`kafka/kafka.test.local@TEST.LOCAL`. Clients MUST connect to that exact
hostname — `localhost` will return `KRB5KDC_ERR_S_PRINCIPAL_UNKNOWN`.

```bash
sudo sh -c 'grep -q kafka.test.local /etc/hosts \
  || echo "127.0.0.1 kafka.test.local kdc.test.local" >> /etc/hosts'
```

**Host port 88.** The fixture publishes the KDC on the standard Kerberos
port (88/tcp + 88/udp) so the same `krb5.conf` works from inside the
compose network and from the host test runner. Check nothing else is
bound before `up`:

```bash
sudo lsof -iTCP:88 -iUDP:88 -sTCP:LISTEN 2>/dev/null
```

## Start / stop

```bash
cd tests/sasl-gssapi-test-infra
docker compose -f docker-compose-gssapi.yml up -d --wait

# Confirm the keytabs were created and principals exist:
docker compose -f docker-compose-gssapi.yml logs kdc | grep "principals created"

# Tear down — `-v` drops Docker-managed volumes so a fresh up re-seeds
# the KDC database. The host-bind-mounted keytabs/ directory is NOT
# wiped by -v, so remove it manually if you want fresh keys:
docker compose -f docker-compose-gssapi.yml down -v
rm -f keytabs/*.keytab
```

Keytabs land in `tests/sasl-gssapi-test-infra/keytabs/` on the host once
the KDC container reaches the healthy state. If a previous run's keytab
is still present when a fresh KDC boots, you will hit `Password
incorrect` / `Credential cache is empty` errors — the keytab's keys do
not match the freshly-minted KDC principal. Delete the old keytabs
before bringing the stack back up.

## Run the E2E tests

```bash
# macOS: keep PKG_CONFIG_PATH exported for the krb5 link step.
cargo test --features gssapi -p kafka-backup-core \
    --test integration_suite_tests -- --ignored sasl_gssapi_
```

## CLI smoke test

From a second shell, with the compose stack up. `offset-rollback
snapshot` hits the broker's OffsetFetch RPC, so a successful return
proves the full GSSAPI handshake completed against the live broker:

```bash
export PKG_CONFIG_PATH="$(brew --prefix krb5)/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
mkdir -p /tmp/gssapi-smoke-snap
cargo run --release --features gssapi -p kafka-backup-cli -- \
    offset-rollback snapshot \
    --path /tmp/gssapi-smoke-snap \
    --bootstrap-servers kafka.test.local:9098 \
    --security-protocol SASL_PLAINTEXT \
    --sasl-mechanism GSSAPI \
    --sasl-keytab tests/sasl-gssapi-test-infra/keytabs/client.keytab \
    --sasl-krb5-config tests/sasl-gssapi-test-infra/krb5.conf \
    --sasl-kerberos-service-name kafka \
    --groups smoke-test-group
```

Turn on `RUST_LOG=kafka_backup_core=debug` to see the
`GSSAPI Phase 2 complete server_layers=0x01` trace that confirms
the wrap/unwrap round-trip.

## Run the release binary smoke script

`run-cli-smoke.sh` drives the fully-built `kafka-backup` binary through
a backup → restore cycle against this fixture using the YAML configs
at `config/gssapi-backup.yaml` and `config/gssapi-restore.yaml`. This
catches regressions that `cargo test` alone cannot — arg parsing, YAML
deserialisation, and the CLI's `populate_sasl_plugin` handoff to the
runtime plugin.

```bash
# From the repo root, with the compose stack up:
bash tests/sasl-gssapi-test-infra/run-cli-smoke.sh
```

The script:
1. Verifies `/etc/hosts` and fixture readiness.
2. Builds `cargo build --release --features gssapi -p kafka-backup-cli`.
3. Creates `smoke-topic` on the broker's internal PLAINTEXT listener
   and produces 50 key/value records.
4. Runs `kafka-backup backup --config config/gssapi-backup.yaml`
   (hits the broker over GSSAPI) and asserts a manifest was emitted.
5. Runs `kafka-backup restore --config config/gssapi-restore.yaml`
   which remaps `smoke-topic` → `smoke-topic-restored` on the same
   cluster.
6. Consumes from `smoke-topic-restored` and asserts 50 records
   arrived.
7. Deletes both topics and removes `/tmp/gssapi-backup-smoke`
   regardless of pass/fail.

## Troubleshooting

- **`KRB5KDC_ERR_S_PRINCIPAL_UNKNOWN`** — host does not resolve
  `kafka.test.local` to `127.0.0.1`, or the client is connecting to
  `localhost` instead of the FQDN. Verify `/etc/hosts` and the
  `bootstrap_servers` value.
- **`Credential cache is empty` / `Password incorrect`** — the client
  keytab on the host is stale. `init-kdc.sh` auto-cleans the bind-mounted
  `keytabs/` directory when it creates a fresh realm, so this should
  only happen if the init script did not run to completion (e.g.,
  interrupted `up`). Fix: `rm keytabs/*.keytab` and `docker compose
  restart kdc`.
- **`gss_acquire_cred` returns `No credentials cache found`** — the
  client keytab is missing or unreadable. Check
  `tests/sasl-gssapi-test-infra/keytabs/client.keytab` exists and is
  not empty; if not, run `docker compose logs kdc` for init errors.
- **`Authentication failed due to invalid credentials`** (broker log)
  — the client sent an AP-REQ that the broker can't decrypt, typically
  because the ticket was issued by an earlier KDC instance whose service
  key has since rotated. `GssapiPlugin` isolates its credential cache
  via `KRB5CCNAME=MEMORY:<ptr>` whenever a `--sasl-keytab` is passed,
  so stale tickets in the OS ccache cannot leak into the plugin. If
  you *see* this error it usually means you are running an older build;
  upgrade, or clear the system ccache (`kdestroy -A`) as a workaround.
- **`Cannot find key of appropriate type`** — the broker enctype list
  in `kdc.conf` (`aes256-cts-hmac-sha1-96`, `aes128-cts-hmac-sha1-96`)
  must be a subset of what the JDK supports. OpenJDK 11+ supports both;
  ancient JDKs without the unlimited-strength JCE policy will fail on
  AES-256.
- **Clock skew** — Kerberos rejects tickets more than 5 minutes out of
  sync. `docker compose` timekeeping follows the host clock, so the
  usual culprit is a stopped host (laptop resume). Restart the stack:
  `docker compose -f docker-compose-gssapi.yml restart`.
- **Port 9098 conflict** — something else bound the port. Either stop
  that service or edit the `ports:` mapping and set
  `advertised_listeners` accordingly.
