#!/usr/bin/env bash
# KDC entrypoint: bootstrap realm, create principals, export keytabs, then run.
#
# Idempotent — skips database creation if /var/lib/krb5kdc/principal already
# exists (e.g. when the keytabs volume is wiped but the database persists).

set -euo pipefail

REALM="TEST.LOCAL"
MASTER_PASSWORD="kdc-test-master"
KEYTAB_DIR="/keytabs"

mkdir -p "${KEYTAB_DIR}"

if [[ ! -f /var/lib/krb5kdc/principal ]]; then
    echo "[init-kdc] Creating realm ${REALM}..."
    kdb5_util create -s -r "${REALM}" -P "${MASTER_PASSWORD}"

    echo "[init-kdc] Creating service principal kafka/kafka.test.local@${REALM}..."
    kadmin.local -q "addprinc -randkey kafka/kafka.test.local@${REALM}"

    echo "[init-kdc] Creating client principal client@${REALM}..."
    kadmin.local -q "addprinc -randkey client@${REALM}"

    echo "[init-kdc] Creating admin principal admin/admin@${REALM}..."
    kadmin.local -q "addprinc -pw admin-test admin/admin@${REALM}"

    # Remove any stale host-mounted keytabs from a previous run before
    # ktadd writes the fresh ones. `docker compose down -v` wipes the
    # KDC principal database but NOT the host bind-mount at ./keytabs,
    # so without this cleanup ktadd would append new-KVNO entries next
    # to old ones and kinit / the Rust harness would pick the stale
    # entry and hit "Password incorrect / Credential cache is empty".
    #
    # Per-file rm (not a glob sweep of the directory) keeps the atomic
    # write ordering: kafka.keytab is removed and rewritten before the
    # healthcheck sees it, so the broker never opens a half-written file.
    echo "[init-kdc] Removing any stale keytabs in ${KEYTAB_DIR}..."
    rm -f "${KEYTAB_DIR}/kafka.keytab" "${KEYTAB_DIR}/client.keytab"

    echo "[init-kdc] Writing keytabs to ${KEYTAB_DIR}..."
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/kafka.keytab kafka/kafka.test.local@${REALM}"
    kadmin.local -q "ktadd -k ${KEYTAB_DIR}/client.keytab client@${REALM}"

    # Keytabs must be readable by both the kafka broker container (UID 1000)
    # and the Rust test process on the host. World-readable is acceptable
    # for a test fixture — they protect nothing outside the compose network.
    chmod 0644 "${KEYTAB_DIR}"/*.keytab

    echo "[init-kdc] principals created"
fi

echo "[init-kdc] Starting KDC (krb5kdc) and kadmind..."
krb5kdc
kadmind -nofork &

# Keep the container running. `krb5kdc` daemonizes; without this the
# container would exit as soon as init-kdc.sh returns.
tail -f /var/log/krb5kdc.log 2>/dev/null || sleep infinity
