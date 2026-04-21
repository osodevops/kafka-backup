# OAUTHBEARER test infrastructure

Companion fixture for the `#[ignore]` E2E test in
`crates/kafka-backup-core/tests/integration_suite/sasl_oauth_tests.rs`.

## What it is

A single-broker Apache Kafka stack (cp-kafka 7.7.0) configured for
`SASL_PLAINTEXT + OAUTHBEARER`, validating inbound tokens with the
broker's bundled **unsecured-JWS** handler (Apache Kafka's
`OAuthBearerUnsecuredValidatorCallbackHandler`). Any JWT with
`alg: none` authenticates — this is the canonical test-mode Apache
documents for OAUTHBEARER development.

This is **not** production-grade OAuth. It exists to prove the OSS
plugin dispatch path (`SaslMechanismPlugin` + KIP-368 reauth) works
against a real broker without requiring an IdP, signing keys, or
network access.

## Start / stop

```bash
cd tests/sasl-oauth-test-infra
docker-compose -f docker-compose-oauth.yml up -d

# Wait for the broker to accept connections (≈ 15s on a cold image pull):
docker logs -f kafka-oauth-test | grep -m1 "started (kafka.server.KafkaServer)"

# Tear down:
docker-compose -f docker-compose-oauth.yml down
```

## Run the E2E test

```bash
cargo test -p kafka-backup-core \
    --test integration_suite_tests \
    -- --ignored sasl_oauth_
```

## What it exercises

- `SaslHandshake` with `mechanism = "OAUTHBEARER"`.
- A single `SaslAuthenticate` round carrying an RFC 7628
  `n,a=test-user,<0x01>auth=Bearer <jwt><0x01><0x01>` payload built by
  `StaticOAuthBearerPlugin::for_test_user()`.
- Post-auth `Metadata` fetch (proves the authenticated session works).

## Troubleshooting

- **`Connection refused` on 9097** — broker is still booting. Wait ~15s
  and retry; the compose healthcheck covers most of it but cold pulls
  take longer.
- **`invalid_token` in broker logs** — the bundled unsecured validator
  is strict about base64url padding. The fixture token has no padding
  (RFC 7515 §2). If you hand-roll a token, ensure `alg: none` and keep
  the signature segment empty.
- **Port 9097 conflict** — another service is using the host port. Stop
  that service or edit the `ports:` mapping.
