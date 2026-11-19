# OAUTHBEARER test infrastructure

Two Docker Compose fixtures:

| File | Broker validates tokens with | Used by |
|---|---|---|
| `docker-compose-oauth-idp.yml` | The JWKS, issuer and audience of a real identity provider | `crates/kafka-backup-cli/tests/oauth_real_broker.rs` |
| `docker-compose-oauth.yml` | Kafka's unsecured-JWS handler (any `alg: none` token) | `crates/kafka-backup-core/tests/integration_suite/sasl_oauth_tests.rs` |

## Identity provider and JWKS-validating broker

`docker-compose-oauth-idp.yml` (compose project `kb-oauth`) runs:

- **`idp`**: [mock-oauth2-server](https://github.com/navikt/mock-oauth2-server),
  an OAuth 2.0 / OIDC server that signs real JWTs, on `localhost:18080`. It
  has three issuers (`idp/mock-oauth2-config.json`), all with 120 s tokens
  whose `sub` is the client ID:

  | Issuer | Claims | The broker |
  |---|---|---|
  | `kafka` | `iss http://idp:8080/kafka`, `aud api://kafka-backup-local` | accepts it |
  | `other` | a different issuer, signed by its own key | refuses it |
  | `wrongaud` | the trusted `iss` claim, another audience | refuses it |

  Any client ID and secret are accepted. Token endpoint:
  `http://localhost:18080/<issuer>/token`; OIDC discovery:
  `http://localhost:18080/<issuer>/.well-known/openid-configuration`.
- **`kafka`**: one KRaft broker (Kafka 4.x, cp-kafka 8.3.1). Its OAUTH
  listener on `localhost:19093` (SASL_PLAINTEXT) validates tokens against the
  `kafka` issuer's JWKS, expected issuer and audience, and forces KIP-368
  re-authentication every 60 s. A PLAINTEXT listener is on `localhost:19092`.

Override the host ports with `KB_OAUTH_IDP_PORT`, `KB_OAUTH_PLAIN_PORT` and
`KB_OAUTH_PORT`.

```bash
docker compose -f tests/sasl-oauth-test-infra/docker-compose-oauth-idp.yml up -d --wait

cargo test -p kafka-backup-cli --test oauth_real_broker -- --ignored --nocapture --test-threads=1

docker compose -f tests/sasl-oauth-test-infra/docker-compose-oauth-idp.yml down -v
```

The suite drives the built `kafka-backup` binary:

| Test | What it checks | Time |
|---|---|---|
| `backup_and_restore_over_oauth_with_basic_and_post` | Backup and restore over OAUTHBEARER with `client_secret_basic` and `client_secret_post`; record counts and SHA-256 checksums of the source and restored topics match | 20 s |
| `wrong_issuer_and_wrong_audience_are_rejected_with_an_actionable_error` | A validly signed token the broker does not accept fails the backup with an error naming the provider and the likely causes; nothing is archived | 5 s |
| `soak_reauthenticates_without_reconnecting` | A continuous backup re-authenticates in place: no full handshake after start-up, no gap or duplicate offset in the archive, token requests match the refresh schedule | `KB_OAUTH_SOAK_SECS` (default 330, acceptance run 1800) |
| `idp_outages_short_and_beyond_token_lifetime` | The identity provider stops mid-run: the backup continues on its cached token, fails cleanly once the token expires, and completes after the provider returns | 8 min |
| `file_provider_subject_change_reconnects_and_completes` | A token file whose subject changes: the broker refuses the re-authentication, the client reconnects, the backup completes | 5 min |
| `azure_managed_identity_through_an_imds_stub`, `azure_workload_identity_through_an_entra_stub` | The Azure providers' request shapes, with tokens the broker accepts | 10 s |
| `oidc_discovery_from_the_mock_issuer` | The token endpoint is discovered from the issuer | 5 s |
| `offset_commands_use_the_config_files_provider` | `offset-rollback snapshot` and `verify` through `--config --cluster` | 10 s |

A configuration for trying the CLI by hand against this stack:

```yaml
mode: backup
backup_id: oauth-local
source:
  bootstrap_servers: ["localhost:19093"]
  topics: { include: ["orders"] }
  security:
    security_protocol: SASL_PLAINTEXT   # this local fixture only
    sasl_mechanism: OAUTHBEARER
    oauth: { provider: idp }
storage: { backend: filesystem, path: /tmp/oauth-local }
oauth:
  providers:
    idp:
      type: oidc_client_credentials
      token_endpoint_url: http://localhost:18080/kafka/token
      client_id: kafka-backup
      client_secret: any-secret
      scope: kafka
```

## Unsecured-JWS broker

Companion fixture for the `#[ignore]` E2E test in
`crates/kafka-backup-core/tests/integration_suite/sasl_oauth_tests.rs`.

### What it is

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

### Start / stop

```bash
cd tests/sasl-oauth-test-infra
docker-compose -f docker-compose-oauth.yml up -d

# Wait for the broker to accept connections (≈ 15s on a cold image pull):
docker logs -f kafka-oauth-test | grep -m1 "started (kafka.server.KafkaServer)"

# Tear down:
docker-compose -f docker-compose-oauth.yml down
```

### Run the E2E test

```bash
cargo test -p kafka-backup-core \
    --test integration_suite_tests \
    -- --ignored sasl_oauth_
```

### What it exercises

- `SaslHandshake` with `mechanism = "OAUTHBEARER"`.
- A single `SaslAuthenticate` round carrying an RFC 7628
  `n,a=test-user,<0x01>auth=Bearer <jwt><0x01><0x01>` payload built by
  `StaticOAuthBearerPlugin::for_test_user()`.
- Post-auth `Metadata` fetch (proves the authenticated session works).

### Troubleshooting

- **`Connection refused` on 9097** — broker is still booting. Wait ~15s
  and retry; the compose healthcheck covers most of it but cold pulls
  take longer.
- **`invalid_token` in broker logs** — the bundled unsecured validator
  is strict about base64url padding. The fixture token has no padding
  (RFC 7515 §2). If you hand-roll a token, ensure `alg: none` and keep
  the signature segment empty.
- **Port 9097 conflict** — another service is using the host port. Stop
  that service or edit the `ports:` mapping.
