# OAuth authentication (SASL/OAUTHBEARER)

Kafka Backup authenticates to Kafka with OAuth 2.0 / OIDC access tokens over
SASL/OAUTHBEARER, so a backup or restore needs no API key and no long-lived
password. Tokens come from Microsoft Entra ID (client credentials, an Azure
managed identity or AKS workload identity), from any other OIDC provider
(Okta, Auth0, Keycloak), or from a token file that something else keeps fresh.
It is all Rust-native: no JVM, no librdkafka and no Azure SDK.

Available from v0.29.0.

| | |
|---|---|
| Commands | `backup`, `restore`, `three-phase-restore`, `validate-restore`, `snapshot-groups`, `validation run`, and the offset commands with `--config <yaml> --cluster source\|target` |
| Kafka | `sasl_mechanism: OAUTHBEARER` plus a `security.oauth` binding on the cluster |
| Providers | `oidc_client_credentials`, `azure_managed_identity`, `azure_workload_identity`, `file` |
| Schema Registry | Not yet: bearer authentication for the registry arrives with Schema Registry backup ([#262](https://github.com/osodevops/kafka-backup/issues/262)) and will use the same providers and token cache |

- [1. Quick start](#1-quick-start)
- [2. Configuration reference](#2-configuration-reference)
- [3. Token cache](#3-token-cache)
- [4. Confluent Cloud with Microsoft Entra ID](#4-confluent-cloud-with-microsoft-entra-id)
- [5. Offset commands and validation](#5-offset-commands-and-validation)
- [6. Observability and security](#6-observability-and-security)
- [7. Troubleshooting](#7-troubleshooting)
- [8. Terraform](#8-terraform)
- [9. Testing locally](#9-testing-locally)

## 1. Quick start

Token providers are named in a top-level `oauth` section. A cluster picks one
with `security.oauth.provider`:

```yaml
mode: backup
backup_id: orders-daily

source:
  bootstrap_servers: ["pkc-xxxxx.westeurope.azure.confluent.cloud:9092"]
  topics:
    include: ["orders-*"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth:
      provider: entra-backup
      extensions:
        logicalCluster: lkc-abc123
        identityPoolId: pool-Ab1      # optional; omit for automatic pool mapping

storage:
  backend: s3
  bucket: kafka-backups
  region: eu-west-1

oauth:
  providers:
    entra-backup:
      type: oidc_client_credentials
      token_endpoint_url: https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token
      client_id: <app id>
      client_secret: ${ENTRA_BACKUP_CLIENT_SECRET}
      scope: api://<api app id>/.default
```

`source` and `target` bind independently, so a restore can run as a different
identity, with different permissions, from the backup that produced the data.
Clusters that name the same provider share one token.

## 2. Configuration reference

Every key below is checked: a misspelt or misplaced key is an error, not an
ignored line. `${ENV_VAR}` references are expanded, so keep secrets in the
environment.

### 2.1 The cluster binding: `source.security.oauth` / `target.security.oauth`

| Key | Required | Description |
|---|---|---|
| `provider` | Yes | A key of `oauth.providers` |
| `extensions.logicalCluster` | No | Confluent Cloud Kafka cluster ID (`lkc-...`) |
| `extensions.identityPoolId` | No | Confluent Cloud identity pool ID (`pool-...`), or a comma-separated list. Omit it to let Confluent Cloud match the token against your pools' filters (automatic pool mapping) |

Extension values are checked against RFC 7628 before they are sent.

These are configuration errors, raised before anything connects:

- `sasl_mechanism: OAUTHBEARER` without a `security.oauth` binding;
- a `security.oauth` binding on a cluster whose mechanism is not OAUTHBEARER;
- a binding to a provider that `oauth.providers` does not define;
- OAUTHBEARER with `security_protocol` `PLAINTEXT` or `SSL` (it needs
  `SASL_SSL`; `SASL_PLAINTEXT` works with a warning, for local testing only).

### 2.2 Providers: `oauth.providers.<name>`

A provider name uses letters, digits, `_`, `-` and `.`. Providers that no
cluster uses are not contacted.

#### `oidc_client_credentials`

RFC 6749 client credentials: Entra ID, Okta, Auth0, Keycloak and any other
standards-compliant token endpoint.

| Key | Required | Default | Description |
|---|---|---|---|
| `token_endpoint_url` | One of these two | - | The token endpoint |
| `issuer` | One of these two | - | OIDC issuer; the token endpoint is discovered from `{issuer}/.well-known/openid-configuration` on first use |
| `client_id` | Yes | - | |
| `client_secret` | Yes | - | Use `${ENV_VAR}` |
| `client_auth` | No | `basic` | `basic` (`client_secret_basic`: HTTP Basic) or `post` (`client_secret_post`: in the form body) |
| `scope` | No | - | Entra ID: `api://<api app id>/.default` |
| `audience` | No | - | Sent as the `audience` parameter (Okta, Auth0). Entra ID uses `scope` |
| `extra_params` | No | `{}` | Extra form parameters for the token request |
| `ca_cert` | No | - | Extra CA certificate (PEM) for a private identity provider |
| `allow_insecure_http` | No | `false` | Allow `http://` to a host other than localhost. Client secrets then cross the network in clear text: isolated test networks only |

#### `azure_managed_identity`

A token from the Azure Instance Metadata Service, for VMs and scale sets. No
secret is stored anywhere.

| Key | Required | Description |
|---|---|---|
| `scope` | One of these two | An Entra scope (`api://<api app id>/.default`); `/.default` is dropped to form the resource |
| `resource` | One of these two | The resource (`api://<api app id>`) |
| `client_id` | No | Client ID of a user-assigned managed identity. Unset: the system-assigned identity |
| `endpoint` | No | Override the IMDS token endpoint (proxies, tests) |

#### `azure_workload_identity`

For AKS: the projected service-account token is exchanged for an Entra token
with a client assertion. The token file is re-read on every refresh, so the
kubelet's rotation is picked up.

| Key | Required | Default |
|---|---|---|
| `scope` | Yes | - |
| `tenant_id` | No | `AZURE_TENANT_ID` |
| `client_id` | No | `AZURE_CLIENT_ID` |
| `token_file` | No | `AZURE_FEDERATED_TOKEN_FILE` |
| `authority_host` | No | `AZURE_AUTHORITY_HOST`, else `https://login.microsoftonline.com/` |

The workload identity webhook injects those variables into a pod labelled
`azure.workload.identity/use: "true"` whose service account is annotated with
`azure.workload.identity/client-id`.

#### `file`

A token kept fresh by a sidecar or another process.

| Key | Required | Description |
|---|---|---|
| `path` | Yes | Read on every refresh; surrounding whitespace is trimmed |
| `expires_at_file` | No | A file holding the expiry, as RFC 3339 or Unix seconds. Unset: the JWT `exp` claim |

An opaque token with no known expiry is re-read every five minutes.

There is also a `static` type (`token: ...`), a fixed token that is never
refreshed. It exists for tests.

### 2.3 Complete examples

Azure VM with a user-assigned managed identity:

```yaml
source:
  bootstrap_servers: ["pkc-xxxxx.westeurope.azure.confluent.cloud:9092"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth: { provider: mi, extensions: { logicalCluster: lkc-abc123 } }

oauth:
  providers:
    mi:
      type: azure_managed_identity
      scope: api://<api app id>/.default
      client_id: <user-assigned identity client id>   # omit for the system-assigned identity
```

AKS workload identity:

```yaml
oauth:
  providers:
    aks:
      type: azure_workload_identity
      scope: api://<api app id>/.default
```

Okta, by issuer discovery, with a token file for the restore cluster:

```yaml
source:
  bootstrap_servers: ["kafka-a:9093"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth: { provider: okta }
target:
  bootstrap_servers: ["kafka-b:9093"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth: { provider: vault }

oauth:
  providers:
    okta:
      type: oidc_client_credentials
      issuer: https://example.okta.com/oauth2/default
      client_id: kafka-backup
      client_secret: ${OKTA_CLIENT_SECRET}
      audience: api://kafka
    vault:
      type: file
      path: /var/run/secrets/kafka/token
      expires_at_file: /var/run/secrets/kafka/token.expires_at
```

## 3. Token cache

There is one cache per provider, shared by every broker connection of every
cluster bound to that provider. Only one request per provider is in flight;
concurrent callers wait for it.

| Setting (`oauth.cache`) | Default | Meaning |
|---|---|---|
| `refresh_fraction` | `0.8` | Refresh after this share of the token's lifetime... |
| `refresh_before_expiry` | `5m` | ...or this long before expiry, whichever is earlier |
| `clock_skew` | `30s` | A token is valid while `exp - clock_skew > now` |
| `min_refresh_interval` | `30s` | Never refresh sooner than this after a fetch; also the first backoff step |

Durations are written as `30s`, `5m`, `1h`. `refresh_fraction` is between 0
and 1, exclusive.

- Past its refresh time a valid token stays in use while a background refresh
  runs. If the identity provider is unavailable, the valid token keeps being
  used; retries back off exponentially (30 s, 60 s, ...) and always make one
  more attempt before the token stops being valid.
- Every SASL handshake, initial or re-authentication, gets a token with at
  least 60 s left (bounded by 80 % of the token's lifetime). KIP-368
  re-authentication therefore happens in place on the existing connection,
  without a reconnect. A slow identity provider holds a handshake up by at
  most 5 s when a valid token is available.
- A broker `invalid_token` rejection drops the cached token, unless it was
  fetched less than `min_refresh_interval` ago (then the rejection is about
  configuration, not expiry).

Identity-provider requests use `https://` only (plain `http://` is accepted
for localhost, or with `allow_insecure_http`). Redirects are never followed.
Requests time out after 15 s (5 s to connect) and are retried three times on
429, 5xx and connection errors, honouring `Retry-After`.

With tokens that live for less than `refresh_before_expiry` (5 minutes by
default) the token is refreshed every `min_refresh_interval`. Lower
`refresh_before_expiry` if your tokens are that short.

## 4. Confluent Cloud with Microsoft Entra ID

```
Entra ID ──token──▶ Kafka Backup ──SASL/OAUTHBEARER (+ logicalCluster, identityPoolId)──▶ Confluent Cloud Kafka

Confluent Cloud: identity provider (issuer + JWKS) → identity pools (CEL filter on the token's claims) → role bindings
```

1. Kafka Backup gets an access token from Entra ID.
2. Confluent Cloud validates it against the identity provider you registered
   (issuer and JWKS), picks the identity pool whose filter matches the
   token's claims, and authorises with that pool's role bindings.
3. Kafka Backup caches the token and shares it across every connection.

### 4.1 The API application (the token audience)

Register one application that represents "Confluent Cloud for Kafka Backup":

- **Expose an API**: set the Application ID URI, for example
  `api://kafka-backup-confluent` (or the default `api://<application-id>`).
- **Request v2 tokens.** Set `"requestedAccessTokenVersion": 2` under `api` in
  the app manifest (the older AAD Graph manifest calls it
  `accessTokenAcceptedVersion`). Without it Entra issues v1 tokens whose
  issuer is `https://sts.windows.net/<tenant>/`, which does not match the v2
  issuer you register in Confluent Cloud, and the cluster refuses them.
- Clients request the scope `<Application ID URI>/.default`. In a v2 token,
  `aud` is the API application's **application (client) ID** (a GUID) and
  `oid` identifies the calling service principal. Look at the claims of a
  real token before you write the pool filter.

### 4.2 The calling identity

| Where Kafka Backup runs | Identity | Provider type |
|---|---|---|
| Anywhere, with a secret | App registration + client secret | `oidc_client_credentials` |
| Azure VM / VMSS | System- or user-assigned managed identity | `azure_managed_identity` |
| AKS | Workload identity: a user-assigned managed identity (or app registration) federated with the cluster's OIDC issuer | `azure_workload_identity` |

Note the identity's **object ID** (the service principal object ID for an
app, the principal ID for a managed identity) and check it against the
token's `oid` claim.

The client-credentials `.default` scope needs no API permissions or admin
consent, unless the API application defines app roles that you want to
require.

### 4.3 The Confluent Cloud identity provider

Accounts & access → Workload identities → Add provider → Microsoft Entra ID:

| Field | Value |
|---|---|
| Issuer URI | `https://login.microsoftonline.com/<tenant-id>/v2.0` |
| JWKS URI | `https://login.microsoftonline.com/<tenant-id>/discovery/v2.0/keys` |

### 4.4 Identity pools

Use separate pools for backup and restore, so each has only its own role
bindings. The filter is CEL over the token's claims:

```
# Backup pool: only the backup identity, only tokens for our API app
claims.aud == "<api-application-id>" && claims.oid == "<backup-identity-object-id>"

# Restore pool
claims.aud == "<api-application-id>" && claims.oid == "<restore-identity-object-id>"
```

Kafka Backup sends the pool ID as the `identityPoolId` SASL extension when
you configure it. Leave it out to let Confluent Cloud match the token against
your pools' filters; check first that your organisation has automatic pool
mapping.

### 4.5 Role bindings

Scope the topic and consumer-group patterns to what you back up.

| Pool | Role | Resource | Why |
|---|---|---|---|
| Backup | `DeveloperRead` | Topics in scope (prefix) | Fetch, offsets, topic configuration |
| Backup | `DeveloperRead` | Consumer groups in scope (prefix) | `consumer_group_snapshot` (OffsetFetch) |
| Restore | `DeveloperWrite` | Target topics (prefix) | Produce |
| Restore | `DeveloperManage` | Target topics (prefix) | Create topics |
| Restore | `DeveloperRead` | Target topics and consumer groups (prefix) | Offset commits (OffsetCommit needs Read on both) |
| Restore (topic settings only) | `CloudClusterAdmin` | Restore cluster | Confluent Cloud needs this to apply archived topic configuration |

The backup identity is read-only. `CloudClusterAdmin` is a wide grant: to
avoid it, create the topics and their settings beforehand with a separate
identity and run the restore with the scoped roles only.

### 4.6 Backup and restore configuration

The backup side is the [quick start](#1-quick-start). The restore binds
`target` to its own provider and pool:

```yaml
mode: restore
backup_id: orders-daily

target:
  bootstrap_servers: ["pkc-yyyyy.westeurope.azure.confluent.cloud:9092"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth:
      provider: entra-restore
      extensions:
        logicalCluster: lkc-def456
        identityPoolId: pool-Rs2

storage:
  backend: s3
  bucket: kafka-backups
  region: eu-west-1

restore:
  target_profile: confluent_cloud

oauth:
  providers:
    entra-restore:
      type: oidc_client_credentials
      token_endpoint_url: https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token
      client_id: <restore app id>
      client_secret: ${ENTRA_RESTORE_CLIENT_SECRET}
      scope: api://<api app id>/.default
```

## 5. Offset commands and validation

The offset commands' flags (`--bootstrap-servers`, `--security-protocol`,
`--sasl-mechanism`) cannot describe a token provider. Point them at a
configuration file instead:

```bash
kafka-backup offset-reset-bulk --path s3://bucket --backup-id orders-daily \
  --groups orders-service --config restore.yaml --cluster target
```

`--config <yaml>` takes the cluster's bootstrap servers and its whole
`security:` block (TLS files, SASL credentials, GSSAPI, OAuth) from a backup
or restore configuration. `--cluster source|target` picks the cluster and
defaults to `target`. An explicit `--bootstrap-servers` still overrides the
file's list. It works on `offset-reset execute`, `offset-reset-bulk` and
`offset-rollback snapshot | rollback | verify`.

`--sasl-mechanism OAUTHBEARER` without `--config` fails straight away with a
message that says this.

`validation run` takes `oauth` in its own configuration file, next to
`target`:

```yaml
backup_id: orders-daily
storage: { backend: s3, bucket: kafka-backups, region: eu-west-1 }
target:
  bootstrap_servers: ["pkc-yyyyy.westeurope.azure.confluent.cloud:9092"]
  security:
    security_protocol: SASL_SSL
    sasl_mechanism: OAUTHBEARER
    oauth: { provider: entra-restore, extensions: { logicalCluster: lkc-def456 } }
oauth:
  providers:
    entra-restore:
      type: oidc_client_credentials
      token_endpoint_url: https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token
      client_id: <restore app id>
      client_secret: ${ENTRA_RESTORE_CLIENT_SECRET}
      scope: api://<api app id>/.default
```

## 6. Observability and security

- `kafka_backup_oauth_token_requests_total{provider,kind,outcome}` counts
  identity-provider requests (`outcome` is `success` or `failure`). It is on
  the metrics endpoint of `backup`, `restore` and `three-phase-restore`
  whenever OAuth is in use.
- Client secrets and tokens never appear in logs, `Debug` output or
  serialised configuration; they are shown as `***`. Error text from the
  identity provider is scrubbed of secrets and JWT-shaped strings. A test
  runs every provider at TRACE, including the failure paths, and checks every
  captured log line.
- Use `SASL_SSL`. Over `SASL_PLAINTEXT` bearer tokens cross the network
  unencrypted, and Kafka Backup logs a warning.
- Tokens are not validated by Kafka Backup; the broker validates them.

## 7. Troubleshooting

| Symptom | Likely cause |
|---|---|
| `SASL OAUTHBEARER rejected by broker: invalid_token ... [OAuth provider '<name>'. ...]` | The identity provider issued a token and the cluster refused it. Wrong issuer (an Entra v1 token: set `requestedAccessTokenVersion: 2`), wrong audience (`scope` / `audience`), no identity pool matching the claims (`identityPoolId`, `logicalCluster`, the pool's filter), or the cluster has not refreshed the JWKS yet |
| `token endpoint returned HTTP 401 invalid_client` | Wrong client secret, or `client_auth` does not match what the application allows |
| `token endpoint returned HTTP 400 invalid_scope` / `invalid_resource` | `scope` does not name the API application (`api://<api app id>/.default`) |
| `token request failed: ...` | The identity provider is unreachable (DNS, proxy, firewall). A running backup keeps its current token until it expires |
| `OAuth configuration error: <cluster>.security.sasl_mechanism is OAUTHBEARER but <cluster>.security.oauth is not set` | Add `security.oauth: { provider: <name> }` to the cluster |
| `... refers to OAuth provider '<name>', which is not defined under oauth.providers (defined: ...)` | The binding's `provider` does not match a key of `oauth.providers` |
| `... is plain http, so credentials would cross the network unencrypted` | Use `https://`, or `allow_insecure_http: true` on an isolated test network |
| `SASL OAUTHBEARER needs a token provider, which flags cannot describe` | `--sasl-mechanism OAUTHBEARER` without `--config`; see [section 5](#5-offset-commands-and-validation) |
| `Cannot change principals during re-authentication`, then a reconnect | The token's subject changed (a rotated identity or token file). Harmless: the connection is re-established with the new identity |
| ``(azure_workload_identity): set `tenant_id` or the AZURE_TENANT_ID environment variable`` | The pod lacks the `azure.workload.identity/use: "true"` label, so the webhook did not inject the variables |

Run with `RUST_LOG=info,kafka_backup_core::oauth=debug` to see each token
fetch (`OAuth token fetched`, with the lifetime and the next refresh, never
the token).

## 8. Terraform

Resource names for automating the setup (providers `azuread`, `azurerm`,
`confluent`):

| Purpose | Resource |
|---|---|
| API application, v2 tokens | `azuread_application` (`api { requested_access_token_version = 2 }`, `identifier_uris`) and `azuread_service_principal` |
| Backup / restore client apps | `azuread_application`, `azuread_service_principal`, `azuread_application_password` |
| Managed identity | `azurerm_user_assigned_identity` (attach to the VM or VMSS) |
| AKS workload identity | `azurerm_user_assigned_identity` + `azurerm_federated_identity_credential` (or `azuread_application_federated_identity_credential` for an app registration) |
| Confluent identity provider | `confluent_identity_provider` (`issuer`, `jwks_uri`) |
| Pools | `confluent_identity_pool` (`identity_claim = "claims.sub"`, `filter`) |
| Role bindings | `confluent_role_binding` (`principal = "User:${confluent_identity_pool.backup.id}"`, `role_name`, `crn_pattern`) |

```hcl
resource "confluent_identity_provider" "entra" {
  display_name = "entra"
  description  = "Microsoft Entra ID"
  issuer       = "https://login.microsoftonline.com/${var.tenant_id}/v2.0"
  jwks_uri     = "https://login.microsoftonline.com/${var.tenant_id}/discovery/v2.0/keys"
}

resource "confluent_identity_pool" "backup" {
  identity_provider { id = confluent_identity_provider.entra.id }
  display_name   = "kafka-backup"
  description    = "Kafka Backup (read)"
  identity_claim = "claims.sub"
  filter         = "claims.aud == \"${azuread_application.confluent_api.client_id}\" && claims.oid == \"${azuread_service_principal.backup.object_id}\""
}

resource "confluent_role_binding" "backup_topics" {
  principal   = "User:${confluent_identity_pool.backup.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${data.confluent_kafka_cluster.prod.rbac_crn}/kafka=${data.confluent_kafka_cluster.prod.id}/topic=orders-*"
}
```

## 9. Testing locally

[`tests/sasl-oauth-test-infra/`](../tests/sasl-oauth-test-infra/README.md)
has a Docker Compose stack with an identity provider that signs real JWTs and
a Kafka 4.x broker that validates them against its JWKS, expected issuer and
audience, and forces re-authentication every 60 s:

```bash
docker compose -f tests/sasl-oauth-test-infra/docker-compose-oauth-idp.yml up -d --wait
cargo test -p kafka-backup-cli --test oauth_real_broker -- --ignored --nocapture --test-threads=1
```

The suite backs up and restores over OAUTHBEARER and compares record counts
and checksums, checks that a token with the wrong issuer or audience is
refused with the error above, soaks a continuous backup through repeated
re-authentication, and stops the identity provider mid-run.

## Using it as a library

`kafka_backup_core::oauth::install(&mut config)` turns the bindings of a
parsed `Config` into plugin factories on its clusters; call it before building
an engine. `OauthTokenProvider` is the trait to implement for a token source
of your own, and `RegistryOauth` adds a bearer token and Confluent Cloud's
`target-sr-cluster` / `Confluent-Identity-Pool-Id` headers to a `reqwest`
request from the same cache.
