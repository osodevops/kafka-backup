# Security policy

## Reporting a vulnerability

Please **do not** open a public GitHub issue for a security problem.

Report it to security@oso.sh (or through
[GitHub private vulnerability reporting](https://github.com/osodevops/kafka-backup/security/advisories/new)
on this repository). Include the kafka-backup version, the storage backend and
Kafka version involved, a description of the issue, and steps to reproduce if
you have them.

You will receive an acknowledgement within two business days and a
severity assessment within five business days. We follow coordinated
disclosure: we ask for up to 90 days to ship a fix before details are
published, and we credit reporters in the advisory unless they prefer not to
be named.

## Supported versions

Security fixes are provided for the current and the previous **minor** release
of kafka-backup (`0.N` and `0.N-1`) and are backported to both. Older versions
keep working but receive no fixes.

## How fixes are published

- A GitHub Security Advisory on this repository (and on the
  [Strimzi Backup Operator](https://github.com/osodevops/strimzi-backup-operator)
  or the enterprise distribution when they are affected), with a CVE where
  applicable.
- A patch release for every supported minor version, noted in the CHANGELOG.
- Direct notification to Enterprise licence holders.

## Scope notes

- kafka-backup never phones home. Storage credentials come from the config
  file, environment variables or the cloud provider's identity mechanism
  (IRSA, Azure Workload Identity, GCP Workload Identity) — the latter is
  preferred; see `docs/storage_guide.md`.
- Release artefacts (container images, binaries, crates) are built in CI and
  published with SHA-256 checksums; verify them before deploying.
- Signed evidence reports (`evidence` commands) are the mechanism for proving
  a backup or restore ran unmodified; see `docs/` for the verification flow.
