# Validation evidence contract

Status: implemented (issue #138). This document is normative for the byte-level
behaviour of evidence artifact creation and verification in
`kafka-backup-core/src/evidence/` and both the OSS and enterprise CLIs, which
share that implementation.

## Problem this contract fixes

The original (v1) implementation computed the report digest while
`integrity.report_sha256` was empty, then populated the field on the in-memory
report only. The stored/signed JSON therefore carried an empty
`report_sha256`, while the PDF (generated from the in-memory report) displayed
a populated value that was not the digest of any stored artifact — and hashing
a report that contains its own hash is self-referentially unsatisfiable in the
first place. Contract v2 removes the embedded digest entirely and moves
digest + signature into a detached, versioned envelope covering the exact
stored bytes.

## Contract v2 (current)

### Report payload (schema `1.1`)

- Built by `evidence::build_evidence_report` (single shared constructor).
- `integrity.report_sha256` is never serialized (empty + `skip_serializing_if`).
  The report does not embed its own digest.
- Serialized exactly once by `evidence::emit_evidence`:
  - signing enabled → `EvidenceReport::to_deterministic_json()`:
    compact JSON of a `serde_json::Value` (BTreeMap-backed objects → keys
    sorted by UTF-8 byte order), UTF-8, no whitespace;
  - signing disabled → `to_pretty_json()` for readability.
- The serialized bytes are stored verbatim at
  `{prefix}{report_id}/{YYYY}/{MM}/{report_id}.json` and are the **only**
  bytes any digest or signature refers to.

### Digest

- `report_sha256 = lowercase hex SHA-256(stored JSON bytes)`.
- Computed after serialization; shown identically by the CLI
  (`Report SHA-256 (stored JSON bytes): …`), recorded in the envelope, and
  printed in the PDF (`Report SHA-256 (stored JSON bytes): …`). The PDF and
  CLI can never disagree because both display the value returned by
  `emit_evidence` for the same byte sequence.

### Signature envelope (schema `kafka-backup/evidence-envelope/v2`)

Stored at `{prefix}{report_id}/{YYYY}/{MM}/{report_id}.sig` as pretty JSON:

```json
{
  "schema": "kafka-backup/evidence-envelope/v2",
  "payload_type": "application/vnd.kafka-backup.evidence-report+json",
  "payload_sha256": "<hex sha-256 of the stored report bytes>",
  "payload_size_bytes": 12345,
  "report_id": "validation-…",
  "algorithm": "ECDSA-P256-SHA256",
  "signature": "<base64 standard encoding>",
  "signed_at": "<RFC 3339, informational only>"
}
```

The signature is a **DSSE v1 signature**: ECDSA P-256 with SHA-256 over

```
PAE(payload_type, payload) =
  "DSSEv1" SP LEN(payload_type) SP payload_type SP LEN(payload) SP payload
```

where `payload` is the exact stored report bytes, `LEN` is ASCII decimal byte
length, and SP is 0x20 — exactly as specified by the DSSE protocol
(<https://github.com/secure-systems-lab/dsse/blob/master/protocol.md>). The
implementation pins the spec's own test vector:
`PAE("http://example.com/HelloWorld", "hello world") =
"DSSEv1 29 http://example.com/HelloWorld 11 hello world"`.

`payload_sha256` and `payload_size_bytes` are unsigned hints used for precise
diagnostics; verification enforces the digest match **and** the signature
independently, so tampering with the hints fails verification.

### Verification (`evidence::verify_evidence`)

1. Read the stored report bytes and signature artifact.
2. Detect the artifact version by content (`{` + `schema` field → v2 JSON
   envelope; `-----BEGIN KAFKA BACKUP EVIDENCE SIGNATURE-----` → legacy v1).
   Unknown envelope schemas are rejected with an explicit error, never
   reinterpreted.
3. Recompute SHA-256 over the stored bytes **as read** — no JSON parsing,
   re-serialization, or canonicalization affects the digest or signature
   check — and compare with the recorded digest.
4. With a public key: verify ECDSA P-256 over `PAE(payload_type, payload)`
   (v2) or over the raw stored bytes (legacy v1).
5. Report a structured outcome (contract version, both digests, digest/
   signature validity, report `schema_version`).

### Emission ordering (issue #138 root cause elimination)

`emit_evidence` performs: serialize once → hash → sign → generate PDF (digest
passed in) → upload JSON/PDF/envelope with one shared timestamp for the
year/month key prefix. Signing failures abort before anything is uploaded.
When signing is enabled the JSON payload is always stored, even if
`evidence.formats` omits `json` (a detached envelope without its payload is
unverifiable); a warning is emitted in that case.

## Legacy contract v1 (report schema `1.0`)

Emitted by earlier releases; **verification remains supported and tested**:

- Report JSON: compact (signed) or pretty (unsigned) serialization that
  contains `integrity.report_sha256` — **empty in signed/stored artifacts**
  because of the original bug. Verification does not interpret this field.
- Signature artifact: text format
  `-----BEGIN KAFKA BACKUP EVIDENCE SIGNATURE-----` with `Algorithm`,
  `Report-ID`, `Report-SHA256`, `Signature` lines; ECDSA P-256 signature over
  the raw stored report bytes (no PAE).
- `verify_evidence` verifies the digest and signature over the stored bytes
  exactly as v1 wrote them. Artifacts affected by the original bug (empty
  embedded `report_sha256`) verify correctly because the digest/signature
  cover the stored bytes, not the in-memory report.

Migration: nothing re-signs old evidence. Old artifacts verify through the
legacy path forever; new artifacts are only emitted as v2. A v1 verifier
build cannot verify v2 envelopes (it fails with an explicit parse error, not
a false result).

## Canonicalization position (research record)

Primary sources reviewed on 2026-08-12 before fixing this design, as required
by the cyber-recovery handoff:

- **RFC 8785 (JSON Canonicalization Scheme)**
  <https://www.rfc-editor.org/rfc/rfc8785>
  - §3.2.3: property names sort by **UTF-16 code units**;
  - §3.2.2.3: numbers serialize per ECMA-262 §7.1.12.1 (e.g. `1e+21`,
    `-0` → `0`);
  - §3.2.2.2: two-char escapes for `\b \t \n \f \r`, lowercase `\uhhhh` for
    other control characters; §3.2.1 no inter-token whitespace; §3.2.4 UTF-8.
- **DSSE envelope + protocol**
  <https://github.com/secure-systems-lab/dsse/blob/master/envelope.md>,
  <https://github.com/secure-systems-lab/dsse/blob/master/protocol.md>
  - the envelope `payload` field is REQUIRED (embedded base64);
  - signatures cover `PAE(payloadType, payload)`; DSSE deliberately avoids
    JSON canonicalization ("Implementations MUST ensure that the same payload
    bytes that are verified are the ones sent to the application layer").
- **in-toto attestation envelope**
  <https://github.com/in-toto/attestation/blob/main/spec/v1/envelope.md>
  - uses DSSE with `payloadType: application/vnd.in-toto+json`.

Decisions and deliberate deviations:

1. **`to_deterministic_json` is NOT RFC 8785 and is never described as
   canonical.** Divergences proven by probing the pinned serde_json build
   (and locked in by the `deterministic_json_is_not_rfc8785` unit test so a
   future serde_json change is noticed):
   - object keys sort by UTF-8 byte order, not UTF-16 code units (RFC 8785
     §3.2.3) — ordering differs when supplementary-plane characters mix with
     U+E000..U+FFFF;
   - `-0.0_f64` serializes as `-0.0`; JCS requires `0`;
   - `0.000001_f64` serializes as `1e-6`; ECMAScript (and therefore JCS)
     prints `0.000001`;
   - u64/i64 beyond 2^53 serialize at full precision, which ECMA-262 double
     formatting cannot produce.
   Notably `1e21_f64` → `1e+21` and control-character escaping (lowercase
   four-digit hex unicode escapes; two-char escapes for backspace, tab,
   newline, form feed, carriage return) do match JCS on the current
   serde_json — the deviation list above is what provably differs.
2. **No canonicalization at verification time, by design.** The contract
   follows DSSE's byte-oriented model: digest and signature cover the exact
   stored bytes; determinism of the producer's serialization is a convenience
   (reproducible emission), not a verification dependency. This removes the
   entire class of canonicalization/parsing mismatch vulnerabilities that
   JCS-based signing must defend against, and is why a JCS dependency was
   evaluated and rejected.
3. **Detached payload (deviation from DSSE envelope.md).** DSSE requires the
   payload embedded base64 in the envelope. Our report must remain a directly
   readable JSON object in object storage for auditors, so the v2 envelope
   references the payload by digest/size instead of embedding it. The
   signature itself remains exactly DSSE v1 (`PAE` + ECDSA P-256), so a v2
   artifact can be re-wrapped as a spec-compliant DSSE envelope
   (`{payloadType, payload: base64(stored bytes), signatures:[{sig}]}`)
   without re-signing.
4. **`Ed25519` was not adopted** to preserve compatibility with existing
   ECDSA P-256 keys and the legacy v1 verification path.

## Test coverage

- `evidence::envelope` unit tests: DSSE PAE spec vector, empty-payload PAE,
  v2 sign/verify roundtrip, tampered payload, tampered envelope signature,
  wrong key, unknown envelope schema rejection, legacy v1 roundtrip +
  tampering, garbage artifact rejection.
- `evidence_contract_test.rs` integration tests (MemoryBackend): emit →
  reload stored JSON + envelope from the backend → recompute digest → verify
  ECDSA → assert schema versions and that stored JSON has no `report_sha256`;
  tampered JSON, tampered envelope, wrong key, legacy v1 artifacts (including
  ones reproducing the original blank-`report_sha256` bug); determinism
  vectors; unsigned-emission digest consistency; PDF/CLI digest consistency.
