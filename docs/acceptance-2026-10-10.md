# Catalogue acceptance evidence

Status at 2026-10-10 17:52 UTC: **partial readiness**. The committed implementation,
installed package and bounded real workflow pass. Catalogue-wide bootstrap has
resumed past the repaired adaptive-response failure and is paused at a verified
checkpoint after a whole-root storage correction passed local, installed, real-smoke
and independent review gates. Its commit and hosted CI remain pending.
This document does not certify full operational readiness.

## Reviewed implementation

- Adaptive-repair baseline: `abc95bc34346ae297d2434c41bfe5016b0fe342a`.
- [Draft pull request](https://github.com/TheOddsFox/polymarket-events/pull/3).
- `scripts/verify-fast`: 838 tests passed in 429.00 seconds.
- `scripts/verify`: 839 tests passed in 386.97 seconds.
- [Hosted CI](https://github.com/TheOddsFox/polymarket-events/actions/runs/38067809505):
  839 tests passed in 863.91 seconds; the separate installed-package workflow passed.
  All job steps passed in 16 minutes 30 seconds.
- Independent architecture, data, security and verification reviews passed.

The installed proof compared all 77 code/dbt resources across the checkout, wheel,
sdist and installed package. All 38 dbt resources remained read-only and unchanged.
The audited installed workflow ran outside the checkout using offline synthetic
sources and a fresh hash-locked dependency installation.

## Bounded real workflow

Public market `5234660`, fresh operator root, production Gamma HTTPS, one worker.
The shared smoke limits were 100 actual requests, 64 MiB downloaded, 1 GiB retained
across the whole output and a 30-minute deadline.

| Measurement | Result |
| --- | ---: |
| Actual HTTP attempts | 2 |
| Downloaded bytes | 15,442 |
| Whole-output retained bytes | 54,188,120 |
| Duration | 126.520459 seconds |
| Market observations / current markets / outcomes | 2 / 1 / 2 |

Capture, load, complete build, publication and a second observation passed. Offline
replay, raw rebuild, backup verification, fresh restoration and injected publication
failure preservation passed. Semantic comparisons covered 29 warehouse relations,
30 schemas, seven public projections, coverage and capture inventory. This selected
market had no captured parent event; event discovery is not established by this run.

The tested wheel SHA-256 was
`e3d1cb45e485f6a123cbf4ae813c8753600b514c7f4ad852ead3814b59a57cd0`.
The metadata.v1 handoff and existing downstream smoke-report fields remain supported.

## Catalogue-wide acceptance in progress

Named bootstrap batch: `20261010T142904Z-bootstrap`. Sealed source high-water marks:
events `1164836`, markets `5503269`. No expanding tail or exhaustive inactive-ID
sweep is enabled. Every invocation retains the agreed limits: one worker, 2 requests
per second, 25,000 attempts, 4 GiB download, four hours, 16 MiB response, 64 GiB root,
8 GiB temporary storage and 2 GB DuckDB memory.

The first invocation stopped at its download allowance with exit 3 after 4,965
attempts, 4,294,978,137 received bytes, 2,548.366 seconds and 4,962 committed pages
containing 484,027 top-level records. The final partial chunk counts toward measured
bytes. No warehouse publication occurred. The same named batch was explicitly resumed
with a new finite allowance after verifying its committed evidence. The original
installed environment is retained. Invocations 2 and 3 used the separately retained
proof-5 wheel `d9cb1ef055210a1103a75360739a143e774124e9061d1a2792bfc37de117e0fb`;
invocations 4–6 used the proof-6 wheel tested above.

Invocation 2 stopped at the same download cap after 3,675 attempts, 4,294,973,904
received bytes, 1,961.625 seconds, 3,674 pages and 365,271 records. Invocation 3
then failed on canonical JSON expansion after 17 attempts, 74,094,421 bytes,
194.358 seconds, 12 pages and 1,195 records. Individually bounded split responses
had been recombined into an oversized synthetic page. The original evidence remains
retained; no incomplete capture was loaded or published.

The implemented fix commits each native response leaf with explicit page-unit revision 2
and resumes from verified source bounds or frozen-list positions. Legacy pages remain
readable, and every original scope and resource bound stays unchanged. Sixty
adversarial unit tests and a full adaptive-page warehouse regression passed.
The latter proved all 68 semantic/schema/publication/coverage/inventory comparisons
with source requests forbidden during rebuild. Independent architecture, data,
security and verification reviews passed. `scripts/verify-fast` passed 838 tests in
429.00 seconds. The completion gate,
`scripts/verify`, passed all 839 tests in 386.97 seconds, including the additional
adaptive warehouse regression. The reviewed fix was committed and pushed; its
exact-head hosted CI passed. Invocation 4 explicitly resumed the same named batch
at 16:28:51 UTC after verifying retained evidence.

The formerly failing window committed three contiguous native responses for IDs
580701–580725, 580726–580750 and 580751–580800. Their bodies are 16,617,311,
308,230 and 311,981 bytes, each below the unchanged 16,777,216-byte limit. Their
17,237,522-byte aggregate explains the former failure. Installed-reader verification
confirmed hashes, target IDs, predecessor continuity, receipt provenance and 100
records without source requests. That partition completed with 502 pages and 49,747
records; subsequent partitions are progressing.

At 16:45:05–06 UTC, 10,149 pages and 999,782 top-level records were committed.
The installed resource measurement found 1,056,981,907 whole-root bytes and zero
temporary bytes in 0.230 seconds, with stable page counts during the walk. This is
a point measurement, not a peak-storage claim.

A separate audited installation passed both offline workflows in 54.038 seconds.
Its wheel SHA-256 is
`f848330beed25375d550c85a1bb1f51283c7580c70b1e5db55810fa8a1e29da9`.
The repeat real selected smoke passed in 42.646549 seconds with two attempts,
15,422 downloaded bytes and 54,184,612 retained bytes. Replay, rebuild, restoration
and previous-pointer preservation passed; these still do not certify catalogue-wide
readiness.

The six bootstrap invocation outcomes are retained separately; an allowance exit
leaves the named batch incomplete and resumable.

| Invocation | Attempts | Received bytes | Duration (seconds) | Added pages | Added records | Result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 4,965 | 4,294,978,137 | 2,548.366 | 4,962 | 484,027 | Download allowance |
| 2 | 3,675 | 4,294,973,904 | 1,961.625 | 3,674 | 365,271 | Download allowance |
| 3 | 17 | 74,094,421 | 194.358 | 12 | 1,195 | Adaptive aggregation failure, repaired |
| 4 | 2,395 | 4,294,974,709 | 1,464.434 | 2,392 | 237,671 | Download allowance |
| 5 | 1,676 | 4,294,975,150 | 1,172.558 | 1,665 | 164,731 | Download allowance |
| 6 | 1,549 | 4,294,969,061 | 1,277.634 | 1,545 | 146,080 | Download allowance |

After invocation 6, 14,250 pages / 1,398,975 top-level records are committed;
14,277 attempts / 21,548,965,382 received bytes / 8,618.975 capture-stage seconds
are accounted. Invocation 5 included one transport error and its retry. Nothing
has been loaded or published from this incomplete bootstrap. The verified next
ID is 1,139,501 in the final event partition; market partitions remain pending.
At 17:39:53 UTC, the quiescent root contained 2,042,618,248 bytes and no temporary
data, measured in 0.309 seconds. A single owned-process sample during invocation 6
was 279.4375 MiB RSS; neither measurement establishes a peak.

## Whole-root storage correction

Review found that configured data/state/warehouse paths were counted, while an
unrelated file elsewhere under the operator root could bypass its retained limit.
The correction counts the whole root plus configured external paths, deduplicates
nested paths, normalizes relative parent segments and rejects original symlink
ancestors before normalization. CLI, Dagster, Make, wrapper logs and inactive
launchd examples now agree on the dedicated `./data` default; explicit overrides
remain authoritative. Existing roots and capture semantics are unchanged.

Twenty regression cases cover root extras, external paths, scratch deduplication,
unsafe paths and operator defaults. Independent architecture and security review
passed; 64 focused checks passed. Independent verifier review passed 76 connected
checks in 181.60 seconds. `scripts/verify` passed all 859 tests in 574.73 seconds,
including lock/install, lint, formatting, dbt parse and diff checks.

Fresh installed proof 7b passed both offline workflows in 89.488 seconds. All 77
code/dbt resources match the checkout, wheel, sdist and installation; all 38 dbt
resources remain read-only and unchanged. The wheel is 154,276 bytes / 82 members,
SHA-256 `e3d1cb45e485f6a123cbf4ae813c8753600b514c7f4ad852ead3814b59a57cd0`.
The sdist is 197,909 bytes / 86 members,
SHA-256 `60cb5425a89c00e5eeadc2400e7278ec32c81013d6bd9fa2a920e6a064634d71`.
The first sandboxed installed check stopped because it could not bind its local
fixture server; that failed evidence remains retained. The permitted loopback-only
rerun passed. The repeat real smoke above used this new wheel and passed all recovery
and semantic comparisons with complete request accounting.

Commit and exact-head hosted CI remain pending for this correction. Every prior
installation remains retained. Live acquisition will switch only at an explicit
continuation boundary. Read-only SQLite inspections may update its shared-memory
readmarks; they do not alter registered evidence or the main ledger's semantic state.

Remaining gates: complete bootstrap, daily refresh and reconcile; immutable release
verification; full-root offline replay and semantic raw rebuild; verified fresh backup
restoration; final acceptance evidence commit and passing hosted CI for that commit.
Scheduling remains inactive. Provider availability, full-load spill demand, cumulative
temporary job storage and total catalogue runtime remain unverified.
