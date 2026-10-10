# Catalogue acceptance evidence

Status at 2026-10-10 16:27 UTC: **partial readiness**. The reviewed implementation,
installed package and bounded real workflow pass. Catalogue-wide acceptance stopped
on an adaptive-response bug; its fix passed local and installed gates before resume. This document does
not certify full operational readiness.

## Reviewed implementation

- Code commit: `a9a5d8c4992dd86d886fac1bc8a625a83612f64d`.
- [Draft pull request](https://github.com/TheOddsFox/polymarket-events/pull/3).
- `scripts/verify-fast`: 778 tests passed in 395.36 seconds.
- `scripts/verify`: 778 tests passed in 392.75 seconds.
- [Hosted CI](https://github.com/TheOddsFox/polymarket-events/actions/runs/38062376348):
  778 tests passed in 840.85 seconds; the separate installed-package workflow passed.
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
| Downloaded bytes | 15,366 |
| Whole-output retained bytes | 54,181,287 |
| Duration | 39.551275 seconds |
| Market observations / current markets / outcomes | 2 / 1 / 2 |

Capture, load, complete build, publication and a second observation passed. Offline
replay, raw rebuild, backup verification, fresh restoration and injected publication
failure preservation passed. Semantic comparisons covered 29 warehouse relations,
30 schemas, seven public projections, coverage and capture inventory. This selected
market had no captured parent event; event discovery is not established by this run.

The tested wheel SHA-256 was
`d9cb1ef055210a1103a75360739a143e774124e9061d1a2792bfc37de117e0fb`.
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
installed environment is retained; the resumed invocation uses the tested wheel above.

Invocation 2 stopped at the same download cap after 3,675 attempts, 4,294,973,904
received bytes, 1,961.625 seconds, 3,674 pages and 365,271 records. Invocation 3
then failed on canonical JSON expansion after 17 attempts, 74,094,421 bytes,
194.358 seconds, 12 pages and 1,195 records. Individually bounded split responses
had been recombined into an oversized synthetic page. The original evidence remains
retained; no incomplete capture was loaded or published.

The implemented fix commits each native response leaf with explicit page-unit revision 2
and resumes from verified source bounds or frozen-list positions. Legacy pages remain
readable, and every original scope and resource bound stays unchanged. Sixty adversarial unit tests and a full adaptive-page warehouse regression passed.
The latter proved all 68 semantic/schema/publication/coverage/inventory comparisons
with source requests forbidden during rebuild. Independent architecture, data,
security and verification reviews passed. `scripts/verify-fast` passed 838 tests in 429.00 seconds. The completion gate,
`scripts/verify`, passed all 839 tests in 386.97 seconds, including the additional
adaptive warehouse regression. The same named batch is ready to resume after
committing the reviewed fix.

A separate audited installation passed both offline workflows in 54.038 seconds.
Its wheel SHA-256 is
`f848330beed25375d550c85a1bb1f51283c7580c70b1e5db55810fa8a1e29da9`.
The repeat real selected smoke passed in 42.646549 seconds with two attempts,
15,422 downloaded bytes and 54,184,612 retained bytes. Replay, rebuild, restoration
and previous-pointer preservation passed; these still do not certify catalogue-wide
readiness.

Remaining gates: complete bootstrap, daily refresh and reconcile; immutable release
verification; full-root offline replay and semantic raw rebuild; verified fresh backup
restoration; final acceptance evidence commit and passing hosted CI for that commit.
Scheduling remains inactive. Provider availability, full-load spill demand, cumulative
temporary job storage and total catalogue runtime remain unverified.
