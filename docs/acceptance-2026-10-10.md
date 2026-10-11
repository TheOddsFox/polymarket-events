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

The correction was committed as `89c409049d67ab9e74acb56d96122a56ae0c5e07`;
[exact-head hosted CI](https://github.com/TheOddsFox/polymarket-events/actions/runs/38073588587)
passed all 859 tests and the installed workflows. Every prior installation remains
retained. Read-only SQLite inspections may update its shared-memory readmarks;
they do not alter registered evidence or the main ledger's semantic state.

## Completed bootstrap capture and active full load

The same sealed batch was explicitly resumed without increasing allowances.
Invocations 7 onward use proof 7b; invocations 8 onward set the existing positive
`load.max_pages_per_run` control to 50. Admission against a frozen 200-page chunk
failed its conservative 8 GiB staging estimate; the same pages passed as four
50-page chunks. This changes chunk size, not capture scope or resource caps.

| Invocation | Attempts | Received bytes | Capture duration (seconds) | Added pages | Added records | Capture result |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 7 | 15,633 | 4,294,981,439 | 8,675.502 | 15,619 | 580,098 | Download allowance |
| 8 | 7,999 | 4,294,975,305 | 4,497.552 | 7,998 | 629,504 | Download allowance |
| 9 | 6,399 | 4,294,970,026 | 3,520.228 | 6,398 | 619,608 | Download allowance |
| 10 | 6,377 | 4,294,979,267 | 3,537.390 | 6,376 | 602,291 | Download allowance |
| 11 | 6,805 | 4,294,970,558 | 3,782.375 | 6,804 | 589,439 | Download allowance |
| 12 | 6,255 | 4,294,972,395 | 3,532.816 | 6,254 | 596,550 | Download allowance |
| 13 | 6,255 | 2,959,428,549 | 4,246.752 | 6,255 | 398,801 | Complete |

Bootstrap capture finished at **2026-10-11 02:59:23.888 UTC** with 69,954 committed
pages / 5,415,266 top-level source records. Cumulative accounting is 70,000 actual
attempts / 50,278,242,921 received bytes, including retries and partial responses.
All 136 core scans reached their original high-water marks. The existing follow-up
mechanism recovered 408 referenced parent events through five successful empty bulk
responses followed by five finite individual-ID scans. Committed scope and plans
agree with the ledger; only observed parent-ID provenance was added. This does not
expand numeric high-water marks or implement exhaustive inactive-ID discovery.

The automatic load began at 02:59:23.896 UTC under the same invocation. At
03:31:09–11 UTC, 3,050 pages / 296,850 source records were committed loaded.
The actual whole root measured 11,163,929,921 bytes and temporary storage
259,956,436 bytes, below the unchanged 64/8 GiB limits. These sequential active-run
measurements are points, not peaks. Warehouse build, certification and publication
have not yet passed.

A frozen recent closed-market audit found 1,485 actual v1 markets / 2,970 outcomes,
including 530 records with both native-ID arrays populated. Checksums, source
bounds, exact decimals, nullable fields, memberships and metadata.v1 parity passed;
zero malformed identities or ownership conflicts occurred in this sample.
The sample contained no v2/position-only records; synthetic coverage supplies
those cases. This read-only audit made no source requests and does not certify
the full warehouse.

Static load investigation found dlt configuration resolver traces accumulate
independently of disabled pipeline tracing. A scoped fix disables that collector
before runtime initialization and restores any caller's context and history on
exit. Architecture and security review approved its boundary. Eighteen focused
tests passed, including caller-history restoration, initialization failure,
interruption, nesting, named-worker propagation and repeated synthetic loads.
Substituting the pre-fix function in an isolated test process failed 12 new cases.
Independent verification passed the same 18 tests and lint/format/diff checks.
The first full regression run passed 871 tests and failed one adversarial smoke
test before its deliberately stalled operation was reached. The test's five-second
budget included parent setup and isolated-child imports. A focused rerun reproduced
deadline exhaustion before child launch; the exact cause of the original timeout
is unavailable. The harness now separates a finite 30-second startup allowance
from the unchanged 0.15-second operation deadline, armed only after authenticated
helper entry. A deterministic 5.1-second startup delay passes; PID/group identity,
termination, stopped heartbeat and failed-report assertions remain required.
Independent verification passed 58 connected tests. Production deadlines are
unchanged. The full-suite rerun passed 870 tests and failed three existing dbt
integration cases at pytest's 120-second timeout (Dagster validation, warning-band
quality accounting and empty-catalogue publication), in 1,100.67 seconds. Both new
smoke-startup cases and the tracing tests passed. This remains a failed full gate;
retain both logs and rerun unchanged checks at a quiescent live-run boundary.
No resource or test timeout is increased. The active installed load is unchanged.
No source-row retention leak or causal explanation of its measured RSS is proven.

Installed proof 8 passed in 133.438 seconds; all 77 code/dbt resources match the
checkout, wheel, sdist and installation, and all 38 dbt resources remain read-only.
Wheel SHA-256: `ec6ce32b0ae1648e2eea184884aba31b94b892d823bbab4206229945cdda1609`;
sdist SHA-256: `7bed15916275dd5de9c37739d350ff808d5fd115e3bc76f54490d39fa9ed0564`.
Both offline workflows passed, including the full semantic and recovery comparisons.

The repeat real proof-8 smoke ran from 03:50:06.145883 to 03:51:51.570805 UTC,
passing in 105.424598 seconds with two actual requests, 15,384 downloaded bytes
and 54,190,588 whole-output retained bytes. It checked 29 semantic warehouse
relations, 30 schemas and seven publications, replay, raw rebuild, verified
restoration and previous-pointer preservation after injected failure. Its complete
accounting remains within all smoke bounds. This still does not prove full readiness.

At 03:44:12–03:45:57 UTC the unchanged bootstrap load had committed 3,250 pages.
Sequential active-run walks measured 16,214,103,048 whole-root bytes and
1,727,783,300 temporary bytes. Later attribution points in that walk included
9,904,599,040 warehouse bytes, 132,793,367 WAL bytes, 342,090,116 dlt bytes and
1,385,693,184 operator temporary bytes. These are neither an atomic sum nor peaks.
The owned process RSS fell to 2,096,560 KiB at 03:46:07 UTC before this run received
any code change; the earlier larger sample therefore does not prove a leak.

At the 04:20:24–04:22:33 UTC observation, 3,750 pages / 364,299 source records
were loaded. The 04:50:23–04:51:36 UTC resource checkpoint found 4,000 loaded
pages / 388,777 records, stable during the 1.782793-second accounting walks.
Whole-root retention was 16,226,162,323 bytes and temporary storage 543,565,862
bytes. Exact owned-PID RSS was 2,913,664 KiB / CPU 99.9% at 04:51:23 UTC.
These active points are within the unchanged limits; full fit remains unverified.
No warehouse build or publication had completed. Source commits are
held while this invocation runs: the legacy audit `git_sha` samples enclosing
checkout HEAD separately at each stage and does not identify the installed binary.
Future prepared commands require separate committed-source, installed-content and
wheel provenance. Their pending source SHA fails closed before operator-root access;
independent security review passed their 16-operation / 68-comparison preservation.
Those commands remain unexecuted.

At 05:16:58.273768 UTC, a supervised operator intervention sent one SIGINT to
the registered CLI child after checking its PID, start time, parent and UID.
Independent architecture and security review passed the interruption/restart
procedure. The CLI and helper both terminated with exit 1; the helper's original
`unexpected_failure` classification is retained. The interrupt occurred during
the read-only persisted-observation conflict check, not during dlt extraction.
The unscoped load stage ended at 05:17:03.442 UTC with
`RuntimeError: Query interrupted`. This was an intentional interruption to obtain
a quiescent verification boundary; it is not a successful load.

The quiescent audit found both owned processes absent and the existing writer
lock available without changing its contents, size or modification time.
All 146 declared scans remain complete and the batch remains captured:
4,150 pages / 403,755 source records are loaded, with 65,804 pages pending.
Build certification is dirty. Whole-root retention is 16,252,199,773 bytes and
temporary storage 573,797,504 bytes. There are 249 retained loaded dlt package
directories and no new or normalized package directories; directory counts do
not establish semantic package verification. All evidence is retained, and the
unchanged limits still apply. No replacement writer or later stage has started.

The unchanged three timeout regressions then passed in 43.75 seconds, with the
same 120-second pytest limit and offline fixtures. Dagster validation, warning-band
build/certification and empty-catalogue publication completed their assertions.
The full unchanged `scripts/verify` then passed all 873 tests in 391.96 seconds,
along with lock/install, lint/format, dbt parse and diff checks, in an isolated
temporary root. Invocation 13's completion summary has SHA-256
`7c9bc5f63bccd0dc7d76d68211f4462a8dacc405849c385fbf377ecc37dd566b`.
The two earlier failed full runs remain retained. Catalogue-wide load and all
subsequent acceptance gates remain pending.

Remaining gates: complete bootstrap load/build/publication, daily refresh and reconcile; immutable release
verification; full-root offline replay and semantic raw rebuild; verified fresh backup
restoration; final acceptance evidence commit and passing hosted CI for that commit.
Scheduling remains inactive. Provider availability, full-load spill demand, cumulative
temporary job storage and total catalogue runtime remain unverified.
