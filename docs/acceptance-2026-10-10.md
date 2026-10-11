# Catalogue acceptance evidence

Status at 2026-10-11 14:12 UTC: **partial readiness**. Bootstrap capture is complete;
the original load stopped at a verified checkpoint of 8,200 loaded pages and
61,754 pending. Its evidence and warehouse remain retained. The compact warehouse
implementation passed native, installed, real-smoke and exact-revision hosted CI
checks. Further streaming and directory-relative accounting refinements passed
the shared native, installed and real-smoke checks; exact-revision hosted CI is
pending. A raw-only copy into a fresh root is in progress. Fresh-root adoption and
catalogue-wide build, publication and recovery acceptance have not passed.
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

The refinement was committed and pushed on the existing feature branch as
`e8953f4ef9095fddda24654b312a53b5d662c66a`; `main` remains unchanged.
[Hosted CI](https://github.com/TheOddsFox/polymarket-events/actions/runs/38114947814)
passed: 873 tests in 593.12 seconds and the installed-package workflow.
The finalized immutable runtime manifest binds that commit to the existing
proof-8 wheel, CLI and 77 code/dbt files; SHA-256
`e3c7def5d19c8f7a8426b4e3156326f5c8051e864704f62c3f796dd980e0b26b`.
Verification of this binding accessed no operator-root data.

A separately supervised same-batch offline load was authorized with unchanged
configuration, working directory, normalization, schema and load50 boundaries.
Its helper began complete-capture verification at 05:37:29 UTC; no load child or
terminal outcome had yet been reported at 05:44 UTC. The reviewed launcher pins
the original terminal evidence and 4,150 committed checkpoints, verifies raw
inventory before and after, and starts only `load --batch-id` for the named
bootstrap. It does not acquire source data or continue to build/publication.
The launcher passed 107 self-checks; independent security review passed focused
rejection and valid-state checks. Catalogue-wide fit and recovery remain unverified.

The proof-8 load child started at 05:45:06.574447 UTC. Its first observer at
05:48:54–58 UTC still found the original 4,150 committed pages. A narrow sample
of that registered child at 05:50:30 UTC found it active, with 96.1% CPU and
2,791,488 KiB RSS. These are point measurements, not evidence of a completed
chunk, peak usage or full-catalogue fit. No further acquisition, build or
publication was started.

The first new 50-page chunk committed at 05:50:29.929 UTC. At 05:56:36–47 UTC,
the named offline load reached 4,400 committed pages / 428,577 source records,
with 65,554 pages pending. No subsequent stage ran.

A separate source refinement reduces persisted-observation validation to one
parameterized query per populated relation and consumes every match in batches
of 512. All projections and exact comparisons remain unchanged, including
conflicting persisted duplicates. Architecture, data and actual-patch security
review passed. The old function failed five query/stream regressions; the
refinement passed all 37 focused cases and independent verification, plus
lint/format/diff checks. A SQL-only synthetic benchmark returned identical rows
with 32 queries reduced to one; its measured speed gain was modest. It neither
diagnoses the live timing nor proves internal DuckDB memory use or full fit.
This refinement is uncommitted and uninstalled; the active loader remains on
the verified proof-8 package. Full-suite and distribution gates remain pending.

At the 06:31:14–06:35:10 UTC resource checkpoint, the named offline load had
committed 5,050 pages / 492,704 source records. Sequential installed resource
walks measured 19,836,798,446 whole-root bytes and 846,285,057 temporary bytes,
within the unchanged 64 GiB and 8 GiB limits. These are active points, not peaks
or proof of final fit. The observer's typed settings matched the pinned effective
configuration and the reviewed installed settings loader. A failed private
observer's incomplete measurement and correction are retained separately.

The supervised operator sent one SIGINT to the registered CLI child at
06:38:41.695704 UTC after checking its exact identity. The child exited 1 at
06:41:06.184582 UTC, with the named load stage failed at 06:41:05.860 UTC and
`RuntimeError: Query interrupted` in the read-only conflict check. The actual
checkpoint was 5,100 loaded pages / 64,854 pending, beyond the pause-point
measurement. The supervisor terminated with exit 1 and preserved its original
`failed_retained_for_explicit_recovery` classification. Its capture/schema/runtime
postflight passed. The narrow final audit confirmed both owned processes absent
and the existing lock available without changing its contents, size or mtime.
The 5,100 committed pages contain 497,674 source records. The batch remains
captured, certification dirty and the publication pointer absent.

The supervisor's quiescent 06:48:42–43 UTC resource point measured
19,848,848,330 retained bytes and 860,353,773 temporary bytes. All caps remain
unchanged. Operational dlt inventory has 306 loaded directories / 860,340,299
bytes, with no new or normalized directories; those counts do not verify package
semantics. No captures, checkpoints or packages were pruned. No replacement
writer or later catalogue-wide stage has started.

The conflict-query refinement then passed isolated `scripts/verify`: all 910
tests in 399.69 seconds, with unchanged test deadlines, plus dependency,
lint/format, dbt parse and diff checks. Log SHA-256:
`d4ba047aeaf86c7ffaa6d2c11ddee28c3e4150af226341bb03471b6c808c4aff`.
Installed proof 9 passed in 54.426 seconds outside the checkout, including
read-only dbt resources, package-content auditing and both offline workflows.
Its catalogue workflow measured ten HTTP attempts / 20,530 bytes and compared
29 semantic relations, 30 warehouse schemas and seven published relations,
including replay, raw rebuild, verified restoration and failure preservation.
Its metadata workflow measured one attempt / 1,220 bytes and remained compatible.

Proof-9 wheel SHA-256:
`14f4790c51c4920cf23213b6e069f0471f9805b94c0b5c8620e840ba239f60a4`
(154,493 bytes / 82 members); sdist:
`448ca1ee32874bed3eac5eaee83a9460b1c2aa726ac9962c4ebb92cb0b47a289`
(204,219 bytes / 86 members). Installed report SHA-256:
`9acaad2a779b194cff346102d01fd3c9a5ea852d5cab37417d1944664ba3b857`.
The proof-9 real-data smoke passed in 39.403776 seconds at 07:02:32–07:03:11 UTC
with two requests, 15,418 downloaded bytes and 54,190,700 whole-output retained
bytes. Market `5234660` provided two observations, one published market and two
CTF outcomes, with no quarantine. All semantic/recovery/failure-preservation checks
passed within the unchanged smoke limits. Report SHA-256:
`303e8de142b4b88e59bcd9e6a5f8b955bb549d463f8e07bc0169ade5378724f0`.
Committed-source binding and full-load resume remain pending. The private restart
check has separate actual terminal and 5,100-prefix
bindings. Its reviewed projection correction permits only the omitted null dbt
field in the compact audit; the complete stage and every other field remain exact.

The conflict-query refinement was committed as
`d05a954a05385c67e4ae368db87526d757ea501a` and its
[hosted CI](https://github.com/TheOddsFox/polymarket-events/actions/runs/38120301632)
passed 910 tests and the installed workflow. The separately attested proof-9
runtime then resumed only the named bootstrap load. It added 400 pages / 39,700
source records in 4,584.031578 seconds. At the operator's performance request,
one identity-checked child-only SIGINT was sent at 08:51:47.298982 UTC. The child
exited 1 at 08:54:57.916966 UTC; the original failed stage and supervisor
classification remain retained separately from the intentional intervention.

Its full postflight passed. The quiescent checkpoint is 5,500 loaded pages /
537,374 source records / 64,454 pending, with unchanged capture, no unresolved
controls, dirty certification and no pointer. Both registered processes are
absent and the existing lock is available. The 09:04:07–09 UTC resource point
measured 21,549,369,593 retained bytes and 983,816,732 temporary bytes; these are
points, not peaks or proof of final fit. All caps, raw evidence and 330 operational
loaded dlt package directories remain retained. Completion summary SHA-256:
`5c7430e324db8b3d291cdaf3aaefef4340f507041c7f155d5c30a5111628358f`;
full actual checkpoint binding:
`04413ba9f3f3b71dad3331105b16b056aa28bc1aa970ef11c21161cfb2284a89`.

A supervised read-only diagnostic then selected the next 50 pages in production
order: 43,578,442 declared body bytes, 4,990 event observations, 8,835 market
observations and no quarantine. It used the attested proof-9 code, immutable
query-only SQLite, read-only DuckDB, 2 GB memory / two threads, zero spill and
300-second query / 420-second process deadlines. The original conflict check
was interrupted at its query deadline. Its 304.605691-second phase is a lower
bound, not a completed-query timing. The source checkpoint and file metadata
remained unchanged. Supervisor SHA-256:
`063a9169afbb4073f66cf71c0c342ed6d035aa0e2f2c9acd1455e5603ecca35e`;
child report:
`3ead50e72989b7869b9480d8e888c1e0a8cb459d50c931fbfaff7241f90854a0`.

The same frozen input was measured once with a no-hit prototype: bind all 11/13
required columns with a zero-row query, then probe only the parameterized
observation IDs. Both populated relations had no matches. Schema binding plus
the probe and connection accounting finished in 6.434563 seconds. Normalization
took 36.623727 seconds in the baseline and 34.762518 seconds in the prototype;
budget resets took 3.387112 and 1.734281 seconds respectively. This one sequential
pair suggests over 47-fold improvement in the measured no-hit conflict phase;
cache effects and profiling overhead remain, and overall loader speed is not
yet measured. The prototype completed in 51.449577 seconds and left the
checkpoint and file metadata unchanged. Supervisor SHA-256:
`492f7f0342497344893dfee7af7f509d223754e7f580ac8f4ce0161bae0abe5b`;
child report:
`0455895e74c057bf0c8e0817d0cc3d93f171640b1bab00eb4b0be2362e99a92c`.

The corresponding production refinement binds the schema before a narrow ID
probe. New IDs avoid the wide JSON scan; any matching ID still triggers the
unchanged exhaustive, bounded conflict comparison. Architecture, data and
production security review approved this design. The test engineer's 85-case
focused module passed in 7.15 seconds; independent verification passed those
85 cases and two conflicting-replay integration cases, plus lint/format/diff
checks. The new cases exercise zero-hit wide-read avoidance, real DuckDB
missing-column binding and mixed IDs with late conflicting persisted duplicates.
It changes neither normalization, schemas, dlt persistence, checkpoints nor
allowances.

Installed proof 10 passed in 63.6 seconds outside the checkout, including
package-content auditing, frozen dbt resources and both offline workflows.
Catalogue acquisition measured ten attempts / 20,530 bytes; metadata measured
one attempt / 1,220 bytes. Wheel SHA-256:
`921e7551d859ec00d9c788b80767228d55e4c31751a33877044b9eb8e511ce2f`
(154,611 bytes); sdist:
`76da7877923a25f5cdbbbdc233c3839743bb751ddd22bda5ca04adca3fca046e`
(206,240 bytes). Installed report SHA-256:
`079a821db5ab150fa625855f8798d0e74b4dc9f80a60c24be730f1ca6fcd73a6`.

The proof-10 real-data smoke passed at 09:39:45–09:40:32 UTC in 46.912871
seconds: two attempts, 15,372 downloaded bytes and 54,192,011 whole-output
retained bytes. It compared all 29 semantic relations, 30 schemas and seven
publications, with two observations, one published market, two CTF outcomes
and no quarantine. Replay, raw rebuild, fresh verified restoration and the
injected-failure pointer check passed within the unchanged smoke bounds.
Report SHA-256:
`461ab8358e93bd8a15155ec8f9dc806b5671e04907ea96f8636f6e098504a9cb`.
Isolated `scripts/verify` passed all 918 tests in 511.43 seconds, with unchanged
test deadlines, plus dependency, lint/format, dbt parse and diff checks. Log
SHA-256:
`c96bcd6e5e430454144920cbf16b928e1e33caa7b648bc6e6b53725deb763ab1`.
Committed-source/runtime attestation passed for `6ff82e1`, and its
[hosted CI run](https://github.com/TheOddsFox/polymarket-events/actions/runs/38129728830)
passed. The named load then added 750 pages / 74,678 source records in
1,548.250021 seconds. Fifteen completed 50-page chunks averaged 100.139 seconds
including startup, with a maximum of 378.229 seconds. This is a measured partial
run, not a full-catalogue completion estimate.

One explicit operator interruption paused that load for further performance
work. Its original `PipelineStepFailed` classification is preserved separately
from the intervention. The final quiescent checkpoint is 6,250 loaded pages /
612,052 source records / 63,704 pending. The helper postflight passed; registered
processes are absent and the existing root lock is free and unchanged. The
10:50:48–50 UTC resource point measured 23,619,079,350 retained bytes and
1,224,809,945 temporary bytes, within the unchanged 64/8 GiB caps. No pruning,
allowance increase or new acquisition occurred. Completion summary SHA-256:
`e54f14a3c3189554ea82676053152fbbfa2eced70cc77810c747947b1089eb48`;
full actual checkpoint binding:
`cd87123bf0ede9fac844c58460d2b82581ec63f4e17554d723427c666cb13eb1`.
An earlier point-in-time checkpoint was misinterpreted as a stalled unit;
that inference was withdrawn. The observer used query-only `mode=ro`, without
immutable SQLite semantics.

The next minimal refinement reuses one decoded sealed scope within consecutive
pages of a same-batch chunk and admits eligible below-high-water entity IDs
before searching large membership lists. Batch transitions and new chunks
decode afresh. Every page retains its raw verification, timestamp and resource
checks; selected scopes, above-boundary membership exceptions, malformed
containers and oversized-ID fallback keep their existing behavior. Schemas,
normalization revisions, dlt persistence and allowances remain unchanged.
Architecture and actual data/security review passed. The test engineer added
37 regressions; independent verification passed 136 focused tests, plus scoped
lint/format/diff checks. Security additionally checked seven cache transitions
and 30 selection-parity cases without accessing the operator root.

Installed proof 11 passed outside the checkout in 56.255 seconds, with audited
package contents, read-only dbt resources and both offline workflows. Wheel
SHA-256:
`530f272134d362ef299d0949b1a0b8ab25d18b72204246ae2030a017585118c1`
(154,707 bytes); sdist:
`c4eece1ebdcffd5556a8bd20568ec3c0fa3c0377c11b2dc2a9c2082671539fd9`
(206,846 bytes). Report SHA-256:
`36cfbd8e881058b2be3de07015cecdb56613c756c75c4577c4bf6c26e1a06613`.
The proof-11 real-data smoke passed at 11:00:08–11:00:50 UTC in 42.108566
seconds: two attempts, 15,388 downloaded bytes and 54,189,186 retained bytes,
one published market, two CTF outcomes and no quarantine. Its replay, raw
rebuild, verified fresh restoration and injected-failure pointer checks passed.
Report SHA-256:
`a83e4594738a11517f73c64083a57f80239f1663e317096c0884f55045644114`.
Full offline checks passed all 955 tests in 569.13 seconds, with dependency,
lint/format, dbt parse and diff checks. Committed-source/runtime attestation and
the exact `b537c9b` [hosted CI run](https://github.com/TheOddsFox/polymarket-events/actions/runs/38134354344)
passed. The named load resumed with that source held unchanged. A dated query-only
snapshot at 12:06:39–12:07:27 UTC recorded 7,250 loaded pages / 711,659 source
records / 62,704 pending; its last committed chunk was at 12:05:21 UTC. This is
partial progress, without a full-load runtime or final storage-fit claim.

A separate managed worktree evaluates compact bronze observations without altering
the running `b537c9b` source or its operator root. Full canonical source hashes and
raw evidence remain; valid observations no longer repeat their complete source JSON
in bronze. Quarantine payloads remain. The incompatible internal layout is explicitly
versioned as warehouse.v3; public catalogue.v2 and metadata.v1 stay unchanged.

A checksummed copy of 50 committed pages contains 4,987 events and 13,193 embedded
market observations, with no quarantine. It retains original receipt times, scope,
predecessor and scan provenance. No new provider requests are made. Initial bounded
builds reproduced memory exhaustion with the declared 2 GB limit. Connection-time
DuckDB settings fixed a separate spill-directory reset error. Materializing market
staging then isolated the remaining exhaustion to its repeated JSON extraction.
Two grouped calls to the existing scalar/JSON extraction functions preserve all
47 projected fields, including missing values and explicit JSON nulls; a direct
STRUCT conversion was rejected because it changes those semantics.

The fresh V4 pair passed all 89 dbt nodes in both layouts and matched all 29 semantic
relations and seven public projections under 2 GB, two threads, 8 GiB temporary
and 64 GiB retained limits. Loading plus registry took 28.992051 seconds in the old
layout and 13.521964 seconds in the compact layout (2.14 times faster). Encoded rows
fell from 185,540,663 to 78,541,274 bytes; final retained accounting fell from
510,798,693 to 251,250,007 bytes. Temporary accounting was 16,541,498 versus
7,863,221 bytes. Both roots and failed earlier runs remain retained.

Total V4 time was 39.050225 versus 42.955436 seconds, so this run does not establish
an overall speedup. The old installed environment and candidate checkout environment
were on different disks, and the original load continued concurrently. dbt node
execution measured 2.232745 versus 3.856743 seconds, within external build times
of 5.468591 versus 16.512494 seconds. These measurements do not establish
whole-catalogue throughput or storage fit.
V4 report SHA-256:
`b6be05b13acb6d332fb6f29e3d662c2835c7a8eaab9655d0a5a43bbd6eee61a3`.

The V5 comparison uses both independently installed packages on the same disk,
with identical 132 locked dependency records and the same bounded dbt project.
Both runs again passed all 89 nodes and all 29/seven semantic comparisons.
Load plus registry measured 22.450585 versus 13.573927 seconds (1.65 times
faster); build measured 5.600353 versus 5.153031 seconds. Total measured time
fell from 37.891299 to 23.156755 seconds (1.64 times faster). Encoded bytes stayed
at 185,540,663 versus 78,541,274; retained accounting was 508,439,419 versus
250,463,577 bytes, and temporary accounting 16,541,510 versus 7,863,339 bytes.
The original child was still draining after one deliberate SIGINT, so this is
an ordered bounded comparison with explicit host contention, rather than sustained
whole-root performance. Failed and previous runs remain immutable.
V5 report SHA-256:
`5d6200891bfdb6f843531a2e75707e9c4acb687642b1ab59b8ff3a2d6addc2c9`.

The installed compact package also passed both offline workflows in 64.141 seconds,
including read-only dbt resources, raw rebuild, replay, restoration and pointer
failure checks. The audit found 82 wheel and 86 sdist members. Synthetic catalogue
capture measured ten attempts / 20,530 bytes; metadata measured one / 1,220 bytes.
Report SHA-256:
`dd46b428c5a5ea16f83d6950df9e2f270dafbd82a171c8a1ed4c8bf553345a02`;
wheel SHA-256:
`28cc23d3107289e7623d562d51cf9c3031901631c5eb7ab9175e1efe0cac275a`.
Architecture/data/security reviews and the independent verifier found no blocking
candidate issue. Focused checks cover the resource profile, old/new warehouse
rejection, complete raw hashes, precise projections and stale-stage publication.
The final native `scripts/verify` gate passed all 1,049 tests in 613.55 seconds,
with locked dependencies, Ruff, formatting and dbt parsing passing. A real
installed-package catalogue smoke for public market 5234660 then passed all
24 steps in 55.429 seconds: two observations, publication, offline replay,
semantic raw rebuild, verified backup restoration and preservation of the
previous pointer after an injected failure. It used two requests / 15,412 bytes,
retained 53,883,388 bytes, and recorded complete accounting with zero quarantine.
Its report SHA-256 is
`983fb3c6f850fa59a387f918a95e926871eb7fa647be64fc4d75f847d8f471f6`.
The installed wheel matches the final source/dbt resources exactly; the final
distribution audit excludes operator data and generated state.
Commit `59d6bc6e45ec12eb59531186a4fbff8023f5a7d5` passed
[exact-revision hosted CI](https://github.com/TheOddsFox/polymarket-events/actions/runs/38141698754),
including all 1,049 tests and the installed workflows. Fresh-root adoption remains
pending. The original loader terminated cooperatively with exit 129; its final
query-only checkpoint has 8,200 loaded pages / 805,933 source records / 61,754
pending, a dirty build record and no publication pointer. Final retained and
temporary point measurements were 33,566,385,249 and 1,810,691,460 bytes. The old
root and matching installation remain available; no state was migrated or pruned.

The streaming refinement removes only the extraction CTE's materialization hint;
the final staging table and all 47 projected fields remain. Two structured physical
plan checks establish one scan per input and one shared extraction projection, with
and without common-subexpression optimization. All 81 focused query and staging
checks passed; architecture, data and security reviews found no blocking issue.
Installed proof 15 passed both offline workflows in 51.643 seconds with unchanged
read-only resources. Its real selected workflow passed all 24 steps in 36.304
seconds, with two requests / 15,416 bytes, 53,880,426 retained bytes, zero quarantine
and all replay, rebuild, restoration and pointer-failure comparisons passing.
Installed and real report SHA-256s are respectively
`1ebf9383cf7d991ed9a93056a0f45ef1cfaa34e1e03cd85aa131be581b464fe8`
and `42317ef64b985367d46cbef99894679ee3622bce8d1fdf3b5ab32e247e26d794`.

A larger copied-input comparison used 200 original pages containing 19,944 events
and 49,260 market observations, with zero quarantine. Both installed runs passed
all 89 dbt nodes and matched four bronze fingerprints, 29 warehouse relations and
seven public projections. The existing 50-page and 200-page chunk settings used
identical installed code, dependencies and unchanged 2 GB / two-thread / 8 GiB
temporary limits, with no HTTP requests. Four 50-page chunks took 64.532 seconds
overall and 47.508 seconds to load; one 200-page chunk took 75.129 and 48.544
seconds. Both fit the resource limits, but this sample shows no larger-batch speed
win. It does not establish full-catalogue throughput or fit. Report SHA-256:
`3d5d3cafbf2a63a5d25a354d348ac5ba050b6bf426bb28c9cb4415b24327cad3`.

One metadata-only accounting pair on the quiescent original root measured
6.175681 seconds for the installed path walker and 1.906709 seconds for a private
directory-relative prototype. Retained/temp byte totals, the complete loaded-page
checkpoint and the existing lock state matched; no HTTP request, payload read,
warehouse read or product command occurred. This was a baseline-first point
comparison subject to operating-system cache and ordering effects. The production
refinement adds complete directory listings to reject entries that `fwalk` can
otherwise silently omit after classification errors, so the prototype timing is
not an exact production benchmark or whole-load speed claim. Pair report SHA-256:
`07f59beed86a234bb6d3287d18f6be986c62e97d90388477526b9e215fbf0a56`.

The production accounting refinement passed 134 focused checks, including both
backends, omitted entries, directory replacements, disappearing temporary paths,
permission/I/O errors, descriptor cleanup and unsupported-platform fallback.
Independent architecture, data, security and verifier reviews found no blocking
issue. The shared final `scripts/verify` gate passed all 1,089 tests in 419.07
seconds, plus locked dependencies, Ruff, formatting, dbt parse and diff checks.
Installed proof 16 passed both offline workflows in 53.77 seconds with all 77
package files and 38 read-only dbt resources matching the source. The distribution
audit found 82 wheel and 86 sdist members. Wheel SHA-256:
`957c316014554a933d5c96b1f8cb2215876c60e28a32c45e44a65bb8d03c603b`.
The real installed smoke passed all 24 steps in 37.841 seconds with two requests /
15,418 downloaded bytes, 53,890,054 retained bytes, zero quarantine, complete
accounting and every replay/rebuild/restore/pointer-failure comparison passing.
Installed and real report SHA-256s are respectively
`4ae676db4cc9ee700b9c35949d949362a6282236c956c9fe99eb4f8ac1a9bbba`
and `a6570712367209b9650703948ced39d74b02b54ced42ad39ad9ea31de6fb5275`.

Commit `aa4b4cb0d76bbc6bd8828692d9f2237bc1f64ead` passed
[exact-revision hosted CI](https://github.com/TheOddsFox/polymarket-events/actions/runs/38146389384).
The complete raw-only fresh-root copy verified 69,954 pages / 140,055 files /
4,284,587,741 bytes. Offline ledger reconstruction then verified all pages,
5,415,266 top-level source records and 146 sealed scans. The named full load is
still running under installed proof 16; the query-only checkpoint at
2026-10-11 15:39 UTC contains 8,000 loaded pages / 786,042 source records /
61,954 pending. This establishes progress, not complete catalogue readiness.

A copied-input native Parquet experiment kept the same 200 pages, installation,
sequential resources and resource limits. It used dlt's native Parquet format
with a private experimental PyArrow 24 copy, rather than a new loader. All four
bronze fingerprints, 29 semantic relations and seven public projections matched
the default-format reference, and all 89 dbt nodes passed. The dlt phase measured
18.301 versus 8.243 seconds (2.22 times faster), load plus registry 44.661 versus
34.250 seconds (1.30 times faster), and total time 64.026 versus 51.593 seconds.
Temporary bytes increased from 29,032,435 to 59,940,122, and retained bytes from
899,056,690 to 933,372,053. These ordered, bounded measurements share the host
with the actual catalogue load; they do not establish sustained throughput,
peak memory or whole-catalogue fit. Result SHA-256:
`fcbbd93a0fc9c18f8e610ca030a54cf9cd3b70a7ca7cdc095855d1f84ce137e6`.

The production candidate declares dlt's Parquet extra and locks PyArrow 24;
it does not import the experimental copy or another repository. Exact row-size
admission uses the same strict JSON encoder in one call, preserving every
checkpoint. A 5,539-row copied-input microbenchmark matched every original byte
count and measured 0.248/0.300 seconds versus 0.149/0.192 seconds. This is a CPU
microbenchmark, not a full-load result. A shared-connection experiment offered
no measured gain, and JSONL failed required empty-schema materialization; neither
is adopted. Gamma returned only 100 records for larger ordinary list requests
and rejected larger ID filters, so acquisition page size remains unchanged.

Installed proof 17 passed both offline workflows in 55.297 seconds, with all
77 package files matching the candidate source and 38 read-only dbt resources.
The audited wheel has 82 members and the sdist 86. Installed report SHA-256:
`1877279a78b0f239d3fec594199c3fa87672968646986c8dbb5c87c447972da3`;
wheel SHA-256:
`1d12447d9b36764a03e3fdf692221a1aa3daaeaeade6e072aa40c5cd02173a6e`.
The real public-market workflow passed all 24 steps in 40.652 seconds with
two attempts / 15,380 downloaded bytes / 53,970,679 retained bytes / zero
quarantine, including full semantic replay/rebuild/restoration and injected
publication failure. Its report SHA-256 is
`747adfb8debbeedc214b8c9099281942f3da5b0e9c8e21058b1d30c027965809`.

Using that actual installed candidate, the same 200-page comparison passed
all four bronze fingerprints, 29 semantic relations, seven public projections
and 89 dbt nodes. Load plus registry measured 44.661 versus 26.764 seconds
(40.1% less time), the dlt phase 18.301 versus 7.302 seconds, and total time
64.026 versus 43.446 seconds. Serialization measured 9.885 versus 3.242 seconds.
Retained bytes were 932,061,077 and temporary bytes 59,940,202, with the same
ordering/cache/shared-host limitations as the earlier experiment. Result SHA-256:
`ec363b1fda8ca545f4fb2865c3690b86ac15cf3a4410e930f0b2db578ea09f5c`.
All 188 focused loader/runtime/scale/encoder checks passed, plus the subsequent
old-format committed-but-unacknowledged to Parquet recovery regression.
Independent actual-diff review found no blocking architecture, data or security
issue. The shared native `scripts/verify` gate passed 1,115 tests in 475.32
seconds, plus locked dependencies, Ruff, formatting, dbt parse and diff checks.
The subsequently added format-transition regression passed separately; hosted CI
must include all 1,116 tests. Exact-revision hosted CI for `a8fbfa99` passed all
1,116 tests and the installed workflows in run `38153793718`. Its verified
installed runtime is now performing the named full load.

Remaining gates: the byte-total candidate's final native/hosted checks and verified adoption;
complete bootstrap load/build/publication, daily refresh and reconcile; immutable release
verification; full-root offline replay and semantic raw rebuild; verified fresh backup
restoration; final acceptance evidence commit and passing hosted CI for that commit.
Scheduling remains inactive. Provider availability, full-load spill demand, cumulative
temporary job storage and total catalogue runtime remain unverified.

### Reuse exact admission byte totals (2026-10-11)

The next isolated loader change computes each normalized row's exact encoded size
once during the existing page admission, returning three chunk-local integer totals.
Full-chunk and resource admission reuse those totals. Every raw, page, chunk and
resource check, actual storage recount, strict encoder, fixed reserve, resource order
and post-write checkpoint remains in place. No encoded rows or cross-chunk totals
are retained; empty relations retain zero sizes and their existing fixed reserve.

A three-second sample of the active loader showed substantial repeated JSON encoding.
One bounded whole-root accounting read took 0.677 seconds at 23.3 GB retained, so
storage traversal changes and raw-byte caching were deferred. Recent live 200-page
chunks contained 85,821–120,640 market observations, materially more than the copied
benchmark's 49,260; neither benchmark predicts whole-catalogue duration.

The first installed comparison reduced size-encoding CPU from 3.242 to 1.177 seconds,
but total load was effectively unchanged (26.764 to 26.675 seconds), with other phases
slower. A subsequent consecutive comparison of both installed versions used the same
200-page input and unchanged 2 GB memory/two threads:

| Measurement | Before | After |
|---|---:|---:|
| Size encoding | 3.825 s | 1.196 s |
| Load and registry | 31.077 s | 25.108 s |
| Complete comparison | 50.294 s | 41.627 s |
| Retained bytes | 931,012,577 | 936,518,329 |
| Temporary bytes | 59,940,165 | 59,940,109 |

The consecutive load was 19.2% shorter, and encoding CPU was 68.7% lower. Other
phases also varied; ordered runs, filesystem caches and the active catalogue/native
test workload limit attribution. This is a measured sample, not a sustained-rate or
peak-resource guarantee. All four bronze, 29 semantic and seven public relations
matched, and all 89 dbt nodes passed. The comparison report SHA-256 is
`7a3283774e090bee5936c93992fd99505d7cf866695d00c9cea6554a4f68d844`.

Focused regression checks passed 20 unit cases and three actual Parquet/byte-heavy
integration cases, including exact totals, one size calculation per row, every
admission boundary, failed-resource checkpoint preservation and input immutability.
Independent architecture and actual-diff security reviews passed. The standalone
installed proof passed both offline workflows and distribution audits in 57.268
seconds; its 77 installed source/dbt members match the candidate and 38 dbt resources
remain read-only. Report SHA-256:
`f1bdce24cb90d97f13b1f02da75c0e3db4aab8067aae7edaf9406e4bae14c7d4`.
The initial sandboxed proof stopped with a local process-permission error; its
separate failed evidence is retained. The successful fresh proof used the required
local subprocess permissions and offline locked dependencies.

The new installed real public smoke passed all 24 steps in 46.136 seconds: two
requests, 15,366 downloaded bytes, 53,984,682 retained bytes and zero quarantine.
Complete semantic replay, raw rebuild, verified backup restoration and the injected
failure's previous-pointer preservation passed. Report SHA-256:
`3be1094f96cbbb79d2acff44323ed0e0e8885ac00bf0b41aa99ac65ff9a5d41e`.

The final native gate and exact candidate hosted CI remain required. The actual
catalogue continues under the previously verified `a8fbfa99` runtime; no active
runtime was modified. Full bootstrap publication, daily refresh, reconcile and
catalogue-wide replay/rebuild/restoration remain incomplete.
