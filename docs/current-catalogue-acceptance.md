# Current catalogue acceptance evidence

Acceptance remains open. Do not activate schedules or cut consumers over on the
strength of the small fixture tests alone.

## Approved operating policy — 2026-10-15

The operator approved these revised requirements; historical measurements below
retain their original limits and do not establish acceptance under the new policy.

| Requirement | Approved target |
| --- | --- |
| Normal refresh through verified publication | Under 1200 seconds |
| Standalone verification | Under 1200 seconds |
| Retained root / combined retained envelope | 96 GiB / 256 GiB |
| Active event membership freshness | 24 hours; due at 20 hours, urgent at 22 hours |
| Cold entities and historical discovery rotation | 60 days; entities due at 58 days |
| Recovery | Daily verified local backups, seven daily archives |

**Disk-failure protection is explicitly waived.** Local backup age remains subject
to the 24-hour target; device loss can destroy both store and backups. Backup and
restore verification, exact identities, atomicity and all other resource and
integrity safeguards remain required. Schedules stay disabled. Explicit old root
configuration remains in force until the operator updates it.

The three-case normal-refresh timing and fresh-root provisioning/headroom gates
passed below. Deployment acceptance remains open for sustained freshness.
Git delivery also requires hosted CI for the exact delivered commit.
The historical 72,589,620,854-byte publication-capacity model fits the new
103,079,215,104-byte ceiling. Seven-day growth remains unmeasured; the model does
not guarantee future capacity or establish freshness throughput.

### Software verification under the approved policy

On 2026-10-15, both `scripts/verify-fast` and `scripts/verify` passed after the
oracle fix: 1,362 tests,
one optional consumer skip, lint and formatting; the completion gate also checked
and synchronized the unchanged lockfile. The three metadata adapter tests passed
separately in the sibling consumer environment. That consumer also loaded the
installed producer's bundle with exact CTF and v2 position identities.

The installed wheel and sdist passed distribution audits. Its isolated offline
workflow passed all 12 steps, including verified backup, fresh-root restore,
restored verification and rebuild. Independent architecture, data, security and
implementation reviews found no introduced blocking defects. These small-fixture
results do not prove full-store performance or sustained reconciliation capacity.
Local measurements do not establish exact-head CI; check the matching hosted run
before deployment.

The operator explicitly approved deleting five superseded generated copies after
inventorying them: 32,267,492,248 bytes were removed. The pinned legacy release,
raw archive, rollback runtime, final supplemented seed and verified full-scale
backup were preserved. Strict special-file rejection remains enabled; only
zero-byte FIFO fixtures from completed tests were removed before accounting.

### Full-store pilot Q — bounded work remains

The first invocation met the approved timing limit, but the pilot did **not** pass
complete-delta acceptance:

| Measurement | Observed |
| --- | ---: |
| Refresh through verified publication | 582.19 seconds |
| Acquisition and application | 124.32 seconds |
| Publication and its verification | 450.83 seconds |
| Separate release/store verification | 163.68 seconds |
| HTTP attempts / downloaded bytes | 212 / 1,469,714 |
| Combined retained bytes before the run reservation | 165,487,217,194 |

Market discovery queued 260 parent events. The documented foreground repair step
processed 100; 160 remained queued, including ten existing events whose new links
had not yet been established by authoritative event responses. The exact oracle
caught those ten missing links and stopped the harness. Successful discovery and
release verification did not imply completion of all relationship work.

The operator approved preserving the frozen payloads and measuring bounded
follow-up invocations. Each invocation must finish within 1200 seconds; the report
must retain individual and cumulative timings, pending IDs and request accounting.
The unchanged full seed-plus-delta oracle runs after the queue drains. A stalled,
unexpected or incomplete queue fails the finite follow-up bound. Unchanged refresh
is measured afterward against the last completed publication; legitimate highwater
rechecks are reported separately. This correction changes neither runtime authority
rules nor the source workload. Manual pilot pauses remain part of actual freshness
age, and this pilot does not replace the required three fresh formal runs.

The pilot's derived 7,025,209,344-byte database was subsequently retired with
explicit operator approval to make room for those fresh runs. Pending-work state,
raw responses, both published snapshots, frozen source and the failed report were
retained. Snapshot physical hashes were rechecked before retirement, and the root
was disabled to prevent accidental canonical reuse.

### Provisioning qualification

The immutable seed retains a historical changed-publication cost of 915.485 seconds.
Its required 50% reserve is 1373.228 seconds, exceeding the new invocation limit;
that copied seed is not ready for direct refresh deployment. No-op publication,
completed-import reruns and restore preserve this measurement. Cutover requires a
fresh root imported from the pinned legacy evidence using the current installed
runtime, followed by actual publication and acquisition-headroom checks. Do not erase
the measurement or reduce the reserve to admit work.

Formal benchmark R preparation performs a genuine changed-generation publication:
run 1 recorded 361.499 seconds, giving a 542.249-second reserve. This establishes
headroom for that prepared benchmark root. It does not measure fresh-root import
or prove deployment readiness, and file-size differences do not establish the cause
of the timing difference.

### Formal R — comparison harness failure

The initial refresh took 725.443 seconds, followed by two ordinary refreshes taking
268.019 and 373.570 seconds. The live parent queue fell from 160 to 60 to zero;
the complete finite sequence took 1367.117 seconds. Separate verification passed
in 103.677 seconds. Each measured invocation met the approved 1200-second ceiling.

The process then failed in the independent exact oracle at the unchanged 8 GiB
temporary-storage ceiling. Its full expected-table copies and symmetric `EXCEPT ALL`
comparison were replaced by one relation at a time, combining seed and delta rows
with positive counts and actual rows with negative counts. Grouping every field
and rejecting nonzero sums preserves exact nulls and duplicate multiplicities.
Only the small typed delta is materialized. All 71 focused benchmark tests passed,
with independent exactness review. The corrected full-store comparison passed in
212.110 seconds under the unchanged limits and matched all four relation counts:
1,138,846 events, 4,322,358 markets, 4,322,342 links and 8,641,433 outcomes.

An append-only continuation passed the remaining checks, binding the original
failed report, frozen source, seed manifest, installed runtime, generation and
release. The restarted loopback origin was separately probed against the identical
source and policy. The unchanged refresh took 235.864 seconds, reused the release,
exported nothing and made three highwater requests with no native market lookups.
Its two already-complete parent rechecks are disclosed separately from the drained
delta. Final verification took 183.806 seconds; closing combined retained storage
was 179,125,962,434 bytes. Manual interruption is outside the original finite refresh
sequence and does not prove sustained freshness.

Independent data review accepted this as one instrumentation-recovered case. The
original failed process remains evidence and is not relabeled as successful. The
verified generated copy was removed under the harness's successful-copy cleanup
policy after recording its inventory; source, preparation, original failure and
continuation reports remain. Both subsequent fresh cases passed below.

The initial refresh reported 4,231,299,072 bytes of peak process RSS. DuckDB retained
its configured 2 GB memory limit and two threads; that setting is not a total-process
RSS ceiling. Reported retained and temporary usage for that invocation were
10,989,606,328 and 1,629,061,120 bytes respectively; these endpoint readings are not
claims about peak temporary usage. The corrected oracle remained subject to the
8 GiB temporary limit throughout.

### Fresh formal case 2 — passed

The first fresh S case passed every check with the same installed runtime and
frozen source as recovered case 1. Initial refresh took 463.549 seconds; bounded
follow-ups took 155.590 and 474.942 seconds, draining the live queue 160 → 60 → 0.
The complete finite sequence took 1094.162 seconds. Standalone verification took
74.096 seconds, unchanged refresh 143.719 seconds and final verification 109.672
seconds. The exact four-table counts matched recovered case 1.

Unchanged refresh reused the release, exported nothing and made three highwater
requests with no native lookups; its two already-complete parent rechecks remain
disclosed. Combined retained usage was 171,352,838,957 bytes before and
180,850,249,957 bytes after the case, within the unchanged 256 GiB envelope.
Peak process RSS across its refresh invocations reached 4,474,290,176 bytes while
DuckDB retained its configured 2 GB limit. The verified generated copy was removed
automatically under the existing harness policy.

### Three-case normal-refresh gate — passed

The final fresh S case also passed. All three cases used the same frozen source,
installed runtime, settings and lockfile. Each initial refresh, bounded follow-up,
unchanged refresh and standalone verification finished below 1200 seconds.

| Case | Initial refresh (s) | Follow-ups (s) | Complete finite sequence (s) | Unchanged (s) | Verification / final verification (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| R, explicitly recovered continuation | 725.443 | 268.019 / 373.570 | 1367.117 | 235.864 | 103.677 / 183.806 |
| S, fresh case 1 | 463.549 | 155.590 / 474.942 | 1094.162 | 143.719 | 74.096 / 109.672 |
| S, fresh case 2 | 394.051 | 103.647 / 212.532 | 710.290 | 128.011 | 65.725 / 70.753 |

The complete sequence includes the initial refresh and automatic follow-ups. Its
duration is separate from the approved per-invocation ceiling. In every case the
live parent queue drained 160 → 60 → 0 within two bounded follow-ups. Each case
used 212 requests for initial refresh, then four per follow-up. Unchanged refresh used three
highwater requests, no native market lookups and no exports; its two already-complete
parent rechecks were disclosed. All four exact relation counts and semantic digests
matched across cases. This is a finite-workload result, not sustained freshness proof.

Maximum closing combined retained usage was 180,850,668,315 bytes, below the
unchanged 256 GiB envelope. Admission reserved the full 96 GiB root allowance before
each fresh copy. No resource ceiling was raised. All successful generated roots were
removed under the harness policy; frozen source and reports remain. OS caches were
not flushed; these are serial fresh copies, not claims of cold filesystem caches.

Provenance:

- Installed runtime wheel SHA-256: `67ed0852928249fa1d03054765b080379c313c0b949449d087b7b9861207cf4f`.
- Corrected harness SHA-256: `a66616a782699a4157dd6e5a4417c24bc4ddea452a988a3e04a0060153a5fcc8`.
- Source SHA-256: `fc77dbeee35e58e43a7f6d5fec95889e39975464133b72c4907cefe737a6e587`.
- Lockfile SHA-256: `2f11132f3d8b8f93caa812e47334b42d6beca4d16c236de6dca984016fe518a4`.
- Seed manifest SHA-256: `f3329df660ff0dd7d825d262259cac381cfe318baa949f97a2afdcd78cd634f9`.

R's original failed process report and append-only successful continuation are
retained separately. The two fresh S cases have a successful combined report.
Independent verification checked their source bytes, seed/distribution/hash bindings,
exact counts/digests, queue progression, request accounting, timings and capacity.

### Remaining deployment evidence

Fresh current-runtime provisioning passed on 2026-10-15 using the pinned legacy
release and inventoried event evidence, in a separate new root. Offline import,
including its genuine first verified publication, took 2,199.955 seconds. The
publication took 282.633 seconds; its unchanged 50% reserve leaves approximately
776 seconds for acquisition under the approved 1,200-second invocation budget.
Separate full-store verification passed in 227.956 seconds. Counts were
1,138,596 events, 4,321,358 markets, 4,321,342 links and 8,639,383 outcomes,
including the approved 254-market supplement. Original receipt ages and discovery
frontiers were preserved. The old seed's retained publication cost was not reset.

The present fixture covers about 260 events. It cannot establish successful receipt
renewal across the full due population or sustained cold rotation: omitted IDs would
only measure attempts, not successful reconciliation. A representative delivery and
churn workload covering recurrence, memberships, identities, response sizes, delays
and failures is still needed while refresh, reconciliation and backups share cadence.
The approved 24-hour/60-day targets remain blocking until explicitly revised.

The new disk-backed characterization provider can serve the full frozen population
without copying the source database. Its actual probe requires two market lifecycle
passes. Bounded receipt comparisons measure successful renewal, not just attempts;
they do not establish recurring freshness or exact content equivalence. The harness
explicitly refuses sustained-acceptance and backup modes. Real recurrence and shared
backup cadence remain required; see the [operator guide](current-catalogue.md).

Require hosted CI for the exact delivered commit in addition to the local evidence.
Seven-day growth remains unmeasured; verified local daily
backups, seven-backup retention, capacity admission and overdue reporting remain
required. No mandatory seven-elapsed-day observation has been added. Schedules stay
disabled; operational cutover is not authorized by code delivery.

## Historical evidence under the original requirements

## Verified import

The selected legacy release is `20261013T191053Z`, manifest SHA-256
`fcc2760fcb9e1808e923c5df7a5ad6fcd6fbc6e89064ca0a74d3aaac5fc9621f`.
Its Parquets omit 254 markets referenced by its complete event evidence. The
approved import supplements those markets and 508 outcomes from verified event
records, retaining original receipt ages and discovery frontiers.

An independent, exact comparison passed for both the current store and its first
publication: 1,138,596 events, 4,321,358 markets and 8,639,383 outcomes. All
5,459,954 event/market receipts also matched. Bidirectional `EXCEPT ALL` covered
every retained field, nulls, unexpected IDs and duplicate multiplicities. The
comparison rechecked source hashes, the publication pointer and database identity.
Latest-complete memberships have a separate raw-evidence oracle;
`scripts/verify-current-import.py` runs that comparison and reports the intentional
difference from the historical bridge union.
That comparison also passed: 12,079 verified event pages produced exactly
4,321,342 current links, with zero differences. The historical bridge differs by
254 links. Legacy file identities and the current pointer remained unchanged.

## Verification and consumer compatibility

For final candidate P, both `scripts/verify-fast` and `scripts/verify` passed 1,264
tests, with one optional consumer-import test skipped and no warnings in the
successful runs. Lockfile, lint and formatting checks passed; the 128-file code,
test and configuration inventory was unchanged across verification. The actual
sibling market-data consumer separately passed all three metadata adapter tests
against the final installed producer, without skips.
The installed P workflow passed in 20.72 seconds, with 19 loopback requests,
44,166 downloaded bytes and complete accounting. Distribution checks found no dlt,
dbt or Dagster dependencies or obsolete runtime resources.
Its 29 package resources remained read-only and matched the source and archives
after all checks. The installed wheel SHA-256 is
`024e5ff52495cd1258df5c46a2aa82fa1fdb91cb06723bca9f6b859942ff91fb`.

A separate installed test used the default resource limits and a genuine durable
pending response. Verified backup and fresh restore preserved that response and the
previous publication. Cached resume applied the captured content exactly once with
zero provider requests, despite changed fixture content. This verifies the failure
path on a small fixture; full-scale recovery and capacity remain separate gates.

A bounded live provider probe supported unfiltered event batches and the two market
lifecycle passes: 13 requests and 208,516 downloaded bytes. Native/bulk identity and
membership parity passed for the known controls. The initial unfiltered market
candidate failed omission parity and was rejected. These controls establish request
compatibility, not exhaustive upstream coverage.

## Performance status

Measurements use an Apple M4 Mac with 32 GiB RAM, 2 GB DuckDB memory and two SQL
threads. OS caches were not flushed. A fresh process is not a cold-cache run.
The catalogue SSD negotiated a 480 Mb/s USB connection. File-read profiles reached
about 30 MB/s. The operator chose to retain that connection for acceptance rather
than change hardware to obtain a passing result.

The first full-scale publication and verification took 915.49 seconds. A later
bound current-release verification diagnostic passed in 448.53 seconds, compared
with 805.66 seconds using the earlier installed implementation. These diagnostics
are not equivalent complete refresh runs or an accepted speedup claim.
**The five-minute refresh target remains unmet.** Three full-store normal-delta
runs, unchanged refreshes and sustained six-hour/monthly reconciliation capacity
must still pass before deployment acceptance.

The first frozen full-store delta case used installed wheel N, 1,000 new
markets, 250 new events, new memberships on ten existing events and one native
identity repair. Preparation measured a changed publication at **549.82 seconds**
and a separate independent verification at **206.74 seconds**. Those preparation
costs were outside the refresh timer. The production reserve retained the measured
publication cost and its required 50% margin: 824.73 seconds.

The timed refresh stopped at **300.60 seconds**, exit 3, with a blocked/deadline
result, zero HTTP requests and zero downloaded bytes. It committed no delta; the
verified generation-2 pointer and uncaptured native repair remained intact. The
harness stopped at that first failure and retained the clone. The remaining two
runs and unchanged cycle were not forced or reported as passes.

This checkpoint has a ZSTD current release and a retained SNAPPY previous release.
Its four current Parquets total 1,160,970,485 bytes; the calibration deliberately
withheld two outcomes for one invalid identity mapping. All-ZSTD steady state is
unmeasured. The clean-seed benchmark also excludes older retained completed response
receipts; their entry-count effect on fresh storage-accounting walks needs a
separate representative measurement. Neither network lower bounds nor the 3:1 work
rotation establishes sustained six-hour/monthly capacity.

That full-store measurement used wheel N. The later private-workspace correction
passed both native suites, but full normal-delta acceptance has not been repeated
after that recovery-only correction. No performance pass is inferred from it.

The controlled benchmark is `scripts/benchmark-current.py`; its commands and
budgets are documented in [the operator guide](current-catalogue.md). Identity
repair preparation uses the production publication timer outside the measured
refresh. It records both the copied and resulting publication measurements.
Preparation has a cooperative four-hour publication deadline; normal refresh and
HTTP budgets remain 300 seconds. No measurement is reset or clamped to make work
fit the deadline.

## Measured candidate decisions

| Candidate | Evidence and disposition |
| --- | --- |
| Compact fingerprint hashes | Retain 32-byte binary intermediate hashes. Three full-outcome pairs preserved existing digests, reduced engine peak memory from about 2.46 GB to 1.36 GB and removed 175–182 MB of measured spill. Median elapsed time was effectively unchanged: 24.33 versus 24.49 seconds. |
| Three key-only identity guards | Reject the tested replacement. Three alternating pairs had medians of 0.638 seconds for the existing query and 0.775 seconds for the replacement; one slow initial read does not establish a repeatable workflow benefit. |
| Physical-only verification of older releases | Reject. Recomputed file and manifest hashes can conceal invalid relationships; retain actual semantic verification. |
| Packaging directory pruning | Retain. An installed-fixture workflow fell from 249.39 to 18.47 seconds after Hatch stopped traversing excluded raw evidence. Wheel bytes and archive member inventories were identical; only the sdist build configuration changed. This is not a full-refresh measurement. |
| ZSTD level-3 snapshots | Retain for catalogue.v3; metadata.v1 is unchanged. A full-outcome pair preserved exact schemas, content and native integrity while reducing bytes from 1,540,963,471 to 927,579,193. Export, hashes, semantic verification and public integrity totaled 156.40 seconds with SNAPPY and 139.55 with ZSTD. One pair does not establish repeatable refresh speed; the full workflow gate remains mandatory. Existing immutable releases keep their original encoding. |
| Compressed backups | Use the explicitly versioned backup.v4 gzip database representation, with expanded and stored hashes, streaming admission and fresh reopened verification. One full-scale backup and O fresh restore passed; default full-store verification and seven-backup operating capacity remain unmet. |

## Recovery and storage status

The measured mixed-codec checkpoint contains a 7,028,617,216-byte database. Its
completed gzip representation is 3,327,438,925 bytes; the verified archive totals
6,462,073,202 bytes including both retained releases. Backup creation and final
retention verification passed in **4,067.75 seconds** (67.8 minutes), with zero
provider or socket attempts and no retention deletions. Expanded recovery content
totals 10,163,249,238 bytes. Peak process RSS was 3,435,692,032 bytes; the DuckDB
buffer limit remained 2 GB with two threads, and no swaps were reported.

That archive was created by installed wheel N, SHA-256
`4ef673233312332fa5596f1187297de686c53f78a82f52a2d7337cdded26e1e0`.
A subsequent interruption review found that a killed verifier could leave an
off-root database copy uncharged by the next invocation. The correction gives
the supervisor ownership of cleanup after worker exit, serializes actual
verification, and charges surviving workspaces before another expansion.
Old flat verification artifacts stop work for explicit operator recovery;
they are never swept by prefix. Archive format and semantic checks are unchanged.
Installed wheel O, SHA-256
`558969c802ccd1a69b889bdc881dea5908531eec726a898f6d4ba30ffe97fb38`,
passed the offline workflow, actual sibling consumer and cached-pending recovery
checks. Its full-size fresh-root restore subsequently passed in **2,195.91 seconds**
(36.6 minutes), exit 0, with zero provider or socket attempts, no swaps and peak
process RSS of 2,881,536,000 bytes. It verified a fresh private database, copied the
full recovery inventory, rechecked the archive and wrote readiness last. Final
installed candidate P then attempted the default full-store `verify`: it stopped
at **301.82 seconds**, exit 3, on its unchanged 300-second deadline. This is a
failed verification-capacity gate, not a semantic-verification pass.

A subsequent bounded read-only comparison passed in 1.40 seconds with zero network
attempts. All 11 applicable synchronization values and all nine domain/operational
table counts matched the source; the archive predates recording its own successful
`last_backup`, so that one operational value is excluded. Both publication IDs and
the exact pointer bytes matched after the timeout. The restored generation has
1,138,596 events, 4,321,358 markets, 4,321,342 links and 8,639,381 outcomes; calibration
had withheld two unsafe outcomes. Its uncaptured native repair remains pending,
with no fabricated response. This state/count check does not replace the timed-out
full semantic verification. Complete archive verification had already passed during
restore using the four-hour recovery command.
The final installed `status` command passed in 5.09 seconds. It reported
`source_complete: false`, 21,874 active entries all overdue, 1,715 identity repairs
and zero unresolved memberships. These are inherited ages and unresolved identities,
not a sustained reconciliation result. The restored snapshot predates its own
backup-success record, so local recovery status correctly remains overdue.

A further interruption review found that only the CLI parent held the canonical
writer lock. A killed parent could leave a worker pruning while another invocation
published. Lock ownership now follows the worker through all canonical mutations;
standalone archive verification and fresh-root restore remain independent. Static
architecture and security reviews passed. Candidate P passed 21 process-only
regressions, both 1,264-test native suites and installed verification, including the
actual sibling consumer and zero-request captured-pending recovery.

The backup above used the internal temporary volume. Before the subsequent native
checks, that volume had only 19,683,434,496 bytes available, below the unchanged
32 GiB reserve. Following checks use fresh private temporary directories on the
existing SSD, outside every catalogue and legacy root. Limits were retained and
unrelated files were not removed. Recovery timings across these locations are not
a paired speed comparison.
The bounded, mocked process-only lock regressions overlapped part of restore;
no other catalogue SQL workload or large benchmark ran alongside it.

An independently checked, equal-size capacity model gives the following results
against the 68,719,476,736-byte retained-root ceiling:

| Modeled state | Bytes | Result |
| --- | ---: | --- |
| Source plus seven equal archives | 55,397,762,788 | Fits |
| Eighth backup during streaming admission | 61,859,899,271 | Fits |
| Eighth completed archive before retention | 61,859,835,990 | Fits |
| Seven archives plus the next changed-publication reservation | 72,589,620,854 | Exceeds the ceiling by 3,870,144,118 |

**The changed-publication storage gate is unmet for this checkpoint.** The
reservation is the existing `2D + 2W + Q` admission rule, using the database,
WAL and retained-publication sizes; it does not add the temporary ceiling twice.
An unchanged generation can use the verified no-op path. This is not seven
observed daily backups: source size, evidence growth, WAL and compression can
change. All-ZSTD steady state remains unmeasured. No limit was increased and no
source evidence was removed to obtain these results.

Recovery must remain offline and preserve applicable catalogue content and pending
progress. Under the policy used for these historical measurements, an independent
copy was required for disk-loss protection. That requirement is now explicitly
waived; verified local backups remain required. Keep legacy roots and their matching
runtime intact throughout acceptance and rollback.

Daily retention prioritizes the last successfully verified, durably recorded
backup before selecting six other dates. Random UUID order does not determine
which same-day recovery point survives. If the recorded local point cannot be
verified, retention stops before deleting verified archives.
