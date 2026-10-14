# Current catalogue acceptance evidence

Acceptance remains open. Do not activate schedules or cut consumers over on the
strength of the small fixture tests alone.

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
progress. Local verified backups do not establish the 24-hour disk-loss target
until an independently stored verified copy exists. Keep legacy roots and their
matching runtime intact throughout acceptance and rollback.

Daily retention prioritizes the last successfully verified, durably recorded
backup before selecting six other dates. Random UUID order does not determine
which same-day recovery point survives. If the recorded local point cannot be
verified, retention stops before deleting verified archives.
