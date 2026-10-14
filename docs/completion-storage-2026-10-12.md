# Storage-efficient completion checkpoint

The acceptance limit remains **256 GiB combined working and recovery storage**, with
32 GiB combined temporary storage. Product defaults remain 64/8 GiB. No retained
catalogue roots have been migrated, deleted or declared complete.

## Current acceptance status — 2026-10-14

The bootstrap warehouse build completed all **89 dbt steps**: 26 models and
63 tests, with no failed or skipped nodes. Full certification covers 29 semantic
relations, 30 schemas and seven public projections. The certified build took
8,695.50 seconds, including 1,627.00 seconds for dbt.

The bootstrap release `20261013T191053Z` was published within the four-hour
invocation limit and independently verified. Its manifest SHA-256 is
`fcc2760fcb9e1808e923c5df7a5ad6fcd6fbc6e89064ca0a74d3aaac5fc9621f`.
It contains 1,138,596 events, 4,321,104 markets, 8,638,875 outcomes and
1,714 accounted unusable market identities. Malformed-record quarantines are zero.
These are bootstrap counts; daily and reconcile must finish before final counts
are frozen.

Daily capture completed its sealed scope under the approved limits: 1,108 latest
scan groups, 105,421 pages and 394,284 top-level records, including the selected
closure checks and parent-event follow-ups. Its seven invocations consumed
105,440 HTTP attempts and 4,668,721,300 downloaded bytes, including failed attempts.
Two separately accounted availability probes added two attempts and 9,955 bytes.
Daily loading is still in progress. Oversized successful keyset responses now
reduce the request limit at the same cursor; partial downloads and retries remain
charged. The installed runtime at
`41065172e8cebe8eb7d395b674db643cc258d8f0` passed 1,520 offline tests,
distribution and installed-workflow checks, a bounded public catalogue smoke,
independent review and [exact-commit CI](https://github.com/TheOddsFox/polymarket-events/actions/runs/38484699593).

Daily/reconcile completion, full-data replay and raw rebuild, complete backup
restoration, final-root verification and approved cleanup remain incomplete.
The historical failures and measurements below explain the changes; they do
not supersede the successful bootstrap certification.

## Implemented layout

Fresh warehouses use `oddsfox.polymarket.warehouse.v4`. Each normalized observation
is stored as fixed, independently compressible JSON-valued text fields. Logical
bronze views reconstruct the original JSON. Missing or unexpected fields and altered
view definitions fail closed. Interrupted loads repair missing views before checking
existing observations or committing page checkpoints.

Market staging, market history and current markets are views, avoiding additional physical
copies of the retained observations. Their columns, semantic values and observation grain are unchanged.
Loads still invalidate build certification; a subset build cannot publish. The public
`catalogue.v2` and `metadata.v1` contracts are unchanged.

Later load chunks avoid another empty dlt run when committed native schema state,
the exact physical table schema and its required observation view are present,
with no extracted or normalized packages pending. Missing state retains ordinary
materialization and recovery; incompatible schemas fail explicitly. Nonempty
resources, raw checks, storage admission and loaded checkpoints remain mandatory.

The old warehouse.v3 root stays intact with its matching runtime. The completion run
must reuse its exact verified raw captures in a fresh owned root; it must not reacquire
bootstrap or borrow raw paths in the eventual portable final root.

## Measured evidence

All benchmarks used disposable files and public source observations. They did not change
the retained bootstrap root or contact Gamma.

| Comparable 10,000-market-observation dlt load | Previous JSON layout | Field-column layout |
|---|---:|---:|
| Checkpointed warehouse file | 100,675,584 B | 42,741,760 B |
| Entire retained benchmark root | 107,132,968 B | 50,193,785 B |

Both loads used the same pinned dependencies, insert-only merge, observation envelopes
and input records. Their canonical decoded normalized-record SHA-256 was identical:
`c88df53704338be4326022c76f2b913c77a324fd31150c37a263ccaab87bb3a0`.
The root reduction is approximately 53%. These measurements include the loader's last
staging chunk and completed job payloads; they are not a full-catalogue capacity result.

A separate column-only SQL prototype reduced 49,819,648 B to 19,410,944 B. That 61%
result excludes dlt overhead and must not be substituted for the comparable load above.

Executing the exact model SQL on 10,000 real market observations measured 18,612,224 B
for market staging and 18,087,936 B for market history. Replacing those two duplicate
tables with views removed 36,700,160 occupied bytes and preserved schema and semantic
fingerprints for staging, history, current markets, outcomes and the market/event bridge.
Current markets were also measured across three replacement cycles. Keeping them as a
table projects approximately 7.9 GB of duplicate contents plus 7.8 GB of leftover allocated
blocks at catalogue scale. The current-market view preserves all seven public fingerprints
and the checked relation schemas. Its 10,000-row public fingerprint took a median 0.407
seconds versus 0.073 seconds for the table, an explicit computation cost.

Existing file free blocks are not reclaimed by dropping tables; acceptance must use
views from initialization of the fresh warehouse and count actual file sizes.

The audited installed distribution passed both offline synthetic workflows with read-only
packaged dbt resources. The final real installed catalogue smoke on market 5234660 passed in
42.05 seconds: two HTTP attempts, 14,900 downloaded bytes, 47,077,905 retained bytes.
It compared 29 semantic relations, 30 schemas and seven public projections, including
replay, raw rebuild, backup restoration and an injected failure preserving the pointer.
Independent architecture, data, security and final code verification found no blocking
defects in the implemented layout and recovery controls.

## Remaining gates

Retaining all previous roots consumes approximately 79.7 GB before a fresh completion
root is added. A full source plus backup plus restoration therefore permits approximately
65 GB per new copy before additional headroom. The complete benchmark forecast includes
41.97 GB of live relations, 8.60 GB of raw evidence, 6.09 GB of three releases, 1 GB of
operational state and 2 GB of allocated free blocks: approximately 59.67 GB per root after
completed-job cleanup. Old roots plus source, backup and restore project to 258.67 GB
(240.9 GiB), leaving approximately 15.1 GiB under the approved cap. Retaining approximately
12.7 GB of completed jobs per copy instead projects to 296.77 GB and fails.

This is provisional: actual scan growth, compression and spill remain measured gates.
The separate 32 GiB free-filesystem reserve is not available workspace headroom. Keep heavy
dbt work out of the three-copy phase and separately preflight each scratch rebuild.
The operator approved removing verified completed job payloads after a successful
source-bound raw rebuild and before backup. The pinned dlt cleanup method passed isolated
replay and interruption checks; failed, pending and unacknowledged packages remain protected.
No payloads have been removed from catalogue-wide roots.

Acceptance admission reserves unrelated temporary usage before launching each child.
Configured source and explicit scratch temporary directories join the same global inventory,
with nested paths counted once. Source fingerprinting also reserves existing scratch jobs;
that transient read allowance does not replace the operation configuration bound on resume.
The global cap remains 32 GiB, and reports expose the child operation allowance. Other roots
must remain quiescent during the sequential acceptance run. Earlier periodic-only monitoring
had no measured cap violation, but did not prevent a child from allocating space occupied
by unrelated roots.

The subsequent full build stopped after 79 of 89 successful dbt steps. Its saved supervisor
report contains only the exception type; a post-stop storage check reproduced combined
temporary allowance exhaustion, without establishing the precise failure-time peak.
Twenty orphaned DuckDB spill files totaling 23,727,013,888 B were inventoried and removed
under the root lock after dead-owner, no-user, path and file-identity checks. Raw captures,
the warehouse/WAL, ledger, dlt jobs and failed-build evidence remain intact.

The packaged profile now schedules one dbt model or test at a time, retaining the configured
DuckDB SQL thread count and memory. This reaches replay, raw rebuild and Dagster as well
as explicit builds. Native dbt task overrides remain available. Serial scheduling requires
fresh full-scale certification; it does not establish storage fit or eliminate repeated
selection work. Full-data certification and recovery gates remain incomplete.

Membership collection now joins selected observations directly. It preserves associations
for unusable identities and conflicting asset owners while keeping complete current/outcome
validation mandatory. A bounded retained-input profile compared 15,621 observations and
15,621 bridge rows with identical nine-column schemas and row multisets. Mean execution
was 113.66 ms before and 20.49 ms after the join change. The fixture contained only explicit,
usable, conflict-free memberships; separate regressions cover the other membership cases.
The profile used `2GB`, two SQL threads, zero disk spill and a 90-second deadline. Its
5.55× result does not prove full-catalogue speed or recovery storage fit.

The next full-data run passed all 26 models and 63 tests in 2,156.70 seconds. Its membership
table built in 57.57 seconds; the previous query was still unfinished after 23.52 minutes.
Certification then failed while fingerprinting the warehouse at the unchanged `2GB` limit;
the CLI exited after 5,489.16 seconds and no release was published.

Fingerprinting now finishes row hashing before sorting the compact hashes. A 250,000-row
join regression at 32 MiB memory, 64 MiB spill and two SQL threads reproduces the old OOM.
Disabling insertion-order preservation alone also fails. Separating the operators succeeds
with the exact independently calculated schema, count and digest. Full-scale fingerprinting,
fresh complete certification and all recovery gates remain required.

The retained raw inventory was copied into a fresh owned v4 root and its ledger reconstructed:
140,055 files, 4,284,587,741 bytes, 69,954 original page identities and 146 complete scans.
All capture fields match the old ledger, which remains unchanged. All bootstrap pages are now
loaded: 1,138,596 event observations, 8,597,758 market observations and zero malformed-record
quarantines. The first full build hit the unchanged `2GB` DuckDB memory limit; required models
and tests failed, so the warehouse remains dirty and no catalogue-wide release was published.
The failed invocation was stopped through the supported signal and its children reaped;
the loaded checkpoints and raw evidence remain intact.

A market-only staging change reads the existing field columns directly, avoiding full JSON
reconstruction for narrow queries. Scalars use root JSON string extraction, and collections
use root JSON extraction to preserve Unicode rendering and JSON-null distinctions. The original
logical views remain unchanged; full semantic snapshots now validate their canonical definitions.
Writable recovery of a missing view remains a loader responsibility.

The paired retained-input benchmark used 15,621 market observations, `2GB`, two threads and no
disk spill. Both staging views had the same 47-column schema and exact row digest:
`c35c1d30ff738a6939e8c397da9ef171817fc44744ada6f8d929b926311434b2`.
Full projections took 0.572 versus 0.403 seconds; median hash-null checks took 0.131 versus
0.00132 seconds. Peak process memory was 516,472,832 B. These small, concurrent-build timings
do not establish full-scale speed or memory fit. Installed, live and full-catalogue acceptance
must pass for changed production bytes before adoption.

The subsequent full build created the outcome table in 3,018 seconds and passed its
current-market build-provenance check in 2,632 seconds. Those repeated selection queries
still carry outcome payloads through recency windows. That older frozen installed build remained
incomplete; no certification or publication can be inferred from individual passing nodes.

A query-only change ranks five envelope fields before joining the winner back to its
complete observation. A paired 15,621-row check preserved all 49 selected-view columns,
schemas and exact rows. One bounded scaling case created 1,999,488 virtual observations
with four receipts per synthetic market key. Both queries returned 499,872 selected rows
with identical three-field schemas and semantic digest:
`98cc0a80cc0c1541aace6ed2024d48f869ed29fd3eecc75c743f29a14b812ff6`.
The projected query took 27.85 versus 4.34 seconds; separate-process peak RSS was
2,226,044,928 versus 612,286,464 B. Both used `2GB`, two threads and zero spill. The entire
scaling experiment took 40.46 seconds under a 90-second deadline, with no network requests
or changes to the owned source fixture.

The scaling fingerprint compares market ID, outcomes and usability, which are identical
across the four receipts; it does not prove the winning locator. Separate regression tests
assert observation IDs, source kinds and receipt times, including older direct observations,
tied timestamps, winning nulls and out-of-order insertion. The full certification boundary
also checks invalid duplicate IDs and corruption in non-winning evidence. These experiments
support the narrower query but do not establish actual catalogue-wide speed or memory fit.

Daily/reconcile collection, complete recovery, the final `data/catalogue`
and catalogue-wide cleanup remain incomplete. Accepted production revisions have passed hosted
CI, including the installed offline workflow; subsequent production edits require their own gates. Do not
claim the bounded smoke proves operational completion. Measure full build/certification
runtime as well as storage: views trade redundant storage for repeated computation.
