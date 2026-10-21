# Current catalogue v1 acceptance

**Status: pending.** No v1 acceptance run has been recorded. Earlier measurements were
taken under the backup and import design that v1 removes, so they are not carried forward.

## Version 5 characterization status

The application-boundary campaign evaluates receipt metadata factoring, consolidated
one-to-one state reads and typed string-key transport independently. Correctness
review and synthetic qualification are in progress; no V5 production optimization
or runtime improvement is accepted yet. The guide documents the V5 manifest,
warm-up-only direct attribution and unchanged strict qualification gates.

Private engineering and evidence are kept beside the checkout in
`catalogue-evidence/application-v5-20261021T210432Z`. Engineering scratch variants
are not measured packages. The fresh package freeze, seed, comparisons, diagnostics,
integrity validation and shutdown share a single 10,800-second budget; correctness
tests and engineering precede it. Final results and the durable bundle checksum
must be recorded here before claiming qualification.

Native Windows and deployment acceptance remain separate and open. This campaign
uses synthetic offline data only and establishes no full-catalogue runtime or
cable-benefit estimate.

## v1 bar

A root meets the v1 bar only when all of these hold on the same root:

- `reconcile --full-pass` reports `full_pass.remaining` of 0 and a set `completed_at`.
- Every active membership is under 24 hours old (`active_overdue_count` is 0 in `status`).
- `full_pass.unresolved` is 0.
- `verify` passes on the published release.
- `refresh` and `verify` each complete within 1200 seconds.

## Recording a run

Record, for each run: the invocation time, the effective `config show` values, the
elapsed seconds for `refresh` and `verify`, and the `status` and `verify` output. Add
measured values here only after a real run. Until a run meets every item above, v1 is
not accepted for deployment.

## Probe 2026-10-17

This probe is not a passed v1 bar. The full pass was not run to completion.
Its timings predate batched application, prefetch and publication-cadence changes.
The four-day projection below is historical, not a current runtime estimate; a completed
current full-catalogue measurement remains unavailable.

The seeded root verified: 1,138,596 events and 4,321,358 markets. After the probe, `full_pass.remaining` was 5,439,842, `active_overdue_count` was 21,874, and unresolved memberships were 0.

Reconcile held about 20 entities per second. A 1,200-second invocation that keeps about 255 seconds for publish clears about 18,900 entities, so the remainder is about 4 days. A 480-second invocation committed 3,938 entities and was cut off by the deadline during publish. `previous_publish_seconds` was 170.

Observed retained storage was 7.57 GiB (8,125,747,037 bytes) and peak RSS was 4.19 GiB. The probe floor is 1.5 times those peaks: 11.4 GiB retained and 6.3 GiB RSS. The sample `config/catalogue.toml` allows 160 GiB retained, which is above that floor. That 160 GiB value is not a measured full-run ceiling.

## Known limit

A known event whose source member list stays incomplete produces no membership receipt, so
its pass stays open until the source returns a complete list. A known entity that upstream
no longer serves is settled by a native 404 during a forced pass and then counts toward it.

## Synthetic optimization evidence

The version-3 campaign described in the [guide](current-catalogue.md#version-3-acquisition-campaign)
uses offline fixtures and separate full-pass, market-ID and discovery oracles. Its
performance qualification does not satisfy the deployment bar above. Neither synthetic
application timings nor index installation measurements establish a current full-catalogue
runtime or a benefit from changing the storage cable.


The 2026-10-18 local campaign was stopped by user direction with one scored queue
pair and no qualified optimization. Code and correctness fixes continue, while full
runs and performance comparisons move to the native Windows workstation. Native
Windows filesystem/process/ACL proofs and deployment acceptance remain unrecorded.
The Windows run needs a fresh common portable baseline; earlier POSIX measurements
are not a substitute. No full-catalogue or cable-benefit estimate follows from this PR.

## Version 4 Mac store-application campaign, 2026-10-20

**Complete: neither optimization qualified; production runtime unchanged.** This synthetic campaign
compares outcome grouping and reverse claim lookup independently against baseline
`25315c6aa6e296261743e0ec42aeb4f37899bdac`, followed by the supplied combined
package if eligible. It uses the 10,000-event / 40,400-market interrupted/resumed
workload and a single 10,800-second budget. The 15-second shutdown reserve, final
integrity reserve, three AB/BA/AB scored pairs and strict HTTP, correctness and
performance gates remain in force. Partial evidence is not qualification.

| Independent comparison | Baseline median acquire/apply | Candidate median acquire/apply | Decision |
| --- | ---: | ---: | --- |
| Outcome grouping | 172.427 s | 198.838 s | Defer: 15.3% slower |
| Reverse claim lookup | 115.203 s | 119.035 s | Defer: 3.3% slower |

Each comparison completed three scored pairs. All samples had equivalent semantic
hashes, 104 settled units, cursor 10,000 and no deferred cleanup. Grouping also
increased runtime-accounted downloaded bytes by 149 in its first pair; its source
attempt/byte counters were unchanged. Reverse lookup matched all four HTTP counters.
The strict counter gate remains unchanged. The supplied combined package was not
timed because neither constituent qualified. These sequential comparisons have
separate baseline medians; their absolute times are not an inter-candidate ranking.

The campaign ran on an Apple M4 Mac (`macOS 27.0.1`, 32 GiB RAM), Python 3.12.13
and DuckDB 1.5.6, with synthetic
roots and evidence on `/Volumes/Mac SSD`, an external USB APFS SSD. Available host
metadata does not expose the negotiated USB link speed; it is recorded as unknown.
No SSD throughput, cable improvement or full-catalogue runtime is inferred.

Extended 200,000- and 1,000,000-claim index fixtures are diagnostics only. Their
query plans, timings, sampled storage and worker lifetime RSS cannot establish
production application behavior, sustained write cost or a safe index reservation.
No production index or index installer is included.

| Claims | Asset query median, before / with indexes present | Market query median, before / with indexes present | Index build | Post-write checkpoint |
| ---: | ---: | ---: | ---: | ---: |
| 200,000 | 30.7 / 38.6 ms | 17.4 / 15.0 ms | 0.424 s | 0.056 s |
| 1,000,000 | 60.7 / 33.7 ms | 44.0 / 17.6 ms | 3.009 s | 0.197 s |

Both namespace-aware query result multisets matched their deterministic expectations
(512 rows each), including duplicates and competing owners. The recorded `EXPLAIN`
plans still show sequential scans with indexes present; these timings do not prove
an index-scan benefit. Bounded replacement, conflicting-owner and deletion/restore
SQL workloads completed at both scales. Post-close database samples were 57,421,824
and 240,922,624 bytes respectively. The worker lifetime RSS peak across both scales
was 2,327,658,496 bytes, including population and all diagnostic phases. WAL/temp
samples were zero at the recorded boundaries; they do not establish zero peak usage.

Final source, seed and harness integrity validation passed. All 26 delivered runtime
source files match the frozen baseline inventory. The shared clock charged
4,529.189 seconds through bundling, including conservative preparation accounting;
ordinary correctness verification was separate. No scored sample was rerun.

The original measured campaign harness SHA-256 is
`e68f2b792fc1e902c6f71f2ab734c8325a7c2370f3b764fe63ac9954e42415c7`.
After measurement, independent review approved two fixes: handle unavailable
Windows `statvfs` metadata, and distinguish invalid, incomplete and complete
nonqualifying comparisons. The delivered harness SHA-256 is
`f7312db80e409effee766c480b27d89030d8064f477c11d78ca9f24707cca2e8`.
The workload, metrics, gates and positive Mac scoring path are unchanged. The raw
report remains immutable: its old aggregate label is `insufficient_evidence`, while
both six-sample comparison stages are complete and `deferred`. No new performance
run was used to replace that report.

Evidence and the checksummed handoff are kept outside Git at
`/Volumes/Mac SSD/oddsfox/polymarket/catalogue-evidence/application-v4-20261020T135115Z`.
The original `source-evidence-bundle.zip` SHA-256 is
`19eb307e3952b0ea3ad6b3c2987333a68585bf65c5fc53dd2978b7120dfa7a42`.
It includes frozen packages, patches, candidate tests, measured harness, reports and
logs; large synthetic databases remain beside it. Candidate-only traversal-count
tests stay in that private bundle; unconditional application regressions ship.
Native Windows acceptance and deployment acceptance remain open.
