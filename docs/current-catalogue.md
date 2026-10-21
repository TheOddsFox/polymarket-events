# Current catalogue v1 operator guide

This is a current-state catalogue, not an observation warehouse. Its public contract is
`oddsfox.polymarket.catalogue.v3` (`PROJECTION_VERSION = 1`), defined in
`src/oddsfox_catalogue/current_contract.py`. Each publication contains exactly these
four Parquet relations, a `manifest.json`, and a root-level `published/current.json`
pointer. Consumers must verify the manifest and contract before reading.

| Relation | Key | Columns in contract order (DuckDB types) |
| --- | --- | --- |
| events | venue, event_id | venue VARCHAR, event_id VARCHAR, title VARCHAR, slug VARCHAR, active BOOLEAN, closed BOOLEAN, archived BOOLEAN |
| markets | venue, market_id | venue VARCHAR, market_id VARCHAR, question VARCHAR, slug VARCHAR, condition_id VARCHAR, source_market_version VARCHAR, protocol VARCHAR, active BOOLEAN, closed BOOLEAN, archived BOOLEAN, usable BOOLEAN, identity_error VARCHAR |
| event_markets | venue, event_id, market_id | venue VARCHAR, event_id VARCHAR, market_id VARCHAR |
| outcomes | venue, market_id, outcome_index | venue VARCHAR, market_id VARCHAR, condition_id VARCHAR, outcome_index INTEGER, outcome_label VARCHAR, asset_kind VARCHAR, asset_id VARCHAR, clob_token_id VARCHAR, position_id VARCHAR |

IDs are strings. Outcomes retain native asset identifiers, including CLOB token IDs
and position IDs where available. An unusable market has an identity error; consumers
must respect `usable` rather than inventing outcome identity. Event membership is
independent from market identity. No odds or historical metrics are exported.
New snapshots use Parquet ZSTD level 3.

## Scope of version 1.0.0

Version 1.0.0 is a manual, single-host tool. It has no scheduler, no import of earlier
catalogues, no backup or restore command, and no metadata export. It opens only a root
that was initialized with `init` (store contract `oddsfox.polymarket.catalogue_store.v4`)
and never migrates or mutates other data. There is no supported path from the earlier
v2 release; preserve any old root and runtime yourself if you need them.

## Root and configuration

Use an external root. `--root` is a global option and must precede the command.
`CATALOGUE_ROOT` is the environment alternative.

```sh
uv sync --locked
uv run catalogue --root /srv/catalogue-v3 init
uv run catalogue --root /srv/catalogue-v3 config show
```

`init` writes `catalogue.json` and refuses nonempty unstamped roots and roots nested
inside another root. Every later command requires the stamp.

Configuration is `ROOT/config.toml` (or `CATALOGUE_CONFIG`) with only `[gamma]` and
`[catalogue]` sections. Both sections must be TOML tables; malformed sections and
unknown keys are rejected. A 0.1.0 config opens unchanged: its
`[load]` section and its `max_raw_bytes` and `raw_max_age_s` keys are ignored. `config show` prints the
effective values. Environment overrides use `CATALOGUE_<SECTION>_<NAME>`.

Limits have two separate values. Defaults are the starting values. Hard maximums are
the largest values any root configuration may set.

| Setting | Default | Hard maximum |
| --- | --- | --- |
| `max_duration_s` (per invocation) | 1200 s | 1200 s |
| `max_requests` | 25,000 | 25,000 |
| `max_download_bytes` | 4 GiB | 4 GiB |
| `max_response_bytes` | 16 MiB | 16 MiB |
| `max_retained_bytes` | 160 GiB | 512 GiB |
| `max_temp_bytes` | 16 GiB | 64 GiB |
| `free_reserve_bytes` | 8 GiB | 256 GiB |
| `active_recheck_s` | 72,000 s (20 h) | — |
| `cold_recheck_s` | 58 days | — |
| `publication_reserve_s` | 60 s (minimum) | — |
| `full_pass_publish_interval_s` | 21,600 s (6 h) | — |
| `duckdb_memory_bytes` | 2 GiB | 16 GiB |
| `duckdb_threads` | 2 | 8 |

Each root is admitted against its own ceilings. No combined envelope across roots
is claimed. Ceilings are set from measured peaks; they are not a promise that any
corpus fits.

## Provider gate

Before discovery or reconciliation, certify each event and market lane against known
open, closed, and absent controls. Use IDs you established independently. An absent
control must be an ID the native endpoint answers with HTTP 404. `/events/2` and
`/markets/10000000` do. A huge ID such as `999999999999` returns HTTP 422 and fails
the lane.

```sh
uv run catalogue --root /srv/catalogue-v3 probe \
  --open-event-id OPEN_EVENT --closed-event-id CLOSED_EVENT --absent-event-id ABSENT_EVENT \
  --open-market-id OPEN_MARKET --closed-market-id CLOSED_MARKET --absent-market-id ABSENT_MARKET
```

The probe compares bounded bulk retrieval with native by-ID responses and records a
policy certificate bound to the provider origin. Both lanes require controls; repeated
flags add controls. If an unfiltered pass omits known controls, the probe certifies
explicit `closed=false` and `closed=true` passes only when their combined native
identities match. Pending work freezes its exact requests and certificate. A changed
provider or policy requires a new probe. An unsupported lane blocks use; it is not
permission to fall back to an unproven query.

### Optional certified acquisition

Add `--api-optimizations` to the same probe command to request a version-3
certificate. Capability proofs are independent: unsupported capabilities keep the
certified legacy acquisition path. Ordinary `probe` still creates a version-2
certificate. Probes refuse replacement while any frozen acquisition work is pending.

Certified market retrieval uses `/markets` with explicit batches of at most 100 IDs
and `closed=any`. This filter is never used for unfiltered market discovery or
highwater requests. Native lookup still confirms omissions; a list omission alone
never establishes absence.

Certified keyset discovery uses ascending numeric `order=id` pages of at most 100,
with a frozen numeric highwater. Events require certified unfiltered lifecycle
coverage; markets run separate open and closed passes. Frontiers advance only after
all required passes cover the boundary or terminate. Known-entity due work, numeric
incremental tails, full-pass receipts and cycle cadence remain in use. Cursor
exhaustion records an observed sweep, not complete source coverage. Later sweeps
remain necessary for lifecycle movement and late records.

An expired cursor restarts only its affected pass, once per invocation. An oversized
page halves the limit and restarts that pass without reusing its old opaque cursor.
A single oversized record or persistent request failure remains visible and resumable.
Durable pages freeze endpoint, parameters, certificate and raw receipt together with
application progress; raw references retain the existing `raw_id` spelling.
Periodic keyset reconciliation uses the same bounded native identity repair as
numeric windows, including nested event markets and durable-page replay. The existing
100-repair limit, retry cooldown and acquisition deadline still apply. Confirmation
follows page application; interruption between those operations can leave an identity
unavailable until it is revisited.

Failed reprobes preserve the working certificate and store diagnostics separately.
If a successful base proof contradicts the old lifecycle passes while an optional
proof fails, acquisition is fenced against that exact certificate until a successful
probe resolves the contradiction. Certificate activation, diagnostics, that fence and
settled keyset-state cleanup commit atomically. Status, publication and verification
remain available.

After activating version 3, use the upgraded executable for every probe and acquisition
command. Older acquisition rejects version 3 before HTTP; older probe code does not
have the new preservation rules. To return to legacy acquisition, finish pending
work and run an ordinary successful probe with the upgraded executable. It discards
only settled keyset metadata, preserving rows and numeric frontiers.

Claim ownership lookups now use bounded scalar predicates with exact per-chunk
namespace filtering. They work without ART indexes. Index build/drop commands are
not included: safe build and ongoing-write admission still need workstation evidence.
The full-pass queue uses a deque and lazy urgent-ID skipping; batch size, prefetch,
omission confirmation and settled cursor semantics are unchanged. These changes have
correctness coverage, but their performance remains unqualified.

## Operations

Run each command by hand. Each invocation is bounded by `max_duration_s` (1200 s),
including publication. Publication time is reserved inside that deadline: the reserve is
at least 60 seconds, or 1.5 times the previous measured publication if that is larger.
Every `reconcile --full-pass` invocation keeps this reserve, including when publication
is initially deferred by cadence, because the pass may complete during acquisition.

```sh
uv run catalogue --root /srv/catalogue-v3 refresh
uv run catalogue --root /srv/catalogue-v3 reconcile
uv run catalogue --root /srv/catalogue-v3 reconcile --full-pass
uv run catalogue --root /srv/catalogue-v3 status
uv run catalogue --root /srv/catalogue-v3 publish
uv run catalogue --root /srv/catalogue-v3 current
uv run catalogue --root /srv/catalogue-v3 verify
```

- `refresh` advances durable discovery frontiers and repairs at most 100 parent events
  per invocation. Follow-up invocations may be needed before every newly discovered
  market has a verified event relationship. Never infer those links from market-side
  references.
- `reconcile` runs bounded units of work. Active and unknown events are checked first.
  Closed events, known markets and historical ID windows rotate on the cold schedule.
  Work survives between invocations.
- `reconcile --full-pass` starts a forced full pass, or resumes an unfinished one.
  While the pass is active it bypasses the 58-day cold gate and the active freshness
  skip, and it does not schedule historical id-range windows. It loads the stale event
  ids once per invocation and walks them in numeric order from `cursor_after`, 100 ids
  per request. If a legacy cursor has passed unresolved events, one startup sweep from
  the beginning recovers them without resetting the saved cursor before a batch settles.
  Nested markets renew from the event response. After those events, a
  markets sweep fetches markets that are still stale, including orphans. An active event
  within two hours of the 24-hour target takes the next slot. The next list request is
  prefetched on a second client that shares the rate limit; a prefetched body is used
  only when it matches the frozen request. Each invocation stops at `max_duration_s`;
  the next invocation resumes from the cursor. When no known entity is left, the pass
  records `completed_at`. The pass never changes `source_complete`, which stays `false`.
  The invocation publishes when the pass completes, or when the last publication is
  older than `full_pass_publish_interval_s`. Otherwise it exits 3 with `published: false`
  and the `full_pass` object. `refresh`, plain `reconcile`, and `publish` still publish
  on every invocation.
- `status` reports progress, freshness, the `full_pass` object, the discovery state and
  the last run.
- `current` and `verify` verify the publication pointer and release. `verify` is the
  acceptance check for an existing release.

The `full_pass` object appears in `status`, in every invocation result and in the
publication manifest:

| Field | Meaning |
| --- | --- |
| `started_at` | Start of the active or most recent pass; `null` if none has started |
| `completed_at` | Set when `remaining` reaches 0; `null` while open |
| `remaining` | Known events and markets without a receipt newer than `started_at` |
| `overdue` | Active memberships that are unresolved or older than the active threshold |
| `unresolved` | Memberships currently marked unresolved (missing or incomplete member arrays) |
| `identity_repair_count` | Markets whose identity is currently unusable |
| `parent_event_repair_count` | Queued parent-event repairs |

During a forced pass, a bounded list that omits a known entity does not prove it is gone.
One native lookup decides it: a 404 is a confirmed miss, which counts as that entity's
receipt for the pass and is not refetched until its normal interval; a 200 applies the
record. Any other status, or a transport failure, is an error and never counts toward
`remaining`. Outside a pass, omitted entities keep the normal retry schedule.

A known event whose source member list stays incomplete produces no membership receipt,
so `remaining` stays above 0 for it and the pass stays open. Investigate those events
before treating a pass as complete.

A busy invocation exits **3** with `status: busy`. A full-pass invocation that does not
publish also exits 3, with `published: false`. Other blocked failures also exit 3.
Read the status and message before retrying. Persistent busy or blocked results need
operator investigation, not a retry loop.

One supervised HTTP worker, one root writer lock, and at most 10 requests per second are
allowed. The worker holds the lock through database and filesystem changes. Killing its
CLI parent cannot admit a second writer while it finishes. A second client may prefetch
the next full-pass request; it shares that worker's rate limit.

When a publication's store generation matches the current release, the publisher checks
the pointer, the manifest SHA-256, the exact file inventory, and each Parquet's size and
SHA-256, and it still rejects a live store that fails its own checks. It does not
re-fingerprint the Parquet. A changed publication, and the standalone `verify` and
`current` commands, keep the full semantic check. Retained-byte
admission walks the root when the invocation connects, again before publication, and
again at exit, and adds bytes as raw responses are written.

## Raw retention

Raw responses are retained by reference. After every publication, including one that
is a no-op because the current release is unchanged, the publisher deletes raw response
directories that nothing references. A raw response is kept if any of these reference it:

- `_receipts.response`, `_membership.response` or `_membership.latest_response`, or
  `_identity.invalidated_response`, in the store;
- a pending durable response or any other sync-state pin, including nested ones.

Pending raw is never deleted. Raw that is not named by a reference is deleted only
after the publication commits, whether that promotes a new release or confirms the
unchanged one; a failed deletion is reported as `retention_warning` and does not undo the
publication. The manifest records `raw.referenced_count` and
`raw.referenced_bytes`. Non-matching names in the raw directory are left alone.

There is no age-based or size-based raw prune and no permanent replay archive.

Optional post-commit maintenance stops starting work when fewer than 15 seconds
remain before the supplied publication deadline. Fresh-release cleanup reports
`cleanup_warning`; raw retention on fresh and unchanged publications reports
`retention_warning`. Completed deletion counts and bytes remain available when
later work is deferred or fails. A later publication rescans the remaining garbage.
Without a deadline, cleanup runs to completion as before.

These checks are cooperative: a recursive filesystem operation can overrun the
cutoff. Mandatory pre-commit cleanup, exact referenced-raw accounting, verification
and storage admission remain required. Deferred garbage still counts toward storage
ceilings. Post-commit maintenance cannot roll back a committed publication, and
operator termination still propagates. Publication-result phase timings describe
pre-commit work, release cleanup and raw cleanup; they are not release-manifest fields.

## Metadata.v1

The targeted handoff uses a separate, isolated producer root:

```sh
uv run catalogue --root /srv/metadata-producer metadata refresh \
  --market-id MARKET_ID --catalogue-root /srv/catalogue-v3 \
  --output /srv/handoffs/metadata-release \
  --max-output-bytes 134217728 --max-response-bytes 8388608 \
  --max-requests 500 --max-download-bytes 134217728
```

Repeat `--market-id` for more IDs. The command fetches only the named markets from Gamma.
Identity comes from the catalogue store's identity tables (`_identity` and
`_identity_claims`), read under the catalogue lock on a read-only connection. The store
is never written. Conflicting asset claims mark a market unusable.

The output directory is a new, immutable bundle containing `manifest.json` and
`markets.json`, `outcomes.json`, `memberships.json`, `identity_history.json` and
`coverage.json`. The contract is `oddsfox.polymarket.metadata.v1`.

Producer and output paths must be outside every catalogue root, including resolved
symlink targets. A producer, output or raw path inside a catalogue root is rejected
before any directory is created or request is made.

## Storage and recovery

Admission checks each root against its retained and temporary ceilings and the free-space
reserve before work starts and before publication. A full root fails explicitly.
Pending responses are never discarded to make room.

Version 1 creates no backups and has no restore command. The root's publication pointer
and its referenced release are the recovery state. Protecting the root with copies or
snapshots, and verifying those copies, is the operator's responsibility while no
invocation is running. Losing the device loses the catalogue unless you have made such a
copy.

Interrupted work resumes from committed progress and pending responses. A failed
publication keeps the last verified pointer. Verify the pointer before redirecting
consumers.

## Native Windows

The native port targets 64-bit Python 3.12 on Windows 10/11. Real Windows execution
and performance validation are pending the workstation run. POSIX and mocked-platform
tests do not establish native NTFS, Job Object or ACL behavior.

Use a local fixed NTFS drive and an elevated PowerShell session with volume-flush
access. Network paths, mounted subvolumes, junctions/reparse points, alternate data
streams and ambiguous trailing-dot/space names are rejected. Keep roots and evidence
outside OneDrive and other reparse-managed locations. All catalogue operations require
this durability access, including nominal reads whose existing implementation can
initialize database or temporary state. Unsupported storage or missing access fails
before the worker opens its lock or database.

Windows durability uses an explicit volume `FlushFileBuffers`, rather than treating
POSIX directory fsync as a no-op. This requires administrative privileges and can
add substantial I/O; measure it on the target machine. See Microsoft's
[volume-flush contract](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers).
Workers enter a kill-on-close [Job Object](https://learn.microsoft.com/en-us/windows/win32/procthread/job-objects)
before their launch gate opens. Capped pipe output, deadlines and descendant cleanup
remain supervised. Windows peak RSS is the process peak working set in bytes;
comparisons across operating systems are rejected.

The existing verification wrappers also run as Python on Windows:

```powershell
uv sync --locked
uv build
$env:UV_OFFLINE = "1"
$env:UV_LINK_MODE = "copy"
python scripts/verify-fast
python scripts/verify
uv run python scripts/verify-installed-workflow.py --output C:\catalogue-evidence\installed-proof
uv run python scripts/benchmark-optimization-campaign.py --manifest C:\catalogue-evidence\manifest.json --output C:\catalogue-evidence\campaign
```

Populate the dependency/build cache before enabling offline mode. Tests and campaign
workers use synthetic loopback HTTP only. Output directories must be fresh and outside
the checkout. The installed proof uses restrictive, restorable Windows ACLs and checks
that writes are denied; Windows read-only mode bits alone are not an equivalent proof.
The native-only Job, parent-death, rename, reparse and ACL tests must run on Windows.

## Verification

Run `scripts/verify-fast` during development and `scripts/verify` before completion.
On Windows, invoke them with `python`; arguments are forwarded to pytest.
Tests use synthetic loopback fixtures and never contact live Gamma. Pytest runs them
in parallel (`-n auto`); pass `-n0` to run one test in-process. The offline
environment is `CATALOGUE_GAMMA_BASE_URL=http://127.0.0.1:9` with
`CATALOGUE_GAMMA_ALLOW_LOOPBACK=1`.

The compatibility integration test initializes a minimal root, then runs refresh and
`reconcile --full-pass` against the fake Gamma server. It confirms that `import-release`
is not available.

Build and audit the distribution, then exercise an installed executable outside the
checkout:

```sh
uv build
uv run python scripts/verify-distribution.py dist/*.whl dist/*.tar.gz
uv run python scripts/verify-installed-workflow.py --output /tmp/catalogue-installed-proof
```

The output destination must not exist. The installed-workflow check runs the same
commands an operator would run, including `reconcile --full-pass`.
It builds and installs offline: the uv cache must contain both the declared build
backend and exported hash-locked runtime requirements. If it reports a cache miss,
preserve that output, prepare a disposable cache separately, and select it with
`UV_CACHE_DIR` for a fresh proof. The proof never downloads missing dependencies.

Benchmarks are characterization tools and never claim sustained acceptance:

```sh
uv run python scripts/benchmark-current.py --help
uv run python scripts/benchmark-capacity.py --help
```

Use a fresh output directory and an idle, verified seed with no pending work. Pass the
installed-wheel Python for the harness and the CLI, and repeat `--account-root` for every
external root that should be reported. Benchmarks report measured elapsed time, HTTP
counts, publication time and resource evidence. Acceptance is recorded separately in
the [acceptance page](current-catalogue-acceptance.md).

The full-pass characterization tool generates its own synthetic seed; it never opens
operator data or contacts live Gamma:

```sh
uv run --locked python scripts/benchmark-full-pass.py --profile smoke --output "$HOME/catalogue-benchmarks/full-pass-smoke"
uv run --locked python scripts/benchmark-full-pass.py --profile perf --output "$HOME/catalogue-benchmarks/full-pass-baseline"
# After a candidate scheduling change, compare against the locally generated baseline:
uv run --locked python scripts/benchmark-full-pass.py --profile perf --compare "$HOME/catalogue-benchmarks/full-pass-baseline" --output "$HOME/catalogue-benchmarks/full-pass-candidate"
```

Every output directory must be fresh, with no symlink ancestors. Use only trusted,
locally generated comparison directories: their saved Python runtime is executed during
comparison. Keep the baseline
seed, runtime inventory, and report unchanged. Smoke exercises correctness and cannot
qualify an optimization. The performance profile contains 10,000 events and 40,400
markets; it is an explicit synthetic distribution, not a production traffic model.

Baseline and comparison share a 600-second measurement budget, including preparation,
validation, and cleanup, with 15 seconds reserved for shutdown. An incomplete comparison
reports `insufficient_evidence`. A candidate qualifies only with three complete pairs,
equivalent scheduler and publication results, at least 10% and one second lower median
acquire/apply time, no more than 5% wall-time or peak-RSS regression, and no increased HTTP
load. Publication comparisons use row fingerprints, schemas, and counts. Each release's
physical hashes and sizes are verified before comparison and excluded from cross-run
equality, so Parquet layout differences do not imply changed catalogue contents.
HTTP accounting records served requests and offered response bodies alongside
client reservations and consumed bytes, including cancellations; neither measure may
increase. Isolated profiles cannot qualify a change. Results never establish deployment
acceptance or a production speedup.

New reports and samples use `measurement_version: 2`. The additive
`worker_setup_wall_s` ends immediately before the first reconciliation;
`runtime_sequence_wall_s` covers the interrupted/resumed sequence and its intermediate
checks; `oracle_wall_s` covers final snapshots through semantic hashing. The existing
`setup_wall_s` remains the provider-probe timing. `worker_peak_before_oracle_bytes`
is the worker's lifetime peak sampled before the oracle, including setup and provider
allocations; it is not isolated runtime RSS. Whole-worker supervised wall time and
lifetime peak RSS remain qualification guards. These phases do not replace the
whole-worker total: provider shutdown, trace writing and supervisor overhead remain
visible there. Missing measurement versions mean
legacy version 1; comparisons across versions require a fresh baseline. Scored runs
are unprofiled, and warm-up profiles cannot establish a qualifying speedup.

For a fixed-budget comparison of multiple locally prepared optimization variants:

```sh
uv run --locked python scripts/benchmark-optimization-campaign.py --manifest /absolute/path/variants.json --output /absolute/path/fresh-campaign
```

The data-only manifest requires `baseline` and `binding` entries and accepts optional
`outcome_grouping`, `queue`, and `projection` entries. Each entry contains `path` (the
trusted local `oddsfox_catalogue` package directory) and `source_hashes` (the complete
mapping of package-relative Python filenames to their SHA-256 hashes). Relative package
paths resolve beside the manifest. Saved package code is executed: hashes establish
integrity, not trust. Use only locally prepared, reviewed sources, with no symlinks or
special files. Keep snapshots, manifests and evidence outside Git and outside existing
catalogue or legacy roots.

The campaign uses one 1,800-second budget, including preparation, source copies,
unscored warm-ups, paired fresh workers, validation and cleanup, with 15 seconds reserved
for shutdown. Existing full-pass commands retain their shared 600-second budget. Every
stage uses the same frozen 10,000-event/40,400-market seed and the production limits.
Each complete sample includes the deterministic 50-unit interruption and resumption.

Binding is compared with the unchanged baseline first. Eligible secondary variants are
compared independently with binding, in grouping, queue, projection order. A secondary
must match the full binding source inventory except for its assigned store, reconciliation
or sync module. Only individually qualified files are assembled, and that final combination
must qualify against the unchanged baseline. If final evidence is incomplete or fails,
only an already qualified binding variant may be retained. Fallback retention also
requires a final check of the measured sources, seed and harness within the remaining
measurement budget. Failed or unfinished integrity verification retains no candidate;
the shutdown reserve remains reserved for cleanup. All existing semantic,
publication, HTTP-load, wall-time and peak-RSS gates remain required. The oracle also
compares private identity, claim, repair, attempt and generation state.

The report records stage results, source inventories, retained variants and explicit
deferrals. Missing pairs or budget exhaustion never qualify a change. Campaign results
are synthetic characterization, not deployment acceptance or a million-event runtime
estimate. Do not replace failed or incomplete samples with selectively repeated runs.

An additive version 2 manifest removes the binding prerequisite:

```json
{
  "version": 2,
  "baseline": {"path": "/trusted/baseline/oddsfox_catalogue", "source_hashes": {}},
  "candidates": {
    "outcome_grouping": {"path": "/trusted/grouping/oddsfox_catalogue", "source_hashes": {}},
    "queue": {"path": "/trusted/queue/oddsfox_catalogue", "source_hashes": {}}
  }
}
```

Replace each empty illustrative inventory with every package-relative Python source
and its SHA-256 hash. Only these two candidate names are accepted. Each candidate
must differ from the common baseline only in its assigned store or reconciliation
module. Unknown fields and executable configuration are rejected. Legacy manifests
keep their binding-first behavior.

Version 2 evaluates outcome grouping first, then queue handling, directly against
the common baseline. Each distinct runtime receives one unscored warm-up; scoring
uses three fresh-worker pairs in AB, BA, AB order. Scored work is admitted only when
warm-up-based estimates fit with 25% headroom and a separate 15-second final-integrity
reserve, in addition to the shutdown reserve. After the first candidate qualifies,
admitting the second also protects 125% of the first comparison's duration for a final
combination comparison. One qualified candidate reuses its independent evidence.
Two qualified candidates require that exact combination to qualify; otherwise the
proven individual with greater absolute median acquire/apply savings is retained,
preferring outcome grouping on a tie. Final source, seed and harness verification
must finish within budget or no optimization is retained.

The same 10% and one-second acquire/apply improvement, 5% wall/RSS guards, semantic
equivalence and four per-pair HTTP-load guards apply. Version 2 also guards the
pre-oracle peak against more than 5% regression. Samples with deferred or failed
cleanup cannot qualify by doing less maintenance. Preparation, copies and all
measurement stages share the 1,800-second budget; ordinary correctness tests are
outside that budget.

The version 2 characterization on 2026-10-17 used a corrected scalar-binding baseline
derived from `e1e4cc8`, with the measurement and cooperative cleanup changes above.
The clock started before private package copies and candidate preparation; the driver
received that existing `Budget` through `run(args, budget=budget)`. The run finished at
1,383.81 seconds. A further independent source, seed and harness integrity check
completed within the same budget at 1,402.03 seconds. No algorithm was retained.

Only the three unscored warm-ups completed:

| Runtime | Supervised wall | Acquire/apply | Oracle | Pre-oracle peak | Whole-worker peak |
| --- | ---: | ---: | ---: | ---: | ---: |
| Common baseline | 255.812 s | 247.176 s | 5.764 s | 211,484,672 bytes | 1,007,288,320 bytes |
| Outcome grouping | 207.413 s | 200.973 s | 3.907 s | 213,909,504 bytes | 1,013,284,864 bytes |
| Queue handling | 230.558 s | 223.131 s | 4.577 s | 214,843,392 bytes | 1,008,041,984 bytes |

Setup took 1.448, 1.422 and 1.385 seconds respectively; runtime sequence time was
248.230, 201.740 and 224.035 seconds. All warm-ups completed the fixed work with the
same semantic hash and verified publication, without deferred cleanup. Each served
and reserved 123 HTTP attempts and offered 19,780,648 response bytes. Client-consumed
bytes were 19,653,534, 19,653,683 and 19,653,534 respectively; warm-ups do not establish
the per-pair HTTP gate.

The projected scored comparisons, including 25% headroom, required 1,737.09 seconds
for grouping and 1,823.89 seconds for queue handling, exceeding the remaining budget.
Zero scored pairs were admitted. These warm-up differences establish no qualifying
median improvement, end-to-end runtime estimate or cable benefit. Both candidates
remain deferred for insufficient evidence; their sources, tests and raw artifacts
remain outside Git. The delivered runtime matches the measured common baseline.

The original raw report used aggregate `deferred` while its individual admission
reasons correctly said `insufficient_evidence`. The driver now reports the latter
when eligible candidates lack complete comparisons. That reporting-only correction
was tested after characterization; the raw report and measured harness snapshot were
preserved, and no performance sample was rerun.

The first optimization campaign (2026-10-17) used baseline commit
`1f2d4dade321aacc4e07d5ebecfd0a09124ec530`. It ended after 1,790.02 seconds,
including supervised shutdown, with `insufficient_evidence` and no retained candidates.
Only one complete scored pair and one additional binding sample finished:

| Scored sample | Acquire/apply | Supervised wall | Fresh-worker peak RSS | Served HTTP attempts |
| --- | ---: | ---: | ---: | ---: |
| Baseline, pair 1 | 164.861 s | 170.756 s | 666,058,752 bytes | 123 |
| Binding, pair 1 | 412.259 s | 418.955 s | 668,221,440 bytes | 122 |
| Binding, pair 2 (unpaired) | 204.317 s | 212.720 s | 673,431,552 bytes | 124 |

The second baseline sample reached the shared deadline. Three complete pairs are
required, so these results establish no qualifying median improvement. The completed
pair showed a binding slowdown. Completed samples had identical semantic hashes,
including private state and verified publication. Unscored warm-ups took 147.148 seconds
for baseline acquisition/application and 423.890 seconds for binding; they cannot qualify
an optimization. Raw metrics, complete source inventories and candidate sources remain
in the private campaign artifacts.

No optimization from that campaign was retained. Outcome grouping and
queue handling were tested in isolated variants but did not reach timed comparison.
Projection reuse was deferred before timing: a decoded boolean and raw numeric JSON
can compare equal in Python while fresh and replay validation accept different inputs.
Urgency caching, more HTTP workers, publication redesign and reduced integrity checks
remain deferred. A future investigation should explain full-pass JSON relation cost and
sample variation before proposing another transport change. No production runtime or
USB cable speedup follows from this campaign.

### Version 3 acquisition campaign

Version 3 keeps the earlier manifest workflows and budgets unchanged and adds a
10,800-second campaign for four independent candidates: `claims`, `queue`,
`closed_any` and `keyset`. Use the same campaign command and a data-only manifest:

```json
{
  "version": 3,
  "claims_mode": "lookup_only",
  "baseline": {"path": "/trusted/baseline/oddsfox_catalogue", "source_hashes": {}},
  "candidates": {
    "claims": {"path": "/trusted/claims/oddsfox_catalogue", "source_hashes": {}},
    "queue": {"path": "/trusted/queue/oddsfox_catalogue", "source_hashes": {}},
    "closed_any": {"path": "/trusted/closed-any/oddsfox_catalogue", "source_hashes": {}},
    "keyset": {"path": "/trusted/keyset/oddsfox_catalogue", "source_hashes": {}}
  },
  "combinations": {
    "closed_any+queue": {"path": "/trusted/combined/oddsfox_catalogue", "source_hashes": {}}
  }
}
```

Every illustrative empty inventory must be replaced with the complete source
inventory, using forward-slash relative filenames on every OS. Package paths may be
relative to the manifest for a relocatable bundle. Backslashes, traversal, drive/ADS
syntax and case-insensitive inventory collisions are rejected.

`claims_mode` is optional in version 3. Its default, `indexed`, preserves the index
resource gate. `lookup_only` restricts the claims variant to `current_store.py`, skips
index diagnostics and applies the unchanged strict full-pass gate to selective lookup
without indexes. It never qualifies index installation. Earlier manifest versions
reject this field. Combination names contain sorted, unique feature names joined with `+`.
The driver never merges code. Only trusted, reviewed local Python packages may be
supplied; hashes prove integrity, not trust. Candidate and combination changes must
stay within their permitted production surfaces.

Preparation, copies, warm-ups, comparisons, validation and cleanup share one monotonic
budget, with 15 seconds reserved for shutdown. Admission protects final-comparison and
integrity time and requires 25% headroom for scored work. Each workload uses one
unscored warm-up per runtime and three fresh-worker pairs in AB, BA, AB order.
Ordinary correctness tests are outside the characterization budget. No selective
reruns or production-limit increases are permitted.

Claims and queue use the unchanged full-pass oracle and performance gates. Index
installation, retained/WAL/temporary growth, peak memory and subsequent writes require
separate evidence; a faster query cannot qualify an unsafe or unusable installer.
Index rollback does not promise physical disk reclamation. Allocated bytes remain
charged to retained storage.

API workloads separately exercise mixed-lifecycle market-ID retrieval, dense and
sparse bootstrap, and an unchanged incremental tail. Their oracle must preserve
expected contents, memberships, identity/claims, absence decisions, receipt coverage
and verified publication while validating intentional request/progress differences.
Later pages may resolve parent nominations without repair requests. Attempt state
must exactly match the ledger of settled units' supplied attempt writes, including
their IDs and timestamps; an arbitrary subset is not sufficient.
The original strict full-pass oracle is not relaxed. API timing requires at least
10% and one second primary runtime improvement, with at most 5% wall/RSS regression;
dense and unchanged-tail runtime regressions may not exceed 5%. Attempt counters
cannot increase. Only keyset workloads permit up to 5% additional downloaded bytes
for page envelopes; all production byte ceilings remain unchanged.

Fixtures model the documented [bounded market lookup](https://docs.polymarket.com/api-reference/markets/list-markets),
[event keyset](https://docs.polymarket.com/api-reference/events/list-events-keyset-pagination)
and [market keyset](https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination)
contracts. Offline certification does not establish live API acceptance or cursor
lifetime. [DuckDB's index guidance](https://duckdb.org/docs/current/guides/performance/indexing)
also warns that index buffers can retain substantial memory; compressed database
size alone is not an installation reservation.

The lookup source contains 2,500 events and 10,100 markets. Its 10,000 targets are
8,900 closed, 900 open and 100 orphan source markets, plus 100 already-stored orphan
markets absent from the provider. Native 404s must renew those known entities'
receipts after an ordinary bounded listing proves their omissions; unknown numeric
gaps do not create absence receipts. The expected rows
retain the absent markets. Discovery uses 1,000 events and 4,040 markets, with a
10-fold ID stride for the sparse profile. The tail fixture starts with completed
numeric frontiers and must avoid another bootstrap crawl.

The exact supplied final package must pass every retained feature's workloads.
If it is missing, fails or cannot finish, the highest-priority independently qualified
single candidate is the only possible fallback, after final integrity validation.
Incomplete pairs, exhausted budgets and unfinished validation never qualify changes.
Reports distinguish synthetic workload results from deployment acceptance; they do
not establish a full-catalogue runtime or USB cable benefit.


#### Version 3 evidence, 2026-10-18

The interrupted local POSIX campaign used runtime baseline commit
`0e03fcf45902ae3adba588f7e839193b8b86c081` and measurement version 2. This commit
predates the native Windows foundation and is not a usable native Windows baseline.
Earlier campaign timings are not reused.

Claim diagnostics completed both requested scales under the existing limits:

| Claims | Fixture population | Index build | Post-index write/checkpoint | Measured storage after write |
| ---: | ---: | ---: | ---: | ---: |
| 200,000 | 135.849 s | 0.256 s | 0.041 s | 44,576,821 bytes |
| 1,000,000 | 521.884 s | 1.893 s | 0.018 s | 190,328,885 bytes |

These are diagnostic storage snapshots, not peak WAL/temporary reservations.
Whole-worker peak RSS was 2,340,995,072 bytes across population, indexing and writes;
it does not isolate index-build memory. A small insert/delete check does not establish
sustained replacement or conflict-repair costs. No safe self-contained build and
ongoing-write admission model was established, so ART indexes and their explicit
maintenance commands remain deferred. Selective queries are implemented independently
without indexes; they require fresh `lookup_only` comparison evidence. No index
break-even invocation count is established.

The private API candidates failed independent correctness review before API scoring:
a transient optional-capability reprobe failure could replace a working version-3
certificate after the legacy lane proofs succeeded. Returning an unsupported capability
on initial activation is valid; losing a working certificate on failed reprobe is not.
The implementation now preserves certificates, fences contradicted proofs, and commits
probe state atomically. Compatibility, corruption/recovery and rollback regression
tests were added. The API paths are included behind explicit certification; native
execution and performance qualification remain pending.

An earlier preparation attempt hit the SQL memory ceiling while constructing the
claim fixture in one oversized transaction. The corrected fixture uses bounded
10,000-row transactions. The failed attempt, completed unscored warm-up and exact
source snapshot were preserved; no scored sample was reused. Both attempts consume
the original shared clock.


The local campaign was stopped at the user's request after 8,653.523 seconds on its
original shared clock. It retained no optimization evidence: only one scored queue
pair completed (350.080 s baseline versus 364.180 s candidate acquisition/application),
while three pairs are required. Completed samples, failed preparation and all source
inventories remain private. No scored sample was selectively rerun.

The user moved full runs and performance comparisons to native Windows and authorized
merging code with that testing to follow. Accordingly, this change makes no speedup
claim. The Windows comparison must start a fresh campaign from a common portable
foundation, use matching OS/measurement versions and frozen source inventories, and
compare the delivered feature set. Historical POSIX timings cannot qualify that run.

### Version 4 store application campaign

Version 4 compares response-local outcome grouping and reverse claim ownership
lookups independently against one current baseline. Both changes belong to the
store module, so the combined runtime must be supplied and reviewed explicitly;
the driver never combines them by copying their shared file. Versions 1–3 keep
their existing manifests, defaults and workflows.

Use the same campaign command with this data-only manifest:

```json
{
  "version": 4,
  "baseline": {"path": "inputs/baseline/oddsfox_catalogue", "source_hashes": {}},
  "candidates": {
    "outcome_grouping": {"path": "inputs/outcome_grouping/oddsfox_catalogue", "source_hashes": {}},
    "claim_reverse_lookup": {"path": "inputs/claim_reverse_lookup/oddsfox_catalogue", "source_hashes": {}}
  },
  "combined": {"path": "inputs/combined/oddsfox_catalogue", "source_hashes": {}}
}
```

Replace every illustrative empty inventory with complete source hashes. Candidate
and combined packages may differ from the baseline only in `current_store.py`.
Run semantic tests on each package before freezing; file ownership alone cannot
establish that both changes survive in the combined implementation.

The campaign uses one 10,800-second budget, beginning before measured package and
seed preparation. It preserves measurement-version-2 metrics and the existing
10,000-event/40,400-market interrupted/resumed workload. Each distinct runtime gets
one unscored warm-up; comparisons use three fresh-worker pairs in AB, BA, AB order.
Scored runs remain unprofiled. Admission requires 25% headroom and protects final
combination, integrity and shutdown time. Missing pairs or unfinished final
source/seed/harness validation cannot retain an optimization.

The unchanged gate requires at least 10% and one second acquisition/application
savings, at most 5% wall-time and worker-peak regressions, exact semantic and
publication equivalence, no increased HTTP counters and equal completed cleanup.
If both candidates qualify, the supplied combination must pass the same comparison.
Otherwise the stronger qualified individual is retained, preferring grouping on
an exact savings tie. A single qualifier reuses its independent comparison.

Optional extended claim diagnostics run only after mandatory comparisons, without
spending integrity or shutdown reserves. The 200,000/1,000,000-claim fixtures compare
actual selective queries and namespace-aware result multisets before and after
diagnostic indexes, with query plans, bounded SQL writes and checkpoint timings.
Filesystem sizes are samples and RSS is a worker lifetime peak. These fixtures do
not prove full application behavior, production write costs or safe index admission.
No production index, installer or configuration switch is added.

Keep the manifest, frozen packages, patches, reports, logs and bundle checksum in a
durable directory outside Git, on the same SSD as the measured synthetic roots.
Record Mac/OS/Python/DuckDB and filesystem provenance, effective settings and the
negotiated USB link when available. An unknown link stays unknown; advertised SSD
throughput is not a measured database rate. Do not use temporary-directory copies
as the only handoff. Old missing evidence remains missing rather than being
represented as reconstructed original measurements.

#### Version 4 evidence, 2026-10-20

The Mac SSD campaign completed both independent three-pair comparisons in its
shared budget. Neither qualified: grouping's median acquire/apply time was
198.838 s versus 172.427 s, and reverse lookup's was 119.035 s versus 115.203 s.
Grouping also failed one strict runtime byte-counter comparison. No store change
is retained; all delivered runtime files match baseline `25315c6`. The explicit
combined package passed semantic tests but was not timed because neither candidate
qualified. Final integrity passed, and the 200,000/1,000,000-claim diagnostics
completed. Their recorded plans still show sequential scans with indexes present.

The [acceptance record](current-catalogue-acceptance.md#version-4-mac-store-application-campaign-2026-10-20)
contains phase/resource results, source hashes and the durable bundle checksum.
It also records the reporting-only fixes applied after measurement; original
reports and measured source are preserved. These synthetic results qualify no
production optimization and establish no full-catalogue or cable-benefit estimate.

### Version 5 application-boundary campaign

Version 5 evaluates three store changes independently: factoring the shared receipt
timestamp and response ID, consolidating one-to-one state reads, and transporting
lookup keys as typed string lists. It uses the existing campaign command:

```sh
uv run --locked python scripts/benchmark-optimization-campaign.py --manifest /absolute/path/variants-v5.json --output /absolute/path/fresh-v5-campaign
```

The data-only manifest requires all three candidates and accepts explicit combinations:

```json
{
  "version": 5,
  "baseline": {"path": "inputs/baseline/oddsfox_catalogue", "source_hashes": {}},
  "candidates": {
    "receipt_factoring": {"path": "inputs/receipt_factoring/oddsfox_catalogue", "source_hashes": {}},
    "read_consolidation": {"path": "inputs/read_consolidation/oddsfox_catalogue", "source_hashes": {}},
    "typed_keys": {"path": "inputs/typed_keys/oddsfox_catalogue", "source_hashes": {}}
  },
  "combinations": {
    "read_consolidation+receipt_factoring+typed_keys": {"path": "inputs/combined/oddsfox_catalogue", "source_hashes": {}}
  }
}
```

Replace illustrative empty inventories with complete package-relative Python source
hashes. Combination keys contain at least two distinct candidate names in lexical
order, joined with `+`; the entire `combinations` object is optional. Every supplied
package is trusted executable local code. Inventories establish integrity, not trust.
Unknown fields, unsafe paths and changes outside `current_store.py` are rejected.
Review and semantic tests must also establish that each combination contains its
declared features; file ownership alone cannot do that.

The common baseline and private candidates pass correctness review before measurement.
Fresh measured packages and one deterministic 10,000-event/40,400-market seed are
frozen under one 10,800-second budget. Preparation, copies, warm-ups, comparisons,
diagnostics, integrity validation and cleanup all consume that budget. Correctness
tests are outside it. Domain clocks are frozen; production deadlines, rate limiting,
application, prefetch, locking and publication remain active. The sequence interrupts
after 50 settled units and resumes to completion.

Candidates run in receipt-factoring, read-consolidation, typed-key order against the
same baseline. Each measured runtime gets one unscored warm-up; each comparison has
three fresh-worker pairs in AB, BA, AB order. Admission protects 25% headroom, the
15-second shutdown reserve and a separate 15 seconds for final integrity. After a
candidate qualifies, later admissions also protect a final-combination reserve of
125% of the largest completed qualifying comparison duration.

The unchanged gate requires at least 10% and one second median acquire/apply savings,
at most 5% regression in supervised wall time, whole-worker RSS and pre-oracle peak,
equivalent public/private state, request/cursor progression and verified publication,
no increase in any of the four HTTP counters, and equal completed maintenance.
Deferred cleanup cannot qualify. A single qualifier reuses its independent evidence.
For multiple qualifiers, only the supplied exact feature combination is evaluated.
If it is absent, fails or cannot finish, retain the qualified single with the greatest
absolute median savings, breaking ties in the candidate order above. The driver
does not compose the shared store file or search smaller combinations.

V5 warm-ups use direct timers without cProfile and emit `warmup_diagnostics` with
`version: 1` and `attribution_mode: "direct_unprofiled"`. Diagnostics separate SQL
execution from result materialization and report helper timings, statement counts,
scalar parameters and list elements. Nested timings overlap; process CPU includes
all worker threads. Native query plans/profiles are separate diagnostic artifacts;
their difference from client timings is not proven parameter-conversion cost.
Scored workers have no attribution instrumentation, and measurement-version-2
qualification fields remain unchanged. Versions 1–4 retain their existing behavior.

Persist partial reports and revalidate source packages, seed and harness before
retention. Missing pairs, source changes or unfinished integrity checks cannot
qualify an optimization. Optional scheduler scaling diagnostics run only with
comparison and integrity reserves protected. Keep private packages, patches, roots,
commands, reports and a checksummed evidence bundle outside Git on the same SSD.
Record host/software/filesystem provenance and effective settings; unavailable USB
link information stays unknown. These synthetic results cannot establish native
Windows acceptance, deployment acceptance, full-catalogue runtime or cable benefit.
