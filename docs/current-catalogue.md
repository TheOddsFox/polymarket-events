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
`[catalogue]` sections. Unknown keys are rejected. A 0.1.0 config opens unchanged: its
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

## Operations

Run each command by hand. Each invocation is bounded by `max_duration_s` (1200 s),
including publication. Publication time is reserved inside that deadline: the reserve is
at least 60 seconds, or 1.5 times the previous measured publication if that is larger.
A `reconcile --full-pass` invocation that is not publishing reserves only
`publication_reserve_s`.

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

## Verification

Run `scripts/verify-fast` during development and `scripts/verify` before completion.
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

The delivery runtime therefore keeps baseline source unchanged. Outcome grouping and
queue handling were tested in isolated variants but did not reach timed comparison.
Projection reuse was deferred before timing: a decoded boolean and raw numeric JSON
can compare equal in Python while fresh and replay validation accept different inputs.
Urgency caching, more HTTP workers, publication redesign and reduced integrity checks
remain deferred. A future investigation should explain full-pass JSON relation cost and
sample variation before proposing another transport change. No production runtime or
USB cable speedup follows from this campaign.
