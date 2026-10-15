# Current catalogue.v3

This is a current-state catalogue, not an observation warehouse. Its public contract is
`oddsfox.polymarket.catalogue.v3` (`PROJECTION_VERSION = 1`), defined in
`src/oddsfox_catalogue/current_contract.py`. Each publication contains exactly these
four Parquet relations, `manifest.json`, and a root-level `published/current.json`
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
New snapshots use Parquet ZSTD level 3. Readers must support that codec; older
releases remain readable in their stored encoding and are never rewritten solely
to change compression. metadata.v1 keeps its existing encoding.

## Fresh root and verified seed

Use a separate external root and preserve the old root, configuration, and matching
runtime unchanged. `--root` is a global option and must precede every command.
`CATALOGUE_ROOT` is the environment alternative. Current configuration is
`ROOT/config.toml` (or `CATALOGUE_CONFIG`) with only `[gamma]` and `[catalogue]` sections;
inspect effective values with `config show`.

```sh
uv sync --locked
uv run catalogue --root /srv/catalogue-v3 init
uv run catalogue --root /srv/catalogue-v3 config show
uv run catalogue --root /srv/catalogue-v3 import-release /archive/releases/RELEASE \
  --manifest-sha256 PINNED_MANIFEST_SHA256 --raw-root /archive/raw
```

`init` writes `catalogue.json` with store contract
`oddsfox.polymarket.catalogue_store.v4`. It rejects legacy roots, nonempty unstamped
roots, and roots nested inside legacy or current roots. There is no in-place migration.
Import requires a pristine current store and verifies the pinned v2 release plus
original event raw pages. It preserves original observation ages; it does not refresh
them to import time. Original response IDs are unrecoverable: the recorded seed tie
adaptation orders `legacy-baseline:<observation_id>` before `legacy-event:<page_id>`.
Membership comes from independently verified event pages, not the old bridge table.
Event evidence supplies memberships only; it does not overwrite imported entity
descriptions, native mappings, or entity receipts. If a latest complete membership
references a market excluded by the legacy capture cutoff, import supplements that
missing market from its latest verified event-source record. This exception adds only
markets required by those memberships, preserves original receipt ages, and leaves
discovery frontiers unchanged. Global native ownership checks include the supplements;
a genuine collision makes every affected owner unavailable. The seed records the
supplement count and SHA-256 of lexically sorted canonical IDs, each followed by a
newline (the empty set hashes empty bytes).
Import commits private baseline,
membership, and validation phases, reopening DuckDB between phases to release index
memory. Until validation commits, only status and an import with the same frozen
source are allowed; publication and normal work remain blocked. Rerun the same import
command after an interruption to resume the last committed phase.
Private import protocol 2 includes this supplementation. A checkpoint from the stopped
protocol 1 import requires a fresh root; preserve that failed root as evidence.
A repeated completed import verifies its pinned seed identity. Import is offline and
makes no source-completeness claim. Never delete the old runtime/root to make room.
Rollback means returning consumers and scheduling to that preserved pair.
Do not deploy a copy of the benchmark seed directly: it retains an older measured
publication cost whose 50% reserve exceeds the 20-minute budget. Provision a fresh
root through the pinned import with the current installed runtime, then measure its
actual publication and acquisition headroom before deployment. An unchanged publish,
completed import rerun or restore does not recalibrate publication cost. Preserve
the measured cost and reserve; insufficient headroom remains an unmet deployment gate.
Select the old root explicitly with the preserved executable, and retain the original
`CATALOGUE_CONFIG` if one was used:

```sh
CATALOGUE_ROOT=/archive/legacy-root /archive/rollback/catalogue config show
```

Check the resolved raw, warehouse and publication paths before redirecting consumers.
The preserved environment must import the preserved source snapshot independently of
the new checkout; verify this before cutover.

## Provider gate and bounded operations

Before discovery or reconciliation, certify each event and market lane against known
open, closed, and absent controls, using IDs independently established by the operator:

```sh
uv run catalogue --root /srv/catalogue-v3 probe \
  --open-event-id OPEN_EVENT --closed-event-id CLOSED_EVENT --absent-event-id ABSENT_EVENT \
  --open-market-id OPEN_MARKET --closed-market-id CLOSED_MARKET --absent-market-id ABSENT_MARKET
uv run catalogue --root /srv/catalogue-v3 refresh
uv run catalogue --root /srv/catalogue-v3 reconcile
uv run catalogue --root /srv/catalogue-v3 status
uv run catalogue --root /srv/catalogue-v3 current
uv run catalogue --root /srv/catalogue-v3 verify
```

The probe compares bounded bulk retrieval to native by-ID responses and records a
policy certificate bound to the provider origin. Both lanes require controls; repeated
flags add controls. The probe prefers an unfiltered pass. If it omits known controls, it certifies explicit
`closed=false` and `closed=true` passes only when their combined native identities match.
Both passes and all deterministic split requests must commit before a window advances.
Pending work freezes its exact requests and certificate. A changed provider/policy
requires a new probe; incompatible pending work fails with a recovery instruction.
Unsupported behavior remains blocked. Lifecycle transitions can cause omissions between
passes, and the boolean controls do not prove coverage of unknown lifecycle values.
Known entities remain retained and historical ranges are revisited.
`refresh` advances durable discovery frontiers. `reconcile` prioritizes active/unknown
events, then rotates closed events, all known markets, and historical ID windows every 60 days.
Event responses also update their embedded markets. Unlinked market corrections may
wait for the 60-day cold rotation. Work survives between invocations.
Historical rotations pin their ID bounds and measure the next cycle from the prior
cycle's start. Completed historical cycles become eligible again after 30 days
(or a stricter configured cold interval), leaving time to finish before the
60-day target. Individual cold entities remain due at 58 days. In-progress
windows retain their frozen bounds; status reports range ages separately from
entity receipt ages.
`publish` explicitly builds a verified release; `current` and `verify` verify the pointer.
`status` reports progress, backlog/freshness, unstarted or overdue historical ranges,
last run, verified backup age, and a
`recovery_point_overdue` flag when no backup exists or the last success exceeds 24 hours.
Its `ready` status means seed initialization is complete; inspect freshness and
recovery fields separately before deployment.

In refresh output, `backlog` describes discovery only. The top-level
`parent_event_repair_count` reports the live queued event checks, including
highwater rechecks whose memberships may already be complete. A no-op release
retains its original manifest backlog snapshot; use the live counter and `status`
for current work. A single refresh repairs at most 100 parents. Follow-up
invocations may be needed before every newly discovered market has a verified
event relationship; never infer those links from market-side references.

One supervised HTTP worker, one root writer lock, and at most 10 requests/second are
allowed. The worker holds that lock through database and filesystem changes, so
killing its CLI parent cannot admit a second canonical writer while it finishes.
Default per-invocation ceilings are 1200 seconds, 25,000 HTTP attempts,
4 GiB downloaded, and 16 MiB per response. Publication time is reserved inside the
refresh deadline (at least 60 seconds). Import and backup operations have separate
four-hour supervision. The approved full-store refresh and standalone verification
target is 20 minutes each; the previous five-minute target was not met.
See the [acceptance record](current-catalogue-acceptance.md). A deadline can stop work before a new
publication; inspect the result and verify the existing pointer.

Active/unknown event membership freshness within 24 hours, and closed events and all
known markets within 60 days,
are conditional targets, requiring compatible provider behavior and adequate cadence
and capacity. Default due thresholds are 20 hours and 58 days, leaving scheduling
margin. Active checks become urgent at 22 hours. Neither a completed scan nor an
empty response establishes source completeness.

Existing roots with explicit 300-second or 64 GiB configuration retain those stricter
values. Inspect `config show` and update the root's `config.toml` deliberately while
idle to adopt the new sample limits; do not overwrite provider settings or modify
immutable seed and legacy roots. The combined retained acceptance envelope remains
256 GiB, including legacy evidence and benchmark roots.

A busy invocation exits **3** with `status: busy`; other blocked failures also exit 3.
Inspect the status/message before retrying. For a busy lock, allow the current worker
to finish, then retry at most twice with a bounded delay (for example 60 seconds).
Persistent busy or blocked results require operator investigation, not an infinite loop.

## Storage and recovery

Admission limits: 96 GiB retained root, 8 GiB raw, raw age at most seven days,
8 GiB temporary space, and 32 GiB free-space reserve. Keep seven daily backups;
backup creation retains one verified backup per day. Pending responses are protected
from pruning, so an exhausted allowance blocks progress rather than discarding needed
recovery data. These are ceilings, not a promise that any corpus fits.
Backups use `catalogue_backup.v4`: the checkpointed database is gzip-compressed;
publication and pending-evidence files retain their original bytes. The inventory
binds both compressed and expanded database sizes and hashes. Verification expands
only the database and root stamp into a fresh directory outside every catalogue and
legacy root, reopens that database, and checks the inventoried publication and pending
evidence through the same bounded connection. Those copied bytes plus SQL spill must
fit the separate 8 GiB temporary ceiling. Restore admits the full expanded bundle.
Verification uses a private owned directory below the external system temporary
directory and a shared fail-fast verification lock. Surviving workspace bytes reduce
the next copy/spill allowance. Keep the selected system temporary directory consistent
across invocations and count this private base in host storage accounting.
The supervisor cleans only its own invocation directory
after its worker has stopped. A whole-process crash can leave charged artifacts;
the private base is `oddsfox-catalogue-verification-<uid>` below that temporary
directory, containing `invocation-<id>` directories. Stop canonical and recovery jobs,
inspect ownership and contents, and confirm no verifier is active before operator recovery.
Legacy flat `catalogue-verify-*` directories block verification until explicitly
recovered when owned by the current user. Foreign-owned entries are ignored.
No automatic prefix cleanup is performed.
Seven retained backups plus the next staging copy must fit the retained ceiling.
They must also leave the normal changed-publication admission reserve available;
fitting the backups alone does not establish an operable root. The measured
mixed-codec checkpoint exceeded the former 64 GiB ceiling. Revalidate it against
the approved 96 GiB ceiling; see the acceptance record.
A full root fails explicitly rather than deleting verified recovery points before
their replacement is ready.

```sh
uv run catalogue --root /srv/catalogue-v3 backup create
uv run catalogue --root /srv/catalogue-v3 backup verify /archive/BACKUP
uv run catalogue --root /srv/catalogue-v3 backup restore /archive/BACKUP \
  --destination /srv/catalogue-v3-restored
uv run catalogue --root /srv/catalogue-v3-restored verify
uv run catalogue --root /srv/catalogue-v3-restored status
```

An additional offline copy is optional; verify any copy before relying on it. Restore
only to a fresh destination. Backup verification checks its inventory and checksums; recovery
includes the store, pending progress/raw dependencies, and last publication pointer.
Recovery is explicitly local-only: disk-failure protection has been waived. Status
reports `recovery_scope: local_only` and `disk_failure_protection: false`; backup age
and `recovery_point_overdue` measure the daily verified local recovery point only.
Losing the catalogue device can lose both the database and its local backups.
Verify the restored pointer before redirecting consumers or scheduling. Interrupted
work resumes from committed progress and pending responses; a failed publication keeps
the last verified pointer. Raw retention is bounded: there is no permanent replay
archive and no promise of reconstruction from arbitrarily old raw data. Preserve the
old runtime/root for rollback.

## Isolated metadata.v1

The existing targeted CLI and `oddsfox.polymarket.metadata.v1` contract remain separate:

```sh
uv run catalogue --root /srv/metadata-producer metadata refresh \
  --market-id MARKET_ID --output /srv/handoffs/metadata-release \
  --max-output-bytes 134217728 --max-response-bytes 8388608 \
  --max-requests 500 --max-download-bytes 134217728
uv run catalogue --root /srv/metadata-producer metadata export \
  --market-id MARKET_ID --output /srv/handoffs/metadata-export \
  --max-output-bytes 134217728
```

Repeat `--market-id` for more IDs. Preserve metadata.v1 validation and consumer behavior;
use an isolated producer root. A v3 root is rejected and refresh rejects an existing
legacy warehouse/ledger producer root. Export is offline; refresh calls Gamma.
Producer and output paths must also stay outside v3 root descendants. Refresh checks
its configured raw-evidence and lock paths too, including resolved symlink targets,
before creating directories or making requests. Offline export from legacy evidence
remains supported.

New targeted request and response receipts retain sub-millisecond precision so
offline export preserves their order against older raw observations. Existing
millisecond receipts remain readable as recorded; lost precision cannot be recovered.

## Verification and deployment evidence

The [acceptance record](current-catalogue-acceptance.md) distinguishes measured
results from outstanding performance, freshness and recovery gates.

Run `scripts/verify-fast` during development and `scripts/verify` before completion.
Tests and installed-workflow checks use synthetic loopback fixtures, never live Gamma.
Build and audit the distribution, then exercise an installed executable outside the
checkout using `uv run python scripts/verify-installed-workflow.py --output /tmp/catalogue-installed-proof`
as the deployment check (the output destination must not exist). Audit built archives
with `uv run python scripts/verify-distribution.py dist/*.whl dist/*.tar.gz`. Use
`uv run python scripts/benchmark-current.py --help` for the full-store synthetic-delta
benchmark interface. For example:

```sh
/opt/catalogue/venv/bin/python scripts/benchmark-current.py --seed-root /srv/catalogue-v3-seed \
  --output /srv/catalogue-v3-benchmark --runs 3 --delta-markets 1000 --delay-s 0.1 \
  --executable /opt/catalogue/venv/bin/catalogue \
  --account-root /archive/legacy-catalogue
```

Use a fresh output directory and an idle, checkpointed, verified seed with no pending
work. Use the same installed-wheel Python for the harness and CLI; `--executable`
alone pins only the CLI children. Repeat `--account-root` for every external
legacy/operator root; repository data
is counted automatically. The harness freezes a controlled loopback HTTP provider,
includes new memberships on existing events, checks exact content independently, and
measures each verified refresh, bounded parent-repair follow-ups and the complete
sequence separately, then checks unchanged refreshes after those repairs drain.
The exact seed-plus-delta comparison remains mandatory. Stalled or unexpected
repair work fails the frozen follow-up bound. Unchanged highwater requests may
queue already-complete parents again; those live counts are disclosed separately.
The harness does not reproduce historical
network delays or prove 24-hour/60-day reconciliation capacity. Measure sustained
reconciliation, initial catch-up, import, backup and restore separately. Retain elapsed
time, HTTP/download counts, publication time, freshness/backlog and resource evidence
before making performance claims.

For bounded full-population reconciliation characterization, use
`uv run python scripts/benchmark-capacity.py --help`. Supply an immutable verified
`--seed-root`, a separate idle `--root` imported with the current runtime, a fresh
`--output`, the installed `--executable`, and every external `--account-root`.
The explicit `--characterize` mode probes a disk-backed loopback provider, runs a
refresh and bounded reconciliation invocations, and reports successful receipt
renewals across the original active and cold populations. It preserves original
receipt ages and counts unresolved memberships separately. The provider reconstructs
current domain responses, including exact native identifiers and complete event
memberships; it does not reproduce original raw response bytes. Unsupported invalid
identity forms fail closed.
Market lists deliberately default to open status so the actual probe requires
both open and closed passes, matching the frozen production request policy.

This checkout-only harness runs invocations consecutively. Its result is always
characterization, never sustained acceptance: it does not exercise real hourly
cadence, daily backups, long-term churn or genuine repeated active renewal.
Backup mode is refused until its external verification workspace is accounted for.
SQL spill is disabled; bounded audit, logs and reports are included in admission.
`--max-total-seconds` bounds admission of child commands; accounting, SQL snapshots
and cleanup can extend total elapsed time beyond it.
Run it serially with other large workloads, retain partial reports after failure,
and independently check domain content before claiming full acceptance.

The Makefile frontends wrap this CLI; inspect the Makefile for current targets. Historical
capture/load/dbt/replay/rebuild/validate commands are retired and reject use here.
[The cron example](../ops/catalogue.cron.example) is **disabled template text only**:
all lines are comments. It illustrates hourly refresh, offset hourly reconcile,
and daily backup with an external root and absolute installed executable placeholders.
Do not activate it until import/probe, recovery, installed verification, and representative
benchmark evidence support the chosen provider and cadence.

## Historical warehouse documentation

The retired warehouse acceptance reports are preserved at Git revision
`a03d7cb1910530358df40180bd7a5caae3da41fd` under
`docs/acceptance-2026-10-10.md` and `docs/completion-storage-2026-10-12.md`.
They describe the retired dlt/dbt workflow. Use this guide and the
[current acceptance record](current-catalogue-acceptance.md) for the current runtime.
The [v2 contract](catalogue-v2.md) remains available for legacy import and rollback.
