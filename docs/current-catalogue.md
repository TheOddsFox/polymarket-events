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
`oddsfox.polymarket.catalogue_store.v3`. It rejects legacy roots, nonempty unstamped
roots, and roots nested inside legacy or current roots. There is no in-place migration.
Import requires a pristine current store and verifies the pinned v2 release plus
original event raw pages. It preserves original observation ages; it does not refresh
them to import time. Original response IDs are unrecoverable: the recorded seed tie
adaptation orders `legacy-baseline:<observation_id>` before `legacy-event:<page_id>`.
Membership comes from independently verified event pages, not the old bridge table.
Event evidence supplies memberships only; it does not overwrite imported entity
descriptions, native mappings, or entity receipts. Import commits private baseline,
membership, and validation phases, reopening DuckDB between phases to release index
memory. Until validation commits, only status and an import with the same frozen
source are allowed; publication and normal work remain blocked. Rerun the same import
command after an interruption to resume the last committed phase.
A repeated completed import verifies its pinned seed identity. Import is offline and
makes no source-completeness claim. Never delete the old runtime/root to make room.
Rollback means returning consumers and scheduling to that preserved pair.

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
events, then rotates closed events, all known markets, and historical ID windows monthly.
Event responses also update their embedded markets. Unlinked market corrections may
wait for monthly reconciliation. Work survives between invocations.
`publish` explicitly builds a verified release; `current` and `verify` verify the pointer.
`status` reports progress, backlog/freshness, last run, verified backup age, and a
`recovery_point_overdue` flag when no backup exists or the last success exceeds 24 hours.

One supervised HTTP worker, one root writer lock, and at most 10 requests/second are
allowed. Default per-invocation ceilings are 300 seconds, 25,000 HTTP attempts,
4 GiB downloaded, and 16 MiB per response. Publication time is reserved inside the
refresh deadline (at least 60 seconds). Import and backup operations have separate
four-hour supervision. Normal refresh below 300 seconds remains a target, unproven
until representative benchmark evidence exists. A deadline can stop work before a new
publication; inspect the result and verify the existing pointer.

Active/unknown event membership freshness within six hours, and closed events and all
known markets within a month,
are conditional targets, requiring compatible provider behavior and adequate cadence
and capacity. Default due thresholds are 4.5 hours and 29 days, leaving scheduling
margin. Neither a completed scan nor an empty response establishes source completeness.

A busy invocation exits **3** with `status: busy`; other blocked failures also exit 3.
Inspect the status/message before retrying. For a busy lock, allow the current worker
to finish, then retry at most twice with a bounded delay (for example 60 seconds).
Persistent busy or blocked results require operator investigation, not an infinite loop.

## Storage and recovery

Admission limits: 64 GiB retained root, 8 GiB raw, raw age at most seven days,
8 GiB temporary space, and 32 GiB free-space reserve. Keep seven daily backups;
backup creation retains one verified backup per day. Pending responses are protected
from pruning, so an exhausted allowance blocks progress rather than discarding needed
recovery data. These are ceilings, not a promise that any corpus fits.

```sh
uv run catalogue --root /srv/catalogue-v3 backup create
uv run catalogue --root /srv/catalogue-v3 backup verify /archive/BACKUP
uv run catalogue --root /srv/catalogue-v3 backup restore /archive/BACKUP \
  --destination /srv/catalogue-v3-restored
uv run catalogue --root /srv/catalogue-v3-restored verify
uv run catalogue --root /srv/catalogue-v3-restored status
```

Copy a completed backup to offline storage, verify that copy, and restore only to a
fresh destination. Backup verification checks its inventory and checksums; recovery
includes the store, pending progress/raw dependencies, and last publication pointer.
Verify the restored pointer before redirecting consumers or scheduling. Interrupted
work resumes from committed progress and pending responses; a failed publication keeps
the last verified pointer. Raw retention is bounded: there is no permanent replay
archive and no promise of reconstruction from arbitrarily old raw data. Retain offline
backups and the old runtime/root independently.

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

## Verification and deployment evidence

Run `scripts/verify-fast` during development and `scripts/verify` before completion.
Tests and installed-workflow checks use synthetic loopback fixtures, never live Gamma.
Build and audit the distribution, then exercise an installed executable outside the
checkout using `uv run python scripts/verify-installed-workflow.py --output /tmp/catalogue-installed-proof`
as the deployment check (the output destination must not exist). Audit built archives
with `uv run python scripts/verify-distribution.py dist/*.whl dist/*.tar.gz`. Use
`uv run python scripts/benchmark-current.py --help` for the full-store synthetic-delta
benchmark interface. For example:

```sh
uv run python scripts/benchmark-current.py --seed-root /srv/catalogue-v3-seed \
  --output /srv/catalogue-v3-benchmark --runs 3 --delta-markets 1000 --delay-s 0.1 \
  --account-root /archive/legacy-catalogue
```

Use a fresh output directory and an idle, checkpointed, verified seed with no pending
work. Repeat `--account-root` for every external legacy/operator root; repository data
is counted automatically. The harness freezes a controlled real-network provider,
includes new memberships on existing events, checks exact content independently, and
measures verified publication plus unchanged refreshes. It does not reproduce historical
network delays or prove six-hour/monthly reconciliation capacity. Measure sustained
reconciliation, initial catch-up, import, backup and restore separately. Retain elapsed
time, HTTP/download counts, publication time, freshness/backlog and resource evidence
before making performance claims.

The Makefile frontends wrap this CLI; inspect `make help` for current targets. Historical
capture/load/dbt/replay/rebuild/validate commands are retired and reject use here.
[The cron example](../ops/catalogue.cron.example) is **disabled template text only**:
all lines are comments. It illustrates quarter-hourly refresh, offset hourly reconcile,
and daily backup with an external root and absolute installed executable placeholders.
Do not activate it until import/probe, recovery, installed verification, and representative
benchmark evidence support the chosen provider and cadence.
