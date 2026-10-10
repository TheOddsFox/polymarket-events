# OddsFox Polymarket catalogue

A local, single-host batch pipeline that keeps a catalogue of Polymarket **events** and their
**markets** (metadata and outcome definitions). It deliberately excludes trades, order books,
and price history.

```
Gamma API ──capture──▶ data/raw (immutable JSON.gz pages + manifests) ──load (dlt)──▶ bronze
        bronze ──dbt──▶ staging ─▶ core (current) ─▶ observation history ─▶ marts
        marts ──publish──▶ data/published/releases/<id>/*.parquet + current.json
```

Raw pages are the source of truth. Everything after them is derived and can be rebuilt.
The replacement warehouse exports `oddsfox.polymarket.catalogue.v2`; see
[the catalogue contract](docs/catalogue-v2.md). Existing legacy warehouse roots must be retained
and replaced with a fresh operator root. No migration or compatibility exports are provided.

## Setup

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```sh
make sync      # uv sync --locked
make test      # full test suite; never calls the live Gamma API
make lint
make test-dev  # fast dev loop: lint, then the full test suite
```

## Operator commands

| Command | What it does |
| --- | --- |
| `make bootstrap` | First run on an empty project: capture everything, load, build, publish. |
| `make refresh` | Daily observations: direct open markets/events and missing open or unknown records. |
| `make reconcile` | Weekly full pass: every event and every closed market by id range, open markets by keyset, then by-ID fetches for referenced events those scans did not return. |
| `make replay` | Load pending raw pages, build, publish. No Gamma calls. |
| `make validate` | Run dbt tests. It invalidates certification; run a complete build or replay before publishing. |
| `make publish` | Recheck the certified warehouse and promote a verified immutable release. |
| `make current` | Verify and show the current published release pointer. |
| `make status` | List capture batches and the warehouse build-validity record. |
| `make verify` | Verify the current release independently of warehouse validity. |
| `make backup` | Write a checksummed backup to `data/backups/<stamp>`. |
| `make verify-backup BACKUP=...` | Re-hash a backup and list any problems. |
| `make restore-backup BACKUP=... DESTINATION=...` | Verify and restore into a new operator root. |
| `make rebuild` | Rebuild raw evidence in fresh scratch storage and compare all semantic schemas, relations, projections and coverage. Exit 4 on mismatch. |

Each stage takes the run lock. A second writer fails immediately (exit 3) instead of racing.
Refresh commands log scan progress and Gamma retries to stderr. The JSON result is still printed to stdout when the command finishes.
Each invocation starts a new batch. Resume only a named batch, using its verified committed
scope and pages; no new high-water mark or automatic tail is added:

```sh
export CATALOGUE_ROOT=/absolute/path/to/fresh/operator-root
catalogue capture --mode selected --market-id 123 --market-id 456
catalogue refresh --mode selected --market-id 123
catalogue capture --mode selected --event-id 789
catalogue capture --mode bootstrap
catalogue capture --mode bootstrap --resume BATCH_ID
```

Selected mode accepts at most 100 explicit market IDs and 100 event IDs. It also fetches parent
events referenced by selected markets. Incidental embedded markets remain raw evidence and are
excluded from the selected dataset. A selected event without selected market IDs contributes
only that event. Confirmed absence records coverage and preserves earlier observations.
Budget exhaustion leaves incomplete work; it cannot publish. Resume receives a new finite
invocation allowance while retaining cumulative batch measurements. Every stage writes its
status, counts and error to the ledger. Do not reuse legacy warehouse roots.

Lower-level commands: `uv run catalogue --help`.

## Manual operation

Completion covers manually invoked catalogue commands. Scheduler definitions and templates are
optional operator tools; none is activated by installation or verification. CLI and Dagster
resolve the same packaged dbt resources, warehouse and resource limits. Generated targets,
logs and dlt state stay under the operator root.

## Invocation limits

Every capture includes planning, retries and follow-up requests in these defaults:

| Resource | Default |
| --- | --- |
| Workers / shared request rate | 1 / 2 per second |
| Actual HTTP attempts | 25,000 |
| Downloaded bytes / individual response | 4 GiB / 16 MiB |
| Invocation duration | 4 hours |
| Retained root / temporary storage | 64 GiB / 8 GiB |
| DuckDB memory | 2 GiB |

Override settings explicitly with `CATALOGUE_<SECTION>_<KEY>` or TOML. Never increase limits
automatically to pass a run. Production requests use Gamma HTTPS, with no redirects, ambient
credentials or proxies. Streamed error bodies and partial downloads count toward the allowance.
Loopback source fixtures require explicit `CATALOGUE_GAMMA_ALLOW_LOOPBACK=1`.

## Quality limits

Each limit is a `[quality]` setting in `config/catalogue.toml`, overridable with
`CATALOGUE_QUALITY_<KEY>`. Values are validated when settings load: ratios must lie in [0, 1],
percentages in [0, 100], and the warn limit may not exceed the error limit.

| Check | Setting | Default | Above the limit |
| --- | --- | --- | --- |
| Quarantined records per batch | `quarantine_max_ratio` | 1% | The batch is not registered and the load exits 3. `refresh` and `replay` then stop before `dbt build`, so nothing publishes until the batch is resolved. Batches registered in the same load are not built either. |
| Open-event drop vs the last published release, warn | `open_events_drop_warn_pct` | 5% | Logged and recorded by the stage. |
| Open-event drop vs the last published release, error | `open_events_drop_error_pct` | 10% | The build is rejected; failed builds never become the next baseline. |

**Recovering a blocked batch.** Inspect the quarantined records and retained evidence. Resolve
source or parser defects before replaying; ordinary unusable identities do not count toward
the 1% malformed-record gate. Do not raise quality limits just to obtain a passing build.

Unusable native identities remain accounted with reasons and no nominated assets; they do not
count toward the malformed-record limit. Confirmed absence does not imply closure or deletion.
Unresolved acquisition failures and untrustworthy envelopes block publication.

## Publication validity

Loads and dbt executions other than parse commit a dirty record before execution. Only a
complete `dbt build` with every required model and test succeeding may certify the warehouse.
Subset, failed and skipped work cannot certify it. CLI and Dagster use the same policy.
A fresh pre-build parse pins the required node inventory independently of the build artifacts;
removed or disabled tests, changed source and mismatched variables cannot certify a build.

The certificate binds complete registered captures, coverage, normalization and model revisions,
effective quality settings, warehouse schemas and semantic content. Publication checks the actual
warehouse again under the root lock, so external dbt edits also invalidate admission. The candidate
release is verified before `current.json` switches atomically. A failed operation leaves the
previous verified release available. Releases and interrupted artifacts are retained; there is no
automatic pruning.

`catalogue verify` checks the pointer, release manifest and every declared output without consulting
the warehouse or its certificate. Use `catalogue verify --release /absolute/path/to/release` to
verify another retained release.

## Recovery

- **Crash during capture**: resume explicitly with `--resume BATCH_ID`. Page IDs and content are
  deterministic, so a resumed page matches what was written before the crash. `scripts/bootstrap-until-done`
  repeats that resume until the refresh exits 0, and it does not start a new crawl after dbt or publish fails.
- **Crash during load**: rerunning the load is a no-op for rows already present. Observation IDs
  are unique and the load is insert-only, so nothing duplicates.
- **Crash during publish**: `current.json` is replaced atomically, so readers keep the previous
  release. Unreferenced immutable releases remain available for diagnosis.
- **Lost warehouse, raw pages intact**: preserve the damaged root. Copy verified raw evidence into
  a fresh root, run `catalogue ledger rebuild` there, then `catalogue replay` and `catalogue rebuild --verify`.
- **Restore a backup**: run `catalogue backup verify BACKUP`, then
  `catalogue backup restore BACKUP --destination /absolute/path/to/fresh/root`. Restore validates
  the complete inventory before copying and refuses existing destinations, unsafe paths, links,
  special files and size or checksum mismatches. Set `CATALOGUE_ROOT` to the restored root, then
  run `catalogue verify` and `catalogue rebuild --verify`.

Portable backups require data, state and warehouse paths inside the operator root and the
packaged dbt project. They retain effective quality and resource settings, then restore into
the standard fresh-root layout. Interrupted copies remain diagnostic evidence and are never
merged by a later restore.

Rebuild scratch directories are new and retained. They use independent state and locks, and read
the original raw evidence without modifying it. Semantic comparison covers bronze, staging, core,
observation history, metrics, relationships, quarantine, every public projection and coverage.
Documented processing columns and invocation snapshot contents are excluded from row equivalence.

## Layout

- `src/oddsfox_catalogue/`: capture, load, dbt runner, publish, rebuild, backup, Dagster definitions, CLI.
- `src/oddsfox_catalogue/dbt/`: explicitly packaged models, macros, tests and profiles; runtime outputs stay outside installed resources.
- `config/catalogue.toml`: defaults. Override any key with `CATALOGUE_<SECTION>_<KEY>`.
- `tests/`: unit, integration, and fakes. `tests/fixtures/gamma/synthetic` holds sanitized payloads.
- `ops/`: Dagster instance and workspace config, launchd templates.
- `data/` and `.state/` are local and never committed.

## Gamma contract

- Daily capture lists direct open markets and open events, then refetches previously open or
  unknown records missing from those lists. Bootstrap and reconcile seal source high-water marks,
  crawl open markets, and scan finite event/closed-market ID windows. Referenced parent events
  receive bounded direct lookups. Scan scope is committed for replay. Failed singleton requests
  remain unresolved work; 404 is confirmed absence and does not imply closure.
- Gamma list endpoints may omit inactive events. Captured market references trigger bounded
  individual parent-event lookups. An inactive event absent from lists and without a captured
  reference may remain undiscovered; no exhaustive individual-ID sweep is performed. Coverage
  records this limitation and never claims whole-source completeness.
- Nested `market.events` entries are references only. They are never treated as full events.
- `outcomes` and native IDs may arrive as JSON arrays or JSON-encoded arrays. Shared Python
  normalization validates the selected identity independently of optional prices. It retains
  both native fields and exact financial strings; dbt handles relational selection and joins.
- Requests share the invocation rate and resource allowance. Retries are explicit, including
  bounded `Retry-After`; hidden client retries are disabled. Resume verifies committed objects
  before network access and skips only verified work in the named batch.

## Status

Every test runs against synthetic fixtures and never calls the live Gamma API. Run
`tools/record_gamma_fixtures.py` from a machine with access to refresh the optional live samples.

## Targeted metadata handoffs

The independent market-data and Polygon collectors consume
`oddsfox.polymarket.metadata.v1`. These commands select at most 100 explicit Gamma
market IDs and write a new immutable directory:

```sh
catalogue metadata refresh --market-id 123 --market-id 456 --output data/metadata/bundles/run-1
catalogue metadata export --market-id 123 --market-id 456 --output data/metadata/bundles/offline-1
```

`refresh` requests only `/markets/<id>`, stores checksummed JSON.gz pages under
`data/metadata/raw`, and indexes request outcomes in the existing SQLite ledger.
`export` makes no requests and uses verified local raw observations. Neither
command loads bronze, builds dbt, changes the six existing exports, or updates the
global `current.json`. Set `CATALOGUE_ROOT` to an explicit operator data root when
invoking the installed executable from elsewhere. The normal catalogue writer lock applies.

The bundle contains JSON-array relations `markets.json`, `outcomes.json`,
`memberships.json`, `identity_history.json`, and `coverage.json`. Its `manifest.json`
declares the contract, targeted coverage, content-derived `source_release_id`,
receipt time and each literal filename's SHA-256 and byte size. Decimal values are
strings. Consumers pin the manifest digest and copy the bundle before acquisition;
the producer's path is not a durable downstream dependency.

Identity projection does not require prices. Outcome ordinals remain 1-based;
`chain_index_set` remains null until a chain collector establishes it independently.
CTF token IDs and Protocol V2 position IDs are separate fields. Explicit `v2`
metadata selects `positionIds`; explicit `v1` selects CTF IDs even when both
arrays are populated. Missing/unknown versions, duplicate/misaligned IDs and conflicting
owners are unusable, with `identity_error` evidence. Metadata protocol names do not
establish an exchange ABI. Equal IDs in different asset kinds are distinct.

Direct market observations outrank event-embedded summaries; ties use receipt time
and observation ID. Selection takes the entire observation, preserving nulls. An
explicit empty `events` array removes memberships; an absent field can use recorded
enclosing event identity. History records observations and receipts, never invented
historical validity intervals. URLs in descriptions remain plain source text.

Coverage distinguishes `found`, source-confirmed `absent` (404), and `failed`.
An unobserved market in offline export is `failed/no_observation`, not absent. A
fresh refresh never fills a failed/absent lookup with stale metadata. CLI stdout
reports the bundle path, release ID, manifest SHA-256, requested/found/absent/failed
counts, `http_attempts` and `downloaded_bytes`; any failed ID returns exit 2 with
those metrics. Input/busy/contract failures return exit 3.
Identity quarantine is inspectable in a successful handoff and consumers must
reject unusable targets.

Targeted refresh permits only the Gamma HTTPS host or an explicit loopback test
server, disables ambient HTTP credentials/proxies and redirects, bounds each
response to 8 MiB, rejects compressed responses before decoding, and allows at
most four retries per selected ID. Immutable bundles are capped at 128 MiB;
`--max-output-bytes` and refresh-only `--max-response-bytes` can lower those limits.
Refresh also defaults to `--max-requests 500` and
`--max-download-bytes 134217728`. These cumulative budgets count retries, HTTP
error bodies and partial downloads. The chunk crossing a byte limit is counted
before aborting, so actual download metrics can exceed the limit by that received
chunk. Zero allowance fails selected lookups without HTTP. Delegating collectors
pass their remaining invocation allowance and debit the returned metrics.

The handoff does not promise automatic whole-catalogue discovery or membership
refresh beyond selected records. Retain raw evidence and consumer copies for
offline replay.

## Repeatable live smoke test

Run from this repository after `uv sync --locked`, or use the installed package outside the
checkout. Live checks are opt-in; CI runs the same workflow against synthetic loopback fixtures.

```sh
uv run --locked python -m oddsfox_catalogue.smoke --market-id 5234660

# Full selected catalogue workflow; repeat --market-id for at most five markets.
uv run --locked python -m oddsfox_catalogue.smoke --workflow catalogue --market-id 5234660
```

Every invocation creates a fresh ignored `data/smoke/<uuid>` root and prints the absolute `report.json` path. Use `--output <fresh-path>` to name a run. Never reuse an output directory. Reports retain the steps, measured requests/bytes, configured limits and coverage. Failed/timed-out commands make `accounting_complete` false; their totals are known lower bounds. No paid requests are made. Run the events smoke first and pass its report to the consumers; consumers validate and pin its metadata bundle. The default metadata workflow needs an explicit usable market; choose another ID when its example becomes unavailable.

The default metadata workflow requires one usable market, verified bundle checksums, an identical
offline metadata replay (coverage observation timestamps excluded), zero replay HTTP requests and
no global catalogue pointer. It checks the targeted handoff.

The catalogue workflow proves capture, load, complete build and publication, a second observation,
semantic offline replay, full raw rebuild, verified backup restoration and preservation of the
previous pointer after an injected publication crash. It shares a 100-attempt and 64 MiB download
allowance across acquisitions, limits the complete output directory to 1 GiB, and applies one
30-minute deadline to the workflow and its child processes. The metadata bundle and existing
downstream report fields remain available. Accounted unusable identities are valid for this catalogue
workflow when they carry a reason and no nominated assets; its report records each selected
identity status and may contain an empty `native_assets` list. Use the default metadata workflow
to prove a usable downstream handoff.

Ambient `CATALOGUE_*` settings are cleared. Explicit loopback source settings are retained only
when the existing loopback opt-in is enabled, for offline fixtures. An unavailable example market
fails explicitly; choose another explicit ID and a fresh output directory.

A selected smoke pass establishes bounded workflow readiness. Full operational readiness also
requires a fresh catalogue-wide bootstrap, daily refresh and reconcile, complete named-batch
resumes, replay/rebuild/restore evidence and passing hosted CI for the final commit. Scheduling
remains inactive, and discovery does not exhaustively sweep inactive event IDs.
Measured results and remaining gates are recorded in the
[acceptance evidence](docs/acceptance-2026-10-10.md).
