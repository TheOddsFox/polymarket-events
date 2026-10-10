# OddsFox Polymarket catalogue

A local, single-host batch pipeline that keeps a catalogue of Polymarket **events** and their
**markets** (metadata and outcome definitions). It deliberately excludes trades, order books,
and price history.

```
Gamma API ──capture──▶ data/raw (immutable JSON.gz pages + manifests) ──load (dlt)──▶ bronze
        bronze ──dbt──▶ staging ─▶ core (current) ─▶ history (SCD2) ─▶ marts
        marts ──publish──▶ data/published/releases/<id>/*.parquet + current.json
```

Raw pages are the source of truth. Everything after them is derived and can be rebuilt.

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
| `make refresh` | Daily incremental run: open events plus recently changed events. |
| `make reconcile` | Weekly full pass: every event and every closed market by id range, open markets by keyset, then by-ID fetches for referenced events those scans did not return. |
| `make replay` | Load pending raw pages, build, publish. No Gamma calls. |
| `make validate` | Run dbt tests against the warehouse. Never builds, so it appends no `catalogue_snapshots` row. |
| `make publish` | Write a release if the last dbt build passed. |
| `make current` | Show the current published release pointer. |
| `make status` | List capture batches and their states. |
| `make backup` | Write a checksummed backup to `data/backups/<stamp>`. |
| `make verify-backup BACKUP=...` | Re-hash a backup and list any problems. |
| `make rebuild` | Rebuild from raw in a scratch area and compare table fingerprints with the live warehouse. Exit 4 on mismatch. |

Each stage takes the run lock. A second writer fails immediately (exit 3) instead of racing.
Refresh commands log scan progress and Gamma retries to stderr. The JSON result is still printed to stdout when the command finishes.
A crashed bootstrap (exit 1, including Gamma retries exhausted) can be resumed with `scripts/bootstrap-until-done`. That wrapper retries only exit 1. Exit 2 (a later stage failed) and exit 3 (the run lock, or a blocked stage) stop immediately. SIGHUP, SIGTERM, and SIGINT also stop immediately: the wrapper logs the signal and exits 128 plus the signal number. A SIGHUP or SIGTERM that arrives while a catalogue stage is running is recorded on that stage's row, and the process exits the same way. A pooled capture stops waiting on a rate-limit pause. Unstarted scans are not fetched, and each running scan finishes its current page and stays `running` for resume. While the pool drains, further signals are held and do not cut the drain short. Once every worker has stopped and every client is closed, the run raises the first failure, or the first held signal when nothing else failed. On resume, a market batch is abandoned rather than resumed, and its raw pages are kept, when its plan has no scans, has a scan name the id-range plan does not produce, has no open-market crawl, or plans a closed-market window before the open-market crawl. Logs are written under `.state/logs/`.
Every stage writes a row to `stage_runs` in the ledger (status, counts, error).

Lower-level commands: `uv run catalogue --help`.

## Scheduling

Run exactly one scheduler. Two are available, and they must not both be enabled:

- **Dagster daemon** (`make dagster-home`, then run `dagster-daemon` with `ops/workspace.yaml`).
  Schedules `daily_refresh_schedule` (`daily_cron`) and `weekly_reconcile_schedule`
  (`weekly_cron`). `ops/dagster.yaml` allows one run at a time through `QueuedRunCoordinator`.
- **launchd agents** (`ops/launchd/`). The daily agent runs `make refresh` Monday to Saturday and
  the weekly agent runs `make reconcile` on Sunday.

Both schedulers fire capture on the same days at different times. Enabling both doubles Gamma
traffic and writes a second batch of raw pages for every run. The run lock stops two writers
from racing, but it does not deduplicate work.

**Timezones.** Dagster schedules are cron strings in `config/catalogue.toml` (`[schedule]`) and
run in UTC (`execution_timezone="UTC"`), so `0 6 * * 1-6` means 06:00 UTC. The launchd templates
use the Mac's local time (`StartCalendarInterval` has no timezone), so 06:00 local. Changing
`[schedule]` does not change the launchd templates; edit both if you switch schedulers.

Jobs: `bootstrap`, `daily_refresh`, `weekly_reconcile`, `replay`, `validate`, `publish`.
`validate` runs `dbt test` only. `replay` and the refresh jobs run `dbt build`.
Sensors: `alert_on_run_failure` and `missed_run_alert` (writes `.state/alerts.log` when no
capture has run for 30 hours).

On macOS, `ops/launchd/` has templates. Install either the daily and reconcile agents (launchd
scheduler) or the Dagster daemon agent (Dagster scheduler), never both. Replace each
`__PROJECT_ROOT__`, copy the files to `~/Library/LaunchAgents/`, and load them with
`launchctl bootstrap gui/$(id -u) <file>`.

## Quality limits

Each limit is a `[quality]` setting in `config/catalogue.toml`, overridable with
`CATALOGUE_QUALITY_<KEY>`. Values are validated when settings load: ratios must lie in [0, 1],
percentages in [0, 100], and the warn limit may not exceed the error limit.

| Check | Setting | Default | Above the limit |
| --- | --- | --- | --- |
| Quarantined records per batch | `quarantine_max_ratio` | 1% | The batch is not registered and the load exits 3. `refresh` and `replay` then stop before `dbt build`, so nothing publishes until the batch is resolved. Batches registered in the same load are not built either. |
| Unresolved event references in market refs | `unresolved_reference_max_ratio` | 1% | `dbt build` fails, so nothing is published. |
| Open-event drop vs the previous build, warn | `open_events_drop_warn_pct` | 5% | Logged. The CLI stage row records `open_events_drop_warn`; the Dagster run logs the warning only. The build passes. If the snapshot history cannot be read (another connection holds the file, or the file or snapshot table is unreadable), the skip is written to the Dagster run log (or to the `oddsfox_catalogue.warehouse` logger outside Dagster), and the build still runs the error check. |
| Open-event drop vs the previous build, error | `open_events_drop_error_pct` | 10% | `dbt build` fails, so nothing is published. |

**Recovering a blocked batch.** A quarantine above the cap is a data problem first. Inspect
`quarantine` in the ledger, and raise `quarantine_max_ratio` only if the records are expected.
Then run `make replay`: it registers the blocked batch and builds from it, with no Gamma calls.

The open-event limits are passed to dbt as `max_open_events_drop_pct` and
`max_unresolved_reference_ratio`, so `dbt build` and the Dagster build enforce the same numbers.

## Recovery

- **Crash during capture**: the next capture resumes the same batch. Page IDs and content are
  deterministic, so a resumed page matches what was written before the crash. `scripts/bootstrap-until-done`
  repeats that resume until the refresh exits 0, and it does not start a new crawl after dbt or publish fails.
- **Crash during load**: rerunning the load is a no-op for rows already present. Observation IDs
  are unique and the load is insert-only, so nothing duplicates.
- **Crash during publish**: `current.json` is replaced atomically, so readers keep the previous
  release. The orphaned release directory is removed by retention.
- **Lost warehouse, raw pages intact**: move the ledger aside, run `uv run catalogue ledger rebuild`
  (it reconstructs batches and pages from raw manifests), then `make replay`.
- **Lost raw data**: restore the newest backup, run `make verify-backup BACKUP=...`, then `make rebuild`
  to prove the restored warehouse matches its raw pages.

## Layout

- `src/oddsfox_catalogue/`: capture, load, dbt runner, publish, rebuild, backup, Dagster definitions, CLI.
- `dbt/`: models, macros, and singular tests. `dbt/profiles.yml` reads the warehouse path from `CATALOGUE_WAREHOUSE`.
- `config/catalogue.toml`: defaults. Override any key with `CATALOGUE_<SECTION>_<KEY>`.
- `tests/`: unit, integration, and fakes. `tests/fixtures/gamma/synthetic` holds sanitized payloads.
- `ops/`: Dagster instance and workspace config, launchd templates.
- `data/` and `.state/` are local and never committed.

## Gamma contract

- Daily capture lists open events with `after_cursor`. Bootstrap and reconcile first list open
  markets with the keyset, then list events and closed markets by explicit id windows of 100, from
  a high-water mark read once at batch start. Closed-market windows wait until the open crawl
  completes, so a market that closes mid-batch is captured by one of them. By-ID lookups fill in
  events referenced from markets that those scans did not return. A window that fails, for any
  reason, is split in half until single ids remain. A one-id window that still fails is answered by
  ID instead. A single id that still fails is quarantined as `fetch_failed` and is not requested again. The same
  applies to a by-ID lookup, and a 200 that describes a different record counts as a failure. A 404
  is recorded as absent, not failed. A market answered by ID requests `include_tag`, as a market
  window does.
- Gamma's list and id-window endpoints return only active events. Inactive events are reached only
  by ID, and only when a captured market references them. Known gap: an inactive event that no
  captured market references is not captured. Event 5364 is one example. In a sample of ids 1 to
  6000, 8 of the 98 ids that the windows did not return exist, and all 8 are inactive. Closing the
  gap means a by-ID pass over about 450k ids, roughly 25 hours at 5 requests per second. The
  `archived` filter is ignored by Gamma, and no archived events were observed in ids 1 to 6000.
- Nested `market.events` entries are references only. They are never treated as full events.
- `outcomes`, `outcomePrices`, and `clobTokenIds`/`positionIds` arrive as JSON-encoded strings.
  dbt decodes them and quarantines any market whose lists do not align.
- Up to 4 scans in the current plan stage run at once (`capture.workers`). They share a limit of
  5 requests per second (`gamma.requests_per_second`), with a 10 s connect timeout and a 60 s read
  timeout. Keyset requests retry up to 12 times (`backoff_base_s` 5, `backoff_cap_s` 300). An id
  window and a by-ID lookup retry 4 times with backoff capped at 30 s. Every request honours
  `Retry-After` up to 15 min (`RETRY_AFTER_CEILING_S`); a longer value is cut to 15 min.

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
global `current.json`. Set `CATALOGUE_ROOT` to the catalogue checkout when invoking
the installed executable from elsewhere. The normal catalogue writer lock applies.

The bundle contains JSON-array relations `markets.json`, `outcomes.json`,
`memberships.json`, `identity_history.json`, and `coverage.json`. Its `manifest.json`
declares the contract, targeted coverage, content-derived `source_release_id`,
receipt time and each literal filename's SHA-256 and byte size. Decimal values are
strings. Consumers pin the manifest digest and copy the bundle before acquisition;
the producer's path is not a durable downstream dependency.

Identity projection does not require prices. Outcome ordinals remain 1-based;
`chain_index_set` remains null until a chain collector establishes it independently.
CTF token IDs and Protocol V2 position IDs are separate fields. Explicit `v2`/`2`
metadata selects `positionIds`; missing/v1 metadata selects CTF IDs only when no
positions are present. Unknown versions, duplicate/misaligned IDs and conflicting
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

Run from this repository after `uv sync --locked`. This is opt-in; ordinary tests and CI stay offline.

```sh
uv run --locked python -m oddsfox_catalogue.smoke --market-id 5234660
```

Every invocation creates a fresh ignored `data/smoke/<uuid>` root and prints the absolute `report.json` path. Use `--output <fresh-path>` to name a run. Never reuse an output directory. Reports retain the steps, measured requests/bytes, configured limits and coverage. Failed/timed-out commands make `accounting_complete` false; their totals are known lower bounds. No paid requests are made. Run the events smoke first and pass its report to the consumers; consumers validate and pin its metadata bundle. Select an explicit currently active, unambiguous market when the example market closes.

A pass requires one usable market, verified bundle checksums, an identical offline metadata replay (coverage observation timestamps excluded), zero replay HTTP requests and no global catalogue pointer. Existing `CATALOGUE_*` settings are cleared to isolate the run. This checks the targeted handoff, not a full catalogue crawl or warehouse build.
