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
```

## Operator commands

| Command | What it does |
| --- | --- |
| `make bootstrap` | First run on an empty project: capture everything, load, build, publish. |
| `make refresh` | Daily incremental run: open events plus recently changed events. |
| `make reconcile` | Weekly full pass: archived events and by-ID checks for open events. |
| `make replay` | Load pending raw pages, build, publish. No Gamma calls. |
| `make validate` | Run dbt tests against the warehouse. |
| `make publish` | Write a release if the last dbt build passed. |
| `make current` | Show the current published release pointer. |
| `make status` | List capture batches and their states. |
| `make backup` | Write a checksummed backup to `data/backups/<stamp>`. |
| `make verify-backup BACKUP=...` | Re-hash a backup and list any problems. |
| `make rebuild` | Rebuild from raw in a scratch area and compare table fingerprints with the live warehouse. Exit 4 on mismatch. |

Each stage takes the run lock. A second writer fails immediately (exit 3) instead of racing.
Every stage writes a row to `stage_runs` in the ledger (status, counts, error).

Lower-level commands: `uv run catalogue --help`.

## Scheduling

Dagster defines the schedules, sensors, and jobs (`make dagster-home` installs
`ops/dagster.yaml`, which allows one run at a time through `QueuedRunCoordinator`).
Jobs: `bootstrap`, `daily_refresh`, `weekly_reconcile`, `replay`, `validate`, `publish`.
Sensors: `alert_on_run_failure` and `missed_run_alert` (writes `.state/alerts.log` when no
capture has run for 30 hours).

On macOS, `ops/launchd/` has templates for the daily and weekly runs and for the Dagster
daemon. Replace each `__PROJECT_ROOT__`, copy the files to `~/Library/LaunchAgents/`, and load
them with `launchctl bootstrap gui/$(id -u) <file>`.

## Recovery

- **Crash during capture**: the next capture resumes the same batch. Page IDs and content are
  deterministic, so a resumed page matches what was written before the crash.
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

- Open events are paginated with `after_cursor` (keyset). Archived events use offset, since that
  is the only supported pagination for them. By-ID lookups fill in events referenced from markets.
- Nested `market.events` entries are references only. They are never treated as full events.
- `outcomes`, `outcomePrices`, and `clobTokenIds`/`positionIds` arrive as JSON-encoded strings.
  dbt decodes them and quarantines any market whose lists do not align.
- Requests are limited to 2 per second, with 10 s connect and 60 s read timeouts, and up to 5
  retries using full-jitter backoff and `Retry-After`.

## Status

The live Gamma API was not reachable from the development machine, so every test runs against
synthetic fixtures. Run `tools/record_gamma_fixtures.py` from a machine with access to refresh them.
