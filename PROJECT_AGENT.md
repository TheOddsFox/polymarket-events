# Project agent notes

Public OddsFox repository for a local, single-host batch pipeline that catalogues Polymarket events and their markets (metadata and outcome definitions). Trades, order books, and price history are out of scope.

Stack: Python 3.12, uv, dlt, dbt, DuckDB, and Dagster. Use the `oddsfox-polymarket-events-engineering` workspace from `.pad.toml`. Follow `AGENTS.md`, `README.md`, and the local `pad-engineering` skill.

## Pipeline

Gamma API capture writes immutable JSON.gz pages and manifests under `data/raw`. Load writes them into bronze with dlt. dbt builds staging, core current, history (SCD2), and marts. Publish writes `data/published/releases/<id>/*.parquet` and `current.json`.

Operator targets: `make bootstrap` (first run), `make refresh` (daily), `make reconcile` (weekly), `make replay` (no Gamma calls), `make validate`, `make publish`, `make current`, `make status`, `make backup`, `make verify-backup BACKUP=...`, and `make rebuild`.

## Verification

- Fast: `git diff --check`, `uv run ruff check .`, `uv run ruff format --check .`, and `uv run pytest -q -n auto --dist loadscope`
- Completion: both diff checks, `uv lock --check`, `uv sync --locked`, the same Ruff checks, `uv run catalogue dbt -- parse`, and `uv run pytest -q` (one process; CI matches this)

These commands are wrapped by `scripts/verify-fast` and `scripts/verify`. Hosted CI runs the same lint, dbt parse, and test steps and never contacts Pad.

## Tests

New capture, load, dbt, publish, or ops behavior gets a test under `tests/`. Use `tests/unit` for pure logic and `tests/integration` for stage behavior. Use the sanitized payloads in `tests/fixtures/gamma/synthetic` and the fakes in `tests/fakes/`. Cover the happy path and the key failure path, such as a quarantined market, a dropped open event, or an unresolved reference. Tests must never call the live Gamma API. Set `CATALOGUE_GAMMA_BASE_URL=http://127.0.0.1:9`, as CI does.

## Git

Commit, push, or open a pull request only when the user asks in that turn. Commit subjects are imperative sentences in the style of existing history, such as `Add ...` or `Remove ...`, with no `feat:` or `fix:` prefix. Never force-push, and never push `main` unless the user explicitly asks. The remote is `git@github.com:TheOddsFox/polymarket-events.git`. Change `uv.lock` only together with a dependency change, and confirm `uv lock --check` passes.

## Invariants

Public repository. Never commit `data/`, `.state/`, `dbt/.user.yml`, or any Pad item body, comment, bootstrap JSON, credential, or backup. Keep private OddsFox trading work out of this workspace and this repository.
