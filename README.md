# OddsFox Polymarket catalogue

A local, single-host catalogue of unique current Polymarket events, markets, their links,
and native outcome asset identifiers. Odds, trades, order books, price history, and
historical analytics belong elsewhere.

The public contract is **`oddsfox.polymarket.catalogue.v3`**: four Parquet relations
(`events`, `markets`, `event_markets`, `outcomes`) and a verified publication pointer.
This breaks the old catalogue.v2 contract. The current path uses a small DuckDB store;
it retires the history/metrics, dlt, dbt, and Dagster pipeline. Preserve old roots and
their matching runtime for rollback; there is no in-place migration.

See [the current catalogue contract and operator guide](docs/current-catalogue.md)
for schemas, import, provider checks, recovery, limits, and scheduling.

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --locked
uv run catalogue --help
uv run catalogue --root /srv/catalogue-v3 init
uv run catalogue --root /srv/catalogue-v3 config show
```

Use an external operator root. Global `--root` comes **before** the command.
Initialization stamps a fresh root and rejects nonempty unstamped or legacy roots.
Before refresh or reconcile, run the guide's provider probe with independently known
open, closed, and absent event and market IDs. An unsupported lane blocks use; it is
not permission to fall back to an unproven query.

```sh
uv run catalogue --root /srv/catalogue-v3 refresh
uv run catalogue --root /srv/catalogue-v3 reconcile
uv run catalogue --root /srv/catalogue-v3 current
uv run catalogue --root /srv/catalogue-v3 status
uv run catalogue --root /srv/catalogue-v3 verify
```

**Deployment acceptance remains open:** the approved operating targets are a
20-minute refresh and verification, a 96 GiB retained root inside the combined
256 GiB envelope, active membership freshness within 24 hours and cold rotation
within 60 days. See the [acceptance evidence](docs/current-catalogue-acceptance.md)
for historical measurements and current validation. Recovery uses daily verified
local backups; disk-failure protection is explicitly waived. Freshness remains
conditional on provider behavior, backlog, invocation cadence and measured capacity.
Observed coverage is never a claim of source completeness.

The isolated `metadata refresh` and `metadata export` CLI and metadata.v1 consumer
contract remain available; see the guide. Refresh requires an isolated producer root;
offline export can read legacy raw evidence. Both commands reject v3 roots.

Development checks (no live Gamma calls):

```sh
scripts/verify-fast
scripts/verify
uv build
uv run python scripts/verify-distribution.py dist/*.whl dist/*.tar.gz
uv run python scripts/benchmark-current.py --help
```

Use the installed-distribution workflow and benchmark instructions in the guide before
making performance or deployment claims. [The cron example](ops/catalogue.cron.example)
is disabled template text only; installation does not enable scheduling.
