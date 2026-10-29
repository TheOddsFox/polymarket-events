# OddsFox Polymarket catalogue

A local, single-host catalogue of unique current Polymarket events, markets, their
memberships, and native outcome asset identifiers. Odds, trades, order books, price
history, and historical analytics belong elsewhere.

The public contract is **`oddsfox.polymarket.catalogue.v3`**: four Parquet relations
(`events`, `markets`, `event_markets`, `outcomes`) and a verified publication pointer.
Version 1.0.0 is a manual tool. It runs one invocation at a time, has no scheduler, and
opens only a root initialized with `init`. It has no import, backup, restore, or metadata
export command.

## Authoritative production database on this MacBook

Accepted on 29 October 2026, the sole production root is
`/Volumes/Mac SSD/oddsfox/polymarket/polymarket-events/data/catalogue-production-duckdb2`. Keep the complete
root, including `config.toml`, raw recovery files, releases and the publication pointer.
The previous `/Volumes/Mac SSD/oddsfox/polymarket/polymarket-events/data/catalogue-production`
root is retained as a historical runnable asset. It is not a rollback point after the
new root receives writes.

Production uses the permanent DuckDB 2.0 runtime at
`/Volumes/Mac SSD/oddsfox/polymarket/polymarket-events/data/catalogue-runtime/duckdb-2.0.0-local-data-6b78d876fa93/bin/catalogue`.
Use an isolated environment and pass the global `--root` before each command:

```sh
CATALOGUE_B='/Volumes/Mac SSD/oddsfox/polymarket/polymarket-events/data/catalogue-runtime/duckdb-2.0.0-local-data-6b78d876fa93/bin/catalogue'
CATALOGUE_ROOT='/Volumes/Mac SSD/oddsfox/polymarket/polymarket-events/data/catalogue-production-duckdb2'
catalogue_prod() {
  /usr/bin/env -i HOME="$HOME" PATH=/usr/bin:/bin:/usr/sbin:/sbin \
    PYTHONDONTWRITEBYTECODE=1 PYTHONNOUSERSITE=1 \
    "$CATALOGUE_B" --root "$CATALOGUE_ROOT" "$@"
}

catalogue_prod status
catalogue_prod verify
# Run only when operationally needed:
catalogue_prod refresh
catalogue_prod reconcile
```

Do not initialize, replace or reset the production root. The old DuckDB 1.5.6 runtime at
`/Volumes/Mac SSD/oddsfox/polymarket/polymarket-events/data/catalogue-runtime/duckdb-1.5.6-accepted-63b1a94/bin/catalogue`
is reserved for verifying a fresh rollback copy of the complete updated root. Stop all
writers, preserve or clone the updated root (including pending work and raw/releases),
settle pending work under DuckDB 2.0 if needed, and verify that fresh copy with the old
runtime before changing the production designation. Never restore the stale old
production root after new writes.

The checkout's `data/` holds local production and rollback roots, runtimes, and
evidence/history. It is Git-ignored and excluded from distributions. Historical evidence
records the former sibling paths; map them with
`data/relocation-20261029/relocation-result.json`. Keep snapshots and evidence ignored.

Repository-operated commands confine catalogue roots, metadata bundles, and maintenance
outputs to `data/`, rejecting external paths before writes. The default root is
`data/catalogue-v3` anchored to this checkout, even from another working directory;
production continues to require the explicit root above. Catalogue inputs such as
benchmark seeds and metadata identity roots must also be local. `data/catalogue-runtime` is
reserved for runtime artifacts. Standalone packages installed outside a catalogue
checkout retain their explicit-root deployment behavior.

The service remains manually operated; no scheduler, automatic full pass, or sustained
freshness claim is enabled. See the operator guide and acceptance record for the
qualification scope and measured limitations.

For `polymarket-market-data`, `--events-executable` selects the catalogue runtime;
point it at `CATALOGUE_B`. Its own `--root` remains the market-data store and must
also be under this checkout’s `data/`, because that consumer creates managed metadata
producer and bundle directories beneath its store. Example:

```sh
polymarket-market-data --root "/Volumes/Mac SSD/oddsfox/polymarket/polymarket-events/data/market-data" run \
  --market-id 667082 --feed clob-history \
  --start 2026-10-28T10:00:00Z --end 2026-10-28T11:00:00Z --duration 60 \
  --events-executable "$CATALOGUE_B"
```

A consumer stored elsewhere needs a standalone catalogue installation outside this
checkout; the guarded local runtime rejects its external metadata paths. Existing
other-project stores are not moved by this policy change.

Other readers should use the new production root only where they already expose a
catalogue-root option. Polygon settlements continues to consume a verified metadata
bundle through its existing `--metadata` input. Metadata.v1 production remains staged
under its isolated producer root; do not point that producer at the production root.

## Usage and verification

See [the operator guide](docs/current-catalogue.md) for the contract, probe procedure,
full-pass semantics, raw retention, limits, and metadata.v1 handoff.
The [acceptance page](docs/current-catalogue-acceptance.md) records the versioned
acceptance criteria and historical measurements; the local production handoff records
the accepted operational state of this MacBook's database.

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

The lock selects qualified DuckDB 2.0.0. Development uses the checkout's `.venv`;
production uses the permanent executable above. The existing storage format and
public contract are unchanged. See the [qualification notes](docs/current-catalogue.md#duckdb-engine-qualification)
for the accepted timing tradeoff and recovery proof.

```sh
uv sync --locked
uv run catalogue --help
uv run catalogue --root ./data/catalogue-v3 init
uv run catalogue --root ./data/catalogue-v3 config show
```

Use a root under this checkout’s ignored `data/`. Global `--root` comes **before** the command. Initialization
stamps a fresh root and rejects nonempty unstamped roots. Before refresh or reconcile,
run the operator guide's provider probe with independently known open, closed, and absent
event and market IDs. An unsupported lane blocks use.

```sh
uv run catalogue --root ./data/catalogue-v3 probe ...
uv run catalogue --root ./data/catalogue-v3 refresh
uv run catalogue --root ./data/catalogue-v3 reconcile
uv run catalogue --root ./data/catalogue-v3 reconcile --full-pass
uv run catalogue --root ./data/catalogue-v3 status
uv run catalogue --root ./data/catalogue-v3 verify
```

Each invocation is bounded by 1200 seconds, including publication. `reconcile --full-pass`
resumes across invocations until every known event and market has a newer receipt; then
`status` reports `full_pass.completed_at`. Observed coverage is never a claim of source
completeness.

Run commands by hand. The repository ships no scheduler or schedule template.

Development checks (no live Gamma calls):

```sh
scripts/verify-fast
scripts/verify
uv build
uv run python scripts/verify-distribution.py dist/*.whl dist/*.tar.gz
uv run python scripts/benchmark-current.py --help
```

macOS on the MacBook is the supported execution target. Native Windows support has
been removed.

Use the installed-distribution workflow and benchmark instructions in the guide before
making performance or deployment claims.
