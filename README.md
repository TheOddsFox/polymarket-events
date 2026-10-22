# OddsFox Polymarket catalogue

A local, single-host catalogue of unique current Polymarket events, markets, their
memberships, and native outcome asset identifiers. Odds, trades, order books, price
history, and historical analytics belong elsewhere.

The public contract is **`oddsfox.polymarket.catalogue.v3`**: four Parquet relations
(`events`, `markets`, `event_markets`, `outcomes`) and a verified publication pointer.
Version 1.0.0 is a manual tool. It runs one invocation at a time, has no scheduler, and
opens only a root initialized with `init`. It has no import, backup, restore, or metadata
export command.

See [the operator guide](docs/current-catalogue.md) for the contract, probe procedure,
full-pass semantics, raw retention, limits, and metadata.v1 handoff.
The [acceptance page](docs/current-catalogue-acceptance.md) records the v1 bar; no
acceptance measurement is recorded yet.

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```sh
uv sync --locked
uv run catalogue --help
uv run catalogue --root /srv/catalogue-v3 init
uv run catalogue --root /srv/catalogue-v3 config show
```

Use an external root. Global `--root` comes **before** the command. Initialization
stamps a fresh root and rejects nonempty unstamped roots. Before refresh or reconcile,
run the operator guide's provider probe with independently known open, closed, and absent
event and market IDs. An unsupported lane blocks use.

```sh
uv run catalogue --root /srv/catalogue-v3 probe ...
uv run catalogue --root /srv/catalogue-v3 refresh
uv run catalogue --root /srv/catalogue-v3 reconcile
uv run catalogue --root /srv/catalogue-v3 reconcile --full-pass
uv run catalogue --root /srv/catalogue-v3 status
uv run catalogue --root /srv/catalogue-v3 verify
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
