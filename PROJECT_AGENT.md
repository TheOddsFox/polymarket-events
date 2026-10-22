# Project agent notes

Public local, single-host catalogue of unique Polymarket events, markets, event-owned memberships and exact native outcome identifiers. Odds, financial metrics and observation history belong outside this runtime.

Use AGENTS.md and the local pad-engineering skill. Git owns code. Local verify scripts own verification. Pad owns scope and evidence. Keep Work/Plans and all operator data outside Git. GitHub Actions were intentionally removed; do not restore hosted workflows unless explicitly requested.

## Runtime

Python3.12, DuckDB, HTTPX and filelock. Version 1.0.0 opens only an already-initialized catalogue_store.v4 root; it never imports, migrates, or mutates legacy data. metadata.v1 stays compatible. Roots must never reuse or nest beneath another root. No dlt/dbt/Dagster runtime or scheduler activation.

Complete event membership arrays replace links, including empty arrays. Missing/incomplete arrays preserve prior links and remain unresolved. List omissions never establish absence or source completeness. Retain closed/unlinked entities. Native IDs are exact decimal strings; unsafe/conflicting ownership is unavailable until native confirmation.

Response application and progress commit atomically. Verified immutable Parquet releases use an atomic current pointer; current and previous releases survive failures. Raw responses are retained by reference: after each publication, raw not referenced by the store, pending durable work, or sync pins is deleted. Default ceilings: 1200s normal refresh including publication, 10 requests/s, 1 HTTP worker, 25k requests, 4GiB download, 16MiB response, 2GB SQL memory, 2 threads, 160GiB retained, 16GiB temporary, and 8GiB free reserve. A root config may raise retained, temporary, and reserve up to the hard maximums in code. Ceilings are set from measured peaks; do not weaken correctness checks to pass. Active membership freshness targets 24 hours and cold rotation 60 days; due thresholds are 20 hours and 58 days. A forced `reconcile --full-pass` bypasses those gates until every known event and market has a newer receipt. During a forced pass, a list omission is settled only by a native lookup: a 404 records a confirmed miss as a receipt, a 200 applies the record, and any other failure leaves the entity unreceipted.

## Verification

Development: scripts/verify-fast. Completion: scripts/verify. Both are Python scripts. macOS on the MacBook is the supported execution target; native Windows support is retired. Both run diff/Ruff/pytest; pytest uses every available core (`-n auto`). Completion checks/syncs the lockfile. Pass `-n0` to run one test in-process. Distribution audit and the installed-workflow proof are the local commands in the guide. Tests must never contact live Gamma. CATALOGUE_GAMMA_BASE_URL=http://127.0.0.1:9 and CATALOGUE_GAMMA_ALLOW_LOOPBACK=1 are the default offline test environment.

Test current projections, atomic progress, membership ordering, exact native identity, corruption detection, publication, raw retention by reference, resource boundaries, process deadlines, forced full-pass resumption, metadata.v1 isolation, and offline refresh/reconcile on a minimal initialized root. Performance and freshness are acceptance gates, never assumptions.

## Git and privacy

Commit, push and open a PR only when requested in the current turn. Use codex/ branches, never force-push or push main without explicit instruction. Keep Pad records, credentials, raw data, catalogue roots and private trading work out of this public repository. Change uv.lock with dependency changes and run uv lock --check.
