# Project agent notes

Public local, single-host catalogue of unique Polymarket events, markets, event-owned memberships and exact native outcome identifiers. Odds, financial metrics and observation history belong outside this runtime.

Use AGENTS.md and the local pad-engineering skill. Git owns code, CI verification, and Pad scope/evidence. Keep Work/Plans and all operator data outside Git.

## Runtime

Python3.12, DuckDB, HTTPX and filelock. catalogue.v3 is a breaking replacement for catalogue.v2; metadata.v1 stays compatible. Current roots have a catalogue_store.v4 stamp and must never reuse, nest beneath, migrate or mutate legacy roots. Preserve a matching baseline runtime for rollback. No dlt/dbt/Dagster runtime or scheduler activation.

Complete event membership arrays replace links, including empty arrays. Missing/incomplete arrays preserve prior links and remain unresolved. List omissions never establish absence or source completeness. Retain closed/unlinked entities. Native IDs are exact decimal strings; unsafe/conflicting ownership is unavailable until native confirmation.

Response application and progress commit atomically. Verified immutable Parquet releases use an atomic current pointer; current and previous releases survive failures. Bounded evidence and verified daily backups replace permanent raw replay. Default ceilings: 1200s normal refresh including publication,10requests/s,1HTTPworker,25krequests,4GiBdownload,16MiBresponse,2GBSQLmemory,2threads,96GiBretained,8GiBtemporary,32GiBfree reserve. The approved 2026-10-15 policy keeps the combined legacy/new-root envelope at 256 GiB. Do not raise limits to pass. Active membership freshness targets 24 hours and cold rotation 60 days; due thresholds are 20 hours and 58 days. Recovery is verified local-only daily backups; disk-failure protection is explicitly waived.

## Verification

Development: scripts/verify-fast. Completion: scripts/verify. Both run diff/Ruff/pytest; completion checks/syncs the lockfile. CI additionally audits distributions and runs scripts/verify-installed-workflow.py with real loopback fixtures and an isolated installed wheel. Tests must never contact live Gamma. CATALOGUE_GAMMA_BASE_URL=http://127.0.0.1:9 and CATALOGUE_GAMMA_ALLOW_LOOPBACK=1 are the default offline test environment.

Test current projections, atomic progress, membership ordering, exact native identity, corruption detection, publication, resource boundaries, process deadlines, verified backup and fresh-root offline recovery. Retained legacy raw/metadata parsing tests remain applicable; obsolete historical stage tests retire with their APIs. Performance and freshness are acceptance gates, never assumptions.

## Git and privacy

Commit, push and open a PR only when requested in the current turn. Use codex/ branches, never force-push or push main without explicit instruction. Keep Pad records, credentials, raw data, warehouse files, backups and private trading work out of this public repository. Change uv.lock with dependency changes and run uv lock --check.
