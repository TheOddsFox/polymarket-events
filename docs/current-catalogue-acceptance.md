# Current catalogue acceptance evidence

Acceptance remains open. Do not activate schedules or cut consumers over on the
strength of the small fixture tests alone.

## Verified import

The selected legacy release is `20261013T191053Z`, manifest SHA-256
`fcc2760fcb9e1808e923c5df7a5ad6fcd6fbc6e89064ca0a74d3aaac5fc9621f`.
Its Parquets omit 254 markets referenced by its complete event evidence. The
approved import supplements those markets and 508 outcomes from verified event
records, retaining original receipt ages and discovery frontiers.

An independent, exact comparison passed for both the current store and its first
publication: 1,138,596 events, 4,321,358 markets and 8,639,383 outcomes. All
5,459,954 event/market receipts also matched. Bidirectional `EXCEPT ALL` covered
every retained field, nulls, unexpected IDs and duplicate multiplicities. The
comparison rechecked source hashes, the publication pointer and database identity.
Latest-complete memberships have a separate raw-evidence oracle;
`scripts/verify-current-import.py` runs that comparison and reports the intentional
difference from the historical bridge union.
That comparison also passed: 12,079 verified event pages produced exactly
4,321,342 current links, with zero differences. The historical bridge differs by
254 links. Legacy file identities and the current pointer remained unchanged.

## Performance status

Measurements use an Apple M4 Mac with 32 GiB RAM, 2 GB DuckDB memory and two SQL
threads. OS caches were not flushed. A fresh process is not a cold-cache run.
The catalogue SSD negotiated a 480 Mb/s USB connection. File-read profiles reached
about 30 MB/s. The operator chose to retain that connection for acceptance rather
than change hardware to obtain a passing result.

The first full-scale publication and verification took 915.49 seconds. A later
bound current-release verification diagnostic passed in 448.53 seconds, compared
with 805.66 seconds using the earlier installed implementation. These diagnostics
are not equivalent complete refresh runs or an accepted speedup claim.
**The five-minute refresh target remains unmet.** Three full-store normal-delta
runs, unchanged refreshes and sustained six-hour/monthly reconciliation capacity
must still pass before deployment acceptance.

The controlled benchmark is `scripts/benchmark-current.py`; its commands and
budgets are documented in [the operator guide](current-catalogue.md). Identity
repair preparation uses the production publication timer outside the measured
refresh. It records both the copied and resulting publication measurements.
Preparation has a cooperative four-hour publication deadline; normal refresh and
HTTP budgets remain 300 seconds. No measurement is reset or clamped to make work
fit the deadline.

## Measured candidate decisions

| Candidate | Evidence and disposition |
| --- | --- |
| Compact fingerprint hashes | Retain 32-byte binary intermediate hashes. Three full-outcome pairs preserved existing digests, reduced engine peak memory from about 2.46 GB to 1.36 GB and removed 175–182 MB of measured spill. Median elapsed time was effectively unchanged: 24.33 versus 24.49 seconds. |
| Three key-only identity guards | Reject the tested replacement. Three alternating pairs had medians of 0.638 seconds for the existing query and 0.775 seconds for the replacement; one slow initial read does not establish a repeatable workflow benefit. |
| Physical-only verification of older releases | Reject. Recomputed file and manifest hashes can conceal invalid relationships; retain actual semantic verification. |
| Packaging directory pruning | Retain. An installed-fixture workflow fell from 249.39 to 18.47 seconds after Hatch stopped traversing excluded raw evidence. Wheel bytes and archive member inventories were identical; only the sdist build configuration changed. This is not a full-refresh measurement. |
| Compressed backups | Use the explicitly versioned backup.v4 gzip database representation, with expanded and stored hashes, streaming admission and fresh reopened verification. Full-scale backup, restore and seven-backup capacity still require measured acceptance. |

Recovery must remain offline and preserve applicable catalogue content and pending
progress. Local verified backups do not establish the 24-hour disk-loss target
until an independently stored verified copy exists. Keep legacy roots and their
matching runtime intact throughout acceptance and rollback.
