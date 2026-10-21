# Changelog

## Unreleased

### Added

- Version-5 synthetic comparisons for receipt metadata factoring, consolidated state
  reads and typed lookup keys. Direct warm-up attribution separates SQL execution
  from result materialization; scored workers preserve the existing performance,
  correctness, HTTP and maintenance gates. Production retention requires complete
  comparison and final-integrity evidence.
- A version-3 synthetic campaign for selective claims, full-pass queue handling,
  certified market-ID retrieval and dense/sparse keyset discovery. Independent
  workload oracles, exact supplied combinations and one shared three-hour budget
  preserve the earlier manifest workflows and measurement-version-2 guards.
  Index installation remains deferred without a safe admission model. Full runs and
  performance qualification move to the Windows workstation; no speedup is claimed.
- Version-4 synthetic campaign support for independently comparing outcome grouping
  and reverse claim lookup, an explicitly supplied combined package, and diagnostic-only
  claim-index measurements. Both complete Mac comparisons failed qualification;
  neither production optimization is retained.
- Selective unindexed claim lookups, linear full-pass queue handling, and explicitly
  certified market-ID/keyset acquisition with version-2 replay compatibility.
- Native Windows process containment, RSS accounting, reparse-safe storage and
  fail-closed NTFS volume durability. Real Windows validation remains pending.
  Existing verification wrappers are runnable with Python on either platform.

- Versioned full-pass phase measurements and independent outcome/queue campaign
  comparisons, preserving legacy manifests and whole-worker performance guards.
  The bounded characterization retained neither candidate: no scored pairs fit.

- A synthetic optimization campaign with frozen source inventories, private-state
  comparison, strict performance gates and one shared thirty-minute budget.

- Synthetic-only full-pass characterization with frozen baseline comparisons and a
  shared ten-minute performance budget. Incomplete evidence cannot qualify a change.
  Verified publication comparisons use semantic row fingerprints independently of Parquet layout.

### Fixed

- Version-4 campaigns preserve invalid-comparison failures, distinguish complete
  deferrals from incomplete evidence, and tolerate unavailable Windows filesystem
  metadata without changing measured performance gates.

- Periodic keyset reconciliation performs the existing bounded native identity
  repairs after fresh pages and durable replay, including nested event markets.

- Failed optional capability reprobes preserve working certificates; contradictory
  lifecycle proofs fence acquisition until a successful reprobe. Certificate,
  diagnostic, fence and keyset-state changes commit atomically.

- Oversized native asset IDs are quarantined as invalid identities without aborting a response.
- Prefetch handoff keeps completed responses available without a duplicate GET or
  unnecessary request-budget exhaustion.
- Full-pass completion keeps the measured publication reserve even when cadence
  initially defers publication.
- Malformed Gamma configuration sections return the handled configuration error.
- Capped retry backoff remains bounded for large accepted retry counts.
- Interrupted optimization campaigns retain a qualified fallback only after final
  integrity verification within the remaining measurement budget.
- Full-pass scheduling preserves the complete accepted 20-digit entity ID range and
  retries stale events below an exhausted saved cursor on the next invocation.
- Installed-workflow and capacity tooling reap exited process leaders before signalling
  their groups, while retaining descendant cleanup and genuine permission failures.

### Changed

- Post-commit publication maintenance cooperatively defers work near its deadline,
  preserving completed deletion statistics and warnings for later resumption.
  Mandatory admission cleanup and referenced-raw accounting remain fail-closed.

- A forced full pass walks stale events in numeric id order, then markets those events
  did not renew. It skips historical id-range windows. The next list request is fetched
  while the current response is applied. Responses are applied as one set of writes.
- `reconcile --full-pass` publishes when the pass completes or when
  `full_pass_publish_interval_s` (default 6 hours) has elapsed since the last publication.
  Other invocations still publish every time. A pass invocation that does not publish
  exits 3 with `published: false`; every pass invocation keeps the measured publication
  reserve in case it completes.
- An unchanged publication checks the pointer, the manifest hash, the file inventory, and
  each Parquet size and SHA-256. It still rejects a live store that fails its own checks,
  and it does not recompute the Parquet semantic fingerprint. `verify` and `current` stay
  full semantic checks.
- Retained-byte admission walks the root at connect, before publication, and at exit, and
  adds raw bytes as they are written.
- `[catalogue] duckdb_memory_bytes` defaults to 2 GiB (hard maximum 16 GiB) and
  `duckdb_threads` defaults to 2 (maximum 8). A config without the new keys still loads.

## 1.0.0

The first release ships the current Polymarket catalogue as a manual, single-host tool.

### Added

- `reconcile --full-pass` starts or resumes a forced full pass over every known event and
  market. Each invocation stops at its 1200-second deadline. `status`, invocation results
  and the publication manifest report the `full_pass` object, including `completed_at`.
- Raw responses are retained by reference. After each publication, responses that the
  store, pending durable work or sync pins do not reference are deleted. Manifests record
  the referenced raw count and bytes.
- A compatibility integration test initializes a minimal root and runs refresh and
  `reconcile --full-pass` against the fake Gamma server.

### Changed

- `metadata refresh` reads `CATALOGUE_ROOT` when `--root` is omitted, and writes an empty
  `identity_history` when `--catalogue-root` is omitted, so existing producers keep working.

- Storage defaults are 160 GiB retained, 16 GiB temporary and 8 GiB free reserve. Root
  configuration may raise retained, temporary and reserve values up to hard maximums of
  512 GiB, 64 GiB and 256 GiB. Each root is admitted against its own ceilings.
- metadata.v1 identity history is read from the catalogue store's identity tables under
  the catalogue lock, on a read-only connection. Metadata refresh no longer depends on
  legacy settings.
- Benchmarks admit each root against its own ceiling instead of a combined envelope.
- During a forced full pass, a bounded list that omits a known event or market is settled
  by one native lookup. A 404 is a confirmed miss and counts toward the pass; a 200 applies
  the record. Other failures are never counted.
- A 0.1.0 `config.toml` opens unchanged. The `[load]` section and the `max_raw_bytes` and
  `raw_max_age_s` keys are ignored; other unknown keys are still rejected.

### Removed

- Backup, restore and backup verification commands.
- Legacy release import (`import-release`) and the legacy capture reader and scans.
- metadata export and the offline legacy export path.
- The disabled scheduling template and the v2 catalogue document.
- The seed-pending and seeding guards. Version 1 opens an already-initialized
  `catalogue_store.v4` root without migration.
- Configuration keys for the removed raw-age and raw-size prune, and the `[load]` section,
  as settings. Existing configs that still contain them are read, not refused.
