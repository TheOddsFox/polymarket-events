# Changelog

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
