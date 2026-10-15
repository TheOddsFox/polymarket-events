# Catalogue v2

Historical contract for verified import and rollback with the preserved legacy
runtime. The current executable uses [catalogue.v3](current-catalogue.md); the
warehouse commands below require that matching legacy runtime.

`oddsfox.polymarket.catalogue.v2` replaces the legacy warehouse and public projections.
Use a fresh operator root. Retain old evidence and state separately; neither migration nor
compatibility views are provided. The independent `oddsfox.polymarket.metadata.v1` JSON bundle
shape remains supported, using the same normalization rules.

The internal warehouse layout is `oddsfox.polymarket.warehouse.v4`. Bronze event and
market observations retain the full canonical source hash, normalized fields and
provenance; the complete source JSON remains in checksummed raw captures. Quarantine
still retains its decoded payload. Normalized fields are stored separately as canonical JSON-valued text so DuckDB can
compress repeated values independently. The logical bronze observation views reconstruct
the original JSON, preserving nulls, nested values and exact financial strings. Frozen field
sets reject drift; partial loads repair absent views before checkpoints and reject altered
views. No source observations are dropped. This also avoids storing a second full source
copy for every valid observation. Load and dbt reject the previous warehouse
stamp before changing its warehouse or build certification. Use a fresh root and
retain the previous root and its matching installation; no state migration is provided.
The public catalogue.v2 and metadata.v1 schemas remain unchanged, and immutable release
verification remains independent of the warehouse layout.

Empty resources materialize missing typed tables. Subsequent chunks can skip an
empty dlt run only with committed matching native schema state, an exact physical
schema and the required validated observation view, with no extracted or normalized
packages pending. Missing state retains ordinary recovery; incompatible schemas fail.
An entirely empty ready chunk checkpoints its verified zero counts without inventing
a dlt load receipt. Registration and all storage and quarantine gates still apply.

Market staging, history and current markets are views over the retained evidence rather than
duplicate physical copies. Their logical columns and observation grain are unchanged.
Publication and aggregate relations still require a full successful build and certification;
loading new evidence dirties that certification even though the views can already read it.
This saves retained and recovery storage at the cost of recomputing those projections.

The six original filenames contain explicit replacement projections: `events.parquet`,
`markets.parquet`, `outcomes.parquet`, `event_tags.parquet`, `event_series.parquet` and
`market_event_bridge.parquet`. `quarantine.parquet` accounts for malformed records and unusable
market identities. `coverage.json` records selected scope, successful empty requests, confirmed
absence and discovery limits. `release.json` declares the contract, normalization revision,
schemas, SHA-256 checksums, semantic row digests and a checksummed `capture-inventory.json`. Public columns
are defined centrally in `oddsfox_catalogue.contract`; optional outcome prices stay in raw evidence.

Native identity follows explicit source `version`: v1 nominates CTF tokens, v2 nominates
Protocol V2 positions. Both arrays may be populated and both fields are retained. Missing or
unsupported versions and missing, duplicate or ambiguous selected IDs make the market unusable.
These markets remain in the catalogue with reasons and no nominated outcomes. Asset ownership
conflicts quarantine every affected owner. Asset keys include venue and asset kind. Outcome
ordinals remain 1-based; chain index sets are separate and never inferred from labels or order.

Financial numbers are decoded as exact decimals and exported as canonical strings. Lifecycle
flags remain nullable. Whole observations are selected by direct-source priority, then receipt
time and observation ID. A selected null is not filled from an older observation. Explicit empty
memberships remove associations; missing membership may use recorded enclosing-event evidence.
The relationship export retains the responsible observation, receipt and inference marker.

History contains one row per observation, retaining labels, native identifiers, nullable fields,
source timestamps and receipt provenance. It makes no claim about historical validity intervals.
Processing timestamps and dbt invocation snapshots are operational evidence, outside semantic
rebuild equivalence.

Individually malformed source records retain the 1% registration limit. Ordinary identity
unavailability does not count toward it. Incomplete captures, unresolved requests, conflicting
observations, corrupt evidence, untrustworthy envelopes and failed validation block publication.
Accounted identity gaps, nullable fields, confirmed absence and successful empty units may publish.
Inactive events absent from list endpoints and without captured parent references are not
exhaustively discovered. A gap never establishes closure or zero activity.

Semantic fingerprints use `sha256-row-multiset-v1`: serialize each complete projected row as
canonical DuckDB JSON in UTC, SHA-256 each row, sort those fixed-width hashes, then stream
SHA-256 over the ordered schema and hash lines. Row counts and duplicate hashes are retained.
This compares exact semantic multisets without sorting large payload strings; equality relies
on SHA-256 collision resistance. Physical Parquet bytes have separate checksums.
Hash projection finishes in a private temporary table before sorting, so source operators
and the hash sorter do not need to remain active together. Only fixed-width hashes are
stored there; the table is removed after streaming, within the existing spill allowance.

Warehouse validity is recorded separately in the capture SQLite ledger. Mutations first mark it
dirty; a complete validated build certifies its captured inputs, coverage, model and normalization
revisions, quality configuration, schemas and semantic digests. Publication first admits a compatible
receipt and verifies its matching candidate, then recomputes the complete actual binding once
under the root lock before promotion. Non-public changes may leave unpromoted staging evidence.
Immutable verification checks only the release and does not require the
warehouse or ledger. The current pointer includes the release-manifest digest.

The packaged dbt profile passes configured engine threads, memory and temporary-storage
limits to DuckDB when the connection opens. Its scheduler runs one model or test at a time,
independently of the configured SQL engine threads.
Spill-directory settings are not reapplied for every adapter cursor after temporary
storage has been used. These settings preserve the operator's configured resource limits.
Market staging is a view over the stored observation fields, preserving its complete
projection, missing values and explicit JSON nulls. Full semantic verification also checks
the canonical logical bronze views. Loading dirties certification; publication requires
a complete successful rebuild of all models and tests.

Offline rebuild uses exactly the complete capture inventory and compares every declared semantic
warehouse relation and public projection, including observation history, metrics and quarantine.
It excludes `_dlt*`, `loaded_at`, `load_id`, `batch_loaded_at`, `built_through` and operational
`marts.catalogue_snapshots` contents. These exclusions describe processing rather than source facts.
Backup restoration verifies every inventoried file before copying into a fresh root.

Explicit `rebuild --verify --scratch PATH --resume` uses a bounded, checksummed phase record.
It revalidates source inventory, release, content, runtime, models and configuration, preserves
committed load checkpoints, and requires complete scratch certification before comparing rows.
Version-2 progress records reference the immutable original binding and byte seal under the
source's `.state/rebuild/source-bindings`; the registration is bound to the canonical scratch
path and cannot be replaced for a reused path. Each resumed phase verifies evidence bytes,
consistent SQLite state including committed WAL, runtime, models and declared configuration
before work and checkpoint commit. Owned temporary SQLite snapshots obey the same resource caps.
`--step` separates source validation, initialization, load, full build/certification and full
comparison into finite invocations. Pending phases explicitly report no semantic match; all
68 comparisons remain required for completion. Old records fail closed and need fresh scratch.
This does not change the public contract or digest revision. Trusted backup verification/restore
allowances may be supplied through `--max-retained-bytes`; defaults stay 64 GiB and manifests cannot
raise the caller's allowance. Version-2 backup JSON is compact and bounded to 128 MiB.

Coverage uses integer `coverage_schema_revision: 1` and retains one unit per response.
For validated contiguous ID-range requests, `id_range.start` and `id_range.end` are
inclusive canonical decimal strings spanning at most 100 IDs; other request filters
remain in `params`. The producer applies this encoding only to ID-range scans. Other
scan kinds retain their exact parameters, and immutable raw manifests always retain
the complete HTTP request. Range size describes requested IDs, not returned records
or individual absence. Bounds reduce cumulative coverage size; the unchanged 128 MiB
allowance still applies as all units accumulate. Unrevisioned development releases require
their original package for verification; new proofs use fresh roots.

Finite ID scans write native response leaves with `page_unit_revision: 2` while
retaining raw manifest version 1. Each split response keeps its original body,
endpoint, encoded query, receipt and row order. Range positions are inclusive
source IDs; frozen ID-list positions are half-open array offsets. Resume continues
after the verified last position, within the original sealed request windows.
Legacy fixed-window pages remain readable before new leaves in the same scan;
revision downgrades, gaps, overlaps and premature terminal units fail validation.
A singleton by-ID fallback retains its actual unfiltered query. Its 404 means
confirmed absence; an empty filtered list response means successful empty coverage.
Failed remaining leaves leave the capture incomplete. Response limits stay at 16 MiB.
Stop the previous process before upgrading. Once revision 2 pages exist, use a
compatible verified installation for resume, replay and rebuild; older wheels
cannot interpret those pages. Retain the previous environment for its older evidence.

New cursor keysets with an explicit positive integer limit use `page_unit_revision: 3`.
Only an oversized successful HTTP response triggers same-cursor limit halving; the
lower limit remains in use for that invocation. Resume starts from the verified
cursor and may halve the planned limit again. Failed attempts and partial bytes
count toward the unchanged invocation allowances. A singleton that exceeds the
body cap remains blocking. Native manifests retain the actual limit, bounded by
the sealed plan, and every other filter and cursor must match exactly. Decoded
record counts cannot exceed that limit. Revision 1/2 evidence stays readable;
older installations cannot read revision 3. No source rows are synthesized or skipped.

Live storage measurements tolerate temporary files removed by active jobs. Missing immutable
evidence still fails inventory verification; unreadable paths, symlinks and special files fail
storage checks. Where supported, accounting uses directory-relative stats and complete
directory listings, retaining the original path walker elsewhere. Skipped directory
identities fail validation unless the path genuinely disappeared. Traversal descriptors
close on success or failure; every existing storage checkpoint remains. Accounting is
a point observation, not an atomic filesystem snapshot. The catalogue smoke runs semantic verification and report queries in bounded
child processes so its shared deadline applies to those operations too.
