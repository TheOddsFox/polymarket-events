# Catalogue v2

`oddsfox.polymarket.catalogue.v2` replaces the legacy warehouse and public projections.
Use a fresh operator root. Retain old evidence and state separately; neither migration nor
compatibility views are provided. The independent `oddsfox.polymarket.metadata.v1` JSON bundle
shape remains supported, using the same normalization rules.

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

Warehouse validity is recorded separately in the capture SQLite ledger. Mutations first mark it
dirty; a complete validated build certifies its captured inputs, coverage, model and normalization
revisions, quality configuration, schemas and semantic digests. Publication recomputes that binding
under the root lock. Immutable verification checks only the release and does not require the
warehouse or ledger. The current pointer includes the release-manifest digest.

Offline rebuild uses exactly the complete capture inventory and compares every declared semantic
warehouse relation and public projection, including observation history, metrics and quarantine.
It excludes `_dlt*`, `loaded_at`, `load_id`, `batch_loaded_at`, `built_through` and operational
`marts.catalogue_snapshots` contents. These exclusions describe processing rather than source facts.
Backup restoration verifies every inventoried file before copying into a fresh root.

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

Live storage measurements tolerate temporary files removed by active jobs. Missing immutable
evidence still fails inventory verification; unreadable paths, symlinks and special files fail
storage checks. The catalogue smoke runs semantic verification and report queries in bounded
child processes so its shared deadline applies to those operations too.
