# Pipeline performance and recovery

The internal layout is `oddsfox.polymarket.warehouse.v5`. Public
`oddsfox.polymarket.catalogue.v2`, `oddsfox.polymarket.metadata.v1`, source observations,
coverage meanings and semantic digest rules remain the contracts to preserve.
The end-to-end 2× performance target is **unproven**. Existing isolated measurements in
[README](../README.md) do not establish this gate, catalogue-scale fit or operational readiness.

## Upgrade into a fresh root

Stop writers and preserve the old root, its verified releases and its matching old wheel/environment.
The v5 installation refuses a v4 warehouse before mutation. Do not remove its stamp, copy its
warehouse into a new root, reuse its dlt/checkpoint state, or run a destructive migration.
Use equivalent quality and resource settings in the fresh root; examples below use the standard
internal data/state layout. Custom external paths need their own confined, fresh destinations.

Use absolute paths outside the checkout for operator state. Supply complete, verified raw captures;
copying evidence is preparation, outside any timed benchmark. These commands retain the source:

```sh
set -eu
OLD_ROOT=/absolute/path/to/preserved-v4-root
NEW_ROOT=/absolute/path/to/fresh-v5-root
NEW_PYTHON=/absolute/path/to/v5-environment/bin/python

test ! -e "$NEW_ROOT"
mkdir -p "$NEW_ROOT/data"
cp -R "$OLD_ROOT/data/raw" "$NEW_ROOT/data/raw"
export CATALOGUE_ROOT="$NEW_ROOT"
"$NEW_PYTHON" -m oddsfox_catalogue.cli ledger rebuild
"$NEW_PYTHON" -m oddsfox_catalogue.cli load
```

`ledger rebuild` verifies raw manifests and bodies and requires an absent destination ledger.
It never overwrites the old ledger. A load command processes all eligible pending pages in
bounded chunks; check that `pages_remaining` is zero. If interrupted, repeat the explicit
load command against the same root. A market-only load leaves the other page types pending.
Incomplete captures require an explicit named capture recovery before they can register.
Then run the complete build, publish and independent immutable-release verification:

```sh
"$NEW_PYTHON" -m oddsfox_catalogue.cli dbt -- build
"$NEW_PYTHON" -m oddsfox_catalogue.cli publish
"$NEW_PYTHON" -m oddsfox_catalogue.cli verify
"$NEW_PYTHON" -m oddsfox_catalogue.cli rebuild --verify \
  --scratch /absolute/path/to/fresh-v5-recovery-scratch
```

`rebuild --verify` compares a compatible source warehouse with fresh scratch; it is not a
v4-to-v5 migration command. Use the new, fully built v5 root as its source. Named scratch
can resume with the same command plus `--resume`; `--step` separates the verified recovery
phases. Pending phases are not semantic matches. Keep the source frozen during comparison.
A raw copy alone, a completed load, a subset build, or immutable release verification alone
does not prove the complete recovery gate.

Rollback uses the preserved root and matching old installation, never the v5 wheel against v4:

```sh
export CATALOGUE_ROOT=/absolute/path/to/preserved-v4-root
OLD_PYTHON=/absolute/path/to/preserved-v4-environment/bin/python
"$OLD_PYTHON" -m oddsfox_catalogue.cli verify
"$OLD_PYTHON" -m oddsfox_catalogue.cli current
```

Point consumers back to that verified retained release through their existing configuration.
Keep the new root for diagnosis. No tool in this workflow automatically deletes either root,
prunes evidence, installs a wheel, or switches a consumer's configured root.

## Visibility and checkpoint repair

Registered bronze observations, staging, and view-based market history expose retained evidence
as soon as it registers. Compact selection state and derived current relations describe the
last build. Event history and event/market metrics use incremental registered-batch checkpoints
and advance during builds. Loading dirties certification even when a view already sees new rows.
Publication requires a complete successful build with every required model and test.

Event and market selection retain only keys, winning observation IDs, counts and first receipt
times. New registered batches identify affected keys, which rerank against their complete history.
Market source priority, receipt/ID ties, selected nulls and whole-observation membership semantics
are unchanged. Current rows join the winning ID back to its complete observation. Compact asset
nominations and conflict-owner tables are rebuilt from selected state; shared native owners still
make every affected market unusable.

Private `__checkpoint_*` tables contain a processing version and registered batch IDs. Missing,
invalid or stale checkpoints fail closed. Follow the failing model name in the diagnostic; rebuild
only that model's checkpoint with an explicit full refresh, then run a complete build:

```sh
# Substitute the exact failing model; this example repairs market selection state.
"$NEW_PYTHON" -m oddsfox_catalogue.cli dbt -- build \
  --full-refresh --select int_market_selection_state
"$NEW_PYTHON" -m oddsfox_catalogue.cli dbt -- build
"$NEW_PYTHON" -m oddsfox_catalogue.cli publish
"$NEW_PYTHON" -m oddsfox_catalogue.cli verify
```

Do not edit checkpoint rows manually or adopt an old checkpoint into v5. A stale dependency
requires its ancestors to build first. A selected repair does not certify the warehouse, and
successful partial state must not be published before the complete build succeeds.

## Implemented mechanisms and rejected alternatives

| Surface | Current behavior |
| --- | --- |
| Capture verification | Resume/adoption validates request identity and fully verifies/decodes each body without the immediately duplicated checksum read. Raw durability, orphan checks and failure handling remain required. |
| Daily missing markets | Production retains native unfiltered lookups. The proposed filtered bulk replacement was rejected; see below. |
| Load | Physical normalized fields are encoded once per incoming row and passed to stored resources. Historical matches compare persisted physical fields with exact canonical values; conflicts still block insert-only merges. |
| Storage accounting | One measurement classifies retained and temporary paths for the same admission checkpoint. This is a point measurement; it is not a persistent cache or atomic filesystem snapshot. |
| dbt | Compact selection state and registered-batch history/metrics checkpoints avoid repeating unchanged work. Current selection and asset-conflict state remain build-consistent. |
| Certification | A single warehouse read transaction verifies logical views/checkpoints. Only structurally verified exact aliases with equal schemas reuse an already computed fingerprint. Other relations still receive complete semantic fingerprints. |
| Publication | Semantic fingerprints avoid unnecessary ordering while retaining multiset semantics; physical output checksums, candidate validation and final actual binding checks remain. |
| Measurement | Payload-free `pipeline_phase` elapsed times and `pipeline_counter` records expose work boundaries. They do not substitute for semantic or recovery evidence. |

The live bulk candidate failed the request contract: native market lookups returned `events`
that corresponding keyset/list bulk responses omitted. That changes `membership_mode`, memberships
and semantic hashes even when market IDs match. Production therefore keeps native daily market
refreshes. Recorded fixtures cannot establish that distinct live routes return equivalent payloads.

ART or other new persistent indexes were not introduced. Broad metadata/content caches,
stat-only seals, and page group commits were not adopted: the current work preserves complete
content verification, explicit checkpoint validation and native page durability. These alternatives
have no accepted end-to-end gain claim. No default worker, rate, memory or storage allowance was
raised to obtain a passing performance result.

| Candidate | Disposition and reason |
| --- | --- |
| ART indexes | Rejected: prior 25,000-key diagnostics added storage without consistent merge gains. No different workload has qualified their write, memory, backup and rebuild costs. |
| Metadata locator | Rejected: skipping unrelated raw pages weakens global corruption detection; a globally validating locator has no demonstrated material remaining benefit. |
| Compression | Gzip level 6 retained; no paired complete-workflow result qualifies a change. |
| Capture concurrency | One worker retained; no representative paired workflow result qualifies increasing it under the same rate and resource caps. |
| Source seals | Fresh byte-bound seals retained; stat or writer-lock evidence cannot replace content checks. |
| Backup verification | Verified creation, CLI final verification, source stability and fresh restore verification retained. |
| Group commit, segments and async framework | Excluded: their protocol and dependency costs are unsupported by the evidence. |
| Narrow ranking and general JSON rewrite | Existing baseline work retained; this change adds compact persisted selection instead of repeating that work. |

## Recorded benchmark tools

These repository tools are manually invoked experiments, separate from the installed package.
They never prepare operator seeds automatically. Inputs must be independently verified before use:

- [`build_corpus`](../tools/speed_corpus.py) accepts an iterable of `(entity, records)` pages,
  an exclusive new SQLite path, `high_water={"events": ..., "markets": ...}`, and positive finite
  `delays={"high_water": seconds, "keyset": seconds, "single": seconds}`. Index only verified
  top-level event/market records, not embedded market context. Conflicting versions of one identity
  invalidate construction; identical duplicates deduplicate. Missing/null lifecycle flags remain
  unknown, retain their original JSON, and do not match filtered open/closed lists. Returned counts,
  corpus SHA-256 and delay-policy SHA-256 pin the experiment. No live API is called.
- `RecordedGammaServer` serves that corpus on `127.0.0.1` with real route sleeps, strict queries,
  bounded ID lists, numeric pagination and native 404s. Keysets stream HTTP 200 responses with exact
  `Content-Length`, reading stored SQLite BLOBs in 64 KiB chunks. Responses above the production
  16 MiB body cap exercise capture's adaptive page-size reduction without truncating fixture data
  or raising that cap. Accounting conservatively reserves the full response length even when the
  client stops reading early; expected disconnects are not fixture errors. It retains counters,
  not response payload logs.
- [`run_variant`](../tools/speed_benchmark.py) measures an installed Python daily workflow with real
  clocks: capture, explicit market-page load at 250 pages, remaining load at 50, complete dbt build,
  publish and verify. It records commands, return codes, wall time and each completed acceptance
  report. Only fresh matching request/download/duration-budget errors permit bounded named capture
  resumes. Storage, corruption and other stage failures stop the workflow.
- [`content_snapshot` and `compare_snapshots`](../tools/speed_oracle.py) verify releases and compare
  public content, schemas, new-batch source evidence, quarantine and committed capture scope.
  Documented acquisition-specific receipt/observation fields are excluded from this cross-run
  content comparison. It explicitly does not certify provenance or raw recovery.

`run_variant` requires an existing captured seed inside the accounting workspace and a bounded
`ROOT/.state/pipeline-speed-owner.json` containing these required fields:

```json
{"purpose":"pipeline-speed-benchmark","revision":1,"baseline_commit":"bd5eb0c1af1e40295305b6558d9ea28d1f1ce5fa"}
```

`seed_release_id` may additionally identify preparation evidence. The operator prepares and verifies
this disposable seed; the tool refuses unmarked roots. Prepare both installed runtimes and equivalent
seeds before timing. Keep interpreters outside operator roots, and all operator/recovery outputs
inside the accounting workspace. Retain source roots unchanged.

Before either timed run, prepare one external experiment JSON file with exclusive creation,
then make it read-only (for example, mode `0444`). Both variants must use the identical file;
the tool hashes its actual bytes, requires its strict schema and refuses a manifest inside a
Git checkout. Its required structure is:

```json
{
  "revision": 1,
  "shared": {
    "lock_sha256": "SHA256",
    "dependencies_sha256": "SHA256",
    "effective_settings_sha256": "SHA256",
    "python_version": "3.12.0",
    "machine": {
      "node": "HOST", "system": "OS", "release": "RELEASE", "machine": "ARCH",
      "cpu_count": 1, "physical_memory_bytes": 1
    },
    "harness_sha256": "SHA256",
    "seed_raw_inventory": {"bytes": 1, "sha256": "SHA256"},
    "seed_coverage": {"bytes": 1, "sha256": "SHA256"}
  },
  "variants": {
    "baseline": {"runtime": {"bytes": 1, "sha256": "SHA256"}, "wheel_sha256": "SHA256"},
    "candidate": {"runtime": {"bytes": 1, "sha256": "SHA256"}, "wheel_sha256": "SHA256"}
  }
}
```

Replace every illustrative value with independently measured preparation evidence; each SHA-256
must be 64 lowercase hexadecimal characters. Runtime descriptors are installed
`_runtime_revision()` results. Dependency hashing uses the sorted installed distribution
`(Name, version)` pairs serialized as compact JSON. Harness hashing uses compact sorted-key JSON
mapping `speed_benchmark.py`, `speed_corpus.py`, and `speed_oracle.py` to their file SHA-256 values.
Raw/coverage descriptors are `capture_binding(settings)["inventory"]` and `["coverage"]`, after
its full fresh verification of complete, loaded, registered seed captures. Equal batch names or
row counts alone are insufficient seed pins.

The required `experiment`, `variant` and `wheel` arguments extend `run_variant`; optional
`lockfile` defaults to this checkout's `uv.lock`. Before timing, an isolated installed-Python
probe (`-I`) freshly verifies raw seed inventory/coverage under the accepted resource settings
and reports the runtime, Python version, dependency list/hash and structured effective settings.
`effective_settings_sha256` hashes compact sorted-key JSON of every field in the accepted daily
`gamma`, `capture`, `load`, `quality` and `schedule` settings. Only `gamma.base_url` becomes the
fixed string `recorded-fixture`; loopback is explicitly forced true for this harness. The installed
probe first uses the noncontacted `http://127.0.0.1:9` sentinel. Root paging, payload flags,
timeouts, retries, partitioning and high-water overrides remain in the pin. The probe rejects
any nondefault `PathSettings`, including external dbt project/profile paths. Absolute root paths
are excluded because variant roots differ; their standard layout is enforced instead.
The observed settings structure is retained and its digest recomputed during comparison, so an
unchanged declared hash cannot hide changed settings. Prepare and freeze this hash before the
first timed run. Local hardware, lock, wheel
and harness bytes must match the frozen manifest. Each completed stage's runtime descriptor must
also match its declared variant; a changed runtime stops later stages. The supplied wheel's hash
is checked, while its relationship to the expected installed runtime is an explicit attestation
in the frozen preparation manifest. The harness does not install or reconstruct that wheel.

```sh
python tools/speed_benchmark.py \
  --python /absolute/path/to/installed-environment/bin/python \
  --root /absolute/path/to/workspace/owned-seed \
  --workspace /absolute/path/to/workspace \
  --corpus /absolute/path/to/workspace/recorded.sqlite \
  --experiment /absolute/path/to/workspace/frozen-experiment.json \
  --variant candidate \
  --wheel /absolute/path/to/prepared-candidate.whl \
  --lockfile /absolute/path/to/pinned/uv.lock \
  --output /absolute/path/to/workspace/candidate-result.json
```

Output creation is exclusive and atomic and must be outside Git checkouts. The fixture's workflow
bound is 200,000 requests and 32 GiB downloaded. At most eight capture invocations are allowed;
each installed acceptance invocation still has 25,000 requests, 4 GiB downloaded, a 16 MiB body cap,
and four hours. Acceptance uses one worker, daily rate 10/s, 2 GB DuckDB memory and two SQL threads;
its combined workspace envelope is 256 GiB retained / 32 GiB temporary with 32 GiB filesystem free
reserve. Ordinary operator defaults remain unchanged. Preparation is outside the workflow timer;
workflow startup, capture resumes and required stages stay inside it. Fixture errors invalidate the
run; native 404s are accounted absence, not fixture failures.

`compare_runs(baseline, candidate, scaling_variability=0)` requires successful workflows, verified
limits, opposite baseline/candidate roles, the same frozen experiment digest and shared pins,
matching observed seed/runtime/wheel/dependency/hardware/harness evidence, matching corpus/delay/
workload/resource pins, positive finite durations and explicit `semantic_validated: true` from
independent proof. It rechecks the observed values and every stage runtime against the manifest;
a manually added verification boolean cannot replace missing or mismatched pins. The runner always initially records false.
Complete content equality, release/schema/coverage and source-evidence checks, native-route contract
checks, same-variant raw recovery/rebuild and failure/retry recovery are separate required evidence;
a content-only match cannot set this gate by itself.

The target is candidate wall time / baseline wall time ≤ 0.50. Ratios from 0.45 through 0.55,
or scaling variability above 10%, require repeats. Failed, capped, incomplete, unvalidated or
mismatched runs produce no passing speed ratio. No benchmark or full acceptance gate is claimed
passed by this guide; record reproducible paired evidence before making that claim.
