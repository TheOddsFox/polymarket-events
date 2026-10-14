"""Content-only benchmark comparison retains every non-packaging invariant."""

import hashlib
import importlib.util
import json
from pathlib import Path

import duckdb
import pytest

from fakes.built_warehouse import build_warehouse, copy_built
from fakes.harness import FIXED_NOW
from oddsfox_catalogue.gamma.http import RequestBudgetExceeded
from oddsfox_catalogue.publish import PublishBlocked, fingerprint_parquet, publish_release

_SPEC = importlib.util.spec_from_file_location(
    "speed_oracle", Path(__file__).resolve().parents[2] / "tools/speed_oracle.py"
)
oracle = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(oracle)


@pytest.fixture(scope="module")
def published(tmp_path_factory):
    root = tmp_path_factory.mktemp("oracle")
    settings = build_warehouse(root)
    info = publish_release(settings, now=FIXED_NOW)
    with duckdb.connect(str(settings.warehouse_path), read_only=True) as con:
        batch_id = con.execute("select batch_id from bronze.batch_registry").fetchone()[0]
    snapshot = oracle.content_snapshot(settings, batch_id, info.path)
    return settings, batch_id, info.release_id, snapshot


@pytest.fixture
def candidate(published, tmp_path):
    original, batch_id, release_id, baseline = published
    settings = copy_built(original.root, tmp_path)
    return settings, batch_id, settings.published_dir / "releases" / release_id, baseline


def _rewrite(release, table, query):
    path = release / f"{table}.parquet"
    with duckdb.connect() as con:
        con.execute("create temp table original as select * from read_parquet(?)", [str(path)])
        con.execute(f"copy ({query}) to '{path}' (format parquet)")
        semantic = fingerprint_parquet(con, path)
    manifest_path = release / "release.json"
    manifest = json.loads(manifest_path.read_text())
    raw = path.read_bytes()
    manifest["tables"][table] = {
        "file": path.name,
        "bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        **semantic,
    }
    manifest_path.write_text(json.dumps(manifest))


def _compare(candidate):
    settings, batch_id, release, baseline = candidate
    return oracle.compare_snapshots(baseline, oracle.content_snapshot(settings, batch_id, release))


def test_all_and_only_packaging_columns_are_ignored(candidate):
    _, _, release, _ = candidate
    for table, columns in oracle.EXCLUDED_COLUMNS.items():
        if not columns:
            continue
        replacements = ", ".join(
            ("cast('2040-01-01' as timestamptz)" if name.endswith("_at") else "'other-package'")
            + f' as "{name}"'
            for name in sorted(columns)
        )
        _rewrite(release, table, f"select * replace ({replacements}) from original")
    assert _compare(candidate) == {"status": "content_equal", "differing_keys": []}


@pytest.mark.parametrize(
    ("table", "replacement"),
    [
        ("markets", "'event_embedded' as source_kind"),
        ("markets", "observation_count + 1 as observation_count"),
        ("markets", "null::varchar as question"),
        ("markets", "'0.123456789123456789123456789' as volume"),
        ("markets", "cast('2040-01-01' as timestamptz) as source_updated_at"),
        ("events", "market_count + 1 as market_count"),
        ("events", "'changed' as volume"),
        ("outcomes", "'changed' as outcome_label"),
        ("market_event_bridge", "'changed' as membership_source_kind"),
        ("event_tags", "'changed' as tag_label"),
        ("event_series", "'changed' as series_title"),
    ],
)
def test_unlisted_content_changes_fail(candidate, table, replacement):
    _, _, release, _ = candidate
    _rewrite(release, table, f"select * replace ({replacement}) from original")
    assert _compare(candidate) == {
        "status": "semantic_mismatch",
        "differing_keys": ["public_content"],
    }


def test_public_duplicate_multiplicity_is_preserved(candidate):
    _, _, release, _ = candidate
    _rewrite(
        release, "markets", "select * from original union all select * from original limit 1000"
    )
    assert _compare(candidate)["status"] == "semantic_mismatch"


@pytest.mark.parametrize(
    "mutation",
    ["event-payload", "market-payload", "market-kind", "market-parent", "duplicate", "quarantine"],
)
def test_new_batch_source_evidence_multisets_detect_changes(candidate, mutation):
    settings, batch_id, _, _ = candidate
    with duckdb.connect(str(settings.warehouse_path)) as con:
        if mutation == "event-payload":
            con.execute(
                "update bronze.event_observations_values set payload_hash = 'different' where batch_id = ?",
                [batch_id],
            )
        elif mutation == "duplicate":
            con.execute(
                "insert into bronze.market_observations_values select * from bronze.market_observations_values where batch_id = ? limit 1",
                [batch_id],
            )
        elif mutation == "quarantine":
            # Loaded malformed rows are evidence even when their packaging differs.
            con.execute(
                "insert into bronze.quarantined_records by name select 'synthetic' as quarantine_id, ? as batch_id, 'page' as page_id, 'market' as entity, '/markets/0' as json_pointer, 'invalid' as reason, current_timestamp as observed_at, '{\"id\":\"123\"}'::json as payload, 'synthetic' as _dlt_load_id, 'synthetic' as _dlt_id",
                [batch_id],
            )
        else:
            column, value = {
                "market-payload": ("payload_hash", "different"),
                "market-kind": ("source_kind", "different"),
                "market-parent": ("n_enclosing_event_id", '"different"'),
            }[mutation]
            con.execute(
                f'update bronze.market_observations_values set "{column}" = ? where batch_id = ?',
                [value, batch_id],
            )
    assert _compare(candidate) == {
        "status": "semantic_mismatch",
        "differing_keys": ["source_evidence"],
    }


def test_source_packaging_changes_do_not_change_content_evidence(candidate):
    settings, batch_id, _, _ = candidate
    with duckdb.connect(str(settings.warehouse_path)) as con:
        for table in ("event_observations_values", "market_observations_values"):
            con.execute(
                f"update bronze.{table} set observation_id = observation_id || '-packaging', page_id = 'other', observed_at = '2040-01-01' where batch_id = ?",
                [batch_id],
            )
    assert _compare(candidate)["status"] == "content_equal"


def test_scope_ignores_packaging_but_detects_high_water_change(candidate):
    settings, batch_id, _, _ = candidate
    marker = next(settings.raw_dir.glob(f"*/{batch_id}/_batch.json"))
    value = json.loads(marker.read_text())
    value["plan"] = []
    value["params"] = {"limit": 1}
    marker.write_text(json.dumps(value))
    assert _compare(candidate)["status"] == "content_equal"
    value["scope"]["high_water"]["markets"] += 1
    marker.write_text(json.dumps(value))
    assert _compare(candidate) == {
        "status": "semantic_mismatch",
        "differing_keys": ["capture_scope"],
    }


@pytest.mark.parametrize("unsafe", ["outside", "release-link", "parquet-link"])
def test_unsafe_release_paths_are_rejected(candidate, tmp_path, unsafe):
    settings, batch_id, release, _ = candidate
    if unsafe == "outside":
        release = tmp_path / "outside"
        release.mkdir()
    elif unsafe == "release-link":
        link = release.parent / "link"
        link.symlink_to(release, target_is_directory=True)
        release = link
    else:
        target = release / "markets.parquet"
        other = tmp_path / "other.parquet"
        target.rename(other)
        target.symlink_to(other)
    with pytest.raises((PublishBlocked, ValueError)):
        oracle.content_snapshot(settings, batch_id, release)


def test_quarantine_payload_canonicalization_and_packaging(candidate):
    settings, batch_id, release, _ = candidate
    with duckdb.connect(str(settings.warehouse_path)) as con:
        con.execute(
            "insert into bronze.quarantined_records by name select 'canonical' as quarantine_id, ? as batch_id, 'page' as page_id, 'market' as entity, '/markets/0' as json_pointer, 'invalid' as reason, current_timestamp as observed_at, '{\"id\":\"123\",\"extra\":null}'::json as payload, 'canonical' as _dlt_load_id, 'canonical' as _dlt_id",
            [batch_id],
        )
    before = oracle.content_snapshot(settings, batch_id, release)
    with duckdb.connect(str(settings.warehouse_path)) as con:
        con.execute(
            "update bronze.quarantined_records set payload = '{\"extra\": null, \"id\": \"123\"}', json_pointer = '/markets/99', page_id = 'new-package'"
        )
    after = oracle.content_snapshot(settings, batch_id, release)
    assert oracle.compare_snapshots(before, after)["status"] == "content_equal"
    with duckdb.connect(str(settings.warehouse_path)) as con:
        con.execute("update bronze.quarantined_records set reason = 'different'")
    assert oracle.compare_snapshots(before, oracle.content_snapshot(settings, batch_id, release))[
        "differing_keys"
    ] == ["source_evidence"]


def test_quarantine_public_ids_are_ignored_but_reason_is_preserved(candidate):
    settings, batch_id, release, _ = candidate
    _rewrite(
        release,
        "quarantine",
        "select * from original union all select 'polymarket', 'market', '123', 'obs', 'page', 'invalid', cast('2026-01-01' as timestamptz)",
    )
    before = oracle.content_snapshot(settings, batch_id, release)
    _rewrite(
        release,
        "quarantine",
        "select * replace ('new-obs' as observation_id, 'new-page' as page_id, cast('2040-01-01' as timestamptz) as received_at) from original",
    )
    assert (
        oracle.compare_snapshots(before, oracle.content_snapshot(settings, batch_id, release))[
            "status"
        ]
        == "content_equal"
    )
    _rewrite(release, "quarantine", "select * replace ('different' as reason) from original")
    assert oracle.compare_snapshots(before, oracle.content_snapshot(settings, batch_id, release))[
        "differing_keys"
    ] == ["public_content"]


def test_comparison_includes_full_schemas_and_never_claims_full_gate(published):
    *_, baseline = published
    candidate = {**baseline, "public_schemas": {}}
    assert oracle.compare_snapshots(baseline, candidate) == {
        "status": "semantic_mismatch",
        "differing_keys": ["public_schemas"],
    }
    serialized = json.dumps(baseline)
    assert "Will candidate" not in serialized
    assert "complete" not in serialized and "passed" not in serialized


@pytest.mark.parametrize("version", ["v4", "v5"])
def test_source_warehouse_stamp_is_not_enforced_or_mutated(candidate, version):
    settings, _, _, _ = candidate
    with duckdb.connect(str(settings.warehouse_path)) as con:
        con.execute(
            "update catalogue_meta.contract set contract = ?",
            ["oddsfox.polymarket.warehouse." + version],
        )
    before = hashlib.sha256(settings.warehouse_path.read_bytes()).hexdigest()
    assert _compare(candidate)["status"] == "content_equal"
    assert hashlib.sha256(settings.warehouse_path.read_bytes()).hexdigest() == before


def test_raw_marker_symlink_is_rejected(candidate, tmp_path):
    settings, batch_id, release, _ = candidate
    marker = next(settings.raw_dir.glob(f"*/{batch_id}/_batch.json"))
    target = tmp_path / "outside-marker.json"
    marker.rename(target)
    marker.symlink_to(target)
    with pytest.raises((ValueError, RequestBudgetExceeded), match="symlink"):
        oracle.content_snapshot(settings, batch_id, release)
