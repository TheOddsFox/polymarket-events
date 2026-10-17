"""Benchmark equivalence ignores encoding while retaining publication meaning."""

from copy import deepcopy

import duckdb
import pytest
from test_full_pass_benchmark import benchmark

from oddsfox_catalogue.fingerprints import semantic_fingerprint


def export_manifest(connection, path, query):
    connection.execute(f"COPY ({query}) TO ? (FORMAT PARQUET)", [str(path)])
    fingerprint = semantic_fingerprint(connection, "SELECT * FROM read_parquet(?)", [str(path)])
    return {
        "contract": "fixture-v1",
        "projection_version": 1,
        "generation": 2,
        "counts": {"events": fingerprint["rows"]},
        "coverage": {"complete": True},
        "backlog": {"events": 0},
        "full_pass": {"cursor_event_id": 3, "completed": True},
        "relations": {
            "events": {
                **fingerprint,
                "sha256": benchmark.digest(path),
                "bytes": path.stat().st_size,
            }
        },
    }


ROWS = "SELECT * FROM (VALUES (1, 'a'), (2, NULL), (3, 'c'), (3, 'c')) AS t(id,label)"


def test_equivalent_parquet_encodings_compare_equal_without_mutating_manifests(tmp_path):
    with duckdb.connect() as connection:
        ascending = export_manifest(
            connection, tmp_path / "ascending.parquet", ROWS + " ORDER BY id ASC"
        )
        descending = export_manifest(
            connection, tmp_path / "descending.parquet", ROWS + " ORDER BY id DESC"
        )
    left = ascending["relations"]["events"]
    right = descending["relations"]["events"]
    assert left["sha256"] != right["sha256"]
    assert {key: value for key, value in left.items() if key not in {"sha256", "bytes"}} == {
        key: value for key, value in right.items() if key not in {"sha256", "bytes"}
    }
    originals = deepcopy((ascending, descending))
    assert benchmark.canonical_hash(benchmark.normalized_publication(ascending)) == (
        benchmark.canonical_hash(benchmark.normalized_publication(descending))
    )
    assert (ascending, descending) == originals
    # The raw evidence must still retain the physical descriptors for integrity checks.
    assert all("sha256" in item["relations"]["events"] for item in originals)
    assert all("bytes" in item["relations"]["events"] for item in originals)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id, coalesce(label, 'changed') AS label FROM (" + ROWS + ")",
        "SELECT CAST(id AS VARCHAR) AS id, label FROM (" + ROWS + ")",
        "SELECT * FROM (" + ROWS + ") LIMIT 3",
    ],
    ids=["changed-row", "changed-schema", "removed-duplicate"],
)
def test_substantive_parquet_changes_remain_different(tmp_path, query):
    with duckdb.connect() as connection:
        original = export_manifest(connection, tmp_path / "original.parquet", ROWS)
        changed = export_manifest(connection, tmp_path / "changed.parquet", query)
    assert benchmark.canonical_hash(benchmark.normalized_publication(original)) != (
        benchmark.canonical_hash(benchmark.normalized_publication(changed))
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("rows", 5),
        ("semantic_sha256", "different-content"),
        ("semantic_digest_revision", "sha256-row-multiset-v2"),
        ("schema", [{"name": "id", "type": "VARCHAR"}]),
    ],
)
def test_relation_semantic_descriptors_are_not_discarded(tmp_path, field, value):
    with duckdb.connect() as connection:
        original = export_manifest(connection, tmp_path / "original.parquet", ROWS)
    changed = deepcopy(original)
    changed["relations"]["events"][field] = value
    assert benchmark.canonical_hash(benchmark.normalized_publication(original)) != (
        benchmark.canonical_hash(benchmark.normalized_publication(changed))
    )


def test_publication_count_remains_part_of_comparison(tmp_path):
    with duckdb.connect() as connection:
        original = export_manifest(connection, tmp_path / "original.parquet", ROWS)
    changed = deepcopy(original)
    changed["counts"]["events"] += 1
    assert benchmark.canonical_hash(benchmark.normalized_publication(original)) != (
        benchmark.canonical_hash(benchmark.normalized_publication(changed))
    )
