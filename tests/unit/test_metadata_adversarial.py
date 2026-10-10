"""A newer broken identity must not silently refill an older usable handoff."""

import json
from decimal import Decimal

import pytest

from oddsfox_catalogue import metadata
from oddsfox_catalogue.capture.writer import write_page
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.metadata import Observation, export_metadata, project


def market(**changes):
    return {
        "id": "123",
        "conditionId": "0x" + "a" * 64,
        "outcomes": ["A", "B"],
        "clobTokenIds": ["11", "12"],
        **changes,
    }


def capture(settings, raw, *, name, received):
    write_page(
        settings.raw_dir / name,
        1,
        json.dumps([raw]).encode(),
        {
            "page_id": name,
            "batch_id": name,
            "observed_at": received,
            "record_key": "markets",
            "endpoint": "/markets",
            "http_status": 200,
        },
    )


def rows(directory, name):
    return json.loads((directory / (name + ".json")).read_text())


@pytest.mark.parametrize("invalid", [True, 0, -1, "00", "٠١", str(2**256)])
def test_native_identity_error_quarantines_whole_market(invalid):
    observed = Observation(
        market(clobTokenIds=[invalid, "12"]),
        {
            "source_kind": "market_direct",
            "received_at": "2026-10-10T00:00:00Z",
            "observation_id": "synthetic",
        },
    )
    projected, outcomes = project(observed)
    assert projected["usable"] is False and projected["identity_error"]
    assert outcomes == []


def test_newest_invalid_direct_observation_does_not_reuse_older_native_ids(tmp_path):
    settings = Settings(tmp_path)
    capture(settings, market(), name="old", received="2026-10-01T00:00:00Z")
    capture(settings, market(clobTokenIds=["11"]), name="new", received="2026-10-02T00:00:00Z")
    output = tmp_path / "handoff"
    export_metadata(settings, ["123"], output)
    assert rows(output, "outcomes") == []
    assert rows(output, "markets")[0]["usable"] is False
    history = rows(output, "identity_history")
    assert [row["usable"] for row in history] == [True, False]
    assert [row["asset_id"] for row in history[0]["identities"]] == ["11", "12"]


def test_label_reordering_changes_ordinals_but_not_native_identity_history(tmp_path):
    settings = Settings(tmp_path)
    capture(settings, market(), name="old", received="2026-10-01T00:00:00Z")
    capture(
        settings,
        market(outcomes=["B", "A"], clobTokenIds=["12", "11"]),
        name="new",
        received="2026-10-02T00:00:00Z",
    )
    output = tmp_path / "handoff"
    export_metadata(settings, ["123"], output)
    assert [(row["asset_id"], row["outcome_index"]) for row in rows(output, "outcomes")] == [
        ("12", 1),
        ("11", 2),
    ]
    assert all(row["chain_index_set"] is None for row in rows(output, "outcomes"))
    history = rows(output, "identity_history")
    assert [row["asset_id"] for row in history[0]["identities"]] == ["11", "12"]


def test_output_commit_failure_does_not_expose_partial_bundle_or_change_global_pointer(
    tmp_path,
    monkeypatch,
):
    settings = Settings(tmp_path)
    capture(settings, market(), name="page", received="2026-10-01T00:00:00Z")
    settings.published_dir.mkdir(parents=True)
    current = settings.published_dir / "current.json"
    current.write_bytes(b'{"release":"existing"}')
    output = tmp_path / "handoff"
    original = metadata.atomic_write_bytes

    def crash(path, body):
        if path.name == "manifest.json":
            raise OSError("synthetic final metadata manifest failure")
        return original(path, body)

    monkeypatch.setattr(metadata, "atomic_write_bytes", crash)
    with pytest.raises(OSError, match="synthetic"):
        export_metadata(settings, ["123"], output)
    assert not output.exists()
    assert not list(output.parent.glob(".metadata-*"))
    assert current.read_bytes() == b'{"release":"existing"}'


def test_exact_decimals_do_not_overwrite_explicit_nullable_lifecycle():
    observed = Observation(
        market(
            orderPriceMinTickSize=Decimal("0.0000000000000000000000000001"),
            active=None,
            closed=False,
            outcomePrices="not-an-array",
        ),
        {
            "source_kind": "market_direct",
            "received_at": "2026-10-10T00:00:00Z",
            "observation_id": "synthetic",
        },
    )
    projected, outcomes = project(observed)
    assert Decimal(projected["tick_size"]) == Decimal("1E-28")
    assert projected["active"] is None and projected["closed"] is False
    assert len(outcomes) == 2
