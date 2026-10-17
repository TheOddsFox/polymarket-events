"""metadata.v1: identity comes from the v3 store tables and the catalogue is never written."""

import hashlib
import json
from dataclasses import replace

import duckdb
import httpx
import pytest

from fakes.current_gamma import CurrentGammaServer
from oddsfox_catalogue import cli, metadata
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.current_store import apply, initialize
from oddsfox_catalogue.gamma.http import GammaClient


def seeded_catalogue(tmp_path, fixture):
    catalogue = CurrentSettings(
        tmp_path / "catalogue", limits=replace(CurrentLimits(), free_reserve_bytes=1)
    )
    validate_root(catalogue.root, initialize=True)
    with duckdb.connect(str(catalogue.store_path)) as conn:
        initialize(conn)
        apply(
            conn,
            project_response(
                list(fixture.events.values()),
                "events",
                received_at="2026-10-14T00:00:00Z",
                response_id="seed",
            ),
        )
    return catalogue


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_refresh_writes_the_v1_bundle_and_leaves_the_catalogue_store_untouched(tmp_path):
    fixture = CurrentGammaServer()
    catalogue = seeded_catalogue(tmp_path, fixture)
    before = digest(catalogue.store_path)
    output = tmp_path / "bundle"
    with GammaClient(catalogue.gamma, transport=httpx.MockTransport(fixture.handle)) as client:
        manifest = metadata.refresh_metadata(
            catalogue.gamma,
            catalogue,
            tmp_path / "producer",
            ["11", "12"],
            output,
            client=client,
        )
    assert manifest["contract"] == metadata.CONTRACT
    assert manifest["coverage_scope"] == "targeted"
    assert set(manifest["files"]) == {f"{name}.json" for name in metadata.RELATIONS}
    assert set(p.name for p in output.iterdir()) == {"manifest.json", *manifest["files"]}
    stored = json.loads((output / "manifest.json").read_text())
    transport = {"http_attempts", "downloaded_bytes"}
    assert stored == {key: value for key, value in manifest.items() if key not in transport}
    assert manifest["http_attempts"] == 2
    coverage = json.loads((output / "coverage.json").read_text())
    assert [(row["market_id"], row["status"]) for row in coverage] == [
        ("11", "found"),
        ("12", "found"),
    ]
    identity = json.loads((output / "identity_history.json").read_text())
    assert {row["market_id"] for row in identity} == {"11", "12"}
    assert all(row["source_kind"] == "catalogue_store" for row in identity)
    assert all(row["json_pointer"] == "/identity" for row in identity)
    assert (
        manifest["files"]["identity_history.json"]["sha256"]
        == hashlib.sha256((output / "identity_history.json").read_bytes()).hexdigest()
    )
    assert digest(catalogue.store_path) == before


def test_identity_history_is_read_from_store_tables_not_observations(tmp_path):
    fixture = CurrentGammaServer()
    catalogue = seeded_catalogue(tmp_path, fixture)
    with duckdb.connect(str(catalogue.store_path), read_only=True) as conn:
        rows = metadata._identity_history(conn, ["11", "missing"])
        stored = conn.execute(
            "SELECT coalesce(candidate, accepted) FROM _identity WHERE market_id='11'"
        ).fetchone()
    assert [row["market_id"] for row in rows] == ["11"]
    assert rows[0]["identities"] == json.loads(stored[0])
    assert rows[0]["usable"] is True
    assert rows[0]["identity_error"] is None
    assert rows[0]["page_id"] == rows[0]["capture_id"]


def test_conflicting_asset_claims_mark_identity_unusable(tmp_path):
    fixture = CurrentGammaServer()
    catalogue = seeded_catalogue(tmp_path, fixture)
    with duckdb.connect(str(catalogue.store_path)) as conn:
        outcome = json.loads(
            conn.execute(
                "SELECT coalesce(candidate, accepted) FROM _identity WHERE market_id='11'"
            ).fetchone()[0]
        )[0]
        conn.execute(
            "INSERT INTO _identity_claims VALUES ('polymarket','12','accepted',?,?)",
            [outcome["asset_kind"], outcome["asset_id"]],
        )
        rows = metadata._identity_history(conn, ["11"])
    assert rows[0]["usable"] is False
    assert rows[0]["identity_error"] == metadata.CONFLICT_ERROR


def test_refresh_refuses_existing_output_and_unbounded_limits(tmp_path):
    fixture = CurrentGammaServer()
    catalogue = seeded_catalogue(tmp_path, fixture)
    existing = tmp_path / "existing"
    existing.mkdir()
    with GammaClient(catalogue.gamma, transport=httpx.MockTransport(fixture.handle)) as client:
        with pytest.raises(metadata.MetadataError, match="new immutable"):
            metadata.refresh_metadata(
                catalogue.gamma, catalogue, tmp_path / "producer", ["11"], existing, client=client
            )
        with pytest.raises(metadata.MetadataError, match="byte limits"):
            metadata.refresh_metadata(
                catalogue.gamma,
                catalogue,
                tmp_path / "producer",
                ["11"],
                tmp_path / "bounded",
                client=client,
                max_body_bytes=metadata.MAX_BODY_BYTES + 1,
            )


def test_metadata_refuses_producer_inside_a_catalogue_root(tmp_path, capsys):
    catalogue = tmp_path / "catalogue"
    validate_root(catalogue, initialize=True)
    code = cli.main(
        [
            "--root",
            str(catalogue / "producer"),
            "metadata",
            "refresh",
            "--market-id",
            "11",
            "--output",
            str(tmp_path / "bundle"),
            "--catalogue-root",
            str(catalogue),
        ]
    )
    assert code == 3
    assert "isolated" in capsys.readouterr().out
    assert not (tmp_path / "bundle").exists()


def test_metadata_refuses_output_inside_a_catalogue_root(tmp_path, capsys):
    catalogue = tmp_path / "catalogue"
    validate_root(catalogue, initialize=True)
    code = cli.main(
        [
            "--root",
            str(tmp_path / "producer"),
            "metadata",
            "refresh",
            "--market-id",
            "11",
            "--output",
            str(catalogue / "bundle"),
            "--catalogue-root",
            str(catalogue),
        ]
    )
    assert code == 3
    assert "isolated" in capsys.readouterr().out
    assert not (catalogue / "bundle").exists()


def test_metadata_requires_an_explicit_producer_root(tmp_path, capsys):
    code = cli.main(
        [
            "metadata",
            "refresh",
            "--market-id",
            "11",
            "--output",
            str(tmp_path / "bundle"),
            "--catalogue-root",
            str(tmp_path / "catalogue"),
        ]
    )
    assert code == 3
    assert "explicit isolated --root" in capsys.readouterr().out
