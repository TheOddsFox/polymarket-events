"""Current ZSTD snapshots preserve exact values and coexist with older codecs."""

import json
from pathlib import Path

import pytest

from fakes.world import make_event, make_market
from oddsfox_catalogue import current_backup as backup
from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, validate_root
from oddsfox_catalogue.current_contract import BACKUP_CONTRACT, SCHEMAS, projection
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.fingerprints import semantic_fingerprint

POSITIONS = [str(2**256 - 1), str(2**255 + 1)]


@pytest.fixture
def settings(tmp_path, monkeypatch):
    settings = CurrentSettings(tmp_path / "catalogue")
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        store.initialize(conn)
    temporary = tmp_path / "verification"
    temporary.mkdir()
    monkeypatch.setattr(backup.tempfile, "gettempdir", lambda: str(temporary))
    return settings


def apply_native(conn, title, day):
    market = make_market("2", "native", outcomes=("Oui", "否"))
    market.update(
        question=None, conditionId=None, positionIds=POSITIONS, active=True, archived=None
    )
    event = make_event("1", title, markets=[market])
    event.update(slug=None, active=None, closed=False, archived=True)
    store.apply(
        conn,
        project_response(
            [event],
            "events",
            received_at=f"2026-10-{day:02d}T00:00:00Z",
            response_id=str(day),
            native_market_ids=["2"],
        ),
    )


def assert_codec_and_schema(conn, path, codec, *, compare_source=False):
    for table, schema in SCHEMAS.items():
        file = Path(path) / f"{table}.parquet"
        assert conn.execute("SELECT count(*) FROM read_parquet(?)", [str(file)]).fetchone()[0]
        assert conn.execute(
            "SELECT DISTINCT compression FROM parquet_metadata(?)", [str(file)]
        ).fetchall() == [(codec,)]
        actual = conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(file)]).fetchall()
        assert tuple((row[0], row[1]) for row in actual) == schema
        if compare_source:
            assert semantic_fingerprint(
                conn, f"SELECT {projection(table)} FROM read_parquet(?)", [str(file)]
            ) == semantic_fingerprint(conn, f"SELECT {projection(table)} FROM {table}")


def published_bytes(settings):
    return {
        str(path.relative_to(settings.published_dir)): path.read_bytes()
        for path in settings.published_dir.rglob("*")
        if path.is_file()
    }


def test_nonempty_zstd_relations_preserve_schema_and_exact_source_values(settings):
    with connect_store(settings) as conn:
        apply_native(conn, "Café 東京", 1)
        release = publisher.publish(settings, conn)
        assert_codec_and_schema(conn, release["path"], "ZSTD", compare_source=True)
        assert conn.execute("SELECT title,slug,active,closed,archived FROM events").fetchall() == [
            ("Café 東京", None, None, False, True)
        ]
        assert conn.execute(
            "SELECT question,active,closed,archived,usable FROM markets"
        ).fetchall() == [(None, True, False, None, True)]
        assert conn.execute(
            "SELECT outcome_label,asset_id,position_id,clob_token_id,condition_id "
            "FROM outcomes ORDER BY outcome_index"
        ).fetchall() == [
            (label, position, position, None, None)
            for label, position in zip(("Oui", "否"), POSITIONS, strict=True)
        ]
        assert publisher.verify_current(settings, connection=conn)["valid"]
    assert publisher.verify_release(release["path"], settings=settings)["valid"]


def test_mixed_snappy_zstd_releases_survive_compressed_backup_restore_and_noop(
    settings, tmp_path, monkeypatch
):
    def old_export(conn, table, destination):
        conn.execute(
            f"COPY (SELECT {projection(table)} FROM {table}) TO ? "
            "(FORMAT PARQUET, COMPRESSION SNAPPY)",
            [str(destination)],
        )

    with connect_store(settings) as conn:
        apply_native(conn, "Café 東京", 1)
        with monkeypatch.context() as patch:
            patch.setattr(publisher, "_export", old_export)
            previous = publisher.publish(settings, conn)
        assert_codec_and_schema(conn, previous["path"], "SNAPPY", compare_source=True)
        previous_bytes = {file.name: file.read_bytes() for file in Path(previous["path"]).iterdir()}
        apply_native(conn, "Café 東京 — changed", 2)
        current = publisher.publish(settings, conn)
        assert current["generation"] == 2
        assert_codec_and_schema(conn, current["path"], "ZSTD", compare_source=True)
        assert {
            file.name: file.read_bytes() for file in Path(previous["path"]).iterdir()
        } == previous_bytes
        pointer = json.loads((settings.published_dir / "current.json").read_text())
        assert pointer["release_id"] == current["release_id"]
        assert pointer["previous_release_id"] == previous["release_id"]
        assert publisher.verify_current(settings, connection=conn)["valid"]
    assert publisher.verify_release(previous["path"], settings=settings)["valid"]
    original = published_bytes(settings)

    saved = backup.create_backup(settings)
    manifest = json.loads((Path(saved["path"]) / "backup.json").read_text())
    assert manifest["contract"] == BACKUP_CONTRACT
    assert manifest["files"]["catalogue.duckdb"]["encoding"] == "gzip"
    assert (Path(saved["path"]) / "catalogue.duckdb.gz").is_file()
    restored = CurrentSettings(tmp_path / "restored")
    assert backup.restore_backup(saved["path"], restored.root)["valid"]
    assert published_bytes(restored) == original
    with connect_store(restored) as conn:
        restored_current = restored.published_dir / "releases" / current["release_id"]
        restored_previous = restored.published_dir / "releases" / previous["release_id"]
        assert_codec_and_schema(conn, restored_current, "ZSTD", compare_source=True)
        assert_codec_and_schema(conn, restored_previous, "SNAPPY")
        assert (
            publisher.verify_current(restored, connection=conn)["release_id"]
            == current["release_id"]
        )
        result = publisher.publish(restored, conn)
        assert result["noop"] is True
        assert result["release_id"] == current["release_id"]
    assert published_bytes(restored) == original
