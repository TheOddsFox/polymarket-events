"""Current ZSTD snapshots preserve exact values and coexist with older codecs."""

from pathlib import Path

import pytest

from fakes.world import make_event, make_market
from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings, connect_store, validate_root
from oddsfox_catalogue.current_contract import SCHEMAS, projection
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
