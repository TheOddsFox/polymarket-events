"""Independent, offline acceptance checks for membership import evidence."""

import importlib.util
from pathlib import Path

import duckdb
import pytest
from test_current_import import add_event_page, release_fixture

from oddsfox_catalogue.current_config import CurrentSettings

SPEC = importlib.util.spec_from_file_location(
    "current_import_oracle", Path(__file__).parents[2] / "scripts/verify-current-import.py"
)
oracle = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(oracle)


@pytest.mark.parametrize(
    "record",
    [
        {},
        {"id": "01", "markets": []},
        {"id": "1", "markets": None},
        {"id": "1", "markets": [None]},
        {"id": "1", "markets": [{"id": True}]},
        {"id": "1", "markets": [{"id": 2.0}]},
        {"id": False, "markets": []},
        {"id": 0, "markets": []},
        {"id": -1, "markets": []},
        {"id": "1", "markets": [{"id": "02"}]},
        *(
            {"id": "1", "markets": [], **flag}
            for flag in [
                {"hasMore": True},
                {"markets_truncated": True},
                {"nextCursor": "next"},
                {"marketCount": 1},
                {"total": False},
                {"pagination": {"next": 2}},
            ]
        ),
    ],
)
def test_incomplete_is_not_absence(record):
    assert oracle.complete_members(record) is None


def page(day, identity, events):
    return {"observed_at": f"2026-10-{day}T00:00:00Z", "page_id": identity}, events


def test_integer_and_string_ids_share_exact_membership(tmp_path):
    conn = duckdb.connect()
    conn.execute("CREATE TABLE event_markets (venue VARCHAR,event_id VARCHAR,market_id VARCHAR)")
    conn.execute("INSERT INTO event_markets VALUES ('polymarket','1','8')")
    bridge = tmp_path / "bridge.parquet"
    conn.execute("COPY event_markets TO ? (FORMAT PARQUET)", [str(bridge)])
    report = oracle.membership_report(
        conn,
        iter(
            [
                page("11", "a", [{"id": "1", "markets": [{"id": "9"}]}]),
                page("12", "b", [{"id": 1, "markets": [{"id": 8}, {"id": "8"}]}]),
            ]
        ),
        bridge,
    )
    assert report["membership_difference_count"] == 0
    assert report["events_with_complete_membership"] == 1
    assert report["expected"]["rows"] == 1
    assert oracle.complete_members({"id": 1, "markets": [{"id": 2}, {"id": "2"}]}) == ["2"]
    conn.close()


def test_latest_complete_empty_ties_and_old_bridge_union(tmp_path):
    conn = duckdb.connect()
    conn.execute("CREATE TABLE event_markets (venue VARCHAR,event_id VARCHAR,market_id VARCHAR)")
    conn.execute("INSERT INTO event_markets VALUES ('polymarket','1','9')")
    bridge = tmp_path / "bridge.parquet"
    conn.execute("COPY event_markets TO ? (FORMAT PARQUET)", [str(bridge)])
    pages = [
        page("11", "a", [{"id": "1", "markets": [{"id": "9"}]}]),
        page("12", "b", [{"id": "1", "markets": [{"id": "8"}]}]),
        page("13", "c", [{"id": "1", "markets": None}]),
        page("12", "z", [{"id": "1", "markets": []}]),
        page("11", "unlinked", [{"id": "2", "markets": []}]),
    ]
    report = oracle.membership_report(conn, iter(pages), bridge)
    assert report["source_event_page_count"] == 5
    assert report["events_with_complete_membership"] == 2
    assert report["expected"]["rows"] == 0
    assert report["actual"]["rows"] == 1
    assert report["membership_difference_count"] == 1
    assert report["old_bridge_difference_count"] == 1
    conn.close()


def test_newer_incomplete_preserves_last_complete(tmp_path):
    conn = duckdb.connect()
    conn.execute("CREATE TABLE event_markets (venue VARCHAR,event_id VARCHAR,market_id VARCHAR)")
    conn.execute("INSERT INTO event_markets VALUES ('polymarket','1','8')")
    bridge = tmp_path / "bridge.parquet"
    conn.execute("COPY event_markets TO ? (FORMAT PARQUET)", [str(bridge)])
    report = oracle.membership_report(
        conn,
        iter(
            [
                page("11", "a", [{"id": "1", "markets": [{"id": "9"}]}]),
                page("12", "b", [{"id": "1", "markets": [{"id": "8"}, {"id": "8"}]}]),
                page("13", "c", [{"id": "1", "markets": [{"id": "7"}], "hasMore": True}]),
            ]
        ),
        bridge,
    )
    assert report["membership_difference_count"] == 0
    assert report["expected"]["rows"] == 1
    conn.close()


def test_raw_is_verified_and_decoded_once(tmp_path, monkeypatch):
    release, _, conn = release_fixture(tmp_path)
    conn.close()
    raw_root = tmp_path / "raw"
    add_event_page(release, raw_root, [{"id": "1", "markets": []}])
    manifest, _ = oracle._read_json(release / "release.json", release, oracle.MANIFEST_LIMIT)
    original = oracle.read_page_records
    calls = []

    def counted(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(oracle, "read_page_records", counted)
    pages = list(
        oracle.event_pages(CurrentSettings(tmp_path / "current"), release, manifest, raw_root)
    )
    assert pages[0][1] == [{"id": "1", "markets": []}]
    assert calls == [1]
    next(raw_root.rglob("*.json.gz")).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="size|checksum"):
        list(oracle.event_pages(CurrentSettings(tmp_path / "current"), release, manifest, raw_root))
    assert calls == [1]


def test_output_cannot_replace_input_or_existing_file(tmp_path):
    existing = tmp_path / "report.json"
    existing.write_text("retain")
    with pytest.raises(ValueError, match="fresh"):
        oracle.run(tmp_path / "current", tmp_path / "release", "0" * 64, tmp_path / "raw", existing)
    assert existing.read_text() == "retain"
    with pytest.raises(ValueError, match="outside"):
        oracle.run(
            tmp_path / "current",
            tmp_path / "release",
            "0" * 64,
            tmp_path / "raw",
            tmp_path / "raw" / "report.json",
        )
