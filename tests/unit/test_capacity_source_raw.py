"""Raw-receipt fidelity and fail-closed behavior for unusable capacity markets."""

import json
import shutil
from urllib.error import HTTPError
from urllib.request import urlopen

import duckdb
import pytest

from fakes.capacity_source import CapacitySource
from fakes.world import make_event, make_market
from oddsfox_catalogue.capture.writer import write_page
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.current_store import apply, initialize

STAMP = "2026-01-01T00:00:13Z"
RAW_IDS = {"13": "a" * 32, "14": "b" * 32}


def _unusable_market(market_id, title, outcomes):
    if outcomes:
        market = make_market(
            market_id,
            title,
            version="v1",
            outcomes=tuple(outcomes),
            prices=tuple(0.5 for _ in outcomes),
        )
    else:
        market = make_market(market_id, title, version="v1")
        market.update(
            outcomes="[]", outcomePrices="[]", lastTradePrice=None, bestBid=None, bestAsk=None
        )
    market.pop("clobTokenIds")
    market.pop("positionIds")
    return market


def _write_native_capture(root, conn, market, received_at):
    market_id = str(market["id"])
    response_id = RAW_IDS[market_id]
    body = json.dumps(market, separators=(",", ":")).encode()
    write_page(
        root / "raw" / response_id,
        1,
        body,
        {
            "request": {"endpoint": f"/markets/{market_id}", "params": {}},
            "received_at": received_at,
            "status": 200,
        },
    )
    apply(
        conn,
        project_response([market], "markets", received_at=received_at, response_id=response_id),
    )


@pytest.fixture
def raw_seed(tmp_path):
    root = tmp_path / "seed"
    root.mkdir()
    home_away = _unusable_market("13", "Home or away", ("Home Team", "Away Team"))
    home_away["id"] = 13  # Native Gamma IDs may be JSON integers.
    no_outcomes = _unusable_market("14", "Unresolved outcome", ())
    events = [
        make_event(
            "1", "Unusable markets", markets=[make_market("10", "Open"), home_away, no_outcomes]
        ),
        make_event("2", "Closed", closed=True, markets=[make_market("11", "Closed", closed=True)]),
        make_event("3", "Shared unusable market", markets=[home_away]),
    ]
    conn = duckdb.connect(str(root / "catalogue.duckdb"))
    initialize(conn)
    apply(
        conn,
        project_response(
            events, "events", received_at="2026-01-01T00:00:00Z", response_id="event-seed"
        ),
    )
    _write_native_capture(root, conn, home_away, STAMP)
    _write_native_capture(root, conn, no_outcomes, "2026-01-01T00:00:14Z")
    conn.close()
    return root, {"13": home_away, "14": no_outcomes}


def _get(source, path):
    try:
        with urlopen(source.url + path, timeout=10) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.load(exc)


def test_native_bulk_keyset_and_event_views_preserve_raw_unusable_markets(raw_seed, tmp_path):
    root, expected = raw_seed
    with duckdb.connect(str(root / "catalogue.duckdb"), read_only=True) as conn:
        assert conn.execute("SELECT usable FROM markets WHERE market_id='13'").fetchone() == (
            False,
        )
        assert conn.execute("SELECT usable FROM markets WHERE market_id='14'").fetchone() == (
            False,
        )
        assert conn.execute("SELECT count(*) FROM outcomes WHERE market_id='13'").fetchone() == (0,)
        assert conn.execute("SELECT count(*) FROM outcomes WHERE market_id='14'").fetchone() == (0,)

    with CapacitySource(root, tmp_path / "audit", delay_s=0) as source:
        native = {key: _get(source, f"/markets/{key}")[1] for key in ("13", "14")}
        status, bulk = _get(source, "/markets?id=13&id=14&limit=2")
        assert status == 200
        status, keyset = _get(source, "/markets/keyset?limit=2&order=id&ascending=true&id=13&id=14")
        assert status == 200
        bulk_by_id = {str(record["id"]): record for record in bulk}
        keyset_by_id = {str(record["id"]): record for record in keyset["markets"]}
        assert bulk_by_id == keyset_by_id == native

        home_away = native["13"]
        assert home_away["id"] == 13
        assert home_away == {**expected["13"], "events": home_away["events"]}
        assert json.loads(home_away["outcomes"]) == ["Home Team", "Away Team"]
        assert "clobTokenIds" not in home_away
        assert "positionIds" not in home_away
        assert native["14"] == {**expected["14"], "events": native["14"]["events"]}
        assert json.loads(native["14"]["outcomes"]) == []
        assert "clobTokenIds" not in native["14"]
        assert "positionIds" not in native["14"]

        status, event = _get(source, "/events/1")
        assert status == 200
        nested = {str(market["id"]): market for market in event["markets"]}
        assert set(nested) == {"10", "13", "14"}
        assert nested["13"] == native["13"]
        assert nested["14"] == native["14"]
        assert {parent["id"] for parent in native["13"]["events"]} == {"1", "3"}
        assert {parent["id"] for parent in native["14"]["events"]} == {"1"}

    audit = [json.loads(line) for line in (tmp_path / "audit").read_text().splitlines()]
    event_request = next(row for row in audit if row["kind"] == "events")
    assert set(map(str, event_request["complete_memberships"]["1"])) == {"10", "13", "14"}


def _damage_capture(root, fault, tmp_path):
    response_id = RAW_IDS["13"]
    raw_dir = root / "raw" / response_id
    manifest_path = raw_dir / "p000001.manifest.json"
    if fault == "missing_receipt":
        with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
            conn.execute("DELETE FROM _receipts WHERE kind='market' AND id='13'")
    elif fault == "missing_capture":
        shutil.rmtree(raw_dir)
    elif fault == "corrupt_body":
        (raw_dir / "p000001.json.gz").write_bytes(b"not a valid gzip body")
    elif fault == "wrong_endpoint":
        manifest = json.loads(manifest_path.read_text())
        manifest["request"]["endpoint"] = "/events/13"
        manifest_path.write_text(json.dumps(manifest))
    elif fault in {"wrong_id", "boolean_id", "noncanonical_string_id"}:
        wrong = _unusable_market("99", "Wrong market", ("Home Team", "Away Team"))
        wrong["id"] = {
            "wrong_id": "99",
            "boolean_id": True,
            "noncanonical_string_id": "013",
        }[fault]
        write_page(
            raw_dir,
            1,
            json.dumps(wrong, separators=(",", ":")).encode(),
            {
                "request": {"endpoint": "/markets/13", "params": {}},
                "received_at": STAMP,
                "status": 200,
            },
        )
    elif fault == "unsafe_reference":
        with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
            conn.execute(
                "UPDATE _receipts SET response='../outside' WHERE kind='market' AND id='13'"
            )
    elif fault == "symlinked_raw_path":
        outside = tmp_path / "detached-raw"
        raw_dir.rename(outside)
        raw_dir.symlink_to(outside, target_is_directory=True)
    else:
        raise AssertionError(f"unknown fault: {fault}")


@pytest.mark.parametrize(
    "fault",
    [
        "missing_receipt",
        "missing_capture",
        "corrupt_body",
        "wrong_endpoint",
        "wrong_id",
        "boolean_id",
        "noncanonical_string_id",
        "unsafe_reference",
        "symlinked_raw_path",
    ],
)
def test_unusable_market_requires_a_verified_native_receipt(raw_seed, tmp_path, fault):
    root, _expected = raw_seed
    _damage_capture(root, fault, tmp_path)
    with (
        CapacitySource(root, tmp_path / "audit", delay_s=0) as source,
        pytest.raises(RuntimeError, match="retained native market"),
    ):
        source.records("markets", ["13"])


def test_invalid_nested_receipt_cannot_return_partial_event_membership(raw_seed, tmp_path):
    root, _expected = raw_seed
    _damage_capture(root, "wrong_endpoint", tmp_path)
    with CapacitySource(root, tmp_path / "audit", delay_s=0) as source:
        status, payload = _get(source, "/events/1")
        assert status == 500
        assert payload == {"error": "source failed"}
        assert source.failure == "source read failed"

    audit = [json.loads(line) for line in (tmp_path / "audit").read_text().splitlines()]
    assert audit[0]["status"] == 500
    assert audit[0]["emitted_ids"] == []
    assert audit[0]["complete_memberships"] == {}


def test_enable_faults_injects_one_bounded_503_and_retry_is_audited(raw_seed, tmp_path):
    root, _expected = raw_seed
    with CapacitySource(root, tmp_path / "audit", delay_s=0) as source:
        status, original = _get(source, "/markets/13")
        assert status == 200

        source.enable_faults()
        assert _get(source, "/markets/13") == (503, {"error": "injected failure"})
        assert _get(source, "/markets/13") == (200, original)
        assert source.stats["requests"] == 3
        assert source.stats["http_attempts"] == 3
        assert source.stats["faults"] == 1

    audit = [json.loads(line) for line in (tmp_path / "audit").read_text().splitlines()]
    assert [row["request"] for row in audit] == [1, 2, 3]
    assert [row["status"] for row in audit] == [200, 503, 200]
    assert [row["emitted_ids"] for row in audit] == [[13], [], [13]]
