"""Recorded corpus tests use synthetic records and loopback HTTP only."""

import hashlib
import importlib.util
import sqlite3
from pathlib import Path

import httpx
import pytest

_SPEC = importlib.util.spec_from_file_location(
    "speed_corpus", Path(__file__).resolve().parents[2] / "tools/speed_corpus.py"
)
corpus = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(corpus)
DELAYS = {"high_water": 0.001, "keyset": 0.002, "single": 0.003}
MARKS = {"events": 99999999999999999999, "markets": 12}


def _build(tmp_path, pages=None):
    path = tmp_path / "corpus.sqlite"
    manifest = corpus.build_corpus(
        pages
        if pages is not None
        else [
            (
                "events",
                [
                    {"id": "2", "closed": False, "markets": [{"id": "77", "closed": True}]},
                    {"id": "10", "closed": True},
                    {"id": "99999999999999999999", "closed": False},
                ],
            ),
            (
                "markets",
                [{"id": "1", "closed": False}, {"id": "12", "closed": True, "tags": [{"id": "5"}]}],
            ),
        ],
        path,
        high_water=MARKS,
        delays=DELAYS,
    )
    return path, manifest


def _server(path, **caps):
    return corpus.RecordedGammaServer(
        path, max_requests=caps.get("requests", 100), max_download_bytes=caps.get("bytes", 100000)
    )


def test_pinned_disk_corpus_native_and_bulk_match_without_indexing_nested_markets(tmp_path):
    path, manifest = _build(tmp_path)
    assert manifest["counts"] == {"events": 3, "markets": 2}
    assert manifest["corpus_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert manifest["ambiguous_records"] == 0
    with _server(path) as server, httpx.Client(base_url=server.url) as client:
        native = client.get("/markets/12", params={"include_tag": "true"})
        bulk = client.get(
            "/markets/keyset", params={"id": [1, 12], "closed": "true", "include_tag": "true"}
        )
        assert bulk.json()["markets"] == [native.json()]
        assert client.get("/markets/77").status_code == 404
        assert client.get("/markets/999").status_code == 404
        assert client.get("/markets/keyset", params={"id": [1, 12], "closed": "false"}).json()[
            "markets"
        ] == [{"id": "1", "closed": False}]
        accounting = server.accounting()
        assert accounting["requests"] == 5 and accounting["statuses"] == {"200": 3, "404": 2}
        assert accounting["bytes"] > 0


def test_numeric_pagination_highwater_and_query_bound_cursor(tmp_path):
    path, _ = _build(tmp_path)
    with _server(path) as server, httpx.Client(base_url=server.url) as client:
        assert client.get(
            "/events", params={"limit": 1, "order": "id", "ascending": "false"}
        ).json() == [{"id": str(MARKS["events"])}]
        params = {"limit": 1, "order": "id", "ascending": "true"}
        seen = []
        while True:
            page = client.get("/events/keyset", params=params).json()
            seen.extend(row["id"] for row in page["events"])
            cursor = page["next_cursor"]
            if cursor is None:
                break
            assert (
                client.get(
                    "/markets/keyset", params={"limit": 1, "after_cursor": cursor}
                ).status_code
                == 400
            )
            params["after_cursor"] = cursor
        assert seen == ["2", "10", "99999999999999999999"]


def test_real_route_delays_and_no_virtual_clock(tmp_path, monkeypatch):
    path, _ = _build(tmp_path)
    slept = []
    monkeypatch.setattr(corpus.time, "sleep", slept.append)
    with _server(path) as server, httpx.Client(base_url=server.url) as client:
        client.get("/events", params={"limit": 1, "order": "id", "ascending": "false"})
        client.get("/events/keyset")
        client.get("/events/2")
        client.get("/events/99")
    assert slept == [0.001, 0.002, 0.003, 0.003]


@pytest.mark.parametrize("caps", [{"requests": 1}, {"bytes": 1}])
def test_caps_stop_serving_success_and_bound_bytes(tmp_path, caps):
    path, _ = _build(tmp_path)
    with _server(path, **caps) as server, httpx.Client(base_url=server.url) as client:
        first = client.get("/events/2")
        second = client.get("/events/2")
        assert first.status_code == (200 if "requests" in caps else 429)
        assert second.status_code == 429
        assert server.accounting()["bytes"] <= caps.get("bytes", 100000)
        assert server.accounting()["errors"] >= 1


@pytest.mark.parametrize(
    "target",
    [
        "/events/01",
        "/events/-1",
        "/events/100000000000000000000",
        "/other/keyset",
        "/events/keyset?limit=101",
        "/events/keyset?id=2&id=2",
        "/events/keyset?id=",
        "/events/keyset?closed=maybe",
        "/events/keyset?unknown=x",
        "/events/keyset?limit=1&limit=2",
        "/events/keyset?after_cursor=garbage",
        "/events/keyset?ascending=false",
        "/events/keyset?" + "&".join(f"id={i}" for i in range(1, 102)),
    ],
)
def test_rejects_invalid_queries(tmp_path, target):
    path, _ = _build(tmp_path)
    with _server(path) as server, httpx.Client(base_url=server.url) as client:
        assert client.get(target).status_code == 400


def test_rejects_credentials_host_and_mutating_methods(tmp_path):
    path, _ = _build(tmp_path)
    with _server(path) as server, httpx.Client(base_url=server.url) as client:
        assert client.get("/events/2", headers={"Host": "external.test"}).status_code == 400
        assert (
            client.get("/events/2", headers={"Authorization": "Bearer synthetic"}).status_code
            == 400
        )
        assert client.post("/events/2").status_code == 405


def test_rejects_ambiguous_versions_and_never_overwrites(tmp_path):
    path = tmp_path / "ambiguous.sqlite"
    with pytest.raises(ValueError, match="ambiguity_count=1"):
        corpus.build_corpus(
            [("events", [{"id": "1", "closed": False}, {"id": "1", "closed": True}])],
            path,
            high_water=MARKS,
            delays=DELAYS,
        )
    assert not path.exists()
    path, manifest = _build(tmp_path)
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        corpus.build_corpus([], path, high_water=MARKS, delays=DELAYS)
    assert path.read_bytes() == before
    assert manifest["corpus_sha256"] == hashlib.sha256(before).hexdigest()


@pytest.mark.parametrize(
    "record",
    [
        {"id": "0", "closed": False},
        {"id": "01", "closed": False},
        {"id": True, "closed": False},
        {"id": "1", "closed": "false"},
        {"id": "1", "closed": 0},
    ],
)
def test_rejects_unsafe_record_ids_and_invalid_closed(tmp_path, record):
    with pytest.raises(ValueError):
        _build(tmp_path, [("events", [record])])


def test_symlink_paths_and_invalid_delay_policy_rejected(tmp_path):
    link = tmp_path / "link"
    link.symlink_to(tmp_path, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        corpus.build_corpus([], link / "corpus.sqlite", high_water=MARKS, delays=DELAYS)
    for bad in (0, -1, float("inf"), float("nan")):
        with pytest.raises(ValueError, match="delays"):
            corpus.build_corpus(
                [], tmp_path / "bad.sqlite", high_water=MARKS, delays={**DELAYS, "single": bad}
            )


def test_identical_repeated_record_is_deduplicated(tmp_path):
    record = {"id": "1", "closed": False}
    path, manifest = _build(tmp_path, [("events", [record]), ("events", [dict(record)])])
    assert manifest["counts"] == {"events": 1, "markets": 0}
    with _server(path) as server, httpx.Client(base_url=server.url) as client:
        assert client.get("/events/keyset").json()["events"] == [record]
        assert client.get("/events/2").status_code == 404
        assert server.accounting()["errors"] == 0


def test_oversize_stream_preserves_http_success_and_client_can_reduce_limit(tmp_path):
    from oddsfox_catalogue.config import GammaSettings
    from oddsfox_catalogue.gamma.http import GammaClient, ResponseTooLarge

    records = [{"id": str(i), "payload": "x" * (9 * 1024**2)} for i in (1, 2)]
    path, _ = _build(tmp_path, [("events", records)])
    with _server(path, bytes=100 * 1024**2) as server:
        with (
            httpx.Client(base_url=server.url) as raw,
            raw.stream("GET", "/events/keyset?limit=2") as response,
        ):
            assert response.status_code == 200
            assert int(response.headers["Content-Length"]) > 16 * 1024**2
        with GammaClient(
            GammaSettings(base_url=server.url, allow_loopback=True, max_retries=0)
        ) as client:
            with pytest.raises(ResponseTooLarge):
                client.get("/events/keyset", {"limit": 2})
            page = client.get("/events/keyset", {"limit": 1}).json
            assert page["events"] == records[:1]
            following = client.get(
                "/events/keyset", {"limit": 1, "after_cursor": page["next_cursor"]}
            ).json
            assert following["events"] == records[1:]
            assert following["next_cursor"] is None
        assert server.accounting()["statuses"] == {"200": 4}
        assert server.accounting()["errors"] == 0
    with _server(path, bytes=16 * 1024**2) as server, httpx.Client(base_url=server.url) as raw:
        assert raw.get("/events/keyset?limit=2").status_code == 429
        assert server.accounting()["bytes"] == 0
        assert server.accounting()["errors"] == 1


def test_stream_chunks_are_bounded_and_close_without_reading_remaining_records(tmp_path):
    path, _ = _build(tmp_path, [("events", [{"id": "1", "payload": "é" * 100000}])])
    server = _server(path)
    length, chunks = server._query("/events/keyset", stream=True)
    pieces = list(chunks)
    assert length == sum(map(len, pieces))
    assert max(map(len, pieces)) <= 64 * 1024
    _, chunks = server._query("/events/keyset", stream=True)
    next(chunks)
    chunks.close()
    assert list(chunks) == []


@pytest.mark.parametrize("entity", ["events", "markets"])
def test_unknown_lifecycle_records_are_preserved_only_on_unfiltered_routes(tmp_path, entity):
    records = [
        {"id": "1", "closed": None},
        {"id": "2"},
        {"id": "3", "closed": False},
        {"id": "4", "closed": True},
    ]
    path, manifest = _build(tmp_path, [(entity, records)])
    assert manifest["counts"][entity] == 4
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM records WHERE closed IS NULL").fetchone()[0] == 2
    with _server(path) as server, httpx.Client(base_url=server.url) as client:
        for record in records:
            response = client.get(f"/{entity}/{record['id']}")
            assert response.status_code == 200 and response.json() == record
        for flag, target in (("false", records[2]), ("true", records[3])):
            response = client.get(f"/{entity}/keyset", params={"id": [1, 2, 3, 4], "closed": flag})
            assert response.status_code == 200 and response.json()[entity] == [target]
        unfiltered = client.get(f"/{entity}/keyset").json()[entity]
        assert unfiltered == (records if entity == "events" else [records[2]])
        assert server.accounting()["errors"] == 0
