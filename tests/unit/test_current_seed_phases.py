"""Private seed checkpoints survive real connection boundaries and failed retries."""

import duckdb
import pytest
from test_current_import import add_event_page, release_fixture

from oddsfox_catalogue import current_import as importer
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import CurrentSettings
from oddsfox_catalogue.current_contract import CurrentError


@pytest.fixture
def seed(tmp_path):
    release, _, fixture = release_fixture(tmp_path, usable=True)
    fixture.close()
    raw = tmp_path / "raw"
    checksum = add_event_page(release, raw, [{"id": "1", "title": "recovered", "markets": []}])
    settings = CurrentSettings(tmp_path / "current")
    database = tmp_path / "seed.duckdb"

    def step(**overrides):
        with duckdb.connect(str(database)) as conn:
            return importer.import_release(
                settings,
                conn,
                release,
                manifest_sha256=checksum,
                raw_root=overrides.get("raw_root", raw),
            )

    def state():
        with duckdb.connect(str(database)) as conn:
            return {
                "pending": store.get_state(conn, "seed_pending"),
                "seed": store.get_state(conn, "seed"),
                "generation": store.generation(conn),
                "discovery": store.get_state(conn, "discovery"),
                "title": conn.execute("SELECT title FROM events").fetchall(),
            }

    return step, state, database, release


def test_baseline_failure_leaves_durable_pristine_marker(seed, monkeypatch):
    step, state, _, _ = seed
    original = importer._identity

    def fail(conn):
        original(conn)
        raise RuntimeError("baseline injection")

    monkeypatch.setattr(importer, "_identity", fail)
    with pytest.raises(CurrentError, match="baseline injection"):
        step()
    assert state()["pending"]["stage"] == "baseline"
    assert state()["title"] == []
    assert state()["generation"] == 0
    monkeypatch.setattr(importer, "_identity", original)
    assert step() == {"complete": False, "phase": "evidence"}
    assert state()["pending"]["stage"] == "evidence"
    assert state()["title"] == [("seed",)]


def test_evidence_rollback_retry_and_verify_does_not_replay(seed, monkeypatch):
    step, state, _, _ = seed
    assert not step()["complete"]
    original = importer._event_pages

    def fail(*args):
        yield from original(*args)
        raise RuntimeError("evidence injection")

    monkeypatch.setattr(importer, "_event_pages", fail)
    with pytest.raises(CurrentError, match="evidence injection"):
        step()
    assert state()["pending"]["stage"] == "evidence"
    assert state()["title"] == [("seed",)]
    monkeypatch.setattr(importer, "_event_pages", original)
    assert step() == {"complete": False, "phase": "verify"}
    assert state()["title"] == [("seed",)]

    def no_replay(*args):
        raise AssertionError("event replay in phase C")

    monkeypatch.setattr(importer, "_event_pages", no_replay)
    result = step()
    assert result["complete"] and result["valid"]
    assert result["generation"] == 1
    final = state()
    assert final["pending"] is None
    assert final["generation"] == 1
    assert final["seed"]["frontiers"] == {"events": 3, "markets": 4}
    assert final["discovery"]["lanes"]["events"]["frontier"] == 3
    assert step()["resumed"]


def test_verify_source_change_preserves_private_checkpoint(seed):
    step, state, _, release = seed
    step()
    step()
    (release / "release.json").write_bytes((release / "release.json").read_bytes() + b" ")
    with pytest.raises(CurrentError, match="changed"):
        step()
    assert state()["pending"]["stage"] == "verify"
    assert state()["generation"] == 0
    assert state()["seed"] is state()["discovery"] is None


def test_final_seal_failure_rolls_back_all_final_progress(seed, monkeypatch):
    step, state, _, _ = seed
    step()
    step()
    original = importer._seal
    calls = 0

    def fail(*args):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("final seal injection")
        return original(*args)

    monkeypatch.setattr(importer, "_seal", fail)
    with pytest.raises(CurrentError, match="final seal injection"):
        step()
    final = state()
    assert final["pending"]["stage"] == "verify"
    assert final["generation"] == 0
    assert final["seed"] is final["discovery"] is None
    monkeypatch.setattr(importer, "_seal", original)
    assert step()["complete"]


@pytest.mark.parametrize("field,value", [("version", 3), ("stage", "unknown"), ("stage", [])])
def test_invalid_pending_marker_rejected(seed, field, value):
    step, state, database, _ = seed
    step()
    marker = state()["pending"]
    marker[field] = value
    with duckdb.connect(str(database)) as conn:
        store.set_state(conn, "seed_pending", marker)
    with pytest.raises(CurrentError, match="invalid seed_pending"):
        step()
    assert state()["generation"] == 0


def test_pending_source_parameters_must_match(seed, tmp_path):
    step, state, _, _ = seed
    step()
    with pytest.raises(CurrentError, match="source mismatch"):
        step(raw_root=tmp_path / "other-raw")
    assert state()["pending"]["stage"] == "evidence"


def test_null_pending_marker_is_not_treated_as_absent(seed):
    step, state, database, _ = seed
    step()
    with duckdb.connect(str(database)) as conn:
        store.set_state(conn, "seed_pending", None)
    with pytest.raises(CurrentError, match="invalid seed_pending"):
        step()
    assert state()["generation"] == 0
