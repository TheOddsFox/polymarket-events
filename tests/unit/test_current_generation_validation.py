"""Persisted generations are nonnegative integers, never JSON booleans."""

from dataclasses import replace

import pytest

from oddsfox_catalogue import current_publish as publisher
from oddsfox_catalogue import current_store as store
from oddsfox_catalogue.current_config import (
    CurrentLimits,
    CurrentSettings,
    connect_store,
    validate_root,
)
from oddsfox_catalogue.current_contract import CurrentError


@pytest.fixture
def catalogue(tmp_path):
    settings = CurrentSettings(
        tmp_path / "catalogue", limits=replace(CurrentLimits(), free_reserve_bytes=1)
    )
    validate_root(settings.root, initialize=True)
    with connect_store(settings) as conn:
        store.initialize(conn)
        publisher.publish(settings, conn)
        yield settings, conn


@pytest.mark.parametrize("value", [True, False, 0.0, 1.0, "0", "1", -1, None])
@pytest.mark.parametrize("operation", ["validate", "publish"])
def test_invalid_generation_is_rejected_without_mutation(catalogue, value, operation):
    settings, conn = catalogue
    pointer = settings.published_dir / "current.json"
    original_pointer = pointer.read_bytes()
    original_files = {
        path.relative_to(settings.published_dir): path.read_bytes()
        for path in settings.published_dir.rglob("*")
        if path.is_file()
    }
    store.set_state(conn, "generation", value)
    before = conn.execute("SELECT * FROM sync_state ORDER BY ALL").fetchall()
    with pytest.raises(CurrentError, match="invalid generation"):
        if operation == "validate":
            store.validate_store(conn)
        else:
            publisher.publish(settings, conn)
    assert conn.execute("SELECT * FROM sync_state ORDER BY ALL").fetchall() == before
    assert pointer.read_bytes() == original_pointer
    assert {
        name: (settings.published_dir / name).read_bytes() for name in original_files
    } == original_files


@pytest.mark.parametrize("value", [0, 1])
def test_nonnegative_integer_generation_remains_valid(catalogue, value):
    settings, conn = catalogue
    store.set_state(conn, "generation", value)
    assert store.validate_store(conn) == {"valid": True, "generation": value}
    result = publisher.publish(settings, conn)
    assert result["generation"] == value
    assert type(result["generation"]) is int
