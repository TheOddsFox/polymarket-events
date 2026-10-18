"""Regression for a failed optional capability reprobe replacing a good certificate."""

from __future__ import annotations

import json
import signal
import time
from dataclasses import replace

import duckdb  # noqa: F401 - validates that the campaign environment has the runtime dependency
import pytest

from oddsfox_catalogue import current_keyset, current_runtime, current_store, current_sync
from oddsfox_catalogue.current_config import (
    CurrentLimits,
    CurrentSettings,
    connect_store,
    validate_root,
)
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.gamma.http import RetriesExhausted
from oddsfox_catalogue.signals import Terminated


def test_optional_503_keeps_existing_supported_v3_certificate(tmp_path, monkeypatch):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    prior = {
        "version": 3,
        "policy": current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        "capabilities": {
            "market_closed_any": {
                "supported": True,
                "controls": ["11", "12", "13"],
                "lifecycle_coverage": "known-controls-only",
            },
            "keyset_discovery": {"supported": False, "error": "not requested"},
        },
    }
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        current_store.set_state(conn, "bulk_policy", prior)
    prior_wire = json.dumps(prior, sort_keys=True, separators=(",", ":"))

    def successful_base_probe(*_args, **_kwargs):
        return {
            "version": 2,
            "policy": current_sync.POLICY,
            "origin": settings.gamma.base_url,
            "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        }

    class Stats:
        requests = 1
        downloaded_bytes = 0

        @staticmethod
        def as_dict():
            return {}

    class OptionalProbe503:
        def __init__(self, *_args, **_kwargs):
            self.stats = Stats()

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def get(*_args):
            raise RetriesExhausted("synthetic transient 503")

    monkeypatch.setattr(current_sync, "probe_policy", successful_base_probe)
    monkeypatch.setattr(current_runtime, "GammaClient", OptionalProbe503)

    current_runtime.execute(
        settings,
        "probe",
        {
            "api_optimizations": True,
            "market_closed_any": True,
            "keyset_discovery": False,
            "event_ids": {"open": ["1"], "closed": ["2"], "absent": ["3"]},
            "market_ids": {"open": ["11"], "closed": ["12"], "absent": ["13"]},
        },
        time.monotonic() + 30,
    )

    with connect_store(settings) as conn:
        stored = current_store.get_state(conn, "bulk_policy")
        assert current_store.get_state(conn, "_acquisition_block") is None
        diagnostics = current_store.get_state(conn, "bulk_policy_probe_diagnostics")

    assert json.dumps(stored, sort_keys=True, separators=(",", ":")) == prior_wire
    assert diagnostics["working_certificate_preserved"] is True
    assert diagnostics["optional_capabilities"]["market_closed_any"]["supported"] is False
    assert diagnostics["optional_capabilities"]["market_closed_any"]["error"]


@pytest.mark.parametrize("changed_proof", ["origin", "lane"])
def test_optional_failure_does_not_retain_capability_across_proof_change(
    tmp_path, monkeypatch, changed_proof
):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    prior = {
        "version": 3,
        "policy": current_sync.POLICY,
        "origin": (
            settings.gamma.base_url + "/previous-origin"
            if changed_proof == "origin"
            else settings.gamma.base_url
        ),
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        "capabilities": {
            "market_closed_any": {"supported": True},
        },
    }
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        current_store.set_state(conn, "bulk_policy", prior)

    def changed_base_probe(*_args, **_kwargs):
        result = {
            "version": 2,
            "policy": current_sync.POLICY,
            "origin": settings.gamma.base_url,
            "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        }
        if changed_proof == "lane":
            result["lanes"]["events"]["controls"] = ["1", "2", "3"]
        return result

    class FailedOptional:
        class Stats:
            requests = 1
            downloaded_bytes = 0

            @staticmethod
            def as_dict():
                return {}

        stats = Stats()

        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def get(*_args):
            raise RetriesExhausted("synthetic transient 503")

    monkeypatch.setattr(current_sync, "probe_policy", changed_base_probe)
    monkeypatch.setattr(current_runtime, "GammaClient", FailedOptional)
    current_runtime.execute(
        settings,
        "probe",
        {
            "api_optimizations": True,
            "market_closed_any": True,
            "keyset_discovery": False,
            "event_ids": {"open": ["1"], "closed": ["2"], "absent": ["3"]},
            "market_ids": {"open": ["11"], "closed": ["12"], "absent": ["13"]},
        },
        time.monotonic() + 30,
    )

    with connect_store(settings) as conn:
        stored = current_store.get_state(conn, "bulk_policy")
    assert stored["version"] == 3
    if changed_proof == "origin":
        assert stored["origin"] == settings.gamma.base_url
        assert stored["capabilities"]["market_closed_any"]["supported"] is False
    else:
        assert stored == prior


def test_failed_optional_reprobe_blocks_semantically_invalidated_lanes(tmp_path, monkeypatch):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    prior = {
        "version": 3,
        "policy": current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        "capabilities": {"market_closed_any": {"supported": True}},
    }
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        current_store.set_state(conn, "bulk_policy", prior)

    def changed_base_probe(*_args, **_kwargs):
        return {
            "version": 2,
            "policy": current_sync.POLICY,
            "origin": settings.gamma.base_url,
            "lanes": {
                "events": {"supported": True, "passes": [False, True]},
                "markets": {"supported": True, "passes": [None]},
            },
        }

    class FailedOptional:
        class Stats:
            requests = 1
            downloaded_bytes = 0

            @staticmethod
            def as_dict():
                return {}

        stats = Stats()

        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @staticmethod
        def get(*_args):
            raise RetriesExhausted("synthetic transient 503")

    monkeypatch.setattr(current_sync, "probe_policy", changed_base_probe)
    monkeypatch.setattr(current_runtime, "GammaClient", FailedOptional)
    current_runtime.execute(
        settings,
        "probe",
        {
            "api_optimizations": True,
            "market_closed_any": True,
            "keyset_discovery": False,
            "event_ids": {"open": ["1"], "closed": ["2"], "absent": ["3"]},
            "market_ids": {"open": ["11"], "closed": ["12"], "absent": ["13"]},
        },
        time.monotonic() + 30,
    )

    class MustNotFetch:
        def get(self, *_args, **_kwargs):
            pytest.fail("acquisition started while the preserved certificate was fenced")

    from datetime import UTC, datetime

    with connect_store(settings) as conn:
        assert current_store.get_state(conn, "bulk_policy") == prior
        block = current_store.get_state(conn, "_acquisition_block")
        assert block == {"origin": settings.gamma.base_url, "certificate": prior}
        for native in (False, True):
            with pytest.raises(CurrentError, match="acquisition is blocked"):
                current_sync.fetch_apply(
                    settings, conn, MustNotFetch(), "events", [1], native=native
                )
        with pytest.raises(CurrentError, match="acquisition is blocked"):
            current_sync.discover(settings, conn, MustNotFetch(), deadline=time.monotonic() + 30)
        with pytest.raises(CurrentError, match="acquisition is blocked"):
            current_keyset.cold_page(
                settings,
                conn,
                MustNotFetch(),
                "events",
                reconcile_state={"windows": {"events": {"started_at": 1.0}}},
                now=datetime.now(UTC),
            )


def test_legacy_probe_downgrade_clears_only_keyset_metadata(tmp_path, monkeypatch):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    prior = {
        "version": 3,
        "policy": current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        "capabilities": {"keyset_discovery": {"supported": True}},
    }
    discovery = {"lanes": {"events": {"frontier": 8}, "markets": {"frontier": 12}}}
    event = {
        "id": "8",
        "active": True,
        "closed": False,
        "archived": False,
        "markets": [],
    }
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        current_store.set_state(conn, "bulk_policy", prior)
        current_store.set_state(conn, "keyset_discovery", {"cursor": "settled"})
        current_store.set_state(conn, "keyset_bootstrap_complete", True)
        current_store.set_state(conn, "discovery", discovery)
        current_store.set_state(
            conn, "_acquisition_block", {"origin": settings.gamma.base_url, "certificate": prior}
        )
        current_store.apply(
            conn,
            project_response(
                [event],
                "events",
                received_at="2026-01-01T00:00:00+00:00",
                response_id="a" * 32,
            ),
        )

    def legacy_probe(*_args, **_kwargs):
        return {
            **current_sync.POLICY,
            "origin": settings.gamma.base_url,
            "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        }

    class Client:
        class Stats:
            requests = 0
            downloaded_bytes = 0

            @staticmethod
            def as_dict():
                return {}

        stats = Stats()

        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(current_sync, "probe_policy", legacy_probe)
    monkeypatch.setattr(current_runtime, "GammaClient", Client)
    current_runtime.execute(settings, "probe", {}, time.monotonic() + 30)

    with connect_store(settings) as conn:
        assert current_store.get_state(conn, "keyset_discovery") is None
        assert current_store.get_state(conn, "keyset_bootstrap_complete") is None
        assert current_store.get_state(conn, "_acquisition_block") is None
        assert current_store.get_state(conn, "discovery") == discovery
        assert conn.execute("SELECT event_id FROM events").fetchall() == [("8",)]


def test_fully_successful_optional_probe_clears_acquisition_block(tmp_path, monkeypatch):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    prior = {
        "version": 3,
        "policy": current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        "capabilities": {},
    }
    successful = {
        **prior,
        "capabilities": {
            "market_closed_any": {"supported": True},
            "keyset_discovery": {"supported": True},
        },
    }
    expected_certificate = json.loads(json.dumps(successful))
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        current_store.set_state(conn, "bulk_policy", prior)
        current_store.set_state(
            conn, "_acquisition_block", {"origin": settings.gamma.base_url, "certificate": prior}
        )

    class Client:
        class Stats:
            requests = 0
            downloaded_bytes = 0

            @staticmethod
            def as_dict():
                return {}

        stats = Stats()

        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(current_runtime, "GammaClient", Client)
    monkeypatch.setattr(
        current_sync, "probe_api_optimizations", lambda *_args, **_kwargs: successful
    )
    current_runtime.execute(
        settings,
        "probe",
        {
            "api_optimizations": True,
            "market_closed_any": True,
            "keyset_discovery": True,
        },
        time.monotonic() + 30,
    )

    with connect_store(settings) as conn:
        assert current_store.get_state(conn, "bulk_policy") == expected_certificate
        assert current_store.get_state(conn, "_acquisition_block") is None


def test_partial_optional_reprobe_cannot_bypass_existing_acquisition_block(tmp_path, monkeypatch):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    prior = {
        "version": 3,
        "policy": current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        "capabilities": {
            "market_closed_any": {"supported": True},
            "keyset_discovery": {"supported": False, "error": "prior transient failure"},
        },
    }
    block = {"origin": settings.gamma.base_url, "certificate": prior}
    partial = {
        **prior,
        "capabilities": {
            "market_closed_any": {"supported": True},
            "keyset_discovery": {"supported": False, "error": "new transient failure"},
        },
    }
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        current_store.set_state(conn, "bulk_policy", prior)
        current_store.set_state(conn, "_acquisition_block", block)

    class Client:
        class Stats:
            requests = 0
            downloaded_bytes = 0

            @staticmethod
            def as_dict():
                return {}

        stats = Stats()

        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(current_runtime, "GammaClient", Client)
    monkeypatch.setattr(current_sync, "probe_api_optimizations", lambda *_args, **_kwargs: partial)
    current_runtime.execute(
        settings,
        "probe",
        {
            "api_optimizations": True,
            "market_closed_any": True,
            "keyset_discovery": True,
        },
        time.monotonic() + 30,
    )

    class MustNotFetch:
        def get(self, *_args, **_kwargs):
            pytest.fail("partial reprobe bypassed the existing acquisition block")

    with connect_store(settings) as conn:
        assert current_store.get_state(conn, "bulk_policy") == prior
        assert current_store.get_state(conn, "_acquisition_block") == block
        for kind, native in (("markets", False), ("events", True)):
            with pytest.raises(CurrentError, match="acquisition is blocked"):
                current_sync.fetch_apply(settings, conn, MustNotFetch(), kind, [11], native=native)


@pytest.mark.parametrize("failure_type", [OSError, Terminated])
def test_probe_state_activation_rolls_back_on_late_write_failure(
    tmp_path, monkeypatch, failure_type
):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    prior = {
        "version": 3,
        "policy": current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
        "capabilities": {"keyset_discovery": {"supported": True}},
    }
    discovery = {"lanes": {"events": {"frontier": 8}, "markets": {"frontier": 12}}}
    diagnostics = {"prior": "diagnostics"}
    with connect_store(settings) as conn:
        current_store.initialize(conn)
        current_store.set_state(conn, "bulk_policy", prior)
        current_store.set_state(conn, "keyset_discovery", {"cursor": "settled"})
        current_store.set_state(conn, "keyset_bootstrap_complete", True)
        current_store.set_state(conn, "discovery", discovery)
        current_store.set_state(
            conn, "_acquisition_block", {"origin": settings.gamma.base_url, "certificate": prior}
        )
        current_store.set_state(conn, "bulk_policy_probe_diagnostics", diagnostics)
        generation = current_store.generation(conn)

    replacement = {
        **current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in current_sync.LANES},
    }
    monkeypatch.setattr(current_sync, "probe_policy", lambda *_args, **_kwargs: replacement)

    class Client:
        class Stats:
            requests = 0
            downloaded_bytes = 0

            @staticmethod
            def as_dict():
                return {}

        stats = Stats()

        def __init__(self, *_args, **_kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    original_set_state = current_runtime.set_state
    failure = (
        Terminated(signal.SIGTERM)
        if failure_type is Terminated
        else OSError("injected later state write failure")
    )

    def fail_after_bootstrap_clear(conn, key, value):
        original_set_state(conn, key, value)
        if key == "keyset_bootstrap_complete":
            raise failure

    monkeypatch.setattr(current_runtime, "GammaClient", Client)
    monkeypatch.setattr(current_runtime, "set_state", fail_after_bootstrap_clear)
    with pytest.raises(failure_type) as raised:
        current_runtime.execute(settings, "probe", {}, time.monotonic() + 30)
    assert raised.value is failure

    with connect_store(settings) as conn:
        assert current_store.get_state(conn, "bulk_policy") == prior
        assert current_store.get_state(conn, "keyset_discovery") == {"cursor": "settled"}
        assert current_store.get_state(conn, "keyset_bootstrap_complete") is True
        assert current_store.get_state(conn, "discovery") == discovery
        assert current_store.get_state(conn, "_acquisition_block") == {
            "origin": settings.gamma.base_url,
            "certificate": prior,
        }
        assert current_store.get_state(conn, "bulk_policy_probe_diagnostics") == diagnostics
        assert current_store.generation(conn) == generation
