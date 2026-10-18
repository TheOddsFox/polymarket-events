"""Persisted acquisition units must fail closed and restart only within bounds."""

from __future__ import annotations

import copy
from dataclasses import replace
from types import SimpleNamespace

import duckdb
import pytest

from oddsfox_catalogue import current_keyset, current_store, current_sync
from oddsfox_catalogue.current_config import CurrentLimits, CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.gamma.http import CaptureStopped, CursorExpired, ResponseTooLarge


@pytest.fixture
def pending_page(tmp_path):
    settings = CurrentSettings(
        tmp_path / "catalogue",
        limits=replace(CurrentLimits(), free_reserve_bytes=1, publication_reserve_s=1),
    )
    validate_root(settings.root, initialize=True)
    certificate = {
        "version": 3,
        "policy": current_sync.POLICY,
        "origin": settings.gamma.base_url,
        "lanes": {lane: {"supported": True, "passes": [None]} for lane in ("events", "markets")},
        "capabilities": {"keyset_discovery": {"supported": True}},
    }
    progress = {
        "highwater": 1000,
        "cursor": "frozen-cursor",
        "last_id": 2,
        "page_limit": 100,
        "done": False,
    }
    state = {
        "version": 1,
        "next_lane": 0,
        "lanes": {
            "events": {"cycle_started": 1, "passes": {"all": progress}},
            "markets": {
                "cycle_started": 1,
                "passes": {key: copy.deepcopy(progress) for key in ("open", "closed")},
            },
        },
    }

    class Interrupted:
        @staticmethod
        def get(*_args):
            raise CaptureStopped("freeze before capture")

    with duckdb.connect(":memory:") as conn:
        current_store.initialize(conn)
        current_store.set_state(conn, "bulk_policy", certificate)
        current_store.set_state(conn, "keyset_discovery", state)
        with pytest.raises(CaptureStopped):
            current_keyset.cold_page(
                settings,
                conn,
                Interrupted(),
                "events",
                reconcile_state=None,
                now=1,
                cycle_started=1,
            )
        yield settings, conn


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("limit", True),
        ("limit", 0),
        ("limit", 101),
        ("ascending", 1),
        ("after_cursor", ""),
        ("after_cursor", "other-cursor"),
        ("order", "updatedAt"),
        ("closed", "any"),
        ("offset", 0),
    ],
)
def test_pending_page_parameter_tamper_is_rejected_before_http(pending_page, field, value):
    settings, conn = pending_page
    pending = current_store.get_state(conn, "keyset_pending")
    pending["request"]["params"][field] = value
    current_store.set_state(conn, "keyset_pending", pending)
    before_generation = current_store.get_state(conn, "generation")

    class MustNotFetch:
        @staticmethod
        def get(*_args):
            pytest.fail("tampered persisted request reached HTTP")

    with pytest.raises(CurrentError, match="keyset"):
        current_keyset.resume_pending(settings, conn, MustNotFetch())
    assert current_store.get_state(conn, "keyset_pending") == pending
    assert current_store.get_state(conn, "generation") == before_generation


def test_origin_change_rejects_pending_page_before_network_or_state_change(pending_page):
    settings, conn = pending_page
    pending = current_store.get_state(conn, "keyset_pending")
    changed = replace(settings, gamma=replace(settings.gamma, base_url="https://different.example"))

    class MustNotFetch:
        @staticmethod
        def get(*_args):
            pytest.fail("certificate from another origin reached HTTP")

    with pytest.raises(CurrentError, match="keyset"):
        current_keyset.resume_pending(changed, conn, MustNotFetch())
    assert current_store.get_state(conn, "keyset_pending") == pending


def test_cursor_expiry_restarts_only_once_per_client_invocation(pending_page):
    settings, conn = pending_page
    requests = []

    class Expired:
        @staticmethod
        def get(endpoint, params):
            requests.append((endpoint, copy.deepcopy(params)))
            raise CursorExpired("synthetic expired cursor")

    client = Expired()
    assert current_keyset.resume_pending(settings, conn, client)["cursor_restart"] is True
    progress = current_store.get_state(conn, "keyset_discovery")["lanes"]["events"]["passes"]["all"]
    assert (progress["cursor"], progress["last_id"], progress["highwater"]) == (None, 0, 1000)
    assert current_store.get_state(conn, "keyset_pending") is None
    with pytest.raises(CurrentError, match="expired twice"):
        current_keyset.cold_page(
            settings, conn, client, "events", reconcile_state=None, now=1, cycle_started=1
        )
    assert requests[0][1]["after_cursor"] == "frozen-cursor"
    assert "after_cursor" not in requests[1][1]
    assert current_store.get_state(conn, "keyset_pending") is not None


def test_oversized_page_restarts_from_beginning_with_smaller_limit(pending_page):
    settings, conn = pending_page
    requests = []

    class Oversized:
        @staticmethod
        def get(endpoint, params):
            requests.append((endpoint, copy.deepcopy(params)))
            raise ResponseTooLarge("synthetic oversized page")

    client = Oversized()
    assert current_keyset.resume_pending(settings, conn, client)["page_restart"] is True
    progress = current_store.get_state(conn, "keyset_discovery")["lanes"]["events"]["passes"]["all"]
    assert (progress["page_limit"], progress["cursor"], progress["last_id"]) == (50, None, 0)
    assert (
        current_keyset.cold_page(
            settings, conn, client, "events", reconcile_state=None, now=1, cycle_started=1
        )["page_restart"]
        is True
    )
    assert requests[0][1]["limit"] == 100
    assert requests[1][1]["limit"] == 50
    assert "after_cursor" not in requests[1][1]


def test_single_oversized_record_remains_pending_and_visible(pending_page):
    settings, conn = pending_page
    pending = current_store.get_state(conn, "keyset_pending")
    pending["request"]["params"]["limit"] = 1
    state = current_store.get_state(conn, "keyset_discovery")
    state["lanes"]["events"]["passes"]["all"]["page_limit"] = 1
    current_store.set_state(conn, "keyset_pending", pending)
    current_store.set_state(conn, "keyset_discovery", state)

    class Oversized:
        @staticmethod
        def get(*_args):
            raise ResponseTooLarge("single oversized record")

    with pytest.raises(ResponseTooLarge, match="single oversized"):
        current_keyset.resume_pending(settings, conn, Oversized())
    assert current_store.get_state(conn, "keyset_pending") == pending
    assert current_store.get_state(conn, "keyset_discovery") == state


@pytest.mark.parametrize(
    "rows",
    [
        [{"id": "4", "markets": []}, {"id": "4", "markets": []}],
        [{"id": "5", "markets": []}, {"id": "4", "markets": []}],
        [{"id": "1001", "markets": []}, {"id": "999", "markets": []}],
    ],
)
def test_ids_must_increase_even_outside_the_frozen_highwater(rows):
    response = SimpleNamespace(status=200, json={"events": rows, "next_cursor": None})
    with pytest.raises(CurrentError, match="strictly increasing"):
        current_keyset._parse(
            response, "events", after="old", previous_id=2, highwater=1000, closed=None
        )
