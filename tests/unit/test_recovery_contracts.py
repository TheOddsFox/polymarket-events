"""Adversarial full-pass recovery contracts across committed and pending work."""

from datetime import timedelta

import httpx
import pytest
from test_current_full_pass import NOW, event, market, run, seed
from test_current_full_pass import setup as setup

from oddsfox_catalogue import current_reconcile as reconcile
from oddsfox_catalogue import current_store
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.gamma.http import CaptureStopped


def test_large_id_checkpoint_resumes_exact_remaining_batches_after_hundred_commits(setup):
    settings, conn = setup
    ids = [str(value) for value in range(2**63 - 50, 2**63 + 50)]
    ids += [str(value) for value in range(2**64 - 50, 2**64 + 50)]
    ids += [str(10**20 - value) for value in range(5, 0, -1)]
    seed(conn, [event(key, closed=True) for key in reversed(ids)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)

    def interrupted(request):
        batch = request.url.params.get_list("id")
        if batch != ids[:100]:
            assert batch == ids[100:200]
            raise CaptureStopped()
        return httpx.Response(200, json=[event(key, closed=True) for key in batch])

    run(settings, conn, interrupted)
    assert current_store.get_state(conn, "full_pass")["cursor_after"] == int(ids[99])
    assert reconcile.full_pass_status(conn, now=NOW)["remaining"] == 105

    resumed = []

    def finish(request):
        batch = request.url.params.get_list("id")
        resumed.append(batch)
        return httpx.Response(200, json=[event(key, closed=True) for key in batch])

    run(settings, conn, finish)
    assert resumed == [ids[100:200], ids[200:]]
    assert current_store.get_state(conn, "full_pass")["cursor_after"] == int(ids[-1])
    status = reconcile.full_pass_status(conn, now=NOW)
    assert status["remaining"] == 0
    assert status["completed_at"] is not None


@pytest.mark.parametrize("native_status", [200, 404])
def test_recovery_omission_interruption_preserves_legacy_cursor_until_native_settlement(
    setup, native_status
):
    settings, conn = setup
    key, saved_cursor = str(2**64), 10**20 - 1
    seed(conn, [event(key, closed=True)], age=3600)
    reconcile.start_full_pass(conn, now=NOW)
    state = current_store.get_state(conn, "full_pass")
    state["cursor_after"] = saved_cursor
    current_store.set_state(conn, "full_pass", state)
    interrupted_paths = []

    def interrupted(request):
        interrupted_paths.append(request.url.path)
        assert current_store.get_state(conn, "full_pass")["cursor_after"] == saved_cursor
        if request.url.path == f"/events/{key}":
            raise CaptureStopped()
        assert request.url.params.get_list("id") == [key]
        return httpx.Response(200, json=[])

    run(settings, conn, interrupted)
    assert interrupted_paths == ["/events", f"/events/{key}"]
    assert current_store.get_state(conn, "full_pass")["cursor_after"] == saved_cursor
    assert reconcile.full_pass_status(conn, now=NOW)["remaining"] == 1
    assert current_store.get_state(conn, "fetch_pending") is not None
    resumed_paths = []

    def finish(request):
        resumed_paths.append(request.url.path)
        payload = event(key, closed=True) if native_status == 200 else {}
        return httpx.Response(native_status, json=payload)

    run(settings, conn, finish)
    assert resumed_paths == [f"/events/{key}"]
    assert current_store.get_state(conn, "fetch_pending") is None
    assert reconcile.full_pass_status(conn, now=NOW)["completed_at"] is not None


def test_successful_incomplete_recovery_retries_only_on_later_invocation(setup):
    settings, conn = setup
    incomplete = event(1, closed=True)
    incomplete.pop("markets")
    seed(conn, [incomplete], age=3600)
    current_store.apply(
        conn,
        project_response(
            [market(10)],
            "markets",
            received_at=(NOW - timedelta(hours=1)).isoformat(),
            response_id="orphan",
        ),
    )
    reconcile.start_full_pass(conn, now=NOW)
    state = current_store.get_state(conn, "full_pass")
    state["cursor_after"] = 100
    current_store.set_state(conn, "full_pass", state)
    first_paths = []

    def incomplete_then_stop(request):
        first_paths.append(request.url.path)
        if request.url.path == "/markets":
            raise CaptureStopped()
        assert request.url.params.get_list("id") == ["1"]
        return httpx.Response(200, json=[incomplete])

    run(settings, conn, incomplete_then_stop)
    assert first_paths == ["/events", "/markets"]
    status = reconcile.full_pass_status(conn, now=NOW)
    assert status["remaining"] == 2
    assert status["unresolved"] == 1
    assert status["completed_at"] is None
    assert current_store.get_state(conn, "full_pass")["cursor_after"] == 1
    resumed_paths = []

    def finish(request):
        resumed_paths.append(request.url.path)
        payload = [event(1, closed=True)] if request.url.path == "/events" else [market(10)]
        return httpx.Response(200, json=payload)

    run(settings, conn, finish)
    assert resumed_paths == ["/markets", "/events"]
    status = reconcile.full_pass_status(conn, now=NOW)
    assert status["remaining"] == 0
    assert status["unresolved"] == 0
    assert status["completed_at"] is not None
