"""One supervised invocation; no historical orchestrator or build engine."""

from __future__ import annotations

import multiprocessing
import os
import time
from contextlib import contextmanager
from datetime import UTC, datetime

from .current_config import admit, connect_store, publication_reserve
from .current_contract import CurrentError
from .current_store import get_state, initialize, set_state, summary
from .gamma.http import CaptureStopped, GammaClient, RequestBudgetExceeded
from .platform_support import WindowsJob, peak_rss_bytes, pinned_path, require_durability
from .runlock import RunBusy, run_lock
from .signals import SIGNALS, Terminated


def _publishing(settings, conn, operation, options):
    """A full pass publishes on completion or once the cadence interval has elapsed."""
    if operation != "reconcile" or not options.get("full_pass"):
        return True
    last = get_state(conn, "last_publish_at")
    if last is None:
        return True
    age = (datetime.now(UTC) - datetime.fromisoformat(last)).total_seconds()
    return age >= settings.limits.full_pass_publish_interval_s


def _publish(settings, conn, deadline):
    from .current_publish import publish

    started = time.monotonic()
    result = publish(settings, conn, deadline=deadline)
    elapsed = time.monotonic() - started
    # A cheap no-op must not erase the measured cost of exporting a changed store.
    if not result.get("noop") or get_state(conn, "previous_publish_seconds") is None:
        set_state(conn, "previous_publish_seconds", elapsed)
    set_state(conn, "last_publish_at", datetime.now(UTC).isoformat())
    return result, elapsed


def _persist_probe_result(conn, result, options, probe_options):
    """Atomically activate a proven certificate and its associated state changes."""
    from .current_sync import semantic_lane_proofs

    conn.execute("BEGIN TRANSACTION")
    try:
        previous_certificate = get_state(conn, "bulk_policy", {})
        failed_capabilities = result.get("capabilities", {})
        previous_capabilities = previous_certificate.get("capabilities", {})
        requested_capabilities = [
            name
            for name, option in (
                ("market_closed_any", "market_closed_any"),
                ("keyset_discovery", "keyset_discovery"),
            )
            if probe_options.get(option, True)
        ]
        failed_requested_capability = options.get("api_optimizations", False) and any(
            failed_capabilities.get(name, {}).get("supported") is not True
            for name in requested_capabilities
        )
        existing_block = get_state(conn, "_acquisition_block")
        block_matches_certificate = (
            isinstance(existing_block, dict)
            and existing_block.get("origin") == result.get("origin")
            and existing_block.get("certificate") == previous_certificate
            and previous_certificate.get("policy") == result.get("policy")
        )
        preserve_supported_certificate = (
            previous_certificate.get("version") == 3
            and previous_certificate.get("origin") == result.get("origin")
            and previous_certificate.get("policy") == result.get("policy")
            and any(
                previous_capabilities.get(name, {}).get("supported") is True
                and capability.get("supported") is False
                for name, capability in failed_capabilities.items()
            )
        )
        preserve_blocked_certificate = block_matches_certificate and failed_requested_capability
        preserve_working_certificate = (
            preserve_supported_certificate or preserve_blocked_certificate
        )
        semantic_lane_change = semantic_lane_proofs(
            previous_certificate.get("lanes", {})
        ) != semantic_lane_proofs(result.get("lanes", {}))
        if preserve_working_certificate:
            if semantic_lane_change:
                set_state(
                    conn,
                    "_acquisition_block",
                    {"origin": result["origin"], "certificate": previous_certificate},
                )
            set_state(
                conn,
                "bulk_policy_probe_diagnostics",
                {
                    "at": datetime.now(UTC).isoformat(),
                    "optional_capabilities": failed_capabilities,
                    "working_certificate_preserved": True,
                },
            )
            result = previous_certificate
        else:
            set_state(conn, "bulk_policy", result)
        if result.get("version") != 3 and previous_certificate.get("version") == 3:
            set_state(conn, "keyset_discovery", None)
            set_state(conn, "keyset_bootstrap_complete", None)
        if not preserve_working_certificate:
            optional_probe_succeeded = (
                options.get("api_optimizations", False)
                and bool(requested_capabilities)
                and all(
                    result.get("capabilities", {}).get(name, {}).get("supported") is True
                    for name in requested_capabilities
                )
            )
            if not options.get("api_optimizations", False) or optional_probe_succeeded:
                set_state(conn, "_acquisition_block", None)
            set_state(
                conn,
                "bulk_policy_probe_diagnostics",
                {
                    "at": datetime.now(UTC).isoformat(),
                    "optional_capabilities": result.get("capabilities", {}),
                },
            )
        conn.execute("COMMIT")
        return result
    except BaseException as original:
        try:
            conn.execute("ROLLBACK")
        except BaseException as rollback_error:
            original.add_note(f"probe-state rollback also failed: {rollback_error}")
        raise


def execute(settings, operation, options, deadline):
    if os.name != "nt":
        return _execute(settings, operation, options, deadline)
    with _windows_root_guard(settings, operation):
        return _execute(settings, operation, options, deadline)


@contextmanager
def _windows_root_guard(settings, operation):
    # Opening a missing/stamped root may create DuckDB state even for commands
    # that are nominally read-only. Require the supported volume durability
    # contract for every native Windows invocation until those paths are truly
    # side-effect free.
    require_durability(settings.root)
    with pinned_path(settings.root):
        yield


def _execute(settings, operation, options, deadline):
    from .current_publish import verify_current

    started = time.monotonic()
    with connect_store(settings) as conn:
        initialize(conn)
        if operation == "init":
            return {"status": "initialized", **summary(conn)}
        if operation == "status":
            from .current_reconcile import freshness, full_pass_status

            return {
                "status": "ready",
                **summary(conn),
                **freshness(conn),
                "full_pass": full_pass_status(conn),
                "discovery": get_state(conn, "discovery"),
                "last_run": get_state(conn, "last_run"),
            }
        if operation in {"verify", "current"}:
            return verify_current(settings, connection=conn)
        if operation == "publish":
            return _publish(settings, conn, deadline)[0]
        from .current_sync import (
            _pending_work,
            discover,
            probe_api_optimizations,
            probe_policy,
            resume_pending,
        )

        publishing = _publishing(settings, conn, operation, options)
        reserve = publication_reserve(settings, conn)
        acquisition_deadline = deadline if operation == "probe" else deadline - reserve
        with GammaClient(
            settings.gamma,
            max_body_bytes=settings.limits.max_response_bytes,
            max_requests=settings.limits.max_requests,
            max_download_bytes=settings.limits.max_download_bytes,
            max_duration_s=settings.limits.max_duration_s,
            deadline=acquisition_deadline,
        ) as client:
            try:
                if operation == "probe":
                    try:
                        probe_options = {
                            key: value
                            for key, value in options.items()
                            if key != "api_optimizations"
                        }
                        if options.get("api_optimizations", False):
                            result = probe_api_optimizations(
                                settings, conn, client, **probe_options
                            )
                        else:
                            if _pending_work(conn):
                                raise CurrentError(
                                    "cannot replace acquisition certificate while work is pending"
                                )
                            result = probe_policy(settings, client, **probe_options)
                    except Exception as exc:
                        set_state(
                            conn,
                            "bulk_policy_probe_diagnostics",
                            {"at": datetime.now(UTC).isoformat(), "error": str(exc)},
                        )
                        raise
                    if not all(lane.get("supported") for lane in result["lanes"].values()):
                        set_state(conn, "bulk_policy_probe_diagnostics", result)
                        raise CurrentError(
                            "provider policy probe failed; inspect recorded lane results"
                        )
                    result = _persist_probe_result(conn, result, options, probe_options)
                else:
                    try:
                        resume_pending(settings, conn, client)
                        if operation == "refresh":
                            result = discover(settings, conn, client, deadline=deadline)
                            from .current_reconcile import repair_pending

                            result["parent_repairs"] = repair_pending(
                                settings, conn, client, deadline=deadline
                            )
                        elif operation == "reconcile":
                            from .current_reconcile import reconcile, start_full_pass

                            if options.get("full_pass"):
                                before_completed = (get_state(conn, "full_pass") or {}).get(
                                    "completed_at"
                                )
                                start_full_pass(conn)
                            result = reconcile(settings, conn, client, deadline=deadline)
                        else:
                            raise CurrentError("unknown current catalogue operation")
                    except (RequestBudgetExceeded, CaptureStopped):
                        result = {
                            "pending": True,
                            "source_complete": False,
                            "budget_exhausted": True,
                        }
                    acquired = time.monotonic()
                    completed = (get_state(conn, "full_pass") or {}).get("completed_at")
                    just_completed = options.get("full_pass") and completed not in (
                        None,
                        before_completed if options.get("full_pass") else None,
                    )
                    if publishing or just_completed:
                        settings.meter.snapshot(settings.root)
                        published, publication_seconds = _publish(settings, conn, deadline)
                        result.update(
                            status="published",
                            published=True,
                            publication=published,
                            timings={
                                "acquire_apply_s": acquired - started,
                                "publish_verify_s": publication_seconds,
                            },
                        )
                    else:
                        result.update(
                            status="deferred",
                            published=False,
                            timings={"acquire_apply_s": acquired - started},
                        )
                result.update(
                    http_attempts=client.stats.requests,
                    downloaded_bytes=client.stats.downloaded_bytes,
                    requests=client.stats.as_dict(),
                    elapsed_s=time.monotonic() - started,
                )
            except Exception as exc:
                exc.http_attempts = client.stats.requests
                exc.downloaded_bytes = client.stats.downloaded_bytes
                raise
        if operation in {"refresh", "reconcile"}:
            result["parent_event_repair_count"] = conn.execute(
                "SELECT count(*) FROM _parent_repairs"
            ).fetchone()[0]
        set_state(
            conn,
            "last_run",
            {"at": datetime.now(UTC).isoformat(), "operation": operation, **result},
        )
        from .current_reconcile import freshness, full_pass_status

        result["freshness"] = freshness(conn)
        result["full_pass"] = full_pass_status(conn)
        result["peak_rss_bytes"] = peak_rss_bytes()
        settings.meter.snapshot(settings.root)
        result["storage"] = admit(settings)
    return result


def _child(pipe, settings, operation, options, deadline):
    try:
        # The worker can outlive its parent, including while no database is open.
        guard = _windows_root_guard(settings, operation) if os.name == "nt" else None
        if guard is None:
            with run_lock(settings.run_lock_path):
                result = execute(settings, operation, options, deadline)
        else:
            with guard, run_lock(settings.run_lock_path):
                result = _execute(settings, operation, options, deadline)
        pipe.send((True, result))
    except Exception as exc:
        pipe.send(
            (
                False,
                {
                    "status": "busy" if isinstance(exc, RunBusy) else "blocked",
                    "message": str(exc),
                    "http_attempts": getattr(exc, "http_attempts", 0),
                    "downloaded_bytes": getattr(exc, "downloaded_bytes", 0),
                },
            )
        )
    finally:
        pipe.close()


def _child_after_gate(pipe, settings, operation, options, deadline, gate):
    try:
        try:
            if gate.recv() is not True:
                return
        except EOFError:
            return
        finally:
            gate.close()
        _child(pipe, settings, operation, options, deadline)
    finally:
        gate.close()


def supervise(settings, operation, options, *, duration):
    """The worker owns the writer lock; the parent drains it before spill cleanup."""
    if os.name == "nt":
        return _supervise_windows(settings, operation, options, duration)
    context = multiprocessing.get_context("spawn")
    deadline = time.monotonic() + duration
    receive, send = context.Pipe(duplex=False)
    process = context.Process(target=_child, args=(send, settings, operation, options, deadline))
    try:
        with SIGNALS.hold() as started:
            process.start()
        send.close()
        if started.signum is not None:
            raise Terminated(started.signum)
        if not receive.poll(max(0, deadline - time.monotonic())):
            raise CurrentError(
                "invocation deadline exhausted; verify the current pointer before retrying"
            )
        try:
            success, result = receive.recv()
        except EOFError as exc:
            raise CurrentError("worker stopped; committed progress can be resumed") from exc
        process.join(timeout=max(0, deadline - time.monotonic()))
        if process.is_alive():
            raise CurrentError("worker did not finish within the invocation deadline")
        return success, result
    finally:
        with SIGNALS.hold() as stopped:
            if process.is_alive():
                process.terminate()
                process.join(timeout=2)
            if process.is_alive():
                process.kill()
                process.join()
            send.close()
            receive.close()
        if stopped.signum is not None:
            raise Terminated(stopped.signum)


def _supervise_windows(settings, operation, options, duration):
    """Gate spawned work until it is contained; the gate also fails closed on parent death."""
    context = multiprocessing.get_context("spawn")
    deadline = time.monotonic() + duration
    receive, send = context.Pipe(duplex=False)
    gate_receive, gate_send = context.Pipe(duplex=False)
    process = context.Process(
        target=_child_after_gate,
        args=(send, settings, operation, options, deadline, gate_receive),
    )
    started_process = False
    assigned = False
    with WindowsJob() as job:
        try:
            with SIGNALS.hold() as started:
                process.start()
                started_process = True
                send.close()
                gate_receive.close()
                job.assign(process.sentinel)
                assigned = True
                if SIGNALS.pending is not None:
                    raise Terminated(SIGNALS.pending)
                gate_send.send(True)
                gate_send.close()
            if started.signum is not None:
                raise Terminated(started.signum)
            if not receive.poll(max(0, deadline - time.monotonic())):
                raise CurrentError(
                    "invocation deadline exhausted; verify the current pointer before retrying"
                )
            try:
                success, result = receive.recv()
            except EOFError as exc:
                raise CurrentError("worker stopped; committed progress can be resumed") from exc
            process.join(timeout=max(0, deadline - time.monotonic()))
            if process.is_alive():
                raise CurrentError("worker did not finish within the invocation deadline")
            return success, result
        finally:
            with SIGNALS.hold() as stopped:
                gate_send.close()
                gate_receive.close()
                if started_process:
                    try:
                        if assigned:
                            job.terminate()
                    finally:
                        process.join(timeout=2)
                        if process.is_alive():
                            if assigned:
                                raise CurrentError(
                                    "contained worker did not stop after Job termination"
                                )
                            process.terminate()
                            process.join(timeout=2)
                            if process.is_alive():
                                raise CurrentError("gated worker did not exit after gate closure")
                send.close()
                receive.close()
            if stopped.signum is not None:
                raise Terminated(stopped.signum)
