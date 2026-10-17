"""One supervised invocation; no historical orchestrator or build engine."""

from __future__ import annotations

import multiprocessing
import resource
import sys
import time
from datetime import UTC, datetime

from .current_config import admit, connect_store, publication_reserve
from .current_contract import CurrentError
from .current_store import get_state, initialize, set_state, summary
from .gamma.http import CaptureStopped, GammaClient, RequestBudgetExceeded
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


def execute(settings, operation, options, deadline):
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
        from .current_sync import discover, probe_policy, resume_pending

        publishing = _publishing(settings, conn, operation, options)
        reserve = publication_reserve(settings, conn, publishing=publishing)
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
                    result = probe_policy(settings, client, **options)
                    set_state(conn, "bulk_policy", result)
                    if not all(lane.get("supported") for lane in result["lanes"].values()):
                        raise CurrentError(
                            "provider policy probe failed; inspect recorded lane results"
                        )
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
                            result = reconcile(
                                settings, conn, client, deadline=deadline, publishing=publishing
                            )
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
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result["peak_rss_bytes"] = peak if sys.platform == "darwin" else peak * 1024
        settings.meter.snapshot(settings.root)
        result["storage"] = admit(settings)
    return result


def _child(pipe, settings, operation, options, deadline):
    try:
        # The worker can outlive its parent, including while no database is open.
        with run_lock(settings.run_lock_path):
            result = execute(settings, operation, options, deadline)
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


def supervise(settings, operation, options, *, duration):
    """The worker owns the writer lock; the parent drains it before spill cleanup."""
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
