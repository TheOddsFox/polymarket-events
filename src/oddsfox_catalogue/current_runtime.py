"""One supervised invocation; no historical orchestrator or build engine."""

from __future__ import annotations

import multiprocessing
import resource
import sys
import time
from contextlib import nullcontext
from datetime import UTC, datetime

from .current_config import admit, connect_store
from .current_contract import CurrentError
from .current_store import get_state, initialize, set_state, summary
from .gamma.http import CaptureStopped, GammaClient, RequestBudgetExceeded
from .runlock import RunBusy, run_lock
from .signals import SIGNALS, Terminated


def _publish(settings, conn, deadline):
    from .current_publish import publish

    started = time.monotonic()
    result = publish(settings, conn, deadline=deadline)
    elapsed = time.monotonic() - started
    # A cheap no-op must not erase the measured cost of exporting a changed store.
    if not result.get("noop") or get_state(conn, "previous_publish_seconds") is None:
        set_state(conn, "previous_publish_seconds", elapsed)
    return result, elapsed


def execute(settings, operation, options, deadline):
    from .current_backup import create_backup, prune, restore_backup, verify_backup
    from .current_publish import verify_current

    started = time.monotonic()
    verification_options = (
        {"_workspace": options["_verification_workspace"]}
        if "_verification_workspace" in options
        else {}
    )
    if operation == "verify-backup":
        return verify_backup(options["backup"], deadline=deadline, **verification_options)
    if operation == "restore":
        return restore_backup(
            options["backup"], options["destination"], deadline=deadline, **verification_options
        )
    if operation == "import-release":
        from .current_import import import_release

        while True:
            with connect_store(settings) as conn:
                result = import_release(settings, conn, deadline=deadline, **options)
            if result["complete"]:
                break
        with connect_store(settings) as conn:
            result["publication"], result["publish_verify_s"] = _publish(settings, conn, deadline)
        return result
    if settings.store_path.exists():
        with connect_store(settings) as guard:
            tables = {row[0] for row in guard.execute("SHOW TABLES").fetchall()}
            pending = (
                "sync_state" in tables
                and guard.execute("SELECT 1 FROM sync_state WHERE key='seed_pending'").fetchone()
            )
        if pending and operation != "status":
            raise CurrentError(
                "seed import incomplete; resume import-release with its frozen source"
            )
    if operation == "backup":
        result = create_backup(settings, deadline=deadline, **verification_options)
        result["retention"] = prune(settings, **verification_options)
        return result
    retention = None
    if operation in {"refresh", "reconcile"}:
        retention = prune(settings, raw_only=True)
    with connect_store(settings) as conn:
        initialize(conn)
        if operation == "init":
            return {"status": "initialized", **summary(conn)}
        if operation == "status":
            from .current_reconcile import freshness

            pending = get_state(conn, "seed_pending")
            result = {
                "status": "seeding"
                if conn.execute("SELECT 1 FROM sync_state WHERE key='seed_pending'").fetchone()
                else "ready",
                **summary(conn),
                **freshness(conn),
            }
            result["seed_pending"] = pending
            result["discovery"] = get_state(conn, "discovery")
            result["last_run"] = get_state(conn, "last_run")
            result["last_backup"] = get_state(conn, "last_backup")
            backup = result["last_backup"]
            age = (
                (datetime.now(UTC) - datetime.fromisoformat(backup["created_at"])).total_seconds()
                if backup
                else None
            )
            result["backup_age_s"] = age
            result["recovery_point_overdue"] = age is None or age > 86400
            result["recovery_scope"] = "local_only"
            result["disk_failure_protection"] = False
            return result
        if operation in {"verify", "current"}:
            return verify_current(settings, connection=conn)
        if operation == "publish":
            return _publish(settings, conn, deadline)[0]
        from .current_sync import discover, probe_policy, resume_pending

        reserve = max(
            settings.limits.publication_reserve_s,
            1.5 * get_state(conn, "previous_publish_seconds", 0),
        )
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
                            from .current_reconcile import reconcile

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
                    published, publication_seconds = _publish(settings, conn, deadline)
                    result.update(
                        status="published",
                        publication=published,
                        timings={
                            "acquire_apply_s": acquired - started,
                            "publish_verify_s": publication_seconds,
                        },
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
        from .current_reconcile import freshness

        result["freshness"] = freshness(conn)
        peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result["peak_rss_bytes"] = peak if sys.platform == "darwin" else peak * 1024
        result["storage"] = admit(settings)
    if retention is not None:
        result["retention"] = retention
    return result


def _child(pipe, settings, operation, options, deadline):
    try:
        # The worker can outlive its parent, including while no database is open.
        lock = (
            nullcontext()
            if operation in {"verify-backup", "restore"}
            else run_lock(settings.run_lock_path)
        )
        with lock:
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
    workspace_context = nullcontext()
    if operation in {"backup", "verify-backup", "restore"}:
        from .current_backup import _verification_workspace

        workspace_context = _verification_workspace()
    with workspace_context as workspace:
        if workspace is not None:
            options = {**options, "_verification_workspace": str(workspace)}
        receive, send = context.Pipe(duplex=False)
        process = context.Process(
            target=_child, args=(send, settings, operation, options, deadline)
        )
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
