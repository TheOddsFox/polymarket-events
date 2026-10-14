"""The canonical worker owns exclusion through database-closed filesystem work."""

import json
import multiprocessing
import os
import signal
from contextlib import suppress
from functools import partial

import pytest

from oddsfox_catalogue import cli
from oddsfox_catalogue import current_backup as backup
from oddsfox_catalogue import current_runtime as runtime
from oddsfox_catalogue.current_config import CurrentSettings, validate_root
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.runlock import RunBusy, run_lock
from oddsfox_catalogue.signals import Terminated

_REAL_CHILD = runtime._child


def _worker(
    pipe,
    settings,
    operation,
    options,
    deadline,
    *,
    phase=None,
    release=None,
    done=None,
    ignore_term=False,
    fail=False,
):
    if ignore_term:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        with pytest.MonkeyPatch.context() as patch:
            if phase is None:
                patch.setattr(runtime, "execute", lambda *_: {"operation": operation})
            else:
                # Keep real backup dispatch and pruning order without SQL or provider work.
                patch.setattr(backup, "create_backup", lambda *_, **__: {"created": True})

                def prune(*args, **kwargs):
                    phase.send(os.getpid())
                    assert release.poll(30), "test did not release the database-closed phase"
                    assert release.recv() == "continue"
                    if fail:
                        raise CurrentError("injected retention failure")
                    (settings.root / "pruned").write_text("finished")
                    return {"pruned": True}

                patch.setattr(backup, "prune", prune)
            # A killed supervisor cannot receive the completed orphan's result.
            with suppress(BrokenPipeError):
                _REAL_CHILD(pipe, settings, operation, options, deadline)
    finally:
        if done is not None:
            with suppress(BrokenPipeError, OSError):
                done.send(True)
            done.close()


def _cli_parent(root, phase, release, done):
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(backup.tempfile, "gettempdir", lambda: str(root.parent))
        patch.setattr(runtime, "_child", partial(_worker, phase=phase, release=release, done=done))
        cli.main(["--root", str(root), "backup", "create"])


@pytest.fixture
def settings(tmp_path, monkeypatch):
    monkeypatch.setattr(backup.tempfile, "gettempdir", lambda: str(tmp_path))
    settings = CurrentSettings(tmp_path / "catalogue")
    validate_root(settings.root, initialize=True)
    return settings


@pytest.mark.parametrize(
    "command",
    [
        ["init"],
        ["refresh"],
        ["reconcile"],
        ["publish"],
        ["status"],
        ["verify"],
        ["current"],
        ["backup", "create"],
        ["import-release", "source", "--manifest-sha256", "a" * 64, "--raw-root", "raw"],
        [
            "probe",
            "--open-event-id",
            "1",
            "--closed-event-id",
            "2",
            "--absent-event-id",
            "3",
            "--open-market-id",
            "4",
            "--closed-market-id",
            "5",
            "--absent-market-id",
            "6",
        ],
    ],
    ids=lambda command: "-".join(command[:2]) if command[0] == "backup" else command[0],
)
def test_every_canonical_command_reports_busy_before_execution(
    settings, monkeypatch, capsys, command
):
    monkeypatch.setattr(runtime, "_child", _worker)
    with run_lock(settings.run_lock_path):
        assert cli.main(["--root", str(settings.root), *command]) == 3
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "busy"
    assert str(settings.run_lock_path) in result["message"]
    assert result["http_attempts"] == result["downloaded_bytes"] == 0
    assert not settings.store_path.exists()


def test_init_stamps_a_fresh_root_and_worker_can_acquire_its_lock(tmp_path, monkeypatch, capsys):
    root = tmp_path / "fresh"
    monkeypatch.setattr(runtime, "_child", _worker)
    assert cli.main(["--root", str(root), "init"]) == 0
    assert json.loads(capsys.readouterr().out) == {"operation": "init"}
    assert validate_root(root) == root
    with run_lock(CurrentSettings(root).run_lock_path):
        pass


@pytest.mark.parametrize("operation", ["verify", "restore"])
@pytest.mark.parametrize("root_exists", [False, True], ids=["uninitialized", "busy-root"])
def test_standalone_recovery_does_not_require_the_canonical_lock(
    tmp_path, settings, monkeypatch, capsys, operation, root_exists
):
    root = settings.root if root_exists else tmp_path / "not-initialized"
    command = ["backup", operation, str(tmp_path / "backup")]
    if operation == "restore":
        command += ["--destination", str(tmp_path / "restored")]
    monkeypatch.setattr(runtime, "_child", _worker)
    with run_lock(settings.run_lock_path):
        assert cli.main(["--root", str(root), *command]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "operation": "verify-backup" if operation == "verify" else "restore"
    }
    if not root_exists:
        assert not root.exists()


@pytest.mark.parametrize("worker_end", ["finish", "kill"])
def test_parent_sigkill_does_not_unlock_a_live_database_closed_worker(
    settings, monkeypatch, capsys, worker_end
):
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    release_receive, release_send = context.Pipe(duplex=False)
    done_receive, done_send = context.Pipe(duplex=False)
    parent = context.Process(
        target=_cli_parent, args=(settings.root, send, release_receive, done_send)
    )
    worker_pid = None
    worker_killed = False
    worker_finished = False
    monkeypatch.setattr(runtime, "_child", _worker)
    parent.start()
    send.close()
    release_receive.close()
    done_send.close()
    try:
        assert receive.poll(10), "worker never reached database-closed retention"
        worker_pid = receive.recv()
        assert worker_pid != parent.pid
        assert not settings.store_path.exists()
        parent.kill()
        parent.join(10)
        assert parent.exitcode == -signal.SIGKILL
        assert not done_receive.poll()
        assert cli.main(["--root", str(settings.root), "status"]) == 3
        result = json.loads(capsys.readouterr().out)
        assert result["status"] == "busy"
        assert result["http_attempts"] == result["downloaded_bytes"] == 0
        assert not (settings.root / "pruned").exists()
        if worker_end == "finish":
            release_send.send("continue")
            assert done_receive.poll(10), "orphan failed to finish after supervisor death"
            assert done_receive.recv() is True
            worker_finished = True
            assert (settings.root / "pruned").read_text() == "finished"
        else:
            os.kill(worker_pid, signal.SIGKILL)
            worker_killed = True
        with run_lock(settings.run_lock_path, timeout=5):
            pass
        assert cli.main(["--root", str(settings.root), "status"]) == 0
        assert json.loads(capsys.readouterr().out) == {"operation": "status"}
    finally:
        with suppress(BrokenPipeError, OSError):
            release_send.send("continue")
        if parent.is_alive():
            parent.kill()
        parent.join(10)
        if (
            worker_pid is not None
            and not worker_killed
            and not worker_finished
            and not done_receive.poll(5)
        ):
            with suppress(ProcessLookupError):
                os.kill(worker_pid, signal.SIGKILL)
        receive.close()
        release_send.close()
        done_receive.close()


@pytest.mark.parametrize("failure", ["timeout", "cancel", "kill", "error"])
def test_supervisor_releases_lock_only_after_stopping_worker(settings, monkeypatch, failure):
    context = multiprocessing.get_context("spawn")
    phase_receive, phase_send = context.Pipe(duplex=False)
    release_receive, release_send = context.Pipe(duplex=False)
    children = []
    if failure == "error":
        release_send.send("continue")

    class Receiver:
        def __init__(self, receive):
            self.receive = receive

        def poll(self, timeout):
            assert phase_receive.poll(10), "worker never reached retention"
            assert phase_receive.recv() == children[0].pid
            if failure == "error":
                return self.receive.poll(10)
            with pytest.raises(RunBusy), run_lock(settings.run_lock_path):
                pass
            if failure == "cancel":
                raise Terminated(signal.SIGTERM)
            return False

        def recv(self):
            return self.receive.recv()

        def close(self):
            self.receive.close()

    class Context:
        @staticmethod
        def Pipe(**kwargs):
            receive, send = context.Pipe(**kwargs)
            return Receiver(receive), send

        @staticmethod
        def Process(**kwargs):
            process = context.Process(**kwargs)
            children.append(process)
            return process

    monkeypatch.setattr(
        runtime,
        "_child",
        partial(
            _worker,
            phase=phase_send,
            release=release_receive,
            ignore_term=failure == "kill",
            fail=failure == "error",
        ),
    )
    monkeypatch.setattr(runtime.multiprocessing, "get_context", lambda _: Context())
    try:
        if failure == "error":
            success, result = runtime.supervise(settings, "backup", {}, duration=30)
            assert not success
            assert result["status"] == "blocked"
            assert result["message"] == "injected retention failure"
        else:
            with pytest.raises(Terminated if failure == "cancel" else CurrentError):
                runtime.supervise(settings, "backup", {}, duration=30)
        assert len(children) == 1
        assert not children[0].is_alive()
        assert children[0].exitcode == (
            0 if failure == "error" else -(signal.SIGKILL if failure == "kill" else signal.SIGTERM)
        )
        with run_lock(settings.run_lock_path):
            assert not children[0].is_alive()
    finally:
        with suppress(BrokenPipeError, OSError):
            release_send.send("continue")
        for child in children:
            if child.is_alive():
                child.kill()
            child.join(10)
        phase_send.close()
        phase_receive.close()
        release_send.close()
        release_receive.close()
