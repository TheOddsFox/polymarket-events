"""The canonical worker owns exclusion until its own process exits."""

import json
import multiprocessing
import os
import signal
from contextlib import suppress
from functools import partial

import pytest

from oddsfox_catalogue import cli
from oddsfox_catalogue import current_runtime as runtime
from oddsfox_catalogue.current_config import CurrentSettings, validate_root
from oddsfox_catalogue.runlock import run_lock

_REAL_CHILD = runtime._child


def _worker(pipe, settings, operation, options, deadline, *, phase=None, release=None, done=None):
    try:
        with pytest.MonkeyPatch.context() as patch:
            if phase is None:
                patch.setattr(runtime, "execute", lambda *_: {"operation": operation})
            else:

                def held(*_):
                    # The lock is held here; the supervisor may die without releasing it.
                    phase.send(os.getpid())
                    assert release.poll(30), "test did not release the locked phase"
                    assert release.recv() == "continue"
                    (settings.root / "released").write_text("finished")
                    return {"operation": operation}

                patch.setattr(runtime, "execute", held)
            with suppress(BrokenPipeError):
                _REAL_CHILD(pipe, settings, operation, options, deadline)
    finally:
        if done is not None:
            with suppress(BrokenPipeError, OSError):
                done.send(True)
            done.close()


def _cli_parent(root, phase, release, done):
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(runtime, "_child", partial(_worker, phase=phase, release=release, done=done))
        cli.main(["--root", str(root), "refresh"])


@pytest.fixture
def settings(tmp_path):
    settings = CurrentSettings(tmp_path / "catalogue")
    validate_root(settings.root, initialize=True)
    return settings


def test_init_stamps_a_fresh_root_and_worker_can_acquire_its_lock(tmp_path, monkeypatch, capsys):
    root = tmp_path / "fresh"
    monkeypatch.setattr(runtime, "_child", _worker)
    assert cli.main(["--root", str(root), "init"]) == 0
    assert json.loads(capsys.readouterr().out) == {"operation": "init"}
    assert validate_root(root) == root
    with run_lock(CurrentSettings(root).run_lock_path):
        pass


@pytest.mark.parametrize("worker_end", ["finish", "kill"])
def test_parent_sigkill_does_not_unlock_a_live_worker(settings, monkeypatch, capsys, worker_end):
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
        assert receive.poll(10), "worker never reached the locked phase"
        worker_pid = receive.recv()
        assert worker_pid != parent.pid
        parent.kill()
        parent.join(10)
        assert parent.exitcode == -signal.SIGKILL
        assert not done_receive.poll()
        assert cli.main(["--root", str(settings.root), "status"]) == 3
        result = json.loads(capsys.readouterr().out)
        assert result["status"] == "busy"
        assert result["http_attempts"] == result["downloaded_bytes"] == 0
        assert not (settings.root / "released").exists()
        if worker_end == "finish":
            release_send.send("continue")
            assert done_receive.poll(10), "orphan failed to finish after supervisor death"
            assert done_receive.recv() is True
            worker_finished = True
            assert (settings.root / "released").read_text() == "finished"
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
