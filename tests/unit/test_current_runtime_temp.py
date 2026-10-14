"""The supervisor owns verification spill until its worker has stopped."""

import multiprocessing
import signal
from pathlib import Path

import pytest

from oddsfox_catalogue import current_backup as backup
from oddsfox_catalogue import current_runtime as runtime
from oddsfox_catalogue.current_contract import CurrentError
from oddsfox_catalogue.signals import Terminated


def _spill_worker(pipe, settings, operation, options, deadline):
    if settings["ignore_term"]:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
    workspace = Path(options["_verification_workspace"])
    (workspace / "database-copy").write_bytes(b"private recovery data")
    settings["ready"].set()
    if settings["complete"]:
        pipe.send((True, {"valid": True}))
        pipe.close()
        return
    while True:
        signal.pause()


@pytest.mark.parametrize("operation", ["backup", "verify-backup", "restore"])
@pytest.mark.parametrize("failure", ["success", "timeout", "cancel", "kill"])
def test_supervisor_removes_only_its_workspace_after_worker_join(
    tmp_path, monkeypatch, operation, failure
):
    temporary = tmp_path / "temporary"
    temporary.mkdir()
    unrelated = temporary / "operator-data"
    unrelated.write_bytes(b"keep")
    monkeypatch.setattr(backup.tempfile, "gettempdir", lambda: str(temporary))
    context = multiprocessing.get_context("spawn")
    ready = context.Event()
    children, workspaces, joins = [], [], []
    options = {"backup": "source", "destination": "destination"}

    class Receiver:
        def __init__(self, receive):
            self.receive = receive

        def poll(self, timeout):
            assert ready.wait(10), "worker never reached its private copy"
            assert workspaces[0].is_dir()
            if failure == "success":
                return self.receive.poll(10)
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

        class Process:
            def __init__(self, **kwargs):
                workspace = Path(kwargs["args"][3]["_verification_workspace"])
                assert workspace.is_dir(), "workspace must exist before process creation"
                workspaces.append(workspace)
                self.process = context.Process(**kwargs)
                children.append(self.process)

            def join(self, timeout=None):
                assert workspaces[0].is_dir(), "parent removed a live worker's spill"
                self.process.join(
                    timeout=0.05 if failure == "kill" and timeout is not None else timeout
                )
                joins.append((self.process.is_alive(), workspaces[0].is_dir()))

            def __getattr__(self, name):
                return getattr(self.process, name)

    monkeypatch.setattr(runtime, "_child", _spill_worker)
    monkeypatch.setattr(runtime.multiprocessing, "get_context", lambda _: Context())
    try:

        def supervise():
            return runtime.supervise(
                {
                    "ready": ready,
                    "ignore_term": failure == "kill",
                    "complete": failure == "success",
                },
                operation,
                options,
                duration=30,
            )

        if failure == "success":
            assert supervise() == (True, {"valid": True})
        else:
            with pytest.raises(Terminated if failure == "cancel" else CurrentError):
                supervise()
        assert len(children) == 1
        assert not children[0].is_alive()
        expected = (
            0
            if failure == "success"
            else -(signal.SIGKILL if failure == "kill" else signal.SIGTERM)
        )
        assert children[0].exitcode == expected
        assert joins and joins[-1] == (False, True)
        assert not workspaces[0].exists()
        assert unrelated.read_bytes() == b"keep"
        assert "_verification_workspace" not in options
    finally:
        for child in children:
            if child.is_alive():
                child.kill()
            child.join(timeout=5)


@pytest.mark.parametrize("operation", ["backup", "verify-backup", "restore"])
@pytest.mark.parametrize("inherited", [False, True], ids=["standalone", "supervised"])
def test_execute_forwards_workspace_to_every_backup_verifier(
    tmp_path, monkeypatch, operation, inherited
):
    from oddsfox_catalogue.current_config import CurrentSettings

    settings = CurrentSettings(tmp_path / "catalogue")
    options = {"backup": tmp_path / "source", "destination": tmp_path / "destination"}
    workspace = tmp_path / "private-invocation"
    if inherited:
        options["_verification_workspace"] = str(workspace)
    calls = []

    def recorder(name):
        def call(*args, **kwargs):
            calls.append((name, args, kwargs))
            return {"valid": True}

        return call

    for name in ("create_backup", "verify_backup", "restore_backup", "prune"):
        monkeypatch.setattr(backup, name, recorder(name))
    result = runtime.execute(settings, operation, options, deadline=1234)
    assert result["valid"]
    workspace_options = {"_workspace": str(workspace)} if inherited else {}
    if operation == "backup":
        assert calls == [
            ("create_backup", (settings,), {"deadline": 1234, **workspace_options}),
            ("prune", (settings,), workspace_options),
        ]
        assert result["retention"] == {"valid": True}
    elif operation == "verify-backup":
        assert calls == [
            ("verify_backup", (options["backup"],), {"deadline": 1234, **workspace_options})
        ]
    else:
        assert calls == [
            (
                "restore_backup",
                (options["backup"], options["destination"]),
                {"deadline": 1234, **workspace_options},
            )
        ]
