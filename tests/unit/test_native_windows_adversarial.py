"""Fail-closed contracts for native Windows, including real workstation checks."""

from __future__ import annotations

import ctypes
import importlib.util
import os
import queue
import subprocess
import sys
import threading
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from oddsfox_catalogue import _process_gate, _windows, current_runtime
from oddsfox_catalogue.capture import writer
from oddsfox_catalogue.signals import SignalHold, Terminated


def _installed_harness():
    script = Path(__file__).resolve().parents[2] / "scripts" / "verify-installed-workflow.py"
    spec = importlib.util.spec_from_file_location("native_adversarial_harness", script)
    assert spec and spec.loader
    harness = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(harness)
    return harness


@pytest.mark.parametrize("token", [b"", b"\x00", b"x"])
def test_closed_or_unapproved_parent_gate_never_starts_target(monkeypatch, token):
    monkeypatch.setattr(_process_gate, "wait_for_parent_gate", lambda _handle: token == b"\x01")
    monkeypatch.setattr(
        _process_gate.subprocess, "call", lambda *_args: pytest.fail("unapproved target started")
    )
    assert _process_gate.main(["--gate-handle", "9", "--", "target"]) == 125


def test_gate_without_target_does_not_consume_release_token(monkeypatch):
    monkeypatch.setattr(
        _process_gate, "wait_for_parent_gate", lambda *_args: pytest.fail("gate consumed")
    )
    assert _process_gate.main(["--gate-handle", "9"]) == 2


@pytest.mark.parametrize("read_error", [None, OSError("gate read failed"), KeyboardInterrupt()])
def test_gate_descriptor_closes_on_eof_read_error_and_operator_interrupt(monkeypatch, read_error):
    descriptor = 123
    closed = []
    monkeypatch.setitem(
        sys.modules, "msvcrt", SimpleNamespace(open_osfhandle=lambda *_: descriptor)
    )
    # Only this helper inspects os.name; do not change the process-wide os module.
    monkeypatch.setattr(
        _process_gate,
        "os",
        SimpleNamespace(name="nt", O_RDONLY=os.O_RDONLY, read=None, close=closed.append),
    )

    def read(fd, count):
        assert (fd, count) == (descriptor, 1)
        if read_error is not None:
            raise read_error
        return b""

    monkeypatch.setattr(_process_gate.os, "read", read)
    if read_error is None:
        assert _process_gate.wait_for_parent_gate(456) is False
    else:
        with pytest.raises(type(read_error)):
            _process_gate.wait_for_parent_gate(456)
    assert closed == [descriptor]


def test_rename_uses_utf16_field_offset_instead_of_padded_structure_size(tmp_path, monkeypatch):
    calls = []

    @contextmanager
    def pin(path, **_kwargs):
        yield Path(path), 0x123456789

    @contextmanager
    def open_file(_path, **_kwargs):
        yield 0x23456789A

    class NativeRenameLayout(ctypes.Structure):
        _fields_ = [
            ("ReplaceIfExists", ctypes.c_ubyte),
            ("RootDirectory", ctypes.c_void_p),
            ("FileNameLength", ctypes.c_uint32),
            ("FileName", ctypes.c_uint16 * 1),
        ]

    def rename(handle, information_class, pointer, size):
        assert handle == 0x23456789A
        assert information_class == _windows.FILE_RENAME_INFO
        payload = ctypes.string_at(pointer, size)
        prefix = NativeRenameLayout.from_buffer_copy(payload[: ctypes.sizeof(NativeRenameLayout)])
        assert prefix.RootDirectory is None
        start = NativeRenameLayout.FileName.offset
        calls.append(payload[start : start + prefix.FileNameLength])
        return 1

    monkeypatch.setattr(_windows, "_pin_directory", pin)
    monkeypatch.setattr(_windows, "_open_nofollow", open_file)
    monkeypatch.setattr(_windows, "_validate_path_spelling", Path)
    monkeypatch.setattr(_windows, "is_link_or_reparse", lambda _path: False)
    monkeypatch.setattr(
        _windows, "_api", lambda: SimpleNamespace(SetFileInformationByHandle=rename)
    )
    destination = tmp_path / "résultat-🦊.json"
    _windows.replace_file(tmp_path / "source.tmp", destination)
    assert calls == [str(destination).encode("utf-16le")]


@pytest.mark.parametrize("method", ["require_durability", "sync_directory"])
def test_failed_volume_flush_is_visible_and_closes_wide_handle(monkeypatch, method):
    handle = 0x123456789
    closed = []
    monkeypatch.setattr(_windows, "_volume_handle", lambda _path: handle)
    monkeypatch.setattr(
        _windows,
        "_api",
        lambda: SimpleNamespace(
            FlushFileBuffers=lambda _handle: 0,
            CloseHandle=lambda handle: closed.append(handle) or 1,
        ),
    )
    monkeypatch.setattr(_windows, "_last_error", lambda message: OSError(message))
    with pytest.raises(OSError, match="flush"):
        getattr(_windows, method)("unused")
    assert closed == [handle]


def test_failed_directory_flush_is_visible_after_atomic_replacement(tmp_path, monkeypatch):
    target = tmp_path / "existing.json"
    target.write_bytes(b"kept")

    def refuse(_path):
        raise PermissionError("volume flush failed")

    monkeypatch.setattr(writer, "_fsync_dir", refuse)
    with pytest.raises(PermissionError, match="volume flush"):
        writer.atomic_write_bytes(target, b"replacement")
    assert target.read_bytes() == b"replacement"
    assert sorted(path.name for path in tmp_path.iterdir()) == ["existing.json"]


@pytest.mark.parametrize("operation", ["init", "probe", "refresh", "reconcile", "publish"])
def test_windows_runtime_preflight_failure_never_enters_mutation_guard(
    tmp_path, monkeypatch, operation
):
    def refuse(_root):
        raise PermissionError("no volume flush access")

    monkeypatch.setattr(current_runtime, "require_durability", refuse)
    monkeypatch.setattr(
        current_runtime, "pinned_path", lambda *_args: pytest.fail("mutation guard entered")
    )
    with (
        pytest.raises(PermissionError, match="volume flush"),
        current_runtime._windows_root_guard(SimpleNamespace(root=tmp_path), operation),
    ):
        pytest.fail("preflight failure allowed mutation")


def test_failed_job_assignment_preserves_error_and_closes_handle_once(monkeypatch):
    closed = []
    failure = PermissionError("synthetic job assignment denied")

    def fail_assignment(*_args):
        raise failure

    monkeypatch.setattr(
        _windows,
        "_api",
        lambda: SimpleNamespace(
            CreateJobObjectW=lambda *_args: 0x123456789,
            SetHandleInformation=lambda *_args: 1,
            SetInformationJobObject=lambda *_args: 1,
            AssignProcessToJobObject=fail_assignment,
            CloseHandle=lambda handle: closed.append(handle) or 1,
        ),
    )
    with pytest.raises(PermissionError) as raised, _windows.WindowsJob() as job:
        job.assign(0x23456789A)
    job.close()
    assert raised.value is failure
    assert closed == [0x123456789]


@pytest.mark.parametrize("phase", ["wait", "cleanup"])
def test_windows_command_operator_signal_survives_wait_and_final_cleanup(
    tmp_path, monkeypatch, phase
):
    import signal

    harness = _installed_harness()
    events = []
    hold = SignalHold()
    monkeypatch.setattr(harness, "SIGNALS", hold)
    monkeypatch.setitem(sys.modules, "msvcrt", SimpleNamespace(get_osfhandle=lambda fd: fd))
    monkeypatch.setattr(
        harness,
        "os",
        SimpleNamespace(
            pipe=lambda: (11, 12),
            close=lambda fd: events.append(("close", fd)),
            set_handle_inheritable=lambda *_args: None,
            write=lambda *_args: events.append(("released",)),
        ),
    )

    class Process:
        stdout = None
        calls = 0

        def poll(self):
            return 0

        def wait(self, timeout):
            self.calls += 1
            events.append(("wait", self.calls))
            if phase == "wait" and self.calls == 1:
                hold.receive(signal.SIGTERM)
            return 0

    class Job:
        def __enter__(self):
            return self

        def assign(self, _process):
            events.append(("assigned",))

        def terminate(self):
            events.append(("terminated",))
            if phase == "cleanup":
                hold.receive(signal.SIGTERM)
                events.append(("signal-deferred",))

        def __exit__(self, *_args):
            events.append(("job-closed",))

    process = Process()
    monkeypatch.setattr(harness, "WindowsJob", Job)
    monkeypatch.setattr(harness.subprocess, "Popen", lambda *_a, **_k: process)
    monkeypatch.setattr(harness.subprocess, "STARTUPINFO", SimpleNamespace, raising=False)
    with pytest.raises(Terminated) as raised:
        harness._run_command_windows(
            [sys.executable, "-c", "unused"], tmp_path, {}, tmp_path / "signal.log", 5, None
        )
    assert raised.value.signum == signal.SIGTERM
    assert process.calls == 3
    assert events.index(("assigned",)) < events.index(("released",))
    assert events.index(("terminated",)) < events.index(("wait", 3))
    assert events[-1] == ("job-closed",)
    if phase == "cleanup":
        assert ("signal-deferred",) in events


def test_windows_runtime_cleanup_holds_signal_until_every_pipe_is_closed(monkeypatch):
    import signal

    events = []
    hold = SignalHold()
    pipes = []

    class Pipe:
        closed = False

        def close(self):
            self.closed = True

        def send(self, _value):
            pass

        def poll(self, _timeout):
            return True

        def recv(self):
            return True, {"status": "synthetic"}

    class Process:
        sentinel = 0x123456789

        def start(self):
            pass

        def join(self, timeout):
            events.append("joined")

        def is_alive(self):
            return False

    def pipe_pair(**_kwargs):
        pair = Pipe(), Pipe()
        pipes.extend(pair)
        return pair

    class Job:
        def __enter__(self):
            return self

        def assign(self, handle):
            assert handle == Process.sentinel

        def terminate(self):
            hold.receive(signal.SIGTERM)
            events.append("signal-deferred")

        def __exit__(self, *_args):
            assert all(pipe.closed for pipe in pipes)
            events.append("closed")

    monkeypatch.setattr(current_runtime, "SIGNALS", hold)
    monkeypatch.setattr(current_runtime, "WindowsJob", Job)
    monkeypatch.setattr(
        current_runtime.multiprocessing,
        "get_context",
        lambda _method: SimpleNamespace(Pipe=pipe_pair, Process=lambda **_kwargs: Process()),
    )
    with pytest.raises(Terminated) as raised:
        current_runtime._supervise_windows(SimpleNamespace(), "refresh", {}, 5)
    assert raised.value.signum == signal.SIGTERM
    assert events == ["joined", "signal-deferred", "joined", "closed"]


@pytest.mark.skipif(os.name != "nt", reason="requires native Windows gated subprocess launch")
@pytest.mark.parametrize("failure", ["permission", "operator"])
def test_native_failed_job_assignment_never_executes_target(tmp_path, monkeypatch, failure):
    import signal

    harness = _installed_harness()
    monkeypatch.setattr(harness, "SIGNALS", SignalHold())
    marker = tmp_path / "must-not-exist"

    class RefusedJob:
        def __enter__(self):
            self.job = _windows.WindowsJob().__enter__()
            return self

        def assign(self, _process):
            if failure == "permission":
                raise PermissionError("synthetic job assignment denied")
            self.job.assign(_process)
            harness.SIGNALS.receive(signal.SIGTERM)

        def terminate(self):
            self.job.terminate()

        def __exit__(self, *_args):
            self.job.close()

    monkeypatch.setattr(harness, "WindowsJob", RefusedJob)
    expected = PermissionError if failure == "permission" else Terminated
    with pytest.raises(expected):
        harness.run_command(
            [sys.executable, "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()"],
            tmp_path,
            dict(os.environ),
            tmp_path / "assignment.log",
            timeout=5,
            max_output_bytes=1024,
        )
    assert not marker.exists()


@pytest.mark.skipif(os.name != "nt", reason="requires real Windows file ACL enforcement")
def test_native_frozen_package_acl_blocks_writes_then_restores_access(tmp_path):
    harness = _installed_harness()
    package = tmp_path / "package"
    package.mkdir()
    module = package / "__init__.py"
    module.write_bytes(b"original")
    before = harness.resource_inventory(package)
    with harness.frozen_resources(package, tmp_path / "saved-acl.txt"):
        harness.assert_resources_frozen(package)
        with pytest.raises(PermissionError):
            module.write_bytes(b"tampered")
        with pytest.raises(PermissionError):
            (package / "injected.py").write_bytes(b"tampered")
        assert harness.resource_inventory(package) == before
    module.write_bytes(b"writable again")
    assert module.read_bytes() == b"writable again"


@pytest.mark.skipif(os.name != "nt", reason="requires real Windows Job Objects")
def test_native_job_close_kills_descendant_even_after_leader_exits():
    import ctypes.wintypes as win

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [win.DWORD, win.BOOL, win.DWORD]
    kernel.OpenProcess.restype = win.HANDLE
    kernel.WaitForSingleObject.argtypes = [win.HANDLE, win.DWORD]
    kernel.WaitForSingleObject.restype = win.DWORD
    kernel.CloseHandle.argtypes = [win.HANDLE]
    kernel.CloseHandle.restype = win.BOOL
    child_code = (
        "import subprocess,sys; "
        "assert sys.stdin.buffer.read(1)==b'1'; "
        "p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],"
        "stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); "
        "print(p.pid,flush=True)"
    )
    process = None
    descendant_handle = None
    try:
        with _windows.WindowsJob() as job:
            process = subprocess.Popen(
                [sys.executable, "-c", child_code], stdin=subprocess.PIPE, stdout=subprocess.PIPE
            )
            job.assign(process)
            process.stdin.write(b"1")
            process.stdin.close()
            assert process.wait(timeout=5) == 0
            descendant = int(process.stdout.readline())
            descendant_handle = kernel.OpenProcess(0x00100000, False, descendant)  # SYNCHRONIZE
            assert descendant_handle, "cannot inspect the synthetic descendant"
            assert kernel.WaitForSingleObject(descendant_handle, 0) == 258  # WAIT_TIMEOUT
            job.close()
            assert kernel.WaitForSingleObject(descendant_handle, 5000) == 0
    finally:
        if descendant_handle:
            kernel.CloseHandle(descendant_handle)
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)
            process.stdout.close()


@pytest.mark.skipif(
    os.name != "nt", reason="requires native Windows kill-on-parent-death containment"
)
def test_native_parent_death_closes_noninherited_job_and_kills_worker():
    import ctypes.wintypes as win

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.OpenProcess.argtypes = [win.DWORD, win.BOOL, win.DWORD]
    kernel.OpenProcess.restype = win.HANDLE
    kernel.WaitForSingleObject.argtypes = [win.HANDLE, win.DWORD]
    kernel.WaitForSingleObject.restype = win.DWORD
    kernel.CloseHandle.argtypes = [win.HANDLE]
    kernel.CloseHandle.restype = win.BOOL
    script = """
import subprocess, sys
from oddsfox_catalogue._windows import WindowsJob
with WindowsJob() as job:
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    try:
        job.assign(child)
        print(child.pid, flush=True)
        sys.stdin.buffer.read(1)
    finally:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
"""
    parent = subprocess.Popen(
        [sys.executable, "-u", "-c", script],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    announced = queue.Queue()
    reader = threading.Thread(target=lambda: announced.put(parent.stdout.readline()), daemon=True)
    reader.start()
    worker_handle = None
    try:
        worker_pid = int(announced.get(timeout=5))
        worker_handle = kernel.OpenProcess(0x00100000, False, worker_pid)
        assert worker_handle, "cannot inspect the synthetic worker"
        assert kernel.WaitForSingleObject(worker_handle, 0) == 258
        parent.kill()
        parent.wait(timeout=5)
        assert kernel.WaitForSingleObject(worker_handle, 5000) == 0
    finally:
        if parent.poll() is None:
            parent.kill()
        parent.wait(timeout=5)
        reader.join(timeout=5)
        if worker_handle:
            kernel.CloseHandle(worker_handle)
        parent.stdin.close()
        parent.stdout.close()
        parent.stderr.close()


@pytest.mark.skipif(os.name != "nt", reason="requires real Windows alternate data streams")
def test_native_regular_open_rejects_leaf_alternate_data_stream(tmp_path):
    target = tmp_path / "preserved"
    target.write_bytes(b"kept")
    with pytest.raises(OSError):
        descriptor = _windows.open_regular_nofollow(
            str(target) + ":forbidden", os.O_WRONLY | os.O_CREAT
        )
        os.close(descriptor)
    assert target.read_bytes() == b"kept"


@pytest.mark.skipif(os.name != "nt", reason="requires native Windows handle-based replacement")
@pytest.mark.parametrize("name", ["x", "résultat-🦊.json"])
def test_native_file_replacement_handles_minimum_and_surrogate_pair_names(tmp_path, name):
    source = tmp_path / "source.tmp"
    source.write_bytes(b"new")
    destination = tmp_path / name
    destination.write_bytes(b"old")
    _windows.replace_file(source, destination)
    assert destination.read_bytes() == b"new"
    assert not source.exists()
