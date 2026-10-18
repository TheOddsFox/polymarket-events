from __future__ import annotations

import ctypes
import errno
import os
import signal
from pathlib import Path

import pytest

from oddsfox_catalogue import _windows, platform_support


def test_platform_helpers_keep_only_supported_operator_signals() -> None:
    signals = platform_support.operator_signals()
    assert signal.SIGTERM in signals
    assert signal.SIGINT not in signals
    if hasattr(signal, "SIGBREAK"):
        assert signal.SIGBREAK in signals
    if hasattr(signal, "SIGHUP"):
        assert signal.SIGHUP in signals
    if hasattr(signal, "SIGKILL"):
        assert signal.SIGKILL not in signals
    assert signal.SIGINT in platform_support.operator_signals(include_interrupt=True)


@pytest.mark.skipif(os.name == "nt", reason="the native Windows APIs are available")
def test_windows_module_import_does_not_load_win32_dlls() -> None:
    assert _windows._api_cache is None
    with pytest.raises(OSError, match="unavailable"):
        _windows._api()


def test_native_handle_values_are_not_truncated_to_32_bits() -> None:
    wide_handle = (1 << (ctypes.sizeof(ctypes.c_void_p) * 8 - 1)) + 123
    if ctypes.sizeof(ctypes.c_void_p) > 4:
        assert _windows._handle_value(wide_handle) == wide_handle


def test_win32_structures_have_fixed_windows_abi_layout() -> None:
    assert ctypes.sizeof(_windows._BY_HANDLE_FILE_INFORMATION) == 52
    assert _windows._BY_HANDLE_FILE_INFORMATION.ftCreationTime.offset == 4
    assert _windows._BY_HANDLE_FILE_INFORMATION.nFileIndexHigh.offset == 44

    pointer_size = ctypes.sizeof(ctypes.c_void_p)
    assert ctypes.sizeof(_windows._PROCESS_MEMORY_COUNTERS) == 8 + 8 * pointer_size
    assert _windows._PROCESS_MEMORY_COUNTERS.PeakWorkingSetSize.offset == 8

    if pointer_size == 8:
        assert _windows._JOBOBJECT_BASIC_LIMIT_INFORMATION.LimitFlags.offset == 16
        assert _windows._JOBOBJECT_BASIC_LIMIT_INFORMATION.Affinity.offset == 48
        assert _windows._JOBOBJECT_EXTENDED_LIMIT_INFORMATION.IoInfo.offset == 64
        assert _windows._JOBOBJECT_EXTENDED_LIMIT_INFORMATION.ProcessMemoryLimit.offset == 112
        assert ctypes.sizeof(_windows._JOBOBJECT_EXTENDED_LIMIT_INFORMATION) == 144
        assert _windows._FILE_RENAME_INFO.FileName.offset == 20
    else:
        assert _windows._JOBOBJECT_BASIC_LIMIT_INFORMATION.LimitFlags.offset == 16
        assert _windows._JOBOBJECT_EXTENDED_LIMIT_INFORMATION.IoInfo.offset == 48
        assert _windows._FILE_RENAME_INFO.FileName.offset == 12


def test_job_creation_configures_kill_on_close_and_closes_on_failure(monkeypatch) -> None:
    events: list[tuple] = []
    wide_handle = (1 << (ctypes.sizeof(ctypes.c_void_p) * 8 - 1)) + 123

    class FakeAPI:
        def CreateJobObjectW(self, *_args):
            events.append(("create",))
            return wide_handle

        def SetHandleInformation(self, handle, mask, flags):
            events.append(("inherit", handle, mask, flags))
            return 1

        def SetInformationJobObject(self, handle, info_class, pointer, size):
            limits = ctypes.cast(
                pointer,
                ctypes.POINTER(_windows._JOBOBJECT_EXTENDED_LIMIT_INFORMATION),
            ).contents
            events.append(("limits", handle, info_class, limits.BasicLimitInformation.LimitFlags))
            return 1

        def AssignProcessToJobObject(self, job, process):
            events.append(("assign", job, process))
            return 1

        def TerminateJobObject(self, job, exit_code):
            events.append(("terminate", job, exit_code))
            return 1

        def CloseHandle(self, handle):
            events.append(("close", handle))
            return 1

    api = FakeAPI()
    monkeypatch.setattr(_windows, "_api", lambda: api)
    with _windows.WindowsJob() as job:
        assert job.handle == wide_handle
        job.assign(type("Process", (), {"_handle": wide_handle + 1})())
        job.terminate(7)

    assert ("limits", wide_handle, 9, _windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE) in events
    assert ("assign", wide_handle, wide_handle + 1) in events
    assert ("terminate", wide_handle, 7) in events
    assert events[-1] == ("close", wide_handle)

    class FailingAPI(FakeAPI):
        def SetInformationJobObject(self, *_args):
            return 0

    monkeypatch.setattr(_windows, "_api", lambda: FailingAPI())
    monkeypatch.setattr(_windows, "_last_error", lambda message: OSError(message))
    with pytest.raises(OSError), _windows.WindowsJob():
        pytest.fail("failed job configuration must not yield")
    assert events[-1] == ("close", wide_handle)


def test_job_close_failure_remains_visible_and_retryable(monkeypatch) -> None:
    class CloseFailureAPI:
        def CloseHandle(self, _handle):
            return 0

    monkeypatch.setattr(_windows, "_api", lambda: CloseFailureAPI())
    monkeypatch.setattr(_windows, "_last_error", lambda message: OSError(message))
    job = _windows.WindowsJob()
    job.handle = 123

    with pytest.raises(OSError, match="cannot close Win32 handle"):
        job.close()
    assert job.handle == 123


def test_close_many_attempts_every_handle_after_failure(monkeypatch) -> None:
    closed: list[int] = []

    class CloseFailureAPI:
        def CloseHandle(self, handle):
            closed.append(handle)
            return int(handle != 2)

    monkeypatch.setattr(_windows, "_api", lambda: CloseFailureAPI())
    monkeypatch.setattr(_windows, "_last_error", lambda message: OSError(message))
    with pytest.raises(OSError, match="cannot close Win32 handle"):
        _windows._close_many([1, 2, 3])
    assert closed == [3, 2, 1]


def test_peak_rss_uses_peak_working_set_and_propagates_api_failure(monkeypatch) -> None:
    class FakeAPI:
        def GetCurrentProcess(self):
            return 55

        def GetProcessMemoryInfo(self, _process, pointer, size):
            assert size == ctypes.sizeof(_windows._PROCESS_MEMORY_COUNTERS)
            counters = ctypes.cast(
                pointer, ctypes.POINTER(_windows._PROCESS_MEMORY_COUNTERS)
            ).contents
            counters.PeakWorkingSetSize = 987654
            return 1

    monkeypatch.setattr(_windows, "_api", lambda: FakeAPI())
    assert _windows.peak_rss_bytes() == 987654

    monkeypatch.setattr(
        _windows,
        "_api",
        lambda: type(
            "Failure",
            (),
            {
                "GetCurrentProcess": lambda self: 55,
                "GetProcessMemoryInfo": lambda self, *_args: 0,
            },
        )(),
    )
    monkeypatch.setattr(_windows, "_last_error", lambda message: OSError(message))
    with pytest.raises(OSError, match="peak working set"):
        _windows.peak_rss_bytes()


def test_windows_volume_flush_failure_is_visible_and_closes_handle(monkeypatch) -> None:
    events: list[tuple] = []

    class FakeAPI:
        def CreateFileW(self, path, *_args):
            events.append(("open", path))
            return 0x123456789

        def FlushFileBuffers(self, handle):
            events.append(("flush", handle))
            return 0

        def CloseHandle(self, handle):
            events.append(("close", handle))
            return 1

    monkeypatch.setattr(_windows, "_local_ntfs_volume", lambda _path: "C:\\")
    monkeypatch.setattr(_windows, "_api", lambda: FakeAPI())
    monkeypatch.setattr(_windows, "_last_error", lambda message: OSError(errno.EACCES, message))
    with pytest.raises(OSError, match="volume flush access"):
        _windows.require_durability(Path("C:/catalogue"))
    assert events[0] == ("open", "\\\\.\\C:")
    assert events[-1] == ("close", 0x123456789)


def test_unsupported_volume_fails_before_opening_or_mutating(monkeypatch) -> None:
    def reject_volume(_path):
        raise OSError(errno.ENOTSUP, "durability unsupported")

    class FakeAPI:
        def CreateFileW(self, *_args):
            pytest.fail("unsupported filesystem must fail before opening a volume handle")

    monkeypatch.setattr(_windows, "_local_ntfs_volume", reject_volume)
    monkeypatch.setattr(_windows, "_api", lambda: FakeAPI())
    with pytest.raises(OSError, match="durability unsupported"):
        _windows.require_durability(Path("C:/catalogue"))


def test_process_peak_api_is_explicitly_windows_only() -> None:
    if os.name == "nt":
        pytest.skip("the Windows process-handle API is available")
    with pytest.raises(OSError, match="Windows-only"):
        platform_support.peak_process_rss_bytes(1)


@pytest.mark.skipif(os.name != "nt", reason="requires native Windows path and NTFS semantics")
def test_windows_nofollow_open_rejects_directory_before_truncating(tmp_path: Path) -> None:
    directory = tmp_path / "kept"
    directory.mkdir()
    sentinel = directory / "sentinel.txt"
    sentinel.write_text("preserve", encoding="utf-8")

    with pytest.raises(OSError):
        platform_support.open_regular_nofollow(directory, os.O_WRONLY | os.O_TRUNC)

    assert sentinel.read_text(encoding="utf-8") == "preserve"

    target = tmp_path / "target.txt"
    target.write_text("preserve", encoding="utf-8")
    link = tmp_path / "link.txt"
    try:
        os.symlink(target, link)
    except OSError as exc:
        pytest.skip(f"creating a Windows file reparse point is unavailable: {exc}")
    with pytest.raises(OSError):
        platform_support.open_regular_nofollow(link, os.O_WRONLY | os.O_TRUNC)
    assert target.read_text(encoding="utf-8") == "preserve"


@pytest.mark.skipif(os.name != "nt", reason="requires native Windows path and NTFS semantics")
def test_windows_atomic_replacement_works_under_nested_directory_pins(tmp_path: Path) -> None:
    source = tmp_path / "stage.txt"
    destination = tmp_path / "current.txt"
    source.write_text("new", encoding="utf-8")
    destination.write_text("old", encoding="utf-8")

    with platform_support.pinned_path(tmp_path):
        platform_support.replace_file(source, destination)

    assert destination.read_text(encoding="utf-8") == "new"
    assert not source.exists()


@pytest.mark.skipif(os.name != "nt", reason="requires native Windows path semantics")
@pytest.mark.parametrize(
    ("name", "message"),
    [
        ("stream:payload", "alternate data streams"),
        ("trailing. ", "ambiguous Windows path"),
        ("CON.txt", "reserved Windows path"),
    ],
)
def test_windows_path_boundary_rejects_ambiguous_names(
    tmp_path: Path, name: str, message: str
) -> None:
    with pytest.raises(OSError, match=message), platform_support.pinned_path(tmp_path / name):
        pass


@pytest.mark.skipif(os.name != "nt", reason="requires native Windows path and NTFS semantics")
def test_windows_path_boundary_rejects_reparse_entries(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "junction"
    try:
        os.symlink(target, link, target_is_directory=True)
    except OSError as exc:
        pytest.skip(f"creating a Windows directory reparse point is unavailable: {exc}")
    assert platform_support.is_link_or_reparse(link)
    with pytest.raises(OSError, match="reparse point"), platform_support.pinned_path(link):
        pass
