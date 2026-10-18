"""Lazy Win32 implementations for platform_support.

This module is safe to import on POSIX; Win32 DLLs are loaded only on use.
The implementation follows Microsoft's CreateFileW reparse-point, Job Object,
and volume FlushFileBuffers contracts:
https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-createfilew
https://learn.microsoft.com/windows/win32/api/fileapi/nf-fileapi-flushfilebuffers
https://learn.microsoft.com/windows/win32/procthread/job-objects
"""

from __future__ import annotations

import ctypes
import errno
import os
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from ctypes import wintypes
from pathlib import Path
from types import SimpleNamespace

INVALID_HANDLE_VALUE = ctypes.c_void_p(-1).value
FILE_ATTRIBUTE_DIRECTORY = 0x10
FILE_ATTRIBUTE_REPARSE_POINT = 0x400
FILE_FLAG_BACKUP_SEMANTICS = 0x02000000
FILE_FLAG_OPEN_REPARSE_POINT = 0x00200000
GENERIC_READ = 0x80000000
GENERIC_WRITE = 0x40000000
FILE_READ_ATTRIBUTES = 0x80
FILE_ADD_FILE = 0x2
FILE_ADD_SUBDIRECTORY = 0x4
FILE_SHARE_READ = 0x1
FILE_SHARE_WRITE = 0x2
FILE_SHARE_DELETE = 0x4
OPEN_EXISTING = 3
CREATE_NEW = 1
OPEN_ALWAYS = 4
FILE_TYPE_DISK = 1
DRIVE_FIXED = 3
FILE_RENAME_INFO = 3
FILE_DISPOSITION_INFO = 4
JOB_OBJECT_EXTENDED_LIMIT_INFORMATION = 9
JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
HANDLE_FLAG_INHERIT = 0x1
_WIN_WCHAR = ctypes.c_uint16
_WIN_DWORD = ctypes.c_uint32
_WIN_BOOL = ctypes.c_int32


def _last_error(message: str) -> OSError:
    error = ctypes.get_last_error()
    return OSError(error, f"{message}: {ctypes.FormatError(error).strip()}")


def _prototype(function, restype, argtypes):
    function.restype = restype
    function.argtypes = argtypes
    return function


_api_cache = None


def _api():
    global _api_cache
    if _api_cache is None:
        if os.name != "nt":
            raise OSError(errno.ENOSYS, "Windows platform APIs are unavailable")
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        handle = wintypes.HANDLE
        dword = _WIN_DWORD
        _api_cache = SimpleNamespace(
            **{
                "kernel32": kernel32,
                "psapi": psapi,
                "CreateFileW": _prototype(
                    kernel32.CreateFileW,
                    handle,
                    [wintypes.LPCWSTR, dword, dword, ctypes.c_void_p, dword, dword, handle],
                ),
                "CloseHandle": _prototype(kernel32.CloseHandle, wintypes.BOOL, [handle]),
                "GetFileType": _prototype(kernel32.GetFileType, dword, [handle]),
                "GetFileInformationByHandle": _prototype(
                    kernel32.GetFileInformationByHandle,
                    wintypes.BOOL,
                    [handle, ctypes.c_void_p],
                ),
                "SetFilePointerEx": _prototype(
                    kernel32.SetFilePointerEx,
                    wintypes.BOOL,
                    [handle, ctypes.c_longlong, ctypes.POINTER(ctypes.c_longlong), dword],
                ),
                "SetEndOfFile": _prototype(kernel32.SetEndOfFile, wintypes.BOOL, [handle]),
                "SetFileInformationByHandle": _prototype(
                    kernel32.SetFileInformationByHandle,
                    wintypes.BOOL,
                    [handle, dword, ctypes.c_void_p, dword],
                ),
                "GetDriveTypeW": _prototype(kernel32.GetDriveTypeW, dword, [wintypes.LPCWSTR]),
                "GetVolumePathNameW": _prototype(
                    kernel32.GetVolumePathNameW,
                    wintypes.BOOL,
                    [wintypes.LPCWSTR, wintypes.LPWSTR, dword],
                ),
                "GetVolumeInformationW": _prototype(
                    kernel32.GetVolumeInformationW,
                    wintypes.BOOL,
                    [
                        wintypes.LPCWSTR,
                        wintypes.LPWSTR,
                        dword,
                        ctypes.POINTER(dword),
                        ctypes.POINTER(dword),
                        ctypes.POINTER(dword),
                        wintypes.LPWSTR,
                        dword,
                    ],
                ),
                "FlushFileBuffers": _prototype(kernel32.FlushFileBuffers, wintypes.BOOL, [handle]),
                "SetHandleInformation": _prototype(
                    kernel32.SetHandleInformation, wintypes.BOOL, [handle, dword, dword]
                ),
                "GetCurrentProcess": _prototype(kernel32.GetCurrentProcess, handle, []),
                "GetProcessMemoryInfo": _prototype(
                    psapi.GetProcessMemoryInfo,
                    wintypes.BOOL,
                    [handle, ctypes.c_void_p, dword],
                ),
                "CreateJobObjectW": _prototype(
                    kernel32.CreateJobObjectW, handle, [ctypes.c_void_p, wintypes.LPCWSTR]
                ),
                "SetInformationJobObject": _prototype(
                    kernel32.SetInformationJobObject,
                    wintypes.BOOL,
                    [handle, dword, ctypes.c_void_p, dword],
                ),
                "AssignProcessToJobObject": _prototype(
                    kernel32.AssignProcessToJobObject, wintypes.BOOL, [handle, handle]
                ),
                "TerminateJobObject": _prototype(
                    kernel32.TerminateJobObject, wintypes.BOOL, [handle, dword]
                ),
            }
        )
    return _api_cache


def _handle_value(handle) -> int:
    value = handle.value if isinstance(handle, ctypes.c_void_p) else int(handle)
    if value is None or value == INVALID_HANDLE_VALUE or value == 0:
        raise _last_error("Win32 returned an invalid handle")
    return value


def _close(handle) -> None:
    if handle is not None and not _api().CloseHandle(handle):
        raise _last_error("cannot close Win32 handle")


def _close_many(handles) -> None:
    failure = None
    for handle in reversed(handles):
        try:
            _close(handle)
        except BaseException as exc:
            if failure is None:
                failure = exc
    if failure is not None:
        raise failure


class _FILETIME(ctypes.Structure):
    _fields_ = [("dwLowDateTime", _WIN_DWORD), ("dwHighDateTime", _WIN_DWORD)]


class _BY_HANDLE_FILE_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("dwFileAttributes", _WIN_DWORD),
        ("ftCreationTime", _FILETIME),
        ("ftLastAccessTime", _FILETIME),
        ("ftLastWriteTime", _FILETIME),
        ("dwVolumeSerialNumber", _WIN_DWORD),
        ("nFileSizeHigh", _WIN_DWORD),
        ("nFileSizeLow", _WIN_DWORD),
        ("nNumberOfLinks", _WIN_DWORD),
        ("nFileIndexHigh", _WIN_DWORD),
        ("nFileIndexLow", _WIN_DWORD),
    ]


class _FILE_RENAME_INFO(ctypes.Structure):
    _fields_ = [
        ("ReplaceIfExists", ctypes.c_ubyte),
        ("RootDirectory", ctypes.c_void_p),
        ("FileNameLength", _WIN_DWORD),
        ("FileName", _WIN_WCHAR * 1),
    ]


def _file_information(handle) -> _BY_HANDLE_FILE_INFORMATION:
    result = _BY_HANDLE_FILE_INFORMATION()
    if not _api().GetFileInformationByHandle(handle, ctypes.byref(result)):
        raise _last_error("cannot inspect file handle")
    return result


def _validate_path_spelling(path: str | os.PathLike[str]) -> Path:
    target = Path(os.path.abspath(path))
    drive = target.drive
    if not drive or not drive.endswith(":") or str(target).startswith("\\\\"):
        raise OSError(errno.EINVAL, "Windows catalogue paths must use a local drive path")
    reserved = {"CON", "PRN", "AUX", "NUL"} | {
        f"{prefix}{number}" for prefix in ("COM", "LPT") for number in range(1, 10)
    }
    for part in target.parts[1:]:
        if ":" in part:
            raise OSError(errno.EINVAL, "alternate data streams are not supported")
        if part.rstrip(" .") != part:
            raise OSError(errno.EINVAL, "ambiguous Windows path component")
        if part.split(".", 1)[0].upper() in reserved:
            raise OSError(errno.EINVAL, "reserved Windows path component")
    return target


def _local_ntfs_volume(path: str | os.PathLike[str]) -> str:
    absolute = _validate_path_spelling(path)
    text = str(absolute)
    drive, _ = os.path.splitdrive(text)
    if not drive or not drive.endswith(":"):
        raise OSError(errno.EINVAL, "Windows catalogue paths must be on a local fixed NTFS volume")
    api = _api()
    root = drive + "\\"
    if api.GetDriveTypeW(root) != DRIVE_FIXED:
        raise OSError(errno.EINVAL, "catalogue path must be on a local fixed volume")
    volume_root = ctypes.create_unicode_buffer(32768)
    if not api.GetVolumePathNameW(text, volume_root, len(volume_root)):
        raise _last_error("cannot identify catalogue volume")
    if os.path.normcase(os.path.abspath(volume_root.value)) != os.path.normcase(root):
        raise OSError(
            errno.ENOTSUP, "mounted NTFS subvolumes are not supported for catalogue writes"
        )
    fs_name = ctypes.create_unicode_buffer(64)
    if not api.GetVolumeInformationW(
        volume_root.value, None, 0, None, None, None, fs_name, len(fs_name)
    ):
        raise _last_error("cannot inspect catalogue filesystem")
    if fs_name.value.upper() != "NTFS":
        raise OSError(errno.ENOTSUP, "durable catalogue writes require a local NTFS volume")
    return root


@contextmanager
def _pin_directory(path: str | os.PathLike[str]):
    target = Path(os.path.abspath(path))
    root = _local_ntfs_volume(target)
    api = _api()
    handles: list[int] = []
    try:
        current = Path(root)
        components = [current]
        components.extend(target.parts[1:])
        final_handle = None
        for component in components:
            current = Path(component) if str(component).endswith("\\") else current / component
            handle = api.CreateFileW(
                str(current),
                FILE_READ_ATTRIBUTES,
                FILE_SHARE_READ,
                None,
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                None,
            )
            _handle_value(handle)
            handles.append(handle)
            info = _file_information(handle)
            if not info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY:
                raise OSError(errno.ENOTDIR, f"not a directory: {current}")
            if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT:
                raise OSError(errno.ELOOP, f"reparse point in protected path: {current}")
            final_handle = handle
        yield target, final_handle
    finally:
        _close_many(handles)


@contextmanager
def pinned_path(path: str | os.PathLike[str]) -> Iterator[Path]:
    """Hold every directory from the volume root through path against rename."""
    with _pin_directory(path) as (target, _handle):
        yield target


def is_link_or_reparse(path: str | os.PathLike[str]) -> bool:
    try:
        attributes = Path(path).lstat().st_file_attributes
    except FileNotFoundError:
        return False
    return bool(attributes & FILE_ATTRIBUTE_REPARSE_POINT)


def open_regular_nofollow(path: str | os.PathLike[str], flags: int, mode: int = 0o600) -> int:
    del mode  # Windows ACLs, not POSIX mode bits, govern access.
    import msvcrt

    target = _validate_path_spelling(path)
    access = (GENERIC_READ if flags & os.O_ACCMODE in (os.O_RDONLY, os.O_RDWR) else 0) | (
        GENERIC_WRITE if flags & (os.O_WRONLY | os.O_RDWR) else 0
    )
    disposition = (
        CREATE_NEW
        if flags & os.O_CREAT and flags & os.O_EXCL
        else OPEN_ALWAYS
        if flags & os.O_CREAT
        else OPEN_EXISTING
    )
    with pinned_path(target.parent):
        handle = _api().CreateFileW(
            str(target),
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            disposition,
            FILE_FLAG_OPEN_REPARSE_POINT,
            None,
        )
        _handle_value(handle)
        try:
            info = _file_information(handle)
            if info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT):
                raise OSError(errno.EINVAL, "path is not a regular non-reparse file")
            if _api().GetFileType(handle) != FILE_TYPE_DISK:
                raise OSError(errno.EINVAL, "path is not a disk file")
            if flags & os.O_TRUNC and (
                not _api().SetFilePointerEx(handle, 0, None, 0) or not _api().SetEndOfFile(handle)
            ):
                raise _last_error("cannot truncate verified file")
            fd_flags = flags & (os.O_ACCMODE | os.O_APPEND | os.O_BINARY)
            descriptor = msvcrt.open_osfhandle(_handle_value(handle), fd_flags)
            handle = None  # open_osfhandle now owns the Win32 handle.
            try:
                os.set_inheritable(descriptor, False)
            except BaseException:
                os.close(descriptor)
                raise
            return descriptor
        except BaseException:
            _close(handle)
            raise


class _FILE_DISPOSITION_INFO(ctypes.Structure):
    _fields_ = [("DeleteFile", _WIN_BOOL)]


@contextmanager
def _open_nofollow(path: Path, *, directory: bool):
    handle = _api().CreateFileW(
        str(path),
        FILE_READ_ATTRIBUTES | 0x00010000,  # DELETE
        FILE_SHARE_READ,
        None,
        OPEN_EXISTING,
        FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
        None,
    )
    _handle_value(handle)
    try:
        info = _file_information(handle)
        if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT:
            raise OSError(errno.ELOOP, f"refusing reparse point: {path}")
        is_directory = bool(info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
        if is_directory != directory or _api().GetFileType(handle) != FILE_TYPE_DISK:
            raise OSError(errno.EINVAL, f"unexpected file type: {path}")
        yield handle
    finally:
        _close(handle)


def _delete_handle(handle, path: Path) -> None:
    disposition = _FILE_DISPOSITION_INFO(True)
    if not _api().SetFileInformationByHandle(
        handle,
        FILE_DISPOSITION_INFO,
        ctypes.byref(disposition),
        ctypes.sizeof(disposition),
    ):
        raise _last_error(f"cannot delete verified path {path}")


def remove_file(path: str | os.PathLike[str]) -> None:
    target = _validate_path_spelling(path)
    with pinned_path(target.parent), _open_nofollow(target, directory=False) as handle:
        _delete_handle(handle, target)


def _remove_tree_entry(path: Path) -> None:
    with _open_nofollow(path, directory=True) as handle:
        with os.scandir(path) as entries:
            for entry in entries:
                if (
                    entry.name in {".", ".."}
                    or ":" in entry.name
                    or entry.name.rstrip(" .") != entry.name
                ):
                    raise OSError(errno.EINVAL, "unsafe name in cleanup directory")
                child = path / entry.name
                if is_link_or_reparse(child):
                    raise OSError(errno.ELOOP, f"refusing reparse point: {child}")
                attributes = Path(child).stat().st_file_attributes
                if attributes & FILE_ATTRIBUTE_DIRECTORY:
                    _remove_tree_entry(child)
                else:
                    with _open_nofollow(child, directory=False) as child_handle:
                        _delete_handle(child_handle, child)
        _delete_handle(handle, path)


def remove_tree(path: str | os.PathLike[str]) -> None:
    target = _validate_path_spelling(path)
    with pinned_path(target.parent):
        _remove_tree_entry(target)


def replace_file(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
    src = Path(os.path.abspath(source))
    dst = Path(os.path.abspath(destination))
    _rename_by_handle(src, dst, directory=False)


def replace_directory(source: str | os.PathLike[str], destination: str | os.PathLike[str]) -> None:
    src = Path(os.path.abspath(source))
    dst = Path(os.path.abspath(destination))
    _rename_by_handle(src, dst, directory=True)


def _rename_by_handle(src: Path, dst: Path, *, directory: bool) -> None:
    src = _validate_path_spelling(src)
    dst = _validate_path_spelling(dst)
    if src.drive.casefold() != dst.drive.casefold():
        raise OSError(errno.EXDEV, "atomic replacement must stay on one volume")
    with ExitStack() as stack:
        if src.parent == dst.parent:
            stack.enter_context(pinned_path(src.parent))
        else:
            stack.enter_context(pinned_path(src.parent))
            stack.enter_context(pinned_path(dst.parent))
        if is_link_or_reparse(dst):
            raise OSError(errno.ELOOP, f"refusing reparse point destination: {dst}")
        with _open_nofollow(src, directory=directory) as source_handle:
            name = str(dst).encode("utf-16le")
            name_offset = _FILE_RENAME_INFO.FileName.offset
            buffer = ctypes.create_string_buffer(
                max(ctypes.sizeof(_FILE_RENAME_INFO), name_offset + len(name))
            )
            info = ctypes.cast(buffer, ctypes.POINTER(_FILE_RENAME_INFO)).contents
            info.ReplaceIfExists = True
            info.RootDirectory = None
            info.FileNameLength = len(name)
            ctypes.memmove(ctypes.addressof(buffer) + name_offset, name, len(name))
            if not _api().SetFileInformationByHandle(
                source_handle,
                FILE_RENAME_INFO,
                ctypes.byref(buffer),
                ctypes.sizeof(buffer),
            ):
                raise _last_error(f"cannot atomically replace {dst}")


def _volume_handle(path: str | os.PathLike[str]):
    root = _local_ntfs_volume(path)
    volume = "\\\\.\\" + root[0] + ":"
    handle = _api().CreateFileW(
        volume,
        GENERIC_WRITE,
        FILE_SHARE_READ | FILE_SHARE_WRITE,
        None,
        OPEN_EXISTING,
        0,
        None,
    )
    _handle_value(handle)
    return handle


def require_durability(path: str | os.PathLike[str]) -> None:
    """Fail before mutation unless the volume can be flushed by this process."""
    handle = _volume_handle(path)
    try:
        if not _api().FlushFileBuffers(handle):
            raise _last_error("Windows volume flush is unavailable; run with volume flush access")
    finally:
        _close(handle)


def sync_directory(path: str | os.PathLike[str]) -> None:
    handle = _volume_handle(path)
    try:
        if not _api().FlushFileBuffers(handle):
            raise _last_error("Windows volume flush failed; catalogue mutation is not durable")
    finally:
        _close(handle)


class _IO_COUNTERS(ctypes.Structure):
    _fields_ = [
        (name, ctypes.c_uint64)
        for name in (
            "ReadOperationCount",
            "WriteOperationCount",
            "OtherOperationCount",
            "ReadTransferCount",
            "WriteTransferCount",
            "OtherTransferCount",
        )
    ]


class _JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_int64),
        ("PerJobUserTimeLimit", ctypes.c_int64),
        ("LimitFlags", _WIN_DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", _WIN_DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", _WIN_DWORD),
        ("SchedulingClass", _WIN_DWORD),
    ]


class _JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _JOBOBJECT_BASIC_LIMIT_INFORMATION),
        ("IoInfo", _IO_COUNTERS),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


class WindowsJob:
    """A kill-on-close Job Object; the caller must assign before releasing work."""

    def __init__(self) -> None:
        self.handle = None

    def __enter__(self) -> WindowsJob:
        api = _api()
        handle = api.CreateJobObjectW(None, None)
        self.handle = _handle_value(handle)
        try:
            if not api.SetHandleInformation(self.handle, HANDLE_FLAG_INHERIT, 0):
                raise _last_error("cannot make job handle non-inheritable")
            limits = _JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
            limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            if not api.SetInformationJobObject(
                self.handle,
                JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                ctypes.byref(limits),
                ctypes.sizeof(limits),
            ):
                raise _last_error("cannot configure kill-on-close job")
            return self
        except BaseException:
            self.close()
            raise

    def assign(self, process_handle) -> None:
        if self.handle is None:
            raise RuntimeError("job is not open")
        handle = getattr(process_handle, "_handle", process_handle)
        value = _handle_value(handle)
        if not _api().AssignProcessToJobObject(self.handle, value):
            raise _last_error("cannot assign process to containment job")

    def terminate(self, exit_code: int = 1) -> None:
        if self.handle is None:
            return
        if not _api().TerminateJobObject(self.handle, exit_code):
            raise _last_error("cannot terminate contained processes")

    def close(self) -> None:
        handle = self.handle
        if handle is not None:
            _close(handle)
            self.handle = None

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class _PROCESS_MEMORY_COUNTERS(ctypes.Structure):
    _fields_ = [
        ("cb", _WIN_DWORD),
        ("PageFaultCount", _WIN_DWORD),
        ("PeakWorkingSetSize", ctypes.c_size_t),
        ("WorkingSetSize", ctypes.c_size_t),
        ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPagedPoolUsage", ctypes.c_size_t),
        ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
        ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
        ("PagefileUsage", ctypes.c_size_t),
        ("PeakPagefileUsage", ctypes.c_size_t),
    ]


def peak_rss_bytes() -> int:
    return _peak_working_set(_api().GetCurrentProcess())


def peak_process_rss_bytes(process_or_handle) -> int:
    handle = getattr(process_or_handle, "_handle", process_or_handle)
    return _peak_working_set(_handle_value(handle))


def _peak_working_set(handle) -> int:
    counters = _PROCESS_MEMORY_COUNTERS()
    counters.cb = ctypes.sizeof(counters)
    api = _api()
    if not api.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
        raise _last_error("cannot read process peak working set")
    return int(counters.PeakWorkingSetSize)
