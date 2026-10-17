"""Adversarial boundaries for finite characterization and its frozen source."""

import importlib.util
import json
import os
import shlex
import signal
import subprocess
import sys
import textwrap
import time
from argparse import Namespace
from contextlib import suppress
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

import duckdb
import pytest

from fakes.capacity_source import CapacitySource
from fakes.world import make_event, make_market
from oddsfox_catalogue.current_config import CurrentSettings
from oddsfox_catalogue.current_rows import project_response
from oddsfox_catalogue.current_store import apply, initialize

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/benchmark-capacity.py"
spec = importlib.util.spec_from_file_location("capacity_adversarial", SCRIPT)
assert spec and spec.loader
capacity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capacity)


def get(source, path):
    try:
        with urlopen(source.url + path, timeout=10) as response:
            return response.status, json.load(response)
    except HTTPError as exc:
        return exc.code, json.load(exc)


def command(tmp_path, code, *, cap=64, timeout=3):
    return capacity.installed.run_command(
        [sys.executable, "-c", code],
        tmp_path,
        os.environ.copy(),
        tmp_path / "child.log",
        timeout=timeout,
        max_output_bytes=cap,
    )


def overflow_emitter(source):
    body = textwrap.indent(textwrap.dedent(source).strip(), "    ")
    return (
        "import signal\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "try:\n"
        f"{body}\n"
        "except BrokenPipeError:\n"
        "    pass\n"
        "while True:\n"
        "    signal.pause()\n"
    )


@pytest.mark.parametrize("length", [63, 64, 65])
def test_output_cap_counts_raw_bytes_across_both_streams(tmp_path, length):
    emitter = f"import os; os.write(1, bytes(range(32))); os.write(2, bytes(range({length - 32})))"
    code = overflow_emitter(emitter) if length > 64 else emitter
    expected = bytes(range(32)) + bytes(range(length - 32))
    if length <= 64:
        command(tmp_path, code)
    else:
        with pytest.raises(ValueError, match="output allowance exhausted"):
            command(tmp_path, code)
    assert (tmp_path / "child.log").read_bytes() == expected[:64]


@pytest.mark.parametrize("cap", [0, -1, True, 1.5])
def test_invalid_output_cap_rejected_before_process_or_log(tmp_path, monkeypatch, cap):
    monkeypatch.setattr(
        capacity.installed.subprocess, "Popen", lambda *a, **kw: pytest.fail("spawned child")
    )
    with pytest.raises(ValueError, match="positive integer"):
        command(tmp_path, "pass", cap=cap)
    assert not (tmp_path / "child.log").exists()


def test_unbounded_default_preserves_complete_binary_log(tmp_path):
    command(tmp_path, "import os; os.write(1, bytes(range(256)) * 4)", cap=None)
    assert (tmp_path / "child.log").read_bytes() == bytes(range(256)) * 4


@pytest.mark.parametrize("overflow", [False, True])
def test_bounded_failure_diagnostic_never_echoes_child_credentials(tmp_path, overflow):
    secret = "https://user:synthetic-password@example.invalid?token=private"
    emitter = f"import os; os.write(2, {secret.encode()!r})"
    code = overflow_emitter(emitter) if overflow else f"{emitter}; raise SystemExit(7)"
    with pytest.raises(ValueError) as error:
        command(tmp_path, code, cap=8 if overflow else 512)
    text = str(error.value)
    assert "synthetic-password" not in text
    assert "example.invalid" not in text
    assert "token=private" not in text
    assert (tmp_path / "child.log").stat().st_size <= (8 if overflow else 512)
    assert ("output allowance exhausted" if overflow else "command failed (7)") in text


@pytest.mark.parametrize("parent_exits", [False, True])
def test_bounded_timeout_kills_term_ignoring_descendant_with_open_pipe(tmp_path, parent_exits):
    heartbeat, group = tmp_path / "heartbeat", tmp_path / "group"
    descendant = """
import signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(sys.argv[1], 'ab', buffering=0) as stream:
    while True:
        stream.write(b'x')
        time.sleep(0.02)
"""
    parent = f"""
import os, subprocess, sys, time
from pathlib import Path
Path({str(group)!r}).write_text(str(os.getpid()))
subprocess.Popen([sys.executable, '-c', {descendant!r}, {str(heartbeat)!r}])
{"pass" if parent_exits else "time.sleep(60)"}
"""
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            command(tmp_path, parent, cap=1024, timeout=0.8)
        assert heartbeat.exists(), "descendant did not start; timeout test is inconclusive"
        time.sleep(0.1)
        stopped = heartbeat.read_bytes()
        time.sleep(0.1)
        assert heartbeat.read_bytes() == stopped
        assert (tmp_path / "child.log").stat().st_size <= 1024
    finally:
        if group.exists():
            with suppress(ProcessLookupError):
                os.killpg(int(group.read_text()), signal.SIGKILL)


@pytest.fixture
def seed(tmp_path):
    root = tmp_path / "seed"
    root.mkdir()
    unknown = make_market("12", "Unknown")
    unknown["closed"] = None
    events = [
        make_event("1", "Open", markets=[make_market("10", "Open"), unknown]),
        make_event("2", "Closed", closed=True, markets=[make_market("11", "Closed", closed=True)]),
    ]
    with duckdb.connect(str(root / "catalogue.duckdb")) as conn:
        initialize(conn)
        apply(
            conn,
            project_response(
                events, "events", received_at="2026-01-01T00:00:00Z", response_id="seed"
            ),
        )
    return root


def test_ambiguous_queries_and_noncanonical_ids_are_rejected(seed, tmp_path):
    with CapacitySource(seed, tmp_path / "audit", delay_s=0) as source:
        for field, value in (
            ("limit", "1"),
            ("closed", "false"),
            ("include_tag", "true"),
        ):
            assert get(source, f"/markets?id=10&limit=1&{field}={value}&{field}={value}")[0] == 400
        for field, value in (("order", "id"), ("ascending", "false")):
            assert (
                get(source, f"/markets?limit=1&order=id&ascending=false&{field}={value}")[0] == 400
            )
        for invalid in ("0", "01", "-1", "%2B1", "1.0", "1%20", "1e1", "1" * 21, ""):
            assert get(source, f"/markets?id={invalid}&limit=1")[0] == 400
        assert get(source, "/markets/12")[1]["closed"] is None
        assert get(source, "/markets?id=12&limit=1&closed=false")[1][0]["id"] == "12"
        assert get(source, "/markets?id=12&limit=1&closed=true")[1] == []


def test_audit_limit_never_returns_or_counts_unrecorded_success(seed, tmp_path):
    audit = tmp_path / "audit"
    with CapacitySource(seed, audit, delay_s=0, max_audit_bytes=1) as source:
        for _ in range(2):
            status, payload = get(source, "/events/1")
            assert status == 507
            assert payload == {"error": "audit limit exceeded"}
        assert source.stats["emitted_events"] == source.stats["emitted_markets"] == 0
        assert source.stats["audit_bytes"] == audit.stat().st_size == 0


def test_oversized_event_is_atomic_and_later_small_response_still_works(
    seed, tmp_path, monkeypatch
):
    import fakes.capacity_source as provider

    with duckdb.connect(str(seed / "catalogue.duckdb")) as conn:
        conn.execute("UPDATE events SET title=? WHERE event_id='1'", ["large" * 300])
    monkeypatch.setattr(provider, "MAX_RESPONSE_BYTES", 1024)
    audit = tmp_path / "audit"
    with CapacitySource(seed, audit, delay_s=0) as source:
        assert get(source, "/events/1") == (413, {"error": "response too large"})
        assert source.stats["emitted_events"] == source.stats["emitted_markets"] == 0
        entry = json.loads(audit.read_text())
        assert entry["status"] == 413
        assert entry["emitted_ids"] == []
        assert entry["complete_memberships"] == {}
        assert get(source, "/markets/10")[0] == 200
        assert source.failure is None


def test_database_and_audit_symlinks_cannot_escape_seed_or_overwrite_evidence(seed, tmp_path):
    database = seed / "catalogue.duckdb"
    moved = tmp_path / "real.duckdb"
    database.rename(moved)
    database.symlink_to(moved)
    with pytest.raises(ValueError, match="regular|symlink"):
        CapacitySource(seed, tmp_path / "audit", delay_s=0)
    database.unlink()
    moved.rename(database)
    target = tmp_path / "evidence"
    target.write_text("preserved")
    audit = tmp_path / "audit-link"
    audit.symlink_to(target)
    with pytest.raises((ValueError, FileExistsError)):
        CapacitySource(seed, audit, delay_s=0)
    assert target.read_text() == "preserved"
    assert audit.is_symlink()


@pytest.mark.parametrize(
    "mutation",
    [
        {"positionIds": None},
        {"version": "v3"},
        {"positionIds": ["1", "1"]},
    ],
)
def test_unsupported_unusable_identity_fails_closed_without_inventing_assets(
    seed, tmp_path, mutation
):
    raw = make_market("13", "Unusable")
    raw.update(mutation)
    with duckdb.connect(str(seed / "catalogue.duckdb")) as conn:
        apply(
            conn,
            project_response(
                [raw], "markets", received_at="2026-01-01T00:00:00Z", response_id="bad"
            ),
        )
        assert conn.execute("SELECT usable FROM markets WHERE market_id='13'").fetchone() == (
            False,
        )
    with CapacitySource(seed, tmp_path / "audit", delay_s=0) as source:
        assert get(source, "/markets?id=10&id=13&limit=2") == (500, {"error": "source failed"})
        assert source.stats["emitted_markets"] == 0
        assert source.failure == "source read failed"
        entry = json.loads((tmp_path / "audit").read_text())
        assert entry["emitted_ids"] == []


def test_real_output_overflow_retains_failed_bounded_log_and_report(tmp_path, monkeypatch):
    seed, root, output = tmp_path / "seed", tmp_path / "root", tmp_path / "output"
    seed.mkdir()
    root.mkdir()
    executable = tmp_path / "noisy"
    # Keep the emitter alive through TERM so this test isolates overflow and forced cleanup.
    noisy = overflow_emitter("import os\nos.write(1, b'x' * 32768)")
    executable.write_text("#!/bin/sh\nexec " + shlex.join([sys.executable, "-c", noisy]) + "\n")
    executable.chmod(0o700)
    monkeypatch.setattr(capacity, "ANCILLARY_BYTES", 16 * 1024)
    monkeypatch.setattr(capacity, "REPORT_BYTES", 4 * 1024)
    monkeypatch.setattr(capacity, "paths", lambda _: (seed, root, output, [seed, root, output]))
    monkeypatch.setattr(capacity, "capacity", lambda *_: {})
    monkeypatch.setattr(capacity, "snapshot", lambda *_: {})
    monkeypatch.setattr(capacity, "load_current_settings", lambda *_, **__: CurrentSettings(root))

    class Source:
        url = "http://127.0.0.1:9"
        controls = {}
        stats = {}

        def __init__(self, *_):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

    monkeypatch.setattr(capacity, "CapacitySource", Source)
    args = Namespace(
        seed_root=seed,
        root=root,
        output=output,
        executable=executable,
        account_root=[seed],
        invocations=1,
        cadence_seconds=0,
        max_total_seconds=4000,
        characterize=True,
    )
    assert capacity.run(args) == 1
    report = json.loads((output / "report.json").read_text())
    assert report["status"] == "failed"
    assert report["sustained_acceptance"] is False
    assert not report.get("characterization_complete", False)
    assert len(report["steps"]) == 1
    assert "output allowance exhausted" in report["error"]
    assert Path(report["steps"][0]["log"]).stat().st_size == 8 * 1024
    assert sum(p.stat().st_size for p in output.glob("*.*")) <= capacity.ANCILLARY_BYTES


@pytest.mark.parametrize(
    "prefix",
    [
        "WARNING retrying after HTTP 503\n",
        '{"status":"partial","requests":0}\nWARNING retrying after HTTP 503\n',
    ],
)
def test_command_result_uses_final_complete_object_after_retry_diagnostics(tmp_path, prefix):
    result = {"status": "passed", "summary": {"requests": 2}}
    payload = json.dumps(result, indent=2) + "\n"
    command(
        tmp_path,
        f"import os; os.write(2, {prefix.encode()!r}); os.write(1, {payload.encode()!r})",
        cap=1024,
    )
    assert capacity.command_result(tmp_path / "child.log") == result


@pytest.mark.parametrize(
    "output",
    [
        '{"status":"partial"}\nWARNING retrying\n{\n  "status":"passed",',
        '{"status":"passed"}\nWARNING unexpected trailing stderr\n',
        '[{"status":"passed"}]\n',
        'WARNING no result; contextual payload {"status":"passed"}\n',
    ],
    ids=["truncated-result", "trailing-stderr", "array-only", "warning-only"],
)
def test_command_result_rejects_incomplete_or_nonfinal_objects(tmp_path, output):
    log = tmp_path / "child.log"
    log.write_text(output)
    with pytest.raises(ValueError, match="final JSON object"):
        capacity.command_result(log)


def test_capacity_sigterm_reaps_child_group_restores_handlers_and_retains_report(tmp_path):
    heartbeat, group = tmp_path / "heartbeat", tmp_path / "child-group"
    seed, root, output = tmp_path / "seed", tmp_path / "candidate", tmp_path / "evidence"
    seed.mkdir()
    root.mkdir()
    descendant = """
import signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(sys.argv[1], 'ab', buffering=0) as stream:
    while True:
        stream.write(b'x')
        time.sleep(0.02)
"""
    child = f"""
import os, subprocess, sys, time
from pathlib import Path
Path({str(group)!r}).write_text(str(os.getpid()))
subprocess.Popen([sys.executable, '-c', {descendant!r}, {str(heartbeat)!r}])
time.sleep(60)
"""
    executable = tmp_path / "blocking-child"
    executable.write_text("#!/bin/sh\nexec " + shlex.join([sys.executable, "-c", child]) + "\n")
    executable.chmod(0o700)
    entrypoint = """
import importlib.util, signal, sys
from pathlib import Path
from oddsfox_catalogue.current_config import CurrentSettings
spec = importlib.util.spec_from_file_location('capacity_signal', sys.argv[1])
capacity = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capacity)
seed, root, output = map(Path, sys.argv[2:5])
capacity.paths = lambda _: (seed, root, output, [seed, root, output])
capacity.capacity = lambda *_: {}
capacity.snapshot = lambda *_: {}
capacity.load_current_settings = lambda *_, **__: CurrentSettings(root)
class Source:
    url = 'http://127.0.0.1:9'
    controls = {}
    stats = {}
    def __init__(self, *_): pass
    def __enter__(self): return self
    def __exit__(self, *_): pass
capacity.CapacitySource = Source
sys.argv = [sys.argv[1], '--seed-root', str(seed), '--root', str(root),
            '--output', str(output), '--account-root', str(seed),
            '--executable', sys.argv[5], '--characterize', '--max-total-seconds', '4000']
previous = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
code = capacity.main()
assert {s: signal.getsignal(s) for s in previous} == previous, 'handlers were not restored'
raise SystemExit(code)
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            entrypoint,
            str(SCRIPT),
            str(seed),
            str(root),
            str(output),
            str(executable),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 10
        while not heartbeat.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert heartbeat.exists(), "child did not start; signal test is inconclusive"
        process.send_signal(signal.SIGTERM)
        _, stderr = process.communicate(timeout=10)
        assert process.returncode == 143, stderr.decode(errors="replace")
        report = json.loads((output / "report.json").read_text())
        assert report["status"] == "partial"
        assert report["error"] == "interrupted; retained evidence"
        assert report["interrupted_exit_code"] == 143
        assert report["sustained_acceptance"] is False
        assert len(report["steps"]) == 1
        assert "elapsed_seconds" in report["steps"][0]
        assert Path(report["steps"][0]["log"]).is_file()
        time.sleep(0.1)
        stopped = heartbeat.read_bytes()
        time.sleep(0.1)
        assert heartbeat.read_bytes() == stopped
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        if group.exists():
            with suppress(ProcessLookupError):
                os.killpg(int(group.read_text()), signal.SIGKILL)
