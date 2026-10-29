import importlib.util
import os
import shutil
import signal
import subprocess
import sys
import time
from collections import deque
from contextlib import suppress
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/verify-installed-workflow.py"
spec = importlib.util.spec_from_file_location("installed_workflow_harness", SCRIPT)
assert spec and spec.loader
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


@pytest.mark.parametrize(
    "output",
    [
        harness.REPO,
        harness.REPO / "data",
        harness.REPO / "data" / "catalogue-runtime" / "proof",
        harness.REPO / "data" / "catalogue-production-duckdb2" / "proof",
        harness.REPO / "data" / "legacy-data-state" / "proof",
        harness.REPO.parent / "outside-proof",
    ],
)
def test_output_rejects_checkout_and_catalogue_paths_before_creating_files(monkeypatch, output):
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--output", str(output)])
    monkeypatch.setattr(harness, "verify", lambda output: pytest.fail("must reject before proof"))
    with pytest.raises(SystemExit) as error:
        harness.main()
    assert error.value.code == 2
    assert output in (harness.REPO, harness.REPO / "data") or not output.exists()


def test_output_accepts_fresh_directory_under_repo_data(tmp_path, monkeypatch):
    output = tmp_path / "installed-proof"
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--output", str(output)])
    monkeypatch.setattr(harness, "verify", lambda path: {"status": "passed"})
    assert harness.main() == 0
    assert harness.json.loads((output / "report.json").read_text())["output"] == str(output)
    assert output.is_relative_to(harness.REPO / "data")


def test_default_output_uses_unique_installed_proofs_directory(monkeypatch):
    created = []
    mkdtemp = harness.tempfile.mkdtemp

    def capture_mkdtemp(*args, **kwargs):
        output = Path(mkdtemp(*args, **kwargs))
        created.append(output)
        return str(output)

    monkeypatch.setattr(sys, "argv", [str(SCRIPT)])
    monkeypatch.setattr(harness, "verify", lambda path: {"status": "passed"})
    monkeypatch.setattr(harness.tempfile, "mkdtemp", capture_mkdtemp)
    try:
        assert harness.main() == 0
        assert len(created) == 1
        report = harness.json.loads((created[0] / "report.json").read_text())
        output = Path(report["output"])
        assert output.parent == harness.REPO / "data" / "installed-proofs"
        assert output.is_relative_to(harness.REPO / "data")
    finally:
        for output in created:
            shutil.rmtree(output, ignore_errors=True)


def test_resource_freeze_preserves_inventory_and_detects_tampering(tmp_path):
    (tmp_path / "__init__.py").write_text("project")
    (tmp_path / "cli.py").write_text("profile")
    before = harness.resource_inventory(tmp_path)
    harness.freeze_resources(tmp_path)
    try:
        harness.assert_resources_frozen(tmp_path)
        assert harness.resource_inventory(tmp_path) == before
        (tmp_path / "cli.py").chmod(0o600)
        with pytest.raises(ValueError, match="writable"):
            harness.assert_resources_frozen(tmp_path)
        (tmp_path / "cli.py").write_text("tampered")
        assert harness.resource_inventory(tmp_path) != before
    finally:
        tmp_path.chmod(0o700)
        for path in tmp_path.iterdir():
            path.chmod(0o600)


def test_resource_inventory_rejects_symlink(tmp_path):
    (tmp_path / "__init__.py").write_text("project")
    (tmp_path / "cli.py").symlink_to(tmp_path / "__init__.py")
    with pytest.raises(ValueError, match="regular"):
        harness.resource_inventory(tmp_path)


def test_fixture_accounting_proves_only_acquisition_used_network():
    report = {
        "status": "passed",
        "accounting_complete": True,
        "workflow": "catalogue",
        "http_attempts": 2,
        "downloaded_bytes": 17,
        "steps": [
            {"name": "capture_1", "result": {"http_attempts": 1, "downloaded_bytes": 8}},
            {"name": "capture_2", "result": {"http_attempts": 1, "downloaded_bytes": 9}},
            {"name": "offline_replay", "result": {"http_attempts": 0}},
        ],
    }
    measured = {
        "http_attempts": 2,
        "downloaded_bytes": 17,
        "exhausted": False,
        "responses": [
            {"path": "/markets/1?include_tag=true", "status": 200, "bytes": 8},
            {"path": "/markets/1", "status": 200, "bytes": 9},
        ],
    }
    harness.assert_accounting(report, measured, {"/markets/1"})
    measured["downloaded_bytes"] = 18
    with pytest.raises(ValueError, match="differs"):
        harness.assert_accounting(report, measured, {"/markets/1"})
    measured["downloaded_bytes"] = 17
    with pytest.raises(ValueError, match="scope"):
        harness.assert_accounting(report, measured, {"/markets/2"})
    report["steps"][-1]["result"]["http_attempts"] = 1
    with pytest.raises(ValueError, match="offline"):
        harness.assert_accounting(report, measured, {"/markets/1"})


@pytest.mark.parametrize(
    ("message", "diagnostic"),
    [
        ("Because agate was not found in the cache", "offline_cache_miss"),
        ("pathspec needs to be downloaded from a registry", "offline_cache_miss"),
        ("Hash mismatch for required package", "dependency_hash_mismatch"),
        ("unrecognized subprocess failure", None),
    ],
)
def test_command_failure_exposes_only_fixed_diagnostics(tmp_path, message, diagnostic):
    secret = "synthetic-secret-not-for-diagnostics"
    output = f"https://username:{secret}@registry.example/package?token={secret}\n{message}"
    with pytest.raises(ValueError) as error:
        harness.run_command(
            [sys.executable, "-c", "import sys; print(sys.argv[1]); sys.exit(1)", output],
            tmp_path,
            os.environ.copy(),
            tmp_path / "failure.log",
        )
    assert "command failed (1)" in str(error.value)
    assert secret not in str(error.value)
    assert "registry.example" not in str(error.value)
    assert "username" not in str(error.value)
    if diagnostic is None:
        assert "diagnostic:" not in str(error.value)
    else:
        assert str(error.value).endswith(f"diagnostic: {diagnostic}")


def test_command_failure_reads_only_bounded_tail(tmp_path):
    output = "Because agate was not found in the cache\n" + "x" * 8192
    with pytest.raises(ValueError) as error:
        harness.run_command(
            [sys.executable, "-c", "import sys; print(sys.argv[1]); sys.exit(1)", output],
            tmp_path,
            os.environ.copy(),
            tmp_path / "failure.log",
        )
    assert "diagnostic:" not in str(error.value)


class FakeProcess:
    pid = 123

    def __init__(self, polls=(), initial_wait_error=None):
        self.polls = deque(polls)
        self.initial_wait_error = initial_wait_error
        self.waits = []

    def poll(self):
        return self.polls.popleft() if self.polls else 0

    def wait(self, timeout):
        self.waits.append(timeout)
        if len(self.waits) == 1 and self.initial_wait_error:
            raise self.initial_wait_error
        return 0


def patch_process(monkeypatch, process, killpg):
    monkeypatch.setattr(harness.subprocess, "Popen", lambda *args, **kwargs: process)
    monkeypatch.setattr(harness.os, "killpg", killpg)


def test_cleanup_signals_group_after_leader_already_exited(tmp_path, monkeypatch):
    process = FakeProcess(polls=[0, 0])
    signals = []
    patch_process(monkeypatch, process, lambda pid, signum: signals.append(signum))

    harness.run_command(["unused"], tmp_path, {}, tmp_path / "child.log")

    assert signals == [signal.SIGTERM, signal.SIGKILL]
    assert process.waits == [600, 5, 5]


def test_cleanup_ignores_missing_process_group(tmp_path, monkeypatch):
    process = FakeProcess()

    def missing_group(pid, signum):
        raise ProcessLookupError

    patch_process(monkeypatch, process, missing_group)

    harness.run_command(["unused"], tmp_path, {}, tmp_path / "child.log")


def test_term_permission_race_retries_after_leader_exit(tmp_path, monkeypatch):
    process = FakeProcess(polls=[None, 0, 0])
    signals = []

    def killpg(pid, signum):
        signals.append(signum)
        if signum == signal.SIGTERM and signals.count(signum) == 1:
            raise PermissionError("race")

    patch_process(monkeypatch, process, killpg)
    harness.run_command(["unused"], tmp_path, {}, tmp_path / "child.log")
    assert signals == [signal.SIGTERM, signal.SIGTERM, signal.SIGKILL]


def test_kill_permission_race_retries_after_leader_exit(tmp_path, monkeypatch):
    process = FakeProcess(polls=[0, None, 0])
    signals = []

    def killpg(pid, signum):
        signals.append(signum)
        if signum == signal.SIGKILL and signals.count(signum) == 1:
            raise PermissionError("race")

    patch_process(monkeypatch, process, killpg)
    harness.run_command(["unused"], tmp_path, {}, tmp_path / "child.log")
    assert signals == [signal.SIGTERM, signal.SIGKILL, signal.SIGKILL]


@pytest.mark.parametrize("polls", [[None, None, 0], [0, 0]])
def test_genuine_permission_failure_is_preserved_and_final_wait_runs(tmp_path, monkeypatch, polls):
    process = FakeProcess(polls=polls)
    signals = []

    def killpg(pid, signum):
        signals.append(signum)
        if signum == signal.SIGTERM:
            raise PermissionError("genuine")

    patch_process(monkeypatch, process, killpg)
    with pytest.raises(PermissionError, match="genuine"):
        harness.run_command(["unused"], tmp_path, {}, tmp_path / "child.log")
    assert signals == [signal.SIGTERM, signal.SIGKILL]
    assert process.waits == [600, 5]


def test_repeated_permission_failure_after_exit_retry_is_visible(tmp_path, monkeypatch):
    process = FakeProcess(polls=[None, 0, 0])
    signals = []

    def killpg(pid, signum):
        signals.append(signum)
        if signum == signal.SIGTERM:
            raise PermissionError("repeated")

    patch_process(monkeypatch, process, killpg)
    with pytest.raises(PermissionError, match="repeated"):
        harness.run_command(["unused"], tmp_path, {}, tmp_path / "child.log")
    assert signals == [signal.SIGTERM, signal.SIGTERM, signal.SIGKILL]
    assert process.waits == [600, 5]


def test_final_wait_runs_when_kill_signal_fails(tmp_path, monkeypatch):
    process = FakeProcess()

    def killpg(pid, signum):
        if signum == signal.SIGKILL:
            raise PermissionError("kill failed")

    patch_process(monkeypatch, process, killpg)
    with pytest.raises(PermissionError, match="kill failed"):
        harness.run_command(["unused"], tmp_path, {}, tmp_path / "child.log")
    assert process.waits == [600, 5, 5]


def test_successful_cleanup_preserves_original_command_error(tmp_path, monkeypatch):
    original = subprocess.TimeoutExpired(["unused"], 1)
    process = FakeProcess(initial_wait_error=original)
    patch_process(monkeypatch, process, lambda pid, signum: None)

    with pytest.raises(subprocess.TimeoutExpired) as error:
        harness.run_command(["unused"], tmp_path, {}, tmp_path / "child.log", timeout=1)
    assert error.value is original


def test_timeout_stops_term_ignoring_descendant_after_parent_exits(tmp_path):
    heartbeat = tmp_path / "heartbeat"
    child = """
import signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(sys.argv[1], 'a') as stream:
    while True:
        stream.write('alive\\n')
        stream.flush()
        time.sleep(0.05)
"""
    parent = """
import subprocess, sys, time
subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]])
time.sleep(60)
"""
    with pytest.raises(subprocess.TimeoutExpired):
        harness.run_command(
            [sys.executable, "-c", parent, child, str(heartbeat)],
            tmp_path,
            os.environ.copy(),
            tmp_path / "child.log",
            timeout=1,
        )
    assert heartbeat.exists()
    time.sleep(0.1)
    stopped = heartbeat.read_bytes()
    time.sleep(0.2)
    assert heartbeat.read_bytes() == stopped


@pytest.mark.parametrize("signum", [signal.SIGINT, signal.SIGTERM])
def test_operator_signal_cleans_child_group_and_records_failure(tmp_path, signum):
    heartbeat = tmp_path / "heartbeat"
    group = tmp_path / "child-group"
    evidence = tmp_path / "evidence"
    child = """
import signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
with open(sys.argv[1], 'a') as stream:
    while True:
        stream.write('alive\\n')
        stream.flush()
        time.sleep(0.05)
"""
    parent = """
import os, subprocess, sys, time
from pathlib import Path
Path(sys.argv[3]).write_text(str(os.getpid()))
subprocess.Popen([sys.executable, '-c', sys.argv[1], sys.argv[2]])
time.sleep(60)
"""
    entrypoint = """
import importlib.util, os, sys
from pathlib import Path
spec = importlib.util.spec_from_file_location('harness', sys.argv[1])
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)

command_args = sys.argv[3:]
def verify(output):
    harness.run_command([sys.executable, '-c', *command_args], output, os.environ.copy(), output/'child.log')
harness.verify = verify
sys.argv = [sys.argv[1], '--output', sys.argv[2]]
raise SystemExit(harness.main())
"""
    process = subprocess.Popen(
        [
            sys.executable,
            "-c",
            entrypoint,
            str(SCRIPT),
            str(evidence),
            parent,
            child,
            str(heartbeat),
            str(group),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        deadline = time.monotonic() + 5
        while not heartbeat.exists() and process.poll() is None and time.monotonic() < deadline:
            time.sleep(0.05)
        assert heartbeat.exists()
        process.send_signal(signum)
        _, stderr = process.communicate(timeout=10)
        assert process.returncode == 128 + signum, stderr.decode()
        report = harness.json.loads((evidence / "report.json").read_text())
        assert report["status"] == "failed" and report["error"] == signal.Signals(signum).name
        time.sleep(0.1)
        stopped = heartbeat.read_bytes()
        time.sleep(0.2)
        assert heartbeat.read_bytes() == stopped
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        if group.exists():
            with suppress(ProcessLookupError):
                os.killpg(int(group.read_text()), signal.SIGKILL)


def test_installed_cli_timeout_tracks_default_refresh_budget(tmp_path, monkeypatch):
    output = tmp_path / "evidence"
    output.mkdir()
    package = output / "venv/lib/python3.12/site-packages/oddsfox_catalogue"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "cli.py").write_text("")
    calls = []

    def command(argv, cwd, env, log, **kwargs):
        if log.name == "build.log":
            dist = output / "dist"
            dist.mkdir()
            (dist / "package.whl").touch()
            (dist / "package.tar.gz").touch()
        elif log.name == "installed-origin.log":
            log.write_text(str(package / "__init__.py"))
        elif log.name == "init.json":
            calls.append((argv, kwargs))
            raise RuntimeError("captured installed CLI")

    monkeypatch.setattr(harness, "run_command", command)
    monkeypatch.setattr(harness, "freeze_resources", lambda root: None)
    monkeypatch.setenv("CATALOGUE_CATALOGUE_MAX_DURATION_S", "1")
    with pytest.raises(RuntimeError, match="captured installed CLI"):
        harness.verify(output)
    assert calls[0][0][-1] == "init"
    assert calls[0][1]["timeout"] == harness.CurrentLimits().max_duration_s + 10 == 1210
