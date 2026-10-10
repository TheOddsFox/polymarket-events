import importlib.util
import os
import signal
import subprocess
import sys
import time
from contextlib import suppress
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts/verify-installed-workflow.py"
spec = importlib.util.spec_from_file_location("installed_workflow_harness", SCRIPT)
assert spec and spec.loader
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)


def test_resource_freeze_preserves_inventory_and_detects_tampering(tmp_path):
    (tmp_path / "dbt_project.yml").write_text("project")
    (tmp_path / "profiles.yml").write_text("profile")
    before = harness.resource_inventory(tmp_path)
    harness.freeze_resources(tmp_path)
    try:
        harness.assert_resources_frozen(tmp_path)
        assert harness.resource_inventory(tmp_path) == before
        (tmp_path / "profiles.yml").chmod(0o600)
        with pytest.raises(ValueError, match="writable"):
            harness.assert_resources_frozen(tmp_path)
        (tmp_path / "profiles.yml").write_text("tampered")
        assert harness.resource_inventory(tmp_path) != before
    finally:
        tmp_path.chmod(0o700)
        for path in tmp_path.iterdir():
            path.chmod(0o600)


def test_resource_inventory_rejects_symlink(tmp_path):
    (tmp_path / "dbt_project.yml").write_text("project")
    (tmp_path / "profiles.yml").symlink_to(tmp_path / "dbt_project.yml")
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
