import importlib.util
import json
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


@pytest.mark.parametrize("missing", [None, "report-count", "rebuild-entry"])
def test_verify_accepts_expanded_inventories_and_rejects_incomplete_comparisons(
    tmp_path, monkeypatch, missing
):
    semantic = (*harness.SEMANTIC_RELATIONS, "core.future_state", "history.future_evidence")
    operational = (*harness.OPERATIONAL_RELATIONS, "marts.future_operations")
    projections = {**harness.projection_queries(), "future_projection": "select 1"}
    schemas = (*semantic, *operational)
    comparisons = {
        **dict.fromkeys(semantic, {}),
        **dict.fromkeys(("schema:" + name for name in schemas), {}),
        **dict.fromkeys(("published:" + name for name in projections), {}),
        "coverage": {},
        "capture_inventory": {},
    }
    monkeypatch.setattr(harness, "SEMANTIC_RELATIONS", semantic)
    monkeypatch.setattr(harness, "OPERATIONAL_RELATIONS", operational)
    monkeypatch.setattr(harness, "projection_queries", lambda: projections)
    measured = {
        "http_attempts": 2,
        "downloaded_bytes": 17,
        "exhausted": False,
        "responses": [
            {"path": "/markets/5001", "status": 200, "bytes": 8},
            {"path": "/markets/5001", "status": 200, "bytes": 9},
        ],
    }

    class Source:
        url = "http://127.0.0.1:9"

        def __init__(self, *args):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def accounting(self):
            return measured

    monkeypatch.setattr(harness, "GammaServer", Source)
    resources = tmp_path / "venv/lib/site-packages/oddsfox_catalogue/dbt"
    calls = []

    def child(argv, cwd, env, log, timeout=600):
        calls.append(log.name)
        if log.name == "build.log":
            dist = tmp_path / "dist"
            dist.mkdir()
            (dist / "catalogue.whl").write_bytes(b"wheel")
            (dist / "catalogue.tar.gz").write_bytes(b"sdist")
        elif log.name == "distribution-audit.json":
            log.write_text('{"verified": true}')
        elif log.name == "installed-probe.log":
            resources.mkdir(parents=True)
            (resources / "dbt_project.yml").write_text("project")
            (resources / "profiles.yml").write_text("profile")
            (tmp_path / "installed-paths.json").write_text(
                json.dumps(
                    {
                        "cli": str(resources.parent / "cli.py"),
                        "smoke": str(resources.parent / "smoke.py"),
                        "dbt": str(resources),
                        "sys_path": [str(resources.parent.parent)],
                    }
                )
            )
        elif log.name in {"catalogue-smoke.log", "metadata-smoke.log"}:
            workflow = log.name.removesuffix("-smoke.log")
            proof = tmp_path / workflow
            proof.mkdir()
            counts = {
                "warehouse": len(semantic),
                "warehouse_schemas": len(schemas),
                "published": len(projections),
            }
            if missing == "report-count":
                counts["warehouse"] -= 1
            acquisition = "capture_1" if workflow == "catalogue" else "refresh"
            steps = [{"name": acquisition, "result": {"http_attempts": 2, "downloaded_bytes": 17}}]
            steps.extend(
                {"name": name}
                for name in (
                    "raw_rebuild",
                    "backup_create",
                    "backup_verify",
                    "backup_restore",
                    "verify_restore",
                    "injected_publication",
                    "verify_after_failure",
                    "offline_replay",
                )
            )
            (proof / "report.json").write_text(
                json.dumps(
                    {
                        "workflow": workflow,
                        "status": "passed",
                        "accounting_complete": True,
                        "http_attempts": 2,
                        "downloaded_bytes": 17,
                        "catalogue_root": str(proof / "catalogue"),
                        "steps": steps,
                        "semantic_comparisons": {
                            "offline_replay": True,
                            "raw_rebuild": True,
                            "restore": True,
                        },
                        "compared_relations": counts,
                    }
                )
            )
        elif log.name.startswith("rebuild-step-"):
            number = int(log.stem.rsplit("-", 1)[1])
            tables = dict(comparisons) if number == 4 else {}
            if missing == "rebuild-entry" and number == 4:
                tables.pop("core.future_state")
            Path(argv[4]).write_text(
                json.dumps(
                    {
                        "pending": number < 4,
                        "matched": number == 4,
                        "mismatches": [],
                        "tables": tables,
                    }
                )
            )

    monkeypatch.setattr(harness, "run_command", child)
    try:
        if missing is None:
            result = harness.verify(tmp_path)
            assert result["status"] == "passed"
            assert set(result["workflows"]) == {"catalogue", "metadata"}
            assert "rebuild-step-4.log" in calls and "metadata-smoke.log" in calls
        else:
            with pytest.raises(ValueError, match="semantic comparisons"):
                harness.verify(tmp_path)
            assert "metadata-smoke.log" not in calls
    finally:
        if resources.exists():
            resources.chmod(0o700)
            for path in resources.iterdir():
                path.chmod(0o600)
