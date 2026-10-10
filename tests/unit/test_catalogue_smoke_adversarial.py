"""Whole-workflow smoke bounds survive failures, exact limits and descendant processes."""

import json
import os
import signal
import subprocess
import sys
import time
from collections import deque
from contextlib import suppress
from copy import deepcopy

import pytest

from oddsfox_catalogue import smoke

MIB = 1024**2


def catalogue_report(*, duration=30.0):
    return {
        "workflow": "catalogue",
        "steps": [],
        "limits": {
            "http_attempts": 100,
            "download_bytes": 64 * MIB,
            "retained_bytes": 1024**3,
            "duration_s": 1800,
        },
        "deadline_monotonic": time.monotonic() + duration,
    }


@pytest.fixture
def processes(monkeypatch):
    """Observe child inputs and timeouts without running catalogue subprocesses."""
    replies, launches = deque(), []

    class Process:
        def __init__(self, argv, **kwargs):
            self.args = argv
            self.returncode = None
            self.pid = 999_999_999
            self.launch = {"argv": argv, **kwargs}
            launches.append(self.launch)
            self.code, self.output = replies.popleft()

        def poll(self):
            return self.returncode

        def wait(self, *, timeout=None):
            self.launch.setdefault("timeouts", []).append(timeout)
            if self.returncode is None:
                self.launch["stdout"].write(self.output.encode())
                self.launch["stdout"].flush()
                self.returncode = self.code
            return self.returncode

    monkeypatch.setattr(smoke.subprocess, "Popen", Process)
    return replies, launches


def reply(value, code=0):
    return code, json.dumps(value)


def test_all_captures_share_the_budget_and_offline_work_can_finish_at_the_limit(
    processes, tmp_path
):
    replies, launches = processes
    report = catalogue_report()
    replies.extend(
        [
            reply({"http_attempts": 60, "downloaded_bytes": 40 * MIB}),
            reply({"http_attempts": 40, "downloaded_bytes": 24 * MIB}),
            reply({"verified": True}),
        ]
    )
    env = {"CATALOGUE_ROOT": str(tmp_path / "catalogue")}
    first_env = smoke.remaining_environment(tmp_path, report, env)
    assert int(first_env["CATALOGUE_CAPTURE_MAX_REQUESTS"]) == 100
    assert int(first_env["CATALOGUE_CAPTURE_MAX_DOWNLOAD_BYTES"]) == 64 * MIB
    smoke.command(report, "first_capture", ["synthetic"], first_env, capture=True)
    second_env = smoke.remaining_environment(tmp_path, report, env)
    assert int(second_env["CATALOGUE_CAPTURE_MAX_REQUESTS"]) == 40
    assert int(second_env["CATALOGUE_CAPTURE_MAX_DOWNLOAD_BYTES"]) == 24 * MIB
    smoke.command(report, "second_capture", ["synthetic"], second_env, capture=True)
    with pytest.raises(smoke.SmokeFailure):
        smoke.command(report, "forbidden_capture", ["synthetic"], {}, capture=True)
    assert len(launches) == 2
    smoke.command(report, "offline_verify", ["synthetic"], {})
    assert len(launches) == 3


def test_failed_capture_bytes_and_attempts_are_deducted_from_the_next_allowance(
    processes, tmp_path
):
    replies, _ = processes
    report = catalogue_report()
    replies.append(reply({"http_attempts": 7, "downloaded_bytes": 19, "message": "failed"}, 3))
    with pytest.raises(smoke.SmokeFailure):
        smoke.command(report, "capture", ["synthetic"], {}, capture=True)
    env = smoke.remaining_environment(
        tmp_path, report, {"CATALOGUE_ROOT": str(tmp_path / "catalogue")}
    )
    assert int(env["CATALOGUE_CAPTURE_MAX_REQUESTS"]) == 93
    assert int(env["CATALOGUE_CAPTURE_MAX_DOWNLOAD_BYTES"]) == 64 * MIB - 19
    assert smoke.finish(tmp_path, report, smoke.SmokeFailure("failed")) == 1
    saved = json.loads((tmp_path / "report.json").read_bytes())
    assert saved["http_attempts"] == 7
    assert saved["downloaded_bytes"] == 19


@pytest.mark.parametrize(
    "accounting",
    [
        {"http_attempts": 101, "downloaded_bytes": 0},
        {"http_attempts": 1, "downloaded_bytes": 64 * MIB + 1},
    ],
)
def test_child_accounting_above_the_shared_limit_is_a_failed_smoke(processes, tmp_path, accounting):
    replies, _ = processes
    report = catalogue_report()
    replies.append(reply(accounting))
    with pytest.raises(smoke.SmokeFailure):
        smoke.command(report, "capture", ["synthetic"], {}, capture=True)
    smoke.finish(tmp_path, report, smoke.SmokeFailure("shared allowance exceeded"))
    saved = json.loads((tmp_path / "report.json").read_bytes())
    assert saved["status"] == "failed"
    assert saved["http_attempts"] == accounting["http_attempts"]
    assert saved["downloaded_bytes"] == accounting["downloaded_bytes"]


def test_ambient_operator_paths_and_limits_cannot_redirect_or_expand_the_smoke(
    monkeypatch, tmp_path
):
    for name, value in {
        "CATALOGUE_ROOT": str(tmp_path / "old-root"),
        "CATALOGUE_CONFIG": str(tmp_path / "unsafe-config.toml"),
        "CATALOGUE_PATHS_DATA_DIR": str(tmp_path / "old-data"),
        "CATALOGUE_PATHS_STATE_DIR": str(tmp_path / "old-state"),
        "CATALOGUE_CAPTURE_MAX_REQUESTS": "99999999",
        "CATALOGUE_CAPTURE_MAX_DOWNLOAD_BYTES": "99999999999",
        "CATALOGUE_CAPTURE_MAX_RETAINED_BYTES": "99999999999",
        "CATALOGUE_CAPTURE_MAX_DURATION_S": "99999999",
        "CATALOGUE_CAPTURE_WORKERS": "99",
    }.items():
        monkeypatch.setenv(name, value)
    output = tmp_path / "fresh"
    env = smoke.catalogue_environment(output)
    assert env["CATALOGUE_ROOT"] == str(output / "catalogue")
    assert not any(
        name in env
        for name in ("CATALOGUE_CONFIG", "CATALOGUE_PATHS_DATA_DIR", "CATALOGUE_PATHS_STATE_DIR")
    )
    env = smoke.remaining_environment(output, catalogue_report(), env)
    assert int(env["CATALOGUE_CAPTURE_MAX_REQUESTS"]) == 100
    assert int(env["CATALOGUE_CAPTURE_MAX_DOWNLOAD_BYTES"]) == 64 * MIB
    assert int(env["CATALOGUE_CAPTURE_MAX_RETAINED_BYTES"]) <= 1024**3
    assert float(env["CATALOGUE_CAPTURE_MAX_DURATION_S"]) <= 30
    assert env["CATALOGUE_CAPTURE_WORKERS"] == "1"


def test_other_smoke_roots_and_artifacts_reduce_the_current_roots_storage_allowance(tmp_path):
    root = tmp_path / "catalogue"
    root.mkdir()
    (root / "existing").write_bytes(b"a" * 100)
    (tmp_path / "backup").write_bytes(b"b" * 250)
    (tmp_path / "restored").write_bytes(b"c" * 300)
    report = catalogue_report()
    env = smoke.remaining_environment(tmp_path, report, {"CATALOGUE_ROOT": str(root)})
    assert int(env["CATALOGUE_CAPTURE_MAX_RETAINED_BYTES"]) <= 1024**3 - 550


def test_storage_over_the_whole_workflow_limit_prevents_the_next_child(processes, tmp_path):
    _, launches = processes
    with (tmp_path / "retained").open("wb") as handle:
        handle.truncate(1024**3 + 1)
    report = catalogue_report()
    report["output"] = str(tmp_path)
    with pytest.raises(smoke.SmokeFailure, match="storage"):
        smoke.command(
            report,
            "capture",
            ["synthetic"],
            {"CATALOGUE_ROOT": str(tmp_path / "catalogue")},
            capture=True,
        )
    assert launches == []


@pytest.mark.parametrize(
    "accounting",
    [
        {},
        {"http_attempts": 1},
        {"downloaded_bytes": 1},
        {"http_attempts": True, "downloaded_bytes": 1},
        {"http_attempts": -1, "downloaded_bytes": 1},
        {"http_attempts": 1.0, "downloaded_bytes": 1},
        {"http_attempts": 1, "downloaded_bytes": False},
        {"http_attempts": 1, "downloaded_bytes": -1},
        {"http_attempts": 1, "downloaded_bytes": "2"},
    ],
)
def test_missing_or_invalid_capture_accounting_cannot_claim_complete_evidence(
    processes, tmp_path, accounting
):
    replies, _ = processes
    report = catalogue_report()
    replies.append(reply(accounting))
    with pytest.raises(smoke.SmokeFailure):
        smoke.command(report, "capture", ["synthetic"], {}, capture=True)
    smoke.finish(tmp_path, report, smoke.SmokeFailure("invalid capture accounting"))
    saved = json.loads((tmp_path / "report.json").read_bytes())
    assert saved["status"] == "failed"
    assert saved["accounting_complete"] is False


def test_deadline_shrinks_across_steps_and_expiry_prevents_another_child(
    processes, monkeypatch, tmp_path
):
    replies, launches = processes
    report = catalogue_report()
    clock = [100.0]
    report["deadline_monotonic"] = 110.0
    monkeypatch.setattr(smoke.time, "monotonic", lambda: clock[0])
    replies.extend([reply({"verified": True}), reply({"verified": True})])
    base_env = {"CATALOGUE_ROOT": str(tmp_path / "catalogue")}
    first_env = smoke.remaining_environment(tmp_path, report, base_env)
    smoke.command(report, "first", ["synthetic"], first_env)
    clock[0] = 107.0
    second_env = smoke.remaining_environment(tmp_path, report, base_env)
    smoke.command(report, "second", ["synthetic"], second_env)
    assert float(launches[0]["env"]["CATALOGUE_CAPTURE_MAX_DURATION_S"]) == pytest.approx(10.0)
    assert float(launches[1]["env"]["CATALOGUE_CAPTURE_MAX_DURATION_S"]) == pytest.approx(3.0)
    assert launches[0]["timeouts"][0] <= 10.0
    assert launches[1]["timeouts"][0] <= 3.0
    clock[0] = 110.0
    with pytest.raises(smoke.SmokeFailure):
        smoke.command(report, "expired", ["synthetic"], {})
    assert len(launches) == 2


def test_only_the_expected_offline_crash_has_complete_accounting(processes, tmp_path):
    replies, _ = processes
    report = catalogue_report()
    replies.append((87, ""))
    smoke.command(report, "injected_publish", ["synthetic"], {}, expected_exit=87)
    smoke.finish(tmp_path, report)
    saved = json.loads((tmp_path / "report.json").read_bytes())
    assert saved["accounting_complete"] is True
    assert saved["http_attempts"] == saved["downloaded_bytes"] == 0


@pytest.mark.parametrize("code", [0, 1, 86, 88])
def test_wrong_exit_cannot_pass_the_publish_crash_check(processes, tmp_path, code):
    replies, _ = processes
    report = catalogue_report()
    replies.append((code, ""))
    with pytest.raises(smoke.SmokeFailure):
        smoke.command(report, "injected_publish", ["synthetic"], {}, expected_exit=87)
    smoke.finish(tmp_path, report, smoke.SmokeFailure("fault not proven"))
    assert json.loads((tmp_path / "report.json").read_bytes())["status"] == "failed"


@pytest.fixture
def successful_workflow(monkeypatch, tmp_path):
    """Valid earlier witnesses let each recovery diagnostic be contradicted independently."""
    from oddsfox_catalogue.contract import projection_queries
    from oddsfox_catalogue.semantics import OPERATIONAL_RELATIONS, SEMANTIC_RELATIONS

    initial = {
        "capture": {"batch_ids": ["first"]},
        "warehouse": {
            "relations": {name: {"rows": 1} for name in SEMANTIC_RELATIONS},
            "schemas": {name: [] for name in (*SEMANTIC_RELATIONS, *OPERATIONAL_RELATIONS)},
        },
        "published": {name: {"rows": 1} for name in projection_queries()},
    }
    second = deepcopy(initial)
    second["capture"]["batch_ids"].append("second")
    second["warehouse"]["relations"]["history.market_history"]["rows"] = 2
    second["warehouse"]["relations"]["bronze.market_observations"]["rows"] = 2
    states = iter([initial, second, second, second, second])
    manifest = {"tables": {"quarantine": {"rows": 0}}}
    monkeypatch.setattr(
        smoke, "_catalogue_state", lambda *a: (deepcopy(next(states)), manifest, {})
    )
    monkeypatch.setattr(smoke, "_metadata_fields", lambda *a: None)
    pointer = tmp_path / "catalogue/data/published/current.json"
    pointer.parent.mkdir(parents=True)
    pointer.write_bytes(b"unchanged verified pointer")
    restored_pointer = tmp_path / "restored/data/published/current.json"
    restored_pointer.parent.mkdir(parents=True)
    restored_pointer.write_bytes(pointer.read_bytes())
    backup = tmp_path / "backups/verified"
    backup.mkdir(parents=True)
    comparison_names = {
        *SEMANTIC_RELATIONS,
        *("schema:" + name for name in (*SEMANTIC_RELATIONS, *OPERATIONAL_RELATIONS)),
        *("published:" + name for name in projection_queries()),
        "coverage",
        "capture_inventory",
    }
    witnesses = {
        "capture_1": {"status": "captured", "records": 1, "batch_id": "first"},
        "capture_2": {"status": "captured", "records": 1, "batch_id": "second"},
        "load_1": {"pages_loaded": 1},
        "load_2": {"pages_loaded": 1},
        "offline_export": {"http_attempts": 0, "found": 1, "failed": 0},
        "raw_rebuild": {
            "matched": True,
            "mismatches": [],
            "tables": {name: {} for name in comparison_names},
        },
        "backup_create": {"verified": True, "backup": str(backup)},
        "backup_verify": {"verified": True, "problems": []},
        "backup_restore": {"restored": True, "destination": str(tmp_path / "restored")},
        "runtime_evidence": {
            "quarantine_reasons": {},
            "runtime": {
                "python": sys.version.split()[0],
                "package": "0.1.0",
                "dbt_resource_sha256": "0" * 64,
                "smoke_source_sha256": "1" * 64,
            },
        },
    }
    launches = []

    def run(report, name, argv, env=None, **kwargs):
        launches.append(name)
        return deepcopy(witnesses.get(name, {"verified": True}))

    monkeypatch.setattr(smoke, "command", run)
    return witnesses, launches, pointer, restored_pointer


@pytest.mark.parametrize(
    "step,field",
    [("raw_rebuild", "matched"), ("backup_verify", "verified"), ("backup_restore", "restored")],
)
def test_zero_exit_with_a_negative_recovery_witness_cannot_claim_success(
    successful_workflow, tmp_path, step, field
):
    witnesses, launches, _, _ = successful_workflow
    witnesses[step][field] = False
    report = {"steps": []}
    with pytest.raises(smoke.SmokeFailure):
        smoke.run_catalogue(tmp_path, ["1"], report)
    assert launches[-1] == step
    assert "injected_publication" not in launches
    assert "recovery" not in report


def test_equal_sized_rebuild_inventory_cannot_replace_a_required_semantic_relation(
    successful_workflow, tmp_path
):
    witnesses, launches, _, _ = successful_workflow
    comparisons = witnesses["raw_rebuild"]["tables"]
    previous_count = len(comparisons)
    comparisons.pop("history.market_history")
    comparisons["unrelated_relation"] = {}
    assert len(comparisons) == previous_count
    report = {"steps": []}
    with pytest.raises(smoke.SmokeFailure, match="declared comparisons"):
        smoke.run_catalogue(tmp_path, ["1"], report)
    assert launches[-1] == "raw_rebuild"
    assert "backup_create" not in launches
    assert "recovery" not in report


def test_termination_records_incomplete_evidence_and_restores_the_signal_handlers(
    monkeypatch, tmp_path
):
    from oddsfox_catalogue.signals import Terminated

    previous_handlers = {
        signum: signal.getsignal(signum)
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    }

    def terminate_capture(output, market_ids, report):
        report["steps"].append(
            {"name": "capture_1", "capture": True, "successful": False, "exit_code": 143}
        )
        raise Terminated(signal.SIGTERM)

    monkeypatch.setattr(smoke, "run_catalogue", terminate_capture)
    output = tmp_path / "interrupted"
    assert (
        smoke.main(["--workflow", "catalogue", "--market-id", "1", "--output", str(output)]) == 143
    )
    saved = json.loads((output / "report.json").read_bytes())
    assert saved["status"] == "failed"
    assert saved["error"] == "Terminated"
    assert saved["accounting_complete"] is False
    assert saved["steps"][0]["name"] == "capture_1"
    assert {signum: signal.getsignal(signum) for signum in previous_handlers} == previous_handlers


def test_semantically_equal_restoration_must_preserve_the_same_verified_pointer(
    successful_workflow, tmp_path
):
    _, launches, _, restored = successful_workflow
    restored.write_bytes(b"a different release with equal semantic rows")
    report = {"steps": []}
    with pytest.raises(smoke.SmokeFailure, match="pointer"):
        smoke.run_catalogue(tmp_path, ["1"], report)
    assert "injected_publication" not in launches
    assert "recovery" not in report


def test_expected_crash_code_cannot_hide_a_replaced_current_pointer(
    successful_workflow, monkeypatch, tmp_path
):
    _, launches, pointer, _ = successful_workflow
    original = smoke.command

    def mutate_pointer(report, name, argv, env=None, **kwargs):
        result = original(report, name, argv, env, **kwargs)
        if name == "injected_publication":
            assert kwargs["expected_exit"] == 87
            pointer.write_bytes(b"incorrectly replaced")
        return result

    monkeypatch.setattr(smoke, "command", mutate_pointer)
    report = {"steps": []}
    with pytest.raises(smoke.SmokeFailure, match="pointer"):
        smoke.run_catalogue(tmp_path, ["1"], report)
    assert launches[-1] == "injected_publication"
    assert "recovery" not in report


@pytest.mark.skipif(os.name != "posix", reason="The package's supported workflow uses POSIX groups")
def test_semantic_helper_is_a_bounded_child_and_a_stall_leaves_failed_evidence(
    monkeypatch, tmp_path
):
    report = catalogue_report(duration=5.0)
    report["output"] = str(tmp_path)
    env = smoke.catalogue_environment(tmp_path)
    marker = tmp_path / "catalogue/helper-entered"
    heartbeat = tmp_path / "catalogue/helper-alive"
    prelude = """
import json, os, pathlib, time
from oddsfox_catalogue import certification
def stalled_certification(settings):
    root = settings.root
    root.mkdir(parents=True, exist_ok=True)
    marker = root / 'helper-entered.tmp'
    marker.write_text(json.dumps({'pid': os.getpid(), 'pgid': os.getpgrp(), 'uid': os.getuid()}))
    marker.replace(root / 'helper-entered')
    while True:
        (root / 'helper-alive').write_text(str(time.monotonic_ns()))
        time.sleep(0.02)
certification.assert_build_valid = stalled_certification
"""
    real_popen = subprocess.Popen
    children = []

    def launch_instrumented_helper(argv, **kwargs):
        assert argv[1:3] == ["-I", "-c"], "Semantic checks must use an isolated child"
        instrumented = [*argv[:3], prelude + "\n" + argv[3], *argv[4:]]
        child = real_popen(instrumented, **kwargs)
        children.append(child)
        while not marker.exists() and time.monotonic() < report["deadline_monotonic"]:
            if child.poll() is not None:
                break
            time.sleep(0.01)
        if marker.exists():
            identity = json.loads(marker.read_bytes())
            assert identity == {"pid": child.pid, "pgid": child.pid, "uid": os.getuid()}
            assert os.getpgid(child.pid) == child.pid
        # Expire during the real helper operation, allowing variable import startup time.
        report["deadline_monotonic"] = min(report["deadline_monotonic"], time.monotonic() + 0.15)
        return child

    monkeypatch.setattr(smoke.subprocess, "Popen", launch_instrumented_helper)
    started = time.monotonic()
    try:
        with pytest.raises(subprocess.TimeoutExpired):
            smoke._catalogue_state(tmp_path, report, env)
        assert time.monotonic() - started < 7.0
        assert marker.exists(), "The installed helper must reach the stalled semantic query"
        assert len(children) == 1 and children[0].poll() is not None
        before = heartbeat.read_bytes()
        time.sleep(0.1)
        assert heartbeat.read_bytes() == before
        smoke.finish(tmp_path, report, subprocess.TimeoutExpired("semantic helper", 5))
        saved = json.loads((tmp_path / "report.json").read_bytes())
        assert saved["status"] == "failed"
        assert saved["accounting_complete"] is False
        assert saved["steps"][-1]["name"] == "semantic_snapshot"
    finally:
        for child in children:
            with suppress(ProcessLookupError):
                child.kill()
            child.wait(timeout=5)


@pytest.mark.skipif(os.name != "posix", reason="The package's supported workflow uses POSIX groups")
@pytest.mark.parametrize("parent_exits", [False, True])
def test_timeout_stops_term_ignoring_descendants_even_after_the_group_leader_exits(
    tmp_path, parent_exits
):
    child_script = """
import os, pathlib, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
root = pathlib.Path(sys.argv[1])
(root / 'grandchild.pid').write_text(str(os.getpid()))
while True:
    (root / 'grandchild.alive').write_text(str(time.monotonic_ns()))
    time.sleep(0.02)
"""
    parent_script = """
import os, pathlib, signal, subprocess, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
root = pathlib.Path(sys.argv[1])
(root / 'parent.pid').write_text(str(os.getpid()))
subprocess.Popen([sys.executable, '-c', sys.argv[2], str(root)])
while not (root / 'grandchild.pid').exists():
    time.sleep(0.01)
if sys.argv[3] == 'exit':
    sys.exit(0)
while True:
    (root / 'parent.alive').write_text(str(time.monotonic_ns()))
    time.sleep(0.02)
"""
    report = catalogue_report(duration=0.6)
    started = time.monotonic()
    try:
        with pytest.raises((smoke.SmokeFailure, subprocess.TimeoutExpired)):
            smoke.command(
                report,
                "timeout",
                [
                    sys.executable,
                    "-c",
                    parent_script,
                    str(tmp_path),
                    child_script,
                    "exit" if parent_exits else "stay",
                ],
                dict(os.environ),
            )
        assert time.monotonic() - started < 8.0
        assert (tmp_path / "grandchild.pid").exists(), "The descendant must run before timeout"
        heartbeats = list(tmp_path.glob("*.alive"))
        assert heartbeats
        before = {path: path.read_bytes() for path in heartbeats}
        time.sleep(0.15)
        assert {path: path.read_bytes() for path in heartbeats} == before
    finally:
        # A regression must not leave the test's descendants running.
        if (tmp_path / "parent.pid").exists():
            with suppress(ProcessLookupError):
                os.killpg(int((tmp_path / "parent.pid").read_text()), signal.SIGKILL)
