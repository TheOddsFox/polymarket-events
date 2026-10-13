"""Finite installed-command supervision and honest workspace-wide accounting."""

import json
import signal

import pytest

from oddsfox_catalogue import acceptance


def test_combined_storage_counts_sibling_roots_and_temp_once(tmp_path, monkeypatch):
    monkeypatch.setattr(acceptance, "FREE_RESERVE", 0)
    source = tmp_path / "source" / ".state" / "dlt"
    source.mkdir(parents=True)
    (source / "jobs").write_bytes(b"a" * 40)
    backup = tmp_path / "backup" / "dlt_pipelines"
    backup.mkdir(parents=True)
    (backup / "jobs").write_bytes(b"b" * 30)
    (tmp_path / "root-evidence").write_bytes(b"c" * 10)
    assert acceptance.storage(tmp_path)["retained_bytes"] == 80
    assert acceptance.storage(tmp_path)["temporary_bytes"] == 70
    monkeypatch.setattr(acceptance, "RETAINED_LIMIT", 89)
    with pytest.raises(ValueError, match="retained"):
        acceptance.storage(tmp_path, additional_bytes=10)
    monkeypatch.setattr(acceptance, "TEMP_LIMIT", 69)
    with pytest.raises(ValueError, match="temporary"):
        acceptance.storage(tmp_path)


@pytest.mark.parametrize("nested", [False, True])
@pytest.mark.parametrize("custom_state", [False, True])
def test_child_temp_quota_reserves_other_roots_and_stays_stable_as_scratch_grows(
    tmp_path, monkeypatch, nested, custom_state
):
    monkeypatch.setattr(acceptance, "FREE_RESERVE", 0)
    monkeypatch.setattr(acceptance, "TEMP_LIMIT", 100)
    root = tmp_path / "source"
    state = root / ("custom-state" if custom_state else ".state")
    if custom_state:
        (root / "config").mkdir(parents=True)
        (root / "config/catalogue.toml").write_text('[paths]\nstate_dir="custom-state"\n')
    own = state / "tmp"
    own.mkdir(parents=True)
    (own / "source-spill").write_bytes(b"s" * 10)
    other = tmp_path / "other" / ".state" / "dlt"
    other.mkdir(parents=True)
    (other / "jobs").write_bytes(b"o" * 40)
    scratch = state / "rebuild" / "proof" if nested else tmp_path / "scratch"
    jobs = scratch / ".state" / "dlt"
    jobs.mkdir(parents=True)
    (jobs / "job").write_bytes(b"x" * 20)
    environments = []

    class Process:
        pid = 12345
        returncode = 0

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def launch(*args, env, **kwargs):
        environments.append(env)
        return Process()

    monkeypatch.setattr(acceptance.subprocess, "Popen", launch)
    arguments = ["rebuild", "--verify", "--scratch", str(scratch), "--resume"]
    assert acceptance.run(tmp_path, root, arguments) == 0
    (jobs / "job").write_bytes(b"x" * 30)
    assert acceptance.run(tmp_path, root, arguments) == 0
    reports = [
        json.loads(path.read_bytes())
        for path in (tmp_path / "completion-proof").glob("*/report.json")
    ]
    assert sorted(report["before"]["temporary_bytes"] for report in reports) == [70, 80]
    assert [env["CATALOGUE_CAPTURE_MAX_TEMP_BYTES"] for env in environments] == ["60", "60"]
    assert all(report["operator_temporary_allowance_bytes"] == 60 for report in reports)
    assert all(report["max_temp_bytes"] == 100 for report in reports)
    assert all(env["CATALOGUE_LOAD_DUCKDB_MEMORY_LIMIT"] == "2GB" for env in environments)
    assert all(env["CATALOGUE_LOAD_DUCKDB_THREADS"] == "2" for env in environments)


def test_exhausted_other_root_temp_budget_does_not_launch(tmp_path, monkeypatch):
    monkeypatch.setattr(acceptance, "FREE_RESERVE", 0)
    monkeypatch.setattr(acceptance, "TEMP_LIMIT", 10)
    jobs = tmp_path / "other" / ".state" / "dlt"
    jobs.mkdir(parents=True)
    (jobs / "job").write_bytes(b"x" * 10)
    monkeypatch.setattr(
        acceptance.subprocess, "Popen", lambda *args, **kwargs: pytest.fail("launched")
    )
    with pytest.raises(ValueError, match="temporary allowance exhausted"):
        acceptance.run(tmp_path, tmp_path / "source", ["status"])
    assert not (tmp_path / "completion-proof").exists()


def test_configured_temporary_paths_cannot_escape_workspace(tmp_path, monkeypatch):
    root = tmp_path / "source"
    (root / "config").mkdir(parents=True)
    outside = tmp_path.parent / "outside-state"
    (root / "config/catalogue.toml").write_text(
        "[paths]\nstate_dir=" + json.dumps(str(outside)) + "\n"
    )
    monkeypatch.setattr(
        acceptance.subprocess, "Popen", lambda *args, **kwargs: pytest.fail("launched")
    )
    with pytest.raises(ValueError, match="inside.*workspace"):
        acceptance.run(tmp_path, root, ["status"])
    assert not outside.exists()


def test_explicit_temporary_union_deduplicates_nested_and_repeated_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(acceptance, "FREE_RESERVE", 0)
    custom = tmp_path / "custom"
    nested = custom / "nested"
    nested.mkdir(parents=True)
    (custom / "first").write_bytes(b"a" * 10)
    (nested / "second").write_bytes(b"b" * 20)
    assert (
        acceptance.storage(tmp_path, temporary_paths=(custom, nested, custom))["temporary_bytes"]
        == 30
    )


@pytest.mark.parametrize("duration", [float("nan"), float("inf"), 0, -1, 14401, True])
def test_invalid_deadlines_cannot_launch_a_child(tmp_path, duration):
    with pytest.raises(ValueError, match="finite"):
        acceptance.run(tmp_path, tmp_path / "root", ["status"], duration=duration)
    assert list(tmp_path.iterdir()) == []


def test_installed_cli_config_runs_offline_and_records_result(tmp_path, monkeypatch):
    monkeypatch.setattr(acceptance, "FREE_RESERVE", 0)
    monkeypatch.setenv("CATALOGUE_CAPTURE_WORKERS", "1000")
    monkeypatch.setenv("CATALOGUE_GAMMA_BASE_URL", "http://127.0.0.1:9")
    monkeypatch.setenv("CATALOGUE_GAMMA_ALLOW_LOOPBACK", "1")
    assert acceptance.run(tmp_path, tmp_path / "root", ["config", "show"], duration=30) == 0
    report_path = next((tmp_path / "completion-proof").glob("*/report.json"))
    report = json.loads(report_path.read_bytes())
    assert report["status"] == "passed" and report["returncode"] == 0
    config = json.loads((report_path.parent / "stdout.log").read_bytes())
    assert config["capture"]["workers"] == 1
    assert config["capture"]["max_retained_bytes"] == 256 * 1024**3
    assert config["quality"]["quarantine_max_ratio"] == 0.01
    assert report["stop_signal_sent"] is None
    assert not (tmp_path / "root").exists()


def test_runner_rebuilds_owned_raw_ledger_without_overwriting_existing_state(tmp_path, monkeypatch):
    from shutil import copytree

    from fakes.fake_gamma import FakeGamma
    from fakes.harness import build_runtime
    from fakes.world import demo_world
    from oddsfox_catalogue.capture.ledger import Ledger
    from oddsfox_catalogue.capture.runner import run_capture

    monkeypatch.setattr(acceptance, "FREE_RESERVE", 0)
    runtime, _ = build_runtime(tmp_path / "source", FakeGamma(demo_world()))
    try:
        captured = run_capture(runtime, "selected", market_ids=["5001"])
        expected_pages = runtime.ledger.pages_for_batch(captured.batch_id)
        fresh = tmp_path / "fresh"
        copytree(runtime.settings.raw_dir, fresh / "data" / "raw")
    finally:
        runtime.client.close()
        runtime.ledger.close()
    assert expected_pages
    assert acceptance.run(tmp_path, fresh, ["ledger", "rebuild"], duration=30) == 0
    with Ledger(fresh / ".state" / "ledger.sqlite") as ledger:
        actual_pages = ledger.pages_for_batch(captured.batch_id)
        assert {row["page_id"] for row in actual_pages} == {
            row["page_id"] for row in expected_pages
        }
        assert all(row["loaded_at"] is None for row in actual_pages)
    before = (fresh / ".state" / "ledger.sqlite").read_bytes()
    assert acceptance.run(tmp_path, fresh, ["ledger", "rebuild"], duration=30) != 0
    assert (fresh / ".state" / "ledger.sqlite").read_bytes() == before
    reports = [
        json.loads(path.read_bytes())
        for path in (tmp_path / "completion-proof").glob("*/report.json")
    ]
    assert sorted(report["status"] for report in reports) == ["failed", "passed"]


def test_storage_failure_sends_one_child_sighup_and_reaps(tmp_path, monkeypatch):
    original = acceptance.storage
    samples, signals, waits = [], [], []
    monkeypatch.setattr(acceptance, "FREE_RESERVE", 0)

    def storage(*args, **kwargs):
        samples.append(True)
        if len(samples) > 1:
            raise ValueError("injected budget exhaustion")
        return original(*args, **kwargs)

    class Process:
        pid = 12345
        returncode = None

        def poll(self):
            return self.returncode

        def send_signal(self, signum):
            signals.append(signum)
            self.returncode = 129

        def wait(self, timeout=None):
            waits.append(timeout)
            return self.returncode

    monkeypatch.setattr(acceptance, "storage", storage)
    monkeypatch.setattr(acceptance.subprocess, "Popen", lambda *args, **kwargs: Process())
    with pytest.raises(ValueError, match="injected"):
        acceptance.run(tmp_path, tmp_path / "root", ["status"])
    assert signals == [signal.SIGHUP]
    assert waits == [None]
    report = json.loads(next((tmp_path / "completion-proof").glob("*/report.json")).read_bytes())
    assert report["status"] == "failed" and report["stop_signal_sent"] == "SIGHUP"
    assert "injected" not in report.get("error_type", "")


@pytest.mark.parametrize("args", [["backup", "restore"], ["rebuild", "--scratch"], ["shell"]])
def test_malformed_commands_are_rejected_before_output_creation(tmp_path, args):
    with pytest.raises(ValueError):
        acceptance.run(tmp_path, tmp_path / "root", args)
    assert list(tmp_path.iterdir()) == []


def test_workspace_symlink_cannot_be_normalized_away(tmp_path):
    other = tmp_path / "elsewhere"
    other.mkdir()
    (tmp_path / "alias").symlink_to(other, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        acceptance.confined(tmp_path / "alias" / ".." / "new", tmp_path)


@pytest.mark.parametrize(
    "arguments,expected",
    [
        (["capture", "--mode=daily"], "10"),
        (["capture", "--mo", "reconcile"], "10"),
        (["capture", "--mode", "daily", "--mode", "bootstrap"], "2"),
        (["capture", "--mode", "bootstrap", "--mode=daily"], "10"),
    ],
)
def test_rate_uses_effective_parsed_mode(tmp_path, arguments, expected):
    assert (
        acceptance.environment(tmp_path, arguments)["CATALOGUE_GAMMA_REQUESTS_PER_SECOND"]
        == expected
    )


def test_rate_increase_is_only_for_daily_and_reconcile(tmp_path):
    assert (
        acceptance.environment(tmp_path, ["capture", "--mode", "daily"])[
            "CATALOGUE_GAMMA_REQUESTS_PER_SECOND"
        ]
        == "10"
    )
    for args in (["load"], ["capture", "--mode", "selected"], ["capture", "--mode", "bootstrap"]):
        assert acceptance.environment(tmp_path, args)["CATALOGUE_GAMMA_REQUESTS_PER_SECOND"] == "2"


@pytest.mark.parametrize(
    "arguments",
    [
        ["rebuild", "--verify", "--scr={outside}"],
        ["rebuild", "--verify", "--scr", "{outside}"],
        ["backup", "create", "--de={outside}"],
        ["backup", "create", "--de", "{outside}"],
        ["backup", "restore", "{backup}", "--des={outside}"],
        ["backup", "restore", "{backup}", "--des", "{outside}"],
        ["verify", "--rel={outside}"],
        ["verify", "--rel", "{outside}"],
    ],
)
def test_argparse_abbreviations_cannot_escape_workspace(tmp_path, arguments):
    outside = tmp_path.parent / "outside"
    arguments = [arg.format(outside=outside, backup=tmp_path / "backup") for arg in arguments]
    with pytest.raises(ValueError, match="inside.*workspace"):
        acceptance.run(tmp_path, tmp_path / "root", arguments)
    assert list(tmp_path.iterdir()) == []


def test_immediate_exit_cannot_bypass_final_log_allowance(tmp_path, monkeypatch):
    monkeypatch.setattr(acceptance, "FREE_RESERVE", 0)
    monkeypatch.setattr(acceptance, "LOG_LIMIT", 10)

    class Process:
        pid = 12345
        returncode = 0

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def launch(*args, stdout, **kwargs):
        stdout.write(b"x" * 11)
        stdout.flush()
        return Process()

    monkeypatch.setattr(acceptance.subprocess, "Popen", launch)
    with pytest.raises(ValueError, match="diagnostics"):
        acceptance.run(tmp_path, tmp_path / "root", ["status"])
    report = json.loads(next((tmp_path / "completion-proof").glob("*/report.json")).read_bytes())
    assert report["status"] == "failed" and report["returncode"] == 0


def test_immediate_exit_cannot_bypass_final_deadline(tmp_path, monkeypatch):
    monkeypatch.setattr(acceptance, "FREE_RESERVE", 0)
    clock = iter([0, 10, 21, 22])
    monkeypatch.setattr(acceptance.time, "monotonic", lambda: next(clock, 22))

    class Process:
        pid = 12345
        returncode = 0

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    monkeypatch.setattr(acceptance.subprocess, "Popen", lambda *args, **kwargs: Process())
    with pytest.raises(TimeoutError, match="deadline"):
        acceptance.run(tmp_path, tmp_path / "root", ["status"], duration=20)
    report = json.loads(next((tmp_path / "completion-proof").glob("*/report.json")).read_bytes())
    assert report["status"] == "failed" and report["returncode"] == 0


def test_restore_reservation_uses_parsed_path_when_options_precede_backup(tmp_path, monkeypatch):
    backup = tmp_path / "backup"
    backup.mkdir()
    (backup / "member").write_bytes(b"x" * 1000)
    observed = []

    def storage(workspace, *, additional_bytes=0, temporary_paths=()):
        observed.append(additional_bytes)
        raise ValueError("stop after measured preflight")

    monkeypatch.setattr(acceptance, "storage", storage)
    with pytest.raises(ValueError, match="measured preflight"):
        acceptance.run(
            tmp_path,
            tmp_path / "root",
            ["backup", "restore", "--destination=" + str(tmp_path / "fresh"), str(backup)],
        )
    assert observed == [1000 + 16384]
    assert not (tmp_path / "fresh").exists()
