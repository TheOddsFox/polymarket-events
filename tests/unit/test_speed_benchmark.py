"""Finite workflow orchestration, with installed subprocesses replaced by reports."""

import importlib.util
import json
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

# Both modules remain standalone command-line tools, outside the installed package.
TOOLS = Path(__file__).resolve().parents[2] / "tools"
spec = importlib.util.spec_from_file_location("speed_corpus", TOOLS / "speed_corpus.py")
fixture_module = importlib.util.module_from_spec(spec)
sys.modules["speed_corpus"] = fixture_module
spec.loader.exec_module(fixture_module)
spec = importlib.util.spec_from_file_location("speed_benchmark", TOOLS / "speed_benchmark.py")
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)
OWNER = {
    "purpose": "pipeline-speed-benchmark",
    "revision": 1,
    "baseline_commit": "bd5eb0c1af1e40295305b6558d9ea28d1f1ce5fa",
}


MACHINE = {
    "node": "synthetic",
    "system": "TestOS",
    "release": "1",
    "machine": "test",
    "cpu_count": 2,
    "physical_memory_bytes": 8 * 1024**3,
}
DEPENDENCIES = [["example", "1.0"]]
BASE_RUNTIME = {"bytes": 1, "sha256": "b" * 64}
CANDIDATE_RUNTIME = {"bytes": 2, "sha256": "c" * 64}
RAW = {"bytes": 100, "sha256": "d" * 64}
COVERAGE = {"bytes": 200, "sha256": "e" * 64}


def _effective_settings():
    from dataclasses import asdict

    from oddsfox_catalogue.config import (
        CaptureSettings,
        GammaSettings,
        LoadSettings,
        QualitySettings,
        ScheduleSettings,
    )

    return {
        "gamma": {
            **asdict(GammaSettings()),
            "base_url": "recorded-fixture",
            "allow_loopback": True,
        },
        "capture": asdict(CaptureSettings()),
        "load": asdict(LoadSettings()),
        "quality": asdict(QualitySettings()),
        "schedule": asdict(ScheduleSettings()),
    }


def _manifest():
    return {
        "revision": 1,
        "shared": {
            "lock_sha256": bench.hashlib.sha256(b"lock").hexdigest(),
            "dependencies_sha256": bench._digest(DEPENDENCIES),
            "effective_settings_sha256": bench._digest(_effective_settings()),
            "python_version": "3.12.1",
            "machine": MACHINE,
            "harness_sha256": "a" * 64,
            "seed_raw_inventory": RAW,
            "seed_coverage": COVERAGE,
        },
        "variants": {
            "baseline": {
                "runtime": BASE_RUNTIME,
                "wheel_sha256": bench.hashlib.sha256(b"wheel").hexdigest(),
            },
            "candidate": {
                "runtime": CANDIDATE_RUNTIME,
                "wheel_sha256": bench.hashlib.sha256(b"wheel").hexdigest(),
            },
        },
    }


def _observed():
    return {
        **_manifest()["shared"],
        "runtime": CANDIDATE_RUNTIME,
        "dependencies": DEPENDENCIES,
        "effective_settings": _effective_settings(),
        "wheel_sha256": _manifest()["variants"]["candidate"]["wheel_sha256"],
    }


@pytest.fixture(autouse=True)
def pin_probes(monkeypatch):
    monkeypatch.setattr(bench, "_probe", lambda python, root: _observed())
    monkeypatch.setattr(bench, "_machine", lambda: MACHINE)
    monkeypatch.setattr(bench, "_harness_sha256", lambda: "a" * 64)


def _run_variant(python, root, workspace, corpus, **kwargs):
    return bench.run_variant(
        python,
        root,
        workspace,
        corpus,
        experiment=workspace / "experiment.json",
        variant="candidate",
        wheel=workspace / "runtime.whl",
        lockfile=workspace / "uv.lock",
        **kwargs,
    )


def _seed(tmp_path):
    (tmp_path / "runtime.whl").write_bytes(b"wheel")
    (tmp_path / "uv.lock").write_bytes(b"lock")
    experiment = tmp_path / "experiment.json"
    experiment.write_text(json.dumps(_manifest()))
    experiment.chmod(0o444)
    root = tmp_path / "owned"
    (root / ".state").mkdir(parents=True)
    (root / ".state" / "pipeline-speed-owner.json").write_text(json.dumps(OWNER))
    with sqlite3.connect(root / ".state" / "ledger.sqlite") as db:
        db.execute(
            "CREATE TABLE batches(batch_id TEXT,mode TEXT,status TEXT,scope_json TEXT,error TEXT)"
        )
        db.execute("CREATE TABLE stage_runs(batch_id TEXT,stage TEXT,status TEXT,error TEXT)")
        db.execute("INSERT INTO batches VALUES ('seed','bootstrap','loaded','{}',NULL)")
    corpus = tmp_path / "corpus.sqlite"
    corpus.write_bytes(b"synthetic corpus pin")
    return root, corpus


def _state(root, status, error=None):
    with sqlite3.connect(root / ".state" / "ledger.sqlite") as db:
        db.execute("DELETE FROM batches WHERE batch_id='daily'")
        db.execute("INSERT INTO batches VALUES ('daily','daily',?,'{}',?)", (status, error))
        db.execute(
            "INSERT INTO stage_runs VALUES ('daily','capture:daily',?,?)",
            ("failed" if error else "succeeded", error),
        )


def _mock_server(monkeypatch, *, errors=0):
    class Fixture:
        manifest = {"delay_policy_sha256": "delay-pin"}
        url = "http://127.0.0.1:32123"

        def __init__(self, path, *, max_requests, max_download_bytes):
            assert max_requests == 200000 and max_download_bytes == 32 * 1024**3

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def accounting(self):
            return {"requests": 2, "bytes": 100, "statuses": {"200": 2}, "errors": errors}

    monkeypatch.setattr(bench, "RecordedGammaServer", Fixture)


def _invocation(arguments, code=0, **report):
    return {
        "command": arguments,
        "returncode": code,
        "wall_s": 0.1,
        "report": {"status": "passed" if code == 0 else "failed", "duration_s": 0.08, **report},
    }


def test_required_stage_order_and_exact_load_page_policy(tmp_path, monkeypatch):
    root, corpus = _seed(tmp_path)
    _mock_server(monkeypatch)
    calls = []

    def invoke(python, root, workspace, arguments, *, load_pages, env, expected_runtime=None):
        calls.append((arguments, load_pages))
        assert env["CATALOGUE_GAMMA_ALLOW_LOOPBACK"] == "true"
        if arguments[0] == "capture":
            _state(root, "captured")
        return _invocation(arguments)

    monkeypatch.setattr(bench, "_invoke", invoke)
    result = _run_variant(Path(sys.executable), root, tmp_path, corpus)
    assert result["status"] == "passed" and result["semantic_validated"] is False
    assert result["wall_s"] > 0 and result["acceptance_duration_s"] == pytest.approx(0.48)
    assert calls == [
        (["capture", "--mode", "daily"], 50),
        (["load", "--batch-id", "daily", "--record-key", "markets"], 250),
        (["load", "--batch-id", "daily"], 50),
        (["dbt", "--", "build"], 50),
        (["publish"], 50),
        (["verify"], 50),
    ]


def test_only_allowlisted_budget_failure_resumes_same_named_batch(tmp_path, monkeypatch):
    root, corpus = _seed(tmp_path)
    _mock_server(monkeypatch)
    calls = []

    def invoke(python, root, workspace, arguments, **kwargs):
        calls.append(arguments)
        if len(calls) == 1:
            _state(root, "capturing", "RequestBudgetExceeded: Gamma request allowance exhausted")
            return _invocation(arguments, 3)
        _state(root, "captured")
        return _invocation(arguments)

    monkeypatch.setattr(bench, "_invoke", invoke)
    result = _run_variant(Path(sys.executable), root, tmp_path, corpus)
    assert result["status"] == "passed"
    assert calls[1] == ["capture", "--mode", "daily", "--resume", "daily"]


@pytest.mark.parametrize(
    "error",
    [
        "DurabilityError: corrupt page",
        "RequestBudgetExceeded: retained storage allowance exhausted",
        "RetriesExhausted: source unavailable",
    ],
)
def test_no_resume_for_corruption_storage_or_source_failure(tmp_path, monkeypatch, error):
    root, corpus = _seed(tmp_path)
    _mock_server(monkeypatch)
    calls = []

    def invoke(python, root, workspace, arguments, **kwargs):
        calls.append(arguments)
        _state(root, "capturing", error)
        return _invocation(arguments, 3)

    monkeypatch.setattr(bench, "_invoke", invoke)
    result = _run_variant(Path(sys.executable), root, tmp_path, corpus)
    assert result["status"] == "stage_failed" and len(calls) == 1


def test_capture_bound_and_fixture_contract_failure_invalidate(tmp_path, monkeypatch):
    root, corpus = _seed(tmp_path)
    _mock_server(monkeypatch)
    calls = []

    def invoke(python, root, workspace, arguments, **kwargs):
        calls.append(arguments)
        _state(root, "capturing", "RequestBudgetExceeded: Gamma download allowance exhausted")
        return _invocation(arguments, 3)

    monkeypatch.setattr(bench, "_invoke", invoke)
    result = _run_variant(Path(sys.executable), root, tmp_path, corpus, max_capture_invocations=2)
    assert result["status"] == "capture_cap_exhausted" and len(calls) == 2
    _mock_server(monkeypatch, errors=1)
    assert (
        _run_variant(Path(sys.executable), root, tmp_path, corpus, max_capture_invocations=1)[
            "status"
        ]
        == "invalid_fixture"
    )


def test_failed_stage_stops_remaining_stages(tmp_path, monkeypatch):
    root, corpus = _seed(tmp_path)
    _mock_server(monkeypatch)
    calls = []

    def invoke(python, root, workspace, arguments, **kwargs):
        calls.append(arguments[0])
        _state(root, "captured")
        return _invocation(arguments, 3 if arguments[0] == "load" else 0)

    monkeypatch.setattr(bench, "_invoke", invoke)
    assert _run_variant(Path(sys.executable), root, tmp_path, corpus)["status"] == "stage_failed"
    assert calls == ["capture", "load"]


def _valid_run(duration):
    variant = "baseline" if duration == 100 else "candidate"
    manifest = _manifest()
    observed = {**_observed(), "runtime": manifest["variants"][variant]["runtime"]}
    return {
        "status": "passed",
        "semantic_validated": True,
        "limits_verified": True,
        "wall_s": duration,
        "corpus_sha256": "corpus",
        "delay_policy_sha256": "delay",
        "workload_pin": "work",
        "limits": bench.LIMITS,
        "variant": variant,
        "experiment": manifest,
        "experiment_sha256": "f" * 64,
        "experiment_content_sha256": bench._digest(manifest),
        "observed_pins": observed,
        "invocations": [{"report": {"runtime": manifest["variants"][variant]["runtime"]}}],
    }


@pytest.mark.parametrize(
    ("duration", "status"),
    [
        (40, "passed"),
        (45, "repeat_required"),
        (50, "repeat_required"),
        (55, "repeat_required"),
        (60, "target_missed"),
    ],
)
def test_comparison_threshold_and_repeat_band(duration, status):
    assert bench.compare_runs(_valid_run(100), _valid_run(duration))["status"] == status
    assert bench.compare_runs(_valid_run(100), _valid_run(40), scaling_variability=0.11)[
        "repeat_required"
    ]


@pytest.mark.parametrize(
    "field,value",
    [
        ("semantic_validated", False),
        ("limits_verified", False),
        ("corpus_sha256", "other"),
        ("delay_policy_sha256", "other"),
        ("workload_pin", "other"),
        ("limits", {}),
        ("wall_s", 0),
        ("wall_s", float("nan")),
        ("status", "stage_failed"),
    ],
)
def test_comparison_refuses_unproven_or_mismatched_runs(field, value):
    candidate = {**_valid_run(40), field: value}
    assert bench.compare_runs(_valid_run(100), candidate)["ratio"] is None


def test_unmarked_operator_root_is_rejected_before_execution(tmp_path, monkeypatch):
    root, corpus = _seed(tmp_path)
    (root / ".state" / "pipeline-speed-owner.json").unlink()
    monkeypatch.setattr(bench, "_invoke", lambda *args, **kwargs: pytest.fail("must not execute"))
    with pytest.raises((ValueError, OSError)):
        _run_variant(Path(sys.executable), root, tmp_path, corpus)


def test_acceptance_subprocess_argv_and_completed_report_binding(tmp_path, monkeypatch):
    root, _ = _seed(tmp_path)
    arguments = ["load", "--batch-id", "daily", "--record-key", "markets"]

    def execute(command, **kwargs):
        assert command[-len(arguments) :] == arguments
        assert command[command.index("--load-pages") + 1] == "250"
        assert command.index("--load-pages") < command.index("--")
        directory = tmp_path / "completion-proof" / ("a" * 32)
        directory.mkdir(parents=True)
        report = {
            "root": str(root),
            "command": arguments,
            "runtime": {"sha256": "runtime"},
            "duration_s": 1.5,
            "duration_limit_s": 14400,
            "load_pages": 250,
            "max_retained_bytes": 256 * 1024**3,
            "max_temp_bytes": 32 * 1024**3,
            "status": "passed",
            "returncode": 0,
        }
        (directory / "report.json").write_text(json.dumps(report))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(bench.subprocess, "run", execute)
    result = bench._invoke(Path(sys.executable), root, tmp_path, arguments, load_pages=250, env={})
    assert result["report"]["duration_s"] == 1.5 and result["returncode"] == 0


def test_exclusive_atomic_output_and_git_rejection(tmp_path):
    output = tmp_path / "result.json"
    bench._write_output(output, {"status": "synthetic"})
    with pytest.raises(FileExistsError):
        bench._write_output(output, {})
    assert json.loads(output.read_text()) == {"status": "synthetic"}
    repository = tmp_path / "repo"
    (repository / ".git").mkdir(parents=True)
    with pytest.raises(ValueError, match="outside Git"):
        bench._write_output(repository / "result.json", {})


def test_stale_budget_error_cannot_authorize_resume(tmp_path, monkeypatch):
    root, corpus = _seed(tmp_path)
    _mock_server(monkeypatch)
    calls = []

    def invoke(python, root, workspace, arguments, **kwargs):
        calls.append(arguments)
        if len(calls) == 1:
            _state(root, "capturing", "RequestBudgetExceeded: Gamma request allowance exhausted")
        # Second acceptance failure has not written a new capture stage.
        return _invocation(arguments, 3)

    monkeypatch.setattr(bench, "_invoke", invoke)
    result = _run_variant(Path(sys.executable), root, tmp_path, corpus)
    assert len(calls) == 2 and result["status"] == "stage_failed"


def test_stage_error_must_match_batch_budget_error(tmp_path, monkeypatch):
    root, corpus = _seed(tmp_path)
    _mock_server(monkeypatch)
    calls = []

    def invoke(python, root, workspace, arguments, **kwargs):
        calls.append(arguments)
        _state(root, "capturing", "RequestBudgetExceeded: Gamma request allowance exhausted")
        with sqlite3.connect(root / ".state" / "ledger.sqlite") as db:
            db.execute("UPDATE stage_runs SET error='DurabilityError: corrupt page'")
        return _invocation(arguments, 3)

    monkeypatch.setattr(bench, "_invoke", invoke)
    result = _run_variant(Path(sys.executable), root, tmp_path, corpus)
    assert len(calls) == 1 and result["status"] == "stage_failed"


def test_missing_acceptance_report_retains_failed_invocation_evidence(tmp_path, monkeypatch):
    root, corpus = _seed(tmp_path)
    _mock_server(monkeypatch)
    monkeypatch.setattr(
        bench.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=3)
    )
    result = _run_variant(Path(sys.executable), root, tmp_path, corpus)
    assert result["status"] == "invalid_run"
    assert len(result["invocations"]) == 1
    invocation = result["invocations"][0]
    assert invocation["returncode"] == 3 and invocation["report"] is None
    assert invocation["wall_s"] > 0 and "capture" in invocation["command"]
    assert bench.compare_runs(_valid_run(100), result)["ratio"] is None


@pytest.mark.parametrize(
    "pin",
    ["runtime", "seed_raw_inventory", "seed_coverage", "dependencies_sha256", "python_version"],
)
def test_preflight_rejects_installed_or_raw_seed_mismatch(tmp_path, monkeypatch, pin):
    root, corpus = _seed(tmp_path)
    wrong = {
        **_observed(),
        pin: {"bytes": 999, "sha256": "0" * 64}
        if pin in {"runtime", "seed_raw_inventory", "seed_coverage"}
        else "wrong",
    }
    monkeypatch.setattr(bench, "_probe", lambda python, root: wrong)
    monkeypatch.setattr(
        bench, "_invoke", lambda *args, **kwargs: pytest.fail("must fail before timing")
    )
    with pytest.raises(ValueError, match="pin mismatch"):
        _run_variant(Path(sys.executable), root, tmp_path, corpus)


@pytest.mark.parametrize("pin", ["wheel", "lock", "machine", "harness"])
def test_preflight_rejects_wheel_lock_hardware_or_harness_mismatch(tmp_path, monkeypatch, pin):
    root, corpus = _seed(tmp_path)
    if pin == "wheel":
        (tmp_path / "runtime.whl").write_bytes(b"other wheel")
    elif pin == "lock":
        (tmp_path / "uv.lock").write_bytes(b"other lock")
    elif pin == "machine":
        monkeypatch.setattr(bench, "_machine", lambda: {**MACHINE, "cpu_count": 3})
    else:
        monkeypatch.setattr(bench, "_harness_sha256", lambda: "0" * 64)
    monkeypatch.setattr(
        bench, "_invoke", lambda *args, **kwargs: pytest.fail("must fail before timing")
    )
    with pytest.raises(ValueError, match="pin mismatch"):
        _run_variant(Path(sys.executable), root, tmp_path, corpus)


def test_manifest_is_required_frozen_and_strict(tmp_path):
    root, corpus = _seed(tmp_path)
    with pytest.raises(TypeError):
        bench.run_variant(Path(sys.executable), root, tmp_path, corpus)
    path = tmp_path / "experiment.json"
    path.chmod(0o644)
    with pytest.raises(ValueError, match="read-only"):
        _run_variant(Path(sys.executable), root, tmp_path, corpus)
    value = _manifest()
    del value["shared"]["seed_raw_inventory"]
    path.write_text(json.dumps(value))
    path.chmod(0o444)
    with pytest.raises(ValueError, match="incomplete experiment"):
        _run_variant(Path(sys.executable), root, tmp_path, corpus)


@pytest.mark.parametrize(
    "pin",
    [
        "runtime",
        "wheel_sha256",
        "seed_raw_inventory",
        "dependencies_sha256",
        "machine",
        "harness_sha256",
    ],
)
def test_comparison_requires_observed_pin_equality_not_a_boolean(pin):
    candidate = _valid_run(40)
    candidate["observed_pins"] = {**candidate["observed_pins"], pin: "tampered"}
    candidate["pins_verified"] = True
    assert bench.compare_runs(_valid_run(100), candidate)["ratio"] is None


def test_comparison_requires_opposite_variants_and_same_frozen_manifest():
    baseline, candidate = _valid_run(100), _valid_run(40)
    candidate["variant"] = "baseline"
    assert bench.compare_runs(baseline, candidate)["ratio"] is None
    candidate = _valid_run(40)
    candidate["experiment_sha256"] = "0" * 64
    assert bench.compare_runs(baseline, candidate)["ratio"] is None


def test_runtime_change_in_completed_stage_aborts_report_admission(tmp_path, monkeypatch):
    root, _ = _seed(tmp_path)
    arguments = ["verify"]

    def execute(command, **kwargs):
        directory = tmp_path / "completion-proof" / ("a" * 32)
        directory.mkdir(parents=True)
        report = {
            "root": str(root),
            "command": arguments,
            "runtime": BASE_RUNTIME,
            "duration_s": 1.5,
            "duration_limit_s": 14400,
            "load_pages": 50,
            "max_retained_bytes": 256 * 1024**3,
            "max_temp_bytes": 32 * 1024**3,
            "status": "passed",
            "returncode": 0,
        }
        (directory / "report.json").write_text(json.dumps(report))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(bench.subprocess, "run", execute)
    with pytest.raises(ValueError, match="inconsistent acceptance report"):
        bench._invoke(
            Path(sys.executable),
            root,
            tmp_path,
            arguments,
            load_pages=50,
            env={},
            expected_runtime=CANDIDATE_RUNTIME,
        )


def test_installed_probe_is_isolated_and_reports_fresh_capture_binding(tmp_path, monkeypatch):
    root, _ = _seed(tmp_path)
    monkeypatch.undo()

    def execute(command, *, env, stdout, stderr, check):
        assert command[:3] == [sys.executable, "-I", "-c"]
        assert command[-1] == str(root)
        assert "capture_binding(settings)" in command[3]
        assert "environment(Path(sys.argv[1])" in command[3]
        assert "PYTHONPATH" not in env and "PYTHONHOME" not in env
        assert not any(k.startswith("CATALOGUE_") for k in env)
        stdout.write(json.dumps(_observed()).encode())
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(bench.subprocess, "run", execute)
    assert bench._probe(Path(sys.executable), root)["seed_raw_inventory"] == RAW


@pytest.mark.parametrize(
    "section,key,value",
    [
        ("gamma", "page_limit", 1),
        ("gamma", "include_chat", True),
        ("gamma", "include_template", True),
        ("gamma", "include_best_lines", True),
        ("gamma", "connect_timeout_s", 1.0),
        ("gamma", "read_timeout_s", 2.0),
        ("gamma", "max_retries", 1),
        ("capture", "id_partition_size", 1000),
        ("capture", "max_id_override", 1),
    ],
)
def test_structured_effective_settings_cannot_keep_stale_shared_hash(
    tmp_path, monkeypatch, section, key, value
):
    root, corpus = _seed(tmp_path)
    observed = _observed()
    observed["effective_settings"][section][key] = value
    # The declared settings hash still matches the manifest; the structure does not.
    monkeypatch.setattr(bench, "_probe", lambda python, root: observed)
    with pytest.raises(ValueError, match="pin mismatch"):
        _run_variant(Path(sys.executable), root, tmp_path, corpus)
    candidate = _valid_run(40)
    candidate["observed_pins"] = observed
    assert bench.compare_runs(_valid_run(100), candidate)["ratio"] is None


@pytest.mark.parametrize(
    "key", ["data_dir", "state_dir", "warehouse_file", "dbt_project_dir", "dbt_profiles_dir"]
)
def test_installed_probe_refuses_root_path_overrides(tmp_path, monkeypatch, key):
    import io
    from contextlib import redirect_stdout

    from oddsfox_catalogue import certification

    __import__("oddsfox_catalogue.acceptance")
    root, _ = _seed(tmp_path)
    config = root / "config"
    config.mkdir()
    (config / "catalogue.toml").write_text(f'[paths]\n{key} = "/synthetic/external/path"\n')
    monkeypatch.undo()
    monkeypatch.setattr(
        certification,
        "capture_binding",
        lambda settings: pytest.fail("path override must fail before seed verification"),
    )
    monkeypatch.setattr(sys, "argv", [sys.executable, str(root)])

    def execute(command, **kwargs):
        try:
            with redirect_stdout(io.StringIO()):
                exec(command[3], {"__name__": "isolated_probe_test"})
        except ValueError as exc:
            assert "default paths" in str(exc)
            return SimpleNamespace(returncode=1)
        pytest.fail("external path was accepted")

    monkeypatch.setattr(bench.subprocess, "run", execute)
    with pytest.raises(ValueError, match="installed runtime/raw-seed probe failed"):
        bench._probe(Path(sys.executable), root)


def test_installed_probe_reads_real_root_config_and_only_normalizes_fixture_address(
    tmp_path, monkeypatch
):
    import io
    from contextlib import redirect_stdout

    from oddsfox_catalogue import certification, rebuild

    __import__("oddsfox_catalogue.acceptance")
    root, _ = _seed(tmp_path)
    config = root / "config"
    config.mkdir()
    (config / "catalogue.toml").write_text(
        "[gamma]\npage_limit = 1\ninclude_chat = true\nmax_retries = 3\n"
        "[capture]\nid_partition_size = 1000\nmax_id_override = 17\n"
    )
    monkeypatch.undo()

    def binding(settings):
        assert settings.gamma.base_url == "http://127.0.0.1:9" and settings.gamma.allow_loopback
        return {"inventory": RAW, "coverage": COVERAGE}

    monkeypatch.setattr(certification, "capture_binding", binding)
    monkeypatch.setattr(rebuild, "_runtime_revision", lambda: CANDIDATE_RUNTIME)
    monkeypatch.setattr(sys, "argv", [sys.executable, str(root)])

    def execute(command, *, stdout, **kwargs):
        captured = io.StringIO()
        with redirect_stdout(captured):
            exec(command[3], {"__name__": "isolated_probe_test"})
        stdout.write(captured.getvalue().encode())
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(bench.subprocess, "run", execute)
    observed = bench._probe(Path(sys.executable), root)
    settings = observed["effective_settings"]
    assert settings["gamma"]["page_limit"] == 1 and settings["gamma"]["include_chat"]
    assert settings["gamma"]["max_retries"] == 3
    assert (
        settings["gamma"]["base_url"] == "recorded-fixture" and settings["gamma"]["allow_loopback"]
    )
    assert settings["capture"]["id_partition_size"] == 1000
    assert settings["capture"]["max_id_override"] == 17
    assert settings["capture"]["max_requests"] == 25000
    assert settings["load"]["max_pages_per_run"] == 50
    assert observed["effective_settings_sha256"] == bench._digest(settings)
