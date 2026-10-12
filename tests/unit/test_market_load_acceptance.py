"""The 250-page operator override is restricted to an explicit market-page load."""

import inspect
import json

import pytest

from oddsfox_catalogue import acceptance, cli
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.load.runner import LoadBlocked, LoadSummary, load_pending
from oddsfox_catalogue.pipeline import load_stage

MARKET_LOAD = ["load", "--batch-id", "synthetic-batch", "--record-key", "markets"]


@pytest.mark.parametrize("args,key", [(["load"], None), (MARKET_LOAD, "markets")])
def test_cli_forwards_optional_filter_and_additive_remaining_count(
    tmp_path, monkeypatch, capsys, args, key
):
    settings = Settings(tmp_path)
    calls = []
    monkeypatch.setattr(cli, "load_settings", lambda: settings)

    def load(selected, **kwargs):
        assert selected is settings
        calls.append(kwargs)
        return LoadSummary(pages_loaded=3, pages_remaining=2)

    monkeypatch.setattr(cli, "load_stage", load)
    parsed = cli.build_parser().parse_args(args)
    assert parsed.record_key == key
    assert parsed.handler(parsed) == 0
    assert calls == [{"batch_id": None if key is None else "synthetic-batch", "record_key": key}]
    assert json.loads(capsys.readouterr().out)["pages_remaining"] == 2


def test_filter_apis_are_optional_keyword_only():
    for function in (load_stage, load_pending):
        parameter = inspect.signature(function).parameters["record_key"]
        assert parameter.kind is inspect.Parameter.KEYWORD_ONLY and parameter.default is None


@pytest.mark.parametrize("batch,key", [(None, "markets"), ("", "markets"), ("batch", "events")])
def test_stage_rejects_invalid_filter_shape_before_lock_or_ledger_creation(tmp_path, batch, key):
    root = tmp_path / "uncreated"
    with pytest.raises(LoadBlocked):
        load_stage(Settings(root), batch_id=batch, record_key=key)
    assert not root.exists()


@pytest.mark.parametrize(
    "args",
    [
        ["load", "--record-key", "events"],
        ["load", "--record-key", "market"],
        ["load", "--record-key"],
    ],
)
def test_cli_rejects_unsupported_filter_flags(args):
    with pytest.raises(SystemExit):
        cli.build_parser().parse_args(args)


@pytest.mark.parametrize(
    "args",
    [
        ["load"],
        ["load", "--record-key", "markets"],
        ["load", "--batch-id", "synthetic-batch"],
        ["load", "--record-key", "markets", "--batch-id", ""],
        [*MARKET_LOAD, "--batch-id", ""],
        ["status"],
        ["replay"],
        ["config", "show"],
        ["capture", "--mode", "daily"],
    ],
)
def test_250_is_rejected_outside_effective_parsed_market_scope_before_output_or_child(
    tmp_path, monkeypatch, args
):
    def forbidden(*args, **kwargs):
        raise AssertionError("an invalid override launched a child")

    monkeypatch.setattr(acceptance.subprocess, "Popen", forbidden)
    with pytest.raises(ValueError):
        acceptance.run(tmp_path, tmp_path / "root", args, load_pages=250)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("pages", [0, 49, 200, 251, True, 250.0, "250", None])
def test_invalid_override_types_and_values_fail_before_output(tmp_path, monkeypatch, pages):
    monkeypatch.setattr(
        acceptance.subprocess, "Popen", lambda *a, **k: pytest.fail("unexpected child")
    )
    with pytest.raises(ValueError):
        acceptance.run(tmp_path, tmp_path / "root", MARKET_LOAD, load_pages=pages)
    assert list(tmp_path.iterdir()) == []


def test_native_allowances_rates_and_quality_are_preserved_by_250_override(tmp_path, monkeypatch):
    monkeypatch.setenv("CATALOGUE_LOAD_MAX_PAGES_PER_RUN", "999")
    ordinary = acceptance.environment(tmp_path, ["load"])
    selected = acceptance.environment(tmp_path, MARKET_LOAD, load_pages=250)
    assert ordinary["CATALOGUE_LOAD_MAX_PAGES_PER_RUN"] == "50"
    assert selected["CATALOGUE_LOAD_MAX_PAGES_PER_RUN"] == "250"
    assert set(ordinary) == set(selected)
    for key in ordinary:
        if key != "CATALOGUE_LOAD_MAX_PAGES_PER_RUN":
            assert ordinary[key] == selected[key], key
    for key, value in {
        "CATALOGUE_CAPTURE_MAX_RETAINED_BYTES": str(256 * 1024**3),
        "CATALOGUE_CAPTURE_MAX_TEMP_BYTES": str(32 * 1024**3),
        "CATALOGUE_LOAD_DUCKDB_MEMORY_LIMIT": "2GB",
        "CATALOGUE_LOAD_DUCKDB_THREADS": "2",
        "CATALOGUE_CAPTURE_WORKERS": "1",
        "CATALOGUE_GAMMA_REQUESTS_PER_SECOND": "2",
        "CATALOGUE_QUALITY_QUARANTINE_MAX_RATIO": "0.01",
    }.items():
        assert selected[key] == value


@pytest.mark.parametrize(
    "args,pages", [(["load"], 50), (MARKET_LOAD, 50), (MARKET_LOAD, 250), (["status"], 50)]
)
def test_supervisor_passes_actual_page_limit_and_reports_it(tmp_path, monkeypatch, args, pages):
    launched = []
    monkeypatch.setattr(acceptance, "FREE_RESERVE", 0)
    monkeypatch.setattr(acceptance, "_runtime_revision", lambda: {"synthetic": True})

    class Process:
        pid = 12345
        returncode = 0

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    def child(command, **kwargs):
        assert command[1:] == ["-m", "oddsfox_catalogue.cli", *args]
        assert kwargs["env"]["CATALOGUE_LOAD_MAX_PAGES_PER_RUN"] == str(pages)
        launched.append(command)
        return Process()

    monkeypatch.setattr(acceptance.subprocess, "Popen", child)
    assert acceptance.run(tmp_path, tmp_path / "root", args, load_pages=pages, duration=30) == 0
    report = json.loads(next((tmp_path / "completion-proof").glob("*/report.json")).read_bytes())
    assert report["status"] == "passed" and report["load_pages"] == pages and len(launched) == 1
    assert report["max_retained_bytes"] == 256 * 1024**3
    assert report["max_temp_bytes"] == 32 * 1024**3


@pytest.mark.parametrize("pages", [None, 50, 250])
def test_acceptance_cli_forwards_default_and_explicit_limit(tmp_path, monkeypatch, pages):
    calls = []
    monkeypatch.setattr(acceptance, "_install_termination_handlers", lambda: lambda: None)
    monkeypatch.setattr(
        acceptance, "run", lambda *args, **kwargs: calls.append((args, kwargs)) or 0
    )
    args = ["--workspace", str(tmp_path), "--root", str(tmp_path / "root")]
    if pages is not None:
        args += ["--load-pages", str(pages)]
    args += ["--", *MARKET_LOAD]
    assert acceptance.main(args) == 0
    assert calls[0][0][2] == MARKET_LOAD
    assert calls[0][1]["load_pages"] == (50 if pages is None else pages)


def test_unsupported_acceptance_flag_value_cannot_call_supervisor(tmp_path, monkeypatch):
    monkeypatch.setattr(acceptance, "run", lambda *a, **k: pytest.fail("unexpected supervisor"))
    with pytest.raises(SystemExit):
        acceptance.main(
            [
                "--workspace",
                str(tmp_path),
                "--root",
                str(tmp_path / "root"),
                "--load-pages",
                "200",
                "--",
                *MARKET_LOAD,
            ]
        )
    assert list(tmp_path.iterdir()) == []
