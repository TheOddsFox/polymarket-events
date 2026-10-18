import runpy
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("name", ["verify-fast", "verify"])
def test_verify_wrappers_preserve_gates_and_forward_pytest_arguments(name, monkeypatch):
    root = Path(__file__).resolve().parents[2]
    calls = []
    monkeypatch.setattr(sys, "argv", [name, "-n0", "tests/unit/test_signals.py"])
    monkeypatch.setattr(
        subprocess, "run", lambda command, **kwargs: calls.append((command, kwargs))
    )
    runpy.run_path(str(root / "scripts" / name))
    commands = [command for command, _ in calls]
    assert commands[0] == ["git", "diff", "--check"]
    assert ["uv", "run", "--locked", "ruff", "check", "."] in commands
    assert ["uv", "run", "--locked", "ruff", "format", "--check", "."] in commands
    assert commands[-1] == [
        "uv",
        "run",
        "--locked",
        "pytest",
        "-q",
        "-n0",
        "tests/unit/test_signals.py",
    ]
    assert (["uv", "lock", "--check"] in commands) is (name == "verify")
    assert (["uv", "sync", "--locked"] in commands) is (name == "verify")
    assert all(options["check"] and options["cwd"] == root for _, options in calls)
    assert all(options["env"]["UV_LINK_MODE"] == "copy" for _, options in calls)
    assert calls[-1][1]["env"]["CATALOGUE_GAMMA_BASE_URL"] == "http://127.0.0.1:9"
    assert calls[-1][1]["env"]["CATALOGUE_GAMMA_ALLOW_LOOPBACK"] == "1"


def test_verify_wrapper_stops_on_failed_gate(monkeypatch):
    root = Path(__file__).resolve().parents[2]
    calls = []
    monkeypatch.setattr(sys, "argv", ["verify-fast"])

    def fail(command, **_kwargs):
        calls.append(command)
        raise subprocess.CalledProcessError(1, command)

    monkeypatch.setattr(subprocess, "run", fail)
    with pytest.raises(subprocess.CalledProcessError):
        runpy.run_path(str(root / "scripts" / "verify-fast"))
    assert calls == [["git", "diff", "--check"]]
