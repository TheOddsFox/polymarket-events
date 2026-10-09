"""scripts/bootstrap-until-done retries a failing bootstrap command.

The command is a stub script, so Gamma is never called and the catalogue CLI is never invoked.
"""

from __future__ import annotations

import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "bootstrap-until-done"
ATTEMPT_LINE = re.compile(r"attempt \d+/\d+ exit=\d+")

# Exits with the code for this call number from a comma-separated list; the last code repeats.
STUB = """
import pathlib
import sys

counter = pathlib.Path(sys.argv[1])
codes = [int(code) for code in sys.argv[2].split(",")]
calls = int(counter.read_text()) if counter.exists() else 0
counter.write_text(str(calls + 1))
print(f"stub attempt {calls + 1}")
sys.exit(codes[min(calls, len(codes) - 1)])
"""


def _run_wrapper(
    tmp_path: Path,
    codes: str,
    max_attempts: int | None = None,
    *,
    sleep_s: str = "30",
):
    stub = tmp_path / "stub.py"
    stub.write_text(STUB, encoding="utf-8")
    counter = tmp_path / "calls.txt"
    log_dir = tmp_path / "logs"
    sleep_log = tmp_path / "sleeps.txt"
    bindir = tmp_path / "bin"
    bindir.mkdir()
    shim = bindir / "sleep"
    shim.write_text(
        '#!/bin/sh\nprintf \'%s\\n\' "$1" >> "$SLEEP_LOG"\n',
        encoding="utf-8",
    )
    shim.chmod(0o755)
    command = shlex.join([sys.executable, str(stub), str(counter), codes])

    env = {key: value for key, value in os.environ.items() if not key.startswith("BOOTSTRAP_")}
    env.update(
        {
            "BOOTSTRAP_COMMAND": command,
            "BOOTSTRAP_RETRY_SLEEP_S": sleep_s,
            "BOOTSTRAP_LOG_DIR": str(log_dir),
            "CATALOGUE_GAMMA_BASE_URL": "http://127.0.0.1:9",
            "PATH": f"{bindir}{os.pathsep}{env.get('PATH', '')}",
            "SLEEP_LOG": str(sleep_log),
        }
    )
    if max_attempts is not None:
        env["BOOTSTRAP_MAX_ATTEMPTS"] = str(max_attempts)

    result = subprocess.run(
        [str(SCRIPT)],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    logs = sorted(log_dir.glob("bootstrap-*.log"))
    calls = int(counter.read_text()) if counter.exists() else 0
    sleeps = sleep_log.read_text(encoding="utf-8").split() if sleep_log.exists() else []
    return result, logs, calls, sleeps


def test_fail_then_success_exits_zero_and_logs_both_attempts(tmp_path: Path) -> None:
    result, logs, calls, sleeps = _run_wrapper(tmp_path, "1,0")

    assert result.returncode == 0, result.stderr
    assert calls == 2
    assert sleeps == ["30"]
    assert len(logs) == 1
    assert re.fullmatch(r"bootstrap-\d{8}T\d{6}Z-\d+\.log", logs[0].name)
    text = logs[0].read_text(encoding="utf-8")
    assert "stub attempt 1" in text and "stub attempt 2" in text
    assert len(ATTEMPT_LINE.findall(text)) == 2
    assert "attempt 1/20 exit=1" in text
    assert "attempt 2/20 exit=0" in text


def test_exit_3_stops_after_one_attempt_without_sleeping(tmp_path: Path) -> None:
    result, logs, calls, sleeps = _run_wrapper(tmp_path, "3")

    assert result.returncode == 3, result.stderr
    assert calls == 1
    assert sleeps == []
    text = logs[0].read_text(encoding="utf-8")
    assert len(ATTEMPT_LINE.findall(text)) == 1
    assert "attempt 2/" not in text


def test_exit_2_does_not_recrawl(tmp_path: Path) -> None:
    """A handled post-capture failure must not start another Gamma crawl."""
    result, logs, calls, sleeps = _run_wrapper(tmp_path, "2")

    assert result.returncode == 2, result.stderr
    assert calls == 1
    assert sleeps == []
    text = logs[0].read_text(encoding="utf-8")
    assert "attempt 1/20 exit=2" in text
    assert "attempt 2/" not in text


def test_attempt_cap_is_inclusive_and_exits_nonzero(tmp_path: Path) -> None:
    result, logs, calls, sleeps = _run_wrapper(tmp_path, "1", max_attempts=3)

    assert result.returncode == 1, result.stderr
    assert calls == 3
    assert sleeps == ["30", "30"]
    text = logs[0].read_text(encoding="utf-8")
    assert len(ATTEMPT_LINE.findall(text)) == 3
    assert "attempt 3/3 exit=1" in text
    assert "attempt 4/" not in text
