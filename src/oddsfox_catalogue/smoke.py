"""Explicit live smoke; ordinary tests and CI never invoke this module."""

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4


class SmokeFailure(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise SmokeFailure(message)


def command(report, name, argv, env=None):
    step = {"name": name}
    report["steps"].append(step)
    result = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=180)
    step["exit_code"] = result.returncode
    try:
        value = json.loads(result.stdout)
    except ValueError:
        try:
            value = json.loads(result.stderr.strip().splitlines()[-1])
        except (ValueError, IndexError):
            raise SmokeFailure(name + ": missing JSON diagnostics") from None
    require(isinstance(value, dict), name + ": invalid JSON diagnostics")
    for key in ("http_attempts", "requests", "downloaded_bytes"):
        require(
            key not in value or (type(value[key]) is int and value[key] >= 0),
            name + ": invalid accounting diagnostics",
        )
    require(
        "message" not in value or isinstance(value["message"], str),
        name + ": invalid error diagnostics",
    )
    step["result"] = value
    require(result.returncode == 0, name + ": " + value.get("message", "command failed"))
    return value


def finish(output, report, error=None):
    report["status"] = "failed" if error else "passed"
    if error:
        report["error"] = str(error) if isinstance(error, SmokeFailure) else type(error).__name__
    report["finished_at"] = datetime.now(UTC).isoformat()
    report["http_attempts"] = sum(
        step.get("result", {}).get("http_attempts", step.get("result", {}).get("requests", 0))
        for step in report["steps"]
    )
    report["downloaded_bytes"] = sum(
        step.get("result", {}).get("downloaded_bytes", 0) for step in report["steps"]
    )
    report["accounting_complete"] = bool(report["steps"]) and all(
        "result" in step and step.get("exit_code", 0) == 0 for step in report["steps"]
    )
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {
                "status": report["status"],
                "report": str(output / "report.json"),
                "http_attempts": report["http_attempts"],
                "downloaded_bytes": report["downloaded_bytes"],
                "error": report.get("error"),
            },
            indent=2,
        )
    )
    return 1 if error else 0


def new_output(value):
    output = (value or Path("data/smoke") / uuid4().hex).absolute()
    require(not output.exists(), "output must be a fresh directory")
    require(not any(path.is_symlink() for path in (output, *output.parents)), "symlink output")
    return output


def run(output, market_id, report):
    env = {key: value for key, value in os.environ.items() if not key.startswith("CATALOGUE_")}
    env["CATALOGUE_ROOT"] = str(output / "catalogue")
    cli = [sys.executable, "-m", "oddsfox_catalogue"]
    bundle = output / "bundle"
    refreshed = command(
        report,
        "refresh",
        [
            *cli,
            "metadata",
            "refresh",
            "--market-id",
            market_id,
            "--output",
            str(bundle),
            "--max-requests",
            "5",
            "--max-download-bytes",
            "1048576",
            "--max-response-bytes",
            "524288",
            "--max-output-bytes",
            "2097152",
        ],
        env,
    )
    require(refreshed["found"] == 1 and refreshed["failed"] == 0, "market lookup not found")
    manifest = json.loads((bundle / "manifest.json").read_text())
    require(manifest["contract"] == "oddsfox.polymarket.metadata.v1", "metadata contract drift")
    for name, expected in manifest["files"].items():
        require(Path(name).name == name, "unsafe bundle inventory")
        raw = (bundle / name).read_bytes()
        require(
            len(raw) == expected["size"] and hashlib.sha256(raw).hexdigest() == expected["sha256"],
            "bundle integrity mismatch",
        )
    markets = json.loads((bundle / "markets.json").read_text())
    outcomes = json.loads((bundle / "outcomes.json").read_text())
    require(
        len(markets) == 1 and markets[0]["usable"] and len(outcomes) >= 2,
        "market identities quarantined or incomplete",
    )
    replay = output / "replay"
    exported = command(
        report,
        "offline_export",
        [
            *cli,
            "metadata",
            "export",
            "--market-id",
            market_id,
            "--output",
            str(replay),
            "--max-output-bytes",
            "2097152",
        ],
        env,
    )
    for name in manifest["files"]:
        if name == "coverage.json":
            continue
        require(
            (bundle / name).read_bytes() == (replay / name).read_bytes(), "offline relation drift"
        )

    def coverage(path):
        return [
            {key: row[key] for key in ("market_id", "requested", "status", "error")}
            for row in json.loads((path / "coverage.json").read_text())
        ]

    require(coverage(bundle) == coverage(replay), "offline coverage drift")
    require(exported["http_attempts"] == 0, "offline export made HTTP requests")
    require(
        not (output / "catalogue/data/published/current.json").exists(), "global pointer changed"
    )
    report.update(
        metadata=str(bundle),
        metadata_sha256=refreshed["manifest_sha256"],
        market_id=market_id,
        catalogue_root=env["CATALOGUE_ROOT"],
        catalogue_executable=str(Path(sys.executable).parent / "catalogue"),
        native_assets=[
            {key: row[key] for key in ("asset_kind", "asset_id", "outcome_index")}
            for row in outcomes
        ],
        checks=[
            "live_refresh",
            "identity_validation",
            "checksums",
            "offline_relation_replay",
            "global_pointer_unchanged",
        ],
    )


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-id", default="5234660")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    output = new_output(args.output)
    checkout = next((p for p in (output, *output.parents) if (p / ".git").exists()), None)
    if checkout:
        ignored = subprocess.run(
            ["git", "-C", str(checkout), "check-ignore", "--no-index", str(output)],
            capture_output=True,
        )
        require(ignored.returncode == 0, "smoke output inside checkout must be ignored")
    output.mkdir(parents=True)
    report = {
        "product": "polymarket-events",
        "steps": [],
        "started_at": datetime.now(UTC).isoformat(),
        "limits": {"http_attempts": 5, "download_bytes": 1048576, "bundle_bytes": 2097152},
    }
    try:
        run(output, args.market_id, report)
    except (ValueError, OSError, subprocess.TimeoutExpired, KeyError) as error:
        return finish(output, report, error)
    return finish(output, report)


if __name__ == "__main__":
    raise SystemExit(main())
