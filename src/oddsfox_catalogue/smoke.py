"""Repeatable metadata or complete catalogue smoke against an explicit source."""

import argparse
import hashlib
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

CATALOGUE_LIMITS = {
    "http_attempts": 100,
    "download_bytes": 64 * 1024**2,
    "retained_bytes": 1024**3,
    "duration_s": 30 * 60,
}
REPORT_RESERVE = 4 * 1024**2
DIAGNOSTIC_LIMIT = 1024**2


class SmokeFailure(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise SmokeFailure(message)


def _totals(report):
    return (
        sum(
            step.get("result", {}).get("http_attempts", step.get("result", {}).get("requests", 0))
            for step in report["steps"]
        ),
        sum(step.get("result", {}).get("downloaded_bytes", 0) for step in report["steps"]),
    )


def _remaining_seconds(report):
    remaining = report["deadline_monotonic"] - time.monotonic()
    require(remaining > 0, "whole-workflow deadline exhausted")
    return remaining


def _storage(output, report, *, additional_bytes=0):
    from oddsfox_catalogue.limits import directory_bytes

    used = directory_bytes(output)
    require(
        used + additional_bytes + REPORT_RESERVE <= report["limits"]["retained_bytes"],
        "whole-workflow retained storage allowance exhausted",
    )
    return used


def catalogue_environment(output):
    env = {key: value for key, value in os.environ.items() if not key.startswith("CATALOGUE_")}
    # Loopback is explicit operator/test configuration; the transport validates its host.
    for key in ("CATALOGUE_GAMMA_BASE_URL", "CATALOGUE_GAMMA_ALLOW_LOOPBACK"):
        if all(
            name in os.environ
            for name in ("CATALOGUE_GAMMA_BASE_URL", "CATALOGUE_GAMMA_ALLOW_LOOPBACK")
        ):
            env[key] = os.environ[key]
    env["CATALOGUE_ROOT"] = str(output / "catalogue")
    return env


def remaining_environment(output, report, env):
    from oddsfox_catalogue.limits import directory_bytes

    result = dict(env)
    attempts, downloaded = _totals(report)
    remaining_s = _remaining_seconds(report)
    used = _storage(output, report)
    root_bytes = directory_bytes(Path(result["CATALOGUE_ROOT"]))
    allowance = report["limits"]["retained_bytes"] - used - REPORT_RESERVE + root_bytes
    result.update(
        CATALOGUE_CAPTURE_MAX_REQUESTS=str(max(1, report["limits"]["http_attempts"] - attempts)),
        CATALOGUE_CAPTURE_MAX_DOWNLOAD_BYTES=str(
            max(1, report["limits"]["download_bytes"] - downloaded)
        ),
        CATALOGUE_CAPTURE_MAX_DURATION_S=str(remaining_s),
        CATALOGUE_CAPTURE_MAX_RESPONSE_BYTES=str(16 * 1024**2),
        CATALOGUE_CAPTURE_MAX_RETAINED_BYTES=str(allowance),
        CATALOGUE_CAPTURE_MAX_TEMP_BYTES=str(min(allowance, 1024**3)),
        CATALOGUE_CAPTURE_WORKERS="1",
        CATALOGUE_GAMMA_REQUESTS_PER_SECOND="2",
    )
    return result


def _stop_group(process):
    from oddsfox_catalogue.signals import SIGNALS, Terminated

    with SIGNALS.hold() as held:
        for signum in (signal.SIGTERM, signal.SIGKILL):
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signum)
            if signum == signal.SIGTERM:
                # Kill surviving descendants even if their parent exits promptly.
                time.sleep(0.2)
                process.poll()
        process.wait(timeout=5)
    if held.signum is not None:
        raise Terminated(held.signum)


def _bounded_process(report, argv, env):
    from oddsfox_catalogue.signals import SIGNALS, Terminated

    timeout = _remaining_seconds(report)
    directory = report.get("output")
    with (
        tempfile.TemporaryFile(dir=directory) as stdout,
        tempfile.TemporaryFile(dir=directory) as stderr,
    ):
        with SIGNALS.hold() as started:
            process = subprocess.Popen(
                argv, env=env, stdout=stdout, stderr=stderr, start_new_session=True
            )
        try:
            if started.signum is not None:
                raise Terminated(started.signum)
            while True:
                if time.monotonic() >= report["deadline_monotonic"]:
                    raise subprocess.TimeoutExpired(argv, timeout)
                require(
                    all(
                        os.fstat(handle.fileno()).st_size <= DIAGNOSTIC_LIMIT
                        for handle in (stdout, stderr)
                    ),
                    "command diagnostics exceed their finite allowance",
                )
                if directory:
                    _storage(Path(directory), report)
                try:
                    process.wait(
                        timeout=min(0.1, max(0, report["deadline_monotonic"] - time.monotonic()))
                    )
                    break
                except subprocess.TimeoutExpired:
                    continue
        finally:
            _stop_group(process)
        require(
            all(
                os.fstat(handle.fileno()).st_size <= DIAGNOSTIC_LIMIT for handle in (stdout, stderr)
            ),
            "command diagnostics exceed their finite allowance",
        )
        stdout.seek(0)
        stderr.seek(0)
        return subprocess.CompletedProcess(
            argv,
            process.returncode,
            stdout.read(DIAGNOSTIC_LIMIT + 1).decode("utf-8", errors="replace"),
            stderr.read(DIAGNOSTIC_LIMIT + 1).decode("utf-8", errors="replace"),
        )


def command(report, name, argv, env=None, *, expected_exit=0, capture=False):
    step = {
        "name": name,
        "capture": capture,
        "expected_exit_code": expected_exit,
        "successful": False,
    }
    report["steps"].append(step)
    if report.get("workflow") == "catalogue":
        if capture:
            attempts, downloaded = _totals(report)
            require(
                attempts < report["limits"]["http_attempts"], "shared request allowance exhausted"
            )
            require(
                downloaded < report["limits"]["download_bytes"],
                "shared download allowance exhausted",
            )
        if report.get("output") and env is not None:
            env = remaining_environment(Path(report["output"]), report, env)
        result = _bounded_process(report, argv, env)
    else:
        result = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=180)
    step["exit_code"] = result.returncode
    if expected_exit != 0:
        require(not capture, "capture cannot use an expected failure")
        require(result.returncode == expected_exit, name + ": fault injection did not fire")
        step.update(result={}, successful=True)
        return {}
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
    if report.get("workflow") == "catalogue":
        value = {key: item for key, item in value.items() if key != "message"}
        if result.returncode != 0:
            value = {
                key: value[key] for key in ("http_attempts", "downloaded_bytes") if key in value
            }
    step["result"] = value
    if capture:
        require(
            all(
                type(value.get(key)) is int and value[key] >= 0
                for key in ("http_attempts", "downloaded_bytes")
            ),
            name + ": missing acquisition accounting",
        )
        attempts, downloaded = _totals(report)
        require(attempts <= report["limits"]["http_attempts"], "shared request allowance exceeded")
        require(
            downloaded <= report["limits"]["download_bytes"], "shared download allowance exceeded"
        )
    message = (
        "command failed"
        if report.get("workflow") == "catalogue"
        else value.get("message", "command failed")
    )
    require(result.returncode == 0, name + ": " + message)
    step["successful"] = True
    return value


def _verified_command(report, name, argv, env):
    result = command(report, name, argv, env)
    require(result.get("verified") is True, name + ": verification did not succeed")
    return result


def finish(output, report, error=None):
    report["status"] = "failed" if error else "passed"
    if error:
        report["error"] = str(error) if isinstance(error, SmokeFailure) else type(error).__name__
    report["finished_at"] = datetime.now(UTC).isoformat()
    report["http_attempts"], report["downloaded_bytes"] = _totals(report)
    report["accounting_complete"] = bool(report["steps"]) and all(
        step.get("successful", "result" in step and step.get("exit_code", 0) == 0)
        for step in report["steps"]
    )
    saved = {key: value for key, value in report.items() if key != "deadline_monotonic"}
    encoded = (json.dumps(saved, indent=2) + "\n").encode()
    require(len(encoded) <= REPORT_RESERVE, "smoke report exceeds its finite allowance")
    if report.get("workflow") == "catalogue" and "retained_bytes" in report["limits"]:
        from oddsfox_catalogue.limits import directory_bytes

        used = directory_bytes(output)
        prior = output / "report.json"
        used -= prior.stat().st_size if prior.exists() else 0
        for _ in range(3):
            report["retained_bytes"] = saved["retained_bytes"] = used + len(encoded)
            encoded = (json.dumps(saved, indent=2) + "\n").encode()
        require(
            used + len(encoded) <= report["limits"]["retained_bytes"],
            "whole-workflow storage limit exceeded",
        )
    (output / "report.json").write_bytes(encoded)
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
    env = catalogue_environment(output)
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


def _read_catalogue_state():
    from oddsfox_catalogue.certification import assert_build_valid
    from oddsfox_catalogue.config import load_settings
    from oddsfox_catalogue.publish import current_release, verify_release
    from oddsfox_catalogue.runlock import run_lock
    from oddsfox_catalogue.semantics import SEMANTIC_RELATIONS

    settings = load_settings()
    with run_lock(settings.run_lock_path):
        receipt = assert_build_valid(settings)
        pointer = current_release(settings)
        require(pointer is not None, "catalogue publication is missing")
        release = settings.published_dir / pointer["path"]
        manifest = verify_release(settings, release, manifest_sha256=pointer["manifest_sha256"])
        binding = receipt["binding"]
        require(
            set(binding["warehouse"]["relations"]) == set(SEMANTIC_RELATIONS)
            and len(binding["published"]) == 7,
            "semantic comparison boundary is incomplete",
        )
        require(
            all(
                all(manifest["tables"][name][key] == value for key, value in relation.items())
                for name, relation in binding["published"].items()
            ),
            "published semantic content differs from the warehouse",
        )
        coverage = json.loads((release / "coverage.json").read_text())
    return {"binding": binding, "manifest": manifest, "coverage": coverage}


def _catalogue_state(output, report, env):
    result = command(
        report,
        "semantic_snapshot",
        [
            sys.executable,
            "-I",
            "-c",
            "import json; from oddsfox_catalogue.smoke import _read_catalogue_state; "
            "print(json.dumps(_read_catalogue_state()))",
        ],
        env,
    )
    return result["binding"], result["manifest"], result["coverage"]


def _read_final_details():
    from oddsfox_catalogue import __version__
    from oddsfox_catalogue.config import load_settings
    from oddsfox_catalogue.contract import projection_queries
    from oddsfox_catalogue.resources import dbt_project_dir
    from oddsfox_catalogue.runlock import run_lock
    from oddsfox_catalogue.semantics import bounded_connection

    settings = load_settings()
    with run_lock(settings.run_lock_path), bounded_connection(settings) as connection:
        reasons = connection.execute(
            f"SELECT reason, count(*) FROM ({projection_queries()['quarantine']}) GROUP BY reason ORDER BY reason"
        ).fetchall()
    resource_hash = hashlib.sha256()
    project = dbt_project_dir()
    for path in sorted(project.rglob("*")):
        if path.is_file() and path.suffix in {".sql", ".yml"}:
            resource_hash.update(
                path.relative_to(project).as_posix().encode() + b"\0" + path.read_bytes()
            )
    return {
        "quarantine_reasons": {reason: count for reason, count in reasons},
        "runtime": {
            "python": sys.version.split()[0],
            "package": __version__,
            "dbt_resource_sha256": resource_hash.hexdigest(),
            "smoke_source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        },
    }


def _metadata_fields(output, market_ids, result, report):
    from oddsfox_catalogue.normalization import _asset_ids

    bundle = output / "bundle"
    raw_manifest = (bundle / "manifest.json").read_bytes()
    require(
        hashlib.sha256(raw_manifest).hexdigest() == result["manifest_sha256"],
        "metadata manifest drift",
    )
    manifest = json.loads(raw_manifest)
    require(manifest["contract"] == "oddsfox.polymarket.metadata.v1", "metadata contract drift")
    for name, expected in manifest["files"].items():
        require(
            Path(name).name == name and not (bundle / name).is_symlink(), "unsafe bundle inventory"
        )
        raw = (bundle / name).read_bytes()
        require(
            len(raw) == expected["size"] and hashlib.sha256(raw).hexdigest() == expected["sha256"],
            "bundle integrity mismatch",
        )
    markets = json.loads((bundle / "markets.json").read_text())
    outcomes = json.loads((bundle / "outcomes.json").read_text())
    require(
        {row["market_id"] for row in markets} == set(market_ids)
        and len(markets) == len(market_ids)
        and all(row["market_id"] in market_ids for row in outcomes),
        "metadata handoff differs from the selected market scope",
    )
    identities = []
    for market in markets:
        rows = [row for row in outcomes if row["market_id"] == market["market_id"]]
        require(type(market["usable"]) is bool, "metadata identity status must be a boolean")
        if market["usable"]:
            kind = {"ctf": "ctf_token", "polymarket_v2": "poly_v2_position"}.get(market["protocol"])
            require(
                len(rows) >= 2
                and kind is not None
                and all(row["asset_kind"] == kind for row in rows)
                and all(type(row["asset_id"]) is str for row in rows)
                and all(type(row["outcome_index"]) is int for row in rows)
                and sorted(row["outcome_index"] for row in rows) == list(range(1, len(rows) + 1)),
                "usable market has incomplete nominated outcomes",
            )
            _asset_ids([row["asset_id"] for row in rows], "asset_id", len(rows))
        else:
            require(
                isinstance(market["identity_error"], str)
                and bool(market["identity_error"].strip())
                and not rows,
                "unusable market must have a reason and no nominated outcomes",
            )
        identities.append({key: market[key] for key in ("market_id", "usable", "identity_error")})
    require(
        len({(row["asset_kind"], row["asset_id"]) for row in outcomes}) == len(outcomes),
        "metadata assets have conflicting owners",
    )
    first = [row for row in outcomes if row["market_id"] == market_ids[0]]
    report.update(
        metadata=str(bundle),
        metadata_sha256=result["manifest_sha256"],
        market_id=market_ids[0],
        selected_market_ids=market_ids,
        metadata_identities=identities,
        catalogue_root=str(output / "catalogue"),
        catalogue_executable=str(Path(sys.executable).parent / "catalogue"),
        native_assets=[
            {key: row[key] for key in ("asset_kind", "asset_id", "outcome_index")} for row in first
        ],
    )


def run_catalogue(output, market_ids, report):
    from oddsfox_catalogue.limits import directory_bytes

    require(
        1 <= len(market_ids) <= 5 and len(set(market_ids)) == len(market_ids),
        "select one to five distinct market IDs",
    )
    require(
        all(value.isascii() and value.isdigit() and int(value) > 0 for value in market_ids),
        "market IDs must be positive integers",
    )
    report.update(
        workflow="catalogue",
        limits=dict(CATALOGUE_LIMITS),
        output=str(output),
        deadline_monotonic=report.get(
            "deadline_monotonic", time.monotonic() + CATALOGUE_LIMITS["duration_s"]
        ),
    )
    env = catalogue_environment(output)
    cli = [sys.executable, "-m", "oddsfox_catalogue"]
    selection = [arg for market_id in market_ids for arg in ("--market-id", market_id)]
    captures, previous = [], None
    for number in (1, 2):
        captured = command(
            report,
            f"capture_{number}",
            [*cli, "capture", "--mode", "selected", *selection],
            env,
            capture=True,
        )
        require(
            captured["status"] == "captured" and captured["records"] > 0,
            "selected capture is empty or incomplete",
        )
        loaded = command(
            report, f"load_{number}", [*cli, "load", "--batch-id", captured["batch_id"]], env
        )
        require(loaded["pages_loaded"] > 0, "selected capture did not load")
        command(report, f"replay_{number}", [*cli, "replay"], env)
        _verified_command(report, f"verify_{number}", [*cli, "verify"], env)
        binding, manifest, coverage = _catalogue_state(output, report, env)
        rows = binding["warehouse"]["relations"]
        require(
            rows["core.markets_current"]["rows"] == len(market_ids),
            "selected scope expanded or lost markets",
        )
        require(
            rows["bronze.market_observations"]["rows"] >= len(market_ids),
            "market observations are empty",
        )
        if previous:
            require(
                rows["history.market_history"]["rows"]
                > previous["warehouse"]["relations"]["history.market_history"]["rows"]
                and len(binding["capture"]["batch_ids"]) == 2,
                "second acquisition did not retain a second observation",
            )
        captures.append(captured)
        previous = binding
    exported = command(
        report,
        "offline_export",
        [
            *cli,
            "metadata",
            "export",
            *selection,
            "--output",
            str(output / "bundle"),
            "--max-output-bytes",
            "2097152",
        ],
        env,
    )
    require(
        exported["http_attempts"] == 0
        and exported["found"] == len(market_ids)
        and exported["failed"] == 0,
        "offline metadata handoff is incomplete",
    )
    _metadata_fields(output, market_ids, exported, report)
    command(report, "offline_replay", [*cli, "replay"], env)
    _verified_command(report, "verify_replay", [*cli, "verify"], env)
    replayed, manifest, coverage = _catalogue_state(output, report, env)
    require(replayed == previous, "complete offline semantic replay drift")
    rebuilt = command(report, "raw_rebuild", [*cli, "rebuild", "--verify"], env)
    require(rebuilt["matched"] is True and not rebuilt["mismatches"], "raw semantic rebuild drift")
    expected_comparisons = {
        *replayed["warehouse"]["relations"],
        *(f"schema:{name}" for name in replayed["warehouse"]["schemas"]),
        *(f"published:{name}" for name in replayed["published"]),
        "coverage",
        "capture_inventory",
    }
    require(
        set(rebuilt["tables"]) == expected_comparisons, "raw rebuild omitted declared comparisons"
    )
    _storage(
        output, report, additional_bytes=directory_bytes(output / "catalogue") + REPORT_RESERVE
    )
    backup = command(
        report, "backup_create", [*cli, "backup", "create", "--dest", str(output / "backups")], env
    )
    backup_path = Path(backup["backup"])
    require(
        backup["verified"] is True and backup_path.is_relative_to(output / "backups"),
        "backup is unverified or unconfined",
    )
    _verified_command(report, "backup_verify", [*cli, "backup", "verify", str(backup_path)], env)
    _storage(output, report, additional_bytes=directory_bytes(backup_path) + REPORT_RESERVE)
    restored = output / "restored"
    restoration = command(
        report,
        "backup_restore",
        [*cli, "backup", "restore", str(backup_path), "--destination", str(restored)],
        env,
    )
    require(
        restoration.get("restored") is True and restoration.get("destination") == str(restored),
        "backup restoration did not succeed",
    )
    pointer = output / "catalogue/data/published/current.json"
    prior_pointer = pointer.read_bytes()
    require(
        (restored / "data/published/current.json").read_bytes() == prior_pointer,
        "restored publication pointer differs from the backup",
    )
    restored_env = {**env, "CATALOGUE_ROOT": str(restored)}
    _verified_command(report, "verify_restore", [*cli, "verify"], restored_env)
    restored_binding, _, _ = _catalogue_state(output, report, restored_env)
    require(restored_binding == replayed, "restored catalogue semantic content drift")
    command(
        report,
        "injected_publication",
        [*cli, "publish"],
        {**env, "CATALOGUE_FAULT": "mid_publish"},
        expected_exit=87,
    )
    require(
        pointer.read_bytes() == prior_pointer, "interrupted publication replaced the valid pointer"
    )
    _verified_command(report, "verify_after_failure", [*cli, "verify"], env)
    final_binding, _, _ = _catalogue_state(output, report, env)
    require(final_binding == replayed, "interrupted publication changed semantic content")
    details = command(
        report,
        "runtime_evidence",
        [
            sys.executable,
            "-I",
            "-c",
            "import json; from oddsfox_catalogue.smoke import _read_final_details; "
            "print(json.dumps(_read_final_details()))",
        ],
        env,
    )
    report.update(
        captures=captures,
        row_counts={
            name: result["rows"] for name, result in replayed["warehouse"]["relations"].items()
        },
        published_rows={name: result["rows"] for name, result in replayed["published"].items()},
        quarantine_rows=manifest["tables"]["quarantine"]["rows"],
        quarantine_reasons=details["quarantine_reasons"],
        coverage=coverage,
        semantic_binding=replayed,
        recovery={
            "raw_rebuild": True,
            "backup_verified": True,
            "restored": True,
            "previous_pointer_preserved": True,
        },
        semantic_comparisons={
            "offline_replay": True,
            "raw_rebuild": True,
            "restore": True,
        },
        compared_relations={
            "warehouse": 29,
            "published": 7,
            "warehouse_schemas": 30,
        },
        runtime=details["runtime"],
        retained_bytes=_storage(output, report),
        duration_s=CATALOGUE_LIMITS["duration_s"] - _remaining_seconds(report),
        checks=[
            "selected_capture",
            "nonempty_load_build_publication",
            "second_observation",
            "metadata_v1_handoff",
            "complete_semantic_replay",
            "raw_rebuild",
            "backup_verification",
            "fresh_restore",
            "previous_pointer_preserved",
        ],
    )


def main(argv=None):
    from oddsfox_catalogue.cli import _install_termination_handlers
    from oddsfox_catalogue.signals import Terminated

    restore_signals = _install_termination_handlers()
    try:
        return _main(argv)
    except Terminated as error:
        return 128 + error.signum
    finally:
        restore_signals()


def _main(argv=None):
    from oddsfox_catalogue.signals import Terminated

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", choices=("metadata", "catalogue"), default="metadata")
    parser.add_argument("--market-id", action="append")
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
        "workflow": args.workflow,
        "steps": [],
        "started_at": datetime.now(UTC).isoformat(),
        "limits": {"http_attempts": 5, "download_bytes": 1048576, "bundle_bytes": 2097152},
    }
    try:
        if args.workflow == "catalogue":
            require(args.market_id is not None, "catalogue smoke requires explicit market IDs")
            run_catalogue(output, args.market_id, report)
        else:
            require(
                not args.market_id or len(args.market_id) == 1,
                "metadata smoke selects one market ID",
            )
            run(output, (args.market_id or ["5234660"])[0], report)
    except Terminated as error:
        finish(output, report, error)
        return 128 + error.signum
    except (ValueError, OSError, subprocess.TimeoutExpired, KeyError, RuntimeError) as error:
        return finish(output, report, error)
    return finish(output, report)


if __name__ == "__main__":
    raise SystemExit(main())
