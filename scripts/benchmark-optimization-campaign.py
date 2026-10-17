#!/usr/bin/env python3
"""Compare frozen catalogue runtime variants using one synthetic full-pass budget.

Version 1 manifests retain the binding-first workflow. Version 2 compares the
outcome-grouping and queue candidates independently against one common baseline.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import signal
import stat
import subprocess
import time
from pathlib import Path

HARNESS_PATH = Path(__file__).with_name("benchmark-full-pass.py")
SPEC = importlib.util.spec_from_file_location("optimization_full_pass", HARNESS_PATH)
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)

CAMPAIGN_BUDGET_S = 1800.0
VARIANTS = {
    "outcome_grouping": "current_store.py",
    "queue": "current_reconcile.py",
    "projection": "current_sync.py",
}
MANIFEST_KEYS = {"baseline", "binding", *VARIANTS}
V2_CANDIDATES = {"outcome_grouping", "queue"}
HEX_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class CampaignIntegrityError(RuntimeError):
    pass


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"manifest contains a duplicate key: {key}")
        result[key] = value
    return result


def _load_manifest(path: Path) -> dict:
    path = bench.safe_path(path)
    if not path.is_file() or path.stat().st_size > 4 * 1024**2:
        raise ValueError("manifest must be a regular file no larger than 4 MiB")
    data = json.loads(path.read_text(), object_pairs_hook=_unique_object)
    if not isinstance(data, dict):
        raise ValueError("manifest must be a JSON object")
    if "version" not in data:
        if not {"baseline", "binding"}.issubset(data) or not set(data).issubset(MANIFEST_KEYS):
            raise ValueError("legacy manifest requires baseline and binding package entries")
        return {**data, "_version": 1}
    if type(data.get("version")) is not int or data["version"] != 2:
        raise ValueError("manifest version must be integer 2")
    if set(data) != {"version", "baseline", "candidates"}:
        raise ValueError("version 2 manifest requires only version, baseline and candidates")
    candidates = data["candidates"]
    if (
        not isinstance(candidates, dict)
        or not candidates
        or not set(candidates).issubset(V2_CANDIDATES)
    ):
        raise ValueError("version 2 manifest contains unknown or missing candidates")
    return {"_version": 2, "baseline": data["baseline"], **candidates}


def _entry_path(entry: object, manifest_path: Path) -> tuple[Path, dict[str, str]]:
    if not isinstance(entry, dict) or set(entry) != {"path", "source_hashes"}:
        raise ValueError("each package entry must contain only path and source_hashes")
    raw_path, expected = entry["path"], entry["source_hashes"]
    if not isinstance(raw_path, str) or not raw_path:
        raise ValueError("package path must be a nonempty string")
    package = Path(raw_path)
    if not package.is_absolute():
        package = manifest_path.parent / package
    package = bench.safe_path(package)
    if not isinstance(expected, dict) or not expected:
        raise ValueError("source_hashes must inventory every Python source file")
    for name, value in expected.items():
        if (
            not isinstance(name, str)
            or Path(name).is_absolute()
            or ".." in Path(name).parts
            or Path(name).suffix != ".py"
            or not isinstance(value, str)
            or not HEX_SHA256.fullmatch(value)
        ):
            raise ValueError("manifest source hash inventory is malformed")
    return package, expected


def _validate_package_tree(package: Path, budget=None) -> list[Path]:
    package = bench.safe_path(package)
    if not package.is_dir():
        raise ValueError("runtime package must be a regular directory")
    paths = []
    for path in sorted(package.rglob("*")):
        if budget:
            budget.admit(0.001)
        mode = path.lstat().st_mode
        if not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
            raise ValueError("runtime package contains a symlink or special file")
        if stat.S_ISREG(mode):
            if path.suffix != ".py":
                raise ValueError("runtime package contains non-source files")
            paths.append(path)
    return paths


def _hash_package(package: Path, budget=None) -> dict[str, str]:
    paths = _validate_package_tree(package, budget)
    result = {}
    for path in paths:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while True:
                if budget:
                    budget.admit(0.001)
                block = stream.read(1024 * 1024)
                if not block:
                    break
                digest.update(block)
        result[str(path.relative_to(package))] = digest.hexdigest()
    return result


def _resolve_entry(entry: object, manifest_path: Path, budget=None) -> tuple[Path, dict[str, str]]:
    package, expected = _entry_path(entry, manifest_path)
    actual = _hash_package(package, budget)
    if actual != expected:
        raise ValueError(f"source hash inventory does not match package: {package}")
    return package, actual


def _preflight_paths(manifest, manifest_path: Path, output: Path) -> dict:
    bench.validate_disjoint(output, manifest_path)
    paths = {
        name: _entry_path(entry, manifest_path)
        for name, entry in manifest.items()
        if name != "_version"
    }
    package_paths = [package for package, _expected in paths.values()]
    for package in package_paths:
        bench.validate_disjoint(output, package)
    for index, package in enumerate(package_paths):
        for other in package_paths[index + 1 :]:
            if package != other and (package in other.parents or other in package.parents):
                raise ValueError("manifest runtime package paths must not overlap")
    return paths


def _preflight(paths: dict, budget) -> dict:
    resolved = {}
    for name, (package, expected_hashes) in paths.items():
        actual_hashes = _hash_package(package, budget)
        if actual_hashes != expected_hashes:
            raise ValueError(f"source hash inventory does not match package: {package}")
        resolved[name] = (package, actual_hashes)
    return resolved


def _validate_variant_ownership(
    binding_hashes: dict[str, str], candidate_hashes: dict[str, str], owned_file: str
) -> bool:
    if set(binding_hashes) != set(candidate_hashes):
        raise ValueError(f"secondary variant changes files outside {owned_file}")
    changed = {name for name in binding_hashes if binding_hashes[name] != candidate_hashes[name]}
    if changed - {owned_file}:
        raise ValueError(f"secondary variant changes files outside {owned_file}")
    if owned_file not in binding_hashes:
        raise ValueError(f"secondary variant does not contain {owned_file}")
    return bool(changed)


def _copy_tree_bounded(source: Path, target: Path, budget) -> None:
    _validate_package_tree(source, budget)
    target.mkdir(parents=True)
    for path in sorted(source.rglob("*")):
        budget.admit(0.001)
        relative = path.relative_to(source)
        destination = target / relative
        if path.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        with path.open("rb") as source_stream, destination.open("wb") as target_stream:
            while True:
                budget.admit(0.001)
                block = source_stream.read(1024 * 1024)
                if not block:
                    break
                target_stream.write(block)


def _copy_file_bounded(source: Path, target: Path, budget) -> None:
    with source.open("rb") as source_stream, target.open("wb") as target_stream:
        while True:
            budget.admit(0.001)
            block = source_stream.read(1024 * 1024)
            if not block:
                break
            target_stream.write(block)


def _freeze_packages(resolved: dict, output: Path, budget) -> dict:
    frozen, originals = {}, {}
    for name, (source, expected_hashes) in resolved.items():
        budget.admit(1)
        target = output / "inputs" / name / "oddsfox_catalogue"
        target.parent.mkdir(parents=True, exist_ok=True)
        _copy_tree_bounded(source, target, budget)
        actual_hashes = _hash_package(target, budget)
        if actual_hashes != expected_hashes:
            raise ValueError(f"package changed while it was being frozen: {name}")
        frozen[name] = target
        originals[name] = {"path": str(source), "source_hashes": expected_hashes}
    comparison_name = "binding" if "binding" in frozen else "baseline"
    comparison_hashes = _hash_package(frozen[comparison_name], budget)
    for name, owned_file in VARIANTS.items():
        if name in frozen and name != comparison_name:
            changed = _validate_variant_ownership(
                comparison_hashes, _hash_package(frozen[name], budget), owned_file
            )
            originals[name]["eligible"] = changed
            if not changed:
                originals[name]["deferred_reason"] = "variant is identical to binding"
    return {"packages": frozen, "provenance": originals}


def _copy_package(source: Path, target: Path, budget) -> str:
    _copy_tree_bounded(source, target, budget)
    return bench.canonical_hash(_hash_package(target, budget))


def _digest_bounded(path: Path, budget) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            budget.admit(0.001)
            block = stream.read(1024 * 1024)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _campaign_provenance(budget) -> dict:
    budget.admit(0.001)
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=bench.REPO,
        check=True,
        capture_output=True,
        text=True,
        timeout=budget.remaining,
    ).stdout.strip()
    paths = [
        Path(__file__).resolve(),
        Path(bench.__file__).resolve(),
        Path(bench.REPO) / "tests" / "fakes" / "world.py",
        Path(bench.REPO) / "tests" / "fakes" / "capacity_source.py",
    ]
    return {
        "git_revision": revision,
        "source_hashes": {
            str(path.relative_to(bench.REPO)): _digest_bounded(path, budget) for path in paths
        },
    }


def _assert_campaign_provenance(expected: dict, budget) -> None:
    if _campaign_provenance(budget) != expected:
        raise CampaignIntegrityError("benchmark harness changed during campaign")


def _optional_stage_estimate(binding_samples: list[dict]) -> float:
    binding_walls = [row["wall_s"] for row in binding_samples if row.get("category") == "binding"]
    if len(binding_walls) != 3:
        raise ValueError("primary binding comparison is incomplete")
    return max(binding_walls) * 1.25 * 7


def _candidate_stage_estimate(baseline_warm: dict, candidate_warm: dict) -> float:
    """Three paired AB/BA/AB runs with 25 percent headroom."""
    return 1.25 * 3 * (baseline_warm["wall_s"] + candidate_warm["wall_s"])


def _best_v2_candidate(report: dict) -> str | None:
    qualified = report.get("qualified_candidates", {})
    eligible = [name for name in ("outcome_grouping", "queue") if name in qualified]
    return max(
        eligible,
        key=lambda name: (
            qualified[name]["comparison"].get("saved_s", 0.0),
            name == "outcome_grouping",
        ),
        default=None,
    )


def _v2_comparison_complete(report: dict, candidate: str) -> bool:
    stage = report.get("stages", {}).get(f"{candidate}_vs_baseline")
    if not isinstance(stage, dict) or not isinstance(stage.get("comparison"), dict):
        return False
    if stage["comparison"].get("status") not in {"qualified", "deferred", "invalid"}:
        return False
    expected = {(side, pair): 1 for side in ("baseline", candidate) for pair in (1, 2, 3)}
    observed = {key: 0 for key in expected}
    samples = stage.get("samples")
    if not isinstance(samples, list):
        return False
    for sample in samples:
        if not isinstance(sample, dict):
            return False
        key = (sample.get("category"), sample.get("pair"))
        if key not in observed:
            return False
        observed[key] += 1
    return observed == expected


def _v2_unqualified_status(eligible: list[str], report: dict) -> str:
    if not eligible:
        return "deferred"
    if any(not _v2_comparison_complete(report, name) for name in eligible):
        return "insufficient_evidence"
    return "deferred"


def _retain_best_v2_candidate(report, packages, provenance, seed, seed_hash, harness, budget):
    report["retained"] = []
    candidate = _best_v2_candidate(report)
    if candidate is None:
        return
    try:
        budget.holdback_s = 0.0  # spend the reserved final integrity window
        _integrity_check(packages, provenance, seed, seed_hash, budget)
        _assert_campaign_provenance(harness, budget)
    except Exception:
        return
    report["retained"] = [candidate]


def _assemble_v2_combination(packages, candidates, output, budget):
    baseline = packages["baseline"]
    target = output / "final" / "oddsfox_catalogue"
    _copy_package(baseline, target, budget)
    for name in candidates:
        owned = VARIANTS[name]
        _copy_file_bounded(packages[name] / owned, target / owned, budget)
    baseline_hashes = _hash_package(baseline, budget)
    combined_hashes = _hash_package(target, budget)
    if set(baseline_hashes) != set(combined_hashes):
        raise CampaignIntegrityError("combined runtime source inventory differs from baseline")
    changed = {name for name in baseline_hashes if baseline_hashes[name] != combined_hashes[name]}
    if changed != {VARIANTS[name] for name in candidates}:
        raise CampaignIntegrityError("combined runtime contains changes outside qualified files")
    return target, combined_hashes


def _run_v2(seed, output, budget, warmed, report, packages, provenance, seed_hash, harness):
    report["retained"] = []
    report["qualified_candidates"] = {}
    budget.holdback_s = 15.0  # final source, seed and harness integrity checks

    eligible = [
        name
        for name in ("outcome_grouping", "queue")
        if name in packages and provenance[name].get("eligible")
    ]
    first_candidate_started = time.monotonic()
    _warm("baseline", packages["baseline"], seed, output, budget, warmed, report)
    baseline_warm = report["warmups"]["baseline"]["sample"]
    first_comparison_duration = None
    for name in eligible:
        if first_comparison_duration is not None:
            budget.holdback_s = 15.0 + first_comparison_duration * 1.25
        candidate_started = (
            first_candidate_started if first_comparison_duration is None else time.monotonic()
        )
        _warm(name, packages[name], seed, output, budget, warmed, report)
        candidate_warm = report["warmups"][name]["sample"]
        estimate = _candidate_stage_estimate(baseline_warm, candidate_warm)
        report.setdefault("admission", {})[name] = {
            "projected_scored_seconds": estimate,
            "headroom_fraction": 0.25,
            "integrity_reserve_seconds": 15.0,
            "shutdown_reserve_seconds": bench.SHUTDOWN_RESERVE_S,
        }
        if first_comparison_duration is not None:
            combo_reserve = first_comparison_duration * 1.25
            report["admission"][name]["combination_reserve_seconds"] = combo_reserve
        if estimate > budget.remaining:
            report.setdefault("deferred", {})[name] = (
                "insufficient_evidence: projected paired samples do not fit the shared budget"
            )
            bench._save(output / "report.json", report)
            continue

        scored = _compare_stage(
            f"{name}_vs_baseline",
            "baseline",
            packages["baseline"],
            name,
            packages[name],
            seed,
            output,
            budget,
            warmed,
            report,
        )
        duration = time.monotonic() - candidate_started
        _assert_campaign_provenance(harness, budget)
        stage = report["stages"][f"{name}_vs_baseline"]
        stage["comparison_duration_seconds"] = duration
        stage["projected_scored_seconds"] = estimate
        if scored["status"] == "qualified":
            report["qualified_candidates"][name] = {
                "comparison": scored,
                "stage": f"{name}_vs_baseline",
            }
            if first_comparison_duration is None:
                first_comparison_duration = duration
        elif scored["status"] == "invalid":
            report.setdefault("invalid_candidates", []).append(name)
        else:
            report.setdefault("deferred", {})[name] = scored.get("reason", scored["status"])
        bench._save(output / "report.json", report)

    qualified = list(report["qualified_candidates"])
    if report.get("invalid_candidates"):
        best = _best_v2_candidate(report)
        report["retained"] = [best] if best else []
        report["status"] = "invalid"
        report.setdefault("deferred", {})["campaign"] = (
            "a candidate produced non-equivalent scheduler or publication results"
        )
        budget.holdback_s = 15.0
        return
    if len(qualified) < 2:
        best = _best_v2_candidate(report)
        if best is not None:
            report["retained"] = [best]
            report["status"] = "qualified"
            report["final_combination"] = {
                "status": "reused_independent_evidence",
                "stage": report["qualified_candidates"][best]["stage"],
                "source_hash": bench.canonical_hash(_hash_package(packages[best], budget)),
            }
        else:
            report["status"] = _v2_unqualified_status(eligible, report)
        budget.holdback_s = 15.0
        return

    combo_reserve = first_comparison_duration * 1.25
    # The scored comparison reserve is now available for combination assembly,
    # its warm-up and the final paired samples; keep integrity time protected.
    budget.holdback_s = 15.0
    try:
        combined, combined_hashes = _assemble_v2_combination(packages, qualified, output, budget)
        _warm("final_combination", combined, seed, output, budget, warmed, report)
        warm = report["warmups"]["final_combination"]["sample"]
        estimate = _candidate_stage_estimate(baseline_warm, warm)
        report["final_combination_source_hashes"] = combined_hashes
        report["final_combination_projected_seconds"] = estimate
        # Spend the explicitly reserved comparison window while retaining the
        # independent final-integrity and shutdown reserves.
        budget.admit(estimate)
        scored = _compare_stage(
            "final_combination_vs_baseline",
            "baseline",
            packages["baseline"],
            "final_combination",
            combined,
            seed,
            output,
            budget,
            warmed,
            report,
        )
        _assert_campaign_provenance(harness, budget)
        if _hash_package(combined, budget) != combined_hashes:
            raise CampaignIntegrityError("final runtime package changed during its measured stage")
        if scored["status"] == "qualified":
            report["retained"] = [*qualified, "combined"]
            report["status"] = "qualified"
            report["final_combination"] = {
                "status": "qualified",
                "comparison": scored,
                "source_hash": bench.canonical_hash(combined_hashes),
            }
        else:
            best = _best_v2_candidate(report)
            report["retained"] = [best] if best else []
            if scored["status"] == "invalid":
                report["status"] = "invalid"
                report.setdefault("deferred", {})["combination"] = (
                    "combined scheduler or publication results differ from baseline"
                )
            else:
                report["status"] = "qualified_individual_combination_deferred"
                report.setdefault("deferred", {})["combination"] = scored.get(
                    "reason", scored["status"]
                )
            report["final_combination"] = {"status": scored["status"], "comparison": scored}
    except (TimeoutError, subprocess.TimeoutExpired) as exc:
        best = _best_v2_candidate(report)
        report["retained"] = [best] if best else []
        report["status"] = "qualified_individual_combination_deferred"
        report.setdefault("deferred", {})["combination"] = str(exc)
        report["final_combination"] = {"status": "insufficient_evidence", "reason": str(exc)}
    budget.holdback_s = 15.0


def _stage_sample(seed, output, package, budget, label, warmup=False):
    return bench.run_sample(seed, output, package, budget, "perf", label, warmup=warmup)


def _save_stage_sample(report, output, stage, row, category, pair=None):
    row = dict(row)
    row["category"] = category
    if pair is not None:
        row["pair"] = pair
    report.setdefault("stages", {}).setdefault(stage, {"status": "running", "samples": []})[
        "samples"
    ].append(row)
    bench._save(output / "report.json", report)
    return row


def _warm(runtime_name, package, seed, output, budget, warmed, report):
    source_hash = bench.canonical_hash(_hash_package(package, budget))
    if source_hash in warmed:
        return
    warm_dir = output / "warmups" / source_hash[:16]
    warm_dir.mkdir(parents=True)
    sample = _stage_sample(seed, warm_dir, package, budget, f"warm-{source_hash[:10]}")
    warmed.add(source_hash)
    report.setdefault("warmups", {})[runtime_name] = {
        "source_hash": source_hash,
        "sample": sample,
    }
    bench._save(output / "report.json", report)


def _compare_stage(stage, left_name, left, right_name, right, seed, output, budget, warmed, report):
    _warm(left_name, left, seed, output, budget, warmed, report)
    _warm(right_name, right, seed, output, budget, warmed, report)
    rows = {left_name: [], right_name: []}
    order = (
        (left_name, left),
        (right_name, right),
        (right_name, right),
        (left_name, left),
        (left_name, left),
        (right_name, right),
    )
    for index, (name, package) in enumerate(order, 1):
        sample = _stage_sample(seed, output, package, budget, f"{stage}-{index:02d}-{name}")
        pair = (index + 1) // 2
        sample = _save_stage_sample(report, output, stage, sample, name, pair)
        rows[name].append(sample)
    scored = bench.comparison(rows[left_name], rows[right_name], "perf")
    stage_report = report["stages"][stage]
    stage_report.update(
        status=scored["status"],
        comparison=scored,
        left=left_name,
        right=right_name,
    )
    bench._save(output / "report.json", report)
    return scored


def _integrity_check(packages, provenance, seed, seed_hash, budget):
    budget.admit(1)
    try:
        for name, package in packages.items():
            if _hash_package(package, budget) != provenance[name]["source_hashes"]:
                raise CampaignIntegrityError(f"frozen package changed during campaign: {name}")
            original = Path(provenance[name]["path"])
            if _hash_package(original, budget) != provenance[name]["source_hashes"]:
                raise CampaignIntegrityError(f"input package changed during campaign: {name}")
        if _digest_bounded(seed / "catalogue.duckdb", budget) != seed_hash:
            raise CampaignIntegrityError("synthetic seed changed during campaign")
    except TimeoutError:
        raise
    except CampaignIntegrityError:
        raise
    except Exception as exc:
        raise CampaignIntegrityError("campaign input integrity check failed") from exc
    budget.admit(0.001)


def _failure_retention(binding_qualified, packages, provenance, seed, seed_hash, harness, budget):
    if not binding_qualified:
        return []
    try:
        _integrity_check(packages, provenance, seed, seed_hash, budget)
        _assert_campaign_provenance(harness, budget)
    except Exception:
        return []
    return ["binding"]


def _record_failure_retention(
    report, binding_qualified, packages, provenance, seed, seed_hash, harness, budget
):
    report["retained"] = []
    budget.holdback_s = 0.0
    try:
        report["retained"] = _failure_retention(
            binding_qualified, packages, provenance, seed, seed_hash, harness, budget
        )
    except (KeyboardInterrupt, bench.Terminated) as stop:
        report["status"] = "partial"
        report["error"] = "interrupted while verifying fallback integrity"
        report["interrupted_exit_code"] = (
            128 + stop.signum if isinstance(stop, bench.Terminated) else 130
        )


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--manifest", type=Path, required=True)
    result.add_argument("--output", type=Path, required=True, help="fresh evidence directory")
    return result


def run(args, *, budget=None) -> int:
    budget = budget or bench.Budget(limit_s=CAMPAIGN_BUDGET_S)
    if budget.limit_s != CAMPAIGN_BUDGET_S:
        raise ValueError("campaign requires the shared 1,800-second budget")
    output = bench.safe_path(args.output)
    manifest_path = bench.safe_path(args.manifest)
    if output.exists():
        raise ValueError("output must be a fresh directory")
    budget.admit(1)
    manifest = _load_manifest(manifest_path)
    paths = _preflight_paths(manifest, manifest_path, output)
    report = {
        "kind": "synthetic-optimization-campaign",
        "measurement_version": bench.MEASUREMENT_VERSION,
        "manifest_version": manifest["_version"],
        "status": "failed",
        "profile": "perf",
        "started_at": bench.datetime.now(bench.UTC).isoformat(),
        "budget_seconds": CAMPAIGN_BUDGET_S,
        "shutdown_reserve_seconds": bench.SHUTDOWN_RESERVE_S,
        "warmup_mode": "one unprofiled fresh-worker run per distinct runtime",
        "samples": [],
        "stages": {},
        "deferred": {},
        "retained": [],
    }
    output.mkdir(parents=True)
    bench._save(output / "report.json", report)
    packages = {}
    provenance = {}
    seed = output / "seed"
    seed_hash = None
    harness = None
    binding_qualified = False
    try:
        harness = _campaign_provenance(budget)
        report["harness_provenance"] = harness
        bench._save(output / "report.json", report)
        resolved = _preflight(paths, budget)
        frozen = _freeze_packages(resolved, output, budget)
        packages, provenance = frozen["packages"], frozen["provenance"]
        report["inputs"] = provenance
        for name in VARIANTS:
            if name in provenance and not provenance[name].get("eligible"):
                report["deferred"][name] = provenance[name]["deferred_reason"]
        bench._save(output / "report.json", report)

        prepare = bench.supervised_prepare(
            bench.prepare_argv(output, "perf"),
            output,
            budget,
            output / "prepare.log",
            package_root=packages["baseline"],
        )
        if prepare.get("status") != "prepared":
            raise ValueError(prepare.get("message", "baseline seed preparation failed"))
        if prepare.get("source_hashes") != provenance["baseline"]["source_hashes"]:
            raise ValueError("seed worker did not use the frozen baseline runtime")
        distribution = prepare["distribution"]
        expected = bench.build_dataset(*bench.PROFILES["perf"])[2]
        if distribution != expected:
            raise ValueError("synthetic seed distribution differs from the fixed profile")
        seed_hash = prepare["seed_sha256"]
        report.update(distribution=distribution, seed_sha256=seed_hash)
        bench._save(output / "report.json", report)

        warmed = set()
        if manifest["_version"] == 2:
            _run_v2(
                seed,
                output,
                budget,
                warmed,
                report,
                packages,
                provenance,
                seed_hash,
                harness,
            )
            budget.holdback_s = 0.0  # spend the protected final-integrity window
            _integrity_check(packages, provenance, seed, seed_hash, budget)
            _assert_campaign_provenance(harness, budget)
            return (
                0
                if report["status"]
                in {
                    "qualified",
                    "qualified_individual_combination_deferred",
                    "deferred",
                    "insufficient_evidence",
                }
                else 1
            )

        primary_started = time.monotonic()
        binding_result = _compare_stage(
            "binding_vs_baseline",
            "baseline",
            packages["baseline"],
            "binding",
            packages["binding"],
            seed,
            output,
            budget,
            warmed,
            report,
        )
        _assert_campaign_provenance(harness, budget)
        primary_duration = time.monotonic() - primary_started
        report["primary_comparison_seconds"] = primary_duration
        if binding_result["status"] != "qualified":
            report["deferred"]["binding"] = binding_result.get("reason", binding_result["status"])
            report["status"] = binding_result["status"]
            bench._save(output / "report.json", report)
            _integrity_check(packages, provenance, seed, seed_hash, budget)
            return 1 if binding_result["status"] == "invalid" else 0

        # Establish source and seed immutability before treating binding as a
        # safe fallback. Later optional stages may time out or be interrupted.
        _integrity_check(packages, provenance, seed, seed_hash, budget)
        binding_qualified = True
        report["retained"] = ["binding"]
        estimated_stage = _optional_stage_estimate(
            report["stages"]["binding_vs_baseline"]["samples"]
        )
        final_reserve = primary_duration * 1.25
        budget.holdback_s = final_reserve
        for name in VARIANTS:
            if name not in packages or not provenance[name].get("eligible"):
                continue
            if budget.remaining < estimated_stage:
                report["deferred"][name] = (
                    "insufficient_evidence: final comparison reserve protected"
                )
                bench._save(output / "report.json", report)
                continue
            result = _compare_stage(
                f"{name}_vs_binding",
                "binding",
                packages["binding"],
                name,
                packages[name],
                seed,
                output,
                budget,
                warmed,
                report,
            )
            _assert_campaign_provenance(harness, budget)
            if result["status"] == "qualified":
                report["retained"].append(name)
            else:
                report["deferred"][name] = result.get("reason", result["status"])
            bench._save(output / "report.json", report)

        retained_components = [name for name in report["retained"] if name in VARIANTS]
        if not retained_components:
            report["final_combination"] = {
                "status": "reused_binding_evidence",
                "source_hash": bench.canonical_hash(_hash_package(packages["binding"], budget)),
                "stage": "binding_vs_baseline",
            }
            report["status"] = "qualified"
        else:
            budget.holdback_s = 0.0
            combined = output / "final" / "oddsfox_catalogue"
            _copy_package(packages["binding"], combined, budget)
            for name in retained_components:
                source = packages[name] / VARIANTS[name]
                target = combined / VARIANTS[name]
                _copy_file_bounded(source, target, budget)
            assembled_hashes = _hash_package(combined, budget)
            final_hash = bench.canonical_hash(assembled_hashes)
            report["final_combination_source_hashes"] = assembled_hashes
            budget.admit(final_reserve)
            result = _compare_stage(
                "final_combination_vs_baseline",
                "baseline",
                packages["baseline"],
                "final_combination",
                combined,
                seed,
                output,
                budget,
                warmed,
                report,
            )
            _assert_campaign_provenance(harness, budget)
            if _hash_package(combined, budget) != assembled_hashes:
                raise CampaignIntegrityError(
                    "final runtime package changed during its measured stage"
                )
            report["final_combination"] = {
                "status": result["status"],
                "comparison": result,
                "source_hash": final_hash,
                "measured_source_hash": bench.canonical_hash(assembled_hashes),
            }
            if result["status"] == "qualified":
                report["retained"] = ["binding", *retained_components, "combined"]
                report["status"] = "qualified"
            else:
                report["retained"] = ["binding"]
                report["status"] = "qualified_binding_combination_deferred"
                report["deferred"]["combination"] = result.get("reason", result["status"])
        _integrity_check(packages, provenance, seed, seed_hash, budget)
    except (TimeoutError, subprocess.TimeoutExpired) as exc:
        report["status"] = "insufficient_evidence"
        report["error"] = str(exc)
        if report.get("manifest_version") == 2:
            _retain_best_v2_candidate(
                report, packages, provenance, seed, seed_hash, harness, budget
            )
            if report["retained"]:
                report["status"] = "qualified_individual_combination_deferred"
        else:
            _record_failure_retention(
                report, binding_qualified, packages, provenance, seed, seed_hash, harness, budget
            )
    except (KeyboardInterrupt, bench.Terminated) as exc:
        report["status"] = "partial"
        report["error"] = "interrupted; retained evidence"
        report["interrupted_exit_code"] = (
            128 + exc.signum if isinstance(exc, bench.Terminated) else 130
        )
        if report.get("manifest_version") == 2:
            _retain_best_v2_candidate(
                report, packages, provenance, seed, seed_hash, harness, budget
            )
        else:
            _record_failure_retention(
                report, binding_qualified, packages, provenance, seed, seed_hash, harness, budget
            )
    except CampaignIntegrityError as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        report["retained"] = []
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        if report.get("manifest_version") == 2:
            _retain_best_v2_candidate(
                report, packages, provenance, seed, seed_hash, harness, budget
            )
        else:
            _record_failure_retention(
                report, binding_qualified, packages, provenance, seed, seed_hash, harness, budget
            )
    finally:
        report["elapsed_seconds"] = budget.elapsed
        bench._save(output / "report.json", report)
    return report.get(
        "interrupted_exit_code",
        0
        if report["status"]
        in {
            "qualified",
            "qualified_individual_combination_deferred",
            "qualified_binding_combination_deferred",
            "deferred",
            "insufficient_evidence",
        }
        else 1,
    )


def main() -> int:
    previous = {
        signum: signal.signal(signum, lambda signum, _frame: bench.SIGNALS.receive(signum))
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    }
    try:
        return run(parser().parse_args())
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
