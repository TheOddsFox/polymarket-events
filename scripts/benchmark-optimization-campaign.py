#!/usr/bin/env python3
"""Compare frozen catalogue runtime variants using one synthetic full-pass budget.

Version 1 manifests retain the binding-first workflow. Version 2 compares the
outcome-grouping and queue candidates independently against one common baseline.
Version 3 evaluates claim, queue and certified-acquisition candidates against a
common baseline with workload-specific evidence. Version 4 evaluates two
independent store candidates and an explicitly supplied combined runtime.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import platform
import re
import signal
import stat
import subprocess
import sys
import time
from pathlib import Path, PurePosixPath

HARNESS_PATH = Path(__file__).with_name("benchmark-full-pass.py")
SPEC = importlib.util.spec_from_file_location("optimization_full_pass", HARNESS_PATH)
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)

CAMPAIGN_BUDGET_S = 1800.0
V3_CAMPAIGN_BUDGET_S = 10800.0
V4_CAMPAIGN_BUDGET_S = 10800.0
VARIANTS = {
    "outcome_grouping": "current_store.py",
    "queue": "current_reconcile.py",
    "projection": "current_sync.py",
}
MANIFEST_KEYS = {"baseline", "binding", *VARIANTS}
V2_CANDIDATES = {"outcome_grouping", "queue"}
V3_CANDIDATES = ("claims", "queue", "closed_any", "keyset")
V3_OWNERSHIP = {
    "claims": {
        "current_store.py",
        "current_config.py",
        "current_runtime.py",
        "cli.py",
        "current_claim_indexes.py",
    },
    "queue": {"current_reconcile.py"},
    "closed_any": {
        "current_sync.py",
        "current_runtime.py",
        "cli.py",
    },
    "keyset": {
        "current_sync.py",
        "current_runtime.py",
        "cli.py",
        "current_reconcile.py",
        "current_keyset.py",
    },
}
V3_PRIORITY = V3_CANDIDATES
V4_CANDIDATES = ("outcome_grouping", "claim_reverse_lookup")
V4_OWNERSHIP = {name: "current_store.py" for name in (*V4_CANDIDATES, "combined")}
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
    if type(data.get("version")) is not int or data["version"] not in {2, 3, 4}:
        raise ValueError("manifest version must be integer 2, 3 or 4")
    if data["version"] == 4:
        if set(data) != {"version", "baseline", "candidates", "combined"}:
            raise ValueError(
                "version 4 manifest requires only version, baseline, candidates and combined"
            )
        candidates = data["candidates"]
        if not isinstance(candidates, dict) or set(candidates) != set(V4_CANDIDATES):
            raise ValueError(
                "version 4 manifest requires outcome_grouping and claim_reverse_lookup"
            )
        return {
            "_version": 4,
            "baseline": data["baseline"],
            **candidates,
            "combined": data["combined"],
        }
    if data["version"] == 3:
        if set(data) - {"version", "baseline", "candidates", "combinations", "claims_mode"}:
            raise ValueError("version 3 manifest contains unknown fields")
        claims_mode = data.get("claims_mode", "indexed")
        if type(claims_mode) is not str or claims_mode not in {"lookup_only", "indexed"}:
            raise ValueError("version 3 claims_mode must be 'lookup_only' or 'indexed'")
        candidates = data.get("candidates")
        if not isinstance(candidates, dict) or set(candidates) != set(V3_CANDIDATES):
            raise ValueError("version 3 manifest requires claims, queue, closed_any and keyset")
        combinations = data.get("combinations", {})
        if not isinstance(combinations, dict):
            raise ValueError("version 3 combinations must be an object")
        normalized = {
            "_version": 3,
            "_claims_mode": claims_mode,
            "baseline": data.get("baseline"),
        }
        normalized.update(candidates)
        for feature_key, entry in combinations.items():
            features = feature_key.split("+") if isinstance(feature_key, str) else []
            if (
                len(features) < 2
                or features != sorted(set(features))
                or not set(features).issubset(V3_CANDIDATES)
                or feature_key != "+".join(features)
            ):
                raise ValueError("combination keys must be sorted feature names joined by '+'")
            normalized[f"_combination:{feature_key}"] = entry
        return normalized
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
            or "\\" in name
            or name.startswith("/")
            or any(part in {"", ".", ".."} for part in name.split("/"))
            or ":" in name
            or PurePosixPath(name).is_absolute()
            or PurePosixPath(name).suffix != ".py"
            or not isinstance(value, str)
            or not HEX_SHA256.fullmatch(value)
        ):
            raise ValueError("manifest source hash inventory is malformed")
    if len({name.casefold() for name in expected}) != len(expected):
        raise ValueError("manifest source hash inventory has case-colliding paths")
    return package, expected


def _validate_package_tree(package: Path, budget=None) -> list[Path]:
    package = bench.safe_path(package)
    if not package.is_dir():
        raise ValueError("runtime package must be a regular directory")
    paths = []
    portable_names: set[str] = set()
    for path in sorted(package.rglob("*")):
        if budget:
            budget.admit(0.001)
        if bench.is_link_or_reparse(path):
            raise ValueError("runtime package contains a link or reparse point")
        mode = path.lstat().st_mode
        if not (stat.S_ISDIR(mode) or stat.S_ISREG(mode)):
            raise ValueError("runtime package contains a symlink or special file")
        if stat.S_ISREG(mode):
            if path.suffix != ".py":
                raise ValueError("runtime package contains non-source files")
            portable_name = path.relative_to(package).as_posix()
            folded_name = portable_name.casefold()
            if folded_name in portable_names:
                raise ValueError("runtime package contains case-colliding source paths")
            portable_names.add(folded_name)
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
        result[path.relative_to(package).as_posix()] = digest.hexdigest()
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
        if name not in {"_version", "_claims_mode"}
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


def _validate_v3_ownership(baseline_hashes, candidate_hashes, allowed, label):
    removed = set(baseline_hashes) - set(candidate_hashes)
    added = set(candidate_hashes) - set(baseline_hashes)
    changed = {
        name
        for name in set(baseline_hashes) & set(candidate_hashes)
        if baseline_hashes[name] != candidate_hashes[name]
    }
    if removed:
        raise ValueError(f"{label} removes source files from the baseline")
    changed.update(added)
    if changed - allowed:
        raise ValueError(f"{label} changes files outside its ownership allowlist")
    return bool(changed)


def _changed_v3_files(baseline_hashes, candidate_hashes):
    return {
        name
        for name in set(baseline_hashes) | set(candidate_hashes)
        if baseline_hashes.get(name) != candidate_hashes.get(name)
    }


def _v3_allowed_ownership(feature, claims_mode):
    if feature == "claims" and claims_mode == "lookup_only":
        return {"current_store.py"}
    return V3_OWNERSHIP[feature]


def _freeze_v3_packages(resolved, output, budget, claims_mode="indexed"):
    frozen, provenance = {}, {}
    for name, (source, expected_hashes) in resolved.items():
        budget.admit(1)
        target_name = name.removeprefix("_combination:")
        target = output / "inputs" / target_name / "oddsfox_catalogue"
        target.parent.mkdir(parents=True, exist_ok=True)
        _copy_tree_bounded(source, target, budget)
        actual = _hash_package(target, budget)
        if actual != expected_hashes:
            raise ValueError(f"package changed while it was being frozen: {name}")
        frozen[name] = target
        provenance[name] = {"path": str(source), "source_hashes": actual}

    baseline_hashes = _hash_package(frozen["baseline"], budget)
    candidate_changes = {}
    for name in V3_CANDIDATES:
        if name not in frozen:
            continue
        candidate_hashes = _hash_package(frozen[name], budget)
        candidate_changes[name] = _changed_v3_files(baseline_hashes, candidate_hashes)
        provenance[name]["eligible"] = _validate_v3_ownership(
            baseline_hashes,
            candidate_hashes,
            _v3_allowed_ownership(name, claims_mode),
            name,
        )
        if not provenance[name]["eligible"]:
            provenance[name]["deferred_reason"] = "candidate is identical to baseline"
    for name in frozen:
        if not name.startswith("_combination:"):
            continue
        feature_key = name.partition(":")[2]
        features = feature_key.split("+")
        allowed = set().union(
            *(_v3_allowed_ownership(feature, claims_mode) for feature in features)
        )
        provenance[name]["features"] = features
        combination_hashes = _hash_package(frozen[name], budget)
        combination_changes = _changed_v3_files(baseline_hashes, combination_hashes)
        missing_features = [
            feature
            for feature in features
            if not (candidate_changes.get(feature, set()) & combination_changes)
        ]
        if missing_features:
            raise ValueError(
                f"{name} contains no changed files for declared features: "
                + ", ".join(missing_features)
            )
        provenance[name]["eligible"] = _validate_v3_ownership(
            baseline_hashes, combination_hashes, allowed, name
        )
        if not provenance[name]["eligible"]:
            provenance[name]["deferred_reason"] = "combination is identical to baseline"
    return {"packages": frozen, "provenance": provenance}


def _freeze_v4_packages(resolved, output, budget):
    """Freeze V4 inputs and enforce the shared-store-file ownership boundary.

    V4 deliberately accepts a preassembled combined package. Since both
    candidates own the same source file, composing it by copying candidate
    files would silently discard one implementation.
    """
    frozen, provenance = {}, {}
    for name, (source, expected_hashes) in resolved.items():
        budget.admit(1)
        target = output / "inputs" / name / "oddsfox_catalogue"
        target.parent.mkdir(parents=True, exist_ok=True)
        _copy_tree_bounded(source, target, budget)
        actual = _hash_package(target, budget)
        if actual != expected_hashes:
            raise ValueError(f"package changed while it was being frozen: {name}")
        frozen[name] = target
        provenance[name] = {"path": str(source), "source_hashes": actual}

    baseline_hashes = _hash_package(frozen["baseline"], budget)
    for name in (*V4_CANDIDATES, "combined"):
        candidate_hashes = _hash_package(frozen[name], budget)
        eligible = _validate_variant_ownership(
            baseline_hashes, candidate_hashes, V4_OWNERSHIP[name]
        )
        provenance[name]["eligible"] = eligible
        provenance[name]["changed_files"] = sorted(
            source
            for source in baseline_hashes
            if baseline_hashes[source] != candidate_hashes[source]
        )
        if not eligible:
            provenance[name]["deferred_reason"] = "package is identical to baseline"
    return {"packages": frozen, "provenance": provenance}


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
        Path(bench.REPO) / "src" / "oddsfox_catalogue" / "platform_support.py",
        Path(bench.REPO) / "src" / "oddsfox_catalogue" / "_windows.py",
        Path(bench.REPO) / "src" / "oddsfox_catalogue" / "_process_gate.py",
        Path(bench.REPO) / "scripts" / "verify-installed-workflow.py",
    ]
    return {
        "git_revision": revision,
        "source_hashes": {
            path.relative_to(bench.REPO).as_posix(): _digest_bounded(path, budget) for path in paths
        },
    }


def _machine_provenance(output: Path) -> dict:
    """Record local runtime and output-volume facts without probing USB devices."""
    import duckdb

    result = {
        "platform": platform.platform(),
        "python": sys.version.split()[0],
        "duckdb": duckdb.__version__,
        "filesystem": None,
        "usb_storage_link": "not exposed by campaign host API",
    }
    if hasattr(os, "statvfs"):
        try:
            statvfs = os.statvfs(output)
            result["filesystem"] = {
                "block_size": statvfs.f_bsize,
                "available_bytes": statvfs.f_bavail * statvfs.f_frsize,
                "device_id": os.stat(output).st_dev,
            }
        except OSError as exc:
            result["filesystem_error"] = type(exc).__name__
    else:
        result["filesystem_error"] = "unsupported_on_this_platform"
    return result


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


def _stage_sample(seed, output, package, budget, label, warmup=False, workload=None):
    kwargs = {"warmup": warmup}
    if workload is not None:
        kwargs["workload"] = workload
    return bench.run_sample(seed, output, package, budget, "perf", label, **kwargs)


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


def _warm(runtime_name, package, seed, output, budget, warmed, report, workload=None):
    source_hash = bench.canonical_hash(_hash_package(package, budget))
    key = (source_hash, workload)
    warmup_key = runtime_name if workload is None else f"{runtime_name}:{workload}"
    if key in warmed:
        if warmup_key not in report.get("warmups", {}):
            existing = next(
                (
                    row
                    for row in report.get("warmups", {}).values()
                    if row.get("source_hash") == source_hash
                    and row.get("workload", row["sample"].get("workload")) == workload
                ),
                None,
            )
            if existing is not None:
                report["warmups"][warmup_key] = existing
                return
        else:
            return
        # A caller may reuse the warmed set with a fresh report. Without the
        # sample in this report, rerun rather than create an incomplete alias.
        warmed.discard(key)
    warm_dir = output / "warmups" / source_hash[:16]
    if workload is not None:
        warm_dir /= workload
    warm_dir.mkdir(parents=True)
    sample = _stage_sample(
        seed, warm_dir, package, budget, f"warm-{source_hash[:10]}", workload=workload
    )
    warmed.add(key)
    report.setdefault("warmups", {})[warmup_key] = {
        "source_hash": source_hash,
        "workload": workload,
        "sample": sample,
    }
    bench._save(output / "report.json", report)


def _compare_stage(
    stage,
    left_name,
    left,
    right_name,
    right,
    seed,
    output,
    budget,
    warmed,
    report,
    *,
    workload=None,
    comparison=None,
):
    _warm(left_name, left, seed, output, budget, warmed, report, workload)
    _warm(right_name, right, seed, output, budget, warmed, report, workload)
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
        sample = _stage_sample(
            seed, output, package, budget, f"{stage}-{index:02d}-{name}", workload=workload
        )
        pair = (index + 1) // 2
        sample = _save_stage_sample(report, output, stage, sample, name, pair)
        rows[name].append(sample)
    scored = (
        comparison(rows[left_name], rows[right_name])
        if comparison
        else bench.comparison(rows[left_name], rows[right_name], "perf")
    )
    stage_report = report["stages"][stage]
    stage_report.update(
        status=scored["status"],
        comparison=scored,
        left=left_name,
        right=right_name,
        workload=workload,
    )
    bench._save(output / "report.json", report)
    return scored


def _v3_workloads(candidate):
    if candidate in {"claims", "queue"}:
        return (None,)
    if candidate == "closed_any":
        return ("closed_any",)
    return ("discovery_sparse", "discovery_dense", "discovery_tail")


def _best_v3_candidate(report):
    qualified = report.get("qualified_candidates", {})
    return next((name for name in V3_PRIORITY if name in qualified), None)


def _best_v4_candidate(report):
    qualified = report.get("qualified_candidates", {})
    return max(
        (name for name in V4_CANDIDATES if name in qualified),
        key=lambda name: (
            qualified[name]["comparison"].get("saved_s", 0.0),
            name == "outcome_grouping",
        ),
        default=None,
    )


def _v4_comparison_complete(report, candidate):
    stage = report.get("stages", {}).get(f"{candidate}_vs_baseline")
    if not isinstance(stage, dict) or not isinstance(stage.get("comparison"), dict):
        return False
    if stage["comparison"].get("status") not in {"qualified", "deferred", "invalid"}:
        return False
    expected = {(side, pair) for side in ("baseline", candidate) for pair in (1, 2, 3)}
    observed = {key: 0 for key in expected}
    for sample in stage.get("samples", []):
        key = (sample.get("category"), sample.get("pair"))
        if key not in observed:
            return False
        observed[key] += 1
    return all(count == 1 for count in observed.values())


def _run_v4(seed, output, budget, warmed, report, packages, provenance, harness):
    """Compare V4 store candidates independently, then the supplied combination."""
    report["retained"] = []
    report["qualified_candidates"] = {}
    report["invalid_candidates"] = []
    budget.holdback_s = 15.0  # final integrity; Budget reserves shutdown separately
    baseline = packages["baseline"]
    _warm("baseline", baseline, seed, output, budget, warmed, report)
    baseline_warm = report["warmups"]["baseline"]["sample"]
    first_qualified_duration = None
    for name in V4_CANDIDATES:
        if not provenance[name].get("eligible"):
            continue
        if first_qualified_duration is not None:
            budget.holdback_s = 15.0 + 1.25 * first_qualified_duration
        started = time.monotonic()
        _warm(name, packages[name], seed, output, budget, warmed, report)
        candidate_warm = report["warmups"][name]["sample"]
        estimate = _candidate_stage_estimate(baseline_warm, candidate_warm)
        report.setdefault("admission", {})[name] = {
            "projected_scored_seconds": estimate,
            "headroom_fraction": 0.25,
            "integrity_reserve_seconds": 15.0,
            "shutdown_reserve_seconds": bench.SHUTDOWN_RESERVE_S,
            "combination_reserve_seconds": (
                1.25 * first_qualified_duration if first_qualified_duration is not None else 0.0
            ),
        }
        if estimate > budget.remaining:
            report.setdefault("deferred", {})[name] = (
                "insufficient_evidence: complete paired comparison does not fit shared budget"
            )
            bench._save(output / "report.json", report)
            continue
        stage_name = f"{name}_vs_baseline"
        try:
            scored = _compare_stage(
                stage_name,
                "baseline",
                baseline,
                name,
                packages[name],
                seed,
                output,
                budget,
                warmed,
                report,
            )
        except (TimeoutError, subprocess.TimeoutExpired) as exc:
            report.setdefault("deferred", {})[name] = (
                f"insufficient_evidence: paired comparison incomplete ({exc})"
            )
            report["stages"].setdefault(stage_name, {"samples": []})["status"] = "incomplete"
            bench._save(output / "report.json", report)
            break
        _assert_campaign_provenance(harness, budget)
        comparison_duration = time.monotonic() - started
        stage = report["stages"][stage_name]
        stage["comparison_duration_seconds"] = comparison_duration
        stage["projected_scored_seconds"] = estimate
        if scored.get("status") == "qualified":
            report["qualified_candidates"][name] = {
                "comparison": scored,
                "stage": stage_name,
            }
            if first_qualified_duration is None:
                first_qualified_duration = comparison_duration
        elif scored.get("status") == "invalid":
            report["invalid_candidates"].append(name)
            report.setdefault("deferred", {})[name] = scored.get("reason", "invalid comparison")
        else:
            report.setdefault("deferred", {})[name] = scored.get(
                "reason", scored.get("status", "incomplete")
            )
        bench._save(output / "report.json", report)

    qualified = list(report["qualified_candidates"])
    invalid = report["invalid_candidates"]
    if not qualified:
        eligible = [name for name in V4_CANDIDATES if provenance[name].get("eligible")]
        if invalid:
            report["status"] = "invalid"
        elif eligible and all(_v4_comparison_complete(report, name) for name in eligible):
            report["status"] = "deferred"
        else:
            report["status"] = "insufficient_evidence" if eligible else "deferred"
        _run_v4_diagnostics(output, budget, report, packages)
        return
    if len(qualified) == 1:
        chosen = qualified[0]
        report["retained"] = [chosen]
        report["status"] = "invalid_with_qualified_fallback" if invalid else "qualified"
        report["final_combination"] = {
            "status": "reused_independent_evidence",
            "features": [chosen],
            "stage": report["qualified_candidates"][chosen]["stage"],
        }
        _run_v4_diagnostics(output, budget, report, packages)
        return

    combination = packages["combined"]
    if not provenance["combined"].get("eligible"):
        chosen = _best_v4_candidate(report)
        report["retained"] = [chosen] if chosen else []
        report["status"] = (
            "invalid_with_qualified_fallback"
            if invalid
            else "qualified_individual_combination_deferred"
        )
        report.setdefault("deferred", {})["combined"] = provenance["combined"].get(
            "deferred_reason", "combined package is not eligible"
        )
        _run_v4_diagnostics(output, budget, report, packages)
        return
    # Spend the protected combination window after the second candidate.
    # Keep only final integrity time in the holdback.
    budget.holdback_s = 15.0
    try:
        combined_hashes = _hash_package(combination, budget)
        report["final_combination_source_hashes"] = combined_hashes
        _warm("combined", combination, seed, output, budget, warmed, report)
        combination_warm = report["warmups"]["combined"]["sample"]
        estimate = _candidate_stage_estimate(baseline_warm, combination_warm)
        report["admission"]["combined"] = {
            "projected_scored_seconds": estimate,
            "headroom_fraction": 0.25,
            "integrity_reserve_seconds": 15.0,
            "shutdown_reserve_seconds": bench.SHUTDOWN_RESERVE_S,
        }
        if estimate > budget.remaining:
            raise TimeoutError("combined comparison does not fit shared budget")
        scored = _compare_stage(
            "combined_vs_baseline",
            "baseline",
            baseline,
            "combined",
            combination,
            seed,
            output,
            budget,
            warmed,
            report,
        )
        _assert_campaign_provenance(harness, budget)
        if _hash_package(combination, budget) != combined_hashes:
            raise CampaignIntegrityError("combined runtime changed during its measured stage")
    except (TimeoutError, subprocess.TimeoutExpired) as exc:
        chosen = _best_v4_candidate(report)
        report["retained"] = [chosen] if chosen else []
        report["status"] = (
            "invalid_with_qualified_fallback"
            if invalid
            else "qualified_individual_combination_deferred"
        )
        report.setdefault("deferred", {})["combined"] = (
            f"insufficient_evidence: combined comparison incomplete ({exc})"
        )
        report["final_combination"] = {"status": "insufficient_evidence", "reason": str(exc)}
        _run_v4_diagnostics(output, budget, report, packages)
        return
    report["final_combination"] = {"status": scored.get("status"), "comparison": scored}
    if scored.get("status") == "qualified":
        report["retained"] = [*qualified, "combined"]
        report["status"] = "invalid_with_qualified_fallback" if invalid else "qualified"
    else:
        if scored.get("status") == "invalid":
            report["invalid_candidates"].append("combined")
        chosen = _best_v4_candidate(report)
        report["retained"] = [chosen] if chosen else []
        report["status"] = (
            "invalid_with_qualified_fallback"
            if invalid
            else "qualified_individual_combination_deferred"
        )
        report.setdefault("deferred", {})["combined"] = scored.get(
            "reason", scored.get("status", "combined comparison incomplete")
        )
    _run_v4_diagnostics(output, budget, report, packages)


def _run_v4_diagnostics(output, budget, report, packages):
    """Run optional diagnostics without spending final-integrity/shutdown reserves."""
    # Keep an additional 15 seconds for the supervised worker's own shutdown;
    # Budget already reserves another 15 seconds for campaign shutdown.
    budget.holdback_s = 30.0
    helper = getattr(bench, "claim_resource_diagnostics", None)
    if helper is None:
        report["claim_resource_diagnostics"] = {
            "status": "unavailable",
            "reason": "extended claim diagnostics are not available in this harness",
        }
        budget.holdback_s = 15.0
        return
    diagnostics_dir = output / "claim-resource-diagnostics"
    diagnostics_dir.mkdir(parents=True, exist_ok=True)
    try:
        budget.admit(1.0)
        result = helper(packages["claim_reverse_lookup"], diagnostics_dir, budget, extended=True)
    except (KeyboardInterrupt, bench.Terminated):
        raise
    except (TimeoutError, subprocess.TimeoutExpired) as exc:
        result = {"status": "insufficient_evidence", "reason": str(exc)}
    except Exception as exc:
        result = {"status": "failed", "reason": type(exc).__name__}
    finally:
        # Leave the final 15-second campaign integrity reserve in place for
        # run() to spend; the global Budget separately preserves shutdown.
        budget.holdback_s = 15.0
    report["claim_resource_diagnostics"] = result
    bench._save(output / "report.json", report)


def _api_compare(baseline, candidate, workload):
    try:
        return bench.api_comparison(baseline, candidate, workload)
    except ValueError as exc:
        return {"status": "invalid", "reason": str(exc)}


def _verify_api_seeds(api_seeds, output, budget):
    for workload, metadata in api_seeds.items():
        root = output / "api-seeds" / workload
        for component in ("provider", "workspace"):
            expected = metadata.get(f"{component}_seed_sha256")
            actual = _digest_bounded(root / component / "catalogue.duckdb", budget)
            if actual != expected:
                raise CampaignIntegrityError(
                    f"API workload {component} seed changed during campaign: {workload}"
                )
        if _digest_bounded(root / "api-seed.json", budget) != metadata.get("_manifest_sha256"):
            raise CampaignIntegrityError(
                f"API workload manifest changed during campaign: {workload}"
            )


def _run_v3(
    seed,
    output,
    budget,
    warmed,
    report,
    packages,
    provenance,
    seed_hash,
    harness,
    claims_mode="indexed",
):
    report["retained"] = []
    report["qualified_candidates"] = {}
    report.setdefault("api_seed_errors", {})
    budget.holdback_s = 15.0
    api_seeds = {}
    api_harness_available = hasattr(bench, "API_WORKLOADS") and hasattr(bench, "prepare_api_seed")

    def ensure_api_seed(workload):
        if not api_harness_available:
            return False
        if workload in api_seeds:
            return True
        root = output / "api-seeds" / workload
        try:
            metadata = bench.prepare_api_seed(root, workload, budget, packages["baseline"])
        except (TimeoutError, subprocess.TimeoutExpired, CampaignIntegrityError):
            raise
        except Exception as exc:
            report["api_seed_errors"][workload] = type(exc).__name__
            bench._save(output / "report.json", report)
            return False
        if metadata.get("status") != "prepared":
            raise ValueError(
                metadata.get("message", f"API workload preparation failed: {workload}")
            )
        expected_runtime_hashes = _hash_package(packages["baseline"], budget)
        if metadata.get("runtime_source_hashes") != expected_runtime_hashes:
            raise CampaignIntegrityError(f"API workload runtime provenance mismatch: {workload}")
        for component in ("provider", "workspace"):
            expected = metadata.get(f"{component}_seed_sha256")
            actual = _digest_bounded(root / component / "catalogue.duckdb", budget)
            if expected != actual:
                raise CampaignIntegrityError(
                    f"API workload {component} seed hash mismatch: {workload}"
                )
        metadata["_manifest_sha256"] = _digest_bounded(root / "api-seed.json", budget)
        api_seeds[workload] = metadata
        report["api_workloads"] = api_seeds
        bench._save(output / "report.json", report)
        return True

    first_comparison_duration = None
    for candidate in V3_PRIORITY:
        if candidate not in packages or not provenance[candidate].get("eligible"):
            continue
        if candidate == "claims" and claims_mode == "indexed":
            if hasattr(bench, "claim_resource_diagnostics"):
                diagnostics_dir = output / "claim-resource-diagnostics"
                diagnostics_dir.mkdir(parents=True, exist_ok=True)
                try:
                    diagnostics = bench.claim_resource_diagnostics(
                        packages[candidate], diagnostics_dir, budget
                    )
                except TimeoutError:
                    raise
                except Exception as exc:
                    diagnostics = {
                        "status": "failed",
                        "resource_gate": False,
                        "reason": f"claim resource diagnostics failed: {type(exc).__name__}",
                    }
                report["claim_resource_diagnostics"] = diagnostics
                if diagnostics.get("resource_gate") is not True:
                    report["deferred"][candidate] = diagnostics.get(
                        "reason", "insufficient_evidence: claim index resource gate failed"
                    )
                else:
                    report["deferred"][candidate] = (
                        "insufficient_evidence: indexed steady-state full-pass samples and "
                        "per-sample supervised index installation are unavailable"
                    )
            else:
                report["claim_resource_diagnostics"] = {
                    "status": "unavailable",
                    "resource_gate": False,
                    "reason": "claim-scale index installation diagnostics are unavailable",
                }
                report["deferred"][candidate] = (
                    "insufficient_evidence: claim-scale index installation diagnostics are unavailable"
                )
            bench._save(output / "report.json", report)
            continue
        if candidate == "claims":
            report.setdefault("candidate_descriptions", {})[candidate] = (
                "Selective lookup without indexes"
            )
        if first_comparison_duration is not None:
            budget.holdback_s = 15.0 + 1.25 * first_comparison_duration
        workloads = _v3_workloads(candidate)
        missing_workload = next(
            (
                workload
                for workload in workloads
                if workload is not None and not ensure_api_seed(workload)
            ),
            None,
        )
        if missing_workload is not None:
            error = report["api_seed_errors"].get(missing_workload)
            report["deferred"][candidate] = (
                f"insufficient_evidence: workload seed preparation failed ({error})"
                if error
                else "insufficient_evidence: workload harness unavailable"
            )
            continue

        estimates = []
        for workload in workloads:
            seed_for = seed if workload is None else output / "api-seeds" / workload
            _warm(
                "baseline", packages["baseline"], seed_for, output, budget, warmed, report, workload
            )
            _warm(
                candidate, packages[candidate], seed_for, output, budget, warmed, report, workload
            )
            left_key = "baseline" if workload is None else f"baseline:{workload}"
            right_key = candidate if workload is None else f"{candidate}:{workload}"
            left_wall = report["warmups"][left_key]["sample"]["wall_s"]
            right_wall = report["warmups"][right_key]["sample"]["wall_s"]
            estimates.append(1.25 * 3 * (left_wall + right_wall))
        estimate = sum(estimates)
        report.setdefault("admission", {})[candidate] = {
            "projected_scored_seconds": estimate,
            "headroom_fraction": 0.25,
            "integrity_reserve_seconds": 15.0,
            "shutdown_reserve_seconds": bench.SHUTDOWN_RESERVE_S,
        }
        if estimate > budget.remaining:
            report["deferred"][candidate] = (
                "insufficient_evidence: complete paired workload set does not fit shared budget"
            )
            bench._save(output / "report.json", report)
            continue

        comparison_started = time.monotonic()
        results = []
        for workload in workloads:
            seed_for = seed if workload is None else output / "api-seeds" / workload
            compare = (
                None
                if workload is None
                else lambda baseline, candidate_rows, w=workload: _api_compare(
                    baseline, candidate_rows, w
                )
            )
            result = _compare_stage(
                f"{candidate}_vs_baseline_{workload or 'full_pass'}",
                "baseline",
                packages["baseline"],
                candidate,
                packages[candidate],
                seed_for,
                output,
                budget,
                warmed,
                report,
                workload=workload,
                comparison=compare,
            )
            results.append(result)
            if result.get("status") != "qualified":
                break
        _assert_campaign_provenance(harness, budget)
        _verify_api_seeds(api_seeds, output, budget)
        comparison_duration = time.monotonic() - comparison_started
        report.setdefault("comparison_durations", {})[candidate] = comparison_duration
        if len(results) == len(workloads) and all(
            row.get("status") == "qualified" for row in results
        ):
            report["qualified_candidates"][candidate] = {
                "comparisons": results,
                "workloads": list(workloads),
            }
            if first_comparison_duration is None:
                first_comparison_duration = comparison_duration
        else:
            failed = next((row for row in results if row.get("status") != "qualified"), {})
            report["deferred"][candidate] = failed.get("reason", failed.get("status", "incomplete"))
        bench._save(output / "report.json", report)

    qualified = list(report["qualified_candidates"])
    if not qualified:
        report["status"] = "insufficient_evidence" if report["deferred"] else "deferred"
        return
    _verify_api_seeds(api_seeds, output, budget)
    if len(qualified) == 1:
        chosen = qualified[0]
        report["retained"] = [chosen]
        report["status"] = "qualified"
        report["final_combination"] = {
            "status": "reused_independent_evidence",
            "features": [chosen],
        }
        budget.holdback_s = 15.0
        return

    feature_key = "+".join(sorted(qualified))
    combination_key = f"_combination:{feature_key}"
    combination = packages.get(combination_key)
    if combination is None or not provenance[combination_key].get("eligible"):
        chosen = _best_v3_candidate(report)
        report["retained"] = [chosen]
        report["status"] = "qualified_individual_combination_deferred"
        report["deferred"]["combination"] = "exact qualified feature-set package is unavailable"
        report["final_combination"] = {"status": "deferred", "features": qualified}
        budget.holdback_s = 15.0
        return

    budget.holdback_s = 15.0
    workloads = []
    for candidate in qualified:
        for workload in _v3_workloads(candidate):
            if workload not in workloads:
                workloads.append(workload)
    for workload in workloads:
        if workload is not None and not ensure_api_seed(workload):
            chosen = _best_v3_candidate(report)
            report["retained"] = [chosen] if chosen else []
            report["status"] = (
                "qualified_individual_combination_deferred" if chosen else "insufficient_evidence"
            )
            report["deferred"]["combination"] = "required workload harness is unavailable"
            return
    combination_hashes = _hash_package(combination, budget)
    report["final_combination_source_hashes"] = combination_hashes
    estimate = 0.0
    for workload in workloads:
        seed_for = seed if workload is None else output / "api-seeds" / workload
        _warm("baseline", packages["baseline"], seed_for, output, budget, warmed, report, workload)
        _warm("final_combination", combination, seed_for, output, budget, warmed, report, workload)
        left_key = "baseline" if workload is None else f"baseline:{workload}"
        right_key = "final_combination" if workload is None else f"final_combination:{workload}"
        estimate += (
            1.25
            * 3
            * (
                report["warmups"][left_key]["sample"]["wall_s"]
                + report["warmups"][right_key]["sample"]["wall_s"]
            )
        )
    report["final_combination_projected_seconds"] = estimate
    if estimate > budget.remaining:
        chosen = _best_v3_candidate(report)
        report["retained"] = [chosen]
        report["status"] = "qualified_individual_combination_deferred"
        report["deferred"]["combination"] = (
            "insufficient_evidence: final comparison reserve unavailable"
        )
        return
    results = []
    for workload in workloads:
        seed_for = seed if workload is None else output / "api-seeds" / workload
        compare = (
            None
            if workload is None
            else lambda baseline, candidate_rows, w=workload: _api_compare(
                baseline, candidate_rows, w
            )
        )
        result = _compare_stage(
            f"final_combination_vs_baseline_{workload or 'full_pass'}",
            "baseline",
            packages["baseline"],
            "final_combination",
            combination,
            seed_for,
            output,
            budget,
            warmed,
            report,
            workload=workload,
            comparison=compare,
        )
        results.append(result)
        if result.get("status") != "qualified":
            break
    if len(results) == len(workloads) and all(row.get("status") == "qualified" for row in results):
        _verify_api_seeds(api_seeds, output, budget)
        if _hash_package(combination, budget) != combination_hashes:
            raise CampaignIntegrityError("combination package changed during its measured stage")
        report["retained"] = [*qualified, "combined"]
        report["status"] = "qualified"
        report["final_combination"] = {
            "status": "qualified",
            "features": qualified,
            "comparisons": results,
        }
    else:
        _verify_api_seeds(api_seeds, output, budget)
        if _hash_package(combination, budget) != combination_hashes:
            raise CampaignIntegrityError("combination package changed during its measured stage")
        chosen = _best_v3_candidate(report)
        failed = next((row for row in results if row.get("status") != "qualified"), {})
        report["retained"] = [chosen]
        report["status"] = "qualified_individual_combination_deferred"
        report["deferred"]["combination"] = failed.get(
            "reason", failed.get("status", "incomplete comparison")
        )
        report["final_combination"] = {
            "status": failed.get("status", "deferred"),
            "features": qualified,
            "comparisons": results,
        }


def _retain_v3_candidate(report, packages, provenance, seed, seed_hash, harness, budget):
    report["retained"] = []
    budget.holdback_s = 0.0
    for candidate in V3_PRIORITY:
        if candidate not in report.get("qualified_candidates", {}):
            continue
        try:
            _integrity_check(packages, provenance, seed, seed_hash, budget)
            _verify_api_seeds(report.get("api_workloads", {}), seed.parent, budget)
            _assert_campaign_provenance(harness, budget)
        except Exception:
            return
        report["retained"] = [candidate]
        return


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
    supplied_budget = budget is not None
    budget = budget or bench.Budget(limit_s=V3_CAMPAIGN_BUDGET_S)
    output = bench.safe_path(args.output)
    manifest_path = bench.safe_path(args.manifest)
    if output.exists():
        raise ValueError("output must be a fresh directory")
    budget.admit(1)
    manifest = _load_manifest(manifest_path)
    claims_mode = manifest.get("_claims_mode", "indexed")
    expected_budget = (
        V3_CAMPAIGN_BUDGET_S
        if manifest["_version"] == 3
        else V4_CAMPAIGN_BUDGET_S
        if manifest["_version"] == 4
        else CAMPAIGN_BUDGET_S
    )
    if supplied_budget and budget.limit_s != expected_budget:
        raise ValueError(
            f"version {manifest['_version']} campaign requires a {expected_budget:,.0f}-second budget"
        )
    if not supplied_budget:
        budget.limit_s = expected_budget
    paths = _preflight_paths(manifest, manifest_path, output)
    report = {
        "kind": "synthetic-optimization-campaign",
        "measurement_version": bench.MEASUREMENT_VERSION,
        "platform": bench.sys.platform,
        "manifest_version": manifest["_version"],
        "status": "failed",
        "profile": "perf",
        "started_at": bench.datetime.now(bench.UTC).isoformat(),
        "budget_seconds": expected_budget,
        "shutdown_reserve_seconds": bench.SHUTDOWN_RESERVE_S,
        "warmup_mode": "one unprofiled fresh-worker run per distinct runtime",
        "samples": [],
        "stages": {},
        "deferred": {},
        "retained": [],
    }
    if manifest["_version"] == 3:
        report["claims_mode"] = claims_mode
    if manifest["_version"] == 4:
        report["effective_settings"] = {
            "profile": "perf",
            "dataset": "10,000 events / 40,400 markets",
            "interruption_after_settled_units": 50,
            "candidate_order": list(V4_CANDIDATES),
            "scored_runs_per_runtime": 3,
            "pair_order": "AB, BA, AB",
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
        if manifest["_version"] == 4:
            report["machine_provenance"] = _machine_provenance(output.parent)
            bench._save(output / "report.json", report)
        harness = _campaign_provenance(budget)
        report["harness_provenance"] = harness
        bench._save(output / "report.json", report)
        resolved = _preflight(paths, budget)
        frozen = (
            _freeze_v3_packages(resolved, output, budget, claims_mode)
            if manifest["_version"] == 3
            else _freeze_v4_packages(resolved, output, budget)
            if manifest["_version"] == 4
            else _freeze_packages(resolved, output, budget)
        )
        packages, provenance = frozen["packages"], frozen["provenance"]
        if manifest["_version"] == 3:
            provenance["_claims_mode"] = {"value": claims_mode, "source": "manifest"}
        report["inputs"] = provenance
        report_candidates = (
            (*V4_CANDIDATES, "combined")
            if manifest["_version"] == 4
            else V3_CANDIDATES
            if manifest["_version"] == 3
            else VARIANTS
        )
        for name in report_candidates:
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
        if manifest["_version"] == 3:
            _run_v3(
                seed,
                output,
                budget,
                warmed,
                report,
                packages,
                provenance,
                seed_hash,
                harness,
                claims_mode,
            )
            budget.holdback_s = 0.0
            _integrity_check(packages, provenance, seed, seed_hash, budget)
            _assert_campaign_provenance(harness, budget)
            return (
                0
                if report["status"]
                in {
                    "qualified",
                    "qualified_individual_combination_deferred",
                    "insufficient_evidence",
                    "deferred",
                }
                else 1
            )
        if manifest["_version"] == 4:
            _run_v4(
                seed,
                output,
                budget,
                warmed,
                report,
                packages,
                provenance,
                harness,
            )
            # Spend the protected final-integrity window only after every
            # comparison has stopped. No candidate is retained on failure.
            budget.holdback_s = 0.0
            _integrity_check(packages, provenance, seed, seed_hash, budget)
            _assert_campaign_provenance(harness, budget)
            report["integrity_validation"] = "passed"
            return (
                0
                if report["status"]
                in {
                    "qualified",
                    "qualified_individual_combination_deferred",
                    "insufficient_evidence",
                    "deferred",
                }
                else 1
            )
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
        if report.get("manifest_version") == 4:
            report["retained"] = []
            report["integrity_validation"] = "incomplete"
        elif report.get("manifest_version") == 3:
            _retain_v3_candidate(report, packages, provenance, seed, seed_hash, harness, budget)
            if report["retained"]:
                report["status"] = "qualified_individual_combination_deferred"
        elif report.get("manifest_version") == 2:
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
        if report.get("manifest_version") == 4:
            report["retained"] = []
            report["integrity_validation"] = "incomplete"
        elif report.get("manifest_version") == 3:
            _retain_v3_candidate(report, packages, provenance, seed, seed_hash, harness, budget)
        elif report.get("manifest_version") == 2:
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
        if report.get("manifest_version") == 4:
            report["integrity_validation"] = "failed"
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"{type(exc).__name__}: {exc}"
        if report.get("manifest_version") == 4:
            report["retained"] = []
            report["integrity_validation"] = "failed"
        elif report.get("manifest_version") == 3:
            _retain_v3_candidate(report, packages, provenance, seed, seed_hash, harness, budget)
        elif report.get("manifest_version") == 2:
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
        for signum in bench.operator_signals(include_interrupt=True)
    }
    try:
        return run(parser().parse_args())
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    raise SystemExit(main())
