"""Maintenance tools reject write destinations outside ignored repository data."""

import importlib.util
import sys
import uuid
from argparse import Namespace
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
SCRIPTS = REPO / "scripts"


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


current = load("storage_benchmark_current", "benchmark-current.py")
capacity = load("storage_benchmark_capacity", "benchmark-capacity.py")
full_pass = load("storage_benchmark_full_pass", "benchmark-full-pass.py")
campaign = load("storage_optimization_campaign", "benchmark-optimization-campaign.py")
qualification = load("storage_duckdb_qualification", "qualify-duckdb-upgrade.py")
capacity_content = load("storage_capacity_content", "verify-capacity-content.py")


def outside(label):
    return REPO.parent / f"storage-policy-{label}-{uuid.uuid4().hex}"


def test_current_benchmark_rejects_external_output_before_creating_it(tmp_path):
    seed = tmp_path / "seed"
    current.validate_root(seed, initialize=True)
    output = outside("current")
    args = current.parser().parse_args(["--seed-root", str(seed), "--output", str(output)])

    with pytest.raises(ValueError, match="repository data directory"):
        current.run(args)
    assert not output.exists()


def test_capacity_rejects_external_candidate_or_output_before_creating_it(tmp_path):
    seed, candidate = tmp_path / "seed", tmp_path / "candidate"
    seed.mkdir()
    output = outside("capacity")
    args = Namespace(
        seed_root=seed,
        root=candidate,
        output=output,
        account_root=[tmp_path],
        executable=Path(sys.executable),
    )

    with pytest.raises(ValueError, match="repository data directory"):
        capacity.paths(args)
    assert not candidate.exists()
    assert not output.exists()


def test_capacity_rejects_external_candidate_before_creating_it(tmp_path):
    seed = tmp_path / "seed"
    seed.mkdir()
    candidate, output = outside("capacity-candidate"), tmp_path / "output"
    args = Namespace(
        seed_root=seed,
        root=candidate,
        output=output,
        account_root=[tmp_path],
        executable=Path(sys.executable),
    )

    with pytest.raises(ValueError, match="repository data directory"):
        capacity.paths(args)
    assert not candidate.exists()
    assert not output.exists()


@pytest.mark.parametrize("tool", ["current", "capacity"])
def test_benchmarks_reject_external_seed_before_output_or_lock_creation(tmp_path, tool):
    seed = outside(f"{tool}-seed")
    output = tmp_path / "output"
    candidate = tmp_path / "candidate"
    account = tmp_path / "account"
    account.mkdir()

    with pytest.raises(ValueError, match="repository data directory"):
        if tool == "current":
            current.run(
                current.parser().parse_args(["--seed-root", str(seed), "--output", str(output)])
            )
        else:
            capacity.paths(
                Namespace(
                    seed_root=seed,
                    root=candidate,
                    output=output,
                    account_root=[account],
                    executable=Path(sys.executable),
                )
            )

    assert not output.exists()
    assert not candidate.exists()
    assert not (seed / ".catalogue.lock").exists()


def test_optimization_campaign_rejects_external_output_before_reading_manifest():
    output = outside("campaign")
    with pytest.raises(ValueError, match="repository data directory"):
        campaign.run(Namespace(output=output))
    assert not output.exists()


def test_full_pass_rejects_external_public_output_before_creating_it():
    output = outside("full-pass")
    args = full_pass.parser().parse_args(["--profile", "smoke", "--output", str(output)])
    with pytest.raises(ValueError, match="repository data directory"):
        full_pass.run(args)
    assert not output.exists()


def full_pass_args(output, *flags):
    return full_pass.parser().parse_args(
        ["--profile", "smoke", "--output", str(output), *map(str, flags)]
    )


def test_snapshot_hidden_flag_rejects_external_target_before_source_lock(tmp_path, capsys):
    source = tmp_path / "source"
    source.mkdir()
    target, requests = outside("snapshot-target"), tmp_path / "requests.json"

    assert (
        full_pass.run(full_pass_args(tmp_path, "--_snapshot_catalogue", source, target, requests))
        == 3
    )
    assert not (source / ".catalogue.lock").exists()
    assert not target.exists()
    assert not requests.exists()
    capsys.readouterr()


def test_snapshot_hidden_flag_rejects_external_request_output_before_source_lock(tmp_path, capsys):
    source = tmp_path / "source"
    source.mkdir()
    target, requests = tmp_path / "snapshot", outside("snapshot-requests")

    assert (
        full_pass.run(full_pass_args(tmp_path, "--_snapshot_catalogue", source, target, requests))
        == 3
    )
    assert not (source / ".catalogue.lock").exists()
    assert not target.exists()
    assert not requests.exists()
    capsys.readouterr()


def test_prepare_v6_hidden_flag_rejects_external_request_output_before_recalibration(
    tmp_path, capsys
):
    source = tmp_path / "snapshot"
    source.mkdir()
    requests = outside("prepare-v6-requests")

    assert full_pass.run(full_pass_args(tmp_path, "--_prepare_v6_snapshot", source, requests)) == 3
    assert not (source / ".catalogue.lock").exists()
    assert not requests.exists()
    capsys.readouterr()


def test_fixed_sample_hidden_flag_rejects_external_target_before_source_lock(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    target = outside("fixed-sample-root")
    requests = outside("fixed-sample-requests")

    with pytest.raises(ValueError, match="repository data directory"):
        full_pass.run(
            full_pass_args(
                tmp_path,
                "--_fixed_catalogue_sample",
                source,
                target,
                "test",
                requests,
            )
        )
    assert not (source / ".catalogue.lock").exists()
    assert not target.exists()


def test_claim_hidden_flag_rejects_external_target_before_source_lock(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    target = outside("claim-target")

    with pytest.raises(ValueError, match="repository data directory"):
        full_pass.run(full_pass_args(tmp_path, "--_claim_resource", source, target))
    assert not (source / ".catalogue.lock").exists()
    assert not target.exists()


def test_scheduler_hidden_flag_rejects_external_root_before_mkdir(tmp_path):
    root = outside("scheduler-root")
    args = full_pass_args(tmp_path, "--_scheduler_diagnostics", outside("seed"), root)

    with pytest.raises(ValueError, match="repository data directory"):
        full_pass.run(args)
    assert not root.exists()


def test_profile_hidden_flag_rejects_external_profile_before_copy_or_http(tmp_path):
    root = tmp_path / "sample-root"
    output = tmp_path / "output"
    output.mkdir()
    profile = outside("profile")
    args = full_pass_args(
        output,
        "--_sample",
        outside("seed"),
        root,
        "profile-test",
        "1",
        "--_profile",
        profile,
    )

    with pytest.raises(ValueError, match="repository data directory"):
        full_pass.run(args)
    assert not root.exists()
    assert not profile.exists()


def test_upgrade_qualification_rejects_external_evidence_and_destination(tmp_path):
    root = tmp_path / "root"
    evidence = outside("qualification-evidence")
    destination = outside("qualification-destination")

    with pytest.raises(ValueError, match="repository data directory"):
        qualification._safe_evidence_output(evidence, root)
    with pytest.raises(ValueError, match="repository data directory"):
        qualification._validate_destination(root, destination, tmp_path / "evidence")
    assert not evidence.exists()
    assert not destination.exists()


def test_capacity_content_rejects_external_output_before_reading_inputs(monkeypatch):
    output = outside("capacity-content")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPTS / "verify-capacity-content.py"),
            "--source",
            str(outside("source")),
            "--root",
            str(outside("root")),
            "--audit",
            str(outside("audit")),
            "--output",
            str(output),
        ],
    )

    with pytest.raises(ValueError, match="repository data directory"):
        capacity_content.main()
    assert not output.exists()


def test_capacity_content_rejects_external_temporary_before_reading_inputs(
    monkeypatch,
):
    temporary = outside("capacity-content-temp")
    monkeypatch.setattr(
        capacity_content,
        "safe_path",
        lambda *_: pytest.fail("database inputs must not be read"),
    )

    with pytest.raises(ValueError, match="repository data directory"):
        capacity_content.verify(outside("source"), outside("root"), outside("audit"), temporary)
    assert not temporary.exists()
