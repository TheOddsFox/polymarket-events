"""Recovery checkpoint boundaries, unchanged inputs, and combined storage admission."""

import json
import subprocess
from collections import Counter
from dataclasses import asdict, replace

import pytest

from oddsfox_catalogue import rebuild
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.limits import directory_bytes, enforce_storage_limits


@pytest.fixture
def recovery(tmp_path, monkeypatch):
    source = Settings(tmp_path / "source")
    source.root.mkdir()
    binding = {
        "warehouse": {"relations": {"core.rows": {"rows": 1}}, "schemas": {"core": []}},
        "published": {"rows": {"rows": 1}},
        "capture": {"coverage": {"units": 1}, "inventory": {"objects": 1}},
        "runtime": "fixed-runtime",
    }
    counts = Counter()
    fault = {"phase": None}

    def step(name):
        counts[name] += 1
        if fault["phase"] == name:
            fault["phase"] = None
            raise RuntimeError(f"interrupted {name}")

    def initialize(settings, ledger):
        (settings.root / "initialized-unit").write_text("verified")
        step("initialization")

    def load(settings):
        # A durable loaded chunk must remain even if its following phase marker was not written.
        (settings.root / "loaded-unit").write_bytes(b"committed")
        enforce_storage_limits(settings)
        step("load")

    def build(settings, args):
        assert args == ["build"]
        step("build")
        (settings.root / "certified").write_text("complete")
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    def certified(settings):
        assert (settings.root / "certified").read_text() == "complete"
        step("comparison")
        return {"binding": binding}

    monkeypatch.setattr(rebuild, "_source_binding", lambda settings, **kwargs: binding)
    monkeypatch.setattr(rebuild, "VERIFIED_TABLES", ("core.rows",))
    monkeypatch.setattr(rebuild, "rebuild_from_raw", initialize)
    monkeypatch.setattr(rebuild, "load_stage", load)
    monkeypatch.setattr(rebuild, "dbt_stage", build)
    monkeypatch.setattr(rebuild, "assert_build_valid", certified)
    return source, binding, counts, fault


@pytest.mark.parametrize("phase", ["initialization", "load", "build", "comparison"])
@pytest.mark.parametrize("nested", [False, True])
def test_interruptions_resume_only_the_uncommitted_phase(recovery, tmp_path, phase, nested):
    source, _, counts, fault = recovery
    scratch = source.state_dir / "rebuild" / "proof" if nested else tmp_path / "proof"
    fault["phase"] = phase
    with pytest.raises(RuntimeError, match=f"interrupted {phase}"):
        rebuild.rebuild_and_verify(source, scratch=scratch)
    assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True).matched
    assert counts["initialization"] == (2 if phase == "initialization" else 1)
    assert counts["load"] == (2 if phase == "load" else 1)
    assert counts["build"] == (2 if phase == "build" else 1)
    assert json.loads((scratch / rebuild.REBUILD_RECORD).read_bytes())["phase"] == "complete"
    prior = dict(counts)
    assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True).matched
    assert {k: counts[k] for k in ("initialization", "load", "build")} == {
        k: prior[k] for k in ("initialization", "load", "build")
    }


def test_changed_binding_is_rejected_without_resetting_loaded_scratch(recovery, tmp_path):
    source, binding, counts, fault = recovery
    scratch = tmp_path / "proof"
    fault["phase"] = "build"
    with pytest.raises(RuntimeError):
        rebuild.rebuild_and_verify(source, scratch=scratch)
    before = (scratch / rebuild.REBUILD_RECORD).read_bytes()
    prior = dict(counts)
    binding["runtime"] = "different-runtime"
    with pytest.raises(ValueError, match="binding changed"):
        rebuild.rebuild_and_verify(source, scratch=scratch, resume=True)
    assert counts == prior
    assert (scratch / rebuild.REBUILD_RECORD).read_bytes() == before
    assert (scratch / "loaded-unit").read_bytes() == b"committed"


@pytest.mark.parametrize("nested", [False, True])
def test_scratch_temp_growth_reserves_source_reads_without_changing_resume_policy(
    recovery, tmp_path, monkeypatch, nested
):
    source, binding, _, fault = recovery
    source = replace(source, capture=replace(source.capture, max_temp_bytes=100))
    source.temporary_dir.mkdir(parents=True)
    (source.temporary_dir / "source-spill").write_bytes(b"s" * 10)
    scratch = source.state_dir / "rebuild" / "proof" if nested else tmp_path / "proof"
    allowances = []

    def source_binding(settings, *, query_settings):
        allowances.append(query_settings.capture.max_temp_bytes)
        return {**binding, "configuration": {"capture": asdict(settings.capture)}}

    monkeypatch.setattr(rebuild, "_source_binding", source_binding)
    fault["phase"] = "build"
    with pytest.raises(RuntimeError, match="build"):
        rebuild.rebuild_and_verify(source, scratch=scratch)
    jobs = scratch / ".state" / "dlt"
    jobs.mkdir(parents=True)
    (jobs / "job").write_bytes(b"j" * 20)
    assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True).matched
    (jobs / "job").write_bytes(b"j" * 30)
    assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True).matched
    assert allowances == [100, 80, 70]
    record = json.loads((scratch / rebuild.REBUILD_RECORD).read_bytes())
    assert record["binding"]["configuration"]["capture"]["max_temp_bytes"] == 100
    changed = replace(source, capture=replace(source.capture, max_temp_bytes=99))
    before = (scratch / rebuild.REBUILD_RECORD).read_bytes()
    with pytest.raises(ValueError, match="binding changed"):
        rebuild.rebuild_and_verify(changed, scratch=scratch, resume=True)
    assert (scratch / rebuild.REBUILD_RECORD).read_bytes() == before


def test_source_binding_uses_query_allowance_but_retains_declared_configuration(
    tmp_path, monkeypatch
):
    source = Settings(tmp_path)
    queried = replace(source, capture=replace(source.capture, max_temp_bytes=1234))
    calls = []
    monkeypatch.setattr(rebuild, "read_build_validity", lambda settings: None)
    monkeypatch.setattr(rebuild, "_runtime_revision", lambda: "runtime")
    monkeypatch.setattr(rebuild, "_model_revision", lambda settings: "model")

    def read(settings):
        calls.append(settings)
        return {"evidence": "unchanged"}

    for name in ("current_release", "capture_binding", "warehouse_snapshot", "published_snapshot"):
        monkeypatch.setattr(rebuild, name, read)
    binding = rebuild._source_binding(source, query_settings=queried)
    assert calls == [queried] * 4
    assert binding["configuration"]["capture"] == asdict(source.capture)
    assert binding["configuration"]["load"] == asdict(source.load)


def test_record_tampering_fails_before_stage_mutation(recovery, tmp_path):
    source, _, counts, _ = recovery
    scratch = tmp_path / "proof"
    assert rebuild.rebuild_and_verify(source, scratch=scratch).matched
    record = json.loads((scratch / rebuild.REBUILD_RECORD).read_bytes())
    record["phase"] = "initialization"
    (scratch / rebuild.REBUILD_RECORD).write_text(json.dumps(record))
    prior = dict(counts)
    with pytest.raises(ValueError, match="checksum"):
        rebuild.rebuild_and_verify(source, scratch=scratch, resume=True)
    assert counts == prior


@pytest.mark.parametrize("nested", [False, True])
def test_resume_cap_includes_existing_scratch_instead_of_subtracting_it_twice(
    recovery, tmp_path, monkeypatch, nested
):
    source, _, _, fault = recovery
    source = replace(source, capture=replace(source.capture, max_retained_bytes=300000))
    (source.root / "evidence").write_bytes(b"s" * 100000)
    scratch = source.state_dir / "rebuild" / "proof" if nested else tmp_path / "proof"
    fault["phase"] = "build"
    with pytest.raises(RuntimeError):
        rebuild.rebuild_and_verify(source, scratch=scratch)
    (scratch / "committed-payload").write_bytes(b"c" * 100000)
    assert directory_bytes(scratch) > 100000
    assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True).matched


def test_resume_requires_an_explicit_bound_directory(recovery, tmp_path):
    source, _, _, _ = recovery
    with pytest.raises(ValueError, match="explicit scratch"):
        rebuild.rebuild_and_verify(source, resume=True)
    existing = tmp_path / "unrelated"
    existing.mkdir()
    (existing / "keep").write_text("retain")
    with pytest.raises(FileNotFoundError):
        rebuild.rebuild_and_verify(source, scratch=existing, resume=True)
    assert (existing / "keep").read_text() == "retain"
    assert sorted(p.name for p in existing.iterdir()) == ["keep"]


@pytest.mark.parametrize("relative", ["data/new", ".state/dlt/new", ".state/new"])
def test_scratch_cannot_overlap_protected_source(recovery, relative):
    source, _, counts, _ = recovery
    with pytest.raises(ValueError, match="protected source"):
        rebuild.rebuild_and_verify(source, scratch=source.root / relative)
    assert not counts


def test_lexical_symlink_ancestor_cannot_be_normalized_away(recovery, tmp_path):
    source, _, _, _ = recovery
    (tmp_path / "alias").symlink_to(source.root, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        rebuild.rebuild_and_verify(source, scratch=tmp_path / "alias" / ".." / "proof")
