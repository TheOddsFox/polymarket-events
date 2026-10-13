"""Recovery checkpoint boundaries, unchanged inputs, and combined storage admission."""

import json
import subprocess
from collections import Counter
from dataclasses import asdict, replace

import pytest

from oddsfox_catalogue import rebuild
from oddsfox_catalogue.config import Settings
from oddsfox_catalogue.limits import directory_bytes, enforce_storage_limits
from oddsfox_catalogue.semantics import json_descriptor


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

    def seal(settings, **kwargs):
        # A snapshot, rather than an aliased mutable binding, must detect changes on resume.
        return json_descriptor(
            {
                "binding": binding,
                "root": str(settings.root),
                "configuration": {
                    name: asdict(getattr(settings, name))
                    for name in ("capture", "load", "gamma", "quality")
                },
            }
        )

    monkeypatch.setattr(rebuild, "_source_seal", seal)
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
    seal_allowances = []
    source_seal = rebuild._source_seal

    def seal(settings, *, query_settings):
        assert settings.capture.max_temp_bytes == 100
        seal_allowances.append(query_settings.capture.max_temp_bytes)
        return source_seal(settings, query_settings=query_settings)

    def source_binding(settings, *, query_settings):
        allowances.append(query_settings.capture.max_temp_bytes)
        return {**binding, "configuration": {"capture": asdict(settings.capture)}}

    monkeypatch.setattr(rebuild, "_source_binding", source_binding)
    monkeypatch.setattr(rebuild, "_source_seal", seal)
    fault["phase"] = "build"
    with pytest.raises(RuntimeError, match="build"):
        rebuild.rebuild_and_verify(source, scratch=scratch)
    jobs = scratch / ".state" / "dlt"
    jobs.mkdir(parents=True)
    (jobs / "job").write_bytes(b"j" * 20)
    seal_allowances.clear()
    assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True).matched
    assert set(seal_allowances) == {80}
    (jobs / "job").write_bytes(b"j" * 30)
    seal_allowances.clear()
    assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True).matched
    assert set(seal_allowances) == {70}
    # Resumes recheck exact source bytes; the full semantic source binding runs only once.
    assert allowances == [100]
    original_binding, phase, _ = rebuild._validated_rebuild_source(source, scratch)
    assert phase == "complete"
    assert original_binding["configuration"]["capture"]["max_temp_bytes"] == 100
    changed = replace(source, capture=replace(source.capture, max_temp_bytes=99))
    monkeypatch.setattr(rebuild, "_source_seal", source_seal)
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


@pytest.mark.parametrize(
    "relative",
    ["data/new", ".state/dlt/new", ".state/new", ".state/rebuild/source-bindings/new"],
)
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


def test_step_performs_one_phase_and_never_certifies_pending_work(recovery, tmp_path, monkeypatch):
    source, binding, counts, _ = recovery
    scratch = tmp_path / "proof"
    semantic_calls = []

    def source_binding(settings, **kwargs):
        semantic_calls.append(settings.root)
        return binding

    monkeypatch.setattr(rebuild, "_source_binding", source_binding)
    report = rebuild.rebuild_and_verify(source, scratch=scratch, step=True)
    assert report.pending and not report.matched and not report.tables
    assert not counts
    assert rebuild._read_record(scratch)["phase"] == "initialization"
    for completed, next_phase in (
        ("initialization", "load"),
        ("load", "build"),
        ("build", "comparison"),
    ):
        before = dict(counts)
        report = rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
        assert report.pending and not report.matched and not report.tables
        assert dict(counts) == {**before, completed: 1}
        assert rebuild._read_record(scratch)["phase"] == next_phase
    report = rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    assert report.matched and not report.pending
    assert counts == {phase: 1 for phase in ("initialization", "load", "build", "comparison")}
    assert set(report.tables) == {
        "core.rows",
        "schema:core",
        "published:rows",
        "coverage",
        "capture_inventory",
    }
    assert rebuild._read_record(scratch)["phase"] == "complete"
    assert semantic_calls == [source.root]
    # A completed proof still executes full scratch certification and comparison.
    assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True).matched
    assert counts["comparison"] == 2
    assert semantic_calls == [source.root]


def test_step_requires_explicit_scratch_without_creating_checkpoint_state(recovery):
    source, _, counts, _ = recovery
    before = list(source.root.iterdir())
    with pytest.raises(ValueError, match="explicit scratch"):
        rebuild.rebuild_and_verify(source, step=True)
    assert list(source.root.iterdir()) == before
    assert not counts


@pytest.mark.parametrize("phase", ["initialization", "load", "build", "comparison"])
def test_interrupted_step_keeps_its_checkpoint_and_resumes_only_that_phase(
    recovery, tmp_path, phase
):
    source, _, counts, fault = recovery
    scratch = tmp_path / "proof"
    assert rebuild.rebuild_and_verify(source, scratch=scratch, step=True).pending
    order = ("initialization", "load", "build", "comparison")
    for _ in range(order.index(phase)):
        assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True).pending
    checkpoint = (scratch / rebuild.REBUILD_RECORD).read_bytes()
    fault["phase"] = phase
    with pytest.raises(RuntimeError, match=f"interrupted {phase}"):
        rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    assert (scratch / rebuild.REBUILD_RECORD).read_bytes() == checkpoint
    before = dict(counts)
    report = rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    assert counts == {**before, phase: before[phase] + 1}
    assert report.matched is (phase == "comparison")
    assert report.pending is (phase != "comparison")


def test_unsuccessful_dbt_step_is_failed_not_pending_and_keeps_build_checkpoint(
    recovery, tmp_path, monkeypatch
):
    source, _, counts, _ = recovery
    scratch = tmp_path / "proof"
    assert rebuild.rebuild_and_verify(source, scratch=scratch, step=True).pending
    for _ in range(2):
        assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True).pending
    checkpoint = (scratch / rebuild.REBUILD_RECORD).read_bytes()
    before = dict(counts)
    original = rebuild.dbt_stage
    monkeypatch.setattr(
        rebuild,
        "dbt_stage",
        lambda settings, args: subprocess.CompletedProcess(args, 1, stdout="failed dbt"),
    )
    report = rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    assert not report.pending and not report.matched
    assert report.mismatches == ["dbt build failed in scratch: failed dbt"]
    assert (scratch / rebuild.REBUILD_RECORD).read_bytes() == checkpoint
    assert counts == before
    monkeypatch.setattr(rebuild, "dbt_stage", original)
    assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True).pending
    assert counts["build"] == 1 and counts["comparison"] == 0


def test_source_changes_during_first_validation_do_not_register_or_create_scratch(
    recovery, tmp_path, monkeypatch
):
    source, binding, counts, _ = recovery
    scratch = tmp_path / "proof"

    def changing_binding(settings, **kwargs):
        binding["runtime"] = "changed-during-validation"
        return binding

    monkeypatch.setattr(rebuild, "_source_binding", changing_binding)
    with pytest.raises(ValueError, match="changed during semantic validation"):
        rebuild.rebuild_and_verify(source, scratch=scratch, step=True)
    assert not scratch.exists()
    assert not rebuild._registration_path(source, scratch)[0].exists()
    assert not counts


@pytest.mark.parametrize("phase", ["initialization", "load", "build", "comparison"])
def test_source_changes_during_phase_block_its_checkpoint(recovery, tmp_path, monkeypatch, phase):
    source, binding, counts, _ = recovery
    scratch = tmp_path / "proof"
    assert rebuild.rebuild_and_verify(source, scratch=scratch, step=True).pending
    order = ("initialization", "load", "build", "comparison")
    for _ in range(order.index(phase)):
        assert rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True).pending
    checkpoint = (scratch / rebuild.REBUILD_RECORD).read_bytes()
    registration = rebuild._registration_path(source, scratch)[0]
    anchored = registration.read_bytes()
    name = {
        "initialization": "rebuild_from_raw",
        "load": "load_stage",
        "build": "dbt_stage",
        "comparison": "assert_build_valid",
    }[phase]
    original = getattr(rebuild, name)

    def change_after_work(*args, **kwargs):
        value = original(*args, **kwargs)
        binding["runtime"] = "changed-after-work"
        return value

    monkeypatch.setattr(rebuild, name, change_after_work)
    with pytest.raises(ValueError, match="binding changed"):
        rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    assert counts[phase] == 1
    assert (scratch / rebuild.REBUILD_RECORD).read_bytes() == checkpoint
    assert registration.read_bytes() == anchored
    before = dict(counts)
    with pytest.raises(ValueError, match="binding changed"):
        rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    assert counts == before


def _rechecksum_record(path, record):
    record.pop("sha256", None)
    record["sha256"] = json_descriptor(record)["sha256"]
    path.write_text(json.dumps(record))


@pytest.mark.parametrize("invalid_phase", [{}, []])
def test_rechecksummed_unhashable_phase_is_rejected_as_a_blocked_input(
    recovery, tmp_path, monkeypatch, capsys, invalid_phase
):
    from oddsfox_catalogue import cli

    source, _, counts, _ = recovery
    scratch = tmp_path / "proof"
    assert rebuild.rebuild_and_verify(source, scratch=scratch, step=True).pending
    path = scratch / rebuild.REBUILD_RECORD
    record = json.loads(path.read_bytes())
    record["phase"] = invalid_phase
    _rechecksum_record(path, record)
    with pytest.raises(ValueError, match="invalid rebuild phase"):
        rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    assert not counts
    monkeypatch.setattr(cli, "load_settings", lambda: source)
    assert cli.main(["rebuild", "--verify", "--scratch", str(scratch), "--resume", "--step"]) == 3
    assert "invalid rebuild phase" in capsys.readouterr().err
    assert not counts


@pytest.mark.parametrize("duplicate", ["record-phase", "record-reference", "registration"])
def test_duplicate_json_keys_cannot_hide_a_different_checkpoint_or_source_registration(
    recovery, tmp_path, duplicate
):
    import hashlib

    source, _, counts, _ = recovery
    scratch = tmp_path / "proof"
    assert rebuild.rebuild_and_verify(source, scratch=scratch, step=True).pending
    path = scratch / rebuild.REBUILD_RECORD
    record = json.loads(path.read_bytes())
    if duplicate == "registration":
        anchor = rebuild._registration_path(source, scratch)[0]
        original = anchor.read_bytes()
        altered = original.replace(b'"revision":1', b'"revision":0,"revision":1', 1)
        assert altered != original
        anchor.write_bytes(altered)
        record["registration"]["sha256"] = hashlib.sha256(altered).hexdigest()
        _rechecksum_record(path, record)
    elif duplicate == "record-phase":
        text = json.dumps(record)
        path.write_text(
            text.replace(
                '"phase": "initialization"', '"phase": "complete", "phase": "initialization"', 1
            )
        )
    else:
        text = json.dumps(record)
        actual = json.dumps(record["registration"]["identity"])
        path.write_text(
            text.replace(
                '"identity": ' + actual,
                '"identity": ' + json.dumps("0" * 64) + ', "identity": ' + actual,
                1,
            )
        )
    before = path.read_bytes()
    # Last-key-wins JSON would reproduce the valid checksummed object in each case.
    with pytest.raises(ValueError, match="duplicate"):
        rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    assert not counts
    assert path.read_bytes() == before


@pytest.mark.parametrize("tamper", ["binding", "seal", "reference", "hash", "old", "bool"])
def test_rechecksummed_scratch_cannot_substitute_the_source_anchor(recovery, tmp_path, tamper):
    source, binding, counts, _ = recovery
    scratch = tmp_path / "proof"
    assert rebuild.rebuild_and_verify(source, scratch=scratch, step=True).pending
    path = scratch / rebuild.REBUILD_RECORD
    record = json.loads(path.read_bytes())
    registration = rebuild._registration_path(source, scratch)[0]
    anchor = registration.read_bytes()
    if tamper in {"binding", "seal"}:
        binding["runtime"] = "changed-source"
        record[tamper] = {"runtime": "changed-source"}
    elif tamper == "reference":
        other = tmp_path / "other-proof"
        assert rebuild.rebuild_and_verify(source, scratch=other, step=True).pending
        record["registration"] = rebuild._read_record(other)["registration"]
    elif tamper == "hash":
        record["registration"]["sha256"] = "0" * 64
    elif tamper == "old":
        record = {"revision": 1, "binding": binding, "phase": "initialization"}
    else:
        record["revision"] = True
    _rechecksum_record(path, record)
    before = sorted(p.relative_to(scratch).as_posix() for p in scratch.rglob("*"))
    with pytest.raises(ValueError):
        rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    assert not counts
    assert registration.read_bytes() == anchor
    assert sorted(p.relative_to(scratch).as_posix() for p in scratch.rglob("*")) == before


def test_forged_complete_phase_still_requires_full_scratch_certification(recovery, tmp_path):
    source, _, counts, _ = recovery
    scratch = tmp_path / "proof"
    assert rebuild.rebuild_and_verify(source, scratch=scratch, step=True).pending
    path = scratch / rebuild.REBUILD_RECORD
    record = json.loads(path.read_bytes())
    record["phase"] = "complete"
    _rechecksum_record(path, record)
    with pytest.raises(FileNotFoundError):
        rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    assert not counts
    assert rebuild._read_record(scratch)["phase"] == "complete"


def test_registration_survives_checkpoint_write_failure_and_cannot_be_overwritten(
    recovery, tmp_path, monkeypatch
):
    source, _, counts, _ = recovery
    scratch = tmp_path / "proof"

    def fail_record(*args):
        raise RuntimeError("checkpoint write failed")

    monkeypatch.setattr(rebuild, "_write_record", fail_record)
    with pytest.raises(RuntimeError, match="checkpoint write failed"):
        rebuild.rebuild_and_verify(source, scratch=scratch, step=True)
    registration = rebuild._registration_path(source, scratch)[0]
    before = registration.read_bytes()
    assert not counts and not (scratch / rebuild.REBUILD_RECORD).exists()
    with pytest.raises(FileNotFoundError):
        rebuild.rebuild_and_verify(source, scratch=scratch, resume=True, step=True)
    # Removing an empty failed scratch does not authorize resetting its source proof.
    for path in scratch.rglob("*"):
        if path.is_file():
            path.unlink()
    for path in sorted(scratch.rglob("*"), reverse=True):
        if path.is_dir():
            path.rmdir()
    scratch.rmdir()
    with pytest.raises(FileExistsError, match="registration exists"):
        rebuild.rebuild_and_verify(source, scratch=scratch, step=True)
    assert registration.read_bytes() == before


@pytest.mark.parametrize(
    "pending,matched,exit_code", [(True, False, 0), (False, True, 0), (False, False, 4)]
)
def test_cli_reports_intentional_pending_without_claiming_a_match(
    recovery, tmp_path, monkeypatch, capsys, pending, matched, exit_code
):
    from oddsfox_catalogue import cli

    source, _, _, _ = recovery
    scratch = tmp_path / "proof"
    calls = []

    def run(settings, **kwargs):
        calls.append((settings, kwargs))
        return rebuild.RebuildReport(matched, scratch / "warehouse", pending=pending)

    monkeypatch.setattr(cli, "load_settings", lambda: source)
    monkeypatch.setattr(cli, "rebuild_and_verify", run)
    args = cli.build_parser().parse_args(
        ["rebuild", "--verify", "--scratch", str(scratch), "--resume", "--step"]
    )
    assert args.handler(args) == exit_code
    value = json.loads(capsys.readouterr().out)
    assert value["pending"] is pending and value["matched"] is matched
    assert calls == [(source, {"scratch": scratch, "resume": True, "step": True})]
