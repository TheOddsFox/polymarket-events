"""Adversarial trust-boundary and evidence checks for the V6 campaign."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark-optimization-campaign.py"
SPEC = importlib.util.spec_from_file_location("campaign_v6_adversarial", SCRIPT)
campaign = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(campaign)
bench = campaign.bench


def _package(path, *, store="SAME = 1\n", **files):
    path.mkdir(parents=True)
    (path / "__init__.py").write_text("")
    (path / "current_store.py").write_text(store)
    for name, value in files.items():
        (path / name).write_text(value)
    return path


def _entry(path):
    return {"path": str(path), "source_hashes": bench.source_hashes(path)}


def _manifest(tmp_path):
    baseline = _package(tmp_path / "baseline")
    source = tmp_path / "source"
    source.mkdir()
    (source / "catalogue.duckdb").write_bytes(b"parser-only source fixture")
    (source / "config.toml").write_text("[limits]\n")
    source_hashes = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in source.iterdir()
    }
    all_features = "+".join(sorted(campaign.V6_CANDIDATES))
    data = {
        "version": 6,
        "baseline": _entry(baseline),
        "candidates": {
            name: _entry(_package(tmp_path / name, store=f"FEATURE = {name!r}\n"))
            for name in campaign.V6_CANDIDATES
        },
        "combinations": {
            all_features: _entry(_package(tmp_path / "all", store="FEATURES = 'all'\n"))
        },
        "catalogue_source": {"path": str(source), "source_hashes": source_hashes},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(data))
    return path, data


@pytest.mark.parametrize("location", ["top", "candidate", "source"])
def test_nested_duplicate_manifest_keys_are_rejected_before_resolution(tmp_path, location):
    path, _ = _manifest(tmp_path)
    raw = path.read_text()
    old, new = {
        "top": ('"version": 6', '"version": 6, "version": 6'),
        "candidate": (
            '"columnar_renewals": {',
            '"columnar_renewals": {}, "columnar_renewals": {',
        ),
        "source": ('"catalogue_source": {', '"catalogue_source": {}, "catalogue_source": {'),
    }[location]
    assert old in raw
    path.write_text(raw.replace(old, new, 1))
    with pytest.raises(ValueError, match="duplicate key"):
        campaign._load_manifest(path)


@pytest.mark.parametrize("value", [True, False, "6", 6.0])
def test_v6_version_is_an_integer_not_a_coercible_value(tmp_path, value):
    path, data = _manifest(tmp_path)
    data["version"] = value
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="version"):
        campaign._load_manifest(path)


def test_v6_requires_explicit_all_features_package_and_rejects_unknown_candidates(tmp_path):
    path, data = _manifest(tmp_path)
    data["combinations"] = {}
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="explicit"):
        campaign._load_manifest(path)
    data["candidates"]["run_command"] = data["baseline"]
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="requires claim_asset_index"):
        campaign._load_manifest(path)


@pytest.mark.parametrize(
    "key",
    [
        "staged_state_keys+claim_asset_index",
        "claim_asset_index+claim_asset_index",
        "claim_asset_index+queue",
        "columnar_renewals",
    ],
)
def test_v6_combinations_cannot_smuggle_unknown_duplicate_or_unsorted_features(tmp_path, key):
    path, data = _manifest(tmp_path)
    data["combinations"][key] = data["baseline"]
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="sorted feature names"):
        campaign._load_manifest(path)


def test_v6_rejects_executable_configuration_at_manifest_and_source_boundaries(tmp_path):
    path, data = _manifest(tmp_path)
    data["command"] = ["python", "anything.py"]
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="requires only"):
        campaign._load_manifest(path)
    data.pop("command")
    data["catalogue_source"]["command"] = ["python", "anything.py"]
    path.write_text(json.dumps(data))
    loaded = campaign._load_manifest(path)
    with pytest.raises(ValueError, match="only path and source_hashes"):
        campaign._validate_v6_source_entry(loaded["_catalogue_source"], path)


@pytest.mark.parametrize(
    "name",
    [
        "../outside",
        "/absolute",
        "nested/../outside",
        "nested\\outside",
        "drive:file",
        "./catalogue.duckdb",
        "",
    ],
)
def test_v6_source_inventory_rejects_unsafe_names(tmp_path, name):
    path, data = _manifest(tmp_path)
    data["catalogue_source"]["source_hashes"] = {name: "a" * 64}
    with pytest.raises(ValueError, match="malformed"):
        campaign._validate_v6_source_entry(data["catalogue_source"], path)


def test_v6_source_inventory_covers_non_python_and_nested_files_without_following_symlinks(
    tmp_path,
):
    path, data = _manifest(tmp_path)
    source = Path(data["catalogue_source"]["path"])
    nested = source / "published" / "releases"
    nested.mkdir(parents=True)
    manifest = nested / "manifest.json"
    manifest.write_text("{}")
    hashes = campaign._hash_catalogue_source(source)
    assert set(hashes) == {"catalogue.duckdb", "config.toml", "published/releases/manifest.json"}
    assert hashes["published/releases/manifest.json"] == hashlib.sha256(b"{}").hexdigest()
    (source / "escape").symlink_to(tmp_path / "baseline", target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        campaign._hash_catalogue_source(source)


@pytest.mark.parametrize("relation", ["child", "parent"])
def test_v6_source_cannot_overlap_evidence_output(tmp_path, relation):
    path, data = _manifest(tmp_path)
    source = Path(data["catalogue_source"]["path"])
    output = source / "evidence" if relation == "child" else tmp_path
    with pytest.raises(ValueError):
        campaign._validate_v6_source_entry(data["catalogue_source"], path, output)


def test_v6_hash_preflight_detects_post_manifest_candidate_tampering(tmp_path):
    path, _ = _manifest(tmp_path)
    loaded = campaign._load_manifest(path)
    paths = campaign._preflight_paths(loaded, path, tmp_path / "out")
    package, _ = paths["staged_state_keys"]
    (package / "current_store.py").write_text("TAMPERED = 1\n")
    with pytest.raises(ValueError, match="hash inventory"):
        campaign._preflight(paths, bench.Budget(limit_s=10800))


def test_v6_freeze_rejects_unowned_added_source_even_with_matching_hash_inventory(tmp_path):
    path, data = _manifest(tmp_path)
    package = Path(data["candidates"]["staged_state_keys"]["path"])
    (package / "surprise.py").write_text("UNOWNED = 1\n")
    data["candidates"]["staged_state_keys"] = _entry(package)
    path.write_text(json.dumps(data))
    paths = campaign._preflight_paths(campaign._load_manifest(path), path, tmp_path / "out")
    resolved = campaign._preflight(paths, bench.Budget(limit_s=10800))
    with pytest.raises(ValueError, match="ownership|outside|inventory"):
        campaign._freeze_v6_packages(resolved, tmp_path / "out", bench.Budget(limit_s=10800))


def _sample(pair, *, acquire=20.0):
    return {
        "measurement_version": 2,
        "pair": pair,
        "acquire_apply_s": acquire,
        "wall_s": 30.0,
        "peak_rss_bytes": 1000,
        "worker_peak_before_oracle_bytes": 900,
        "http_attempts": 10,
        "runtime_reserved_attempts": 12,
        "downloaded_bytes": 110,
        "runtime_downloaded_bytes": 90,
        "semantic_hash": "exact-public-private-state-and-completed-work",
        "settled_units": 37,
        "completed_work": {
            "closed_batches_requested": 32,
            "active_batches_requested": 4,
            "event_ids_requested": 3600,
            "orphan_market_ids_requested": 1,
            "omission_ids_confirmed": 0,
            "native_repair_requests": 0,
            "native_repair_responses": 0,
        },
        "fixed_receipt_coverage": True,
        "publication_verified": True,
        "native_repair_requests": 0,
        "native_repair_responses": 0,
        "scheduler_cursor_progression": "not_applicable_fixed_work_units",
        "classification_signature": {"usable": 1},
        "capture_units": [],
        "publication_noop": False,
        "settled_requests": [
            {"lane": lane, "ids": batch, "native": False, "omitted": []}
            for lane, batches in (
                ("events", _work_spec()["closed_event_batches"]),
                ("events", _work_spec()["active_event_batches"]),
                ("markets", [_work_spec()["orphan_market_ids"]]),
            )
            for batch in batches
        ],
        "cursor": 10000,
        "counts": {"events": 10000, "markets": 40400},
    }


@pytest.mark.parametrize(
    "counter",
    ["http_attempts", "runtime_reserved_attempts", "downloaded_bytes", "runtime_downloaded_bytes"],
)
def test_shared_v6_guard_rejects_one_pair_http_increase_even_when_median_is_unchanged(counter):
    baseline = [_sample(pair) for pair in (1, 2, 3)]
    candidate = [_sample(pair, acquire=15.0) for pair in (1, 2, 3)]
    assert bench.comparison(baseline, candidate, "perf")["status"] == "qualified"
    candidate[2][counter] += 1
    assert bench.comparison(baseline, candidate, "perf")["status"] == "deferred"


@pytest.mark.parametrize("defect", ["duplicate_pair", "cleanup", "private_state", "preoracle_peak"])
def test_shared_v6_guard_cannot_qualify_less_work_or_incomplete_evidence(defect):
    baseline = [_sample(pair) for pair in (1, 2, 3)]
    candidate = [_sample(pair, acquire=15.0) for pair in (1, 2, 3)]
    if defect == "duplicate_pair":
        candidate[2]["pair"] = 2
    elif defect == "cleanup":
        candidate[2]["cleanup_deferred"] = True
    elif defect == "private_state":
        candidate[2]["semantic_hash"] = "private-claims-differ"
    else:
        for sample in candidate:
            sample["worker_peak_before_oracle_bytes"] = 955
    assert bench.comparison(baseline, candidate, "perf")["status"] != "qualified"


def _oracle_root(path, response_id, literal_note):
    import duckdb

    from oddsfox_catalogue import current_store
    from oddsfox_catalogue.current_contract import ResponseRows

    path.mkdir()
    with duckdb.connect(str(path / "catalogue.duckdb")) as conn:
        current_store.initialize(conn)
        current_store.apply(
            conn,
            ResponseRows(
                "2026-01-01T00:00:00Z",
                response_id,
                events=[{"venue": "polymarket", "event_id": "1", "title": "same"}],
            ),
        )
        current_store.set_state(conn, "literal_operator_note", literal_note)
    return path


def test_v6_oracle_normalizes_capture_columns_without_rewriting_literal_state(tmp_path):
    first = bench.stream_catalogue_oracle(
        _oracle_root(tmp_path / "a", "capture-a", "capture-a"),
        generated_responses={"capture-a": "unit:1"},
    )
    second = bench.stream_catalogue_oracle(
        _oracle_root(tmp_path / "b", "capture-b", "capture-a"),
        generated_responses={"capture-b": "unit:1"},
    )
    assert first["tables"] == second["tables"]
    assert first["state"]["literal_operator_note"] == "capture-a"
    assert first["state"] == second["state"]


def test_v6_oracle_cannot_mask_private_state_changes_that_look_like_capture_ids(tmp_path):
    first = bench.stream_catalogue_oracle(
        _oracle_root(tmp_path / "a", "capture-a", "capture-a"),
        generated_responses={"capture-a": "unit:1"},
    )
    second = bench.stream_catalogue_oracle(
        _oracle_root(tmp_path / "b", "capture-b", "capture-b"),
        generated_responses={"capture-b": "unit:1"},
    )
    assert first["state"] != second["state"]


def _source_for_snapshot(path):
    import duckdb

    from oddsfox_catalogue.current_config import CurrentSettings, validate_root
    from oddsfox_catalogue.current_store import initialize

    settings = CurrentSettings(path)
    validate_root(path, initialize=True)
    with duckdb.connect(str(settings.store_path)) as conn:
        initialize(conn)
        conn.execute(
            "INSERT INTO events(venue,event_id,title,closed) "
            "SELECT 'polymarket', i::VARCHAR, 'fixture', i<=3200 FROM range(1,3601) t(i)"
        )
        conn.execute(
            "INSERT INTO _receipts SELECT 'event',venue,event_id,"
            "'2026-01-01T00:00:00Z'::TIMESTAMPTZ,'seed' FROM events"
        )
        conn.execute(
            "INSERT INTO markets(venue,market_id,usable,identity_error) "
            "VALUES ('polymarket','9999',false,'native_confirmation_required')"
        )
    return path


@pytest.mark.parametrize("overlap", ["target", "requests"])
def test_v6_snapshot_rejects_source_descendants_before_copying(tmp_path, monkeypatch, overlap):
    source = _source_for_snapshot(tmp_path / "source")
    target = source / "nested-copy" if overlap == "target" else tmp_path / "snapshot"
    requests = source / "requests.json" if overlap == "requests" else tmp_path / "requests.json"

    def forbidden_copy(*_args, **_kwargs):
        pytest.fail("overlapping snapshot paths must be rejected before source copying")

    monkeypatch.setattr(bench.shutil, "copytree", forbidden_copy)
    with pytest.raises(ValueError, match="overlap|disjoint"):
        bench.snapshot_catalogue(source, target, requests)


@pytest.mark.parametrize(
    "regression",
    [
        None,
        "http_attempts",
        "runtime_reserved_attempts",
        "downloaded_bytes",
        "runtime_downloaded_bytes",
        "wall_s",
        "peak_rss_bytes",
        "worker_peak_before_oracle_bytes",
    ],
)
def test_v6_synthetic_guard_requires_every_resource_and_http_guard_not_just_acquisition(
    tmp_path, monkeypatch, regression
):
    baseline = [{**_sample(pair), "category": "baseline"} for pair in (1, 2, 3)]
    candidate = [{**_sample(pair, acquire=20.5), "category": "candidate"} for pair in (1, 2, 3)]
    if regression in {
        "http_attempts",
        "runtime_reserved_attempts",
        "downloaded_bytes",
        "runtime_downloaded_bytes",
    }:
        candidate[2][regression] += 1
    elif regression is not None:
        for sample in candidate:
            sample[regression] *= 1.06
    report = {"stages": {}}

    def measured_comparison(stage, *_args, **_kwargs):
        report["stages"][stage] = {"samples": [*baseline, *candidate]}
        return bench.comparison(baseline, candidate, "perf")

    monkeypatch.setattr(campaign, "_compare_stage", measured_comparison)
    result = campaign._v6_synthetic_guard(
        "candidate",
        tmp_path / "candidate",
        tmp_path / "baseline",
        tmp_path / "seed",
        tmp_path,
        bench.Budget(limit_s=10800),
        set(),
        report,
    )
    assert result["status"] == ("passed" if regression is None else "failed")


def _work_spec():
    return {
        "closed_event_batches": [
            [str(1 + batch * 100 + index) for index in range(100)] for batch in range(32)
        ],
        "active_event_batches": [
            [str(10001 + batch * 100 + index) for index in range(100)] for batch in range(4)
        ],
        "orphan_market_ids": ["9999"],
    }


def _frozen_requests(path):
    path.write_text(json.dumps({"requests": _work_spec()}))
    return path


def test_v6_fixed_comparison_runs_exact_ab_ba_ab_completed_pairs(tmp_path, monkeypatch):
    calls = []
    _frozen_requests(tmp_path / "requests.json")
    baseline, candidate = tmp_path / "baseline", tmp_path / "candidate"
    report = {"stages": {}}
    monkeypatch.setattr(campaign, "_v6_warm", lambda *_args, **_kwargs: {"wall_s": 1})

    def sample(_source, _requests, _output, package, _budget, _label):
        calls.append(package.name)
        return _sample(0, acquire=20 if package == baseline else 15)

    monkeypatch.setattr(campaign, "_v6_sample", sample)
    result = campaign._v6_fixed_comparison(
        "fixed",
        "baseline",
        baseline,
        "candidate",
        candidate,
        tmp_path / "source",
        tmp_path / "requests.json",
        tmp_path,
        bench.Budget(limit_s=10800),
        report,
        set(),
    )
    assert calls == ["baseline", "candidate", "candidate", "baseline", "baseline", "candidate"]
    assert result["status"] == "qualified"
    assert [(row["category"], row["pair"]) for row in report["stages"]["fixed"]["samples"]] == [
        ("baseline", 1),
        ("candidate", 1),
        ("candidate", 2),
        ("baseline", 2),
        ("baseline", 3),
        ("candidate", 3),
    ]


def test_v6_incomplete_fixed_comparison_preserves_partial_evidence(tmp_path, monkeypatch):
    _frozen_requests(tmp_path / "requests.json")
    report = {"stages": {}}
    calls = 0
    monkeypatch.setattr(campaign, "_v6_warm", lambda *_args, **_kwargs: {"wall_s": 1})

    def sample(*_args):
        nonlocal calls
        calls += 1
        if calls == 4:
            raise TimeoutError("shared campaign deadline")
        return _sample(0)

    monkeypatch.setattr(campaign, "_v6_sample", sample)
    with pytest.raises(TimeoutError, match="shared campaign"):
        campaign._v6_fixed_comparison(
            "fixed",
            "baseline",
            tmp_path / "base",
            "candidate",
            tmp_path / "candidate",
            tmp_path / "source",
            tmp_path / "requests.json",
            tmp_path,
            bench.Budget(limit_s=10800),
            report,
            set(),
        )
    persisted = json.loads((tmp_path / "report.json").read_text())
    assert len(persisted["stages"]["fixed"]["samples"]) == 3
    assert "retained" not in persisted or not persisted["retained"]


@pytest.mark.parametrize("budget_s, admitted", [(30, False), (50, True)])
def test_v6_admission_reserves_integrity_and_shutdown_before_scored_work(
    tmp_path, monkeypatch, budget_s, admitted
):
    packages = {name: tmp_path / name for name in ("baseline", *campaign.V6_CANDIDATES)}
    provenance = {
        name: {"eligible": name == "staged_state_keys"} for name in campaign.V6_CANDIDATES
    }
    report = {
        "stages": {},
        "warmups": {},
        "catalogue_source": {"request_manifest_sha256": "a" * 64},
    }
    budget = bench.Budget(limit_s=budget_s)
    monkeypatch.setattr(campaign, "_v6_warm", lambda *_args, **_kwargs: {"wall_s": 1})

    def warm(name, _package, _seed, _output, _budget, _warmed, current):
        current["warmups"][name] = {"sample": {"wall_s": 1}}

    scored = []

    def comparison(*_args, **_kwargs):
        scored.append(True)
        return {"status": "deferred", "reason": "no material savings"}

    monkeypatch.setattr(campaign, "_warm", warm)
    monkeypatch.setattr(campaign, "_v6_fixed_comparison", comparison)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    campaign._v6_run(
        tmp_path / "seed",
        "seed-hash",
        tmp_path / "source",
        tmp_path / "requests.json",
        tmp_path,
        budget,
        set(),
        report,
        packages,
        provenance,
        {},
    )
    assert bool(scored) == admitted
    assert report["retained"] == []
    if not admitted:
        assert "insufficient_evidence" in report["deferred"]["staged_state_keys"]
        assert budget.remaining == 0


@pytest.mark.parametrize("failure", ["source", "snapshot", "requests"])
def test_v6_final_integrity_rechecks_each_frozen_input(tmp_path, monkeypatch, failure):
    source = tmp_path / "source"
    source.mkdir()
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    (snapshot / "catalogue.duckdb").write_bytes(b"frozen database")
    requests = tmp_path / "requests.json"
    requests.write_text('{"frozen":true}')
    snapshot_hashes = campaign._hash_catalogue_source(snapshot)
    request_hash = hashlib.sha256(requests.read_bytes()).hexdigest()
    expected_source = {"catalogue.duckdb": "a" * 64}
    if failure == "snapshot":
        (snapshot / "catalogue.duckdb").write_bytes(b"changed database")
    if failure == "requests":
        requests.write_text('{"frozen":false}')
    monkeypatch.setattr(campaign, "_integrity_check", lambda *_args: None)
    monkeypatch.setattr(campaign, "_assert_campaign_provenance", lambda *_args: None)
    monkeypatch.setattr(
        campaign,
        "_v6_control_worker",
        lambda *_args: {
            "source_hashes": {"catalogue.duckdb": "b" * 64}
            if failure == "source"
            else expected_source,
        },
    )
    with pytest.raises(campaign.CampaignIntegrityError, match="changed"):
        campaign._v6_final_integrity(
            {},
            {},
            tmp_path / "seed",
            "seed-hash",
            {},
            source,
            expected_source,
            snapshot,
            snapshot_hashes,
            requests,
            request_hash,
            tmp_path,
            bench.Budget(limit_s=10800),
            tmp_path / "baseline",
        )


@pytest.mark.parametrize("failure", ["changed", "unfinished"])
def test_v6_run_clears_provisional_retention_when_final_integrity_fails(
    tmp_path, monkeypatch, failure
):
    path, data = _manifest(tmp_path)
    output = tmp_path / "evidence"
    args = campaign.parser().parse_args(["--manifest", str(path), "--output", str(output)])
    distribution = {"test_fixture": "bounded final-integrity guard"}
    monkeypatch.setattr(
        bench, "build_dataset", lambda *_args, **_kwargs: (None, None, distribution)
    )
    monkeypatch.setattr(campaign, "_machine_provenance", lambda *_args: {})
    monkeypatch.setattr(campaign, "_campaign_provenance", lambda *_args: {})
    monkeypatch.setattr(
        bench,
        "supervised_prepare",
        lambda *_args, **_kwargs: {
            "status": "prepared",
            "source_hashes": data["baseline"]["source_hashes"],
            "distribution": distribution,
            "seed_sha256": "0" * 64,
        },
    )

    def prepare(_manifest, _path, _output, _package, _budget, report):
        report["catalogue_source"] = {"request_manifest_sha256": "a" * 64}
        return (tmp_path / "source", {}, tmp_path / "snapshot", tmp_path / "requests.json", {})

    def qualify(_seed, _hash, _source, _requests, _output, _budget, _warmed, report, *_args):
        report["status"] = "qualified"
        report["retained"] = ["staged_state_keys"]

    def fail_integrity(*_args, **_kwargs):
        if failure == "changed":
            raise campaign.CampaignIntegrityError("source changed")
        raise TimeoutError("final integrity budget exhausted")

    monkeypatch.setattr(campaign, "_v6_prepare_source", prepare)
    monkeypatch.setattr(campaign, "_v6_run", qualify)
    monkeypatch.setattr(campaign, "_v6_resource_diagnostic", lambda *_args: {})
    monkeypatch.setattr(campaign, "_v6_final_integrity", fail_integrity)
    result = campaign.run(args, budget=bench.Budget(limit_s=10800))
    persisted = json.loads((output / "report.json").read_text())
    assert persisted["retained"] == []
    assert persisted["integrity_validation"] == ("failed" if failure == "changed" else "incomplete")
    assert persisted["status"] == ("failed" if failure == "changed" else "insufficient_evidence")
    assert result == (1 if failure == "changed" else 0)


def test_v6_snapshot_preserves_source_and_creates_complete_frozen_request_manifest(tmp_path):
    from oddsfox_catalogue.current_config import CurrentSettings
    from oddsfox_catalogue.runlock import run_lock

    source = _source_for_snapshot(tmp_path / "source")
    with run_lock(CurrentSettings(source).run_lock_path):
        pass
    before = bench._regular_tree_hashes(source)
    requests = tmp_path / "requests.json"
    result = bench.snapshot_catalogue(source, tmp_path / "snapshot", requests)
    assert result["source_hashes"] == before == bench._regular_tree_hashes(source)
    assert requests.is_file()
    frozen = json.loads(requests.read_text())
    assert len(frozen["requests"]["closed_event_batches"]) == 32
    assert len(frozen["requests"]["active_event_batches"]) == 4
    assert all(
        len(batch) == 100
        for batches in (
            frozen["requests"]["closed_event_batches"],
            frozen["requests"]["active_event_batches"],
        )
        for batch in batches
    )
    assert frozen["requests"]["orphan_market_ids"] == ["9999"]


def test_v6_actual_fixed_worker_accounts_http_and_preserves_source(tmp_path, monkeypatch, capsys):
    import duckdb

    source = tmp_path / "source"
    bench.create_seed(source, 4, 20)
    source_before = bench._regular_tree_hashes(source)
    with duckdb.connect(str(source / "catalogue.duckdb"), read_only=True) as conn:
        closed = conn.execute(
            "SELECT event_id FROM events WHERE closed IS TRUE ORDER BY event_id LIMIT 1"
        ).fetchone()[0]
        active = conn.execute(
            "SELECT event_id FROM events WHERE closed IS FALSE ORDER BY event_id LIMIT 1"
        ).fetchone()[0]
        orphans = [
            row[0]
            for row in conn.execute(
                "SELECT market_id FROM markets ANTI JOIN event_markets USING(venue,market_id) ORDER BY market_id"
            ).fetchall()
        ]
        counts = {
            table: conn.execute(f'SELECT count(*) FROM "{table}"').fetchone()[0]
            for table in ("events", "markets", "event_markets", "outcomes", "_identity_claims")
        }
    requests = tmp_path / "requests.json"
    requests.write_text(
        json.dumps(
            {
                "requests": {
                    "closed_event_batches": [[closed]],
                    "active_event_batches": [[active]],
                    "orphan_market_ids": orphans,
                    "counts": counts,
                },
                "domain_clock": "2026-10-14T01:00:00+00:00",
            }
        )
    )
    output = tmp_path / "evidence"
    output.mkdir()
    package = Path(bench.oddsfox_catalogue.__file__).resolve().parent
    monkeypatch.setenv("FULL_PASS_EXPECTED_PACKAGE", str(package))
    code = bench.fixed_catalogue_worker(source, tmp_path / "target", output, "tiny", requests)
    report = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert code == 0, report
    assert report["status"] == "measured"
    assert report["publication_verified"] is True
    assert report["settled_units"] == 3
    assert report["completed_work"]["closed_batches_requested"] == 1
    assert report["completed_work"]["active_batches_requested"] == 1
    assert report["source_failure"] is None
    assert report["http_attempts"] == report["source_stats"]["http_attempts"]
    assert report["downloaded_bytes"] == report["source_stats"]["downloaded_bytes"]
    assert report["http_attempts"] == report["runtime_reserved_attempts"]
    assert report["downloaded_bytes"] == report["runtime_downloaded_bytes"]
    assert bench._regular_tree_hashes(source) == source_before


def test_v6_fixed_manifest_cannot_change_between_scored_samples(tmp_path, monkeypatch):
    requests = _frozen_requests(tmp_path / "requests.json")
    expected = hashlib.sha256(requests.read_bytes()).hexdigest()
    calls = []
    report = {"stages": {}}
    monkeypatch.setattr(campaign, "_v6_warm", lambda *_args, **_kwargs: {"wall_s": 1})

    def sample(*_args):
        calls.append(True)
        requests.write_text('{"requests": "changed"}')
        return _sample(0)

    monkeypatch.setattr(campaign, "_v6_sample", sample)
    with pytest.raises(campaign.CampaignIntegrityError, match="request manifest changed"):
        campaign._v6_fixed_comparison(
            "fixed",
            "baseline",
            tmp_path / "baseline",
            "candidate",
            tmp_path / "candidate",
            tmp_path / "source",
            requests,
            tmp_path,
            bench.Budget(limit_s=10800),
            report,
            set(),
            expected_requests_hash=expected,
        )
    assert len(calls) == 1
    assert len(report["stages"]["fixed"]["samples"]) == 1


def test_v6_fixed_comparison_rejects_equal_but_incomplete_unit_counts(tmp_path, monkeypatch):
    requests = _frozen_requests(tmp_path / "requests.json")
    monkeypatch.setattr(campaign, "_v6_warm", lambda *_args, **_kwargs: {"wall_s": 1})

    def sample(_source, _requests, _output, package, _budget, _label):
        result = _sample(0, acquire=20 if package.name == "baseline" else 15)
        result["settled_units"] = 36
        result["settled_requests"] = result["settled_requests"][:36]
        return result

    monkeypatch.setattr(campaign, "_v6_sample", sample)
    with pytest.raises(ValueError, match="unit|work"):
        campaign._v6_fixed_comparison(
            "fixed",
            "baseline",
            tmp_path / "baseline",
            "candidate",
            tmp_path / "candidate",
            tmp_path / "source",
            requests,
            tmp_path,
            bench.Budget(limit_s=10800),
            {"stages": {}},
            set(),
        )


def test_v6_attribution_is_confined_to_warmups_and_never_scored_samples(tmp_path, monkeypatch):
    requests = _frozen_requests(tmp_path / "requests.json")
    baseline = _package(tmp_path / "baseline", store="BASELINE = 1\n")
    candidate = _package(tmp_path / "candidate", store="CANDIDATE = 1\n")
    calls = []

    def sample(_source, _requests, _output, package, _budget, _label, **kwargs):
        calls.append((package.name, kwargs.get("attribution", False)))
        return _sample(0, acquire=20 if package == baseline else 15)

    monkeypatch.setattr(campaign, "_v6_sample", sample)
    result = campaign._v6_fixed_comparison(
        "fixed",
        "baseline",
        baseline,
        "candidate",
        candidate,
        tmp_path / "source",
        requests,
        tmp_path,
        bench.Budget(limit_s=10800),
        {"stages": {}},
        set(),
    )
    assert result["status"] == "qualified"
    assert calls[:2] == [("baseline", True), ("candidate", True)]
    assert calls[2:] == [
        ("baseline", False),
        ("candidate", False),
        ("candidate", False),
        ("baseline", False),
        ("baseline", False),
        ("candidate", False),
    ]
